"""Behavioral regressions from the Tape Pressure calculation review."""
import json
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from src.bidask import rvol_at_time as rv, server
from src.bidask.config import load_config
from src.bidask.feed import Payload
from src.bidask.grouping import build_columns


class TestBaselineValidity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.engine = server.TapeEngine(load_config(), Path(self.temp.name), markets=("crypto",))
        self.day = "2026-09-18"
        self.rows = pd.DataFrame([{"symbol": "BTC", "feed_symbol": "BINANCE:BTCUSDT.P"}])
        self.curve = np.cumsum(np.full(rv.CRYPTO_GRID.slots, 0.1))

    def join(self):
        worker = self.engine._profile_threads["crypto"]
        if worker:
            worker.join(timeout=3)
            self.assertFalse(worker.is_alive())

    def test_rollover_clears_profiles_and_discards_old_worker_and_cache(self):
        started, release = threading.Event(), threading.Event()
        def build(*args, **kwargs):
            started.set()
            self.assertTrue(release.wait(3))
            return {"BINANCE:BTCUSDT.P": self.curve}
        with mock.patch.object(server, "build_for_symbols", side_effect=build), \
             mock.patch.object(server, "save_profiles") as save, \
             mock.patch.object(server, "prune_cache") as prune:
            self.engine.ensure_profiles("crypto", self.day, self.rows)
            self.assertTrue(started.wait(3))
            self.engine.profiles["crypto"] = {"BINANCE:BTCUSDT.P": self.curve}
            self.engine.profile_dates["crypto"] = self.day
            self.engine.profile_status["crypto"] = "ready (1/1)"
            try:
                self.engine.ensure_profiles("crypto", "2026-09-19", self.rows)
                self.assertEqual(self.engine.profiles["crypto"], {})
                self.assertEqual(self.engine.profile_status["crypto"], "pending")
            finally:
                release.set()
                self.join()
            self.assertIsNone(self.engine.profile_dates["crypto"])
            self.assertEqual(self.engine.profiles["crypto"], {})
            save.assert_not_called()
            prune.assert_not_called()

    def test_instrument_switch_keeps_memory_after_cache_write_failure(self):
        with mock.patch.object(server, "save_profiles", return_value=False), \
             mock.patch.object(server, "build_for_symbols", return_value={"BINANCE:BTCUSDT.P": self.curve}):
            self.engine.ensure_profiles("crypto", self.day, self.rows)
            self.join()
        spot = self.rows.assign(feed_symbol="BINANCE:BTCUSDT")
        with mock.patch.object(server, "build_for_symbols", return_value={"BINANCE:BTCUSDT": self.curve * 10}) as build:
            self.engine.ensure_profiles("crypto", self.day, spot)
            self.join()
        self.assertEqual(build.call_args.args[0], ["BINANCE:BTCUSDT"])
        self.assertEqual(set(self.engine.profiles["crypto"]), {"BINANCE:BTCUSDT", "BINANCE:BTCUSDT.P"})

    def test_partial_build_retries_only_missing_instruments_with_backoff(self):
        rows = pd.concat([self.rows, pd.DataFrame([{"symbol": "ETH", "feed_symbol": "BINANCE:ETHUSDT"}])])
        with mock.patch.object(server, "build_for_symbols", return_value={"BINANCE:BTCUSDT.P": self.curve}) as build:
            self.engine.ensure_profiles("crypto", self.day, rows)
            self.join()
            self.engine.ensure_profiles("crypto", self.day, rows)
            self.assertEqual(build.call_count, 1)
        self.assertGreater(self.engine._profile_retry_at["crypto"], 0)
        self.engine._profile_retry_at["crypto"] = 0
        with mock.patch.object(server, "build_for_symbols", return_value={"BINANCE:ETHUSDT": self.curve}) as build:
            self.engine.ensure_profiles("crypto", self.day, rows)
            self.join()
        self.assertEqual(build.call_args.args[0], ["BINANCE:ETHUSDT"])
        self.assertEqual(self.engine._profile_retry_at["crypto"], 0)

    def test_cache_read_exception_is_reported_and_backed_off(self):
        with mock.patch.object(server, "load_profiles", side_effect=ValueError("bad cache")):
            self.engine.ensure_profiles("crypto", self.day, self.rows)
            self.join()
        self.assertIn("failed", self.engine.profile_status["crypto"])
        self.assertGreater(self.engine._profile_retry_at["crypto"], 0)


class TestCalculationBoundaries(unittest.TestCase):
    def test_fractional_cache_round_trip_preserves_ratio_and_rejects_old_version(self):
        with tempfile.TemporaryDirectory() as temp:
            curve = np.full(rv.CRYPTO_GRID.slots, 0.4)
            self.assertTrue(rv.save_profiles({"BINANCE:BTCUSDT": curve}, Path(temp), "2026-09-18", market="crypto", grid=rv.CRYPTO_GRID))
            loaded = rv.load_profiles(Path(temp), "2026-09-18", market="crypto", grid=rv.CRYPTO_GRID)
            self.assertEqual(rv.rvol_at_time(0.8, loaded["BINANCE:BTCUSDT"], 5, rv.CRYPTO_GRID), 2)
            path = rv.cache_path(Path(temp), "2026-09-18", "crypto")
            data = json.loads(path.read_text())
            data["version"] = 3
            path.write_text(json.dumps(data))
            self.assertEqual(rv.load_profiles(Path(temp), "2026-09-18", market="crypto", grid=rv.CRYPTO_GRID), {})

    def test_invalid_volume_excludes_day_but_absent_no_trade_bar_is_allowed(self):
        indices = [pd.date_range(f"2026-09-{day} 04:00", periods=192, freq="5min", tz="America/New_York") for day in (16, 17)]
        frame = pd.DataFrame({"Volume": [100.] * 192 + [1000.] * 192}, index=indices[0].append(indices[1]))
        for invalid in (np.nan, np.inf, -1):
            broken = frame.copy()
            broken.iloc[192, 0] = invalid
            curve = rv.build_profiles({"AAA": broken})["AAA"]
            self.assertEqual(curve[-1], 19200.)
        absent = frame.drop(indices[1][0])
        self.assertEqual(rv.build_profiles({"AAA": absent})["AAA"][-1], (19200 + 191000) / 2)

    def test_full_payload_keeps_scoring_members_beyond_display_limit(self):
        cfg = load_config({"max_rows_per_column": 1, "group_rvol_cap": 2.0})
        rows = [{"symbol": symbol, "rvol_at_time": ratio, "sides": ["strong"], "industry": "Group"} for symbol, ratio in (("A", 5), ("B", 1), ("C", 1))]
        columns = build_columns(rows, {}, cfg, universe=rows, include_all=True)
        self.assertEqual(len(columns["strong"][0]["members"]), 3)
        self.assertEqual(columns["strong"][0]["roster"], 3)
        self.assertAlmostEqual(columns["strong"][0]["score"], 1.8333)
        self.assertEqual(columns["scoring"]["rvol_cap"], 2.)
        self.assertEqual(columns["scoring"]["max_rows_per_column"], 1)


class TestClockAndFeedIntegration(unittest.TestCase):
    def test_crypto_clock_grid_and_warmup_use_utc_not_et(self):
        # It is still Sep17 in New York. Real elapsed-time and profile
        # reduction functions must nevertheless use Sep18, 02:30 UTC.
        now = datetime(2026, 9, 18, 2, 30, tzinfo=server.UTC)
        clock = mock.Mock()
        clock.now.side_effect = lambda tz=None: now.astimezone(tz) if tz else now
        index = pd.date_range("2026-09-16", periods=288 * 2 + 30, freq="5min", tz="UTC").tz_convert(server.ET)
        frame = pd.DataFrame({"Volume": [1.] * 576 + [1000.] * 30}, index=index)
        rows = pd.DataFrame([{"symbol": "BTC", "feed_symbol": "BINANCE:BTCUSDT.P", "volume": 60., "close": 100., "change_pct": 1.}])
        payload = Payload(rows=rows, feed="streaming", matched=1, market_status="24/7")
        with tempfile.TemporaryDirectory() as temp, \
             mock.patch.object(server, "datetime", clock), \
             mock.patch.object(server, "fetch", return_value=payload), \
             mock.patch.object(rv, "fetch_bars", return_value={"BINANCE:BTCUSDT.P": frame}):
            engine = server.TapeEngine(load_config(), Path(temp), markets=("crypto",))
            engine.poll_once()
            engine._profile_threads["crypto"].join(timeout=3)
            engine.poll_once()
            self.assertEqual(engine.profile_dates["crypto"], "2026-09-18")
            self.assertEqual(engine.qualified["crypto"][0]["rvol_at_time"], 2.)
            self.assertEqual(engine.build_state()["crypto"]["rvol"]["anchor"], "00:00 UTC")

    def test_delayed_rows_fail_closed_even_in_mixed_feed(self):
        now = datetime(2026, 9, 18, 12, tzinfo=server.UTC)
        clock = mock.Mock()
        clock.now.side_effect = lambda tz=None: now.astimezone(tz) if tz else now
        for mixed in (False, True):
            rows = pd.DataFrame([{"symbol": s, "volume": 1000., "close": 100., "change_pct": 1., "update_mode": mode} for s, mode in (("BTC", "delayed_streaming_900"), ("ETH", "streaming" if mixed else "delayed_streaming_900"))])
            payload = Payload(rows=rows, feed="mixed" if mixed else "delayed_streaming_900", matched=2, market_status="24/7")
            with tempfile.TemporaryDirectory() as temp, \
                 mock.patch.object(server, "datetime", clock), \
                 mock.patch.object(server, "fetch", return_value=payload), \
                 mock.patch.object(server, "build_for_symbols", return_value={s: np.full(288, 100.) for s in ("BTC", "ETH")}):
                engine = server.TapeEngine(load_config(), Path(temp), markets=("crypto",))
                engine.poll_once()
                engine._profile_threads["crypto"].join(timeout=3)
                engine.poll_once()
                self.assertEqual([r["symbol"] for r in engine.qualified["crypto"]], ["ETH"] if mixed else [])
                status = engine.build_state()["crypto"]["rvol"]
                self.assertEqual(status["delayed_rows"], 1 if mixed else 2)
                if not mixed:
                    self.assertIn("observation time", status["reason"])


if __name__ == "__main__":
    unittest.main()
