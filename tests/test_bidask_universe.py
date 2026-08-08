"""Universe filter tests."""

import unittest

import pandas as pd

from src.bidask.config import load_config
from src.bidask.feed import _bare_ticker
from src.bidask.universe import apply_in_play, apply_liquidity, build_universe

CFG = load_config()


def frame(rows):
    return pd.DataFrame(rows)


class TestTickerNormalization(unittest.TestCase):
    def test_exchange_prefix_is_stripped(self):
        self.assertEqual(_bare_ticker("NASDAQ:WOLF"), "WOLF")
        self.assertEqual(_bare_ticker("NYSE:OC"), "OC")
        self.assertEqual(_bare_ticker("AMEX:BTG"), "BTG")

    def test_bare_ticker_passes_through(self):
        self.assertEqual(_bare_ticker("WOLF"), "WOLF")


class TestLiquidity(unittest.TestCase):
    def test_avg_dollar_volume_is_derived_and_filtered(self):
        # The screener has no avg-dollar-volume column and the library rejects
        # column arithmetic, so this floor can only be applied client-side.
        df = frame([
            {"symbol": "RICH", "close": 100.0, "avg_volume": 1_000_000},   # $100M
            {"symbol": "THIN", "close": 2.0, "avg_volume": 1_000_000},     # $2M
        ])
        out = apply_liquidity(df, CFG)
        self.assertEqual(out["symbol"].tolist(), ["RICH"])
        self.assertEqual(out.iloc[0]["avg_dollar_vol"], 100_000_000)

    def test_missing_avg_volume_skips_the_floor(self):
        # Crypto carries no average-volume field; rows must not be dropped as if
        # they had failed the floor.
        df = frame([{"symbol": "BTC", "close": 65000.0, "avg_volume": None}])
        out = apply_liquidity(df, CFG)
        self.assertEqual(len(out), 1)
        self.assertIsNone(out.iloc[0]["avg_dollar_vol"])


class TestInPlayGate(unittest.TestCase):
    def test_admits_on_relative_volume_alone(self):
        df = frame([{"symbol": "AAA", "rvol": 3.0, "change_pct": 0.2}])
        self.assertEqual(len(apply_in_play(df, CFG)), 1)

    def test_admits_on_change_alone(self):
        df = frame([{"symbol": "AAA", "rvol": 0.4, "change_pct": -8.0}])
        self.assertEqual(len(apply_in_play(df, CFG)), 1)

    def test_rejects_when_neither_leg_qualifies(self):
        df = frame([{"symbol": "AAA", "rvol": 0.5, "change_pct": 0.3}])
        self.assertEqual(len(apply_in_play(df, CFG)), 0)

    def test_disabling_both_legs_passes_everything_through(self):
        cfg = load_config({"in_play_min_rvol": None, "in_play_min_change_pct": None})
        # load_config's override drops None values, so build a config whose legs
        # are genuinely absent by replacing them on the frozen instance.
        from dataclasses import replace
        cfg = replace(cfg, in_play_min_rvol=None, in_play_min_change_pct=None)
        df = frame([{"symbol": "AAA", "rvol": 0.1, "change_pct": 0.1}])
        self.assertEqual(len(apply_in_play(df, cfg)), 1)

    def test_missing_metrics_do_not_crash(self):
        df = frame([{"symbol": "AAA", "rvol": None, "change_pct": None}])
        self.assertEqual(len(apply_in_play(df, CFG)), 0)


class TestBuildUniverse(unittest.TestCase):
    def test_in_play_can_be_bypassed(self):
        df = frame([{"symbol": "AAA", "close": 100.0, "avg_volume": 1_000_000,
                     "rvol": 0.1, "change_pct": 0.1}])
        self.assertEqual(len(build_universe(df, CFG, in_play=True)), 0)
        self.assertEqual(len(build_universe(df, CFG, in_play=False)), 1)

    def test_empty_frame_survives(self):
        self.assertTrue(build_universe(frame([]), CFG).empty)


class TestConfigValidation(unittest.TestCase):
    def test_twenty_day_window_is_rejected_with_a_clear_message(self):
        with self.assertRaises(ValueError) as ctx:
            load_config({"avg_window_days": 20})
        self.assertIn("20", str(ctx.exception))
        self.assertIn("null", str(ctx.exception))

    def test_avg_volume_field_tracks_the_window(self):
        self.assertEqual(load_config({"avg_window_days": 30}).avg_volume_field,
                         "average_volume_30d_calc")


if __name__ == "__main__":
    unittest.main()
