"""Universe filter tests."""

import unittest
from unittest import mock

import numpy as np
import pandas as pd

from src.bidask.config import load_config
from src.bidask.feed import _bare_ticker
from src.bidask.rvol_at_time import BARS_PER_SESSION
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
    """The volume leg is Relative Volume at Time, against each ticker's own past.

    The gate divides today's session volume by that ticker's mean volume by the
    same time of day, so every case below supplies a baseline profile and an
    explicit elapsed-minutes value rather than reading the clock.
    """

    @staticmethod
    def profile(total_by_close):
        return np.cumsum(np.full(BARS_PER_SESSION, total_by_close / BARS_PER_SESSION))

    def setUp(self):
        # A ticker that normally trades 780k shares evenly: 10k per 5-min bar,
        # so 60 minutes in it has usually done 120k.
        self.profiles = {"AAA": self.profile(780_000), "BE": self.profile(780_000)}

    def test_admits_on_volume_alone(self):
        # 2x its usual by 60 minutes, and barely moved on price.
        df = frame([{"symbol": "AAA", "volume": 240_000, "change_pct": 0.2}])
        kept = apply_in_play(df, CFG, profiles=self.profiles, elapsed_minutes=60)
        self.assertEqual(len(kept), 1)

    def test_admits_on_change_alone(self):
        df = frame([{"symbol": "AAA", "volume": 1_000, "change_pct": -8.0}])
        kept = apply_in_play(df, CFG, profiles=self.profiles, elapsed_minutes=60)
        self.assertEqual(len(kept), 1)

    def test_rejects_when_neither_leg_qualifies(self):
        # 0.65x its usual by 60 minutes, and barely moved.
        df = frame([{"symbol": "AAA", "volume": 78_000, "change_pct": 0.3}])
        kept = apply_in_play(df, CFG, profiles=self.profiles, elapsed_minutes=60)
        self.assertEqual(len(kept), 0)

    def test_the_same_relative_volume_reads_the_same_at_any_hour(self):
        """The defect this replaces: the raw floor let nothing in before noon.

        FCEL held ~2x its usual participation from 10:00 on 2026-08-14 while up
        double digits, and the raw figure could not clear 1.5 until mid-session.
        """
        for minutes in (15, 30, 60, 120, 300):
            usual = 10_000 * (minutes / 5.0)
            df = frame([{"symbol": "AAA", "volume": 2.0 * usual, "change_pct": 0.1}])
            kept = apply_in_play(df, CFG, profiles=self.profiles,
                                 elapsed_minutes=minutes)
            self.assertEqual(len(kept), 1, f"2x usual rejected at t={minutes}")

    def test_thin_tape_is_rejected_at_any_hour(self):
        """BE traded at 0.55-0.70x its usual participation all session."""
        for minutes in (15, 30, 60, 120, 300):
            usual = 10_000 * (minutes / 5.0)
            df = frame([{"symbol": "BE", "volume": 0.65 * usual, "change_pct": 1.4}])
            kept = apply_in_play(df, CFG, profiles=self.profiles,
                                 elapsed_minutes=minutes)
            self.assertEqual(len(kept), 0, f"thin tape admitted at t={minutes}")

    def test_early_bands_are_looser_than_late_ones(self):
        """0.9x its usual clears the 09:35 floor but not the 10:30 one."""
        for minutes, expected in ((10, 1), (120, 0)):
            usual = 10_000 * (minutes / 5.0)
            df = frame([{"symbol": "AAA", "volume": 0.9 * usual, "change_pct": 0.1}])
            kept = apply_in_play(df, CFG, profiles=self.profiles,
                                 elapsed_minutes=minutes)
            self.assertEqual(len(kept), expected, f"wrong at t={minutes}")

    def test_a_ticker_with_no_baseline_is_not_admitted_on_volume(self):
        """Warm-up still running, fresh listing, or a download miss."""
        df = frame([{"symbol": "ZZZ", "volume": 99_000_000, "change_pct": 0.1}])
        kept = apply_in_play(df, CFG, profiles=self.profiles, elapsed_minutes=60)
        self.assertEqual(len(kept), 0)

    def test_that_ticker_still_reaches_the_board_on_price(self):
        """Failing closed on volume must not blind the change leg."""
        df = frame([{"symbol": "ZZZ", "volume": 99_000_000, "change_pct": 7.0}])
        kept = apply_in_play(df, CFG, profiles={}, elapsed_minutes=60)
        self.assertEqual(len(kept), 1)

    def test_disabling_both_legs_passes_everything_through(self):
        from dataclasses import replace
        cfg = replace(CFG, in_play_rvol_schedule=(), in_play_min_change_pct=None)
        df = frame([{"symbol": "AAA", "volume": 1, "change_pct": 0.1}])
        self.assertEqual(len(apply_in_play(df, cfg, elapsed_minutes=60)), 1)

    def test_missing_metrics_do_not_crash(self):
        df = frame([{"symbol": "AAA", "volume": None, "change_pct": None}])
        kept = apply_in_play(df, CFG, profiles=self.profiles, elapsed_minutes=60)
        self.assertEqual(len(kept), 0)

    def test_omitted_elapsed_falls_back_to_the_clock(self):
        """Callers may omit it; the gate must not then admit everything."""
        df = frame([{"symbol": "AAA", "volume": 0, "change_pct": 0.0}])
        self.assertEqual(len(apply_in_play(df, CFG, profiles=self.profiles)), 0)


class TestRetiredConfigKeys(unittest.TestCase):
    """Silently ignoring a retired key would disable the volume leg with no
    signal — the exact failure this whole module exists to prevent."""

    def test_raw_rvol_key_is_rejected_with_a_migration_message(self):
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_min_rvol": 1.5})
        self.assertIn("in_play_rvol_schedule", str(caught.exception))

    def test_the_interim_pace_key_is_also_rejected(self):
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_min_volume_pace": 1.5})
        self.assertIn("in_play_rvol_schedule", str(caught.exception))

    def test_a_malformed_schedule_entry_raises(self):
        """A skipped band is a hole in the gate at one time of day only."""
        with self.assertRaises(ValueError):
            load_config({"in_play_rvol_schedule": [[5, 0.8], "nonsense"]})


class TestStablecoinExclusion(unittest.TestCase):
    def test_stablecoins_are_dropped_from_crypto(self):
        # Pegged assets accumulate hits from micro-oscillation around $1 and
        # float to the top of the column carrying no information.
        df = frame([
            {"symbol": "USDC", "close": 1.0, "avg_volume": None},
            {"symbol": "USDT", "close": 1.0, "avg_volume": None},
            {"symbol": "BTC", "close": 65000.0, "avg_volume": None},
        ])
        out = build_universe(df, CFG, in_play=False, market="crypto")
        self.assertEqual(out["symbol"].tolist(), ["BTC"])

    def test_exclusion_is_case_insensitive(self):
        df = frame([{"symbol": "usdc", "close": 1.0, "avg_volume": None},
                    {"symbol": "BTC", "close": 65000.0, "avg_volume": None}])
        out = build_universe(df, CFG, in_play=False, market="crypto")
        self.assertEqual(out["symbol"].tolist(), ["BTC"])

    def test_gold_backed_tokens_are_kept(self):
        # PAXG/XAUT track a real moving asset; they are not pegged.
        df = frame([{"symbol": "PAXG", "close": 2600.0, "avg_volume": None},
                    {"symbol": "XAUT", "close": 2600.0, "avg_volume": None}])
        out = build_universe(df, CFG, in_play=False, market="crypto")
        self.assertEqual(sorted(out["symbol"].tolist()), ["PAXG", "XAUT"])

    def test_equity_market_is_unaffected(self):
        # "USDC" as an equity ticker must not be filtered by a crypto rule.
        df = frame([{"symbol": "USDC", "close": 50.0, "avg_volume": 1_000_000}])
        out = build_universe(df, CFG, in_play=False, market="equity")
        self.assertEqual(len(out), 1)


class TestMarketStatus(unittest.TestCase):
    """Market state and feed entitlement are different questions.

    A real-time entitlement on a closed market is still a closed market, so the
    UI must not read `update_mode` as "the market is live".
    """

    def test_session_field_maps_to_a_human_label(self):
        from src.bidask.feed import _market_status
        # `market` is the value the feed actually sends during the regular
        # session (verified live 2026-08-12). The original `regular` spelling was
        # assumed, never observed, and left the UI styling an open market as
        # delayed; both map now so neither vintage regresses.
        self.assertEqual(_market_status(frame([{"current_session": "market"}])),
                         "market open")
        self.assertEqual(_market_status(frame([{"current_session": "regular"}])),
                         "market open")
        self.assertEqual(_market_status(frame([{"current_session": "out_of_session"}])),
                         "market closed")
        self.assertEqual(_market_status(frame([{"current_session": "pre_market"}])),
                         "pre-market")
        self.assertEqual(_market_status(frame([{"current_session": "post_market"}])),
                         "after hours")

    def test_unknown_session_value_is_passed_through_readably(self):
        from src.bidask.feed import _market_status
        self.assertEqual(_market_status(frame([{"current_session": "some_new_state"}])),
                         "some new state")

    def test_missing_session_column_yields_empty(self):
        from src.bidask.feed import _market_status
        self.assertEqual(_market_status(frame([{"close": 10.0}])), "")

    def test_payload_market_open_only_for_trading_states(self):
        from src.bidask.feed import Payload
        import pandas as pd
        empty = pd.DataFrame()
        self.assertTrue(Payload(rows=empty, market_status="market open").market_open)
        self.assertTrue(Payload(rows=empty, market_status="pre-market").market_open)
        self.assertFalse(Payload(rows=empty, market_status="market closed").market_open)
        self.assertFalse(Payload(rows=empty, market_status="").market_open)


class TestLiquidityFieldsReachThePayload(unittest.TestCase):
    def test_volume_and_dollar_vol_are_carried_per_ticker(self):
        # The UI sliders filter client-side against these, so they must survive
        # into the per-ticker payload rather than being dropped as unused meta.
        from src.bidask.session import SessionAccumulator
        acc = SessionAccumulator(CFG)
        for i, price in enumerate((10.09, 10.10)):
            acc.apply([{"symbol": "AAA", "close": price, "bid": 10.08, "ask": 10.10,
                        "volume": 1000 + i * 500, "dollar_vol": 5_050_000.0}],
                      session_date="d")
        payload = acc.states["AAA"].as_dict()
        self.assertEqual(payload["volume"], 1500)
        self.assertEqual(payload["dollar_vol"], 5_050_000.0)


class TestCadenceClamp(unittest.TestCase):
    def test_requested_cadence_is_bounded(self):
        self.assertEqual(CFG.clamp_poll_seconds(1), CFG.min_poll_seconds)
        self.assertEqual(CFG.clamp_poll_seconds(9999), CFG.max_poll_seconds)
        self.assertEqual(CFG.clamp_poll_seconds(15), 15)

    def test_garbage_cadence_falls_back_to_the_configured_value(self):
        self.assertEqual(CFG.clamp_poll_seconds(None), CFG.poll_seconds)
        self.assertEqual(CFG.clamp_poll_seconds("fast"), CFG.poll_seconds)


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



class _StubQuery:
    """Stands in for the screener builder and records what was asked for.

    The fetchers build their query as one long method chain, so every builder
    method returns `self` and only `get_scanner_data` produces anything. The
    recorded `selected` / `filters` / `ordered` are what let a test assert on
    the request rather than only on the response.
    """

    last = None

    def __init__(self, matched, df):
        self._matched, self._df = matched, df
        self.selected, self.filters, self.ordered = (), (), None
        type(self).last = self

    def set_markets(self, *args):
        return self

    def select(self, *columns):
        self.selected = columns
        return self

    def where(self, *conditions):
        self.filters = conditions
        return self

    def order_by(self, field, **kwargs):
        self.ordered = field
        return self

    def limit(self, count):
        return self

    def get_scanner_data(self, **kwargs):
        return self._matched, self._df


def stub_screener(rows, matched=None):
    """Patch the screener with a canned response for one fetch."""
    df = frame(rows)
    return mock.patch("src.bidask.feed.Query",
                      lambda: _StubQuery(len(df) if matched is None else matched, df))


# One screener row, shaped as the live feed shapes it. `Value.Traded` is absent
# on purpose — that is the whole point of these tests.
def screener_row(**overrides):
    row = {"ticker": "NASDAQ:AAA", "name": "AAA", "close": 20.0, "bid": None,
           "ask": None, "change": 4.0, "volume": 5_000_000,
           "relative_volume_10d_calc": 1.4, "market_cap_basic": 1e9,
           "sector": "Technology", "industry": "Software",
           "update_mode": "streaming", "current_session": "pre_market",
           CFG.avg_volume_field: 4_000_000}
    row.update(overrides)
    return row


class TestTradedValueFieldIsGone(unittest.TestCase):
    """`Value.Traded` is not a field the `america` scanner serves.

    Selecting a field the scanner does not publish does NOT error — it returns
    null for every row, the same trap `tvquote.py` documents for `bid`/`ask`.
    A server-side floor on it therefore matches zero rows, and an empty frame
    is indistinguishable from a closed market: verified 2026-08-21 at 09:34 ET
    with the market open and `volume`/`close` both live, `Value.Traded` was
    null on every row and `Value.Traded >= $1M` matched 0 of 13,661, while the
    average-volume leg alone matched 2,806. The field is absent from the
    scanner's 3,771-field metainfo, which lists every other column selected
    here.
    """

    def test_the_query_never_names_the_missing_field(self):
        from src.bidask.feed import fetch_equities
        with stub_screener([screener_row()]):
            fetch_equities(CFG)
        query = _StubQuery.last
        self.assertNotIn("Value.Traded", query.selected)
        self.assertNotIn("Value.Traded", [c.get("left") for c in query.filters])
        self.assertNotEqual(query.ordered, "Value.Traded")

    def test_a_frame_without_the_field_still_yields_a_universe(self):
        # The regression: with the floor pushed server-side against a null
        # column, this came back empty on every poll and the board went dark.
        from src.bidask.feed import fetch_equities
        with stub_screener([screener_row()]):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.rows["symbol"].tolist(), ["AAA"])
        self.assertEqual(payload.rows.iloc[0]["dollar_vol"], 100_000_000)

    def test_todays_traded_value_floor_still_cuts(self):
        # The replacement must be a real floor, not a no-op that admits the
        # whole screener. close x volume is this file's own long-standing
        # definition of the same quantity.
        from src.bidask.feed import fetch_equities
        thin = screener_row(ticker="NASDAQ:BBB", name="BBB", close=0.10,
                            volume=1_000)  # $100 traded
        with stub_screener([screener_row(), thin]):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.rows["symbol"].tolist(), ["AAA"])

    def test_an_unusable_reading_fails_closed(self):
        # A missing price or volume is an unknown, and an unknown must not
        # clear a floor as though it had qualified.
        from src.bidask.feed import fetch_equities
        with stub_screener([screener_row(volume=np.nan)]):
            self.assertTrue(fetch_equities(CFG).rows.empty)

    def test_a_text_typed_column_cannot_kill_the_poll(self):
        # The derivation sits OUTSIDE the fetcher's try block, so a raise here
        # escapes `poll_once` to the poll loop, which never writes the state
        # file — the page then freezes on the last good poll while the console
        # scrolls one line. On a text column `"20.0" * 5_000_000` builds a
        # 35MB string and comparing it against the floor raises TypeError.
        # Coercing both legs keeps the poll alive: a parseable figure is
        # recovered, an unparseable one becomes NaN and fails the floor.
        from src.bidask.feed import fetch_equities
        rows = [
            screener_row(close="20.0"),                      # $100M, parseable
            screener_row(ticker="NASDAQ:CCC", name="CCC", close="n/a"),
        ]
        with stub_screener(rows):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.rows["symbol"].tolist(), ["AAA"])
        self.assertEqual(payload.rows.iloc[0]["dollar_vol"], 100_000_000)


    def test_a_withdrawn_column_surfaces_as_a_feed_error(self):
        # The derivation reads df["close"] directly, so the next withdrawn
        # column raises KeyError rather than nulling. Nothing above catches it
        # — not poll_once — so the poll loop would swallow it and never write
        # the state file, freezing every field on the page including the
        # clock. A visible feed error is the whole point of the guard.
        from src.bidask.feed import fetch_equities
        row = screener_row()
        row.pop("close")
        with stub_screener([row]):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.error, "KeyError")
        self.assertTrue(payload.rows.empty)
        # The reading the response did carry survives the failure, so the
        # session pill keeps telling the truth while the feed pill reports.
        self.assertEqual(payload.market_status, "pre-market")


class TestEmptyResultStillReportsTheFeed(unittest.TestCase):
    """An empty board must not be reported as an unknown market on a dead feed.

    `market_status=""` renders as the literal word "unknown" and `feed=""`
    renders as "delayed feed", so dropping both on the empty path turned a
    real-time pre-market session into two false claims on screen. That is what
    made the missing-field failure above unreadable for a full session.
    """

    def test_equity_feed_and_session_survive_a_fully_filtered_frame(self):
        from src.bidask.feed import fetch_equities
        thin = screener_row(close=0.10, volume=1_000)  # below every floor
        with stub_screener([thin]):
            payload = fetch_equities(CFG)
        self.assertTrue(payload.rows.empty)
        self.assertEqual(payload.market_status, "pre-market")
        self.assertEqual(payload.feed, "streaming")
        self.assertFalse(payload.delayed)

    def test_crypto_feed_survives_a_frame_of_unnamed_rows(self):
        from src.bidask.feed import fetch_crypto
        with mock.patch("src.bidask.feed.screeners") as screeners:
            df = frame([{"base_currency": None, "close": 1.0, "volume": 1.0,
                         "update_mode": "streaming"}])
            screeners.crypto = lambda: _StubQuery(1, df)
            payload = fetch_crypto(CFG)
        self.assertTrue(payload.rows.empty)
        self.assertEqual(payload.feed, "streaming")
        self.assertEqual(payload.market_status, "24/7")


if __name__ == "__main__":
    unittest.main()
