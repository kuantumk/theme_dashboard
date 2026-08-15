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
    """The volume leg reads a time-of-day pace, not the raw screener figure.

    `relative_volume_10d_calc` divides session-to-date volume by a FULL-DAY
    average, so a fixed floor on it is a different filter every hour. Every
    case below therefore pins an explicit `session_fraction`.
    """

    # Expected share of the session's volume done by 10:00 and by 15:00.
    MORNING = 0.1926
    LATE = 0.8103

    def test_admits_on_volume_pace_alone(self):
        # 3x normal pace at 10:00 is a raw rvol of 0.58 — far under the old floor.
        df = frame([{"symbol": "AAA", "rvol": 3.0 * self.MORNING, "change_pct": 0.2}])
        self.assertEqual(len(apply_in_play(df, CFG, session_fraction=self.MORNING)), 1)

    def test_admits_on_change_alone(self):
        df = frame([{"symbol": "AAA", "rvol": 0.4, "change_pct": -8.0}])
        self.assertEqual(len(apply_in_play(df, CFG, session_fraction=self.LATE)), 1)

    def test_rejects_when_neither_leg_qualifies(self):
        # 0.6x normal pace at 15:00, and barely moved.
        df = frame([{"symbol": "AAA", "rvol": 0.6 * self.LATE, "change_pct": 0.3}])
        self.assertEqual(len(apply_in_play(df, CFG, session_fraction=self.LATE)), 0)

    def test_the_same_pace_is_admitted_at_any_hour(self):
        """The defect this replaces: the old floor let nothing in before noon.

        BE/FCEL, 2026-08-14. FCEL held ~2x normal participation from 10:00 while
        up double digits, and the raw floor rejected it until roughly 13:00.
        """
        for fraction in (0.0908, self.MORNING, 0.2982, 0.5021, self.LATE):
            df = frame([{"symbol": "AAA", "rvol": 2.0 * fraction, "change_pct": 0.1}])
            kept = apply_in_play(df, CFG, session_fraction=fraction)
            self.assertEqual(len(kept), 1, f"2x pace rejected at fraction {fraction}")

    def test_thin_tape_is_rejected_at_any_hour(self):
        """BE traded at 0.55-0.70x its normal pace all session on 2026-08-14."""
        for fraction in (0.0908, self.MORNING, 0.2982, 0.5021, self.LATE):
            df = frame([{"symbol": "BE", "rvol": 0.65 * fraction, "change_pct": 1.4}])
            self.assertEqual(len(apply_in_play(df, CFG, session_fraction=fraction)), 0)

    def test_disabling_both_legs_passes_everything_through(self):
        # load_config's override drops None values, so build a config whose legs
        # are genuinely absent by replacing them on the frozen instance.
        from dataclasses import replace
        cfg = replace(CFG, in_play_min_volume_pace=None, in_play_min_change_pct=None)
        df = frame([{"symbol": "AAA", "rvol": 0.1, "change_pct": 0.1}])
        self.assertEqual(len(apply_in_play(df, cfg, session_fraction=self.LATE)), 1)

    def test_missing_metrics_do_not_crash(self):
        df = frame([{"symbol": "AAA", "rvol": None, "change_pct": None}])
        self.assertEqual(len(apply_in_play(df, CFG, session_fraction=self.LATE)), 0)

    def test_omitted_fraction_falls_back_to_the_clock(self):
        """Callers may omit it; the gate must not then admit everything."""
        df = frame([{"symbol": "AAA", "rvol": 0.0, "change_pct": 0.0}])
        self.assertEqual(len(apply_in_play(df, CFG)), 0)


class TestRetiredConfigKey(unittest.TestCase):
    def test_old_raw_rvol_key_is_rejected_with_a_migration_message(self):
        """Silently ignoring it would disable the volume leg with no signal.

        That is the failure mode the whole module exists to prevent, so it must
        not be reintroduced by a stale config file.
        """
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_min_rvol": 1.5})
        self.assertIn("in_play_min_volume_pace", str(caught.exception))


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


if __name__ == "__main__":
    unittest.main()
