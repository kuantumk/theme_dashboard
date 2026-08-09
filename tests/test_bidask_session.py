"""Session accumulator tests."""

import math
import unittest

from src.bidask.config import load_config
from src.bidask.session import MIN_SAMPLES_FOR_WINSOR, SessionAccumulator

CFG = load_config()


def row(symbol, close, bid, ask, volume, **extra):
    return {"symbol": symbol, "close": close, "bid": bid, "ask": ask,
            "volume": volume, **extra}


def buy_sequence(symbol, polls, start_vol=1000, step=500):
    """Polls that print at the ask on a stable quote — unambiguous buying."""
    out = []
    for i in range(polls):
        out.append([row(symbol, close=10.10, bid=10.08, ask=10.10,
                        volume=start_vol + i * step)])
    return out


class TestAccumulation(unittest.TestCase):
    def test_repeated_buys_increment_ask_hits_only(self):
        acc = SessionAccumulator(CFG)
        for poll in buy_sequence("AAA", 5):
            acc.apply(poll, session_date="2026-08-08")
        state = acc.states["AAA"]
        self.assertEqual(state.ask_hits, 4)  # first poll is warmup
        self.assertEqual(state.bid_hits, 0)
        self.assertGreater(state.delta, 0)

    def test_hits_never_exceed_polls(self):
        acc = SessionAccumulator(CFG)
        for poll in buy_sequence("AAA", 6):
            acc.apply(poll, session_date="2026-08-08")
        self.assertLessEqual(acc.states["AAA"].total_hits, acc.polls)

    def test_imbalance_is_bounded_and_zero_when_balanced(self):
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.09, 10.08, 10.10, 1000)], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1500)], session_date="d")
        acc.apply([row("AAA", 10.08, 10.08, 10.10, 2000)], session_date="d")
        state = acc.states["AAA"]
        self.assertEqual(state.ask_hits, 1)
        self.assertEqual(state.bid_hits, 1)
        self.assertEqual(state.imbalance, 0.0)
        self.assertTrue(-1.0 <= state.imbalance <= 1.0)


class TestWinsorization(unittest.TestCase):
    def test_outsized_volume_delta_is_capped(self):
        acc = SessionAccumulator(CFG)
        # Establish a median from steady 500-share increments.
        polls = buy_sequence("AAA", MIN_SAMPLES_FOR_WINSOR + 2)
        for poll in polls:
            acc.apply(poll, session_date="d")
        delta_before = acc.states["AAA"].delta
        last_vol = polls[-1][0]["volume"]

        # A single block print far above the running median.
        acc.apply([row("AAA", 10.10, 10.08, 10.10, last_vol + 10_000_000)],
                  session_date="d")
        contributed = acc.states["AAA"].delta - delta_before
        self.assertLess(contributed, 10_000_000)
        self.assertGreater(contributed, 0)


class TestDivergence(unittest.TestCase):
    def test_disagreeing_count_and_volume_signals_flag_divergent(self):
        acc = SessionAccumulator(CFG)
        state_rows = [
            row("AAA", 10.10, 10.08, 10.10, 1000),   # warmup
            row("AAA", 10.10, 10.08, 10.10, 1100),   # small buy
            row("AAA", 10.10, 10.08, 10.10, 1200),   # small buy
            row("AAA", 10.08, 10.08, 10.10, 900_000),  # one huge sell
        ]
        for r in state_rows:
            acc.apply([r], session_date="d")
        state = acc.states["AAA"]
        self.assertEqual(state.margin, 1)      # counts favour buying
        self.assertLess(state.delta, 0)        # volume favours selling
        self.assertTrue(state.divergent)

    def test_agreeing_signals_are_not_divergent(self):
        acc = SessionAccumulator(CFG)
        for poll in buy_sequence("AAA", 4):
            acc.apply(poll, session_date="d")
        self.assertFalse(acc.states["AAA"].divergent)


class TestCoverage(unittest.TestCase):
    def test_coverage_measures_classification_not_trade_frequency(self):
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")  # no trade
        acc.apply([row("AAA", 10.10, 10.11, 10.10, 1500)], session_date="d")  # crossed
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 2000)], session_date="d")  # good
        stats = acc.snapshot_stats()
        self.assertEqual(stats["attempted"], 3)
        self.assertEqual(stats["traded"], 2)     # the no-trade poll is excluded
        self.assertEqual(stats["classified"], 1)
        # 1 of 2 actual trades classified — not 1 of 3 attempts. A symbol that
        # simply did not print is not a classification failure.
        self.assertAlmostEqual(stats["coverage"], 0.5, places=4)
        self.assertAlmostEqual(stats["trade_rate"], 2 / 3, places=4)
        self.assertIn("no_trade", stats["rejections"])
        self.assertIn("crossed", stats["rejections"])

    def test_coverage_is_one_when_every_trade_classifies(self):
        acc = SessionAccumulator(CFG)
        for poll in buy_sequence("AAA", 4):
            acc.apply(poll, session_date="d")
        self.assertEqual(acc.coverage, 1.0)

    def test_coverage_is_zero_when_nothing_traded(self):
        acc = SessionAccumulator(CFG)
        for _ in range(3):
            acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")
        self.assertEqual(acc.coverage, 0.0)
        self.assertEqual(acc.trade_rate, 0.0)


class TestSessionRoll(unittest.TestCase):
    def test_new_session_date_clears_counters(self):
        acc = SessionAccumulator(CFG)
        for poll in buy_sequence("AAA", 4):
            acc.apply(poll, session_date="2026-08-07")
        self.assertGreater(acc.states["AAA"].total_hits, 0)

        rolled = acc.apply([row("AAA", 10.10, 10.08, 10.10, 50)],
                           session_date="2026-08-08")
        self.assertTrue(rolled)
        self.assertEqual(acc.states["AAA"].total_hits, 0)
        self.assertEqual(acc.attempted, 0)
        self.assertEqual(acc.snapshot_stats()["session_date"], "2026-08-08")

    def test_same_session_date_does_not_roll(self):
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")
        rolled = acc.apply([row("AAA", 10.10, 10.08, 10.10, 1500)], session_date="d")
        self.assertFalse(rolled)
        self.assertEqual(acc.states["AAA"].ask_hits, 1)


class TestSymbolHygiene(unittest.TestCase):
    def test_nan_symbol_is_dropped(self):
        # float('nan') is truthy, so a plain falsiness check lets it through and
        # it renders as a literal "nan" ticker.
        acc = SessionAccumulator(CFG)
        acc.apply([row(float("nan"), 10.10, 10.08, 10.10, 1000)], session_date="d")
        self.assertEqual(acc.states, {})

    def test_none_and_blank_symbols_are_dropped(self):
        acc = SessionAccumulator(CFG)
        acc.apply([row(None, 10.10, 10.08, 10.10, 1000),
                   row("   ", 10.10, 10.08, 10.10, 1000)], session_date="d")
        self.assertEqual(acc.states, {})


class TestTickRuleFreshness(unittest.TestCase):
    def test_oscillating_midspread_price_classifies_by_tick_rule(self):
        # Regression: prior_different_price used to be refreshed *after*
        # classify consumed it, so on the poll where the price moved the
        # classifier still held the pre-change value. On an oscillating price
        # that value equalled cur.last, the tick rule returned 0, and every
        # mid-spread print came back unclassified.
        acc = SessionAccumulator(CFG)
        prices = [10.09, 10.10, 10.09, 10.10, 10.09]
        for i, price in enumerate(prices):
            acc.apply([row("AAA", price, 10.05, 10.15, 1000 + i * 500)],
                      session_date="d")
        state = acc.states["AAA"]
        self.assertEqual(state.total_hits, 4)
        self.assertGreater(state.ask_hits, 0)
        self.assertGreater(state.bid_hits, 0)
        self.assertEqual(acc.coverage, 1.0)


class TestInPlayChurn(unittest.TestCase):
    def test_absence_gap_is_not_booked_as_one_print(self):
        # A ticker that drops below the in-play gate and returns would otherwise
        # have its entire absence booked as a single signed print, because
        # volume_delta spans the whole gap.
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1500)], session_date="d")
        delta_before = acc.states["AAA"].delta

        for _ in range(4):           # ticker absent from the feed
            acc.apply([], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 9_000_000)], session_date="d")

        # The re-entry poll is a warmup, contributing nothing.
        self.assertEqual(acc.states["AAA"].delta, delta_before)

    def test_continuous_presence_still_accumulates(self):
        acc = SessionAccumulator(CFG)
        for poll in buy_sequence("AAA", 4):
            acc.apply(poll, session_date="d")
        self.assertEqual(acc.states["AAA"].ask_hits, 3)


class TestNonFinitePayload(unittest.TestCase):
    def test_nan_meta_field_is_dropped_from_the_payload(self):
        # json.dumps would otherwise emit a bare NaN token — valid Python,
        # invalid JSON — and the browser would reject the whole document.
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.09, 10.08, 10.10, 1000,
                       **{"High.6M": float("nan"), "rvol": float("nan")})],
                  session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1500,
                       **{"High.6M": float("nan"), "rvol": 2.0})],
                  session_date="d")
        payload = acc.states["AAA"].as_dict()
        self.assertNotIn("High.6M", payload)
        self.assertEqual(payload["rvol"], 2.0)

    def test_payload_serializes_as_strict_json(self):
        import json
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.09, 10.08, 10.10, 1000,
                       **{"High.1M": float("nan")})], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1500,
                       **{"High.1M": float("nan")})], session_date="d")
        # allow_nan=False is what the browser effectively enforces.
        json.dumps(acc.states["AAA"].as_dict(), allow_nan=False)

    def test_nan_numeric_field_does_not_poison_delta(self):
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, float("nan"))], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 2000)], session_date="d")
        self.assertTrue(math.isfinite(acc.states["AAA"].delta))


class TestSerialization(unittest.TestCase):
    def test_as_dict_carries_counters_and_meta(self):
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.09, 10.08, 10.10, 1000, change_pct=4.2)],
                  session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1500, change_pct=4.5)],
                  session_date="d")
        payload = acc.states["AAA"].as_dict()
        self.assertEqual(payload["symbol"], "AAA")
        self.assertEqual(payload["ask_hits"], 1)
        self.assertEqual(payload["change_pct"], 4.5)
        self.assertIn("imbalance", payload)


if __name__ == "__main__":
    unittest.main()
