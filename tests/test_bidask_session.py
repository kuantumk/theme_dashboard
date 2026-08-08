"""Session accumulator tests."""

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
    def test_coverage_falls_and_reasons_are_tallied(self):
        acc = SessionAccumulator(CFG)
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000)], session_date="d")  # no trade
        acc.apply([row("AAA", 10.10, 10.11, 10.10, 1500)], session_date="d")  # crossed
        acc.apply([row("AAA", 10.10, 10.08, 10.10, 2000)], session_date="d")  # good
        stats = acc.snapshot_stats()
        self.assertEqual(stats["attempted"], 3)
        self.assertEqual(stats["classified"], 1)
        self.assertAlmostEqual(stats["coverage"], 1 / 3, places=4)
        self.assertIn("no_trade", stats["rejections"])
        self.assertIn("crossed", stats["rejections"])


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
