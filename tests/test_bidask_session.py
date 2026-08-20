"""Session accumulator tests."""

import math
import unittest

from src.bidask.config import load_config
from src.bidask.session import MIN_SAMPLES_FOR_WINSOR, SessionAccumulator, TickerState

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
        # The good poll prints at a *different* price, so the tick rule can
        # speak. It has to: the poll before it was crossed, so there is no
        # usable pre-trade book for the quote rule to read, and a same-price
        # print here would be honestly unclassified rather than counted.
        acc.apply([row("AAA", 10.09, 10.08, 10.10, 2000)], session_date="d")  # good
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


class TestRollingWindow(unittest.TestCase):
    """Counters cover a trailing window, not the whole session.

    Measured live on 2026-08-20 against the running dashboard: the app's hit
    increments track price well over minutes (Spearman +0.31 at 30s, +0.40 at
    60s, +0.47 at 3min) while its cumulative session margin over the same
    stretch scored +0.18, and against the day's move +0.10. The reason is
    arithmetic: each poll contributes a small constant per-ticker bias on top of
    the directional signal, so the bias grows with the poll count while the
    signal stays bounded by the day's net move. A trailing window bounds the
    bias term instead of letting it compound.
    """

    def setUp(self):
        self.cfg = load_config({"hit_window_minutes": 5})

    def _one_sided(self, acc, polls, start=0.0, step=10.0):
        """Identical ask-side prints, one per `step` seconds."""
        for i in range(polls):
            acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000 + i * 500)],
                      session_date="d", now=start + i * step)

    def test_margin_is_bounded_by_the_window_not_the_session(self):
        acc = SessionAccumulator(self.cfg)
        self._one_sided(acc, 400)          # 400 polls x 10s = 66 minutes
        # 5-minute window at a 10s cadence holds at most 31 observations.
        self.assertLessEqual(acc.states["AAA"].total_hits, 31)
        self.assertGreater(acc.states["AAA"].total_hits, 25)

    def test_the_window_is_measured_in_time_not_polls(self):
        # The in-app cadence control retunes the poll rate at runtime. A window
        # counted in polls would silently change horizon with it.
        fast = SessionAccumulator(self.cfg)
        self._one_sided(fast, 200, step=5.0)
        slow = SessionAccumulator(self.cfg)
        self._one_sided(slow, 200, step=20.0)
        self.assertGreater(fast.states["AAA"].total_hits,
                           slow.states["AAA"].total_hits * 3)

    def test_a_ticker_that_stops_printing_leaves_the_board(self):
        acc = SessionAccumulator(self.cfg)
        self._one_sided(acc, 20)
        self.assertGreater(acc.states["AAA"].total_hits, 0)
        # Ten minutes later the feed still carries it, but nothing has traded.
        acc.apply([row("BBB", 10.09, 10.08, 10.10, 1000)],
                  session_date="d", now=600.0)
        self.assertEqual(acc.states["AAA"].total_hits, 0)
        self.assertEqual(acc.active(min_hits=1), [])

    def test_volume_delta_ages_out_with_its_observation(self):
        acc = SessionAccumulator(self.cfg)
        self._one_sided(acc, 20)
        self.assertGreater(acc.states["AAA"].delta, 0)
        acc.apply([row("BBB", 10.09, 10.08, 10.10, 1000)],
                  session_date="d", now=600.0)
        self.assertEqual(acc.states["AAA"].delta, 0.0)

    def test_the_window_clock_is_monotonic_not_wall_clock(self):
        """A wall clock steps backwards; a duration must not.

        On an NTP correction or a resume from sleep `time.time()` can jump back,
        which pushes `cutoff` further into the past than the window is wide.
        Nothing prunes until wall time catches up, so the horizon silently
        widens while the pill still reads its configured value.
        """
        import inspect
        from src.bidask import session as mod
        src = inspect.getsource(mod.SessionAccumulator.apply)
        self.assertIn("time.monotonic()", src)
        self.assertNotIn("time.time()", src)

    def test_a_zero_sign_is_not_booked_as_a_hit(self):
        """`record` is a named entry point now, not inline inside the guard."""
        st = TickerState(symbol="AAA")
        st.record(at=0.0, sign=0, signed_volume=900.0, uncertain=False, windowed=True)
        self.assertEqual((st.ask_hits, st.bid_hits, st.delta), (0, 0, 0.0))
        self.assertEqual(len(st.events), 0)

    def test_a_null_window_keeps_the_cumulative_behaviour(self):
        acc = SessionAccumulator(load_config({"hit_window_minutes": 0}))
        for i in range(50):
            acc.apply([row("AAA", 10.10, 10.08, 10.10, 1000 + i * 500)],
                      session_date="d", now=float(i * 10))
        self.assertEqual(acc.states["AAA"].total_hits, 49)


class TestWindowConfigGuard(unittest.TestCase):
    """The window reaches the state payload, serialized with allow_nan=False.

    A NaN there costs the whole document, not one field, and the page then
    reports the server as unreachable. A negative window would prune every
    observation the moment it was recorded and empty the board with no visible
    cause. Both raise instead.
    """

    def test_a_non_finite_window_raises(self):
        with self.assertRaises(ValueError) as caught:
            load_config({"hit_window_minutes": float("nan")})
        self.assertIn("hit_window_minutes", str(caught.exception))

    def test_a_negative_window_raises(self):
        with self.assertRaises(ValueError):
            load_config({"hit_window_minutes": -5})

    def test_a_boolean_window_raises(self):
        """`float(True)` is 1.0, so `hit_window_minutes: true` would silently
        become a one-minute horizon against a key documented as "0 disables"."""
        with self.assertRaises(ValueError) as caught:
            load_config({"hit_window_minutes": True})
        self.assertIn("boolean", str(caught.exception))

    def test_a_non_numeric_window_raises(self):
        with self.assertRaises(ValueError):
            load_config({"hit_window_minutes": "half an hour"})

    def test_zero_is_accepted_as_the_disable_switch(self):
        self.assertEqual(load_config({"hit_window_minutes": 0}).hit_window_minutes, 0.0)


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
