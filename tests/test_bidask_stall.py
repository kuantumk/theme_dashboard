"""Tests for the frozen-numerator watch.

⛔ The failure this module exists for is not an empty column — it is a full
one. `RvolGate.source_unavailable` fires only when every reading is zero, so a
vendor column that keeps returning plausible numbers and simply stops
advancing passes every check the board already has.

That case is not hypothetical for this vendor. Two instances are already
measured and recorded: `volume` holds the PREVIOUS completed session before the
bell (median 2,213,074 against a `premarket_volume` median of 6,448), which is
the whole reason `universe.VOLUME_FIELDS` is a table rather than one column;
and `relative_volume_intraday|5` stayed non-null on 2,805 of 2,805 rows after
the close while changing for 0 of 250 symbols across a 240-second gap. Both
were found by probing. This is the same probe, moved into the running server.

⛔ A frozen numerator does not hold the board still — it DRAINS it.
`rvol_at_time.baseline_at` reads the baseline curve at the current minute, so
the denominator keeps growing with the clock while the numerator does not.
Every reading therefore falls, names slip back under the floor one at a time,
and the columns empty across the morning. Without this watch the page reaches
`emptyReason`'s last branch and prints "the source is working and the market is
quiet" — the one claim this board was built never to make.

The detector is DIAGNOSTIC ONLY. It must never change a score, a side, or an
admission. It changes what the page says.
"""

import unittest

from src.bidask.rvol_at_time import CRYPTO
from src.bidask.session_state import MARKET, PRE_MARKET
from src.bidask.stall import StallWatch


def volumes(count, *, base=1000.0, start=0):
    """`count` symbols carrying a positive numerator."""
    return {f"S{i}": base + i for i in range(start, start + count)}


class TestAMovingSourceIsNeverAccused(unittest.TestCase):
    """Every clause here rules out an honest reading of a still column."""

    def test_a_source_that_keeps_advancing_never_stalls(self):
        watch = StallWatch()
        reading = None
        for tick in range(6):
            reading = watch.observe("equity", MARKET,
                                    volumes(200, base=1000.0 + 50 * tick),
                                    now=tick * 130.0)
        self.assertFalse(reading.stalled)
        self.assertEqual(reading.seconds, 0.0)

    def test_one_symbol_moving_is_enough_to_clear_the_watch(self):
        """A quiet tape is not a dead one. Any print proves the column is live."""
        watch = StallWatch()
        frozen = volumes(200)
        watch.observe("equity", MARKET, frozen, now=0.0)
        moved = dict(frozen)
        moved["S7"] = frozen["S7"] + 1.0
        first = watch.observe("equity", MARKET, moved, now=130.0)
        second = watch.observe("equity", MARKET, moved, now=260.0)
        self.assertFalse(first.stalled)
        self.assertEqual(first.moved, 1)
        # The second window is genuinely still, but it is the FIRST strike
        # after movement, so it must not convict on its own.
        self.assertFalse(second.stalled)

    def test_readings_inside_the_window_are_never_compared(self):
        """⛔ The poll cadence is 10s; thin pre-market can be still that long.

        Comparing consecutive polls would accuse the vendor every quiet minute
        before 05:00. The watch only judges readings a full window apart.
        """
        watch = StallWatch(window_seconds=120.0)
        frozen = volumes(200)
        readings = [watch.observe("equity", PRE_MARKET, frozen, now=t * 10.0)
                    for t in range(12)]
        self.assertFalse(any(r.stalled for r in readings))
        self.assertTrue(all(r.watched == 0 for r in readings[1:]),
                        "a reading inside the window is not a comparison")

    def test_too_few_active_symbols_cannot_convict(self):
        """04:02 with three names trading is not evidence of a dead column."""
        watch = StallWatch(min_active=25)
        frozen = volumes(3)
        readings = [watch.observe("equity", PRE_MARKET, frozen, now=t * 130.0)
                    for t in range(5)]
        self.assertFalse(any(r.stalled for r in readings))

    def test_symbols_with_no_volume_are_not_evidence(self):
        """A universe that has not traded yet proves nothing about the feed.

        Zero equals zero on every comparison, so counting untraded names would
        convict the vendor of the market being shut.
        """
        watch = StallWatch(min_active=25)
        silent = {f"S{i}": 0.0 for i in range(500)}
        readings = [watch.observe("equity", PRE_MARKET, silent, now=t * 130.0)
                    for t in range(5)]
        self.assertFalse(any(r.stalled for r in readings))
        self.assertEqual(readings[-1].watched, 0)

    def test_a_state_change_drops_the_reference(self):
        """⛔ Pre-market reads `premarket_volume`; the session reads `volume`.

        Those are different quantities, so a comparison across the boundary
        measures the column swap rather than the feed. The reference has to be
        abandoned, not carried over.
        """
        watch = StallWatch()
        frozen = volumes(200)
        watch.observe("equity", PRE_MARKET, frozen, now=0.0)
        watch.observe("equity", PRE_MARKET, frozen, now=130.0)
        crossing = watch.observe("equity", MARKET, frozen, now=260.0)
        self.assertEqual(crossing.watched, 0,
                         "the pre-market reference must not survive the open")
        self.assertFalse(crossing.stalled)

    def test_each_market_is_watched_separately(self):
        """A frozen equity column says nothing about the crypto tab."""
        watch = StallWatch()
        frozen = volumes(200)
        for tick in range(4):
            watch.observe("equity", MARKET, frozen, now=tick * 130.0)
            watch.observe("crypto", CRYPTO, volumes(200, base=1000.0 + 50 * tick),
                          now=tick * 130.0)
        self.assertTrue(watch.observe("equity", MARKET, frozen, now=520.0).stalled)
        self.assertFalse(watch.observe("crypto", CRYPTO, volumes(200, base=9000.0),
                                       now=520.0).stalled)


class TestAFrozenColumnIsConvicted(unittest.TestCase):
    """The measured signature: many active names, zero movement, minutes apart."""

    def test_two_still_windows_report_a_stall(self):
        watch = StallWatch(window_seconds=120.0, strikes=2)
        frozen = volumes(250)
        first = watch.observe("equity", MARKET, frozen, now=0.0)
        second = watch.observe("equity", MARKET, frozen, now=130.0)
        third = watch.observe("equity", MARKET, frozen, now=260.0)
        self.assertFalse(first.stalled, "the first reading sets the reference")
        self.assertFalse(second.stalled, "one still window is not yet a verdict")
        self.assertTrue(third.stalled)
        self.assertEqual(third.moved, 0)
        self.assertEqual(third.watched, 250)

    def test_the_verdict_reports_how_long_the_column_has_been_still(self):
        watch = StallWatch(window_seconds=120.0, strikes=2)
        frozen = volumes(250)
        watch.observe("equity", MARKET, frozen, now=1000.0)
        watch.observe("equity", MARKET, frozen, now=1130.0)
        verdict = watch.observe("equity", MARKET, frozen, now=1260.0)
        self.assertTrue(verdict.stalled)
        self.assertEqual(verdict.seconds, 260.0,
                         "the age is measured from the last observed movement")

    def test_movement_after_a_stall_clears_it(self):
        """A recovered feed must stop accusing the vendor on the next window."""
        watch = StallWatch(window_seconds=120.0, strikes=2)
        frozen = volumes(250)
        for tick in range(3):
            watch.observe("equity", MARKET, frozen, now=tick * 130.0)
        recovered = watch.observe("equity", MARKET, volumes(250, base=2000.0),
                                  now=390.0)
        self.assertFalse(recovered.stalled)
        self.assertEqual(recovered.seconds, 0.0)

    def test_a_shifting_roster_still_convicts_on_the_overlap(self):
        """The universe changes between polls; the comparison must survive it.

        Comparing whole-universe sums would read a roster change as movement
        and miss the freeze. Only symbols present in both readings are judged.
        """
        watch = StallWatch(window_seconds=120.0, strikes=2)
        for tick in range(3):
            frame = volumes(250, start=tick)  # two names churn every poll
            reading = watch.observe("equity", MARKET, frame, now=tick * 130.0)
        self.assertTrue(reading.stalled)
        self.assertGreaterEqual(reading.watched, 240)


class TestAShutBoardCannotConvictTheVendor(unittest.TestCase):
    """⛔ A closed market's volume column IS frozen, and correctly so.

    Nothing trades, so nothing advances, and a naive watch would accuse the
    vendor every night. The protection is indirect and worth pinning because it
    is easy to break by accident: `universe.VOLUME_FIELDS` has no entry for a
    closed board, so `volume_since_anchor` returns zero for every row, every
    reading is dropped as untraded, and no comparison ever happens.

    Adding a `closed` entry to that table to "fill a gap" would therefore hand
    the watch a universe of frozen numbers to convict on. The same table that
    decides the numerator decides that a shut board is not evidence.
    """

    def test_the_closed_state_has_no_numerator(self):
        from src.bidask.session_state import CLOSED
        from src.bidask.universe import VOLUME_FIELDS
        self.assertNotIn(CLOSED, VOLUME_FIELDS,
                         "a closed board would give the stall watch frozen "
                         "numbers to convict the vendor on every night")

    def test_a_universe_with_no_numerator_never_stalls(self):
        import pandas as pd

        from src.bidask.session_state import CLOSED
        from src.bidask.universe import volume_since_anchor

        frame = pd.DataFrame([{"symbol": f"S{i}", "volume": 5_000_000.0,
                               "premarket_volume": 9_000.0}
                              for i in range(300)])
        readings = volume_since_anchor(frame, CLOSED)
        watch = StallWatch()
        verdicts = [watch.observe("equity", CLOSED,
                                  dict(zip(frame["symbol"], readings)),
                                  now=tick * 130.0)
                    for tick in range(5)]
        self.assertFalse(any(v.stalled for v in verdicts))


class TestThePageNamesTheStall(unittest.TestCase):
    """The verdict is worthless unless it displaces the sentence it refutes.

    ⛔ `emptyReason`'s last branch says "the source is working and the market
    is quiet". A frozen column reaches exactly that branch: every row scores,
    so `unavailable` is false, and every reading falls under the floor as the
    denominator advances. The stall test must be read BEFORE it, or the page
    keeps printing the one claim this board exists to avoid.

    There is no JavaScript test runner in this repo, so these pin the source.
    Weaker than executing the branch, and better than nothing pinning the join
    between `rvol_status` and the page that reads it.
    """

    @classmethod
    def setUpClass(cls):
        from pathlib import Path

        from src.bidask import server
        web = Path(server.__file__).parent / "web"
        cls.app = (web / "app.js").read_text(encoding="utf-8")
        cls.html = (web / "index.html").read_text(encoding="utf-8")
        cls.css = (web / "style.css").read_text(encoding="utf-8")

    def test_the_stall_branch_precedes_the_quiet_market_sentence(self):
        quiet = self.app.index("the market is quiet")
        stalled = self.app.index("r.stalled")
        self.assertLess(stalled, quiet,
                        "a frozen column would still be called a quiet market")

    def test_the_empty_column_explains_a_frozen_source(self):
        self.assertIn("stopped advancing", self.app,
                      "the reader is told the column froze, not that it is empty")

    def test_the_pill_marks_a_stall_even_while_rows_are_still_showing(self):
        """The usual case is a FULL board going stale, not an empty one.

        `emptyReason` only renders when a column has nothing in it. A source
        that froze two minutes ago still has rows on screen, so the pill is the
        only thing that can say so.
        """
        pill = self.app[self.app.index("els.coverage.textContent"):
                        self.app.index("els.floor.textContent")]
        self.assertIn("stalled", pill,
                      "the coverage pill must report the stall on its own")

    def test_the_stall_pill_has_somewhere_to_render(self):
        self.assertIn('id="coverage-pill"', self.html)
        self.assertIn(".pill.error", self.css)


if __name__ == "__main__":
    unittest.main()
