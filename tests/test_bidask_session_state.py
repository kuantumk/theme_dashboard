"""Session state and reference-price tests.

The board asks two questions of every row, and they are INDEPENDENT: is this
ticker above a reference the current session uses, and is it below one? A stock
that gapped down and is being bid up off its open answers yes to both, and the
board shows it on both sides (R5). Several cases below exist only to keep that
true — an `if strong ... elif weak` implementation passes every single-sided
case and fails exactly those.

The second theme is fail-closed resolution. An unmapped `current_session`, a
null change field, and a missing column each yield no side rather than a
defaulted one, because a state carries both a reference price and a volume
floor: a wrong default applies the wrong pair of rules with nothing on screen
to say so (KTD7).
"""

import unittest

import numpy as np
import pandas as pd

from src.bidask.feed import CRYPTO_COLUMNS, EQUITY_COLUMNS
from src.bidask.session_state import (
    CLOSED,
    CRYPTO,
    MARKET,
    POST_MARKET,
    PRE_MARKET,
    REF_24H,
    REF_OPEN,
    REF_PREV_CLOSE,
    REF_SESSION_CLOSE,
    REFERENCE_FIELDS,
    SESSIONS,
    resolve_session,
    resolve_state,
    sides_for,
)


def row(**fields):
    """One feed row. Absent keys stand for columns the vendor did not send."""
    return dict(fields)


class TestResolveState(unittest.TestCase):
    """`current_session` is the only input; the local clock is never consulted."""

    def test_regular_session_spellings(self):
        # `market` is what the feed actually sends; `regular` was assumed for
        # months and never observed. Both map, so neither costs a dark board.
        self.assertEqual(resolve_state("market"), MARKET)
        self.assertEqual(resolve_state("regular"), MARKET)

    def test_extended_hours_spellings(self):
        self.assertEqual(resolve_state("pre_market"), PRE_MARKET)
        self.assertEqual(resolve_state("premarket"), PRE_MARKET)
        self.assertEqual(resolve_state("post_market"), POST_MARKET)
        self.assertEqual(resolve_state("postmarket"), POST_MARKET)

    def test_closed_values(self):
        self.assertEqual(resolve_state("out_of_session"), CLOSED)
        self.assertEqual(resolve_state("holiday"), CLOSED)

    def test_unmapped_value_is_closed_not_open(self):
        # KTD7: a new state selects a reference price AND a volume floor, so a
        # fall-through to "open" would silently apply the wrong pair of rules.
        self.assertEqual(resolve_state("auction"), CLOSED)
        self.assertEqual(resolve_state("half_day_close"), CLOSED)

    def test_extended_is_recognised_but_still_closed(self):
        # The value has a banner label yet carries no reference/floor pair,
        # and it was not observed in the after-hours probe. Closed until one is
        # assigned, rather than borrowed from a neighbouring state.
        self.assertEqual(resolve_session("extended"), (CLOSED, "extended hours"))

    def test_case_and_whitespace_are_normalised(self):
        self.assertEqual(resolve_state(" Market "), MARKET)
        self.assertEqual(resolve_state("PRE_MARKET"), PRE_MARKET)

    def test_absent_reading_is_closed(self):
        # A zero-row response has no session to read; pandas yields NaN for a
        # null cell, and every comparison against NaN is False.
        self.assertEqual(resolve_state(None), CLOSED)
        self.assertEqual(resolve_state(""), CLOSED)
        self.assertEqual(resolve_state(float("nan")), CLOSED)

    def test_an_unknown_value_is_labelled_readably_and_still_closed(self):
        # The banner may name a state the board has no rules for; the board
        # never applies any to it.
        self.assertEqual(resolve_session("half_day_close"), (CLOSED, "half day close"))

    def test_crypto_is_never_a_value_the_feed_can_send(self):
        # `fetch_crypto` stamps it; no `current_session` reading may reach it,
        # or an equity response could be judged against the 24-hour reference.
        self.assertNotIn(CRYPTO, {state for state, _ in SESSIONS.values()})
        self.assertEqual(resolve_state("crypto"), CLOSED)


class TestRegularSession(unittest.TestCase):
    """Two references, either of which can place a ticker on either side (R2)."""

    def test_above_open_and_above_previous_close_is_strong_only(self):
        sides = sides_for(row(change_from_open=1.4, change=2.6), MARKET)
        self.assertTrue(sides.strong)
        self.assertFalse(sides.weak)
        self.assertEqual(sides.strong, (REF_OPEN, REF_PREV_CLOSE))
        self.assertEqual(sides.weak, ())

    def test_below_open_and_below_previous_close_is_weak_only(self):
        sides = sides_for(row(change_from_open=-1.4, change=-2.6), MARKET)
        self.assertTrue(sides.weak)
        self.assertFalse(sides.strong)
        self.assertEqual(sides.weak, (REF_OPEN, REF_PREV_CLOSE))
        self.assertEqual(sides.strong, ())

    def test_gapped_down_and_recovering_earns_both_sides(self):
        # AE1: closed at $10, opened at $9, now $9.50. Strong against the open,
        # weak against yesterday close, and both readings are true.
        sides = sides_for(row(change_from_open=5.6, change=-5.0), MARKET)
        self.assertTrue(sides.strong)
        self.assertTrue(sides.weak)
        self.assertEqual(sides.strong, (REF_OPEN,))
        self.assertEqual(sides.weak, (REF_PREV_CLOSE,))

    def test_gapped_up_and_fading_earns_both_sides(self):
        sides = sides_for(row(change_from_open=-3.1, change=4.2), MARKET)
        self.assertEqual(sides.strong, (REF_PREV_CLOSE,))
        self.assertEqual(sides.weak, (REF_OPEN,))

    def test_exactly_flat_earns_no_side(self):
        # A zero move is not a direction. Both tests are strict.
        sides = sides_for(row(change_from_open=0.0, change=0.0), MARKET)
        self.assertFalse(sides.strong)
        self.assertFalse(sides.weak)
        self.assertEqual((sides.strong, sides.weak), ((), ()))

    def test_flat_against_one_reference_still_reads_the_other(self):
        sides = sides_for(row(change_from_open=0.0, change=1.9), MARKET)
        self.assertEqual(sides.strong, (REF_PREV_CLOSE,))
        self.assertEqual(sides.weak, ())

    def test_one_null_reference_does_not_silence_the_other(self):
        # The guard is per field, not per row: a vendor dropping one column
        # must not cost the reading the other column still supports.
        sides = sides_for(row(change_from_open=None, change=-2.2), MARKET)
        self.assertEqual(sides.weak, (REF_PREV_CLOSE,))
        self.assertEqual(sides.strong, ())

    def test_missing_column_raises_nothing(self):
        sides = sides_for(row(change=3.3), MARKET)
        self.assertEqual(sides.strong, (REF_PREV_CLOSE,))


class TestPreMarket(unittest.TestCase):
    """Only the previous close is consulted (R3)."""

    def test_above_previous_close_is_strong(self):
        sides = sides_for(row(premarket_change=6.8), PRE_MARKET)
        self.assertEqual(sides.strong, (REF_PREV_CLOSE,))
        self.assertEqual(sides.weak, ())

    def test_below_previous_close_is_weak(self):
        sides = sides_for(row(premarket_change=-6.8), PRE_MARKET)
        self.assertEqual(sides.weak, (REF_PREV_CLOSE,))
        self.assertEqual(sides.strong, ())

    def test_session_open_is_ignored_even_when_present(self):
        # The row carries yesterday `change_from_open` and `change` before the
        # bell. Reading either would date the board to the wrong session.
        sides = sides_for(
            row(premarket_change=4.0, change_from_open=-9.9, change=-9.9),
            PRE_MARKET,
        )
        self.assertEqual(sides.strong, (REF_PREV_CLOSE,))
        self.assertEqual(sides.weak, ())

    def test_flat_pre_market_earns_no_side(self):
        sides = sides_for(row(premarket_change=0.0), PRE_MARKET)
        self.assertEqual((sides.strong, sides.weak), ((), ()))


class TestAfterHours(unittest.TestCase):
    """Only the regular session close is consulted (R4)."""

    def test_above_the_session_close_is_strong(self):
        sides = sides_for(row(postmarket_change=2.4), POST_MARKET)
        self.assertEqual(sides.strong, (REF_SESSION_CLOSE,))
        self.assertEqual(sides.weak, ())

    def test_below_the_session_close_is_weak(self):
        # `change` still reads the gain against yesterday; an earnings miss
        # sold after the bell is weak on the reference that now matters.
        sides = sides_for(row(postmarket_change=-7.5, change=3.1), POST_MARKET)
        self.assertEqual(sides.weak, (REF_SESSION_CLOSE,))
        self.assertEqual(sides.strong, ())

    def test_flat_after_hours_earns_no_side(self):
        sides = sides_for(row(postmarket_change=0.0), POST_MARKET)
        self.assertEqual((sides.strong, sides.weak), ((), ()))


class TestCrypto(unittest.TestCase):
    """R15. One rolling 24-hour reference, labelled so it does not read as a
    session. The same `sides_for` path the equity states take."""

    def test_up_over_24_hours_is_strong_against_the_24_hour_reference(self):
        sides = sides_for(row(change_pct=3.4), CRYPTO)
        self.assertEqual((sides.strong, sides.weak), ((REF_24H,), ()))

    def test_down_over_24_hours_is_weak(self):
        sides = sides_for(row(change_pct=-3.4), CRYPTO)
        self.assertEqual((sides.strong, sides.weak), ((), (REF_24H,)))

    def test_the_equity_change_fields_are_ignored(self):
        sides = sides_for(row(change_pct=-1.0, change=9.0, change_from_open=9.0),
                          CRYPTO)
        self.assertEqual((sides.strong, sides.weak), ((), (REF_24H,)))

    def test_the_reference_is_labelled_as_24_hour_not_as_a_session(self):
        self.assertIn("24h", REF_24H)
        self.assertNotIn(REF_24H, (REF_OPEN, REF_PREV_CLOSE, REF_SESSION_CLOSE))

    def test_an_unreadable_flat_or_missing_change_earns_no_side(self):
        # A dead `24h_close_change|5` must empty the board, not fill a column.
        for value in (None, float("nan"), "n/a", 0.0):
            sides = sides_for(row(change_pct=value), CRYPTO)
            self.assertEqual((sides.strong, sides.weak), ((), ()),
                             f"{value!r} earned a side")
        sides = sides_for(row(), CRYPTO)
        self.assertEqual((sides.strong, sides.weak), ((), ()))


class TestClosedBoard(unittest.TestCase):
    """No state, no reference, no side — however far the price has moved."""

    def test_closed_state_earns_no_side(self):
        moving = row(change_from_open=8.0, change=8.0,
                     premarket_change=8.0, postmarket_change=8.0)
        sides = sides_for(moving, CLOSED)
        self.assertFalse(sides.strong)
        self.assertFalse(sides.weak)

    def test_out_of_session_and_holiday_resolve_to_a_closed_board(self):
        for raw in ("out_of_session", "holiday", "some_new_value"):
            with self.subTest(raw=raw):
                self.assertEqual(resolve_state(raw), CLOSED)
                sides = sides_for(row(change=5.0), resolve_state(raw))
                self.assertEqual((sides.strong, sides.weak), ((), ()))


class TestNonFiniteReferences(unittest.TestCase):
    """An unusable reading yields no side rather than a defaulted one."""

    def test_nan_and_infinity_earn_no_side(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                sides = sides_for(row(change_from_open=value, change=value), MARKET)
                self.assertEqual((sides.strong, sides.weak), ((), ()))

    def test_unparseable_value_earns_no_side(self):
        sides = sides_for(row(premarket_change="n/a"), PRE_MARKET)
        self.assertEqual((sides.strong, sides.weak), ((), ()))

    def test_pandas_null_cell_earns_no_side(self):
        # The real caller passes a DataFrame row, where a null cell arrives as
        # NaN rather than None and every comparison against it is False. This
        # is the shape that let an unquoted row reach the retired classifier as
        # a confident observation.
        frame = pd.DataFrame([
            {"symbol": "AAA", "change_from_open": np.nan, "change": np.nan},
            {"symbol": "BBB", "change_from_open": 1.0, "change": np.nan},
        ])
        blank = sides_for(frame.iloc[0], MARKET)
        self.assertEqual((blank.strong, blank.weak), ((), ()))
        partial = sides_for(frame.iloc[1], MARKET)
        self.assertEqual(partial.strong, (REF_OPEN,))
        self.assertEqual(partial.weak, ())


class TestFeedColumns(unittest.TestCase):
    """The select list and the reference table have to agree."""

    def test_every_reference_field_is_selected(self):
        # A reference reading a column the feed never requests is null on every
        # row, which reads as a flat market rather than as a missing field.
        for state, pairs in REFERENCE_FIELDS.items():
            if state == CRYPTO:
                continue
            for name, _ in pairs:
                with self.subTest(state=state, field=name):
                    self.assertIn(name, EQUITY_COLUMNS)

    def test_the_crypto_reference_is_selected_under_its_vendor_name(self):
        # `fetch_crypto` renames `24h_close_change|5` to `change_pct`.
        self.assertEqual(REFERENCE_FIELDS[CRYPTO], (("change_pct", REF_24H),))
        self.assertIn("24h_close_change|5", CRYPTO_COLUMNS)

    def test_the_volume_numerators_are_selected(self):
        for name in ("volume", "premarket_volume", "current_session"):
            with self.subTest(field=name):
                self.assertIn(name, EQUITY_COLUMNS)

    def test_no_list_asks_for_a_book(self):
        # The america scanner publishes no quote field: selecting one returns
        # null for every row, which is indistinguishable from a dead feed. The
        # crypto scanner does publish one, but nothing on the board reads it.
        for columns in (EQUITY_COLUMNS, CRYPTO_COLUMNS):
            self.assertNotIn("bid", columns)
            self.assertNotIn("ask", columns)


if __name__ == "__main__":
    unittest.main()
