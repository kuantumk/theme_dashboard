"""The highlight ladder: one tier per ticker, and the first rung wins.

The order is coil, short interest, stacked moving averages, split stack. It is a
DISPLAY preference, not a ranking claim — nothing measures whether a coil
predicts better than a crowded short, or either better than a stacked average.

⛔ A tint names the highest rung whose input the caller HOLDS. It never names
the absence of a higher rung. A missing input skips its rung and the ladder
carries on, so a genuinely crowded short with no fundamentals row renders as
stacked on every session, not once. These tests pin that fall-through, because
the alternative reading — "stacked and not crowded" — is a claim the ladder
never makes.

⛔ Zero is missing, not a level. The snapshot builders call `.fillna(0)`, so an
absent `sma50` arrives as `0.0`. A price-scale figure of zero is impossible for
a real security, so the ladder reads it as absent. Without that rule
`ema20 > 0` reads as a stacked trend on a stock that has none. See
`docs/solutions/logic-errors/nan-defeats-numeric-guard-chains.md`.

⛔ The split rung asserts NO direction. The same reading covers a stock
reclaiming its short-term averages from below and a stock that has just lost its
50-day. These are opposite trades and the rung separates neither.
"""

import inspect
import unittest

from src.indicators.create_technical_indicators import (
    HIGHLIGHT_SHORT_FLOOR,
    compute_highlight_tier,
)

# EMA10 > EMA20 > SMA50: the stacked rung, used wherever a case needs the
# moving averages to be answerable but is testing something else.
STACKED = dict(ema10=12.0, ema20=11.0, sma50=10.0)
# EMA10 > EMA20, EMA20 under SMA50: the split rung.
SPLIT = dict(ema10=12.0, ema20=11.0, sma50=13.0)


def _tier(tight_base=False, short_interest=None,
          ema10=None, ema20=None, sma50=None):
    """Run the ladder with every input defaulting to missing."""
    return compute_highlight_tier(
        tight_base=tight_base,
        short_interest=short_interest,
        ema10=ema10,
        ema20=ema20,
        sma50=sma50,
    )


class LadderOrderTests(unittest.TestCase):
    """The first rung a ticker satisfies wins, and the rest never run."""

    def test_a_coil_outranks_a_crowded_short_and_a_stacked_average(self):
        # AE1. Every rung fires. Only the top one may come back.
        self.assertEqual(
            _tier(tight_base=True, short_interest=34.0, **STACKED), 'coil')

    def test_a_crowded_short_outranks_a_stacked_average(self):
        self.assertEqual(
            _tier(short_interest=34.0, **STACKED), 'short')

    def test_a_crowded_short_outranks_a_split_stack(self):
        self.assertEqual(
            _tier(short_interest=34.0, **SPLIT), 'short')

    def test_a_coil_alone_still_reads_coil(self):
        self.assertEqual(_tier(tight_base=True), 'coil')

    def test_no_rung_leaves_the_ticker_plain(self):
        self.assertIsNone(_tier())

    def test_the_ladder_returns_one_of_five_values(self):
        cases = [
            _tier(tight_base=True),
            _tier(short_interest=99.0),
            _tier(**STACKED),
            _tier(**SPLIT),
            _tier(),
        ]
        self.assertEqual(cases, ['coil', 'short', 'ma_up', 'ma_split', None])


class CoilRungTests(unittest.TestCase):
    """The coil rung reads the `tight_base` boolean and fails closed."""

    def test_a_false_flag_falls_through_to_the_next_rung(self):
        self.assertEqual(
            _tier(tight_base=False, short_interest=34.0), 'short')

    def test_a_missing_flag_falls_through(self):
        self.assertEqual(
            _tier(tight_base=None, short_interest=34.0), 'short')

    def test_a_nan_flag_is_not_a_coil(self):
        # bool(float('nan')) is True, so a bare truth test would tint every
        # ticker whose tightness columns are absent.
        self.assertIsNone(_tier(tight_base=float('nan')))


class ShortRungTests(unittest.TestCase):
    """Short interest of 20% of float or more. The level is chosen, not
    measured — the SI tab gates at 12% and the EP screener at 10%."""

    def test_exactly_the_floor_is_inside_the_rung(self):
        # AE2.
        self.assertEqual(_tier(short_interest=20.0), 'short')

    def test_just_under_the_floor_is_outside_it(self):
        self.assertIsNone(_tier(short_interest=19.9))

    def test_the_floor_is_twenty_percent(self):
        self.assertEqual(HIGHLIGHT_SHORT_FLOOR, 20.0)

    def test_a_number_written_as_text_still_counts(self):
        # Finviz values arrive as text on some paths.
        self.assertEqual(_tier(short_interest='34.0'), 'short')

    def test_text_that_is_not_a_number_reads_as_missing(self):
        self.assertEqual(_tier(short_interest='N/A', **STACKED), 'ma_up')


class MissingInputTests(unittest.TestCase):
    """R7: a rung whose input is missing is skipped, and the ladder carries on.
    A skipped rung never blocks a lower one."""

    def test_unknown_short_interest_falls_through_to_the_stacked_rung(self):
        # AE3. Nothing, rather than green, would hide the rung the tab holds.
        self.assertEqual(_tier(short_interest=None, **STACKED), 'ma_up')

    def test_a_nan_short_interest_falls_through(self):
        self.assertEqual(_tier(short_interest=float('nan'), **STACKED), 'ma_up')

    def test_a_missing_input_demotes_on_every_session(self):
        # AE8. The chip really carries 34% short interest. Its tab holds no
        # fundamentals row, so the short rung is unanswerable there. Green must
        # therefore read as "stacked, and no higher rung this tab holds" — never
        # as "stacked and not crowded".
        for _ in range(5):
            self.assertEqual(_tier(short_interest=None, **STACKED), 'ma_up')

    def test_a_zero_sma50_reads_as_missing_not_as_a_floor(self):
        # The value `.fillna(0)` produces. Every average sits above it, so a
        # bare comparison would report a stacked trend on a stock with none.
        self.assertIsNone(_tier(ema10=12.0, ema20=11.0, sma50=0.0))

    def test_a_zero_ema_reads_as_missing(self):
        self.assertIsNone(_tier(ema10=12.0, ema20=0.0, sma50=10.0))

    def test_a_negative_average_reads_as_missing(self):
        self.assertIsNone(_tier(ema10=12.0, ema20=11.0, sma50=-5.0))

    def test_one_missing_average_voids_both_moving_average_rungs(self):
        # The split rung reads SMA50 too, so an absent SMA50 answers neither.
        self.assertIsNone(_tier(ema10=12.0, ema20=11.0, sma50=None))

    def test_a_missing_average_does_not_block_a_higher_rung(self):
        self.assertEqual(_tier(short_interest=34.0, sma50=None), 'short')


class NonFiniteInputTests(unittest.TestCase):
    """An infinity or a NaN must return None, never raise."""

    def test_a_nan_ema10_returns_no_tier(self):
        self.assertIsNone(_tier(ema10=float('nan'), ema20=11.0, sma50=10.0))

    def test_an_infinite_ema10_returns_no_tier(self):
        self.assertIsNone(_tier(ema10=float('inf'), ema20=11.0, sma50=10.0))

    def test_a_nan_sma50_returns_no_tier(self):
        self.assertIsNone(_tier(ema10=12.0, ema20=11.0, sma50=float('nan')))

    def test_every_input_missing_returns_no_tier(self):
        self.assertIsNone(_tier(
            tight_base=None, short_interest=None,
            ema10=None, ema20=None, sma50=None))


class MovingAverageRungTests(unittest.TestCase):
    """EMA10 > EMA20 opens the two moving-average rungs. EMA20 against SMA50
    decides which one, and equality falls to split."""

    def test_stacked_averages_read_ma_up(self):
        self.assertEqual(_tier(ema10=12.0, ema20=11.0, sma50=10.0), 'ma_up')

    def test_sma50_above_ema20_reads_ma_split(self):
        self.assertEqual(_tier(ema10=12.0, ema20=11.0, sma50=13.0), 'ma_split')

    def test_sma50_equal_to_ema20_reads_ma_split(self):
        self.assertEqual(_tier(ema10=12.0, ema20=11.0, sma50=11.0), 'ma_split')

    def test_ema10_below_ema20_earns_nothing_whatever_sma50_reads(self):
        # AE4.
        for sma50 in (1.0, 9.0, 10.5, 11.0, 50.0):
            self.assertIsNone(
                _tier(ema10=10.0, ema20=11.0, sma50=sma50), f'{sma50=}')

    def test_ema10_equal_to_ema20_earns_nothing_whatever_sma50_reads(self):
        for sma50 in (1.0, 9.0, 11.0, 50.0):
            self.assertIsNone(
                _tier(ema10=11.0, ema20=11.0, sma50=sma50), f'{sma50=}')


class RequestEquivalenceTests(unittest.TestCase):
    """KTD4. The request stated the split rung as two clauses:

        EMA10 > EMA20 AND (EMA20 < SMA50 OR EMA10 < SMA50)

    The second clause is redundant. With EMA10 > EMA20 already true,
    `EMA10 < SMA50` forces `EMA20 < SMA50`, so the disjunction reduces to its
    first clause. The shipped rung carries the reduced form.

    This test writes the request's wording out literally and compares the two
    predicates over a grid. Restating the shipped predicate here would compare
    the rung against itself and prove nothing.
    """

    VALUES = (9.0, 10.0, 11.0)

    @staticmethod
    def _request_clause(ema10, ema20, sma50):
        """The request's original two-clause wording, written out as stated."""
        return ema10 > ema20 and (ema20 < sma50 or ema10 < sma50)

    def _grid(self):
        for ema10 in self.VALUES:
            for ema20 in self.VALUES:
                for sma50 in self.VALUES:
                    yield ema10, ema20, sma50

    def test_the_shipped_rung_matches_the_request_on_every_strict_ordering(self):
        checked = 0
        for ema10, ema20, sma50 in self._grid():
            if ema20 == sma50:
                continue  # settled by KTD4, checked below
            shipped = _tier(ema10=ema10, ema20=ema20, sma50=sma50) == 'ma_split'
            self.assertEqual(
                shipped, self._request_clause(ema10, ema20, sma50),
                f'{ema10=} {ema20=} {sma50=}')
            checked += 1
        self.assertEqual(checked, 18)

    def test_the_grid_exercises_both_answers(self):
        # Guards against an equivalence that holds because nothing ever fires.
        answers = {
            self._request_clause(*case)
            for case in self._grid() if case[1] != case[2]
        }
        self.assertEqual(answers, {True, False})

    def test_equality_is_the_only_case_the_two_forms_read_differently(self):
        # KTD4 settles EMA20 == SMA50 to split. The request's wording leaves it
        # with no tier at all, so the one disagreement is deliberate and named.
        differing = [
            case for case in self._grid()
            if (_tier(ema10=case[0], ema20=case[1], sma50=case[2]) == 'ma_split')
            != self._request_clause(*case)
        ]
        self.assertEqual(differing, [(10.0, 9.0, 9.0), (11.0, 9.0, 9.0),
                                     (11.0, 10.0, 10.0)])


class SplitRungWordingTests(unittest.TestCase):
    """R5. No wording in the code or its docstring may give the split rung a
    direction. The rung covers a reclaim from below and a lost 50-day alike."""

    DIRECTIONAL = (
        'reclaim', 'recover', 'bullish', 'bearish', 'breakdown', 'breakout',
        'turning', 'rebound', 'reversal', 'weakening', 'strengthening',
    )

    def test_the_source_names_no_direction(self):
        src = inspect.getsource(compute_highlight_tier).lower()
        for word in self.DIRECTIONAL:
            self.assertNotIn(
                word, src,
                f'{word!r} gives the split rung a direction it does not have. '
                'The same reading covers two opposite trades.')


if __name__ == '__main__':
    unittest.main()
