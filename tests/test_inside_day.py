"""Inside-day definition: candle engulfed OR body engulfed.

The old rule was strict range containment (`high < prev_high and low > prev_low`).
It rejected two shapes that are exactly the coiled setups the green day-pattern
colouring exists to surface: a bar that *ties* the prior high or low, and a
tight-bodied bar whose wicks poke outside the prior range. The rule now reads:

    (high <= prev_high and low >= prev_low)
      or (body_top <= prev_body_top and body_bottom >= prev_body_bottom)

where body_top/bottom are max/min of (open, close) — direction-agnostic, so a red
previous bar compares the same as a green one.

One helper serves both computation sites. The definition previously existed twice
— vectorized in the indicator pipeline and scalar in the dashboard's standalone
ETF recompute — and the two were one edit away from disagreeing.
"""

import unittest

import pandas as pd

from src.indicators.create_technical_indicators import compute_inside_day

# Previous bar for every single-step case: green, body 100..105, range 90..110.
PREV_GREEN = {'open': 100.0, 'high': 110.0, 'low': 90.0, 'close': 105.0}
# Same body and range, drawn red. body_top/bottom must not care which.
PREV_RED = {'open': 105.0, 'high': 110.0, 'low': 90.0, 'close': 100.0}


def _verdict(prev, cur):
    """Run the helper over a two-bar frame and return the current bar's verdict."""
    df = pd.DataFrame([prev, cur])
    flags = compute_inside_day(df['open'], df['high'], df['low'], df['close'])
    return bool(flags.iloc[-1])


class RangeEngulfTests(unittest.TestCase):
    def test_strictly_inside_range(self):
        # The old definition's only true case still holds.
        self.assertTrue(_verdict(
            PREV_GREEN, {'open': 101.0, 'high': 108.0, 'low': 95.0, 'close': 103.0}))

    def test_high_ties_previous_high(self):
        # The tie the strict `<` rejected.
        self.assertTrue(_verdict(
            PREV_GREEN, {'open': 101.0, 'high': 110.0, 'low': 95.0, 'close': 103.0}))

    def test_low_ties_previous_low(self):
        self.assertTrue(_verdict(
            PREV_GREEN, {'open': 101.0, 'high': 108.0, 'low': 90.0, 'close': 103.0}))

    def test_both_extremes_tie(self):
        self.assertTrue(_verdict(
            PREV_GREEN, {'open': 101.0, 'high': 110.0, 'low': 90.0, 'close': 103.0}))

    def test_high_above_previous_high_fails_the_range_clause(self):
        # Body also breaks out, so nothing rescues it.
        self.assertFalse(_verdict(
            PREV_GREEN, {'open': 106.0, 'high': 112.0, 'low': 95.0, 'close': 111.0}))

    def test_low_below_previous_low_fails_the_range_clause(self):
        self.assertFalse(_verdict(
            PREV_GREEN, {'open': 99.0, 'high': 108.0, 'low': 88.0, 'close': 94.0}))


class BodyEngulfTests(unittest.TestCase):
    # Range breaks out on BOTH sides (85..115 vs 90..110); only the body saves it.
    WICKY_INSIDE_BODY = {'open': 101.0, 'high': 115.0, 'low': 85.0, 'close': 104.0}

    def test_body_inside_previous_body_after_a_green_bar(self):
        self.assertTrue(_verdict(PREV_GREEN, self.WICKY_INSIDE_BODY))

    def test_body_inside_previous_body_after_a_red_bar(self):
        # Same body extremes, opposite direction. max/min normalization must make
        # these identical -- comparing open-to-open would flip this case.
        self.assertTrue(_verdict(PREV_RED, self.WICKY_INSIDE_BODY))

    def test_body_exactly_matching_previous_body(self):
        self.assertTrue(_verdict(
            PREV_GREEN, {'open': 100.0, 'high': 115.0, 'low': 85.0, 'close': 105.0}))

    def test_body_wider_than_previous_body_fails(self):
        self.assertFalse(_verdict(
            PREV_GREEN, {'open': 99.0, 'high': 115.0, 'low': 85.0, 'close': 108.0}))

    def test_body_shifted_above_previous_body_fails(self):
        # Body 106..109 sits entirely above 100..105: contained in neither clause.
        self.assertFalse(_verdict(
            PREV_GREEN, {'open': 106.0, 'high': 115.0, 'low': 85.0, 'close': 109.0}))


class SeriesShapeTests(unittest.TestCase):
    def test_first_bar_is_never_an_inside_day(self):
        df = pd.DataFrame([PREV_GREEN, PREV_GREEN])
        flags = compute_inside_day(df['open'], df['high'], df['low'], df['close'])
        self.assertFalse(bool(flags.iloc[0]))

    def test_returns_a_boolean_series_aligned_to_the_input(self):
        df = pd.DataFrame([PREV_GREEN] * 4)
        flags = compute_inside_day(df['open'], df['high'], df['low'], df['close'])
        self.assertEqual(flags.dtype, bool)
        self.assertEqual(len(flags), 4)
        self.assertTrue(flags.index.equals(df.index))

    def test_is_strictly_looser_than_the_old_definition(self):
        # Whatever the old rule accepted, the new rule must still accept. If this
        # ever fails, the widening silently dropped a setup it used to colour.
        # Each bar is judged against the one before it, so these are written as a
        # walk: tie -> strictly inside -> body-only -> neither.
        rows = [
            PREV_GREEN,                                                    # h110 l90  body 100..105
            {'open': 101.0, 'high': 110.0, 'low': 90.0, 'close': 103.0},   # ties both: new only
            {'open': 102.0, 'high': 108.0, 'low': 95.0, 'close': 104.0},   # strictly inside: both
            {'open': 102.5, 'high': 115.0, 'low': 85.0, 'close': 103.5},   # body only: new only
            {'open': 106.0, 'high': 120.0, 'low': 95.0, 'close': 119.0},   # neither
        ]
        df = pd.DataFrame(rows)
        old = (df['high'] < df['high'].shift(1)) & (df['low'] > df['low'].shift(1))
        new = compute_inside_day(df['open'], df['high'], df['low'], df['close'])
        self.assertTrue(bool((new | ~old).all()))
        self.assertTrue(bool((new & ~old).any()))  # and genuinely looser


class PartialQuoteTests(unittest.TestCase):
    """A missing open must read as unknown, not as a zero-width body.

    The pipeline drops whole rows with any NaN, but the dashboard's ETF path
    only drops rows missing Close, so a row with a valid close and a missing
    open does reach the helper. pandas' max/min default to skipna=True, which
    would silently treat such a row as a single-point body at close and hand
    back a confident verdict computed from half a quote.
    """

    def test_missing_open_is_not_an_inside_day(self):
        # Body would be a zero-width point at close=103 under skipna=True, which
        # sits inside the previous body (100..105) and would wrongly flag True.
        self.assertFalse(_verdict(
            PREV_GREEN,
            {'open': float('nan'), 'high': 115.0, 'low': 85.0, 'close': 103.0}))

    def test_missing_open_does_not_contaminate_the_following_bar(self):
        # shift(1) carries the previous bar's body into the next comparison, so
        # a fabricated body would corrupt the bar after it too.
        df = pd.DataFrame([
            PREV_GREEN,
            {'open': float('nan'), 'high': 115.0, 'low': 85.0, 'close': 103.0},
            {'open': 102.0, 'high': 116.0, 'low': 84.0, 'close': 104.0},
        ])
        flags = compute_inside_day(df['open'], df['high'], df['low'], df['close'])
        self.assertFalse(bool(flags.iloc[1]))
        self.assertFalse(bool(flags.iloc[2]))

    def test_missing_close_still_allows_the_range_clause(self):
        # The range clause reads only high/low, which are present here. An
        # unknown body does not invalidate an unambiguously engulfed range.
        self.assertTrue(_verdict(
            PREV_GREEN,
            {'open': 102.0, 'high': 108.0, 'low': 95.0, 'close': float('nan')}))

    def test_missing_close_cannot_carry_the_body_clause(self):
        # Range breaks out both sides, so only the body could qualify it. Under
        # skipna=True the body would collapse to a point at open=102, sitting
        # inside 100..105, and wrongly flag True off a half-quote.
        self.assertFalse(_verdict(
            PREV_GREEN,
            {'open': 102.0, 'high': 115.0, 'low': 85.0, 'close': float('nan')}))

    def test_a_complete_row_still_qualifies_on_the_range_clause(self):
        # Guard against over-correcting into "any NaN anywhere kills the series".
        df = pd.DataFrame([
            PREV_GREEN,
            {'open': float('nan'), 'high': 115.0, 'low': 85.0, 'close': 103.0},
            {'open': 101.0, 'high': 110.0, 'low': 90.0, 'close': 103.0},
        ])
        flags = compute_inside_day(df['open'], df['high'], df['low'], df['close'])
        self.assertTrue(bool(flags.iloc[2]))  # range clause needs no body values


class EtfParityTests(unittest.TestCase):
    """The dashboard's ETF path recomputes from yfinance's capitalized columns."""

    def test_capitalized_columns_produce_identical_verdicts(self):
        rows = [
            PREV_GREEN,
            {'open': 101.0, 'high': 110.0, 'low': 95.0, 'close': 103.0},   # tie
            {'open': 101.0, 'high': 115.0, 'low': 85.0, 'close': 104.0},   # body only
            {'open': 106.0, 'high': 122.0, 'low': 95.0, 'close': 121.0},   # neither
            PREV_RED,
            {'open': 101.0, 'high': 115.0, 'low': 85.0, 'close': 104.0},   # body, red prev
        ]
        lower = pd.DataFrame(rows)
        upper = lower.rename(columns=str.capitalize)

        from_lower = compute_inside_day(
            lower['open'], lower['high'], lower['low'], lower['close'])
        from_upper = compute_inside_day(
            upper['Open'], upper['High'], upper['Low'], upper['Close'])

        self.assertEqual(list(from_lower), list(from_upper))
        # And the table actually exercises both verdicts, so parity isn't vacuous.
        self.assertIn(True, list(from_lower))
        self.assertIn(False, list(from_lower))


if __name__ == '__main__':
    unittest.main()
