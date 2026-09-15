"""Tightness and the tight-base flag.

`tightness` is the trailing-window mean of each bar's absolute close-to-close
change divided by *that bar's* ADR%. Lower is tighter. The mean is taken over
per-bar ratios, not by dividing a windowed mean change by a single ADR%:
`adr_pct` is itself a 20-session rolling mean and moves inside the window, so
the two forms are not equivalent, and the calibration behind the 0.30 default
used the per-bar form.

`tight_base` is tightness qualified by location: still on the 10/20-day EMAs
and not more than 30% off the 50-day high. The location gate is a DISQUALIFIER
— it removes charts that already broke down so tightness can choose among what
is left. At 0.70 it passes 82% of rows on its own and removes 26% of otherwise
tight names, which moves forward 10-session excess from 0.66pp BELOW the
universe baseline (bare tightness) to 0.40pp above it.

⛔ It is a descriptive marker, not a ranking input, and these tests pin the
definition rather than any claim about returns. Theme-level breadth of the flag
scores a rank IC of +0.011 — a coin flip. A period-high test with no tightness
at all scores +0.169, so on theme ranking the location half carries everything.

`period_high` is a rolling max of HIGHS (the pipeline passes `max50`), not of
closes. A calibration against close-based highs produced 0.90, which rejected
every name in the motivating case once run against the real column.
"""

import unittest

import numpy as np
import pandas as pd

from src.indicators.create_technical_indicators import (
    TIGHTNESS_FRACTION,
    TIGHTNESS_HIGH_FRAC,
    TIGHTNESS_HIGH_LOOKBACK,
    TIGHTNESS_WINDOW,
    compute_tight_base,
    compute_tightness,
)


def _closes(changes, start=100.0):
    """Build a close series from successive fractional changes."""
    out = [start]
    for c in changes:
        out.append(out[-1] * (1 + c))
    return pd.Series(out, dtype=float)


class ComputeTightnessTests(unittest.TestCase):
    def test_flat_closes_score_zero(self):
        close = pd.Series([50.0] * 8)
        adr = pd.Series([0.04] * 8)
        t = compute_tightness(close, adr, window=4)
        self.assertAlmostEqual(t.iloc[-1], 0.0)

    def test_moves_equal_to_adr_score_one(self):
        # Alternating +/-4% against a 4% ADR: every per-bar ratio is 1.0.
        close = _closes([0.04, -0.04, 0.04, -0.04, 0.04, -0.04])
        adr = pd.Series([0.04] * len(close))
        t = compute_tightness(close, adr, window=4)
        self.assertAlmostEqual(t.iloc[-1], 1.0, places=2)

    def test_partial_window_is_nan_not_a_short_mean(self):
        # 4 closes give 3 change ratios — one short of the window.
        close = pd.Series([10.0, 10.1, 10.0, 10.1])
        adr = pd.Series([0.05] * 4)
        t = compute_tightness(close, adr, window=4)
        self.assertTrue(t.isna().all())

    def test_nan_adr_contributes_no_ratio_and_voids_its_window(self):
        close = _closes([0.01] * 6)
        adr = pd.Series([0.05] * len(close))
        adr.iloc[3] = np.nan
        t = compute_tightness(close, adr, window=4)
        # Every window covering index 3 is short one ratio.
        self.assertTrue(t.iloc[3:6].isna().all())

    def test_zero_adr_is_nan_not_infinity(self):
        close = _closes([0.01] * 6)
        adr = pd.Series([0.0] * len(close))
        t = compute_tightness(close, adr, window=4)
        self.assertTrue(t.isna().all())
        self.assertFalse(np.isinf(t.to_numpy(dtype=float)).any())

    def test_direction_agnostic(self):
        up = _closes([0.01, 0.02, 0.01, 0.015, 0.01])
        down = _closes([-0.01, -0.02, -0.01, -0.015, -0.01])
        adr = pd.Series([0.05] * len(up))
        a = compute_tightness(up, adr, window=4).iloc[-1]
        b = compute_tightness(down, adr, window=4).iloc[-1]
        # Not identical to the last decimal (compounding differs), but the
        # measure must not care about sign — same magnitude ordering.
        self.assertAlmostEqual(a, b, places=2)

    def test_widening_the_window_never_rescues_a_nan(self):
        # A longer window needs strictly more history, so wherever the short
        # window already reported NaN for want of bars, the long one must too.
        close = _closes([0.01] * 10)
        adr = pd.Series([0.05] * len(close))
        short = compute_tightness(close, adr, window=3)
        long_ = compute_tightness(close, adr, window=6)
        self.assertTrue(bool((short.notna() | long_.isna()).all()))

    def test_returns_a_float_series_aligned_to_the_input(self):
        close = _closes([0.01] * 6)
        adr = pd.Series([0.05] * len(close))
        t = compute_tightness(close, adr, window=4)
        self.assertEqual(len(t), len(close))
        self.assertTrue(t.index.equals(close.index))
        self.assertEqual(t.dtype, float)

    def test_per_bar_form_differs_from_dividing_a_windowed_mean(self):
        # The guard against the arithmetic drifting back to the simpler form.
        # ADR% moves inside the window, so the two are not interchangeable.
        close = _closes([0.01, 0.05, 0.01, 0.05])
        adr = pd.Series([0.02, 0.02, 0.10, 0.10, 0.10])
        per_bar = compute_tightness(close, adr, window=4).iloc[-1]
        naive = close.pct_change().abs().rolling(4).mean().iloc[-1] / adr.iloc[-1]
        self.assertNotAlmostEqual(per_bar, naive, places=3)


class ComputeTightBaseTests(unittest.TestCase):
    """Each conjunct gets its own falsifying case, so a dropped clause fails."""

    N = 6

    def _frame(self, tightness, close, period_high, close_to_ma):
        return dict(
            tightness=pd.Series([tightness] * self.N),
            close=pd.Series([close] * self.N),
            period_high=pd.Series([period_high] * self.N),
            close_to_ma=pd.Series([close_to_ma] * self.N),
        )

    def _verdict(self, **kw):
        return bool(compute_tight_base(**kw).iloc[-1])

    def test_all_three_conjuncts_true(self):
        self.assertTrue(self._verdict(
            **self._frame(tightness=0.20, close=95.0, period_high=100.0, close_to_ma=True)))

    def test_loose_tightness_fails(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.60, close=95.0, period_high=100.0, close_to_ma=True)))

    def test_away_from_the_moving_averages_fails(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.20, close=95.0, period_high=100.0, close_to_ma=False)))

    def test_a_broken_chart_fails_however_quiet_it_is(self):
        # 55% of the 50-day high: a stock that has already fallen apart and
        # then gone still. This is the case the gate exists to reject — bare
        # tightness would flag it, and bare tightness measures the wrong way.
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.05, close=55.0, period_high=100.0, close_to_ma=True)))

    def test_a_shallow_pullback_still_qualifies(self):
        # 75% of the high: down but not broken, which the 0.70 gate admits by
        # design. A gate tight enough to reject this would be a selector, and
        # the selecting is the tightness test's job.
        self.assertTrue(self._verdict(
            **self._frame(tightness=0.20, close=75.0, period_high=100.0, close_to_ma=True)))

    def test_nan_tightness_is_false_not_na(self):
        v = compute_tight_base(
            **self._frame(tightness=np.nan, close=95.0, period_high=100.0, close_to_ma=True))
        self.assertFalse(bool(v.iloc[-1]))
        self.assertEqual(v.dtype, bool)

    def test_nan_period_high_is_false(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.20, close=95.0, period_high=np.nan, close_to_ma=True)))

    def test_boundaries_are_inclusive(self):
        # Exactly at the fraction and exactly at the high threshold both
        # qualify, matching compute_inside_day: a tie is a coil, not a miss.
        self.assertTrue(self._verdict(**self._frame(
            tightness=TIGHTNESS_FRACTION,
            close=TIGHTNESS_HIGH_FRAC * 100.0,
            period_high=100.0,
            close_to_ma=True,
        )))

    def test_returns_boolean_dtype_aligned_to_the_input(self):
        v = compute_tight_base(
            **self._frame(tightness=0.2, close=95.0, period_high=100.0, close_to_ma=True))
        self.assertEqual(v.dtype, bool)
        self.assertEqual(len(v), self.N)

    def test_the_gate_admits_far_more_than_it_rejects(self):
        # Pins the disqualifier-not-selector intent. If someone re-tunes
        # high_frac upward into selector territory, this is what catches it:
        # a stock 25% off its high is a pullback, not a broken chart.
        self.assertLessEqual(TIGHTNESS_HIGH_FRAC, 0.75)

    def test_defaults_match_the_calibration(self):
        # A silent change to any of these re-tunes the flag's firing rate.
        # The lookback must also exist as a max<N> column in the pipeline's
        # min_max_lookback list, or calculate_technical_indicators raises.
        self.assertEqual(TIGHTNESS_WINDOW, 4)
        self.assertEqual(TIGHTNESS_FRACTION, 0.30)
        self.assertEqual(TIGHTNESS_HIGH_LOOKBACK, 50)
        self.assertEqual(TIGHTNESS_HIGH_FRAC, 0.70)


if __name__ == '__main__':
    unittest.main()
