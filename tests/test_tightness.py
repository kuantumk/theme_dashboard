"""Tightness and the tight-base flag.

`tightness` is the trailing-window mean of each bar's absolute close-to-close
change divided by *that bar's* ADR%. Lower is tighter. The mean is taken over
per-bar ratios, not by dividing a windowed mean change by a single ADR%:
`adr_pct` is itself a 20-session rolling mean and moves inside the window, so
the two forms are not equivalent, and the calibration behind the 0.30 default
used the per-bar form.

`tight_base` is tightness qualified by location — still on the 10/20-day EMAs
and still near the 60-day high.

⛔ It is a descriptive marker, not a predictive signal, and these tests pin the
definition rather than any claim about returns. Theme-level rank IC over 165
sessions of 2026 at a 10-session horizon: bare tightness breadth -0.077, the
full flag +0.074, and a near-the-60-day-high-only control +0.167. The location
half carries the edge and the tightness half subtracts from it.

`high_frac` is 0.85 because `max60` is a rolling max of HIGHS. The calibration
that produced 0.90 used closes, which are strictly lower; against the real
column that threshold rejected every name in the motivating case.
"""

import unittest

import numpy as np
import pandas as pd

from src.indicators.create_technical_indicators import (
    TIGHTNESS_FRACTION,
    TIGHTNESS_HIGH_FRAC,
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

    def _frame(self, tightness, close, max60, close_to_ma):
        return dict(
            tightness=pd.Series([tightness] * self.N),
            close=pd.Series([close] * self.N),
            max60=pd.Series([max60] * self.N),
            close_to_ma=pd.Series([close_to_ma] * self.N),
        )

    def _verdict(self, **kw):
        return bool(compute_tight_base(**kw).iloc[-1])

    def test_all_three_conjuncts_true(self):
        self.assertTrue(self._verdict(
            **self._frame(tightness=0.20, close=95.0, max60=100.0, close_to_ma=True)))

    def test_loose_tightness_fails(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.60, close=95.0, max60=100.0, close_to_ma=True)))

    def test_away_from_the_moving_averages_fails(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.20, close=95.0, max60=100.0, close_to_ma=False)))

    def test_far_below_the_60_day_high_fails(self):
        # Quiet but broken: 70% of the 60-day high.
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.20, close=70.0, max60=100.0, close_to_ma=True)))

    def test_nan_tightness_is_false_not_na(self):
        v = compute_tight_base(
            **self._frame(tightness=np.nan, close=95.0, max60=100.0, close_to_ma=True))
        self.assertFalse(bool(v.iloc[-1]))
        self.assertEqual(v.dtype, bool)

    def test_nan_max60_is_false(self):
        self.assertFalse(self._verdict(
            **self._frame(tightness=0.20, close=95.0, max60=np.nan, close_to_ma=True)))

    def test_boundaries_are_inclusive(self):
        # Exactly at the fraction and exactly at the high threshold both
        # qualify, matching compute_inside_day: a tie is a coil, not a miss.
        self.assertTrue(self._verdict(**self._frame(
            tightness=TIGHTNESS_FRACTION,
            close=TIGHTNESS_HIGH_FRAC * 100.0,
            max60=100.0,
            close_to_ma=True,
        )))

    def test_returns_boolean_dtype_aligned_to_the_input(self):
        v = compute_tight_base(
            **self._frame(tightness=0.2, close=95.0, max60=100.0, close_to_ma=True))
        self.assertEqual(v.dtype, bool)
        self.assertEqual(len(v), self.N)

    def test_defaults_match_the_calibration(self):
        # A silent change to any of these re-tunes the flag's firing rate.
        # high_frac in particular is 0.85 against a HIGH-based max60; 0.90 came
        # from a close-based calibration and rejects the motivating cohort.
        self.assertEqual(TIGHTNESS_WINDOW, 4)
        self.assertEqual(TIGHTNESS_FRACTION, 0.30)
        self.assertEqual(TIGHTNESS_HIGH_FRAC, 0.85)


if __name__ == '__main__':
    unittest.main()
