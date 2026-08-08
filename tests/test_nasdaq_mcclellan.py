"""Tests for the Nasdaq McClellan Summation Index and its RSI."""

import unittest

import numpy as np
import pandas as pd

from src.indicators.nasdaq_mcclellan import (
    compute_breadth_frame,
    mcclellan_oscillator,
    ratio_adjusted_net_advances,
    summation_index,
    wilder_rsi,
)


def _sessions(n):
    return pd.bdate_range("2024-01-01", periods=n)


class TestRatioAdjustedNetAdvances(unittest.TestCase):
    def test_scales_net_advances_to_plus_minus_1000(self):
        idx = _sessions(3)
        adv = pd.Series([100, 0, 50], index=idx)
        dec = pd.Series([0, 100, 50], index=idx)
        rana = ratio_adjusted_net_advances(adv, dec)
        self.assertAlmostEqual(rana.iloc[0], 1000.0)
        self.assertAlmostEqual(rana.iloc[1], -1000.0)
        self.assertAlmostEqual(rana.iloc[2], 0.0)

    def test_session_with_no_issues_traded_is_nan_not_zero(self):
        """A zero would read as a neutral day and wrongly pull the EMAs."""
        idx = _sessions(2)
        rana = ratio_adjusted_net_advances(
            pd.Series([0, 10], index=idx), pd.Series([0, 5], index=idx)
        )
        self.assertTrue(np.isnan(rana.iloc[0]))
        self.assertFalse(np.isnan(rana.iloc[1]))


class TestOscillatorAndSummation(unittest.TestCase):
    def test_oscillator_is_zero_for_a_flat_series(self):
        rana = pd.Series(250.0, index=_sessions(60))
        self.assertAlmostEqual(float(mcclellan_oscillator(rana).iloc[-1]), 0.0, places=6)

    def test_summation_first_difference_is_the_oscillator(self):
        """The property the RSI-seed invariance rests on."""
        rng = np.random.default_rng(7)
        rana = pd.Series(rng.normal(0, 300, 200), index=_sessions(200))
        osc = mcclellan_oscillator(rana)
        recovered = summation_index(osc).diff().dropna()
        pd.testing.assert_series_equal(recovered, osc.iloc[1:], check_names=False)


class TestWilderRsi(unittest.TestCase):
    def test_unbroken_gains_and_losses_reach_the_ends(self):
        rising = pd.Series(np.arange(40, dtype=float), index=_sessions(40))
        self.assertAlmostEqual(float(wilder_rsi(rising).iloc[-1]), 100.0)
        self.assertAlmostEqual(float(wilder_rsi(rising[::-1].reset_index(drop=True)).iloc[-1]), 0.0)

    def test_warmup_rows_are_nan_not_zero(self):
        """Regression: warmup rows must not read as a real oversold value.

        An earlier guard pinned the ends with `.where(avg_loss > 0, 100)`. NaN
        fails that comparison, so every min_periods warmup row was rewritten to
        0.0 — it then survived `dropna(subset=["rsi"])`, satisfied `rsi <= 10`,
        and drew 14 fabricated bottom-signal markers on the dashboard.
        """
        rng = np.random.default_rng(5)
        series = pd.Series(np.cumsum(rng.normal(0, 5, 60)), index=_sessions(60))
        rsi = wilder_rsi(series, period=14)
        self.assertTrue(rsi.iloc[:14].isna().all(), "warmup rows must stay NaN")
        self.assertFalse(rsi.iloc[14:].isna().any(), "post-warmup rows must be defined")
        self.assertEqual(int((rsi.iloc[:14] <= 10).sum()), 0)

    def test_matches_the_closed_form_wilder_fixed_point(self):
        """Pins Wilder's recursion specifically, not just "some average".

        Feed alternating +2 / -1 steps and sample right after a loss step. With
        b = 1 - 1/period, the two-step fixed point is
            avg_gain = 2*alpha*b / (1 - b**2),  avg_loss = alpha / (1 - b**2)
        so RS = 2b and RSI = 100 * 2b / (1 + 2b). For period 14 that is exactly
        65.0. A simple moving average would instead give 100*2/3 = 66.67, so this
        value is what separates the two smoothings.
        """
        steps = np.resize([2.0, -1.0], 400)
        self.assertEqual(steps[-1], -1.0, "fixed point below assumes a trailing loss step")
        series = pd.Series(np.cumsum(np.r_[100.0, steps]))

        b = 1 - 1 / 14
        expected = 100 * (2 * b) / (1 + 2 * b)
        self.assertAlmostEqual(expected, 65.0, places=9)
        self.assertAlmostEqual(float(wilder_rsi(series).iloc[-1]), expected, places=6)


class TestSeedInvariance(unittest.TestCase):
    def test_rsi_is_unaffected_by_where_the_summation_started(self):
        """The whole reason we can publish this without agreeing on an epoch.

        The summation index is a running total with an arbitrary origin, so its
        level is not comparable to any vendor's. RSI reads only differences, so
        it is — shifting the entire series must not move the RSI at all.
        """
        rng = np.random.default_rng(11)
        osc = pd.Series(rng.normal(0, 40, 300), index=_sessions(300))
        baseline = wilder_rsi(summation_index(osc))
        shifted = wilder_rsi(summation_index(osc) + 12_345.678)
        pd.testing.assert_series_equal(baseline, shifted)


class TestBreadthFrame(unittest.TestCase):
    def setUp(self):
        rng = np.random.default_rng(3)
        idx = _sessions(300)
        total = 3000
        self.adv = pd.Series(rng.integers(800, 2200, 300), index=idx)
        self.dec = pd.Series(total - self.adv.to_numpy(), index=idx)

    def test_exposes_the_columns_the_dashboard_consumes(self):
        frame = compute_breadth_frame(self.adv, self.dec)
        for column in ("issues", "rana", "oscillator", "summation", "summation_ma", "rsi"):
            self.assertIn(column, frame.columns)

    def test_rsi_stays_within_bounds(self):
        rsi = compute_breadth_frame(self.adv, self.dec)["rsi"].dropna()
        self.assertTrue(((rsi >= 0) & (rsi <= 100)).all())

    def test_sustained_decline_drives_rsi_into_oversold_territory(self):
        """The behaviour the oversold band is read off: a persistent run of
        negative breadth must push RSI(14) of the summation below 10."""
        idx = _sessions(300)
        adv = pd.Series(np.r_[np.full(200, 1800), np.full(100, 700)], index=idx)
        dec = 3000 - adv
        rsi = compute_breadth_frame(adv, dec)["rsi"].dropna()
        self.assertLess(float(rsi.iloc[-1]), 10.0)


if __name__ == "__main__":
    unittest.main()
