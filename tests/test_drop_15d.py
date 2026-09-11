"""Guards for the SI tab's selloff indicators.

`drop_15d` exists because today's bar cannot see a crash that has already
finished. The obvious definition — today's close against its own trailing
15-session high — was measured against a true window search over 183 heavily
shorted names on 2026-09-10. It correlated **0.37**, and it read AEHR at
**-22.5%** where the real figure is **-47.6%**, so AEHR would have failed the
gate built for it. The 45-session rolling minimum correlates **0.914** and
matches AEHR exactly.

The property test below is the durable half of that finding: the rolling
minimum can never read shallower than the per-bar form. If someone "simplifies"
the definition back, that test fails rather than the tab quietly shrinking.

`max_down_streak` is display colour only. It is pinned here so a future edit
does not promote it into the gate, which would reject a name that fell 40% in
three gap-downs. Its own trap is the same shape as `drop_15d`'s: the streak
ending *today* reads 1 for almost every row and answers nothing.
"""

import unittest

import numpy as np
import pandas as pd

from src.indicators.create_technical_indicators import (
    compute_down_streak,
    compute_drop_15d,
    compute_max_down_streak,
)


class Drop15dTests(unittest.TestCase):
    def test_recovers_a_past_crash_that_todays_bar_cannot_see(self):
        """A 50% fall, then 25 flat bars. Still a broken chart."""
        close = pd.Series([100.0] * 10 + [50.0] * 26)

        self.assertAlmostEqual(compute_drop_15d(close).iloc[-1], -0.5, places=6)

    def test_todays_bar_alone_would_miss_that_crash(self):
        """The contrast the whole definition exists for."""
        close = pd.Series([100.0] * 10 + [50.0] * 26)
        per_bar = close / close.rolling(16, min_periods=2).max() - 1

        self.assertAlmostEqual(per_bar.iloc[-1], 0.0, places=6)
        self.assertAlmostEqual(compute_drop_15d(close).iloc[-1], -0.5, places=6)

    def test_never_reads_shallower_than_the_per_bar_form(self):
        """Property: the rolling minimum is a lower bound on today's bar."""
        rng = np.random.default_rng(0)
        close = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.03, 200))))
        per_bar = close / close.rolling(16, min_periods=2).max() - 1

        out = compute_drop_15d(close)

        both = out.notna() & per_bar.notna()
        self.assertTrue(both.sum() > 150, "too few comparable bars to be a test")
        self.assertTrue((out[both] <= per_bar[both] + 1e-12).all())

    def test_a_drop_ages_out_of_the_lookback(self):
        """Past the 45-session window the crash stops counting."""
        close = pd.Series([100.0] * 5 + [50.0] * 70)

        self.assertAlmostEqual(compute_drop_15d(close).iloc[-1], 0.0, places=6)

    def test_a_slide_longer_than_the_window_is_capped_at_the_window(self):
        """15 sessions of -5% compounding, not the whole 30-session slide."""
        close = pd.Series([100.0 * (0.95 ** i) for i in range(31)])

        expected = 0.95 ** 15 - 1
        self.assertAlmostEqual(compute_drop_15d(close).iloc[-1], expected, places=6)

    def test_a_rising_series_never_reads_positive(self):
        close = pd.Series(np.linspace(10.0, 50.0, 80))

        self.assertLessEqual(compute_drop_15d(close).max(), 0.0)

    def test_only_the_first_bar_is_nan(self):
        """Bar 0 has no predecessor, so it carries no drawdown. Every later
        bar scores, which keeps a recent listing usable.

        NaN here is the honest answer and the SI gate fails closed on it, so a
        one-bar ticker is rejected rather than admitted on a guessed zero.
        """
        out = compute_drop_15d(pd.Series([10.0, 9.0, 8.0]))

        self.assertTrue(pd.isna(out.iloc[0]))
        self.assertFalse(out.iloc[1:].isna().any())


class DownStreakTests(unittest.TestCase):
    def test_counts_consecutive_down_closes(self):
        close = pd.Series([10.0, 9.0, 8.0, 7.0, 8.0, 7.0, 6.0])

        self.assertEqual(list(compute_down_streak(close)), [0, 1, 2, 3, 0, 1, 2])

    def test_a_flat_close_breaks_the_streak(self):
        """An unchanged close is not a down day."""
        close = pd.Series([10.0, 9.0, 9.0, 8.0])

        self.assertEqual(list(compute_down_streak(close)), [0, 1, 0, 1])

    def test_a_monotonic_decline_counts_every_bar(self):
        close = pd.Series([10.0, 9.0, 8.0, 7.0, 6.0])

        self.assertEqual(compute_down_streak(close).iloc[-1], 4)

    def test_returns_integers(self):
        close = pd.Series([10.0, 9.0, 8.0])

        self.assertTrue(pd.api.types.is_integer_dtype(compute_down_streak(close)))


class MaxDownStreakTests(unittest.TestCase):
    """The column the SI tab actually shows.

    Measured 2026-09-10, the running streak read 1 for AEHR, AMKR and COHU
    alike, while AEHR's real slide ran 11 sessions. A column that reads 1 for
    every row answers no question.
    """

    def test_reports_a_past_slide_not_todays_run(self):
        close = pd.Series([10.0 - i for i in range(6)] + [20.0] * 10)

        self.assertEqual(compute_max_down_streak(close).iloc[-1], 5)

    def test_running_streak_would_miss_that_slide(self):
        """The contrast this helper exists for."""
        close = pd.Series([10.0 - i for i in range(6)] + [20.0] * 10)

        self.assertEqual(compute_down_streak(close).iloc[-1], 0)
        self.assertEqual(compute_max_down_streak(close).iloc[-1], 5)

    def test_a_slide_ages_out_of_the_lookback(self):
        close = pd.Series([10.0 - i for i in range(6)] + [20.0] * 60)

        self.assertEqual(compute_max_down_streak(close).iloc[-1], 0)

    def test_never_reads_below_the_running_streak(self):
        rng = np.random.default_rng(1)
        close = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.03, 200))))

        self.assertTrue(
            (compute_max_down_streak(close) >= compute_down_streak(close)).all()
        )

    def test_returns_integers(self):
        close = pd.Series([10.0, 9.0, 8.0])

        self.assertTrue(pd.api.types.is_integer_dtype(compute_max_down_streak(close)))


if __name__ == "__main__":
    unittest.main()
