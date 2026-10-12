"""Weekly EMA20 for the zone map: a live weekly bar with no look-ahead.

Each daily bar reads the EMA of COMPLETED weeks, plus its own close as the
current week's bar. That is how a TradingView weekly chart reads mid-week.
"""

import unittest

import numpy as np
import pandas as pd

from src.indicators.create_technical_indicators import compute_weekly_ema

A = 2.0 / 21.0


def _closes(n=200, seed=7):
    # 2025-01-06 is a Monday, so bar i falls on weekday i % 5.
    idx = pd.bdate_range('2025-01-06', periods=n)
    rng = np.random.default_rng(seed)
    return pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.02, n))), index=idx)


def _weekly_ema(close):
    weekly = close.groupby(close.index.to_period('W-FRI')).last()
    return weekly.ewm(span=20, adjust=False).mean()


class WeeklyEmaTests(unittest.TestCase):
    def test_each_bar_matches_the_series_cut_at_that_bar(self):
        close = _closes()
        full = compute_weekly_ema(close, span=20, min_weeks=20)
        for i in range(100, len(close), 3):
            cut = compute_weekly_ema(close.iloc[:i + 1], span=20, min_weeks=20)
            self.assertAlmostEqual(full.iloc[i], cut.iloc[-1], places=10, msg=f'bar {i}')

    def test_mid_week_value_blends_todays_close_with_last_weeks_ema(self):
        close = _closes()
        out = compute_weekly_ema(close, span=20, min_weeks=20)
        wema = _weekly_ema(close)
        i = 152  # a Wednesday
        week = close.index[i].to_period('W-FRI')
        prev = wema.iloc[wema.index.get_loc(week) - 1]
        self.assertAlmostEqual(out.iloc[i], A * close.iloc[i] + (1 - A) * prev, places=10)

    def test_the_last_bar_of_a_week_equals_that_weeks_ema(self):
        close = _closes()
        out = compute_weekly_ema(close, span=20, min_weeks=20)
        i = 154  # a Friday
        self.assertEqual(4, close.index[i].dayofweek)
        self.assertAlmostEqual(
            out.iloc[i], _weekly_ema(close).loc[close.index[i].to_period('W-FRI')], places=10)

    def test_the_value_starts_once_twenty_weeks_are_complete(self):
        close = _closes()
        out = compute_weekly_ema(close, span=20, min_weeks=20)
        self.assertTrue(np.isnan(out.iloc[99]), 'week 19 has only 19 completed weeks')
        self.assertTrue(np.isfinite(out.iloc[100]), 'week 20 has 20 completed weeks')

    def test_a_short_history_is_all_nan(self):
        out = compute_weekly_ema(_closes(n=99), span=20, min_weeks=20)
        self.assertTrue(out.isna().all())

    def test_a_holiday_friday_closes_the_week_on_thursday(self):
        close = _closes()
        friday, thursday, monday = close.index[154], close.index[153], close.index[155]
        holiday = close.drop(friday)
        out = compute_weekly_ema(holiday, span=20, min_weeks=20)
        wema = _weekly_ema(holiday)
        self.assertAlmostEqual(
            out.loc[monday],
            A * holiday.loc[monday] + (1 - A) * wema.loc[thursday.to_period('W-FRI')],
            places=10)

    def test_the_index_is_unchanged(self):
        close = _closes()
        out = compute_weekly_ema(close)
        self.assertTrue(out.index.equals(close.index))


if __name__ == '__main__':
    unittest.main()
