"""Screeners judge a new listing on the bars it has.

The standard averages need half their window (`sma200` from bar 100,
`vol_sma50` from 25, `avg_dollar_vol`/`adr_pct` from 10). The screeners read a
zero-filled master, so a young listing's moving-average legs passed without
testing anything while its liquidity floors failed. The `*_partial` twins
start at PARTIAL_MIN_BARS and equal the originals once half the window exists,
so only young listings screen differently, and the archived originals keep
their meaning for market_history and winner_research.
"""

import unittest

import numpy as np
import pandas as pd

from src.indicators.create_technical_indicators import (
    PARTIAL_MIN_BARS, compute_partial_windows,
)
from src.screening.screeners import steady_trend, volspike


def bars(n, start=10.0, step=0.5, volume=3e6):
    idx = pd.bdate_range('2026-01-02', periods=n)
    close = start + step * np.arange(n)
    return pd.DataFrame({'open': close, 'high': close * 1.05, 'low': close * 0.99,
                         'close': close, 'volume': np.full(n, volume)}, index=idx)


class PartialWindowTests(unittest.TestCase):
    def test_defined_from_the_minimum_bar_count(self):
        out = compute_partial_windows(bars(30))
        for col, series in out.items():
            with self.subTest(col=col):
                self.assertTrue(series.iloc[:PARTIAL_MIN_BARS - 1].isna().all())
                self.assertFalse(np.isnan(series.iloc[PARTIAL_MIN_BARS - 1]))

    def test_equal_to_the_originals_once_half_the_window_exists(self):
        daily = bars(260)
        out = compute_partial_windows(daily)
        close = daily['close']
        for n, half in ((25, 13), (30, 15), (50, 25), (100, 50), (200, 100)):
            original = close.rolling(n, min_periods=half).mean()
            pd.testing.assert_series_equal(out[f'sma{n}_partial'].iloc[half - 1:],
                                           original.iloc[half - 1:], check_names=False)
        pd.testing.assert_series_equal(
            out['vol_sma50_partial'].iloc[24:],
            daily['volume'].rolling(50, min_periods=25).mean().iloc[24:], check_names=False)


def ipo_row(n_bars):
    """The last master row of a rising n-bar listing, as the screeners see it."""
    daily = bars(n_bars)
    row = {k: v.iloc[-1] for k, v in compute_partial_windows(daily).items()}
    last = daily.iloc[-1]
    row.update(close=last['close'], volume=last['volume'], rs_sts_pct=95.0,
               days_since_highest_volume=0, up_dollar_vol_max=last['close'] * last['volume'])
    return pd.DataFrame([row]).fillna(0)


class YoungListingTests(unittest.TestCase):
    def test_a_twenty_bar_listing_reaches_the_screeners(self):
        row = ipo_row(20)
        # Liquidity floors now read real figures instead of a zero fill.
        self.assertGreater(row['vol_sma50_partial'].iloc[0], 1e6)
        self.assertTrue(volspike.filter_master_table(row).iloc[0])

    def test_equal_partial_averages_pass_the_trend_stack(self):
        # Under 50 bars sma50_partial and sma200_partial cover the same bars.
        row = ipo_row(30)
        self.assertAlmostEqual(row['sma50_partial'].iloc[0], row['sma200_partial'].iloc[0])
        row['adr_pct_partial'] = 0.03            # steady_trend's 2-4% ADR band
        row['avg_dollar_vol_partial'] = 50e6
        self.assertTrue(steady_trend.filter_master_table(row).iloc[0])


if __name__ == '__main__':
    unittest.main()
