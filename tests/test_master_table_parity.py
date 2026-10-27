"""The batched master-table build equals the one-session-at-a-time build.

`create_master_tables` takes each ticker's rows for every session at once,
stacks tickers that share a schema and scores RS_STS% in one vectorised pass
(`rs_sts_by_date`). market_history archives every master column and value
daily, so the batched path has to reproduce the original construction
exactly, dtypes and column order included. `_reference_master_table` below is
that original construction, kept verbatim apart from returning the frame
instead of saving it.
"""

import unittest

import numpy as np
import pandas as pd

from src.indicators.calculate_rs_score import (
    BENCHMARK_TICKER,
    calculate_rs_sts_for_tickers,
    rs_sts_by_date,
)
from src.screening.create_master_table import build_master_tables


def _reference_rs_sts(price_data, lookback_days=27):
    spy_prices = price_data[BENCHMARK_TICKER]['close'].tail(lookback_days)
    ticker_rs_sts = {}
    for ticker, ticker_df in price_data.items():
        if ticker in [BENCHMARK_TICKER, '^GSPC', 'SPY', 'MDY', 'QQQ', 'IWM']:
            continue
        if 'close' not in ticker_df.columns:
            continue
        ticker_prices = ticker_df['close'].tail(lookback_days)
        common_index = ticker_prices.index.intersection(spy_prices.index)
        if len(common_index) < lookback_days * 0.95:
            continue
        ticker_prices_aligned = ticker_prices.loc[common_index]
        spy_prices_aligned = spy_prices.loc[common_index]
        relative_perf_series = ticker_prices_aligned / spy_prices_aligned
        if len(relative_perf_series) < 2:
            continue
        today_ratio = relative_perf_series.iloc[-1]
        count_less_than = (relative_perf_series < today_ratio).sum()
        total_count = len(relative_perf_series)
        if total_count > 1:
            percentrank = (count_less_than / (total_count - 1)) * 100
            ticker_rs_sts[ticker] = min(percentrank, 100)
        else:
            ticker_rs_sts[ticker] = 50
    return pd.Series(ticker_rs_sts)


def _reference_master_table(offset_days, daily_price, date_list):
    run_date = date_list[-(offset_days + 1)]
    daily_tickers = daily_price.keys()
    df = daily_price['^GSPC'][:run_date].tail(1).copy()
    df['ticker'] = 'SPX'
    tickers = [t for t in daily_tickers if t not in ['SPY', 'MDY', 'QQQ', 'IWM', '^GSPC']]
    ticker_dfs = []
    for ticker in tickers:
        if ticker in daily_price:
            ticker_df = daily_price[ticker][:run_date].tail(1).copy()
            if not ticker_df.empty:
                if (run_date - ticker_df.index[0]).days > 7:
                    continue
                ticker_df['ticker'] = ticker
                ticker_dfs.append(ticker_df)
    if ticker_dfs:
        df = pd.concat([df] + ticker_dfs, ignore_index=True)
    df['vol_ratio'] = df['vol_sma40'] / df['vol_sma252']
    df['vol_ratio_rank'] = pd.qcut(df['vol_ratio'].rank(method='first'), 100, labels=False)
    for month in [1, 3, 6]:
        if f'perf_{month}mo' in df.columns:
            df[f'perf_{month}mo_rank'] = pd.qcut(df[f'perf_{month}mo'].rank(method='first'), 100, labels=False)
        if f'rela_perf_{month}mo' in df.columns:
            df[f'rela_perf_{month}mo_rank'] = pd.qcut(df[f'rela_perf_{month}mo'].rank(method='first'), 100, labels=False)
    for lookback in [30, 60, 90, 120]:
        if f'min{lookback}' in df.columns:
            df[f'c0_c{lookback}_rank'] = pd.qcut((df['close'] / df[f'min{lookback}']).rank(method='first'), 100, labels=False)
    price_data_as_of = {t: d[:run_date] for t, d in daily_price.items()}
    rs_sts = _reference_rs_sts(price_data_as_of)
    df['rs_sts_pct'] = df['ticker'].map(rs_sts)
    for col_name in ['ticker']:
        if col_name in df.columns:
            ticker_col = df.pop(col_name)
            df.insert(df.shape[1], col_name, ticker_col)
    df.insert(0, 'date', run_date.strftime('%Y-%m-%d'))
    return df


SESSIONS = pd.bdate_range(end='2026-10-23', periods=260)


def _frame(rng, index, *, base=50.0):
    """Indicator-shaped bars: floats, NaNs, a bool, an int and a string column."""
    k = len(index)
    close = base * np.exp(np.cumsum(rng.normal(0.001, 0.03, k)))
    # Ties in the ratio series exercise the strict `<` count.
    close[k // 2:k // 2 + 3] = close[k // 2]
    f = pd.DataFrame({
        'open': close * 1.001, 'high': close * 1.02, 'low': close * 0.98, 'close': close,
        'volume': rng.integers(100_000, 5_000_000, k).astype(float),
    }, index=index)
    f['vol_sma40'] = f['volume'].rolling(40, min_periods=20).mean()
    f['vol_sma252'] = f['volume'].rolling(120, min_periods=60).mean()
    for m, dt in ((1, 21), (3, 63), (6, 126)):
        f[f'perf_{m}mo'] = f['close'] / f['close'].shift(dt) - 1
        f[f'rela_perf_{m}mo'] = 1 + f[f'perf_{m}mo']
    for lb in (30, 60, 90, 120):
        f[f'min{lb}'] = f['low'].rolling(lb, min_periods=lb // 2).min()
    f['tight_range'] = rng.random(k) > 0.6
    f['tight_len'] = rng.integers(0, 10, k)
    f['tight_gate'] = np.where(rng.random(k) > 0.5, 'trend', None)
    return f


def _universe():
    rng = np.random.default_rng(7)
    data = {t: _frame(rng, SESSIONS, base=400.0) for t in ('SPY', 'MDY', 'QQQ', 'IWM', '^GSPC')}
    for i in range(48):
        kind = i % 12
        index = SESSIONS
        if kind == 1:                       # stale by 1-6 sessions (some pass the 7-day rule)
            index = SESSIONS[:-(1 + i % 6)]
        elif kind == 2:                     # stale well past 7 days
            index = SESSIONS[:-12]
        elif kind == 3:                     # short history (recent listing)
            index = SESSIONS[-(3 + i % 30):]
        elif kind == 4:                     # sessions missing mid-window
            index = SESSIONS.delete(slice(240, 246))
        f = _frame(rng, index, base=rng.uniform(2, 300))
        if kind == 5:                       # NaN closes, one on the newest bar
            f.iloc[[10, 251, -1], f.columns.get_loc('close')] = np.nan
        elif kind == 6:                     # a bar on a non-SPY date
            extra = f.iloc[[-1]].copy()
            extra.index = [SESSIONS[-1] + pd.Timedelta(days=1)]
            f = pd.concat([f, extra])
        elif kind == 7:                     # indicator step failed: raw bars only
            f = f[['high', 'low', 'close', 'volume']]
        elif kind == 8:                     # int volume
            f['volume'] = f['volume'].astype('int64')
        elif kind == 9:                     # columns in another order
            f = f[list(reversed(f.columns))]
        elif kind == 10:                    # an extra column only some tickers carry
            f['only_some'] = 1.5
        data[f'T{i:02d}'] = f
    return data


class MasterTableParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = _universe()
        cls.dates = cls.data['SPY'].index
        cls.offsets = [0, 1, 2, 3, 5, 6, 9, 20, 59]

    def test_batched_build_equals_per_session_build(self):
        run_dates = [self.dates[-(o + 1)] for o in self.offsets]
        built = dict(build_master_tables(self.data, run_dates))
        self.assertEqual(len(built), len(self.offsets))
        for offset in self.offsets:
            expected = _reference_master_table(offset, self.data, self.dates)
            got = built[expected['date'].iloc[0]]
            with self.subTest(offset=offset):
                self.assertEqual(list(got.columns), list(expected.columns))
                pd.testing.assert_frame_equal(got, expected)

    def test_single_session_build_equals_per_session_build(self):
        # market_history builds one session per call.
        for offset in (0, 4):
            expected = _reference_master_table(offset, self.data, self.dates)
            [(_, got)] = list(build_master_tables(self.data, [self.dates[-(offset + 1)]]))
            pd.testing.assert_frame_equal(got, expected)

    def test_the_fixture_covers_the_edge_cases(self):
        newest = _reference_master_table(0, self.data, self.dates)
        self.assertIn('only_some', newest.columns)
        self.assertEqual(newest['tight_range'].dtype, object)  # bool + a missing column
        self.assertTrue(newest['rs_sts_pct'].isna().any())
        self.assertTrue(newest['rs_sts_pct'].notna().sum() > 20)
        self.assertNotIn('T02', set(newest['ticker']))           # stale > 7 days

    def test_rs_by_date_equals_per_date_scoring(self):
        run_dates = [self.dates[-(o + 1)] for o in self.offsets]
        by_date = rs_sts_by_date(self.data, run_dates, lookback_days=27)
        for rd in run_dates:
            expected = calculate_rs_sts_for_tickers(
                {t: d[:rd] for t, d in self.data.items()}, lookback_days=27)
            pd.testing.assert_series_equal(by_date[rd], expected)
            pd.testing.assert_series_equal(
                expected, _reference_rs_sts({t: d[:rd] for t, d in self.data.items()}))


if __name__ == '__main__':
    unittest.main()
