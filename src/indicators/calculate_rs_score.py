"""
Calculate RS_STS% (Relative Strength Status Percentage) for all tickers.

Replicates the Google Sheets PERCENTRANK formula:
- Compares ticker performance vs SPY over lookback period
- Returns percentile rank (0-100%)
"""

import pandas as pd
import numpy as np
from typing import Dict

from config.settings import CONFIG

LOOKBACK_DAYS = CONFIG["rs_score"]["lookback_days"]
BENCHMARK_TICKER = CONFIG["rs_score"]["benchmark_ticker"]


# Never scored: the benchmark and the index rows the downloader fetches.
EXCLUDED = (BENCHMARK_TICKER, '^GSPC', 'SPY', 'MDY', 'QQQ', 'IWM')


def _rs_sts_one(ticker_prices: pd.Series, spy_prices: pd.Series, lookback_days: int):
    """RS_STS% for one ticker's trailing closes against SPY's, or None if unscored."""
    common_index = ticker_prices.index.intersection(spy_prices.index)
    if len(common_index) < lookback_days * 0.95:
        return None

    relative_perf_series = ticker_prices.loc[common_index] / spy_prices.loc[common_index]
    if len(relative_perf_series) < 2:
        return None

    today_ratio = relative_perf_series.iloc[-1]
    count_less_than = (relative_perf_series < today_ratio).sum()
    total_count = len(relative_perf_series)
    if total_count > 1:
        return min((count_less_than / (total_count - 1)) * 100, 100)
    return 50


def calculate_rs_sts_for_tickers(price_data: Dict, lookback_days: int = None) -> pd.Series:
    """
    Calculate RS_STS% for all tickers.

    Args:
        price_data: Dict mapping ticker -> DataFrame with 'close' column
        lookback_days: Number of days to look back (default from config)

    Returns:
        pd.Series mapping ticker -> RS_STS% (0-100)
    """
    if lookback_days is None:
        lookback_days = LOOKBACK_DAYS

    if BENCHMARK_TICKER not in price_data:
        raise ValueError(f"Benchmark ticker {BENCHMARK_TICKER} not found in price data")

    spy_prices = price_data[BENCHMARK_TICKER]['close'].tail(lookback_days)
    if len(spy_prices) < lookback_days:
        print(f"Warning: Only {len(spy_prices)} days of SPY data available")

    ticker_rs_sts = {}
    for ticker, ticker_df in price_data.items():
        if ticker in EXCLUDED or 'close' not in ticker_df.columns:
            continue
        value = _rs_sts_one(ticker_df['close'].tail(lookback_days), spy_prices, lookback_days)
        if value is not None:
            ticker_rs_sts[ticker] = value

    return pd.Series(ticker_rs_sts)


def _vectorisable(close: pd.Series) -> bool:
    index = close.index
    return close.dtype == np.float64 and index.is_monotonic_increasing and index.is_unique


def rs_sts_by_date(price_data: Dict, run_dates, lookback_days: int = None) -> Dict:
    """RS_STS% as of each run date: ``{run_date: Series}``.

    Equal, value for value, to ``calculate_rs_sts_for_tickers({t: d[:run_date]})``
    for each date (tests/test_master_table_parity.py pins it), without slicing
    every frame once per date. Each ticker's windows for all dates are read in
    one vectorised pass. A ticker whose index is not unique and increasing
    takes the per-date path; so does every ticker if SPY's index is not.
    """
    if lookback_days is None:
        lookback_days = LOOKBACK_DAYS
    if BENCHMARK_TICKER not in price_data:
        raise ValueError(f"Benchmark ticker {BENCHMARK_TICKER} not found in price data")

    run_dates = pd.DatetimeIndex(run_dates)
    spy_close = price_data[BENCHMARK_TICKER]['close']
    if not _vectorisable(spy_close):
        return {rd: calculate_rs_sts_for_tickers({t: d[:rd] for t, d in price_data.items()},
                                                 lookback_days)
                for rd in run_dates}

    L = lookback_days
    spy_values = spy_close.to_numpy()
    spy_end = spy_close.index.searchsorted(run_dates, side='right')   # (K,)
    spy_start = np.maximum(spy_end - L, 0)
    window = np.arange(L) - L                                          # (L,)
    rows = np.arange(len(run_dates))
    scores = {rd: {} for rd in run_dates}

    for ticker, ticker_df in price_data.items():
        if ticker in EXCLUDED or 'close' not in ticker_df.columns:
            continue
        close = ticker_df['close']
        if not _vectorisable(close):
            for rd in run_dates:
                value = _rs_sts_one(close[:rd].tail(L), spy_close[:rd].tail(L), L)
                if value is not None:
                    scores[rd][ticker] = value
            continue
        if len(close) == 0:
            continue

        values = close.to_numpy()
        on_spy = spy_close.index.get_indexer(close.index)              # -1: not a SPY date
        end = close.index.searchsorted(run_dates, side='right')        # (K,)
        pos = end[:, None] + window                                    # (K, L)
        in_range = pos >= 0
        pos = np.where(in_range, pos, 0)
        spy_pos = on_spy[pos]
        # A ticker date is common with SPY's window when it is a SPY date at or
        # after the window's first session (it is never after the run date).
        common = in_range & (spy_pos >= spy_start[:, None])
        n = common.sum(axis=1)
        with np.errstate(divide='ignore', invalid='ignore'):
            ratio = values[pos] / spy_values[np.where(common, spy_pos, 0)]
        last = L - 1 - np.argmax(common[:, ::-1], axis=1)
        today = ratio[rows, last]
        below = (common & (ratio < today[:, None])).sum(axis=1)
        scored = (n >= L * 0.95) & (n >= 2)
        with np.errstate(divide='ignore', invalid='ignore'):
            pct = np.minimum(below / (n - 1) * 100, 100)
        for k in np.flatnonzero(scored):
            scores[run_dates[k]][ticker] = pct[k]

    return {rd: pd.Series(found) for rd, found in scores.items()}


if __name__ == '__main__':
    import src.stock_utils as su
    from config.settings import PRICE_DATA_TA_FILE

    print("Loading price data...")
    price_data = su.load_object_from_pickle(PRICE_DATA_TA_FILE)

    print("Calculating RS_STS%...")
    rs_sts = calculate_rs_sts_for_tickers(price_data)

    print(f"\nCalculated RS_STS% for {len(rs_sts)} tickers")
    print("\nTop 10 tickers by RS_STS%:")
    print(rs_sts.sort_values(ascending=False).head(10))
