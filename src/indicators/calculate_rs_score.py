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

    spy_df = price_data[BENCHMARK_TICKER]
    spy_prices = spy_df['close'].tail(lookback_days)

    if len(spy_prices) < lookback_days:
        print(f"Warning: Only {len(spy_prices)} days of SPY data available")

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

    print(f"Debug: RS Score calculated for {len(ticker_rs_sts)} tickers. Total input tickers: {len(price_data)}")
    if len(ticker_rs_sts) == 0:
        print("Debug: No RS scores calculated! Check if SPY data aligns with tickers.")

    return pd.Series(ticker_rs_sts)


def add_rs_sts_to_master_table(master_df: pd.DataFrame, price_data: Dict) -> pd.DataFrame:
    """Add RS_STS% column to the master table."""
    rs_sts = calculate_rs_sts_for_tickers(price_data)
    master_df['rs_sts_pct'] = master_df['ticker'].map(rs_sts)
    master_df['rs_sts_pct'] = master_df['rs_sts_pct'].fillna(0)
    return master_df


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
