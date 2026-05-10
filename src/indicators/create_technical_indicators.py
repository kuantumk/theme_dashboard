"""
Optimized technical indicators calculation.

Only calculates the indicators actually used by screeners and master table.
Uses pandas only - NO TA-Lib required for easier installation!
"""

import numpy as np
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

import src.stock_utils as su
from config.settings import PRICE_DATA_FILE, PRICE_DATA_TA_FILE


def calculate_technical_indicators():
    """
    Calculate only the technical indicators that are actually used.
    Uses pandas only - NO TA-Lib dependency!
    """
    daily_price = su.load_object_from_pickle(PRICE_DATA_FILE)
    daily_tickers = daily_price.keys()

    min_max_lookback = [30, 60, 90, 120, 150, 252]
    dts = [21, 63, 126]
    months = [1, 3, 6]

    # SPX performance for relative performance calculation
    spx = daily_price['^GSPC'].copy(deep=True)
    for month, dt in zip(months, dts):
        spx[f'perf_{month}mo'] = spx['close'] / spx['close'].shift(periods=dt) - 1

    # SPY ATR14 + cumulative normalized change for VARS calculation (computed once)
    spx_high_low = spx['high'] - spx['low']
    spx_high_prev = (spx['high'] - spx['close'].shift(1)).abs()
    spx_low_prev = (spx['low'] - spx['close'].shift(1)).abs()
    spx_tr = pd.concat([spx_high_low, spx_high_prev, spx_low_prev], axis=1).max(axis=1)
    spx_atr14 = spx_tr.rolling(window=14, min_periods=1).mean()
    spx_norm_change = (spx['close'] - spx['close'].shift(1)) / spx_atr14
    spx_cum_norm_100 = spx_norm_change.rolling(window=100, min_periods=1).sum()

    for ticker in tqdm(daily_tickers, desc="Calculating indicators"):
        daily = daily_price[ticker].dropna()

        try:
            # % price change
            daily['price_chg_pct0'] = daily['close'] / daily['close'].shift(periods=1) - 1

            # EMA10, EMA20
            daily['ema10'] = daily['close'].ewm(span=10, adjust=False).mean()
            daily['ema20'] = daily['close'].ewm(span=20, adjust=False).mean()

            # SMAs — require half the window to avoid spurious values for new listings
            daily['sma25'] = daily['close'].rolling(window=25, min_periods=13).mean()
            daily['sma30'] = daily['close'].rolling(window=30, min_periods=15).mean()
            daily['sma50'] = daily['close'].rolling(window=50, min_periods=25).mean()
            daily['sma100'] = daily['close'].rolling(window=100, min_periods=50).mean()
            daily['sma200'] = daily['close'].rolling(window=200, min_periods=100).mean()

            # MIN/MAX lookbacks
            for lookback in min_max_lookback:
                daily[f'min{lookback}'] = daily['low'].rolling(window=lookback, min_periods=max(lookback // 2, 1)).min()
                daily[f'max{lookback}'] = daily['high'].rolling(window=lookback, min_periods=max(lookback // 2, 1)).max()

            # Volume indicators
            daily['vol_sma40'] = daily['volume'].rolling(window=40, min_periods=20).mean()
            daily['vol_sma50'] = daily['volume'].rolling(window=50, min_periods=25).mean()
            daily['vol_sma252'] = daily['volume'].rolling(window=252, min_periods=126).mean()

            # Average dollar volume
            daily['avg_dollar_vol'] = (daily['volume'] * daily['close']).rolling(window=20, min_periods=10).mean()

            # ADR%
            daily['adr_pct'] = (daily['high'] / daily['low']).rolling(window=20, min_periods=10).mean() - 1

            # ATR14 (14-period Average True Range)
            high_low = daily['high'] - daily['low']
            high_prev = (daily['high'] - daily['close'].shift(1)).abs()
            low_prev = (daily['low'] - daily['close'].shift(1)).abs()
            tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
            daily['atr14'] = tr.rolling(window=14, min_periods=1).mean()
            daily['atr_pct'] = daily['atr14'] / daily['close']

            # ATR multiple from the 50-day SMA, matching Project608 parash logic.
            daily['atr_multi_50sma'] = (daily['close'] / daily['sma50'] - 1) / daily['atr_pct']

            # VARS — Volatility-Adjusted Relative Strength vs SPY (lookback 100, ATR 14, EMA 20).
            # Each leg is normalized by its own ATR before summing, so values are comparable across tickers.
            daily['vars_norm_change'] = (daily['close'] - daily['close'].shift(1)) / daily['atr14']
            ticker_cum_norm_100 = daily['vars_norm_change'].rolling(window=100, min_periods=1).sum()
            spx_aligned = spx_cum_norm_100.reindex(daily.index)
            daily['vars'] = ticker_cum_norm_100 - spx_aligned
            daily['vars_20ema'] = daily['vars'].ewm(span=20, adjust=False, min_periods=1).mean()

            # Previous-session fields for gap/no-overlap screeners.
            daily['previous_session_high'] = daily['high'].shift(1)
            daily['previous_session_low'] = daily['low'].shift(1)
            daily['previous_session_volume'] = daily['volume'].shift(1)

            # Inside Day: current bar's range is within previous bar's range
            daily['inside_day'] = (daily['high'] < daily['high'].shift(1)) & (daily['low'] > daily['low'].shift(1))

            # Tight Day: fractional body size (vs close) < 0.2 of ADR%
            daily['tight_day'] = (daily['close'] - daily['open']).abs() / daily['close'] < 0.2 * daily['adr_pct']

            # Close to MAs: close within 0.5 ATR of EMA10 or EMA20
            daily['close_to_ma'] = (
                ((daily['close'] - daily['ema10']).abs() < 0.5 * daily['atr14']) |
                ((daily['close'] - daily['ema20']).abs() < 0.5 * daily['atr14'])
            )

            # Performance metrics
            for month, dt in zip(months, dts):
                daily[f'perf_{month}mo'] = daily['close'] / daily['close'].shift(periods=dt) - 1
                daily[f'rela_perf_{month}mo'] = (1 + daily[f'perf_{month}mo']) / (1 + spx[f'perf_{month}mo'])

            daily_price[ticker] = daily

        except Exception as e:
            print(f"Error for {ticker}: {e}")
            continue

    su.pickle_object_to_file(daily_price, PRICE_DATA_TA_FILE)
    print(f"\nOK Saved technical indicators to {PRICE_DATA_TA_FILE}")

    return daily_price


if __name__ == '__main__':
    calculate_technical_indicators()
