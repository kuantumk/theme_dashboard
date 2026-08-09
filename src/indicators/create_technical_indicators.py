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


def compute_spy_cum_norm_100(spy_df):
    """Return the 100-session rolling sum of SPY's ATR14-normalized daily change.

    Shared by stock VARS (calculate_technical_indicators) and ETF VARS
    (export_dashboard_data.fetch_etf_metrics) so the baseline can't drift.
    """
    high_low = spy_df['high'] - spy_df['low']
    high_prev = (spy_df['high'] - spy_df['close'].shift(1)).abs()
    low_prev = (spy_df['low'] - spy_df['close'].shift(1)).abs()
    tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
    atr14 = tr.rolling(window=14, min_periods=1).mean()
    norm_change = (spy_df['close'] - spy_df['close'].shift(1)) / atr14
    return norm_change.rolling(window=100, min_periods=1).sum()


def compute_inside_day(open_, high, low, close):
    """Return the inside-day flag: candle engulfed OR body engulfed.

        (high <= prev_high and low >= prev_low)
          or (body_top <= prev_body_top and body_bottom >= prev_body_bottom)

    where body_top/bottom are max/min of (open, close), so the comparison is
    direction-agnostic — a red previous bar reads the same as a green one.

    Both clauses are inclusive. The earlier strict form (`high < prev_high and
    low > prev_low`) rejected a bar that ties the prior high or low, and rejected
    a tight-bodied bar whose wicks poke outside the prior range — both of which
    are the coiled setups the green day-pattern colouring exists to surface.

    Takes four Series rather than a frame so the caller's column naming is its
    own business: the pipeline passes lowercase OHLC, the dashboard's ETF
    recompute passes yfinance's capitalized columns. Shared with
    `export_dashboard_data.fetch_etf_metrics` for the same reason
    `compute_spy_cum_norm_100` is — one definition cannot drift from itself.

    The first bar has no predecessor and is never an inside day.
    """
    prev_high = high.shift(1)
    prev_low = low.shift(1)
    range_engulf = (high <= prev_high) & (low >= prev_low)

    bodies = pd.concat([open_, close], axis=1)
    body_top = bodies.max(axis=1)
    body_bottom = bodies.min(axis=1)
    body_engulf = (
        (body_top <= body_top.shift(1)) & (body_bottom >= body_bottom.shift(1))
    )

    return (range_engulf | body_engulf).astype(bool)


VOL_SPIKE_WINDOW = '365D'  # trailing 1-year (calendar) lookback for volume-spike detection


def _days_since_window_high(series, index, window=VOL_SPIKE_WINDOW):
    """Calendar days since the most recent bar that printed a high of the trailing ``window``.

    ``window`` is a pandas time offset (e.g. ``'365D'``). A bar counts as a window-high when
    its value equals the trailing-window rolling max as of that bar, so a stock's record
    volume that has aged out of the window no longer suppresses a fresh in-window spike.
    Point-in-time safe (rolling only looks back) and fully vectorized.

    Returns ``(days_since, rolling_max)``.
    """
    roll_max = series.rolling(window, min_periods=1).max()
    is_high = (series >= roll_max).to_numpy()
    pos = np.where(is_high, np.arange(len(index)), np.nan)
    last_pos = pd.Series(pos, index=index).ffill().fillna(0).astype(int)
    last_high_date = index[last_pos.to_numpy()]
    return (index - last_high_date).days.to_numpy(), roll_max


def calculate_technical_indicators():
    """
    Calculate only the technical indicators that are actually used.
    Uses pandas only - NO TA-Lib dependency!
    """
    daily_price = su.load_object_from_pickle(PRICE_DATA_FILE)
    daily_tickers = daily_price.keys()

    min_max_lookback = [30, 60, 90, 120, 150, 252]
    dts = [21, 63, 126, 252]
    months = [1, 3, 6, 12]

    # SPX performance for relative performance calculation
    spx = daily_price['^GSPC'].copy(deep=True)
    for month, dt in zip(months, dts):
        spx[f'perf_{month}mo'] = spx['close'] / spx['close'].shift(periods=dt) - 1

    # SPY ATR14 + cumulative normalized change for VARS calculation (computed once)
    spy_cum_norm_100 = compute_spy_cum_norm_100(daily_price['SPY'])

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

            # Volume-spike indicators (trailing 365-calendar-day window).
            # highest_volume = today's volume is the highest in the trailing ~1 year.
            # up_dollar_vol_max = trailing-1yr max of signed dollar volume (up days positive).
            daily['price_up_down'] = np.where(daily['price_chg_pct0'] > 0, 1, -1)
            vol = daily['volume']
            days_since_vol, vol_roll_max = _days_since_window_high(vol, daily.index)
            daily['highest_volume'] = vol >= vol_roll_max
            daily['days_since_highest_volume'] = days_since_vol
            daily['days_since_vol_max'] = days_since_vol  # denvol alias (same series)
            up_dollar_vol = daily['volume'] * daily['price_up_down'] * daily['close']
            days_since_udv, udv_roll_max = _days_since_window_high(up_dollar_vol, daily.index)
            daily['up_dollar_vol_max'] = udv_roll_max
            daily['days_since_up_vol_max'] = days_since_udv

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
            spy_aligned = spy_cum_norm_100.reindex(daily.index)
            daily['vars'] = ticker_cum_norm_100 - spy_aligned
            daily['vars_20ema'] = daily['vars'].ewm(span=20, adjust=False, min_periods=1).mean()

            # Previous-session fields for gap/no-overlap screeners.
            daily['previous_session_high'] = daily['high'].shift(1)
            daily['previous_session_low'] = daily['low'].shift(1)
            daily['previous_session_volume'] = daily['volume'].shift(1)

            # Inside Day: candle engulfed by the previous candle, or body engulfed
            # by the previous body. See compute_inside_day for why both clauses.
            daily['inside_day'] = compute_inside_day(
                daily['open'], daily['high'], daily['low'], daily['close'])

            # Tight Day: fractional body size (vs close) < 0.2 of ADR%
            daily['tight_day'] = (daily['close'] - daily['open']).abs() / daily['close'] < 0.2 * daily['adr_pct']

            # Close to MAs: close within 0.5 ATR of EMA10 or EMA20
            daily['close_to_ma'] = (
                ((daily['close'] - daily['ema10']).abs() < 0.5 * daily['atr14']) |
                ((daily['close'] - daily['ema20']).abs() < 0.5 * daily['atr14'])
            )

            # Coiled-theme reusable setup features.
            # Retained so the standalone `coiled_theme` screener (kept but no longer
            # in the daily workflow) still has its time-series inputs precomputed.
            daily['range_pct'] = (daily['high'] - daily['low']) / daily['close']
            daily['range10_pct'] = (
                daily['high'].rolling(window=10, min_periods=5).max()
                - daily['low'].rolling(window=10, min_periods=5).min()
            ) / daily['close']
            daily['range20_pct'] = (
                daily['high'].rolling(window=20, min_periods=10).max()
                - daily['low'].rolling(window=20, min_periods=10).min()
            ) / daily['close']
            daily['range_contraction_10_20'] = daily['range10_pct'] / daily['range20_pct']
            daily['vol_dry_10_50'] = daily['volume'].rolling(window=10, min_periods=5).mean() / daily['vol_sma50']
            daily['dist_sma50_pct'] = daily['close'] / daily['sma50'] - 1
            daily['close_vs_252h'] = daily['close'] / daily['max252']
            daily['nr7'] = daily['range_pct'] <= daily['range_pct'].rolling(window=7, min_periods=7).min()
            daily['nr20'] = daily['range_pct'] <= daily['range_pct'].rolling(window=20, min_periods=20).min()

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
