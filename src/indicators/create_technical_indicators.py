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
from config.settings import CONFIG, PRICE_DATA_FILE, PRICE_DATA_TA_FILE


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

    # skipna=False is load-bearing: pandas would otherwise treat a NaN open as
    # "just use close", collapsing the body to a single point and returning a
    # confident verdict computed from half a quote — for this bar and, via the
    # shift below, the next one. The pipeline dropna's whole rows, but the
    # dashboard's ETF path only drops rows missing Close, so a NaN open with a
    # valid close does reach here. Propagating NaN makes both clauses False,
    # which is the honest answer: unknown, so don't flag it.
    bodies = pd.concat([open_, close], axis=1)
    body_top = bodies.max(axis=1, skipna=False)
    body_bottom = bodies.min(axis=1, skipna=False)
    body_engulf = (
        (body_top <= body_top.shift(1)) & (body_bottom >= body_bottom.shift(1))
    )

    return (range_engulf | body_engulf).astype(bool)


TIGHTNESS_WINDOW = 3        # sessions in the closing range
TIGHTNESS_FRACTION = 0.30   # tightness at or below this is "tight"
TIGHTNESS_HIGH_LOOKBACK = 50  # sessions in the period high the broken-chart gate reads
TIGHTNESS_HIGH_FRAC = 0.70  # close must hold at least this share of that high


def compute_tightness(close, adr_pct, window=TIGHTNESS_WINDOW):
    """How wide a band the last ``window`` closes sat in, measured in ADR units.

        (max(close) - min(close)) / mean(close) / adr_pct

    Lower is tighter. A stock whose three closes span half its average daily
    range reads 0.5; one that barely moves reads near 0.

    **This is a containment measure, not a per-bar one, and the distinction is
    the point.** An earlier version averaged each bar's |close-to-close change|
    over the window. That answers "were the daily moves small", which is not
    the same question: a stock oscillating a full ADR up and down and closing
    where it started has small containment and large per-bar movement. A trader
    reading a chart sees the band, so the band is what this measures. The two
    correlate at +0.78, so this is a legibility choice more than a performance
    one — do not reintroduce the per-bar form on the grounds that it scores
    about the same.

    A bar with a missing or non-positive ADR% yields NaN rather than dividing
    to infinity, and ``min_periods=window`` voids a partial window — an
    incomplete base must not report as a complete one.
    """
    adr = pd.to_numeric(adr_pct, errors='coerce')
    hi = close.rolling(window, min_periods=window).max()
    lo = close.rolling(window, min_periods=window).min()
    mid = close.rolling(window, min_periods=window).mean()
    rng = (hi - lo) / mid.where(mid > 0) / adr.where(adr > 0)
    return rng.replace([np.inf, -np.inf], np.nan).astype(float)


def compute_tight_base(tightness, close, period_high,
                       fraction=TIGHTNESS_FRACTION,
                       high_frac=TIGHTNESS_HIGH_FRAC):
    """A tight closing range on a chart that has not broken down.

    Two conditions, and deliberately only two:

    1. ``tightness <= fraction`` — the closing range is narrow. This is the
       measure; nothing else here is.
    2. ``close >= high_frac * period_high`` — the stock is not more than
       ``1 - high_frac`` off its period high. A **disqualifier, not a
       selector**: at 0.70 it passes 82% of rows on its own, so it chooses
       nothing and only throws out wreckage. That removal is what makes the
       flag worth rendering — measured over 165 sessions of 2026, forward
       10-session excess runs 0.66pp *below* the universe baseline for bare
       tightness and above it once this gate applies.

    ⛔ **There is no moving-average test, and adding one back is a mistake
    already made.** An earlier version required the close to sit within
    0.5 ATR of the EMA10 or EMA20. EMA distance is not tightness — it is a
    second location test doing a job the closing range already does, since a
    stock drifting away from its averages has a wide closing range by
    construction. It also ejected names on rounding: PANW missed by $1.33 on a
    $330 stock. Swept across 0.5 to 1.5 ATR the threshold moved ticker-level
    excess by 0.08pp and theme IC by 0.026, both inside noise, so it was never
    earning its complexity either.

    ``period_high`` is a rolling max of **highs** (the pipeline passes
    ``max50``), not of closes. An earlier calibration used closes, which are
    strictly lower, and its 0.90 threshold rejected every name in the case this
    was built for. Do not re-derive a threshold from a close-based high.

    Both comparisons are inclusive, matching `compute_inside_day`: a bar that
    exactly ties the threshold is the setup, not a near-miss.

    Any NaN input yields False rather than pandas NA. The flag asserts that a
    base happened, so it fails closed — a stock with no measurable history has
    proved nothing, and an NA would read as truthy downstream.
    """
    tight = pd.to_numeric(tightness, errors='coerce') <= fraction
    holding = pd.to_numeric(close, errors='coerce') >= (
        high_frac * pd.to_numeric(period_high, errors='coerce'))
    return (tight & holding).fillna(False).astype(bool)


DROP_WINDOW = 15    # sessions in the drawdown window
DROP_LOOKBACK = 45  # sessions searched for the worst such window


def compute_drop_15d(close, window=DROP_WINDOW, lookback=DROP_LOOKBACK):
    """Worst drawdown over any stretch of ``window`` sessions or fewer, found
    anywhere inside the trailing ``lookback`` sessions.

    ⛔ The per-bar form — today's close against its own trailing high — is not
    a substitute, and swapping to it shrinks the SI tab silently. Measured over
    183 heavily shorted names on 2026-09-10, it correlates **0.37** with a true
    window search, against **0.914** for this rolling minimum. It reads AEHR at
    **-22.5%** where the real figure is **-47.6%**, so AEHR fails the -25% gate
    that was calibrated on AEHR.

    The reason is the question being asked. A name that fell 50% three weeks
    ago and has gone flat since is still a broken chart, and today's bar cannot
    see that. `tests/test_drop_15d.py` pins the lower-bound property.

    ``min_periods`` stays at 1 on the outer roll so a recent listing scores
    rather than reading NaN, matching how `vars` treats short histories.
    """
    per_bar = close / close.rolling(window + 1, min_periods=2).max() - 1
    return per_bar.rolling(lookback, min_periods=1).min()


def compute_down_streak(close):
    """Consecutive down closes ending at each bar.

    A flat close breaks the streak — an unchanged close is not a down day.

    Display only. Never gate on it: a name that fell 40% in three gap-downs
    carries a streak of 1 and is exactly what the SI tab is looking for.
    """
    down = close.diff() < 0
    # Each unbroken run of down days shares one (~down).cumsum() group id, so
    # a cumulative sum inside the group counts that run and resets after it.
    return down.groupby((~down).cumsum()).cumsum().astype(int)


def compute_max_down_streak(close, lookback=DROP_LOOKBACK):
    """Longest run of consecutive down closes ending inside the trailing
    ``lookback`` sessions.

    The running streak is the wrong figure to show. Measured 2026-09-10, AEHR,
    AMKR and COHU all read 1 — the run ending today — while AEHR's actual
    slide was 11 sessions. A column that reads 1 for every row answers no
    question. This reports the slide that happened, which is what the tab
    claims to find.

    Display only, like the streak it is built from. Never gate on it.
    """
    return compute_down_streak(close).rolling(lookback, min_periods=1).max().astype(int)


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

    # 50 is here for the tight-base location gate (config: tightness.high_lookback).
    min_max_lookback = [30, 50, 60, 90, 120, 150, 252]
    dts = [21, 63, 126, 252]
    months = [1, 3, 6, 12]

    # SPX performance for relative performance calculation
    spx = daily_price['^GSPC'].copy(deep=True)
    for month, dt in zip(months, dts):
        spx[f'perf_{month}mo'] = spx['close'] / spx['close'].shift(periods=dt) - 1

    # SPY ATR14 + cumulative normalized change for VARS calculation (computed once)
    spy_cum_norm_100 = compute_spy_cum_norm_100(daily_price['SPY'])

    # Tightness tunables, read once. The helpers keep module-level defaults so
    # tests pin behaviour without reaching into config.
    _tight_cfg = {
        'window': TIGHTNESS_WINDOW,
        'fraction': TIGHTNESS_FRACTION,
        'high_lookback': TIGHTNESS_HIGH_LOOKBACK,
        'high_frac': TIGHTNESS_HIGH_FRAC,
    }
    _tight_cfg.update(CONFIG.get('tightness', {}) or {})
    _high_col = f"max{int(_tight_cfg['high_lookback'])}"
    if int(_tight_cfg['high_lookback']) not in min_max_lookback:
        # Raise rather than fall back: a silently substituted lookback changes
        # what the flag means with nothing on screen to show it moved.
        raise ValueError(
            f"tightness.high_lookback={_tight_cfg['high_lookback']} has no "
            f"{_high_col} column; add it to min_max_lookback.")

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

            # Tightness + the located tight-base flag. Both ride the master
            # table, so a back-dated session reports the flag as it stood then
            # — unlike the day-pattern colouring, which reads only the last bar.
            # See compute_tight_base for why the location conjuncts are not
            # optional (bare tightness measures the wrong way round).
            daily['tightness'] = compute_tightness(
                daily['close'], daily['adr_pct'], window=_tight_cfg['window'])
            daily['tight_base'] = compute_tight_base(
                daily['tightness'], daily['close'], daily[_high_col],
                fraction=_tight_cfg['fraction'], high_frac=_tight_cfg['high_frac'])

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

            # SI-tab selloff legs. See compute_drop_15d for why the rolling
            # minimum is not interchangeable with today's bar.
            daily['drop_15d'] = compute_drop_15d(daily['close'])
            daily['max_down_streak'] = compute_max_down_streak(daily['close'])
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
