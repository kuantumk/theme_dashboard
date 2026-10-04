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


def compute_ema_pair(close):
    """EMA10 and EMA20 on one close series, as ``(ema10, ema20)``.

    Three producers feed the highlight ladder — this pipeline, the ETF recompute
    and the EP scans — and each used to spell these two formulas out with a
    comment asserting it matched the others. That is the shape this project was
    already bitten by and fixed once: `compute_inside_day` below exists because
    the same definition lived in two places and was one edit away from drifting.
    """
    return (close.ewm(span=10, adjust=False).mean(),
            close.ewm(span=20, adjust=False).mean())


def compute_sma50_full(close):
    """The 50-day mean the highlight ladder reads, over a FULL window only.

    Distinct from the pipeline's `sma50`, which settles for 25 bars because
    `atr_multi_50sma`, `dist_sma50_pct` and the screeners want it that way. On a
    listing with 25 to 49 sessions that column holds a partial mean wearing a
    50-day label — finite and positive, so the ladder's zero-as-missing rule
    cannot see it, and the stock scores a stacked trend it has no window to
    support. Below 50 bars this returns NaN, which every consumer reads as absent.
    """
    return close.rolling(window=50, min_periods=50).mean()


def compute_inside_day(open_, high, low, close):
    """Return the inside-day flag: candle engulfed OR body engulfed.

        (high <= prev_high and low >= prev_low)
          or (body_top <= prev_body_top and body_bottom >= prev_body_bottom)

    where body_top/bottom are max/min of (open, close), so the comparison is
    direction-agnostic — a red previous bar reads the same as a green one.

    Both clauses are inclusive. The earlier strict form (`high < prev_high and
    low > prev_low`) rejected a bar that ties the prior high or low, and rejected
    a tight-bodied bar whose wicks poke outside the prior range — both of which
    are the tight setups the green day-pattern colouring exists to surface.

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


# Tight range (TR) tunables. Defaults match the canonical note (Tight Range
# v2.1); config/workflow_config.yaml `tight_range:` overrides them at run time.
TIGHT_MIN_WINDOW = 3          # shortest window of closes tested
TIGHT_MAX_WINDOW = 10         # longest; caps chaining (see compute_tight_range)
TIGHT_RATIO_MAX = 0.35        # tightness ratio at or below this passes
TIGHT_PCTILE_MAX = 15.0       # ...or today's ratio sits in this own-history percentile
TIGHT_PCTILE_LOOKBACK = 120   # sessions of own ratios the percentile reads
TIGHT_MIN_HISTORY = 20        # bars of ADR and of ratio history before anything passes
TIGHT_MA_RISING_LAG = 5       # an MA is rising when above its value this many sessions ago
TIGHT_MA_HOLD_ADR = 0.6       # MA-hold: window's lowest close within this many ADRs of the MA
TIGHT_EMA_ROLLOVER_ADR = 0.1  # MA-hold: EMA10 at most this many ADRs below EMA20
TIGHT_SUPPORT_ADR = 1.0       # support: window's lowest close within this many ADRs of a swing low
TIGHT_SWING_HALF_WIDTH = 10   # swing low = lowest low within this many bars either side
TIGHT_SWING_MIN_AGE = 5       # swing low formed at least this many sessions before the window
TIGHT_SWING_MAX_AGE = 60      # ...and at most this many

#: The gate branches, in the order `tight_gate` reports them. The first branch
#: that holds is the one published.
TIGHT_GATES = ('trend', 'ma_hold', 'support')


def compute_tight_ratio(close, adr_pct, window):
    """The tightness ratio of the last ``window`` closes.

        close range = (max(close) - min(close)) / mean(close)
        ratio       = close range / (ADR% * sqrt(window))

    Lower is tighter. Read it as "what fraction of its normal travel did the
    stock actually move". A 0.25 means the closes covered about a quarter of
    the distance a stock with this ADR would drift over ``window`` days.

    **Closes, not bodies or high-low.** Wicks are allowed: long wicks with tight
    closes are the equilibrating behaviour a tight range is. Bodies stretch on
    opens and gaps. Closes are where each day settled.

    **Divide by sqrt(window).** Random drift grows with the square root of
    time, so this puts a 3-day and an 8-day window on one scale.

    ADR% is read at the window's last bar, as the note specifies. The window's
    own quiet days pull it down a little, which makes the ratio slightly harder
    to pass. Do not switch to a pre-window ADR without retesting.

    A missing or non-positive ADR% yields NaN rather than dividing to
    infinity, and ``min_periods=window`` voids a partial window.
    """
    adr = pd.to_numeric(adr_pct, errors='coerce')
    hi = close.rolling(window, min_periods=window).max()
    lo = close.rolling(window, min_periods=window).min()
    mid = close.rolling(window, min_periods=window).mean()
    ratio = (hi - lo) / mid.where(mid > 0) / (adr.where(adr > 0) * np.sqrt(window))
    return ratio.replace([np.inf, -np.inf], np.nan).astype(float)


def _swing_low_support(low, close, adr, rows, lengths,
                       half_width, min_age, max_age, support_adr):
    """The support branch of the gate, for the given rows only.

    A **major swing low** is a bar whose low is the lowest low within
    ``half_width`` bars either side, formed ``min_age`` to ``max_age``
    sessions before the window starts. The branch holds when the window's
    lowest close is within ``support_adr`` ADRs of such a low, or when a low
    inside the window undercuts it and the last close is back above it
    (undercut and rally).

    ⛔ The right side of the swing test stops at the window's last bar. The
    note's reference slices ``L[i-10 : i+11]`` against the full series, which
    on a short window reads up to five bars past the session being scored.
    That is look-ahead on every back-dated session. Truncating at the last bar
    gives a back-dated session the answer it would have given live.

    A Python loop, but only over rows whose shape passes and whose trend and
    MA-hold branches both fail, so it runs on a small share of bars.
    """
    n = len(low)
    out = np.zeros(len(rows), dtype=bool)
    if not len(rows):
        return out
    # Centred min over the full +/- half_width window. Valid for any swing
    # candidate whose right side ends at or before the scored bar.
    cmin = pd.Series(low).rolling(2 * half_width + 1, center=True,
                                  min_periods=1).min().to_numpy()
    for k, (b, length) in enumerate(zip(rows, lengths)):
        a_idx = b - length + 1
        p_hi = a_idx - min_age
        p_lo = max(0, a_idx - max_age)
        if p_hi < p_lo:
            continue
        ps = np.arange(p_lo, p_hi + 1)
        full = ps + half_width <= b
        swing = np.zeros(len(ps), dtype=bool)
        swing[full] = low[ps[full]] <= cmin[ps[full]]
        for j in np.flatnonzero(~full):
            p = ps[j]
            swing[j] = low[p] <= low[max(0, p - half_width):b + 1].min()
        levels = low[ps[swing]]
        if not len(levels):
            continue
        lo_close = close[a_idx:b + 1].min()
        lo_low = low[a_idx:b + 1].min()
        last = close[b]
        hold = np.abs(lo_close / levels - 1) <= support_adr * adr[b]
        reclaim = (lo_low < levels) & (levels < last)
        out[k] = bool((hold | reclaim).any())
    return out


def compute_tight_range(low, close, adr_pct, ema10, ema20, sma50,
                        min_window=TIGHT_MIN_WINDOW,
                        max_window=TIGHT_MAX_WINDOW,
                        ratio_max=TIGHT_RATIO_MAX,
                        pctile_max=TIGHT_PCTILE_MAX,
                        pctile_lookback=TIGHT_PCTILE_LOOKBACK,
                        min_history=TIGHT_MIN_HISTORY,
                        ma_rising_lag=TIGHT_MA_RISING_LAG,
                        ma_hold_adr=TIGHT_MA_HOLD_ADR,
                        ema_rollover_adr=TIGHT_EMA_ROLLOVER_ADR,
                        support_adr=TIGHT_SUPPORT_ADR,
                        swing_half_width=TIGHT_SWING_HALF_WIDTH,
                        swing_min_age=TIGHT_SWING_MIN_AGE,
                        swing_max_age=TIGHT_SWING_MAX_AGE):
    """Tight range (TR): shape + place, evaluated at every bar.

    A tight range is a stretch of 3 to 10 sessions in which the closes barely
    move relative to how far this stock normally travels, forming somewhere
    constructive. Two layers, both required:

    1. **Shape.** For each window length N, the tightness ratio
       (`compute_tight_ratio`) passes when it is at or below ``ratio_max``
       OR when it sits in the tightest ``pctile_max`` percent of this stock's
       own ratios for the same N over the last ``pctile_lookback`` sessions.
       The **longest** passing N is reported — length is information.
    2. **Gate.** Pass if ANY branch holds, tested in this order:
       - ``trend``: EMA10 >= EMA20 and the last close >= EMA20.
       - ``ma_hold``: one of EMA10 / EMA20 / SMA50 is rising (above its value
         ``ma_rising_lag`` sessions ago), the window's lowest close is within
         ``ma_hold_adr`` ADRs of it, the last close is ABOVE it, and EMA10 is
         at most ``ema_rollover_adr`` ADRs below EMA20. The rising MA must be
         underfoot, not overhead.
       - ``support``: see `_swing_low_support`.

    The note's third layer (quality: RS in a correction, theme, float, short
    interest) ranks survivors and never filters, so it is not here. Its
    context tags (momentum pause, MA hug, ...) are not here either.

    Each bar answers "does a tight window END here". Consecutive passing days
    are not clustered: a dashboard reads one session at a time, and the note's
    clustering is for picking one window out of a historical run.

    ``sma50`` must be a FULL 50-bar mean (the pipeline passes `sma50_full`).
    The first ``min_history`` bars never pass: ADR there is unreliable and the
    percentile has nothing to compare against.

    Returns a DataFrame aligned to the input with:

    - ``tightness``: the ratio of the reported window; when no window passes,
      the lowest ratio across all N. NaN when ADR is unavailable.
    - ``tight_len``: the reported N, or 0 when the shape fails.
    - ``tight_pctile``: own-history percentile of the reported ratio, or NaN.
    - ``tight_gate``: the first gate branch that holds, or '' (shape must pass).
    - ``tight_range``: shape AND gate. Fails closed: missing data is False.
    """
    idx = close.index
    n = len(close)
    c = pd.to_numeric(close, errors='coerce')
    adr = pd.to_numeric(adr_pct, errors='coerce')
    warm = pd.Series(np.arange(n) >= min_history - 1, index=idx)

    lengths = np.arange(int(min_window), int(max_window) + 1)
    ratios = np.full((n, len(lengths)), np.nan)
    pctiles = np.full((n, len(lengths)), np.nan)
    lows = np.full((n, len(lengths)), np.nan)
    for j, length in enumerate(lengths):
        r = compute_tight_ratio(c, adr, int(length)).where(warm)
        ratios[:, j] = r.to_numpy()
        pctiles[:, j] = (r.rolling(int(pctile_lookback), min_periods=int(min_history))
                         .rank(method='max', pct=True) * 100).to_numpy()
        lows[:, j] = c.rolling(int(length), min_periods=int(length)).min().to_numpy()

    with np.errstate(invalid='ignore'):
        passing = (ratios <= ratio_max) | (pctiles <= pctile_max)
    shape = passing.any(axis=1)
    # Longest passing N: the last True column.
    last_col = len(lengths) - 1 - np.argmax(passing[:, ::-1], axis=1)
    rows = np.arange(n)
    chosen_ratio = ratios[rows, last_col]
    tightest = pd.DataFrame(ratios).min(axis=1).to_numpy()  # skips NaN
    tightness = np.where(shape, chosen_ratio, tightest)
    tight_len = np.where(shape, lengths[last_col], 0)
    tight_pctile = np.where(shape, pctiles[rows, last_col], np.nan)
    lo_close = pd.Series(np.where(shape, lows[rows, last_col], np.nan), index=idx)

    # ---- gate ----
    e10 = pd.to_numeric(ema10, errors='coerce')
    e20 = pd.to_numeric(ema20, errors='coerce')
    s50 = pd.to_numeric(sma50, errors='coerce')
    trend = ((e10 >= e20) & (c >= e20)).to_numpy()

    not_rolled = ((e10 / e20 - 1) / adr >= -ema_rollover_adr)
    held = pd.Series(False, index=idx)
    for ma in (e10, e20, s50):
        rising = ma > ma.shift(int(ma_rising_lag))
        near = (lo_close / ma - 1).abs() <= ma_hold_adr * adr
        above = c > ma
        held = held | (rising & near & above)
    ma_hold = (held & not_rolled).fillna(False).to_numpy(dtype=bool)

    need_support = np.flatnonzero(shape & ~trend & ~ma_hold)
    support = np.zeros(n, dtype=bool)
    support[need_support] = _swing_low_support(
        pd.to_numeric(low, errors='coerce').to_numpy(dtype=float),
        c.to_numpy(dtype=float), adr.to_numpy(dtype=float),
        need_support, tight_len[need_support],
        int(swing_half_width), int(swing_min_age), int(swing_max_age),
        float(support_adr))

    gate = np.where(trend, 'trend',
                    np.where(ma_hold, 'ma_hold',
                             np.where(support, 'support', '')))
    gate = np.where(shape, gate, '')
    tight = shape & (gate != '')

    return pd.DataFrame({
        'tightness': tightness.astype(float),
        'tight_len': tight_len.astype(int),
        'tight_pctile': tight_pctile.astype(float),
        'tight_gate': gate.astype(object),
        'tight_range': tight.astype(bool),
    }, index=idx)


def tight_range_config(config=None):
    """The `compute_tight_range` keyword arguments, defaults overlaid by config.

    Reads the `tight_range:` block of config/workflow_config.yaml. An unknown
    key raises: a misspelled tunable would otherwise be ignored, and the flag
    would keep its default with nothing on screen to show the edit did nothing.
    """
    cfg = {
        'min_window': TIGHT_MIN_WINDOW,
        'max_window': TIGHT_MAX_WINDOW,
        'ratio_max': TIGHT_RATIO_MAX,
        'pctile_max': TIGHT_PCTILE_MAX,
        'pctile_lookback': TIGHT_PCTILE_LOOKBACK,
        'min_history': TIGHT_MIN_HISTORY,
        'ma_rising_lag': TIGHT_MA_RISING_LAG,
        'ma_hold_adr': TIGHT_MA_HOLD_ADR,
        'ema_rollover_adr': TIGHT_EMA_ROLLOVER_ADR,
        'support_adr': TIGHT_SUPPORT_ADR,
        'swing_half_width': TIGHT_SWING_HALF_WIDTH,
        'swing_min_age': TIGHT_SWING_MIN_AGE,
        'swing_max_age': TIGHT_SWING_MAX_AGE,
    }
    block = (CONFIG if config is None else config).get('tight_range', {}) or {}
    unknown = set(block) - set(cfg)
    if unknown:
        raise ValueError(f"tight_range: unknown key(s) {sorted(unknown)}")
    cfg.update(block)
    return cfg


HIGHLIGHT_SHORT_FLOOR = 20.0  # percent of float; chosen, never measured

#: Every value `compute_highlight_tier` can return, in ladder order. The
#: browser's class and tooltip tables key off these exact strings, so renaming
#: one here would blank that rung's tint and wording on every tab while both
#: sides still looked internally consistent.
#: `tests/test_dashboard_highlight_markup.py` pins the two vocabularies together.
HIGHLIGHT_TIERS = ('tight', 'short', 'ma_up', 'ma_split')


def _highlight_flag(value):
    """Read a rung's boolean input. Anything that is not plainly true is False.

    `bool(float('nan'))` is True, so a bare truth test would fire the tight
    rung on every ticker whose tight-range columns are absent. A missing flag fails
    closed instead.
    """
    try:
        if value is None or pd.isna(value):
            return False
        return bool(value)
    except (TypeError, ValueError):
        return False


def _highlight_number(value):
    """Read a rung's numeric input, or None when the value says nothing."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _highlight_price(value):
    """Read a price-scale input. Zero and below read as missing.

    The snapshot builders call `.fillna(0)`, so an absent `sma50` arrives as
    `0.0`. Every average sits above zero, so a bare comparison would report a
    stacked trend on a stock that has none. A price of zero is impossible for a
    real security, which makes zero a safe sentinel for absent. See
    `docs/solutions/logic-errors/nan-defeats-numeric-guard-chains.md`.
    """
    number = _highlight_number(value)
    return number if number is not None and number > 0 else None


def compute_highlight_tier(tight_range=None, short_interest=None,
                           ema10=None, ema20=None, sma50=None):
    """Which one highlight a ticker earns: 'tight', 'short', 'ma_up',
    'ma_split', or None.

    The ladder runs in that order and stops at the first rung the ticker
    satisfies:

    1. ``tight_range`` is true — the same state the Themes tab tints.
    2. ``short_interest >= 20`` percent of float.
    3. ``ema10 > ema20 > sma50``.
    4. ``ema10 > ema20`` and ``ema20`` is not above ``sma50``.

    **The order is a display preference, not a ranking claim.** Nothing
    measures whether a tight range predicts better than a crowded short, or either
    better than a stacked average. The 20% floor is chosen as well: the SI tab
    gates its roster at 12% and the EP screener at 10%, so three numbers
    describe one idea and none is calibrated.

    **A rung whose input is missing is skipped, and the ladder carries on.** So
    a tier names the highest rung whose input the CALLER HOLDS, never the
    absence of a higher one. Most Themes chips have no short-interest row, so a
    crowded short there reads as stacked on every session — 'ma_up' must not be
    read as "stacked and not crowded".

    Rung 4 states where the averages sit and nothing more. Two opposite trades
    produce that same reading, and this rung separates neither. Do not give it a
    direction in the code, in a comment, or in a tooltip.

    Rung 4 needs only the ``ema20`` against ``sma50`` comparison, because rung 3
    has already established ``ema10 > ema20``. The request stated it as
    ``ema20 < sma50 or ema10 < sma50``; with ``ema10 > ema20`` true,
    ``ema10 < sma50`` forces ``ema20 < sma50``, so the second clause adds
    nothing. Equal averages fall to rung 4. `tests/test_highlight_tier.py` pins
    the reduced form against the request's original wording.

    Takes plain scalars rather than a frame row, because the ETF recompute and
    the EP scans hold neither. Shared with every producer for the same reason
    `compute_inside_day` is — one definition cannot drift from itself.
    """
    if _highlight_flag(tight_range):
        return 'tight'

    short = _highlight_number(short_interest)
    if short is not None and short >= HIGHLIGHT_SHORT_FLOOR:
        return 'short'

    fast = _highlight_price(ema10)
    slow = _highlight_price(ema20)
    base = _highlight_price(sma50)
    # Both moving-average rungs read sma50, so one absent average answers
    # neither of them.
    if fast is None or slow is None or base is None:
        return None
    if fast <= slow:
        return None
    return 'ma_up' if slow > base else 'ma_split'


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

    min_max_lookback = [30, 50, 60, 90, 120, 150, 252]
    dts = [21, 63, 126, 252]
    months = [1, 3, 6, 12]

    # SPX performance for relative performance calculation
    spx = daily_price['^GSPC'].copy(deep=True)
    for month, dt in zip(months, dts):
        spx[f'perf_{month}mo'] = spx['close'] / spx['close'].shift(periods=dt) - 1

    # SPY ATR14 + cumulative normalized change for VARS calculation (computed once)
    spy_cum_norm_100 = compute_spy_cum_norm_100(daily_price['SPY'])

    # Tight-range tunables, read once. `compute_tight_range` keeps module-level
    # defaults so tests pin behaviour without reaching into config.
    _tight_cfg = tight_range_config()

    for ticker in tqdm(daily_tickers, desc="Calculating indicators"):
        daily = daily_price[ticker].dropna()

        try:
            # % price change
            daily['price_chg_pct0'] = daily['close'] / daily['close'].shift(periods=1) - 1

            # EMA10, EMA20
            daily['ema10'], daily['ema20'] = compute_ema_pair(daily['close'])

            # SMAs — require half the window to avoid spurious values for new listings
            daily['sma25'] = daily['close'].rolling(window=25, min_periods=13).mean()
            daily['sma30'] = daily['close'].rolling(window=30, min_periods=15).mean()
            daily['sma50'] = daily['close'].rolling(window=50, min_periods=25).mean()
            # The highlight ladder's third input. See `compute_sma50_full` for
            # why it cannot share the 25-bar `sma50` above.
            daily['sma50_full'] = compute_sma50_full(daily['close'])
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

            # Tight range: shape + gate. The columns ride the master table, so
            # a back-dated session reports the flag as it stood then — unlike
            # the day-pattern colouring, which reads only the last bar. The
            # gate's SMA50 is the full 50-bar mean, never the 25-bar `sma50`.
            tr = compute_tight_range(
                daily['low'], daily['close'], daily['adr_pct'],
                daily['ema10'], daily['ema20'], daily['sma50_full'], **_tight_cfg)
            for col in tr.columns:
                daily[col] = tr[col]

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
