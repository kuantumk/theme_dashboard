"""EMA20 cross setups: the 20EMA tab's short and long lists.

A setup starts on the session a close crosses EMA20 after spending most of
the prior sessions on the other side. EMA10 then crosses EMA20 the same way,
and price pulls back to the EMAs. The setup stays live while the EMAs stay
crossed. It ends on a close more than `overshoot_atr` through EMA20, or when
EMA10 crosses back. Before the EMAs first cross there is nothing to hold it
open, so that stage alone ends after `pre_cross_sessions`.

Each list is a night-before list: a name is on it the session BEFORE it can
trigger, so it can be traded on the next session.

Short: a previous leader that lost EMA20. The stock spent at least
`short_min_lead_sessions` of the `lead_window` sessions before the cross in
the top `1 - lead_pct` of `lead_return_sessions`-session returns, is down at
least `short_min_drawdown` from its high over the same window, and today's
high came within `short_zone_atr` of EMA20 from below.

Long: a name in a theme where several companies are setting up at once. At
least `long_min_theme_names` tickers of one L2 theme hold a live long setup
no older than `long_fresh_sessions`. The candidate pulled back to EMA10
today, sits within `long_max_drawdown` of its high, has a rising SMA50 and
ranks in the top `1 - long_min_rs20` of 20-session returns.

Measured from 2025-10-01 to 2026-10-09 with this code on the pipeline's
stock universe: the short list returned +2.01% against SPY over the next 10
sessions (t 2.5, median 9 names a day), the long list +0.96% (t 1.4, median
4). About 40 rule variants were tried on that one year, with no costs or
borrow fees, so treat both as estimates. Long-side ETFs were measured as
shorts and lost money (-5.5%, 51 setups), which is why the pipeline's
stock-only universe is enough here.
"""

import numpy as np
import pandas as pd

from config.settings import CONFIG

# Defaults double as documentation; override in workflow_config.yaml `ema20_tab:`.
_DEFAULTS = {
    'min_avg_dollar_vol': 100e6,
    'min_avg_volume': 2e6,
    'min_adr_pct': 0.04,
    'min_price': 5.0,
    'prior_sessions': 10,
    'prior_min': 6,
    'overshoot_atr': 0.5,
    'pre_cross_sessions': 15,
    'pullback_atr': 0.25,
    'short_zone_atr': 0.25,
    'short_min_drawdown': 0.25,
    'short_min_lead_sessions': 40,
    'lead_window': 126,
    'lead_return_sessions': 63,
    'lead_pct': 0.9,
    'long_max_drawdown': 0.15,
    'long_min_rs20': 0.7,
    'long_fresh_sessions': 20,
    'long_min_theme_names': 3,
}

#: The per-ticker columns the scan reads from the indicator pickle.
BAR_COLUMNS = ('open', 'high', 'low', 'close', 'volume', 'ema10', 'ema20', 'atr14',
               'adr_pct', 'avg_dollar_vol', 'sma50_full')

#: Theme buckets that name no trading narrative, so they form no theme group.
_CATCHALL = ('Singleton', 'Uncategorized')


def ema20_config():
    cfg = CONFIG.get('ema20_tab') or {}
    return {k: type(v)(cfg.get(k, v)) for k, v in _DEFAULTS.items()}


def setup_state(bars, side, cfg):
    """Per-bar setup columns for one ticker and one side.

    Returns a frame on `bars.index` with `live` (a setup is open and the bar
    passes the liquidity floor), `days` (sessions since the cross, -1 before
    any), `cross_date`, `pullback` (a live bar whose low, for a long, or
    high, for a short, came within `pullback_atr` of EMA10) and `gap20` (for
    a short, how far the high stayed under EMA20 in ATR; negative means it
    went above).
    """
    s = 1 if side == 'long' else -1
    close = bars['close'].to_numpy(float)
    ema10 = bars['ema10'].to_numpy(float)
    ema20 = bars['ema20'].to_numpy(float)
    atr = bars['atr14'].to_numpy(float)
    n = len(bars)
    idx = np.arange(n)
    prior = int(cfg['prior_sessions'])

    beyond = s * (close - ema20) > 0
    e10 = s * (ema10 - ema20) > 0
    broke = s * (close - ema20) < -cfg['overshoot_atr'] * atr
    behind = pd.Series(~beyond).astype(int).shift(1).rolling(prior).sum().to_numpy()
    was_beyond = np.r_[True, beyond[:-1]]
    cross = beyond & ~was_beyond & (behind >= cfg['prior_min']) & ~e10
    cross[:prior] = False

    last = pd.Series(np.where(cross, idx, np.nan)).ffill().to_numpy()
    has = ~np.isnan(last)
    c = np.where(has, last, 0).astype(int)
    seg = np.where(has, c, -1)

    def ever(x):
        """True from the first bar since the cross where x holds."""
        return pd.Series(x).groupby(seg).cumsum().to_numpy() > 0

    e10_seen = ever(e10)
    dead = has & (ever(broke) | ever(e10_seen & ~e10)
                  | ever(~e10_seen & (idx - c >= cfg['pre_cross_sessions'])))
    vol20 = bars['volume'].rolling(20, min_periods=10).mean().to_numpy(float)
    liquid = (((bars['avg_dollar_vol'].to_numpy(float) >= cfg['min_avg_dollar_vol'])
               | (vol20 >= cfg['min_avg_volume']))
              & (bars['adr_pct'].to_numpy(float) >= cfg['min_adr_pct']))
    live = has & ~dead & liquid

    edge = (bars['low'] if s == 1 else bars['high']).to_numpy(float)
    with np.errstate(divide='ignore', invalid='ignore'):
        gap20 = (ema20 - bars['high'].to_numpy(float)) / atr
    return pd.DataFrame({
        'live': live,
        'days': np.where(has, idx - c, -1),
        'cross_date': pd.Series(bars.index[c], index=bars.index).where(has),
        'pullback': live & (idx > c) & (s * (edge - ema10) <= cfg['pullback_atr'] * atr),
        'gap20': gap20,
    }, index=bars.index)


def _l2_themes(paths):
    """The ``L1 / L2`` prefixes of a ticker's tags, catch-alls left out."""
    out = set()
    for path in paths or []:
        parts = [p.strip() for p in str(path).split('/')]
        if parts[0] in _CATCHALL:
            continue
        out.add(' / '.join(parts[:2]))
    return sorted(out)


def scan(bars_by_ticker, sessions, ticker_themes, cfg=None):
    """Both lists for every session in `sessions`.

    `bars_by_ticker` maps a ticker to its indicator frame (BAR_COLUMNS, on a
    date index). Leadership and 20-session strength are percentiles across
    every ticker that passes the volume floor that day, so pass the whole
    universe, not a pre-filtered one.

    Returns one frame with a row per (session, ticker, side): `date`,
    `ticker`, `side`, `days`, `cross_date`, `off_high` (fraction below the
    `lead_window` high), and per side `lead` and `gap` (short) or
    `theme_size` and `rs20` (long).
    """
    cfg = cfg or ema20_config()
    sessions = pd.DatetimeIndex(sorted(pd.to_datetime(list(sessions))))
    frames = {t: b for t, b in bars_by_ticker.items() if len(b) > cfg['prior_sessions']}
    close = pd.DataFrame({t: b['close'] for t, b in frames.items()}).sort_index()
    high = pd.DataFrame({t: b['high'] for t, b in frames.items()}).reindex(close.index)
    volume = pd.DataFrame({t: b['volume'] for t, b in frames.items()}).reindex(close.index)
    dollar = pd.DataFrame({t: b['avg_dollar_vol'] for t, b in frames.items()}).reindex(close.index)
    sma50 = pd.DataFrame({t: b['sma50_full'] for t, b in frames.items()}).reindex(close.index)

    # The ranking universe is the volume floor alone: ADR is a gate on the
    # candidate, not on the names it is compared against.
    ranked = ((dollar >= cfg['min_avg_dollar_vol'])
              | (volume.rolling(20, min_periods=10).mean() >= cfg['min_avg_volume']))
    window = int(cfg['lead_window'])
    leader = ((close / close.shift(int(cfg['lead_return_sessions'])) - 1)
              .where(ranked).rank(axis=1, pct=True) >= cfg['lead_pct'])
    lead = leader.astype(float).rolling(window, min_periods=1).sum()
    rs20 = (close / close.shift(20) - 1).where(ranked).rank(axis=1, pct=True)
    off_high = close / high.rolling(window, min_periods=20).max() - 1
    sma50_up = sma50 > sma50.shift(10)

    l2_of = {t: _l2_themes(p) for t, p in ticker_themes.items()}
    rows, fresh = [], []
    for t, bars in frames.items():
        keep = bars.index.intersection(sessions)
        if keep.empty:
            continue
        short = setup_state(bars, 'short', cfg).loc[keep]
        hit = (short['live'] & (short['gap20'] <= cfg['short_zone_atr'])
               & (off_high[t].loc[keep] <= -cfg['short_min_drawdown'])
               & (close[t].loc[keep] >= cfg['min_price']))
        for d in short.index[hit]:
            cross = short.at[d, 'cross_date']
            n_lead = lead.at[cross, t]
            if n_lead >= cfg['short_min_lead_sessions']:
                rows.append({'date': d, 'ticker': t, 'side': 'short',
                             'days': int(short.at[d, 'days']), 'cross_date': cross,
                             'off_high': off_high.at[d, t], 'lead': int(n_lead),
                             'gap': -short.at[d, 'gap20']})
        if not l2_of.get(t):
            continue
        long_ = setup_state(bars, 'long', cfg).loc[keep]
        live = long_['live'] & (long_['days'] <= cfg['long_fresh_sessions'])
        for d in long_.index[live]:
            fresh.append((d, t, int(long_.at[d, 'days']), long_.at[d, 'cross_date'],
                          bool(long_.at[d, 'pullback'])))

    if fresh:
        f = pd.DataFrame(fresh, columns=['date', 'ticker', 'days', 'cross_date', 'pullback'])
        f['l2'] = f['ticker'].map(l2_of)
        by_theme = f.explode('l2').groupby(['date', 'l2'])['ticker'].nunique()
        for r in f.itertuples(index=False):
            size = max((by_theme.get((r.date, x), 0) for x in r.l2), default=0)
            if not (r.pullback and size >= cfg['long_min_theme_names']):
                continue
            d, t = r.date, r.ticker
            if (off_high.at[d, t] >= -cfg['long_max_drawdown'] and bool(sma50_up.at[d, t])
                    and rs20.at[d, t] >= cfg['long_min_rs20'] and close.at[d, t] >= cfg['min_price']):
                rows.append({'date': d, 'ticker': t, 'side': 'long', 'days': r.days,
                             'cross_date': r.cross_date, 'off_high': off_high.at[d, t],
                             'theme_size': int(size), 'rs20': rs20.at[d, t]})

    return pd.DataFrame(rows, columns=SETUP_COLUMNS)


SETUP_COLUMNS = ['date', 'ticker', 'side', 'days', 'cross_date', 'off_high',
                 'lead', 'gap', 'theme_size', 'rs20']


def write_sessions(setups, sessions, master_dir, out_dir):
    """One ``ema20_<date>.parquet`` per session: master rows plus setup columns.

    Shaped like a screener's output, so the export, the screened union and
    the prune treat it the same way. A ticker on both lists has two rows. A
    session with no setup still writes an empty file, like a screener with
    no match.
    """
    import src.stock_utils as su

    for date_str in sessions:
        day = setups[setups['date'] == pd.Timestamp(date_str)]
        day = day.assign(date=date_str, cross_date=day['cross_date'].dt.strftime('%Y-%m-%d'))
        master = su.load_df_from_parquet(master_dir / f'master_{date_str}.parquet')
        merged = day.merge(master.drop(columns=['date']), on='ticker', how='left')
        su.save_df_to_parquet(merged, out_dir / f'ema20_{date_str}.parquet')
        print(f"OK {date_str}: {(day['side'] == 'short').sum()} short, "
              f"{(day['side'] == 'long').sum()} long", flush=True)


def main(argv=None):
    import argparse

    import src.stock_utils as su
    from config.settings import PRICE_DATA_TA_FILE, SCREENING_OUTPUT_DIR
    from src.screening.run_screener import newest_master_dates
    from src.themes.theme_registry import load_ticker_themes

    parser = argparse.ArgumentParser(description='Find EMA20 cross setups.')
    parser.add_argument('--days', '-d', type=int, default=1, help='Number of sessions.')
    args = parser.parse_args(argv)

    print("Loading technical indicators...", flush=True)
    daily = su.load_object_from_pickle(PRICE_DATA_TA_FILE)
    bars = {t: df[list(BAR_COLUMNS)] for t, df in daily.items()
            if not t.startswith('^') and set(BAR_COLUMNS) <= set(df.columns)}
    del daily
    sessions = newest_master_dates(args.days)
    setups = scan(bars, sessions, load_ticker_themes())
    write_sessions(setups, sessions, SCREENING_OUTPUT_DIR / 'master', SCREENING_OUTPUT_DIR / 'ema20')


if __name__ == '__main__':
    main()
