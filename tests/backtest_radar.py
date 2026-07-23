"""L1 Radar backtest harness — forward-return evaluation of the composite
weights and the sibling-confirmation boost beta.

Research tool, never part of the daily workflow (excluded from unittest
discovery: pattern is test*.py). Methodology, pre-registered metrics and
results live in tests/RADAR_BACKTEST_FINDINGS.md.

Data prep (once, from repo root; ~2-4h total, mostly master regen):
    uv run python src/data_collection/download_price_daily.py
    uv run python src/indicators/create_technical_indicators.py
    uv run python src/screening/create_master_table.py --days 130

Usage (from tests/):
    uv run python backtest_radar.py --mode history        # no parquet needed
    uv run python backtest_radar.py --mode legs
    uv run python backtest_radar.py --mode weights [--basket all|top] [--skip-day]
    uv run python backtest_radar.py --mode beta [--weights current]
    uv run python backtest_radar.py --mode anchor-diff
    uv run python backtest_radar.py --mode episodes-scan
Optional everywhere: --horizons 5,10,20  --pit-tags  --out sweep.csv
(--out is for local inspection only — never commit sweep CSVs.)
"""

import argparse
import json
import subprocess
import sys
from glob import glob
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

import src.stock_utils as su
from config.settings import DOCS_DATA_DIR, PRICE_DATA_FILE, SCREENING_OUTPUT_DIR
from src.themes.l1_score import (
    build_radar_universe,
    compute_leaf_scores,
    radar_config,
    rollup_l1s,
)
from src.themes.theme_registry import load_ticker_themes
from src.themes.theme_taxonomy import build_theme_to_tickers

MASTER_DIR = SCREENING_OUTPUT_DIR / "master"

HORIZONS = (5, 10, 20)
BETAS = (0.0, 0.15, 0.3, 0.5)
WEIGHT_GRID = {                       # rs / vars_pct / fast
    'current':    (0.4, 0.4, 0.2),
    'equal':      (1 / 3, 1 / 3, 1 / 3),
    'rs_only':    (1.0, 0.0, 0.0),    # corners measure each leg's solo IC
    'vars_only':  (0.0, 1.0, 0.0),
    'fast_only':  (0.0, 0.0, 1.0),
    'no_fast':    (0.5, 0.5, 0.0),
    'fast_heavy': (0.2, 0.4, 0.4),
    'rs_heavy':   (0.6, 0.2, 0.2),
}
MIN_L1S_FOR_IC = 8      # sessions with fewer scored+returned L1s are skipped
MIN_COVERAGE = 0.7      # basket needs >= 70% of members with both endpoints


# ── Data layer ───────────────────────────────────────────────────────────

def get_available_dates():
    """Session dates with a local master parquet, oldest first."""
    files = sorted(glob(str(MASTER_DIR / "master_*.parquet")))
    return [Path(f).stem.replace("master_", "") for f in files]


def load_master(date_str):
    df = su.load_df_from_parquet(MASTER_DIR / f"master_{date_str}.parquet")
    return None if df is None or df.empty else df


def build_close_matrix():
    """sessions x tickers close matrix (SPY column included), from the raw
    price pickle. Built once per process (~1 min)."""
    data = su.load_object_from_pickle(PRICE_DATA_FILE)
    idx = data['SPY'].index
    cols = {}
    for t, df in data.items():
        if df is not None and 'close' in df.columns:
            cols[t] = df['close'].reindex(idx)
    mx = pd.DataFrame(cols, index=idx)
    return mx


def date_positions(close_mx):
    return {d.strftime('%Y-%m-%d'): i for i, d in enumerate(close_mx.index)}


def load_tags_asof(date_str=None, pit=False, _cache={}):
    """Ticker->paths map. pit=True: best-effort point-in-time via git history
    of data/ticker_themes.json (legacy labels normalized; invalid dropped)."""
    if not pit or date_str is None:
        if 'live' not in _cache:
            _cache['live'] = load_ticker_themes()
        return _cache['live']

    if 'log' not in _cache:
        out = subprocess.run(
            ['git', 'log', '--format=%H|%cs', '--', 'data/ticker_themes.json'],
            capture_output=True, text=True, cwd=ROOT, check=True,
        ).stdout.strip().splitlines()
        # newest first -> list of (sha, date)
        _cache['log'] = [tuple(line.split('|')) for line in out if '|' in line]

    sha = None
    for commit_sha, commit_date in _cache['log']:
        if commit_date <= date_str:
            sha = commit_sha
            break
    if sha is None:
        return {}
    key = f'pit:{sha}'
    if key not in _cache:
        raw = subprocess.run(
            ['git', 'show', f'{sha}:data/ticker_themes.json'],
            capture_output=True, text=True, cwd=ROOT, check=True,
        ).stdout
        tags = json.loads(raw)
        _cache[key] = _normalize_tags(tags)
    return _cache[key]


def _normalize_tags(tags):
    """Alias-map legacy free-form labels; otherwise keep paths as written.

    Historical tags reference the taxonomy AS OF their commit (L1/L2 names
    have been renamed since), so validating against today's taxonomy would
    drop whole branches. Per-session scoring only needs internally
    consistent paths — each path groups under its own first segment — so
    unvalidated-but-structured paths are kept. Caveat: taxonomy renames make
    L1 names drift across PIT sessions (affects cross-session stability
    metrics near rename dates, not within-session IC)."""
    from src.themes.legacy_aliases import normalize_legacy_theme
    out = {}
    for ticker, paths in tags.items():
        kept = [normalize_legacy_theme(p) or p for p in paths or [] if p]
        if kept:
            out[str(ticker).upper()] = kept
    return out


# ── Scoring layer ────────────────────────────────────────────────────────

def apply_weights(universe_df, weights):
    """Recompute the composite from the (weight-independent) legs — mirrors
    l1_score.build_radar_universe's formula so weight sweeps skip the
    universe rebuild."""
    w_rs, w_vars, w_fast = (float(w) for w in weights)
    total = w_rs + w_vars + w_fast
    if total <= 0:
        w_rs = w_vars = w_fast = 1.0
        total = 3.0
    df = universe_df.copy()
    df['composite'] = (
        w_rs * df['rs_leg'] + w_vars * df['vars_leg'] + w_fast * df['fast_leg']
    ) / total
    return df


def l1_snapshots_multi_beta(leaves, cfg, betas):
    """rollup_l1s once per beta (idempotent over leaf_scores), snapshotting
    plain records because the rollup mutates shared leaf dicts in place."""
    out = {}
    for beta in betas:
        rolled = rollup_l1s(leaves, dict(cfg, beta=beta))
        out[beta] = [
            {
                'name': e['name'],
                'raw': e['raw'],
                'boosted': e['boosted'],
                'rank': e['rank'],
                'n_leaves': e['n_leaves'],
            }
            for e in rolled['l1s']
        ]
    return out


def l1_baskets(leaves, mode='all', top_chips=10):
    """L1 name -> distinct member tickers of its scored leaves.
    'all' = every universe member (weight-invariant); 'top' = the dashboard
    chip set (leaf members are composite-sorted, so this one is
    weight-dependent)."""
    baskets = {}
    for leaf in leaves:
        members = leaf['members'] if mode == 'all' else leaf['members'][:top_chips]
        baskets.setdefault(leaf['l1'], set()).update(m['ticker'] for m in members)
    return {k: sorted(v) for k, v in baskets.items()}


# ── Forward-return layer ─────────────────────────────────────────────────

def forward_excess_return(close_mx, dpos, date_str, tickers, horizon,
                          skip_days=0, min_coverage=MIN_COVERAGE):
    """Equal-weight close-to-close basket return over `horizon` sessions,
    minus SPY over the identical window. None when the window is incomplete
    or member coverage is below the floor."""
    i = dpos.get(date_str)
    if i is None:
        return None
    e, x = i + skip_days, i + skip_days + horizon
    if x >= len(close_mx.index):
        return None
    entry, exit_ = close_mx.iloc[e], close_mx.iloc[x]
    sub_e = entry.reindex(tickers)
    sub_x = exit_.reindex(tickers)
    valid = sub_e.notna() & sub_x.notna() & (sub_e > 0)
    if len(tickers) == 0 or valid.sum() / len(tickers) < min_coverage:
        return None
    basket = float((sub_x[valid] / sub_e[valid] - 1.0).mean())
    if pd.isna(entry.get('SPY')) or pd.isna(exit_.get('SPY')):
        return None
    spy = float(exit_['SPY'] / entry['SPY'] - 1.0)
    return basket - spy


# ── Metrics ──────────────────────────────────────────────────────────────

def session_rank_ic(snapshot, fwd_by_l1, min_n=MIN_L1S_FOR_IC):
    """Spearman(boosted, forward excess) across scored L1s with returns."""
    pairs = [(e['boosted'], fwd_by_l1[e['name']])
             for e in snapshot if fwd_by_l1.get(e['name']) is not None]
    if len(pairs) < min_n:
        return None
    b, f = zip(*pairs)
    rho = spearmanr(b, f).statistic
    return None if pd.isna(rho) else float(rho)


def topk_hit(snapshot, fwd_by_l1, k=3):
    """(top-k mean beats the median L1, spread top-k mean minus median)."""
    fwd = [(e['rank'], fwd_by_l1[e['name']])
           for e in snapshot if fwd_by_l1.get(e['name']) is not None]
    if len(fwd) < MIN_L1S_FOR_IC:
        return None, None
    fwd.sort(key=lambda t: t[0])
    top = [f for _, f in fwd[:k]]
    med = float(np.median([f for _, f in fwd]))
    spread = float(np.mean(top)) - med
    return spread > 0, spread


def rank_autocorr(prev_snapshot, snapshot):
    """Day-over-day Spearman of boosted scores on the common L1 names."""
    if not prev_snapshot:
        return None
    prev = {e['name']: e['boosted'] for e in prev_snapshot}
    pairs = [(prev[e['name']], e['boosted']) for e in snapshot if e['name'] in prev]
    if len(pairs) < MIN_L1S_FOR_IC:
        return None
    a, b = zip(*pairs)
    rho = spearmanr(a, b).statistic
    return None if pd.isna(rho) else float(rho)


def top5_jaccard(prev_snapshot, snapshot):
    if not prev_snapshot:
        return None
    a = {e['name'] for e in sorted(prev_snapshot, key=lambda e: e['rank'])[:5]}
    b = {e['name'] for e in sorted(snapshot, key=lambda e: e['rank'])[:5]}
    return len(a & b) / len(a | b) if (a | b) else None


def leg_correlations(universe_df):
    """Pairwise Spearman of the three legs across the session's universe."""
    legs = universe_df[['rs_leg', 'vars_leg', 'fast_leg']]
    out = {}
    for a, b in (('rs_leg', 'vars_leg'), ('rs_leg', 'fast_leg'), ('vars_leg', 'fast_leg')):
        rho = spearmanr(legs[a], legs[b]).statistic
        out[f'{a}~{b}'] = None if pd.isna(rho) else float(rho)
    return out


def moving_block_bootstrap_ci(series, block_len, n_boot=2000, alpha=0.10, seed=7):
    """CI on the mean of an autocorrelated daily series (overlapping H-day
    forward windows) via moving-block bootstrap."""
    vals = np.asarray([v for v in series if v is not None], dtype=float)
    n = len(vals)
    if n == 0:
        return None, None, None
    if n <= block_len:
        return float(vals.mean()), float(vals.mean()), float(vals.mean())
    rng = np.random.default_rng(seed)
    n_blocks = int(np.ceil(n / block_len))
    starts = rng.integers(0, n - block_len + 1, size=(n_boot, n_blocks))
    means = np.empty(n_boot)
    for i in range(n_boot):
        sample = np.concatenate([vals[s:s + block_len] for s in starts[i]])[:n]
        means[i] = sample.mean()
    lo, hi = np.quantile(means, [alpha / 2, 1 - alpha / 2])
    return float(lo), float(vals.mean()), float(hi)


# ── Sweep orchestration ──────────────────────────────────────────────────

def run_sweep(dates, weight_names, betas, horizons, basket_mode='all',
              skip_day=False, pit_tags=False, collect_snapshots=None):
    """Outer loop sessions, inner loop weight configs x betas. Returns one
    aggregate row per (weights, beta, horizon). `collect_snapshots`, when a
    dict, receives {date: snapshot} for the ('current', config-beta) cell —
    used by episodes-scan."""
    close_mx = build_close_matrix()
    dpos = date_positions(close_mx)
    base_cfg = radar_config()
    skip = 1 if skip_day else 0

    rows = []            # per-session metric rows
    prev_snapshots = {}  # (weights, beta) -> last session's snapshot
    fwd_cache = {}

    for date_str in dates:
        master = load_master(date_str)
        if master is None:
            continue
        tags = load_tags_asof(date_str, pit=pit_tags)
        if not tags:
            continue
        universe = build_radar_universe(master, set(tags.keys()), base_cfg)
        if universe.empty:
            continue
        theme_map = build_theme_to_tickers(tags)
        fwd_cache.clear()

        for wname in weight_names:
            uni_w = apply_weights(universe, WEIGHT_GRID[wname])
            leaves = compute_leaf_scores(uni_w, theme_map, base_cfg)
            if not leaves:
                continue
            baskets = l1_baskets(leaves, mode=basket_mode)
            snaps = l1_snapshots_multi_beta(leaves, base_cfg, betas)

            fwd_by_h = {}
            for h in horizons:
                fwd = {}
                for name, tickers in baskets.items():
                    key = (tuple(tickers), h)
                    if key not in fwd_cache:
                        fwd_cache[key] = forward_excess_return(
                            close_mx, dpos, date_str, tickers, h, skip_days=skip)
                    fwd[name] = fwd_cache[key]
                fwd_by_h[h] = fwd

            for beta, snapshot in snaps.items():
                key = (wname, beta)
                ac = rank_autocorr(prev_snapshots.get(key), snapshot)
                jac = top5_jaccard(prev_snapshots.get(key), snapshot)
                prev_snapshots[key] = snapshot
                if collect_snapshots is not None and wname == 'current' \
                        and abs(beta - float(base_cfg['beta'])) < 1e-12:
                    collect_snapshots[date_str] = snapshot
                for h in horizons:
                    ic = session_rank_ic(snapshot, fwd_by_h[h])
                    hit, spread = topk_hit(snapshot, fwd_by_h[h])
                    rows.append({
                        'date': date_str, 'weights': wname, 'beta': beta,
                        'horizon': h, 'ic': ic, 'top3_hit': hit,
                        'top3_spread': spread, 'autocorr': ac, 'jaccard': jac,
                        'n_l1s': len(snapshot),
                    })

    return pd.DataFrame(rows)


def aggregate(df):
    """One row per (weights, beta, horizon) with bootstrap CI on mean IC."""
    out = []
    for (w, b, h), grp in df.groupby(['weights', 'beta', 'horizon']):
        ics = grp['ic'].dropna()
        lo, mean, hi = moving_block_bootstrap_ci(
            ics.tolist(), block_len=max(2 * int(h), 10))
        hits = grp['top3_hit'].dropna()
        spreads = grp['top3_spread'].dropna()
        out.append({
            'weights': w, 'beta': b, 'horizon': h,
            'sessions': int(ics.size),
            'mean_ic': None if mean is None else round(mean, 4),
            'ic_ci_lo': None if lo is None else round(lo, 4),
            'ic_ci_hi': None if hi is None else round(hi, 4),
            'ic_pos_share': round(float((ics > 0).mean()), 3) if ics.size else None,
            'top3_hit_rate': round(float(hits.mean()), 3) if hits.size else None,
            'top3_spread_bps': round(float(spreads.mean()) * 1e4, 1) if spreads.size else None,
            'autocorr': round(float(grp['autocorr'].dropna().mean()), 3)
                        if grp['autocorr'].notna().any() else None,
            'top5_jaccard': round(float(grp['jaccard'].dropna().mean()), 3)
                            if grp['jaccard'].notna().any() else None,
        })
    return pd.DataFrame(out).sort_values(['horizon', 'weights', 'beta'])


# ── Modes ────────────────────────────────────────────────────────────────

def mode_legs(dates, pit_tags):
    base_cfg = radar_config()
    mats = []
    for date_str in dates:
        master = load_master(date_str)
        if master is None:
            continue
        tags = load_tags_asof(date_str, pit=pit_tags)
        universe = build_radar_universe(master, set(tags.keys()), base_cfg)
        if universe.empty:
            continue
        mats.append(leg_correlations(universe))
    df = pd.DataFrame(mats)
    print(f"\nPairwise leg Spearman correlations over {len(df)} sessions "
          f"(mean / p10 / p90):")
    for col in df.columns:
        s = df[col].dropna()
        print(f"  {col:22s} {s.mean():+.3f}   {s.quantile(0.1):+.3f} / "
              f"{s.quantile(0.9):+.3f}")
    print("\nSolo-leg predictive power: run --mode weights (rs_only / "
          "vars_only / fast_only corners).")
    return df


def mode_history(betas):
    """Cheap beta re-rank on the exported radar_history.json — no parquet.
    Exact within the kept entries; NO forward returns (rank sensitivity and
    stability only)."""
    history_file = DOCS_DATA_DIR / 'radar_history.json'
    history = json.loads(history_file.read_text(encoding='utf-8'))
    cfg_beta = float(radar_config()['beta'])

    def rerank(entries, beta):
        scored = [
            dict(e, boosted_new=(e['raw'] * (1 + beta) if e.get('n_leaves', 0) >= 2
                                 else e['raw']))
            for e in entries
        ]
        scored.sort(key=lambda e: (-e['boosted_new'], e['name']))
        return [e['name'] for e in scored]

    print(f"\nbeta re-rank over {len(history)} radar_history sessions "
          f"(top-20 truncated entries — ranks near the tail are censored; "
          f"no forward returns here):")
    print(f"{'beta':>6} {'mean|shift|':>12} {'top3 changed':>13} {'top5 changed':>13}")
    for beta in betas:
        shifts, top3_chg, top5_chg = [], 0, 0
        for snap in history:
            entries = snap.get('l1s') or snap.get('ecosystems') or []
            if len(entries) < 5:
                continue
            base_names = rerank(entries, cfg_beta)
            new_names = rerank(entries, beta)
            pos_base = {n: i for i, n in enumerate(base_names)}
            shifts.extend(abs(pos_base[n] - i) for i, n in enumerate(new_names))
            top3_chg += set(new_names[:3]) != set(base_names[:3])
            top5_chg += set(new_names[:5]) != set(base_names[:5])
        n = len(history)
        print(f"{beta:>6.2f} {np.mean(shifts):>12.3f} {top3_chg:>10d}/{n:<3d}"
              f"{top5_chg:>10d}/{n:<3d}")


def mode_anchor_diff(dates, pit_tags):
    """Quantify the vars-leg re-anchoring (tagged pool vs the pre-2026-07
    floor-survivor pool): per-session L1 rank correlation and top-3 overlap
    between the two anchorings, at production weights/beta."""
    base_cfg = radar_config()
    rhos, top3_same = [], []
    for date_str in dates:
        master = load_master(date_str)
        if master is None:
            continue
        tags = load_tags_asof(date_str, pit=pit_tags)
        universe = build_radar_universe(master, set(tags.keys()), base_cfg)
        if universe.empty:
            continue
        theme_map = build_theme_to_tickers(tags)

        # Old anchoring: re-rank vars among the floor survivors only.
        old = universe.copy()
        vars_num = pd.to_numeric(old.get('vars'), errors='coerce')
        old['vars_leg'] = (vars_num.rank(pct=True, method='average') * 100) \
            .fillna(float(base_cfg['missing_default']))
        w = base_cfg['composite_weights']
        weights = (w['rs'], w['vars_pct'], w['fast'])
        new_snap = l1_snapshots_multi_beta(
            compute_leaf_scores(apply_weights(universe, weights), theme_map, base_cfg),
            base_cfg, [float(base_cfg['beta'])])
        old_snap = l1_snapshots_multi_beta(
            compute_leaf_scores(apply_weights(old, weights), theme_map, base_cfg),
            base_cfg, [float(base_cfg['beta'])])
        new_l1s = list(new_snap.values())[0]
        old_l1s = list(old_snap.values())[0]
        rho = rank_autocorr(old_l1s, new_l1s)
        if rho is not None:
            rhos.append(rho)
        a = {e['name'] for e in sorted(new_l1s, key=lambda e: e['rank'])[:3]}
        b = {e['name'] for e in sorted(old_l1s, key=lambda e: e['rank'])[:3]}
        top3_same.append(a == b)
    print(f"\nvars-leg anchoring diff over {len(rhos)} sessions "
          f"(tagged pool vs floor-survivor pool, production config):")
    print(f"  mean rank correlation: {np.mean(rhos):+.4f}")
    print(f"  identical top-3 sets:  {sum(top3_same)}/{len(top3_same)}")


def mode_episodes_scan(dates, horizons, basket_mode, skip_day, pit_tags):
    """Propose validate_radar regression episodes: an L1 enters the top-3 for
    the first time in >= 10 sessions AND its H=10 'all'-basket excess > +3%.
    Candidates must be human-verified before landing in an episodes file."""
    snapshots = {}
    df = run_sweep(dates, ['current'], [float(radar_config()['beta'])],
                   horizons, basket_mode, skip_day, pit_tags,
                   collect_snapshots=snapshots)
    close_mx = build_close_matrix()
    dpos = date_positions(close_mx)
    base_cfg = radar_config()

    last_top3_session = {}
    candidates = []
    ordered = [d for d in dates if d in snapshots]
    for si, date_str in enumerate(ordered):
        snapshot = snapshots[date_str]
        top3 = [e['name'] for e in sorted(snapshot, key=lambda e: e['rank'])[:3]]
        for name in top3:
            prev = last_top3_session.get(name)
            fresh = prev is None or (si - prev) >= 10
            last_top3_session[name] = si
            if not fresh:
                continue
            row = df[(df.date == date_str) & (df.horizon == 10)]
            # re-derive this L1's forward excess from the sweep row's session
            master = load_master(date_str)
            tags = load_tags_asof(date_str, pit=pit_tags)
            universe = build_radar_universe(master, set(tags.keys()), base_cfg)
            theme_map = build_theme_to_tickers(tags)
            w = base_cfg['composite_weights']
            leaves = compute_leaf_scores(
                apply_weights(universe, (w['rs'], w['vars_pct'], w['fast'])),
                theme_map, base_cfg)
            baskets = l1_baskets(leaves, mode='all')
            fwd = forward_excess_return(close_mx, dpos, date_str,
                                        baskets.get(name, []), 10,
                                        skip_days=1 if skip_day else 0)
            if fwd is not None and fwd > 0.03:
                candidates.append((date_str, name, fwd))
    print(f"\nEpisode candidates (first top-3 entry in >=10 sessions AND "
          f"H=10 all-basket excess > +3%):")
    for date_str, name, fwd in candidates:
        print(f"  {date_str}  {name:<28} fwd10 excess {fwd:+.2%}")
    if not candidates:
        print("  (none)")
    return candidates


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--mode', required=True,
                    choices=['history', 'legs', 'weights', 'beta',
                             'anchor-diff', 'episodes-scan'])
    ap.add_argument('--horizons', default='5,10,20')
    ap.add_argument('--basket', default='all', choices=['all', 'top'])
    ap.add_argument('--betas', default=None,
                    help='comma list; default module BETAS')
    ap.add_argument('--weights', default=None,
                    help='beta mode: single WEIGHT_GRID name (default current)')
    ap.add_argument('--skip-day', action='store_true',
                    help='enter at close of d+1 instead of d (execution-lag sensitivity)')
    ap.add_argument('--pit-tags', action='store_true',
                    help='best-effort point-in-time tags from git history')
    ap.add_argument('--out', default=None, help='write per-session rows CSV')
    args = ap.parse_args()

    horizons = tuple(int(h) for h in args.horizons.split(','))
    betas = tuple(float(b) for b in args.betas.split(',')) if args.betas else BETAS
    dates = get_available_dates()

    if args.mode == 'history':
        mode_history(betas)
        return
    if not dates:
        print(f"No master parquet under {MASTER_DIR} — run the data prep "
              "commands in the module docstring first.")
        sys.exit(1)

    if args.mode == 'legs':
        mode_legs(dates, args.pit_tags)
        return
    if args.mode == 'anchor-diff':
        mode_anchor_diff(dates, args.pit_tags)
        return
    if args.mode == 'episodes-scan':
        mode_episodes_scan(dates, horizons, args.basket, args.skip_day,
                           args.pit_tags)
        return

    if args.mode == 'weights':
        weight_names = list(WEIGHT_GRID)
        run_betas = (float(radar_config()['beta']),)
    else:  # beta
        weight_names = [args.weights or 'current']
        run_betas = betas

    df = run_sweep(dates, weight_names, run_betas, horizons,
                   args.basket, args.skip_day, args.pit_tags)
    agg = aggregate(df)
    with pd.option_context('display.width', 160, 'display.max_columns', 20):
        print(agg.to_string(index=False))
    if args.out:
        df.to_csv(args.out, index=False)
        print(f"\nper-session rows -> {args.out}")


if __name__ == '__main__':
    main()
