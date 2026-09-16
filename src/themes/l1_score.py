"""
L1 Radar — screener-independent theme-basket scoring with L1 roll-up and
sibling-confirmation boost.

The screened Themes lens scores only tagged tickers that passed a screener
that day, per leaf path, with no aggregation above the leaf — an L1 whose
sub-themes strengthen together in fragments never surfaces as one move. The
radar scores fixed theme baskets daily over ALL tagged members (liquidity-
floored), then rolls leaves up to their taxonomy L1 and boosts co-firing L1s:

R1  Universe   = tagged tickers present in the master table, close >=
                 min_close, avg_dollar_vol >= min_avg_dollar_vol, and
                 vol_sma50 >= min_avg_volume unless avg_dollar_vol >=
                 min_avg_volume_dollar_exempt (stacked on the dollar floor a
                 share floor only ejects high-priced names — the exemption
                 keeps liquid leaders; NaN vol_sma50 = young IPO, passes).
                 No screener gate and no fundamentals (unscreened members
                 have none).
R2b Coil leg   = BINARY. 100 when `tight_base` holds, 0 when it does not, and
                 missing_default only when tightness could not be computed.
                 The flag is already a threshold decision, so one knob carries
                 one meaning: composite_weights.coil alone says how many
                 composite points a coil is worth. Per-leaf and per-L1
                 `n_coiled` counts ride the payload at any weight.
R2  Composite  = weighted mean of four 0-100 legs (missing leg -> neutral
                 missing_default): rs (rs_sts_pct), vars_pct (percentile of
                 raw VARS across ALL tagged tickers in the master table,
                 computed before the price/liquidity floors — wide anchoring
                 consistent with the fast leg, and invariant to floor
                 changes; raw VARS is unbounded and must not be averaged
                 with a 0-100 leg), fast (rela_perf_1mo_rank so the radar
                 reacts pre-breakout). Cross-sectional ranks of
                 rela_perf_1mo are benchmark-invariant: the SPX leg divides
                 every ticker by the same per-session scalar, so ranking it
                 equals ranking raw perf_1mo (the SPY-vs-^GSPC difference
                 cannot move this leg).
R3  Leaf raw   = mean of the top-M member composites; a leaf needs
                 min_breadth universe members to score.
R4  Leaf z     = cross-sectional z-score of leaf raws (session-relative;
                 decompresses the narrow raw-score band so ranks separate).
R5  L1 score   = leaves grouped by taxonomy L1. l1_raw = mean of the top-K
                 leaf z-scores. L1s with >= min_leaves_for_boost scored
                 leaves earn boost = beta * l1_raw; every leaf under the L1
                 inherits it additively (boosted = z + boost) and the L1
                 itself scores boosted = l1_raw + boost. Single-leaf L1s get
                 no self-confirmation. A negative l1_raw yields a negative
                 boost — confirmation is symmetric.
                 A second, ASYMMETRIC term adds coil_gamma * the share of
                 members in a tight base, per leaf on its own share and per L1
                 on its own. Share, not count, or it ranks by roster size.
                 It never subtracts: a theme with no coiled members is not
                 worse for it, merely not basing.
R6  Ranks      = global_rank of leaves by boosted score across ALL scored
                 leaves; L1s ranked by boosted score. No display cap at
                 scoring level.
"""

from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

from config.settings import CONFIG
from src.themes.theme_registry import load_ticker_themes
from src.themes.theme_taxonomy import (
    build_theme_to_tickers,
    resolve_l1,
    split_path,
)

HIDDEN_L1S = {'Uncategorized', 'Singleton'}

DEFAULTS = {
    'beta': 0.3,
    'top_k_leaves': 5,
    'top_m_members': 5,
    'min_breadth': 2,
    'min_leaves_for_boost': 2,
    'min_avg_dollar_vol': 10_000_000,
    'min_close': 3.0,
    'min_avg_volume': 750_000,
    'min_avg_volume_dollar_exempt': 40_000_000,
    # backtest 2026-07: fast leg is rho~0.81 redundant with rs and adds no
    # IC — zero-weighted (tests/RADAR_BACKTEST_FINDINGS.md §3-4)
    'composite_weights': {'rs': 0.4, 'vars_pct': 0.4, 'fast': 0.0, 'coil': 0.2},
    'coil_gamma': 0.5,   # theme-level coil-share boost; see rollup_l1s
    'fast_leg_column': 'rela_perf_1mo_rank',
    'missing_default': 50.0,
}


def radar_config(overrides: Optional[dict] = None) -> dict:
    """DEFAULTS <- config/workflow_config.yaml `radar:` <- explicit overrides."""
    merged = dict(DEFAULTS)
    merged.update(CONFIG.get('radar', {}) or {})
    if overrides:
        merged.update(overrides)
    return merged


def _leg_from_column(df: pd.DataFrame, column: str, missing_default: float) -> pd.Series:
    """0-100 leg from a master column; absent column or NaN -> missing_default."""
    if column in df.columns:
        leg = pd.to_numeric(df[column], errors='coerce').clip(0, 100)
    else:
        leg = pd.Series(np.nan, index=df.index)
    return leg.fillna(missing_default)


def _coil_leg(df: pd.DataFrame, missing_default: float) -> pd.Series:
    """0-100 leg from the coiled-base flag (R2b). **Binary: 100 or 0.**

    A coiled stock scores 100, an uncoiled one 0, and a stock whose tightness
    could not be computed scores `missing_default` — unmeasurable is not the
    same as uncoiled, and every other leg treats absent data that way.

    Binary is a deliberate choice over a graded percentile of tightness. The
    flag is already a threshold decision, so grading it inside the leg would
    re-introduce a continuum the user reads as a yes/no on the chart, and make
    the weight mean two things at once (how coiled, and how much coiling
    matters). One knob, one meaning: `radar.composite_weights.coil` alone says
    how many composite points a coil is worth. The cost is a cliff at the
    threshold, which is the price of legibility here.

    ⛔ Tightness is inverted (lower is tighter), so a zero-filled absence reads
    as the *strongest* possible value. `_build_radar_snapshot` deliberately
    loads the master parquet without `.fillna(0)` for this reason; the explicit
    NaN check here is the second line of defence, not the only one.
    """
    if 'tightness' not in df.columns or 'tight_base' not in df.columns:
        return pd.Series(missing_default, index=df.index, dtype=float)

    tight = pd.to_numeric(df['tightness'], errors='coerce')
    flagged = df['tight_base'].fillna(False).astype(bool)

    leg = pd.Series(0.0, index=df.index, dtype=float)
    leg[flagged] = 100.0
    # Unmeasurable is not the same as uncoiled.
    leg[tight.isna() & ~flagged] = missing_default
    return leg.astype(float)


def build_radar_universe(
    master_df: pd.DataFrame,
    tagged_tickers: Set[str],
    cfg: Optional[dict] = None,
) -> pd.DataFrame:
    """Filter the master table to the radar universe and attach composites (R1+R2)."""
    cfg = radar_config(cfg)
    missing = float(cfg['missing_default'])

    df = master_df.copy()
    df['ticker'] = df['ticker'].astype(str).str.upper()
    tagged = {str(t).upper() for t in tagged_tickers}
    df = df[(df['ticker'] != 'SPX') & df['ticker'].isin(tagged)]
    df = df.drop_duplicates(subset='ticker', keep='first').copy()
    if df.empty:
        return df.reset_index(drop=True)

    # vars percentile is anchored to the full tagged pool BEFORE the floors —
    # wide anchoring consistent with the fast leg, invariant to floor changes.
    if 'vars' in df.columns:
        vars_num = pd.to_numeric(df['vars'], errors='coerce')
        df['vars_leg'] = (vars_num.rank(pct=True, method='average') * 100).fillna(missing)
    else:
        df['vars_leg'] = missing

    if 'close' in df.columns:
        close = pd.to_numeric(df['close'], errors='coerce')
        df = df[close >= float(cfg['min_close'])]
    if 'avg_dollar_vol' in df.columns:
        adv = pd.to_numeric(df['avg_dollar_vol'], errors='coerce')
        df = df[adv >= float(cfg['min_avg_dollar_vol'])]
    if 'vol_sma50' in df.columns:
        vol50 = pd.to_numeric(df['vol_sma50'], errors='coerce')
        passes = vol50 >= float(cfg['min_avg_volume'])
        if 'avg_dollar_vol' in df.columns:
            adv = pd.to_numeric(df['avg_dollar_vol'], errors='coerce')
            passes |= adv >= float(cfg['min_avg_volume_dollar_exempt'])
        # vol_sma50 needs 25 sessions (avg_dollar_vol only 10) — keep young
        # IPOs on dollar-floor-only gating instead of blanking them for weeks
        df = df[passes | vol50.isna()]
    if df.empty:
        return df.reset_index(drop=True)

    df = df.copy()
    df['rs_leg'] = _leg_from_column(df, 'rs_sts_pct', missing)
    df['fast_leg'] = _leg_from_column(df, str(cfg['fast_leg_column']), missing)
    df['coil_leg'] = _coil_leg(df, missing)

    weights = cfg['composite_weights'] or DEFAULTS['composite_weights']
    w_rs = float(weights.get('rs', 0))
    w_vars = float(weights.get('vars_pct', 0))
    w_fast = float(weights.get('fast', 0))
    w_coil = float(weights.get('coil', 0))
    total = w_rs + w_vars + w_fast + w_coil
    if total <= 0:
        w_rs = w_vars = w_fast = w_coil = 1.0
        total = 4.0
    df['composite'] = (
        w_rs * df['rs_leg'] + w_vars * df['vars_leg']
        + w_fast * df['fast_leg'] + w_coil * df['coil_leg']
    ) / total

    return df.reset_index(drop=True)


def compute_leaf_scores(
    universe_df: pd.DataFrame,
    theme_to_tickers: Dict[str, List[str]],
    cfg: Optional[dict] = None,
) -> List[dict]:
    """Score every leaf theme over its universe members (R3)."""
    cfg = radar_config(cfg)
    min_breadth = int(cfg['min_breadth'])
    top_m = int(cfg['top_m_members'])

    rows = {}
    for _, row in universe_df.iterrows():
        rows[str(row['ticker'])] = row

    leaf_scores = []
    for theme_path, tickers in theme_to_tickers.items():
        l1 = resolve_l1(theme_path) or split_path(theme_path)[0]
        if theme_path in HIDDEN_L1S or l1 in HIDDEN_L1S:
            continue

        members = []
        for t in tickers:
            row = rows.get(str(t).upper())
            if row is None:
                continue
            vars_val = row.get('vars')
            # avg_dollar_vol/adr_pct are payload-only: they feed the dashboard's
            # V/A cutoff filters and take no part in scoring. Note this is the
            # same column `build_radar_universe` gates on above — the universe
            # floor and the view lens now read one metric, but they remain
            # separate decisions: the floor keeps its share-volume leg and its
            # $40M dollar-volume waiver, which the view lens has no part in.
            dvol_val = row.get('avg_dollar_vol')
            adr_val = row.get('adr_pct')
            tight_val = row.get('tightness')
            # pd.notna, not `.get(..., False)`: Series.get returns its default
            # only when the KEY is absent, so a present-but-NaN tight_base would
            # come back as NaN — and bool(nan) is True. That would publish an
            # unmeasurable row as coiled, the exact inversion the rest of this
            # feature defends against. Fails closed, like _coil_leg above.
            coiled_val = row.get('tight_base')
            members.append({
                'ticker': str(t).upper(),
                'composite': float(row['composite']),
                'rs': float(row['rs_leg']),
                'vars': float(vars_val) if pd.notna(vars_val) else None,
                'price': float(row['close']) if pd.notna(row.get('close')) else None,
                'avg_dollar_vol': float(dvol_val) if pd.notna(dvol_val) else None,
                'adr_pct': float(adr_val) if pd.notna(adr_val) else None,
                'tightness': float(tight_val) if pd.notna(tight_val) else None,
                'coiled': bool(coiled_val) if pd.notna(coiled_val) else False,
            })
        if len(members) < min_breadth:
            continue

        members.sort(key=lambda m: -m['composite'])
        top = members[:top_m]
        _, l2, l3 = split_path(theme_path)
        leaf_scores.append({
            'theme': theme_path,
            'l1': l1,
            'l2': l2,
            'l3': l3,
            'composite_avg': float(np.mean([m['composite'] for m in top])),
            'breadth': len(members),
            'n_coiled': sum(1 for m in members if m['coiled']),
            'members': members,
        })
    return leaf_scores


def rollup_l1s(leaf_scores: List[dict], cfg: Optional[dict] = None) -> dict:
    """Z-score leaves, group by L1, apply the confirmation boost, rank (R4-R6)."""
    cfg = radar_config(cfg)
    beta = float(cfg['beta'])
    gamma = float(cfg.get('coil_gamma', 0.0))
    top_k = int(cfg['top_k_leaves'])
    min_leaves = int(cfg['min_leaves_for_boost'])

    raws = np.array([leaf['composite_avg'] for leaf in leaf_scores], dtype=float)
    std = float(np.std(raws)) if len(raws) else 0.0
    mean = float(np.mean(raws)) if len(raws) else 0.0
    for leaf, raw in zip(leaf_scores, raws):
        leaf['raw'] = float((raw - mean) / std) if std > 1e-9 else 0.0

    by_l1: Dict[str, List[dict]] = {}
    for leaf in leaf_scores:
        by_l1.setdefault(leaf['l1'], []).append(leaf)

    l1s = []
    for l1, leaves in by_l1.items():
        leaves.sort(key=lambda lf: (-lf['raw'], lf['theme']))
        l1_raw = float(np.mean([lf['raw'] for lf in leaves[:top_k]]))
        boost = beta * l1_raw if len(leaves) >= min_leaves else 0.0
        # Coil boost: gamma * the SHARE of members in a tight base, applied to
        # each leaf on its own share and to the L1 on its own. Share, not count
        # — the repo has learned three times (tape pressure, the SI tab, the
        # coil sort) that a count ranks by roster size and buries exactly the
        # small densely-coiled theme this is meant to surface.
        #
        # Unlike beta, this is asymmetric: it only ever adds. Beta encodes
        # sibling confirmation, where broad weakness genuinely argues a theme
        # down, so it is signed. Coiling carries no such claim — a theme with
        # no coiled members is not thereby worse, it is simply not basing.
        for leaf in leaves:
            leaf_share = (leaf.get('n_coiled', 0) / leaf['breadth']) if leaf['breadth'] else 0.0
            leaf['coil_delta'] = gamma * leaf_share
            leaf['boosted'] = leaf['raw'] + boost + leaf['coil_delta']
        members = {m['ticker'] for lf in leaves for m in lf['members']}
        # Distinct tickers: a stock tagged into two leaves of one L1 counts once,
        # matching how n_members is derived. Summing the leaf counts instead
        # would inflate exactly the densely-tagged L1s.
        coiled = {m['ticker'] for lf in leaves for m in lf['members'] if m['coiled']}
        l1_coil_delta = gamma * (len(coiled) / len(members)) if members else 0.0
        l1s.append({
            'name': l1,
            'raw': l1_raw,
            'boosted': l1_raw + boost + l1_coil_delta,
            'delta': boost,
            'coil_delta': l1_coil_delta,
            'n_leaves': len(leaves),
            'n_members': len(members),
            'n_coiled': len(coiled),
            'leaves': leaves,
        })

    all_leaves = sorted(leaf_scores, key=lambda lf: (-lf['boosted'], -lf['raw'], lf['theme']))
    for i, leaf in enumerate(all_leaves, start=1):
        leaf['global_rank'] = i

    l1s.sort(key=lambda e: (-e['boosted'], e['name']))
    for i, l1_entry in enumerate(l1s, start=1):
        l1_entry['rank'] = i

    return {'l1s': l1s, 'n_leaves_scored': len(leaf_scores)}


def compute_radar(
    master_df: pd.DataFrame,
    ticker_themes: Optional[Dict[str, List[str]]] = None,
    screened_tickers: Optional[Set[str]] = None,
    cfg: Optional[dict] = None,
) -> Optional[dict]:
    """Full radar for one session's master table.

    Returns the snapshot body (no report_date — the caller knows the session)
    or None when there is nothing to score.
    """
    cfg = radar_config(cfg)
    if ticker_themes is None:
        ticker_themes = load_ticker_themes()
    if not ticker_themes:
        return None

    universe = build_radar_universe(master_df, set(ticker_themes.keys()), cfg)
    if universe.empty:
        return None

    theme_to_tickers = build_theme_to_tickers(ticker_themes)
    leaf_scores = compute_leaf_scores(universe, theme_to_tickers, cfg)
    if not leaf_scores:
        return None

    rolled = rollup_l1s(leaf_scores, cfg)

    screened = {str(t).upper() for t in screened_tickers} if screened_tickers else set()
    for l1_entry in rolled['l1s']:
        l1_members = set()
        for leaf in l1_entry['leaves']:
            for m in leaf['members']:
                m['is_screened'] = m['ticker'] in screened
            l1_members.update(m['ticker'] for m in leaf['members'])
        l1_entry['n_screened'] = len(l1_members & screened)

    return {
        'params': {
            'beta': float(cfg['beta']),
            'top_k_leaves': int(cfg['top_k_leaves']),
            'top_m_members': int(cfg['top_m_members']),
            'min_avg_dollar_vol': float(cfg['min_avg_dollar_vol']),
            'min_close': float(cfg['min_close']),
            'min_avg_volume': float(cfg['min_avg_volume']),
            'min_avg_volume_dollar_exempt': float(cfg['min_avg_volume_dollar_exempt']),
            'composite_weights': dict(cfg['composite_weights']),
            'coil_weight': float((cfg['composite_weights'] or {}).get('coil', 0)),
        },
        'universe_size': int(len(universe)),
        'n_leaves_scored': rolled['n_leaves_scored'],
        'l1s': rolled['l1s'],
    }
