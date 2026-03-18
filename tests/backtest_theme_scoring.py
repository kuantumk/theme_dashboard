"""
Backtest: Old vs New theme scoring formula.

Runs both scoring systems on all available historical master CSVs
and compares rankings, score distributions, and specific theme behavior.
"""

import sys
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from src.themes.theme_registry import load_ticker_themes

# ── Paths ────────────────────────────────────────────────────────
MASTER_DIR = ROOT / "screening_output" / "master"
CONSOLIDATED_DIR = ROOT / "screening_output" / "consolidated"

# ── Load themes ──────────────────────────────────────────────────
TICKER_THEMES = load_ticker_themes()

# Reverse: theme -> tickers
THEME_TICKERS = defaultdict(list)
for ticker, themes in TICKER_THEMES.items():
    for theme in themes:
        THEME_TICKERS[theme].append(ticker)

# ── Config (matching workflow_config.yaml) ───────────────────────
OLD_WEIGHTS_BULL = {"rs_avg": 0.2, "momentum": 0.6, "breadth": 0.2}
OLD_WEIGHTS_BEAR = {"rs_avg": 0.6, "momentum": 0.1, "breadth": 0.3}
NEW_WEIGHTS_BULL = {"strength": 0.50, "confirmation": 0.50}
NEW_WEIGHTS_BEAR = {"strength": 0.70, "confirmation": 0.30}
MOMENTUM_THRESHOLD = 80.0


# ═════════════════════════════════════════════════════════════════
# SCORING FUNCTIONS
# ═════════════════════════════════════════════════════════════════

def score_theme_old(theme, tickers, master_df, screened_tickers, weights):
    """Current production scoring (from analyze_theme_strength.py)."""
    scoring_tickers = list(set(tickers) & screened_tickers)
    theme_df = master_df[master_df['ticker'].isin(scoring_tickers)]
    if len(theme_df) == 0:
        return None

    rs_values = theme_df['rs_sts_pct'].values
    median_rs = np.median(rs_values)
    high_momentum_pct = (np.sum(rs_values > MOMENTUM_THRESHOLD) / len(rs_values)) * 100
    breadth = len(theme_df)

    breadth_penalty = 1.0 if breadth >= 3 else (0.5 if breadth == 2 else 0.3)

    strength_score = (
        weights["rs_avg"] * median_rs +
        weights["momentum"] * high_momentum_pct +
        weights["breadth"] * np.log1p(breadth) * 10
    ) * breadth_penalty

    # Also compute the supplementary metrics (for comparison)
    dist_25sma = np.nan
    if 'close' in theme_df.columns and 'sma25' in theme_df.columns:
        valid = theme_df[theme_df['sma25'] > 0]
        if not valid.empty:
            dist_25sma = np.nanmean(((valid['close'] - valid['sma25']) / valid['sma25']) * 100)

    pct_above_50sma = np.nan
    if 'close' in theme_df.columns and 'sma50' in theme_df.columns:
        valid = theme_df[~theme_df['sma50'].isna()]
        if len(valid) > 0:
            pct_above_50sma = (np.sum(valid['close'] > valid['sma50']) / len(valid)) * 100

    return {
        'theme': theme,
        'score': strength_score,
        'median_rs': median_rs,
        'high_momentum_pct': high_momentum_pct,
        'breadth': breadth,
        'breadth_penalty': breadth_penalty,
        'pct_above_50sma': pct_above_50sma,
        'avg_dist_25sma': dist_25sma,
        # Component breakdown
        'c_rs': weights["rs_avg"] * median_rs * breadth_penalty,
        'c_momentum': weights["momentum"] * high_momentum_pct * breadth_penalty,
        'c_breadth': weights["breadth"] * np.log1p(breadth) * 10 * breadth_penalty,
    }


def score_theme_new(theme, tickers, master_df, screened_tickers, weights):
    """Proposed new scoring system."""
    scoring_tickers = list(set(tickers) & screened_tickers)
    theme_df = master_df[master_df['ticker'].isin(scoring_tickers)]
    if len(theme_df) == 0:
        return None

    rs_values = theme_df['rs_sts_pct'].values
    breadth = len(theme_df)

    # ── STRENGTH (0-100) ─────────────────────────────────────
    median_rs = np.median(rs_values)
    leader_pct = (np.sum(rs_values >= 90) / len(rs_values)) * 100
    strength = 0.5 * median_rs + 0.5 * leader_pct

    # ── CONFIRMATION (0-100) ─────────────────────────────────
    # B1: Structural health — % above 50SMA
    pct_above_50sma = 0.0
    if 'close' in theme_df.columns and 'sma50' in theme_df.columns:
        valid = theme_df[~theme_df['sma50'].isna()]
        if len(valid) > 0:
            pct_above_50sma = (np.sum(valid['close'] > valid['sma50']) / len(valid)) * 100

    # B2: Near highs — % within 15% of 252-day high
    pct_near_highs = 0.0
    if 'close' in theme_df.columns and 'max252' in theme_df.columns:
        valid = theme_df[~theme_df['max252'].isna()]
        if len(valid) > 0:
            pct_near_highs = (np.sum(valid['close'] >= 0.85 * valid['max252']) / len(valid)) * 100

    # B3: Breadth — linear, cap at 5
    breadth_score = min(breadth / 5.0, 1.0) * 100

    confirmation = 0.35 * pct_above_50sma + 0.35 * pct_near_highs + 0.30 * breadth_score

    # ── ACTIONABILITY (multiplier) ───────────────────────────
    # Extension penalty
    dist_25sma = np.nan
    if 'close' in theme_df.columns and 'sma25' in theme_df.columns:
        valid = theme_df[theme_df['sma25'] > 0]
        if not valid.empty:
            dist_25sma = np.nanmean(((valid['close'] - valid['sma25']) / valid['sma25']) * 100)

    extension_factor = 1.0
    if not np.isnan(dist_25sma):
        if dist_25sma > 25:
            extension_factor = 0.65
        elif dist_25sma > 15:
            extension_factor = 0.80
        elif dist_25sma > 10:
            extension_factor = 0.92

    # Volume expansion bonus
    vol_factor = 1.0
    if 'vol_sma40' in theme_df.columns and 'vol_sma252' in theme_df.columns:
        valid = theme_df[(theme_df['vol_sma252'] > 0) & (~theme_df['vol_sma252'].isna())]
        if not valid.empty:
            avg_vol_ratio = np.nanmean(valid['vol_sma40'] / valid['vol_sma252'])
            if avg_vol_ratio > 2.0:
                vol_factor = 1.15
            elif avg_vol_ratio > 1.5:
                vol_factor = 1.08

    actionability = extension_factor * vol_factor

    # ── BREADTH PENALTY (unchanged) ──────────────────────────
    breadth_penalty = 1.0 if breadth >= 3 else (0.5 if breadth == 2 else 0.3)

    # ── FINAL SCORE ──────────────────────────────────────────
    score = (
        weights["strength"] * strength +
        weights["confirmation"] * confirmation
    ) * actionability * breadth_penalty

    return {
        'theme': theme,
        'score': score,
        'median_rs': median_rs,
        'leader_pct': leader_pct,
        'breadth': breadth,
        'breadth_penalty': breadth_penalty,
        'strength': strength,
        'confirmation': confirmation,
        'pct_above_50sma': pct_above_50sma,
        'pct_near_highs': pct_near_highs,
        'breadth_score': breadth_score,
        'avg_dist_25sma': dist_25sma,
        'extension_factor': extension_factor,
        'vol_factor': vol_factor,
        'actionability': actionability,
        # Component breakdown
        'c_strength': weights["strength"] * strength * actionability * breadth_penalty,
        'c_confirmation': weights["confirmation"] * confirmation * actionability * breadth_penalty,
    }


# ═════════════════════════════════════════════════════════════════
# DATA LOADING
# ═════════════════════════════════════════════════════════════════

def load_day_data(date_str):
    """Load master_df and screened_tickers for a given date."""
    # Master CSV
    master_file = MASTER_DIR / f"master_{date_str}.csv"
    if not master_file.exists():
        return None, None
    master_df = pd.read_csv(master_file).fillna(0)

    # Union file (screened tickers)
    mmddyyyy = datetime.strptime(date_str, '%Y-%m-%d').strftime('%m%d%Y')
    union_file = CONSOLIDATED_DIR / f"_union_{mmddyyyy}.txt"
    if not union_file.exists():
        return master_df, set()
    screened = set(pd.read_csv(union_file, header=None)[0].tolist())

    return master_df, screened


def get_available_dates():
    """Get all dates with master CSVs."""
    files = sorted(MASTER_DIR.glob("master_*.csv"))
    return [f.stem.replace("master_", "") for f in files]


# ═════════════════════════════════════════════════════════════════
# BACKTEST ENGINE
# ═════════════════════════════════════════════════════════════════

def run_scoring_for_date(date_str, regime="bull"):
    """Run both old and new scoring for a date. Returns (old_results, new_results)."""
    master_df, screened = load_day_data(date_str)
    if master_df is None:
        return [], []

    old_w = OLD_WEIGHTS_BULL if regime == "bull" else OLD_WEIGHTS_BEAR
    new_w = NEW_WEIGHTS_BULL if regime == "bull" else NEW_WEIGHTS_BEAR

    old_results = []
    new_results = []

    for theme, tickers in THEME_TICKERS.items():
        old = score_theme_old(theme, tickers, master_df, screened, old_w)
        new = score_theme_new(theme, tickers, master_df, screened, new_w)
        if old:
            old_results.append(old)
        if new:
            new_results.append(new)

    old_results.sort(key=lambda x: x['score'], reverse=True)
    new_results.sort(key=lambda x: x['score'], reverse=True)

    return old_results, new_results


# ═════════════════════════════════════════════════════════════════
# ANALYSIS & REPORTING
# ═════════════════════════════════════════════════════════════════

def print_header(title):
    print(f"\n{'='*90}")
    print(f"  {title}")
    print(f"{'='*90}")


def print_subheader(title):
    print(f"\n{'-'*90}")
    print(f"  {title}")
    print(f"{'-'*90}")


def analyze_single_day(date_str, old_results, new_results):
    """Detailed comparison for one day."""
    print_subheader(f"DATE: {date_str}")

    if not old_results or not new_results:
        print("  No results for this date.")
        return

    # Build rank maps
    old_rank = {r['theme']: i+1 for i, r in enumerate(old_results)}
    new_rank = {r['theme']: i+1 for i, r in enumerate(new_results)}
    old_score_map = {r['theme']: r for r in old_results}
    new_score_map = {r['theme']: r for r in new_results}

    # ── Top 15 comparison ────────────────────────────────────
    print(f"\n  {'RANK':>4}  {'OLD FORMULA':^42}  {'NEW FORMULA':^42}")
    print(f"  {'':>4}  {'Theme':<24} {'Score':>6} {'B':>2}  {'Theme':<24} {'Score':>6} {'B':>2}")
    print(f"  {'-'*4}  {'-'*24} {'─'*6} {'─'*2}  {'-'*24} {'─'*6} {'─'*2}")

    max_show = 15
    for i in range(max_show):
        o = old_results[i] if i < len(old_results) else None
        n = new_results[i] if i < len(new_results) else None

        o_name = o['theme'][:24] if o else ''
        o_score = f"{o['score']:.1f}" if o else ''
        o_b = f"{o['breadth']}" if o else ''

        n_name = n['theme'][:24] if n else ''
        n_score = f"{n['score']:.1f}" if n else ''
        n_b = f"{n['breadth']}" if n else ''

        print(f"  {i+1:>4}  {o_name:<24} {o_score:>6} {o_b:>2}  {n_name:<24} {n_score:>6} {n_b:>2}")

    # ── Biggest rank changes ─────────────────────────────────
    all_themes = set(old_rank.keys()) & set(new_rank.keys())
    rank_changes = []
    for t in all_themes:
        delta = old_rank[t] - new_rank[t]  # positive = moved UP in new
        rank_changes.append((t, old_rank[t], new_rank[t], delta))

    rank_changes.sort(key=lambda x: x[3], reverse=True)

    print(f"\n  BIGGEST RANK IMPROVEMENTS (new formula):")
    print(f"  {'Theme':<32} {'Old→New':>10} {'Delta':>6}  Why")
    print(f"  {'-'*32} {'─'*10} {'─'*6}  {'-'*30}")
    for t, orank, nrank, delta in rank_changes[:8]:
        n = new_score_map.get(t, {})
        o = old_score_map.get(t, {})
        reasons = []
        if n.get('pct_above_50sma', 0) > 70:
            reasons.append(f"50SMA={n['pct_above_50sma']:.0f}%")
        if n.get('pct_near_highs', 0) > 60:
            reasons.append(f"nearHi={n['pct_near_highs']:.0f}%")
        if n.get('vol_factor', 1.0) > 1.0:
            reasons.append(f"vol×{n['vol_factor']:.2f}")
        if n.get('breadth', 0) >= 5:
            reasons.append(f"b={n['breadth']}")
        why = ', '.join(reasons) if reasons else 'blended improvement'
        print(f"  {t:<32} {orank:>4}→{nrank:<4} {delta:>+5}  {why}")

    print(f"\n  BIGGEST RANK DROPS (new formula):")
    print(f"  {'Theme':<32} {'Old→New':>10} {'Delta':>6}  Why")
    print(f"  {'-'*32} {'─'*10} {'─'*6}  {'-'*30}")
    for t, orank, nrank, delta in rank_changes[-8:]:
        n = new_score_map.get(t, {})
        reasons = []
        if n.get('extension_factor', 1.0) < 1.0:
            reasons.append(f"ext×{n['extension_factor']:.2f}")
        if n.get('pct_above_50sma', 100) < 50:
            reasons.append(f"50SMA={n['pct_above_50sma']:.0f}%")
        if n.get('pct_near_highs', 100) < 50:
            reasons.append(f"nearHi={n['pct_near_highs']:.0f}%")
        if n.get('breadth', 5) < 3:
            reasons.append(f"b={n['breadth']}")
        why = ', '.join(reasons) if reasons else 'lower blended score'
        print(f"  {t:<32} {orank:>4}→{nrank:<4} {delta:>+5}  {why}")


def analyze_score_distributions(all_old, all_new):
    """Analyze score distributions across all days."""
    print_header("SCORE DISTRIBUTION ANALYSIS")

    old_scores = [r['score'] for day_results in all_old.values() for r in day_results]
    new_scores = [r['score'] for day_results in all_new.values() for r in day_results]

    print(f"\n  {'Metric':<25} {'Old Formula':>12} {'New Formula':>12}")
    print(f"  {'-'*25} {'─'*12} {'─'*12}")
    for label, func in [('Mean', np.mean), ('Median', np.median), ('Std Dev', np.std),
                         ('Min', np.min), ('Max', np.max),
                         ('P10', lambda x: np.percentile(x, 10)),
                         ('P25', lambda x: np.percentile(x, 25)),
                         ('P75', lambda x: np.percentile(x, 75)),
                         ('P90', lambda x: np.percentile(x, 90)),
                         ('P99', lambda x: np.percentile(x, 99))]:
        ov = func(old_scores) if old_scores else 0
        nv = func(new_scores) if new_scores else 0
        print(f"  {label:<25} {ov:>12.2f} {nv:>12.2f}")

    # Score histogram (text-based)
    print(f"\n  Score Histogram (buckets of 10):")
    print(f"  {'Bucket':<12} {'Old':>6} {'New':>6}")
    for lo in range(0, 110, 10):
        hi = lo + 10
        o_count = sum(1 for s in old_scores if lo <= s < hi)
        n_count = sum(1 for s in new_scores if lo <= s < hi)
        o_bar = '#' * min(o_count // 5, 30)
        n_bar = '#' * min(n_count // 5, 30)
        print(f"  [{lo:>3}-{hi:>3})   {o_count:>5} {n_count:>5}  OLD:{o_bar}  NEW:{n_bar}")


def analyze_component_contribution(all_new):
    """Show how much each component contributes to the new score."""
    print_header("NEW FORMULA: COMPONENT CONTRIBUTION ANALYSIS")

    # Pool top-15 themes per day
    top_entries = []
    for date, results in all_new.items():
        for r in results[:15]:
            top_entries.append(r)

    if not top_entries:
        print("  No data.")
        return

    # Average component contribution for top themes
    avg_strength = np.mean([r['c_strength'] for r in top_entries])
    avg_confirm = np.mean([r['c_confirmation'] for r in top_entries])
    avg_total = np.mean([r['score'] for r in top_entries])

    print(f"\n  Average for top-15 themes across all days:")
    print(f"    Strength component:     {avg_strength:.1f}  ({avg_strength/avg_total*100:.0f}%)")
    print(f"    Confirmation component: {avg_confirm:.1f}  ({avg_confirm/avg_total*100:.0f}%)")
    print(f"    Total score:            {avg_total:.1f}")

    # Old formula component breakdown
    print(f"\n  Comparison: OLD formula component breakdown (top-15):")
    old_top = []
    for date, results in all_old_global.items():
        for r in results[:15]:
            old_top.append(r)
    if old_top:
        avg_rs = np.mean([r['c_rs'] for r in old_top])
        avg_mom = np.mean([r['c_momentum'] for r in old_top])
        avg_br = np.mean([r['c_breadth'] for r in old_top])
        avg_t = np.mean([r['score'] for r in old_top])
        print(f"    RS component:       {avg_rs:.1f}  ({avg_rs/avg_t*100:.0f}%)")
        print(f"    Momentum component: {avg_mom:.1f}  ({avg_mom/avg_t*100:.0f}%)")
        print(f"    Breadth component:  {avg_br:.1f}  ({avg_br/avg_t*100:.0f}%)")
        print(f"    Total score:        {avg_t:.1f}")


def analyze_actionability_impact(all_new):
    """Show how extension/volume multipliers affect scoring."""
    print_header("ACTIONABILITY MULTIPLIER ANALYSIS")

    extended_themes = []
    volume_boosted = []
    neutral_themes = []

    for date, results in all_new.items():
        for r in results:
            if r['extension_factor'] < 1.0:
                extended_themes.append((date, r))
            if r['vol_factor'] > 1.0:
                volume_boosted.append((date, r))
            if r['extension_factor'] == 1.0 and r['vol_factor'] == 1.0:
                neutral_themes.append((date, r))

    print(f"\n  Across all days:")
    print(f"    Extension-penalized themes:  {len(extended_themes)}")
    print(f"    Volume-boosted themes:       {len(volume_boosted)}")
    print(f"    Neutral (no multiplier):     {len(neutral_themes)}")

    if extended_themes:
        print(f"\n  EXTENSION-PENALIZED (sample up to 15):")
        print(f"  {'Date':<12} {'Theme':<30} {'Dist25SMA':>10} {'ExtFactor':>10} {'Score':>7}")
        for date, r in extended_themes[:15]:
            print(f"  {date:<12} {r['theme'][:30]:<30} {r['avg_dist_25sma']:>9.1f}% {r['extension_factor']:>10.2f} {r['score']:>7.1f}")

    if volume_boosted:
        print(f"\n  VOLUME-BOOSTED (sample up to 15):")
        print(f"  {'Date':<12} {'Theme':<30} {'VolFactor':>10} {'Score':>7}")
        for date, r in volume_boosted[:15]:
            print(f"  {date:<12} {r['theme'][:30]:<30} {r['vol_factor']:>10.2f} {r['score']:>7.1f}")


def analyze_theme_stability(all_old, all_new):
    """Track specific themes across days to see ranking stability."""
    print_header("THEME RANKING STABILITY ACROSS DAYS")

    dates = sorted(all_old.keys())

    # Build rank-per-day for each theme
    old_ranks = defaultdict(dict)
    new_ranks = defaultdict(dict)
    for d in dates:
        for i, r in enumerate(all_old[d]):
            old_ranks[r['theme']][d] = i + 1
        for i, r in enumerate(all_new[d]):
            new_ranks[r['theme']][d] = i + 1

    # Find themes that appear in top-10 on any day
    top_themes = set()
    for d in dates:
        for r in all_old[d][:10]:
            top_themes.add(r['theme'])
        for r in all_new[d][:10]:
            top_themes.add(r['theme'])

    print(f"\n  Themes that appeared in top-10 on any day ({len(top_themes)} themes):")
    print(f"\n  OLD FORMULA RANKS:")
    header = f"  {'Theme':<30} " + " ".join(f"{d[-5:]:>6}" for d in dates) + f" {'StdDev':>7}"
    print(header)
    print(f"  {'-'*30} " + " ".join(f"{'─'*6}" for _ in dates) + f" {'─'*7}")

    rows = []
    for t in sorted(top_themes):
        ranks = [old_ranks[t].get(d) for d in dates]
        rank_strs = [f"{r:>6}" if r else f"{'--':>6}" for r in ranks]
        valid_ranks = [r for r in ranks if r is not None]
        std = np.std(valid_ranks) if len(valid_ranks) > 1 else 0
        avg = np.mean(valid_ranks) if valid_ranks else 999
        rows.append((avg, t, rank_strs, std))

    rows.sort()
    for avg, t, rank_strs, std in rows[:20]:
        print(f"  {t[:30]:<30} {' '.join(rank_strs)} {std:>7.1f}")

    print(f"\n  NEW FORMULA RANKS:")
    print(header)
    print(f"  {'-'*30} " + " ".join(f"{'─'*6}" for _ in dates) + f" {'─'*7}")

    rows = []
    for t in sorted(top_themes):
        ranks = [new_ranks[t].get(d) for d in dates]
        rank_strs = [f"{r:>6}" if r else f"{'--':>6}" for r in ranks]
        valid_ranks = [r for r in ranks if r is not None]
        std = np.std(valid_ranks) if len(valid_ranks) > 1 else 0
        avg = np.mean(valid_ranks) if valid_ranks else 999
        rows.append((avg, t, rank_strs, std))

    rows.sort()
    for avg, t, rank_strs, std in rows[:20]:
        print(f"  {t[:30]:<30} {' '.join(rank_strs)} {std:>7.1f}")


def analyze_structural_vs_momentum(all_new):
    """Check if themes with good structure but moderate RS are properly surfaced."""
    print_header("STRUCTURAL HEALTH vs PURE RS STRENGTH")

    print(f"\n  Themes with HIGH structural health (pct_above_50sma >= 80%) but MODERATE RS (median < 80):")
    print(f"  {'Date':<12} {'Theme':<30} {'Med RS':>7} {'50SMA%':>7} {'NearHi%':>8} {'Score':>7} {'Rank':>5}")

    for date, results in sorted(all_new.items()):
        rank_map = {r['theme']: i+1 for i, r in enumerate(results)}
        for r in results:
            if r['pct_above_50sma'] >= 80 and r['median_rs'] < 80 and r['breadth'] >= 3:
                rank = rank_map[r['theme']]
                print(f"  {date:<12} {r['theme'][:30]:<30} {r['median_rs']:>7.1f} {r['pct_above_50sma']:>7.0f} {r['pct_near_highs']:>8.0f} {r['score']:>7.1f} {rank:>5}")

    print(f"\n  Themes with HIGH RS (median >= 90) but LOW structural health (pct_above_50sma < 50%):")
    print(f"  {'Date':<12} {'Theme':<30} {'Med RS':>7} {'50SMA%':>7} {'NearHi%':>8} {'Score':>7} {'Rank':>5}")

    for date, results in sorted(all_new.items()):
        rank_map = {r['theme']: i+1 for i, r in enumerate(results)}
        for r in results:
            if r['median_rs'] >= 90 and r['pct_above_50sma'] < 50 and r['breadth'] >= 2:
                rank = rank_map[r['theme']]
                print(f"  {date:<12} {r['theme'][:30]:<30} {r['median_rs']:>7.1f} {r['pct_above_50sma']:>7.0f} {r['pct_near_highs']:>8.0f} {r['score']:>7.1f} {rank:>5}")


def analyze_breadth_impact(all_old, all_new):
    """Compare how breadth is valued between old and new formulas."""
    print_header("BREADTH IMPACT ANALYSIS")

    print(f"\n  Average score by breadth bucket (across all days):")
    print(f"\n  {'Breadth':<10} {'Old Avg':>8} {'Old Med':>8} {'New Avg':>8} {'New Med':>8} {'Count':>6}")
    print(f"  {'-'*10} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*6}")

    old_by_breadth = defaultdict(list)
    new_by_breadth = defaultdict(list)

    for date in all_old:
        old_map = {r['theme']: r for r in all_old[date]}
        new_map = {r['theme']: r for r in all_new[date]}
        for theme in old_map:
            b = old_map[theme]['breadth']
            if b == 1:
                bucket = '1'
            elif b == 2:
                bucket = '2'
            elif b <= 4:
                bucket = '3-4'
            elif b <= 7:
                bucket = '5-7'
            elif b <= 10:
                bucket = '8-10'
            else:
                bucket = '11+'
            old_by_breadth[bucket].append(old_map[theme]['score'])
            if theme in new_map:
                new_by_breadth[bucket].append(new_map[theme]['score'])

    for bucket in ['1', '2', '3-4', '5-7', '8-10', '11+']:
        os = old_by_breadth.get(bucket, [])
        ns = new_by_breadth.get(bucket, [])
        o_avg = np.mean(os) if os else 0
        o_med = np.median(os) if os else 0
        n_avg = np.mean(ns) if ns else 0
        n_med = np.median(ns) if ns else 0
        cnt = len(os)
        print(f"  {bucket:<10} {o_avg:>8.1f} {o_med:>8.1f} {n_avg:>8.1f} {n_med:>8.1f} {cnt:>6}")


def analyze_specific_themes(all_old, all_new):
    """Deep dive into well-known theme categories."""
    print_header("SPECIFIC THEME DEEP DIVES")

    # Themes of interest for momentum trading
    interest_patterns = [
        'AI', 'Drones', 'Nuclear', 'Quantum', 'Cybersecurity', 'Space',
        'Mining / Gold', 'Cryptocurrency', 'Solar', 'Biotech', 'Strategic Minerals'
    ]

    dates = sorted(all_old.keys())

    for pattern in interest_patterns:
        matching = [t for t in THEME_TICKERS if pattern.lower() in t.lower()]
        if not matching:
            continue

        print(f"\n  Pattern: '{pattern}' — {len(matching)} matching themes")

        for theme in sorted(matching)[:5]:
            old_data = {}
            new_data = {}
            for d in dates:
                for r in all_old[d]:
                    if r['theme'] == theme:
                        old_data[d] = r
                for r in all_new[d]:
                    if r['theme'] == theme:
                        new_data[d] = r

            if not new_data:
                continue

            latest = sorted(new_data.keys())[-1]
            n = new_data[latest]
            o = old_data.get(latest)
            o_score = o['score'] if o else 0

            print(f"\n    {theme}  (breadth={n['breadth']}, total_roster={len(THEME_TICKERS[theme])})")
            print(f"      Latest ({latest}): OLD={o_score:.1f}  NEW={n['score']:.1f}")
            print(f"      Strength={n['strength']:.1f} (medRS={n['median_rs']:.1f}, leader%={n['leader_pct']:.0f})")
            print(f"      Confirmation={n['confirmation']:.1f} (50SMA%={n['pct_above_50sma']:.0f}, nearHi%={n['pct_near_highs']:.0f}, bScore={n['breadth_score']:.0f})")
            print(f"      Actionability={n['actionability']:.2f} (ext={n['extension_factor']:.2f}, vol={n['vol_factor']:.2f})")
            print(f"      Dist 25SMA: {n['avg_dist_25sma']:.1f}%")


def analyze_bear_market_simulation(dates):
    """Re-run scoring with bear market weights and compare."""
    print_header("BEAR MARKET REGIME SIMULATION")

    # Pick the latest date
    date_str = dates[-1]
    master_df, screened = load_day_data(date_str)
    if master_df is None:
        print("  No data available.")
        return

    # Run with bear weights
    old_bear = []
    new_bear = []
    for theme, tickers in THEME_TICKERS.items():
        o = score_theme_old(theme, tickers, master_df, screened, OLD_WEIGHTS_BEAR)
        n = score_theme_new(theme, tickers, master_df, screened, NEW_WEIGHTS_BEAR)
        if o:
            old_bear.append(o)
        if n:
            new_bear.append(n)

    # Old bear sorts by market_relative_score (avg_rs * breadth_penalty)
    # We simulate that here
    for r in old_bear:
        r['bear_sort_key'] = r['median_rs'] * r['breadth_penalty']
    old_bear.sort(key=lambda x: x['bear_sort_key'], reverse=True)
    new_bear.sort(key=lambda x: x['score'], reverse=True)

    print(f"\n  Date: {date_str}, Bear Market Weights")
    print(f"\n  {'RANK':>4}  {'OLD (sorted by mkt_rel_score)':^42}  {'NEW (sorted by score)':^42}")
    print(f"  {'':>4}  {'Theme':<24} {'MRS':>6} {'B':>2}  {'Theme':<24} {'Score':>6} {'B':>2}")
    print(f"  {'-'*4}  {'-'*24} {'─'*6} {'─'*2}  {'-'*24} {'─'*6} {'─'*2}")

    for i in range(15):
        o = old_bear[i] if i < len(old_bear) else None
        n = new_bear[i] if i < len(new_bear) else None
        o_name = o['theme'][:24] if o else ''
        o_score = f"{o['bear_sort_key']:.1f}" if o else ''
        o_b = f"{o['breadth']}" if o else ''
        n_name = n['theme'][:24] if n else ''
        n_score = f"{n['score']:.1f}" if n else ''
        n_b = f"{n['breadth']}" if n else ''
        print(f"  {i+1:>4}  {o_name:<24} {o_score:>6} {o_b:>2}  {n_name:<24} {n_score:>6} {n_b:>2}")


def compute_rank_correlation(all_old, all_new):
    """Spearman rank correlation between old and new for each day."""
    print_header("RANK CORRELATION (Old vs New)")

    from scipy import stats

    dates = sorted(all_old.keys())
    print(f"\n  {'Date':<12} {'Spearman r':>12} {'p-value':>12} {'Themes':>8}")
    print(f"  {'-'*12} {'─'*12} {'─'*12} {'─'*8}")

    for d in dates:
        old_map = {r['theme']: r['score'] for r in all_old[d]}
        new_map = {r['theme']: r['score'] for r in all_new[d]}
        common = set(old_map.keys()) & set(new_map.keys())
        if len(common) < 5:
            continue
        old_vals = [old_map[t] for t in common]
        new_vals = [new_map[t] for t in common]
        r, p = stats.spearmanr(old_vals, new_vals)
        print(f"  {d:<12} {r:>12.4f} {p:>12.6f} {len(common):>8}")


def generate_findings_report(all_old, all_new, dates):
    """Generate the final findings summary."""
    print_header("FINDINGS SUMMARY")

    # 1. How different are the rankings?
    all_deltas = []
    for d in dates:
        old_rank = {r['theme']: i+1 for i, r in enumerate(all_old[d])}
        new_rank = {r['theme']: i+1 for i, r in enumerate(all_new[d])}
        common = set(old_rank.keys()) & set(new_rank.keys())
        for t in common:
            all_deltas.append(abs(old_rank[t] - new_rank[t]))

    avg_delta = np.mean(all_deltas) if all_deltas else 0
    med_delta = np.median(all_deltas) if all_deltas else 0
    max_delta = np.max(all_deltas) if all_deltas else 0

    print(f"\n  1. RANKING DIVERGENCE:")
    print(f"     Average rank change:  {avg_delta:.1f} positions")
    print(f"     Median rank change:   {med_delta:.1f} positions")
    print(f"     Max rank change:      {max_delta:.0f} positions")

    # 2. Score range improvement
    old_ranges = []
    new_ranges = []
    for d in dates:
        if all_old[d]:
            old_ranges.append(all_old[d][0]['score'] - all_old[d][-1]['score'])
        if all_new[d]:
            new_ranges.append(all_new[d][0]['score'] - all_new[d][-1]['score'])

    print(f"\n  2. SCORE DISCRIMINATION (spread between #1 and last):")
    print(f"     Old: avg spread = {np.mean(old_ranges):.1f}")
    print(f"     New: avg spread = {np.mean(new_ranges):.1f}")

    # 3. Extension penalty impact
    ext_count = 0
    ext_themes = set()
    for d in dates:
        for r in all_new[d]:
            if r['extension_factor'] < 1.0:
                ext_count += 1
                ext_themes.add(r['theme'])

    print(f"\n  3. EXTENSION PENALTY ACTIVATIONS:")
    print(f"     Total instances: {ext_count} (across {len(ext_themes)} unique themes)")

    # 4. Volume bonus impact
    vol_count = 0
    vol_themes = set()
    for d in dates:
        for r in all_new[d]:
            if r['vol_factor'] > 1.0:
                vol_count += 1
                vol_themes.add(r['theme'])

    print(f"\n  4. VOLUME BONUS ACTIVATIONS:")
    print(f"     Total instances: {vol_count} (across {len(vol_themes)} unique themes)")

    # 5. Top-5 stability
    old_top5_sets = []
    new_top5_sets = []
    for d in dates:
        old_top5_sets.append(set(r['theme'] for r in all_old[d][:5]))
        new_top5_sets.append(set(r['theme'] for r in all_new[d][:5]))

    # Jaccard similarity between consecutive days
    old_stability = []
    new_stability = []
    for i in range(1, len(dates)):
        if old_top5_sets[i] and old_top5_sets[i-1]:
            inter = len(old_top5_sets[i] & old_top5_sets[i-1])
            union = len(old_top5_sets[i] | old_top5_sets[i-1])
            old_stability.append(inter / union if union else 0)
        if new_top5_sets[i] and new_top5_sets[i-1]:
            inter = len(new_top5_sets[i] & new_top5_sets[i-1])
            union = len(new_top5_sets[i] | new_top5_sets[i-1])
            new_stability.append(inter / union if union else 0)

    print(f"\n  5. TOP-5 DAY-OVER-DAY STABILITY (Jaccard similarity):")
    print(f"     Old: {np.mean(old_stability):.3f} avg")
    print(f"     New: {np.mean(new_stability):.3f} avg")
    print(f"     (1.0 = identical top-5 every day, 0.0 = completely different)")


# ═════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    dates = get_available_dates()
    print(f"Available dates: {dates}")
    print(f"Total themes in taxonomy: {len(THEME_TICKERS)}")

    # Run both formulas on all dates
    all_old = {}
    all_new = {}

    for d in dates:
        old_results, new_results = run_scoring_for_date(d, regime="bull")
        all_old[d] = old_results
        all_new[d] = new_results
        print(f"  {d}: {len(old_results)} themes scored")

    # Store globally for cross-function access
    all_old_global = all_old

    # ── Run all analyses ─────────────────────────────────────
    # 1. Per-day comparisons
    print_header("PER-DAY RANKING COMPARISON")
    for d in dates:
        analyze_single_day(d, all_old[d], all_new[d])

    # 2. Score distributions
    analyze_score_distributions(all_old, all_new)

    # 3. Component contribution
    analyze_component_contribution(all_new)

    # 4. Actionability multiplier
    analyze_actionability_impact(all_new)

    # 5. Theme stability across days
    analyze_theme_stability(all_old, all_new)

    # 6. Structural vs momentum
    analyze_structural_vs_momentum(all_new)

    # 7. Breadth impact
    analyze_breadth_impact(all_old, all_new)

    # 8. Specific theme deep dives
    analyze_specific_themes(all_old, all_new)

    # 9. Bear market simulation
    analyze_bear_market_simulation(dates)

    # 10. Rank correlation
    try:
        compute_rank_correlation(all_old, all_new)
    except ImportError:
        print("\n  (scipy not installed, skipping rank correlation)")

    # 11. Final findings
    generate_findings_report(all_old, all_new, dates)
