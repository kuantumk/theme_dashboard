"""
Two-track theme scoring.

Composite score (per ticker) — pure trend strength, equal-weighted:
- rs_score:    rs_sts_pct from master table (0-100)
- vars_score:  raw VARS from master table (volatility-adjusted, used as-is)
Composite = mean(rs_score, vars_score).

Demand score (per ticker) — equal-weighted supply pressure:
- si_score:    short_interest from fundamentals.db, mapped via si_anchors
- float_score: shares_float from fundamentals.db, mapped via float_anchors
Demand = mean(si_score, float_score).

Theme score = mean of top-N (max_scoring_tickers) composites among the
theme's screened, in-master members (top by composite desc). Within-theme
ticker display order is sorted by demand score desc.
"""

import sqlite3
from glob import glob
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

import src.stock_utils as su
from collections import defaultdict
from config.settings import CONFIG, SCREENING_OUTPUT_DIR, DATA_DIR, FUNDAMENTALS_DB
from src.themes.theme_registry import load_ticker_themes
from src.themes.theme_taxonomy import build_theme_to_tickers, load_theme_groups

# ── Config ──────────────────────────────────────────────────────────────
HOT_THRESHOLD = CONFIG["themes"]["hot_theme_rs_threshold"]
MOMENTUM_THRESHOLD = CONFIG["themes"]["high_momentum_threshold"]
MIN_BREADTH = CONFIG["themes"].get("min_scored_breadth", 2)

SCORING_CFG = CONFIG["themes"].get("scoring", {})
MAX_SCORING_TICKERS = SCORING_CFG.get("max_scoring_tickers", 10)
SI_ANCHORS = SCORING_CFG.get("si_anchors", [[0, 0], [5, 25], [10, 50], [20, 100]])
FLOAT_ANCHORS = SCORING_CFG.get("float_anchors", [[100, 100], [150, 75], [500, 50]])
MISSING_DEFAULT = float(SCORING_CFG.get("missing_default", 50))

# Pre-split anchor arrays for np.interp
_SI_X = np.array([a[0] for a in SI_ANCHORS], dtype=float)
_SI_Y = np.array([a[1] for a in SI_ANCHORS], dtype=float)
_FL_X = np.array([a[0] for a in FLOAT_ANCHORS], dtype=float)
_FL_Y = np.array([a[1] for a in FLOAT_ANCHORS], dtype=float)


# ── Fundamentals fetch ──────────────────────────────────────────────────

def _get_fundamentals_data(tickers: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """Bulk-load short_interest and shares_float for the given tickers.

    Returns {ticker: {"si": pct_or_None, "fl": raw_count_or_None}}.
    """
    if not tickers:
        return {}
    if not FUNDAMENTALS_DB.exists():
        return {}
    try:
        conn = sqlite3.connect(str(FUNDAMENTALS_DB))
        placeholders = ",".join(["?"] * len(tickers))
        rows = conn.execute(
            f"SELECT ticker, short_interest, shares_float FROM fundamentals WHERE ticker IN ({placeholders})",
            tickers,
        ).fetchall()
        conn.close()
    except sqlite3.Error:
        return {}
    return {r[0]: {"si": r[1], "fl": r[2]} for r in rows}


# ── Signal scoring ──────────────────────────────────────────────────────

def _is_missing(v) -> bool:
    return v is None or (isinstance(v, float) and np.isnan(v))


def _rs_score(rs) -> float:
    if _is_missing(rs):
        return MISSING_DEFAULT
    return float(np.clip(rs, 0, 100))


def _vars_score(vars_raw) -> float:
    """Raw VARS — volatility-adjusted, comparable across tickers, used as-is."""
    if _is_missing(vars_raw):
        return MISSING_DEFAULT
    return float(vars_raw)


def _si_score(si_pct) -> float:
    """Short interest (%) → 0-100 via piecewise anchors. Missing → 50."""
    if _is_missing(si_pct):
        return MISSING_DEFAULT
    return float(np.clip(np.interp(si_pct, _SI_X, _SI_Y), 0, 100))


def _float_score(shares_float) -> float:
    """Shares float (raw count) → 0-100 via piecewise anchors on millions.

    Below the smallest anchor (100M) clamps to anchor's score (100) — low float bonus.
    Above the largest anchor (500M) clamps to anchor's score (50) — no penalty.
    """
    if _is_missing(shares_float):
        return MISSING_DEFAULT
    fl_m = float(shares_float) / 1e6
    return float(np.clip(np.interp(fl_m, _FL_X, _FL_Y), _FL_Y.min(), _FL_Y.max()))


def _ticker_composite(rs, vars_raw) -> float:
    """Strength composite — equal-weighted RS and VARS."""
    return (_rs_score(rs) + _vars_score(vars_raw)) / 2.0


def _ticker_demand(si, fl) -> float:
    """Demand composite — equal-weighted short interest and float."""
    return (_si_score(si) + _float_score(fl)) / 2.0


# ── Theme grouping (legacy helper kept for backward compat) ─────────────

def group_tickers_by_theme(ticker_themes: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Reverse mapping: theme -> list of tickers (no consolidation)."""
    theme_tickers = defaultdict(list)
    for ticker, themes in ticker_themes.items():
        for theme in themes:
            theme_tickers[theme].append(ticker)
    return dict(theme_tickers)


# ── Core scoring ────────────────────────────────────────────────────────

def calculate_theme_metrics(
    theme: str,
    tickers: List[str],
    master_df: pd.DataFrame,
    fundamentals: Dict[str, Dict[str, Optional[float]]],
    screened_tickers: Optional[set] = None,
) -> Optional[Dict]:
    """Per-ticker equal-weighted composite, theme score = mean of top-N composites."""
    scoring_tickers = list(set(tickers) & screened_tickers) if screened_tickers is not None else list(tickers)
    theme_df = master_df[master_df['ticker'].isin(scoring_tickers)]

    total_breadth = len(tickers)
    full_breadth = len(theme_df)

    if full_breadth < MIN_BREADTH:
        return None

    # Per-ticker composite for every surviving member
    rs_col = theme_df['rs_sts_pct'].values
    vars_col = theme_df['vars'].values if 'vars' in theme_df.columns else np.full(full_breadth, np.nan)
    tickers_arr = theme_df['ticker'].values

    composites = []
    demands = []
    for i, tk in enumerate(tickers_arr):
        f = fundamentals.get(tk, {})
        composites.append(_ticker_composite(rs_col[i], vars_col[i]))
        demands.append(_ticker_demand(f.get('si'), f.get('fl')))

    ticker_scores = {tk: float(c) for tk, c in zip(tickers_arr, composites)}
    ticker_demands = {tk: float(d) for tk, d in zip(tickers_arr, demands)}

    # Theme score = mean of top-N composites (strongest tickers).
    ranked_by_composite = sorted(ticker_scores.items(), key=lambda kv: -kv[1])
    top_n = ranked_by_composite[:MAX_SCORING_TICKERS]
    breadth = len(top_n)
    theme_score = float(np.mean([c for _, c in top_n]))

    # Reporting fields kept for daily report + downstream compat
    rs_values = rs_col
    avg_rs = float(np.mean(rs_values))
    median_rs = float(np.median(rs_values))
    high_momentum_count = int(np.sum(rs_values > MOMENTUM_THRESHOLD))
    high_momentum_pct = (high_momentum_count / len(rs_values)) * 100

    # Daily-report top_stocks: top 3 by demand desc (within-theme display order).
    rs_lookup = dict(zip(tickers_arr, rs_col))
    ranked_by_demand = sorted(ticker_demands.items(), key=lambda kv: -kv[1])
    top_stocks = []
    for tk, d in ranked_by_demand[:3]:
        top_stocks.append({
            'ticker': tk,
            'score': round(ticker_scores.get(tk, 0.0), 2),
            'demand': round(d, 2),
            'rs_sts_pct': float(rs_lookup.get(tk, 0)),
        })

    return {
        'theme': theme,
        'score': theme_score,
        'strength_score': theme_score,  # alias for legacy consumers
        'final_score': theme_score,     # alias for legacy consumers
        'avg_rs_sts': avg_rs,
        'median_rs_sts': median_rs,
        'breadth': breadth,
        'total_breadth': total_breadth,
        'tickers': tickers,
        'ticker_scores': ticker_scores,
        'ticker_demands': ticker_demands,
        'top_stocks': top_stocks,
        'high_momentum_count': high_momentum_count,
        'high_momentum_pct': high_momentum_pct,
    }


def analyze_theme_strength(
    master_df: pd.DataFrame,
    market_breadth: Dict = None,
    screened_tickers: set = None,
) -> pd.DataFrame:
    """Score every theme and return a ranked DataFrame (highest score first)."""
    ticker_themes = load_ticker_themes()
    if not ticker_themes:
        print("No ticker themes found")
        return pd.DataFrame()

    theme_groups = load_theme_groups()
    theme_tickers = build_theme_to_tickers(ticker_themes, theme_groups)

    print(f"Analyzing {len(theme_tickers)} themes (after consolidation)...")

    # Bulk fundamentals fetch — one DB query covering every candidate ticker.
    all_candidates = set()
    for tks in theme_tickers.values():
        all_candidates.update(tks)
    if screened_tickers is not None:
        all_candidates &= set(screened_tickers)
    fundamentals = _get_fundamentals_data(sorted(all_candidates))

    theme_metrics = []
    for theme, tickers in theme_tickers.items():
        m = calculate_theme_metrics(theme, tickers, master_df, fundamentals, screened_tickers)
        if m:
            theme_metrics.append(m)

    theme_df = pd.DataFrame(theme_metrics)
    if theme_df.empty:
        return theme_df

    theme_df = theme_df.sort_values('score', ascending=False).reset_index(drop=True)
    theme_df['is_hot'] = (theme_df['avg_rs_sts'] > HOT_THRESHOLD) & (theme_df['breadth'] >= 3)
    return theme_df


def get_hot_themes(theme_df: pd.DataFrame) -> pd.DataFrame:
    return theme_df[theme_df['is_hot']]


if __name__ == '__main__':
    master_files = sorted(glob(str(SCREENING_OUTPUT_DIR / 'master' / 'master_*.csv')))
    if not master_files:
        print("No master tables found. Run create_master_table.py first.")
    else:
        latest_master = master_files[-1]
        print(f"Loading {latest_master}")
        master_df = pd.read_csv(latest_master)
        theme_df = analyze_theme_strength(master_df)

        print(f"\n{'='*80}")
        print("THEME STRENGTH ANALYSIS")
        print(f"{'='*80}\n")
        print(f"Total themes: {len(theme_df)}")
        if not theme_df.empty:
            print(f"Hot themes (RS > {HOT_THRESHOLD}%): {theme_df['is_hot'].sum()}\n")
            pd.options.display.float_format = '{:.1f}'.format
            print("Top 15 themes by score:")
            print(theme_df[['theme', 'score', 'avg_rs_sts', 'breadth']].head(15).to_string())
