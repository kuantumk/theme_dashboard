"""
Analyze theme strength based on RS_STS% scores — Iteration 9d.

Scores themes along three dimensions for momentum trading:
- Strength: RS quality, leader concentration (RS >= 80), and participation
- Confirmation: structural uptrend health, proximity to 60-day highs, active breadth
- Actionability: sigmoid extension penalty and volume expansion bonus

Five multipliers refine the final score:
- RS acceleration (5-day momentum)
- Breadth momentum (5-day expansion)
- Enrichment ratio (theme concentration vs market)
- Short interest / squeeze potential (breadth <= 6 only)
- Breadth penalty (breadth == 2 discount)

Designed to surface themes suitable for Qullamaggie / Marios Stamatoudis
style breakout trading: strong RS, broad participation, tight near highs.
"""

import json
import sqlite3
from datetime import datetime, timezone
from math import exp
from pathlib import Path

import numpy as np
import pandas as pd
from typing import Dict, List, Optional
from collections import defaultdict
from glob import glob

import src.stock_utils as su
from config.settings import CONFIG, SCREENING_OUTPUT_DIR, DATA_DIR, FUNDAMENTALS_DB
from src.themes.theme_registry import load_ticker_themes
from src.themes.theme_taxonomy import build_theme_to_tickers, load_theme_groups

# ── Config ──────────────────────────────────────────────────────────────
HOT_THRESHOLD = CONFIG["themes"]["hot_theme_rs_threshold"]
MOMENTUM_THRESHOLD = CONFIG["themes"]["high_momentum_threshold"]
MIN_BREADTH = CONFIG["themes"].get("min_scored_breadth", 2)

SCORING_CFG = CONFIG["themes"].get("scoring", {})
MAX_SCORING_TICKERS = SCORING_CFG.get("max_scoring_tickers", 10)
LEADER_RS_THRESHOLD = SCORING_CFG.get("leader_rs_threshold", 80)
NEAR_HIGHS_COL = SCORING_CFG.get("near_highs_column", "max60")
STRENGTH_COMPONENTS = SCORING_CFG.get("strength_components", {"median_rs": 0.40, "leader_pct": 0.35, "participation": 0.25})
EXT_SIGMOID = SCORING_CFG.get("extension_sigmoid", {"midpoint": 15, "steepness": 5})
VOL_CFG = SCORING_CFG.get("volume_thresholds", {"high": 1.5, "medium": 1.2, "high_factor": 1.15, "medium_factor": 1.08})
BREADTH_PENALTY_TWO = SCORING_CFG.get("breadth_penalty_two", 0.75)
RS_ACCEL_CFG = SCORING_CFG.get("rs_acceleration", {"lookback": 5, "coeff": 0.015, "min": 0.85, "max": 1.35})
BREADTH_MOM_CFG = SCORING_CFG.get("breadth_momentum", {"lookback": 5, "coeff": 0.05, "min": 0.90, "max": 1.25})
ENRICHMENT_CFG = SCORING_CFG.get("enrichment", {"coeff": 0.08, "max": 1.35})
SI_CFG = SCORING_CFG.get("short_interest", {"max_breadth": 6, "min_si": 0.07, "coeff": 2.5, "max_bonus": 0.45})
HISTORY_FILE = Path(SCORING_CFG.get("history_file", "data/theme_score_history.json"))

REGIME_CFG = CONFIG["themes"].get("regime", {})
REGIME_SOURCE = REGIME_CFG.get("source", "master_table")
REGIME_THRESHOLDS = REGIME_CFG.get("thresholds", {"strong_bull": 60, "bull": 40, "choppy": 25})
REGIME_WEIGHTS = REGIME_CFG.get("weights", {
    "strong_bull": {"strength": 0.50, "confirmation": 0.50},
    "bull": {"strength": 0.50, "confirmation": 0.50},
    "choppy": {"strength": 0.70, "confirmation": 0.30},
    "bear": {"strength": 0.80, "confirmation": 0.20},
})

# Legacy weights (used only as fallback)
LEGACY_WEIGHTS = CONFIG["themes"].get("strength_weights", {})


# ── History persistence ─────────────────────────────────────────────────

def _load_score_history() -> Dict:
    """Load theme score history from disk (last 10 days)."""
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def _save_score_history(history: Dict, today_key: str, today_data: Dict) -> None:
    """Save today's metrics and prune entries older than 10 days."""
    history[today_key] = today_data
    sorted_dates = sorted(history.keys(), reverse=True)[:10]
    pruned = {d: history[d] for d in sorted_dates}
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "w") as f:
        json.dump(pruned, f, indent=2)


def _get_historical_value(history: Dict, theme: str, field: str, lookback: int = 5) -> Optional[float]:
    """Look up a theme's metric from ~lookback trading days ago."""
    sorted_dates = sorted(history.keys(), reverse=True)
    # sorted_dates[0] is today (most recent). We want the entry at index >= lookback.
    # If exact offset is missing (weekends/holidays), use the nearest older entry.
    for date_key in sorted_dates[1:]:  # skip today (index 0)
        if sorted_dates.index(date_key) >= lookback:
            theme_data = history.get(date_key, {}).get(theme)
            if theme_data and field in theme_data:
                return theme_data[field]
    # Fallback: use the oldest available entry if we have at least 2 days
    if len(sorted_dates) > 1:
        oldest = sorted_dates[-1]
        theme_data = history.get(oldest, {}).get(theme)
        if theme_data and field in theme_data:
            return theme_data[field]
    return None


# ── Market regime ───────────────────────────────────────────────────────

def compute_market_regime(master_df: pd.DataFrame, market_breadth: Dict = None) -> tuple:
    """Compute 4-tier market regime.

    Returns (regime_name, weights_dict, pct_above_50sma).
    """
    if REGIME_SOURCE == "master_table":
        pct = 50.0  # default
        if 'close' in master_df.columns and 'sma50' in master_df.columns:
            valid = master_df[~master_df['sma50'].isna()]
            if len(valid) > 0:
                pct = (np.sum(valid['close'] > valid['sma50']) / len(valid)) * 100
    else:
        # MMFI fallback
        pct = 50.0
        if market_breadth and 'mmfi' in market_breadth and market_breadth['mmfi'] is not None:
            pct = market_breadth['mmfi']

    if pct > REGIME_THRESHOLDS["strong_bull"]:
        regime = "strong_bull"
    elif pct > REGIME_THRESHOLDS["bull"]:
        regime = "bull"
    elif pct > REGIME_THRESHOLDS["choppy"]:
        regime = "choppy"
    else:
        regime = "bear"

    weights = REGIME_WEIGHTS.get(regime, {"strength": 0.50, "confirmation": 0.50})
    return regime, weights, pct


# ── Short interest ──────────────────────────────────────────────────────

def _get_short_interest_data(tickers: List[str]) -> Dict[str, float]:
    """Load short interest from fundamentals DB for the given tickers."""
    if not tickers:
        return {}
    if not FUNDAMENTALS_DB.exists():
        return {}
    try:
        conn = sqlite3.connect(str(FUNDAMENTALS_DB))
        placeholders = ",".join(["?"] * len(tickers))
        cursor = conn.execute(
            f"SELECT ticker, short_interest FROM fundamentals WHERE ticker IN ({placeholders})",
            tickers,
        )
        result = {}
        for row in cursor.fetchall():
            if row[1] is not None:
                result[row[0]] = row[1]
        conn.close()
        return result
    except Exception:
        return {}


def _compute_si_multiplier(scoring_tickers: List[str], breadth: int) -> tuple:
    """Compute short interest squeeze multiplier.

    Only applies for themes with breadth <= max_breadth (default 6).
    Returns (si_mult, median_si).
    """
    if breadth > SI_CFG["max_breadth"]:
        return 1.0, 0.0

    si_data = _get_short_interest_data(scoring_tickers)
    if not si_data:
        return 1.0, 0.0

    # Finviz stores as percentage (12.5 = 12.5%), convert to fraction
    si_values = [v / 100.0 for v in si_data.values() if v > 0]
    if not si_values:
        return 1.0, 0.0

    median_si = float(np.median(si_values))
    if median_si <= SI_CFG["min_si"]:
        return 1.0, median_si

    boost = min(SI_CFG["max_bonus"], (median_si - SI_CFG["min_si"]) * SI_CFG["coeff"])
    return 1.0 + boost, median_si


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
    active_weights: Dict[str, float],
    screened_tickers: set = None,
    history: Dict = None,
    total_screened: int = 0,
    total_universe: int = 0,
) -> Optional[Dict]:
    """
    Calculate aggregate metrics for a single theme — Iteration 9d formula.

    Scores along three axes:
    - Strength (0-100): median RS + leader concentration (RS >= 80) + participation
    - Confirmation (0-100): structural health, near-highs proximity, active breadth
    - Actionability (multiplier): sigmoid extension penalty + volume bonus

    Five multipliers applied to the final score:
    - RS acceleration, breadth momentum, enrichment ratio, SI squeeze, breadth penalty
    """
    scoring_tickers = list(set(tickers) & screened_tickers) if screened_tickers is not None else tickers
    theme_df = master_df[master_df['ticker'].isin(scoring_tickers)]

    if len(theme_df) == 0:
        return None

    total_breadth = len(tickers)
    breadth = len(theme_df)

    if breadth < MIN_BREADTH:
        return None

    # Cap large themes to prevent dilution — top N by RS
    if breadth > MAX_SCORING_TICKERS:
        theme_df = theme_df.nlargest(MAX_SCORING_TICKERS, 'rs_sts_pct')

    rs_values = theme_df['rs_sts_pct'].values

    # Base RS metrics (kept for reporting compatibility)
    avg_rs = float(np.mean(rs_values))
    median_rs = float(np.median(rs_values))
    high_momentum_count = int(np.sum(rs_values > MOMENTUM_THRESHOLD))
    high_momentum_pct = (high_momentum_count / len(rs_values)) * 100

    # ── STRENGTH (0-100) ─────────────────────────────────────────────
    leader_count = int(np.sum(rs_values >= LEADER_RS_THRESHOLD))
    leader_pct = (leader_count / len(rs_values)) * 100
    participation = min((breadth / max(total_breadth, 1)) * 100, 100)

    strength = (
        STRENGTH_COMPONENTS["median_rs"] * median_rs +
        STRENGTH_COMPONENTS["leader_pct"] * leader_pct +
        STRENGTH_COMPONENTS["participation"] * participation
    )

    # ── CONFIRMATION (0-100) ─────────────────────────────────────────
    # B1: Structural health — % above 50SMA (Weinstein Stage 2 proxy)
    pct_above_50sma = 0.0
    if 'close' in theme_df.columns and 'sma50' in theme_df.columns:
        valid = theme_df[~theme_df['sma50'].isna()]
        if len(valid) > 0:
            pct_above_50sma = (np.sum(valid['close'] > valid['sma50']) / len(valid)) * 100

    # B2: Near highs — % within 15% of 60-day high (breakout proximity)
    pct_near_highs = 0.0
    near_highs_col = NEAR_HIGHS_COL if NEAR_HIGHS_COL in theme_df.columns else 'max252'
    if 'close' in theme_df.columns and near_highs_col in theme_df.columns:
        valid = theme_df[~theme_df[near_highs_col].isna()]
        if len(valid) > 0:
            pct_near_highs = (np.sum(valid['close'] >= 0.85 * valid[near_highs_col]) / len(valid)) * 100

    # B3: Breadth — linear scale, 5+ screened stocks = full credit
    breadth_score = min(breadth / 5.0, 1.0) * 100

    confirmation = 0.35 * pct_above_50sma + 0.35 * pct_near_highs + 0.30 * breadth_score

    # ── ACTIONABILITY (multiplier) ───────────────────────────────────
    # Sigmoid extension penalty — smooth transition centered at midpoint
    dist_25sma = np.nan
    if 'close' in theme_df.columns and 'sma25' in theme_df.columns:
        valid = theme_df[theme_df['sma25'] > 0]
        if not valid.empty:
            pct_from_25sma = ((valid['close'] - valid['sma25']) / valid['sma25']) * 100
            dist_25sma = float(np.nanmedian(pct_from_25sma))

    extension_factor = 1.0
    if not np.isnan(dist_25sma):
        extension_factor = 1.0 / (1.0 + exp((dist_25sma - EXT_SIGMOID["midpoint"]) / EXT_SIGMOID["steepness"]))

    # Volume expansion bonus — institutional accumulation signal
    vol_factor = 1.0
    if 'vol_sma40' in theme_df.columns and 'vol_sma252' in theme_df.columns:
        valid = theme_df[(theme_df['vol_sma252'] > 0) & (~theme_df['vol_sma252'].isna())]
        if not valid.empty:
            avg_vol_ratio = float(np.nanmean(valid['vol_sma40'] / valid['vol_sma252']))
            if avg_vol_ratio > VOL_CFG["high"]:
                vol_factor = VOL_CFG["high_factor"]
            elif avg_vol_ratio > VOL_CFG["medium"]:
                vol_factor = VOL_CFG["medium_factor"]

    actionability = extension_factor * vol_factor

    # ── BREADTH PENALTY ──────────────────────────────────────────────
    if breadth >= 3:
        breadth_penalty = 1.0
    elif breadth == 2:
        breadth_penalty = BREADTH_PENALTY_TWO
    else:
        return None

    # ── RS ACCELERATION (5-day momentum) ─────────────────────────────
    rs_accel_mult = 1.0
    rs_accel = 0.0
    if history:
        hist_rs = _get_historical_value(history, theme, "median_rs", RS_ACCEL_CFG["lookback"])
        if hist_rs is not None:
            rs_accel = median_rs - hist_rs
            rs_accel_mult = max(RS_ACCEL_CFG["min"], min(RS_ACCEL_CFG["max"], 1.0 + RS_ACCEL_CFG["coeff"] * rs_accel))

    # ── BREADTH MOMENTUM (5-day expansion) ───────────────────────────
    breadth_momentum_mult = 1.0
    breadth_delta = 0
    if history:
        hist_breadth = _get_historical_value(history, theme, "breadth", BREADTH_MOM_CFG["lookback"])
        if hist_breadth is not None:
            breadth_delta = breadth - hist_breadth
            breadth_momentum_mult = max(BREADTH_MOM_CFG["min"], min(BREADTH_MOM_CFG["max"], 1.0 + BREADTH_MOM_CFG["coeff"] * breadth_delta))

    # ── ENRICHMENT RATIO ─────────────────────────────────────────────
    enrichment = 0.0
    enrichment_mult = 1.0
    if total_screened > 0 and total_universe > 0:
        screening_rate = total_screened / total_universe
        theme_rate = breadth / max(total_breadth, 1)
        enrichment = theme_rate / max(screening_rate, 0.001)
        enrichment_mult = min(ENRICHMENT_CFG["max"], 1.0 + max(0, enrichment - 1.0) * ENRICHMENT_CFG["coeff"])

    # ── SHORT INTEREST / SQUEEZE POTENTIAL ───────────────────────────
    si_mult, si_median = _compute_si_multiplier(scoring_tickers, breadth)

    # ── FINAL SCORE ──────────────────────────────────────────────────
    base_score = (
        active_weights["strength"] * strength +
        active_weights["confirmation"] * confirmation
    )
    final_score = (
        base_score * actionability * breadth_penalty *
        rs_accel_mult * breadth_momentum_mult * enrichment_mult *
        si_mult
    )

    top_stocks = theme_df.nlargest(min(3, len(theme_df)), 'rs_sts_pct')[['ticker', 'rs_sts_pct']].to_dict('records')

    return {
        'theme': theme,
        # Reporting-compatible fields
        'avg_rs_sts': avg_rs,
        'median_rs_sts': median_rs,
        'high_momentum_count': high_momentum_count,
        'high_momentum_pct': high_momentum_pct,
        'leader_count': leader_count,
        'leader_pct': leader_pct,
        'breadth': breadth,
        'total_breadth': total_breadth,
        'pct_above_50sma': pct_above_50sma,
        'pct_near_highs': pct_near_highs,
        'avg_dist_25sma': dist_25sma,
        # Scoring components
        'strength': strength,
        'confirmation': confirmation,
        'participation': participation,
        'extension_factor': extension_factor,
        'vol_factor': vol_factor,
        'actionability': actionability,
        'breadth_penalty': breadth_penalty,
        'rs_accel': rs_accel,
        'rs_accel_mult': rs_accel_mult,
        'breadth_delta': breadth_delta,
        'breadth_momentum_mult': breadth_momentum_mult,
        'enrichment': enrichment,
        'enrichment_mult': enrichment_mult,
        'si_median': si_median,
        'si_mult': si_mult,
        # Final scores
        'final_score': final_score,
        'strength_score': final_score,  # backward compat alias
        'top_stocks': top_stocks,
        'tickers': tickers,
    }


def analyze_theme_strength(master_df: pd.DataFrame, market_breadth: Dict = None, screened_tickers: set = None) -> pd.DataFrame:
    """Analyze all themes and return ranked DataFrame using Iteration 9d scoring."""
    ticker_themes = load_ticker_themes()

    if not ticker_themes:
        print("No ticker themes found")
        return pd.DataFrame()

    # Apply theme consolidation (consume/remove groups)
    theme_groups = load_theme_groups()
    theme_tickers = build_theme_to_tickers(ticker_themes, theme_groups)

    print(f"Analyzing {len(theme_tickers)} themes (after consolidation)...")

    # Determine market regime (4-tier from pct_above_50sma)
    regime, active_weights, regime_pct = compute_market_regime(master_df, market_breadth)
    print(f"Regime: {regime} (pct_above_50sma: {regime_pct:.1f}%, weights: S={active_weights['strength']:.0%}/C={active_weights['confirmation']:.0%})")

    # Load historical data for acceleration/momentum multipliers
    history = _load_score_history()

    # Compute totals for enrichment ratio
    total_screened = len(screened_tickers) if screened_tickers else 0
    total_universe = len(master_df)

    theme_metrics = []
    today_history = {}

    for theme, tickers in theme_tickers.items():
        metrics = calculate_theme_metrics(
            theme, tickers, master_df, active_weights, screened_tickers,
            history=history,
            total_screened=total_screened,
            total_universe=total_universe,
        )
        if metrics:
            theme_metrics.append(metrics)
            today_history[theme] = {
                "median_rs": metrics["median_rs_sts"],
                "breadth": metrics["breadth"],
            }

    theme_df = pd.DataFrame(theme_metrics)

    if theme_df.empty:
        return theme_df

    # Sort by final score (aliased as strength_score)
    theme_df = theme_df.sort_values('strength_score', ascending=False).reset_index(drop=True)

    theme_df['is_hot'] = theme_df['avg_rs_sts'] > HOT_THRESHOLD
    theme_df['regime'] = regime

    # Persist today's data for tomorrow's acceleration/momentum lookback
    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _save_score_history(history, today_key, today_history)

    return theme_df


def get_hot_themes(theme_df: pd.DataFrame) -> pd.DataFrame:
    """Filter to only hot themes."""
    return theme_df[theme_df['is_hot']]


if __name__ == '__main__':
    master_files = sorted(glob(str(SCREENING_OUTPUT_DIR / 'master' / 'master_*.csv')))
    if master_files:
        latest_master = master_files[-1]
        print(f"Loading {latest_master}")

        master_df = pd.read_csv(latest_master)
        theme_df = analyze_theme_strength(master_df)

        print(f"\n{'='*80}")
        print("THEME STRENGTH ANALYSIS (Iteration 9d)")
        print(f"{'='*80}\n")

        print(f"Total themes: {len(theme_df)}")
        print(f"Hot themes (RS > {HOT_THRESHOLD}%): {theme_df['is_hot'].sum()}\n")

        print("Top 15 Themes by Final Score:")
        pd.options.display.float_format = '{:.1f}'.format
        cols = ['theme', 'median_rs_sts', 'leader_pct', 'participation',
                'pct_above_50sma', 'pct_near_highs', 'avg_dist_25sma',
                'breadth', 'actionability', 'rs_accel_mult', 'si_mult',
                'strength_score']
        available = [c for c in cols if c in theme_df.columns]
        print(theme_df[available].head(15).to_string())
    else:
        print("No master tables found. Run create_master_table.py first.")
