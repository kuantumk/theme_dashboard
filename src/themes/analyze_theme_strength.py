"""
Analyze theme strength based on RS_STS% scores.

Scores themes along three dimensions for momentum trading:
- Strength: RS quality and leader concentration (RS >= 90)
- Confirmation: structural uptrend health, proximity to highs, active breadth
- Actionability: extension penalty and volume expansion bonus

Designed to surface themes suitable for Qullamaggie / Marios Stamatoudis
style breakout trading: strong RS, broad participation, tight near highs.
"""

import numpy as np
import pandas as pd
from typing import Dict, List
from collections import defaultdict
from glob import glob

import src.stock_utils as su
from config.settings import CONFIG, SCREENING_OUTPUT_DIR
from src.themes.theme_registry import load_ticker_themes
HOT_THRESHOLD = CONFIG["themes"]["hot_theme_rs_threshold"]
MOMENTUM_THRESHOLD = CONFIG["themes"]["high_momentum_threshold"]
WEIGHTS = CONFIG["themes"]["strength_weights"]
MIN_BREADTH = CONFIG["themes"].get("min_scored_breadth", 2)


def group_tickers_by_theme(ticker_themes: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Reverse mapping: theme -> list of tickers."""
    theme_tickers = defaultdict(list)

    for ticker, themes in ticker_themes.items():
        for theme in themes:
            theme_tickers[theme].append(ticker)

    return dict(theme_tickers)


def calculate_theme_metrics(theme: str, tickers: List[str], master_df: pd.DataFrame, active_weights: Dict[str, float], screened_tickers: set = None) -> Dict:
    """
    Calculate aggregate metrics for a single theme.

    Scores along three axes:
    - Strength (0-100): median RS blended with leader concentration (RS >= 90)
    - Confirmation (0-100): structural health, near-highs proximity, active breadth
    - Actionability (0.65-1.15 multiplier): extension penalty + volume bonus

    If screened_tickers is provided, scoring is based ONLY on the tickers that
    passed screening that day.
    """
    scoring_tickers = list(set(tickers) & screened_tickers) if screened_tickers is not None else tickers
    theme_df = master_df[master_df['ticker'].isin(scoring_tickers)]

    if len(theme_df) == 0:
        return None

    total_breadth = len(tickers)
    breadth = len(theme_df)

    # Skip singletons — can't confirm a theme move with one stock
    if breadth < MIN_BREADTH:
        return None

    rs_values = theme_df['rs_sts_pct'].values

    # Base RS metrics (kept for reporting compatibility)
    avg_rs = np.mean(rs_values)
    median_rs = np.median(rs_values)
    high_momentum_count = int(np.sum(rs_values > MOMENTUM_THRESHOLD))
    high_momentum_pct = (high_momentum_count / len(rs_values)) * 100

    # ── STRENGTH (0-100) ─────────────────────────────────────────────
    # Median RS captures the center; leader_pct captures concentration
    # at the top. RS >= 90 is the threshold for true leaders among
    # screened stocks (which already passed high bars).
    leader_count = int(np.sum(rs_values >= 90))
    leader_pct = (leader_count / len(rs_values)) * 100
    strength = 0.5 * median_rs + 0.5 * leader_pct

    # ── CONFIRMATION (0-100) ─────────────────────────────────────────
    # B1: Structural health — % above 50SMA (Weinstein Stage 2 proxy)
    pct_above_50sma = 0.0
    if 'close' in theme_df.columns and 'sma50' in theme_df.columns:
        valid = theme_df[~theme_df['sma50'].isna()]
        if len(valid) > 0:
            pct_above_50sma = (np.sum(valid['close'] > valid['sma50']) / len(valid)) * 100

    # B2: Near highs — % within 15% of 252-day high (breakout proximity)
    pct_near_highs = 0.0
    if 'close' in theme_df.columns and 'max252' in theme_df.columns:
        valid = theme_df[~theme_df['max252'].isna()]
        if len(valid) > 0:
            pct_near_highs = (np.sum(valid['close'] >= 0.85 * valid['max252']) / len(valid)) * 100

    # B3: Breadth — linear scale, 5+ screened stocks = full credit
    breadth_score = min(breadth / 5.0, 1.0) * 100

    confirmation = 0.35 * pct_above_50sma + 0.35 * pct_near_highs + 0.30 * breadth_score

    # ── ACTIONABILITY (multiplier 0.65-1.15) ─────────────────────────
    # Extension penalty — use median (not mean) to resist outliers
    dist_25sma = np.nan
    if 'close' in theme_df.columns and 'sma25' in theme_df.columns:
        valid = theme_df[theme_df['sma25'] > 0]
        if not valid.empty:
            pct_from_25sma = ((valid['close'] - valid['sma25']) / valid['sma25']) * 100
            dist_25sma = np.nanmedian(pct_from_25sma)

    extension_factor = 1.0
    if not np.isnan(dist_25sma):
        if dist_25sma > 25:
            extension_factor = 0.65
        elif dist_25sma > 15:
            extension_factor = 0.80
        elif dist_25sma > 10:
            extension_factor = 0.92

    # Volume expansion bonus — institutional accumulation signal
    vol_factor = 1.0
    if 'vol_sma40' in theme_df.columns and 'vol_sma252' in theme_df.columns:
        valid = theme_df[(theme_df['vol_sma252'] > 0) & (~theme_df['vol_sma252'].isna())]
        if not valid.empty:
            avg_vol_ratio = np.nanmean(valid['vol_sma40'] / valid['vol_sma252'])
            if avg_vol_ratio > 1.75:
                vol_factor = 1.15
            elif avg_vol_ratio > 1.5:
                vol_factor = 1.08

    actionability = extension_factor * vol_factor

    # ── BREADTH PENALTY ──────────────────────────────────────────────
    # Hard floor for very small themes (breadth < 3 = unconfirmed)
    breadth_penalty = 1.0 if breadth >= 3 else 0.5

    # ── FINAL SCORE ──────────────────────────────────────────────────
    strength_score = (
        active_weights["strength"] * strength +
        active_weights["confirmation"] * confirmation
    ) * actionability * breadth_penalty

    top_stocks = theme_df.nlargest(3, 'rs_sts_pct')[['ticker', 'rs_sts_pct']].to_dict('records')

    return {
        'theme': theme,
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
        'strength': strength,
        'confirmation': confirmation,
        'extension_factor': extension_factor,
        'vol_factor': vol_factor,
        'actionability': actionability,
        'strength_score': strength_score,
        'top_stocks': top_stocks,
        'tickers': tickers
    }


def analyze_theme_strength(master_df: pd.DataFrame, market_breadth: Dict = None, screened_tickers: set = None) -> pd.DataFrame:
    """Analyze all themes and return ranked DataFrame using regime-based weights."""
    ticker_themes = load_ticker_themes()

    if not ticker_themes:
        print("No ticker themes found")
        return pd.DataFrame()

    theme_tickers = group_tickers_by_theme(ticker_themes)

    print(f"Analyzing {len(theme_tickers)} themes...")

    # Determine market regime from MMFI
    mmfi_value = None
    if market_breadth and 'mmfi' in market_breadth and market_breadth['mmfi'] is not None:
        mmfi_value = market_breadth['mmfi']

    if mmfi_value is None:
        print("Warning: MMFI data unavailable - defaulting to neutral (50.0). Check market breadth scraper.")
        mmfi_value = 50.0

    if mmfi_value > 50.0:
        print(f"Regime: Bull Market (MMFI: {mmfi_value:.1f}%)")
        active_weights = WEIGHTS.get("bull_market", {"strength": 0.5, "confirmation": 0.5})
    else:
        print(f"Regime: Bear/Choppy Market (MMFI: {mmfi_value:.1f}%)")
        active_weights = WEIGHTS.get("bear_market", {"strength": 0.7, "confirmation": 0.3})

    theme_metrics = []

    for theme, tickers in theme_tickers.items():
        metrics = calculate_theme_metrics(theme, tickers, master_df, active_weights, screened_tickers)
        if metrics:
            theme_metrics.append(metrics)

    theme_df = pd.DataFrame(theme_metrics)

    if theme_df.empty:
        return theme_df

    # Always sort by strength_score — bear weights already emphasize
    # pure RS strength (0.7) over confirmation (0.3)
    theme_df = theme_df.sort_values('strength_score', ascending=False)

    theme_df['is_hot'] = theme_df['avg_rs_sts'] > HOT_THRESHOLD

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
        print("THEME STRENGTH ANALYSIS")
        print(f"{'='*80}\n")

        print(f"Total themes: {len(theme_df)}")
        print(f"Hot themes (RS > {HOT_THRESHOLD}%): {theme_df['is_hot'].sum()}\n")

        print("Top 15 Themes by Strength Score:")
        pd.options.display.float_format = '{:.1f}'.format
        cols = ['theme', 'median_rs_sts', 'leader_pct', 'pct_above_50sma',
                'pct_near_highs', 'avg_dist_25sma', 'breadth', 'actionability',
                'strength_score']
        print(theme_df[cols].head(15).to_string())
    else:
        print("No master tables found. Run create_master_table.py first.")
