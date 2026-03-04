"""
Analyze theme strength based on RS_STS% scores.

Calculates aggregate metrics for each theme:
- Average/median RS_STS%
- High momentum percentage (>80%)
- Theme breadth (number of stocks)
- Composite strength score
"""

import json
import numpy as np
import pandas as pd
from typing import Dict, List
from collections import defaultdict
from glob import glob

import src.stock_utils as su
from config.settings import CONFIG, TICKER_THEMES_FILE, SCREENING_OUTPUT_DIR

THEMES_FILE = TICKER_THEMES_FILE
HOT_THRESHOLD = CONFIG["themes"]["hot_theme_rs_threshold"]
MOMENTUM_THRESHOLD = CONFIG["themes"]["high_momentum_threshold"]
WEIGHTS = CONFIG["themes"]["strength_weights"]


def load_ticker_themes() -> Dict[str, List[str]]:
    """Load ticker themes from JSON."""
    if not THEMES_FILE.exists():
        print(f"Warning: {THEMES_FILE} not found")
        return {}

    with THEMES_FILE.open() as f:
        return json.load(f)


def group_tickers_by_theme(ticker_themes: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Reverse mapping: theme -> list of tickers."""
    theme_tickers = defaultdict(list)

    for ticker, themes in ticker_themes.items():
        for theme in themes:
            theme_tickers[theme].append(ticker)

    return dict(theme_tickers)


def calculate_theme_metrics(theme: str, tickers: List[str], master_df: pd.DataFrame, active_weights: Dict[str, float]) -> Dict:
    """Calculate aggregate metrics for a single theme."""
    theme_df = master_df[master_df['ticker'].isin(tickers)]

    if len(theme_df) == 0:
        return None

    rs_values = theme_df['rs_sts_pct'].values

    # Base RS metrics
    avg_rs = np.mean(rs_values)
    median_rs = np.median(rs_values)
    high_momentum_count = np.sum(rs_values > MOMENTUM_THRESHOLD)
    high_momentum_pct = (high_momentum_count / len(rs_values)) * 100
    breadth = len(tickers)

    # 1. Extension Metric (Avg distance from 25SMA)
    # Using 25SMA as the proxy for short-term extension
    dist_25sma = np.nan
    if 'close' in theme_df.columns and 'sma25' in theme_df.columns:
        # Avoid division by zero
        theme_df_valid = theme_df[theme_df['sma25'] > 0]
        if not theme_df_valid.empty:
            pct_from_25sma = ((theme_df_valid['close'] - theme_df_valid['sma25']) / theme_df_valid['sma25']) * 100
            dist_25sma = np.nanmean(pct_from_25sma)
            
    # 2. Theme Breakout Breadth (% of stocks above 50SMA)
    pct_above_50sma = np.nan
    if 'close' in theme_df.columns and 'sma50' in theme_df.columns:
        theme_df_valid = theme_df[~theme_df['sma50'].isna()]
        if len(theme_df_valid) > 0:
            above_50sma_count = np.sum(theme_df_valid['close'] > theme_df_valid['sma50'])
            pct_above_50sma = (above_50sma_count / len(theme_df_valid)) * 100

    breadth_penalty = 1.0 if breadth >= 3 else (0.5 if breadth == 2 else 0.3)

    # Strength Score - Keep it simple but weight momentum
    strength_score = (
        active_weights["rs_avg"] * median_rs +
        active_weights["momentum"] * high_momentum_pct +
        active_weights["breadth"] * np.log1p(breadth) * 10
    ) * breadth_penalty

    top_stocks = theme_df.nlargest(3, 'rs_sts_pct')[['ticker', 'rs_sts_pct']].to_dict('records')

    return {
        'theme': theme,
        'avg_rs_sts': avg_rs,
        'median_rs_sts': median_rs,
        'high_momentum_count': high_momentum_count,
        'high_momentum_pct': high_momentum_pct,
        'breadth': breadth,
        'pct_above_50sma': pct_above_50sma,
        'avg_dist_25sma': dist_25sma,
        'strength_score': strength_score,
        'market_relative_score': avg_rs * breadth_penalty,  # Apply breadth penalty so singletons don't dominate
        'top_stocks': top_stocks,
        'tickers': tickers
    }


def analyze_theme_strength(master_df: pd.DataFrame, market_breadth: Dict = None) -> pd.DataFrame:
    """Analyze all themes and return ranked DataFrame using regime-based weights."""
    ticker_themes = load_ticker_themes()

    if not ticker_themes:
        print("No ticker themes found")
        return pd.DataFrame()

    theme_tickers = group_tickers_by_theme(ticker_themes)

    print(f"Analyzing {len(theme_tickers)} themes...")
    
    # Determine the market regime based on MMFI
    # Default to bull market weights if market breadth is unavailable
    mmfi_value = 51.0 
    if market_breadth and 'mmfi' in market_breadth and market_breadth['mmfi'] is not None:
         mmfi_value = market_breadth['mmfi']
         
    if mmfi_value > 50.0:
        print(f"Regime: Bull Market (MMFI: {mmfi_value:.1f}%)")
        active_weights = WEIGHTS.get("bull_market", {"rs_avg": 0.5, "momentum": 0.3, "breadth": 0.2})
    else:
        print(f"Regime: Bear/Choppy Market (MMFI: {mmfi_value:.1f}%)")
        active_weights = WEIGHTS.get("bear_market", {"rs_avg": 0.5, "momentum": 0.3, "breadth": 0.2})

    theme_metrics = []

    for theme, tickers in theme_tickers.items():
        metrics = calculate_theme_metrics(theme, tickers, master_df, active_weights)
        if metrics:
            theme_metrics.append(metrics)

    theme_df = pd.DataFrame(theme_metrics)

    if theme_df.empty:
        return theme_df

    # Sort logic: if Bear Market, true absolute strength (market_relative_score) is paramount
    if mmfi_value <= 50.0:
        theme_df = theme_df.sort_values('market_relative_score', ascending=False)
    else:
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

        print("Top 10 Themes by Strength:")
        # Format the new columns if they exist
        pd.options.display.float_format = '{:.2f}'.format
        print(theme_df[['theme', 'avg_rs_sts', 'high_momentum_pct', 'pct_above_50sma', 'avg_dist_25sma', 'strength_score']].head(10).to_string())
    else:
        print("No master tables found. Run create_master_table.py first.")
