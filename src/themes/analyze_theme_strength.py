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


def calculate_theme_metrics(theme: str, tickers: List[str], master_df: pd.DataFrame) -> Dict:
    """Calculate aggregate metrics for a single theme."""
    theme_df = master_df[master_df['ticker'].isin(tickers)]

    if len(theme_df) == 0:
        return None

    rs_values = theme_df['rs_sts_pct'].values

    avg_rs = np.mean(rs_values)
    median_rs = np.median(rs_values)
    high_momentum_count = np.sum(rs_values > MOMENTUM_THRESHOLD)
    high_momentum_pct = (high_momentum_count / len(rs_values)) * 100
    breadth = len(tickers)

    breadth_penalty = 1.0 if breadth >= 3 else (0.5 if breadth == 2 else 0.3)

    strength_score = (
        WEIGHTS["rs_avg"] * median_rs +
        WEIGHTS["momentum"] * high_momentum_pct +
        WEIGHTS["breadth"] * np.log1p(breadth) * 10
    ) * breadth_penalty

    top_stocks = theme_df.nlargest(3, 'rs_sts_pct')[['ticker', 'rs_sts_pct']].to_dict('records')

    return {
        'theme': theme,
        'avg_rs_sts': avg_rs,
        'median_rs_sts': median_rs,
        'high_momentum_count': high_momentum_count,
        'high_momentum_pct': high_momentum_pct,
        'breadth': breadth,
        'strength_score': strength_score,
        'top_stocks': top_stocks,
        'tickers': tickers
    }


def analyze_theme_strength(master_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze all themes and return ranked DataFrame."""
    ticker_themes = load_ticker_themes()

    if not ticker_themes:
        print("No ticker themes found")
        return pd.DataFrame()

    theme_tickers = group_tickers_by_theme(ticker_themes)

    print(f"Analyzing {len(theme_tickers)} themes...")

    theme_metrics = []

    for theme, tickers in theme_tickers.items():
        metrics = calculate_theme_metrics(theme, tickers, master_df)
        if metrics:
            theme_metrics.append(metrics)

    theme_df = pd.DataFrame(theme_metrics)

    if theme_df.empty:
        return theme_df

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
        print(theme_df[['theme', 'avg_rs_sts', 'high_momentum_pct', 'breadth', 'strength_score']].head(10).to_string())
    else:
        print("No master tables found. Run create_master_table.py first.")
