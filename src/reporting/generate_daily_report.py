"""
Generate comprehensive daily stock screening report in markdown format.

Combines all data sources:
- Market breadth (NCFD, MMFI)
- Theme strength analysis
- Individual screener results
- Fundamental data
"""

import json
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set
from glob import glob

import src.stock_utils as su
from src.themes.analyze_theme_strength import analyze_theme_strength
from config.settings import CONFIG, REPORTS_DIR, FUNDAMENTALS_DB, SCREENING_OUTPUT_DIR, TICKER_THEMES_FILE

OUTPUT_DIR = REPORTS_DIR
OUTPUT_DIR.mkdir(exist_ok=True)


def load_fundamentals(tickers: List[str]) -> Dict[str, Dict]:
    """Load fundamental data from SQLite for given tickers."""
    if not FUNDAMENTALS_DB.exists():
        return {}

    conn = sqlite3.connect(FUNDAMENTALS_DB)
    cursor = conn.cursor()

    placeholders = ','.join(['?'] * len(tickers))
    cursor.execute(f'''
        SELECT ticker, shares_float, eps_growth_yoy, sales_growth_yoy,
               short_interest, inst_transactions
        FROM fundamentals
        WHERE ticker IN ({placeholders})
    ''', tickers)

    results = {}
    for row in cursor.fetchall():
        ticker = row[0]
        results[ticker] = {
            'float_m': row[1] / 1e6 if row[1] else None,
            'eps_growth': row[2],
            'sales_growth': row[3],
            'short_interest': row[4],
            'inst_trans': int(row[5]) if row[5] else None
        }

    conn.close()
    return results


def format_number(value, decimals=1, suffix=''):
    """Format number with proper decimals and suffix."""
    if value is None or pd.isna(value):
        return 'N/A'
    return f"{value:.{decimals}f}{suffix}"


def format_inst_trans(value):
    """Format institutional transactions as percentage (from finviz Inst Trans)."""
    if value is None or pd.isna(value):
        return 'N/A'

    val = float(value)
    if val > 0:
        return f"+{val:.1f}"
    return f"{val:.1f}"


def generate_market_context(market_breadth: Dict) -> str:
    """Generate market context section."""
    ncfd = market_breadth.get('ncfd')
    mmfi = market_breadth.get('mmfi')

    # Determine trend indicators
    ncfd_emoji = '📈' if ncfd and ncfd > 50 else '📉' if ncfd else '📊'
    mmfi_emoji = '📈' if mmfi and mmfi > 50 else '📉' if mmfi else '📊'

    return f"""## 📊 Market Context
**Market Breadth Indicators**:
- NCFD (Nasdaq above 5-day avg): {format_number(ncfd, 1, '%')} {ncfd_emoji}
- MMFI (Stocks above 50-day avg): {format_number(mmfi, 1, '%')} {mmfi_emoji}
- Timestamp: {market_breadth.get('timestamp', 'N/A')}
"""


def generate_executive_summary(master_df: pd.DataFrame, theme_df: pd.DataFrame, new_tickers: List[str]) -> str:
    """Generate executive summary section."""
    total_stocks = len(master_df)
    hot_themes = theme_df[theme_df['is_hot']].shape[0] if 'is_hot' in theme_df.columns else 0

    return f"""## Executive Summary
- **Total stocks screened**: {total_stocks}
- **Hot themes identified**: {hot_themes} (avg RS_STS% > {CONFIG['themes']['hot_theme_rs_threshold']}%)
- **New tickers tagged**: {len(new_tickers)}
"""


def generate_hot_themes_section(theme_df: pd.DataFrame, master_df: pd.DataFrame) -> str:
    """Generate hot themes section with detailed tables."""
    if theme_df.empty or 'is_hot' not in theme_df.columns:
        return "## 🔥 Hot Themes\n\n*No hot themes identified.*\n"

    hot_themes = theme_df[theme_df['is_hot']].copy()

    if hot_themes.empty:
        return "## 🔥 Hot Themes\n\n*No hot themes identified.*\n"

    section = "## 🔥 Hot Themes (Ranked by Strength Score)\n\n"

    # Load fundamentals for all tickers in hot themes
    all_tickers = []
    for tickers in hot_themes['tickers']:
        all_tickers.extend(tickers)
    fundamentals = load_fundamentals(all_tickers)

    for idx, (_, theme_row) in enumerate(hot_themes.head(10).iterrows(), 1):
        theme_name = theme_row['theme']
        score = theme_row['strength_score']
        avg_rs = theme_row['avg_rs_sts']
        median_rs = theme_row['median_rs_sts']
        breadth = theme_row['breadth']
        momentum_count = theme_row['high_momentum_count']
        momentum_pct = theme_row['high_momentum_pct']

        # Emoji based on rank
        rank_emoji = '🚀' if idx <= 3 else '⭐' if idx <= 5 else '💫'

        section += f"### {idx}. {theme_name} - Strength Score: {score:.1f} {rank_emoji}\n\n"
        section += f"**Theme Metrics**:\n"
        section += f"- Avg RS_STS%: {avg_rs:.1f}% | Median: {median_rs:.1f}%\n"
        section += f"- Stocks: {breadth} | High Momentum (RS>{CONFIG['themes']['high_momentum_threshold']}%): {momentum_count} ({momentum_pct:.0f}%)\n\n"

        # Get stocks in this theme from master table
        theme_tickers = theme_row['tickers']
        theme_stocks_df = master_df[master_df['ticker'].isin(theme_tickers)].copy()

        if not theme_stocks_df.empty:
            # Add fundamentals
            theme_stocks_df['float_m'] = theme_stocks_df['ticker'].map(
                lambda t: fundamentals.get(t, {}).get('float_m')
            )
            theme_stocks_df['eps_growth'] = theme_stocks_df['ticker'].map(
                lambda t: fundamentals.get(t, {}).get('eps_growth')
            )
            theme_stocks_df['sales_growth'] = theme_stocks_df['ticker'].map(
                lambda t: fundamentals.get(t, {}).get('sales_growth')
            )
            theme_stocks_df['short_interest'] = theme_stocks_df['ticker'].map(
                lambda t: fundamentals.get(t, {}).get('short_interest')
            )
            theme_stocks_df['inst_trans'] = theme_stocks_df['ticker'].map(
                lambda t: fundamentals.get(t, {}).get('inst_trans')
            )

            # Sort by RS_STS%
            theme_stocks_df = theme_stocks_df.sort_values('rs_sts_pct', ascending=False)

            section += "**Top Stocks**:\n\n"
            section += "| Ticker | RS_STS% | Float (M) | EPS Growth | Sales Growth | Short % | Inst Trans |\n"
            section += "|--------|---------|-----------|------------|--------------|---------|------------|\n"

            for _, stock in theme_stocks_df.head(10).iterrows():
                ticker = stock['ticker']
                rs = format_number(stock.get('rs_sts_pct'), 1, '%')
                float_m = format_number(stock.get('float_m'), 1)
                eps = format_number(stock.get('eps_growth'), 1, '%')
                sales = format_number(stock.get('sales_growth'), 1, '%')
                short = format_number(stock.get('short_interest'), 1, '%')
                inst = format_inst_trans(stock.get('inst_trans'))

                section += f"| {ticker} | {rs} | {float_m} | {eps} | {sales} | {short} | {inst} |\n"

            section += "\n"

        section += "---\n\n"

    return section


def generate_theme_report_section(theme_df: pd.DataFrame, master_df: pd.DataFrame, screened_tickers: Set[str]) -> str:
    """
    Generate the core theme report section.

    Displays ALL active themes containing screened stocks, ranked by theme strength.
    """
    if theme_df.empty:
        return "## Theme Analysis\n\n*No theme data available.*\n"

    # Filter master table to only screened stocks
    if not screened_tickers:
        return "## Theme Analysis\n\n*No screened tickers provided.*\n"

    screened_df = master_df[master_df['ticker'].isin(screened_tickers)].copy()

    if screened_df.empty:
        return "## Theme Analysis\n\n*No screened stocks found in master table.*\n"

    section = "## 🌍 Market Themes (Ranked by Strength)\n\n"

    # Load fundamentals
    fundamentals = load_fundamentals(screened_df['ticker'].tolist())

    displayed_tickers = set()
    theme_count = 0
    max_themes = 30

    for _, theme_row in theme_df.iterrows():
        if theme_count >= max_themes:
            break

        theme_name = theme_row['theme']

        # Skip special themes to display them at the bottom
        if theme_name in ['Uncategorized', 'Singleton']:
            continue

        theme_tickers_in_db = set(theme_row['tickers'])

        # Find which screened stocks are in this theme
        active_theme_tickers = list(theme_tickers_in_db.intersection(screened_tickers))

        if not active_theme_tickers:
            continue

        theme_count += 1

        # Metrics
        score = theme_row['strength_score']
        avg_rs = theme_row['avg_rs_sts']

        # Emoji
        rank_emoji = '🔥' if theme_row.get('is_hot', False) else '⚡'

        section += f"### {theme_count}. {theme_name} {rank_emoji}\n"
        section += f"**Theme Score**: {score:.1f} | **Avg RS**: {avg_rs:.1f}%\n\n"

        # Create table for these stocks
        theme_stocks_df = screened_df[screened_df['ticker'].isin(active_theme_tickers)].copy()

        # Add fundamentals
        theme_stocks_df['float_m'] = theme_stocks_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('float_m'))
        theme_stocks_df['sales_growth'] = theme_stocks_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('sales_growth'))
        theme_stocks_df['eps_growth'] = theme_stocks_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('eps_growth'))
        theme_stocks_df['inst_trans'] = theme_stocks_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('inst_trans'))
        theme_stocks_df['short_interest'] = theme_stocks_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('short_interest'))

        # Sort by RS%, then ADR% as tiebreaker — show only top tickers
        MAX_TICKERS_PER_THEME = 10
        theme_stocks_df = theme_stocks_df.sort_values(
            ['rs_sts_pct', 'adr_pct'], ascending=[False, False]
        )
        total_in_theme = len(theme_stocks_df)
        theme_stocks_display = theme_stocks_df.head(MAX_TICKERS_PER_THEME)

        section += "| Ticker |  RS% |   Price | Vol(M) | Float(M) | EPS% | Sales% | Inst% | Short% |\n"
        section += "|:-------|-----:|--------:|-------:|---------:|-----:|-------:|------:|-------:|\n"

        for _, stock in theme_stocks_display.iterrows():
            t = stock['ticker']
            displayed_tickers.add(t)

            rs_val = stock['rs_sts_pct']
            rs = f"{rs_val:4.1f}" if pd.notna(rs_val) else "   -"

            close_val = stock['close']
            price = f"{close_val:7.2f}" if pd.notna(close_val) else "      -"

            vol_val = stock['volume']
            vol = f"{vol_val/1e6:6.1f}" if pd.notna(vol_val) else "     -"

            flt = format_number(stock.get('float_m'), 1)
            short = format_number(stock.get('short_interest'), 1)
            eps = format_number(stock.get('eps_growth'), 0)
            sales = format_number(stock.get('sales_growth'), 0)
            inst = format_inst_trans(stock.get('inst_trans'))

            t_pad = f"{t:<6}"
            flt_pad = f"{flt:>8}"
            short_pad = f"{short:>6}"
            eps_pad = f"{eps:>4}"
            sales_pad = f"{sales:>6}"
            inst_pad = f"{inst:>5}"

            section += f"| {t_pad} | {rs} | {price} | {vol} | {flt_pad} | {eps_pad} | {sales_pad} | {inst_pad} | {short_pad} |\n"

        section += "\n"

        # Add ticker list on single line
        ticker_list = ' '.join(sorted(active_theme_tickers))
        section += f"**Tickers:** {ticker_list}\n\n"

    # Handle Uncategorized / Singleton stocks
    try:
        with open(TICKER_THEMES_FILE, 'r') as f:
            ticker_themes = json.load(f)
    except Exception:
        ticker_themes = {}

    # Tickers without ANY theme
    truly_uncategorized = set(t for t in screened_tickers if t not in ticker_themes or not ticker_themes.get(t))

    if truly_uncategorized:
        section += "### 🧩 Uncategorized / Other\n\n"

        other_df = screened_df[screened_df['ticker'].isin(truly_uncategorized)].copy()

        # Fundamentals
        other_df['float_m'] = other_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('float_m'))
        other_df['short_interest'] = other_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('short_interest'))
        other_df['eps_growth'] = other_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('eps_growth'))
        other_df['inst_trans'] = other_df['ticker'].map(lambda t: fundamentals.get(t, {}).get('inst_trans'))

        section += "| Ticker |  RS% |   Price | Vol(M) | Float(M) | EPS% | Sales% | Inst% | Short% |\n"
        section += "|:-------|-----:|--------:|-------:|---------:|-----:|-------:|------:|-------:|\n"

        for _, stock in other_df.sort_values('rs_sts_pct', ascending=False).iterrows():
            t = stock['ticker']

            rs_val = stock['rs_sts_pct']
            rs = f"{rs_val:4.1f}" if pd.notna(rs_val) else "   -"

            close_val = stock['close']
            price = f"{close_val:7.2f}" if pd.notna(close_val) else "      -"

            vol_val = stock['volume']
            vol = f"{vol_val/1e6:6.1f}" if pd.notna(vol_val) else "     -"

            flt = format_number(stock.get('float_m'), 1)
            short = format_number(stock.get('short_interest'), 1)
            eps = format_number(stock.get('eps_growth'), 0)
            sales = format_number(stock.get('sales_growth'), 0)
            inst = format_inst_trans(stock.get('inst_trans'))

            t_pad = f"{t:<6}"
            flt_pad = f"{flt:>8}"
            short_pad = f"{short:>6}"
            eps_pad = f"{eps:>4}"
            sales_pad = f"{sales:>6}"
            inst_pad = f"{inst:>5}"

            section += f"| {t_pad} | {rs} | {price} | {vol} | {flt_pad} | {eps_pad} | {sales_pad} | {inst_pad} | {short_pad} |\n"

    return section


def generate_daily_report(
    date_str: str,
    master_df: pd.DataFrame,
    theme_df: pd.DataFrame,
    market_breadth: Dict,
    screener_results: Dict[str, pd.DataFrame] = None,
    screened_tickers: Set[str] = None,
    new_tickers: List[str] = None
) -> str:
    """
    Generate complete daily report in markdown.

    Args:
        date_str: Date string (YYYY-MM-DD)
        master_df: Master table DataFrame
        theme_df: Theme analysis DataFrame
        market_breadth: Dict with NCFD, MMFI values
        screener_results: Dict mapping screener name -> results DataFrame
        screened_tickers: Set of tickers from all screeners
        new_tickers: List of newly tagged tickers

    Returns:
        Markdown report string
    """
    if new_tickers is None:
        new_tickers = []

    report = f"# Daily Stock Screening Report - {date_str}\n\n"

    # Market context
    report += generate_market_context(market_breadth)
    report += "\n"

    # Executive summary
    report += generate_executive_summary(master_df, theme_df, new_tickers)
    report += "\n"

    # Theme Report (The Main Body)
    if screened_tickers:
        report += generate_theme_report_section(theme_df, master_df, screened_tickers)
    else:
        report += generate_hot_themes_section(theme_df, master_df)
    report += "\n"

    # Footer
    report += f"---\n\n*Report generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n"

    return report


def save_report(report: str, date_str: str):
    """Save report to file."""
    filename = OUTPUT_DIR / f"daily_report_{date_str}.md"

    with filename.open('w', encoding='utf-8') as f:
        f.write(report)

    print(f"OK Report saved to {filename}")
    return filename


if __name__ == '__main__':
    # Load latest master table
    master_files = sorted(glob(str(SCREENING_OUTPUT_DIR / 'master' / 'master_*.csv')))
    if master_files:
        latest_master = master_files[-1]
        print(f"Loading {latest_master}")

        master_df = pd.read_csv(latest_master)
        date_str = Path(latest_master).stem.replace('master_', '')

        # Mock market breadth
        market_breadth = {
            'ncfd': 65.2,
            'mmfi': 58.7,
            'timestamp': datetime.now().isoformat()
        }

        # Analyze themes
        theme_df = analyze_theme_strength(master_df, market_breadth)

        # Generate report
        report = generate_daily_report(
            date_str=date_str,
            master_df=master_df,
            theme_df=theme_df,
            market_breadth=market_breadth,
            new_tickers=[]
        )

        # Save report
        save_report(report, date_str)

        print("\nReport preview (first 2000 chars):")
        print(report[:2000])
    else:
        print("No master tables found")
