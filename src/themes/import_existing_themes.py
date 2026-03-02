"""
Import existing theme groups from Google Sheet and create initial ticker_themes.json.
"""

import json
import pandas as pd
from typing import Dict, List
from collections import Counter

from config.settings import CONFIG, TICKER_THEMES_FILE

GOOGLE_SHEET_URL = CONFIG["themes"]["google_sheet_url"]
OUTPUT_FILE = TICKER_THEMES_FILE


def parse_theme_from_description(description: str, theme_company: str) -> List[str]:
    """Parse the theme/sub-theme from the Theme/Company column."""
    themes = []
    desc_lower = description.lower()

    # AI Infrastructure sub-themes
    if 'memory' in desc_lower or 'hard disk' in desc_lower:
        themes.append("AI - Infra / Memory")
    elif any(word in desc_lower for word in ['optic', 'fiber', 'photonic', 'laser']):
        themes.append("AI - Infra / Optics")
    elif 'silicon carbide' in desc_lower or 'gan' in desc_lower or 'sic' in desc_lower:
        themes.append("AI - Infra / Silicon Carbide")
    elif 'power' in desc_lower or 'cooling' in desc_lower:
        themes.append("AI - Infra / Power/Cooling")
    elif any(word in desc_lower for word in ['connectivity', 'transceiver']):
        themes.append("AI - Infra / Connectivity")
    elif any(word in desc_lower for word in ['chip', 'cpu', 'gpu', 'semiconductor', 'nvidia', 'broadcom', 'amd', 'intel']):
        themes.append("AI - Infra / Core Chips")
    elif 'cloud' in desc_lower or 'data center' in desc_lower or 'hpc' in desc_lower:
        themes.append("AI - Data Center & Cloud")
    elif 'robot' in desc_lower or 'automation' in desc_lower:
        themes.append("AI - Robotics & Automation")
    elif 'analytics' in desc_lower or 'software' in desc_lower or 'ai' in desc_lower:
        themes.append("AI - Software & Analytics")
    elif 'smr' in desc_lower or 'small modular reactor' in desc_lower or 'microreactor' in desc_lower:
        themes.append("Nuclear / SMR")
    elif 'uranium' in desc_lower:
        themes.append("Nuclear / Uranium")
    elif 'nuclear fuel' in desc_lower:
        themes.append("Nuclear / Fuel Technology")
    elif 'nuclear' in desc_lower and 'fleet' in desc_lower:
        themes.append("Electricity / Power Generation")
    elif 'electric utility' in desc_lower or 'power' in desc_lower:
        themes.append("Electricity / Power Generation")
    elif 'bitcoin mining' in desc_lower or 'cryptocurrency mining' in desc_lower:
        themes.append("Cryptocurrency / Mining")
    elif 'crypto' in desc_lower or 'blockchain' in desc_lower or 'bitcoin etf' in desc_lower:
        themes.append("Cryptocurrency / Infrastructure")
    elif 'satellite launch' in desc_lower or 'rocket' in desc_lower:
        themes.append("Space / Launches")
    elif 'satellite' in desc_lower or 'broadband' in desc_lower:
        themes.append("Space / Satellites & Communication")
    elif 'space infrastructure' in desc_lower or 'space manufacturing' in desc_lower:
        themes.append("Space / Manufacturing")
    elif 'lunar' in desc_lower or 'mars' in desc_lower:
        themes.append("Space / Exploration")
    elif 'drone' in desc_lower or 'vertical take' in desc_lower or 'evtol' in desc_lower:
        themes.append("Drones")

    if not themes:
        if 'ai' in desc_lower:
            themes.append("AI - Other")
        else:
            themes.append("Uncategorized")

    return themes


def import_google_sheet_themes() -> Dict[str, List[str]]:
    """Read Google Sheet CSV and extract ticker-theme mappings."""
    print(f"Reading Google Sheet: {GOOGLE_SHEET_URL}")

    df = pd.read_csv(GOOGLE_SHEET_URL)

    print(f"Found {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")

    ticker_themes = {}
    current_theme = None

    for idx, row in df.iterrows():
        ticker = str(row.get('Ticker', '')).strip() if pd.notna(row.get('Ticker')) else ''
        theme_company = str(row.get('Theme/Company', '')).strip() if pd.notna(row.get('Theme/Company')) else ''

        if (not ticker or ticker == '' or ticker == 'nan') and theme_company and theme_company != 'nan':
            current_theme = theme_company
            continue

        if ticker and ticker != '' and ticker != 'nan':
            if current_theme:
                if ticker in ticker_themes:
                    if current_theme not in ticker_themes[ticker]:
                        ticker_themes[ticker].append(current_theme)
                else:
                    ticker_themes[ticker] = [current_theme]
            else:
                if ticker not in ticker_themes:
                    ticker_themes[ticker] = ["Uncategorized"]

    print(f"\nExtracted themes for {len(ticker_themes)} tickers")

    return ticker_themes


def save_ticker_themes(ticker_themes: Dict[str, List[str]]):
    """Save ticker themes to JSON file."""
    OUTPUT_FILE.parent.mkdir(exist_ok=True)

    with OUTPUT_FILE.open('w') as f:
        json.dump(ticker_themes, f, indent=2, sort_keys=True)

    print(f"\nSaved ticker themes to {OUTPUT_FILE}")


if __name__ == '__main__':
    ticker_themes = import_google_sheet_themes()

    print("\nSample ticker themes:")
    for ticker, themes in list(ticker_themes.items())[:10]:
        print(f"  {ticker}: {themes}")

    save_ticker_themes(ticker_themes)

    print(f"\nOK Theme import complete!")
    print(f"  Total tickers: {len(ticker_themes)}")

    theme_counts = Counter()
    for themes in ticker_themes.values():
        for theme in themes:
            theme_counts[theme] += 1

    print(f"\n  Tickers per theme:")
    for theme, count in sorted(theme_counts.items(), key=lambda x: -x[1]):
        print(f"    {theme}: {count}")
