"""
Read the curated Google Sheet's ticker-to-theme labels.

The labels are raw and often legacy; ``tag_new_tickers`` remaps and validates
them before anything is written.
"""

import pandas as pd
from typing import Dict, List

from config.settings import CONFIG, GOOGLE_SHEET_ID

gid = CONFIG["themes"]["google_sheet_gid"]
GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={gid}"


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
