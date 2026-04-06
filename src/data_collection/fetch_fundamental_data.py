"""
Fetch fundamental data from Finviz and cache in SQLite.

Includes: float, market cap, P/E, EPS/sales growth, short interest, institutional transactions.
Uses SQLite for caching with 7-day refresh policy.
"""

import re
import time
import sqlite3
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List
from bs4 import BeautifulSoup
from tqdm import tqdm

from config.settings import CONFIG, FUNDAMENTALS_DB, PRICE_DATA_TA_FILE
import src.stock_utils as su

DB_PATH = FUNDAMENTALS_DB
DB_PATH.parent.mkdir(exist_ok=True)

RATE_LIMIT = CONFIG["fundamental_data"]["rate_limit_seconds"]
MAX_RETRIES = CONFIG["fundamental_data"]["max_retries"]
CACHE_DAYS = CONFIG["fundamental_data"]["cache_days"]


def init_database():
    """Initialize SQLite database with fundamentals schema."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fundamentals (
            ticker TEXT PRIMARY KEY,
            market_cap REAL,
            shares_float REAL,
            pe_ratio REAL,
            eps_growth_yoy REAL,
            sales_growth_yoy REAL,
            short_interest REAL,
            inst_ownership REAL,
            inst_transactions REAL,
            last_updated TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()


def parse_finviz_value(value_str: str) -> Optional[float]:
    """Parse finviz value strings (e.g., '12.34B', '5.67%', '-') to float."""
    if not value_str or value_str == '-':
        return None

    value_str = value_str.strip()
    if value_str.endswith('%'):
        try:
            return float(value_str[:-1])
        except ValueError:
            return None

    multipliers = {'B': 1e9, 'M': 1e6, 'K': 1e3}
    for suffix, multiplier in multipliers.items():
        if value_str.endswith(suffix):
            try:
                return float(value_str[:-1]) * multiplier
            except ValueError:
                continue

    try:
        return float(value_str)
    except ValueError:
        return None


def get_fundamental_data(ticker: str) -> Optional[Dict]:
    """Fetch fundamental data for a single ticker from finviz."""
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    headers = {
        'User-Agent': CONFIG["market_breadth"]["user_agent"]
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            table = soup.find('table', class_='snapshot-table2')
            if not table:
                return None

            data_dict = {}
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                for i in range(0, len(cells), 2):
                    if i + 1 < len(cells):
                        key = cells[i].get_text(strip=True)
                        value = cells[i + 1].get_text(strip=True)
                        data_dict[key] = value

            fundamentals = {
                'market_cap': parse_finviz_value(data_dict.get('Market Cap')),
                'shares_float': parse_finviz_value(data_dict.get('Shs Float')),
                'pe_ratio': parse_finviz_value(data_dict.get('P/E')),
                'eps_growth_yoy': parse_finviz_value(data_dict.get('EPS this Y', data_dict.get('EPS next 5Y'))),
                'sales_growth_yoy': parse_finviz_value(data_dict.get('Sales Q/Q', data_dict.get('Sales Y/Y TTM'))),
                'short_interest': parse_finviz_value(data_dict.get('Short Float')),
                'inst_ownership': parse_finviz_value(data_dict.get('Inst Own')),
                'inst_transactions': parse_finviz_value(data_dict.get('Inst Trans')),
            }

            return fundamentals

        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2 ** attempt) * RATE_LIMIT
                print(f"Error fetching {ticker}, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch {ticker} after {MAX_RETRIES} attempts")
                return None

    return None


def save_to_database(ticker: str, fundamentals: Dict):
    """Save fundamentals data to SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO fundamentals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        ticker,
        fundamentals.get('market_cap'),
        fundamentals.get('shares_float'),
        fundamentals.get('pe_ratio'),
        fundamentals.get('eps_growth_yoy'),
        fundamentals.get('sales_growth_yoy'),
        fundamentals.get('short_interest'),
        fundamentals.get('inst_ownership'),
        fundamentals.get('inst_transactions'),
        datetime.now(timezone.utc)
    ))

    conn.commit()
    conn.close()


def get_cached_fundamentals(tickers: List[str], max_age_days: int = None) -> Dict[str, Dict]:
    """Retrieve cached fundamentals for tickers (within max_age_days)."""
    init_database()

    if max_age_days is None:
        max_age_days = CACHE_DAYS

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    placeholders = ','.join(['?'] * len(tickers))
    cursor.execute(f'''
        SELECT * FROM fundamentals
        WHERE ticker IN ({placeholders})
        AND last_updated >= ?
    ''', (*tickers, cutoff_date))

    results = {}
    for row in cursor.fetchall():
        ticker = row[0]
        results[ticker] = {
            'market_cap': row[1],
            'shares_float': row[2],
            'pe_ratio': row[3],
            'eps_growth_yoy': row[4],
            'sales_growth_yoy': row[5],
            'short_interest': row[6],
            'inst_ownership': row[7],
            'inst_transactions': row[8],
            'last_updated': row[9]
        }

    conn.close()
    return results


def batch_fetch_fundamentals(tickers: List[str], refresh_stale: bool = True):
    """Batch fetch fundamentals for multiple tickers with caching."""
    init_database()

    cached = get_cached_fundamentals(tickers) if refresh_stale else {}

    to_fetch = [t for t in tickers if t not in cached]

    if not to_fetch:
        print("All tickers have fresh cached data")
        return

    print(f"Fetching fundamentals for {len(to_fetch)} tickers (cached: {len(cached)})")

    for ticker in tqdm(to_fetch, desc="Fetching fundamentals"):
        fundamentals = get_fundamental_data(ticker)

        if fundamentals:
            save_to_database(ticker, fundamentals)

        time.sleep(RATE_LIMIT)

    print(f"Completed fetching {len(to_fetch)} tickers")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Fetch fundamental data.')
    parser.add_argument('--all', action='store_true', help='Fetch for all tickers in daily_price.pkl')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of tickers to fetch')
    args = parser.parse_args()

    if args.all:
        print(f"Loading tickers from {PRICE_DATA_TA_FILE}...")
        try:
            daily_price = su.load_object_from_pickle(PRICE_DATA_TA_FILE)
            tickers = [t for t in daily_price.keys() if t not in ['SPY', 'MDY', 'QQQ', 'IWM', '^GSPC']]
            print(f"Found {len(tickers)} tickers")

            if args.limit:
                tickers = tickers[:args.limit]
                print(f"Limiting to first {len(tickers)} tickers")

            batch_fetch_fundamentals(tickers)

        except FileNotFoundError:
            print(f"Error: {PRICE_DATA_TA_FILE} not found. Run create_technical_indicators.py first.")
    else:
        print("No --all flag provided. Running test with sample tickers.")
        test_tickers = ['AAPL', 'NVDA', 'TSLA']
        batch_fetch_fundamentals(test_tickers)

        cached = get_cached_fundamentals(test_tickers)
        for ticker, data in cached.items():
            print(f"\n{ticker}:")
            for key, value in data.items():
                print(f"  {key}: {value}")
