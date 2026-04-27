"""
Export dashboard data from the daily stock screening output.

Reads the latest daily report and market breadth data, then exports
JSON files for the web dashboard in docs/data/.
"""

import json
import re
import csv
import io
import urllib.request
from pathlib import Path
from datetime import datetime

from config.settings import (
    CONFIG, REPORTS_DIR, BREADTH_FILE, BREADTH_HISTORY_FILE,
    DOCS_DATA_DIR, FUNDAMENTALS_DB, GOOGLE_SHEET_ID, PRICE_DATA_TA_FILE,
    SCREENING_OUTPUT_DIR
)
import src.stock_utils as su
from src.data_collection.fetch_macro_events import fetch_macro_events, write_events_json

OUTPUT_DIR = DOCS_DATA_DIR

# Google Sheets
etf_gid = CONFIG["dashboard"]["etf_sheet_gid"]
ind_gid = CONFIG["dashboard"]["industry_etf_sheet_gid"]

ETF_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={etf_gid}"
INDUSTRY_ETF_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={ind_gid}"

# Yahoo Finance tickers for macro data
MACRO_TICKERS = {
    'indices': [
        {'yahoo': 'ES=F', 'label': 'ES', 'name': 'S&P 500 Futures', 'tv': 'OANDA:SPX500USD'},
        {'yahoo': 'NQ=F', 'label': 'NQ', 'name': 'Nasdaq 100 Futures', 'tv': 'OANDA:NAS100USD'},
        {'yahoo': 'RTY=F', 'label': 'RTY', 'name': 'Russell 2000 Futures', 'tv': 'OANDA:US2000USD'},
        {'yahoo': 'YM=F', 'label': 'YM', 'name': 'Dow Jones Futures', 'tv': 'OANDA:US30USD'},
        {'yahoo': '^VIX', 'label': 'VIX', 'name': 'Volatility Index', 'tv': 'VIX'},
    ],
    'crypto': [
        {'yahoo': 'BTC-USD', 'label': 'BTC', 'name': 'Bitcoin', 'tv': 'BINANCE:BTCUSDT'},
        {'yahoo': 'ETH-USD', 'label': 'ETH', 'name': 'Ethereum', 'tv': 'BINANCE:ETHUSDT'},
        {'yahoo': 'SOL-USD', 'label': 'SOL', 'name': 'Solana', 'tv': 'BINANCE:SOLUSDT'},
        {'yahoo': 'XRP-USD', 'label': 'XRP', 'name': 'XRP', 'tv': 'BINANCE:XRPUSDT'},
        {'yahoo': 'BNB-USD', 'label': 'BNB', 'name': 'BNB Chain', 'tv': 'BINANCE:BNBUSDT'},
    ],
    'precious_metals': [
        {'yahoo': 'GC=F', 'label': 'Gold', 'name': 'Gold', 'tv': 'OANDA:XAUUSD'},
        {'yahoo': 'SI=F', 'label': 'Silver', 'name': 'Silver', 'tv': 'OANDA:XAGUSD'},
        {'yahoo': 'PL=F', 'label': 'Platinum', 'name': 'Platinum', 'tv': 'OANDA:XPTUSD'},
    ],
    'base_metals': [
        {'yahoo': 'HG=F', 'label': 'Copper', 'name': 'Copper', 'tv': 'CAPITALCOM:COPPER'},
        {'yahoo': 'ALI=F', 'label': 'Aluminum', 'name': 'Aluminum', 'tv': 'CAPITALCOM:ALUMINUM'},
    ],
    'energy': [
        {'yahoo': 'CL=F', 'label': 'WTI Crude', 'name': 'WTI Crude Oil', 'tv': 'OANDA:WTICOUSD'},
        {'yahoo': 'NG=F', 'label': 'Natural Gas', 'name': 'Natural Gas', 'tv': 'OANDA:NATGASUSD'},
    ],
    'yields': [
        {'yahoo': '^IRX', 'label': '2Y', 'name': '2Y Treasury', 'tv': 'CAPITALCOM:US2YR'},
        {'yahoo': '^TNX', 'label': '10Y', 'name': '10Y Treasury', 'tv': 'CAPITALCOM:US10YR'},
        {'yahoo': '^TYX', 'label': '30Y', 'name': '30Y Treasury', 'tv': 'CAPITALCOM:US30YR'},
    ],
    'dollar': [
        {'yahoo': 'DX-Y.NYB', 'label': 'DXY', 'name': 'Dollar Index', 'tv': 'CAPITALCOM:DXY'},
    ],
}


def find_latest_report():
    """Find the most recent daily report file."""
    reports = sorted(REPORTS_DIR.glob("daily_report_*.md"), reverse=True)
    if not reports:
        print("No daily reports found.")
        return None
    return reports[0]


def parse_report(filepath):
    """Parse the daily report markdown into structured data."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    result = {
        'report_date': None,
        'ncfd': None,
        'mmfi': None,
        'themes': [],
    }

    date_match = re.search(r'daily_report_(\d{4}-\d{2}-\d{2})', filepath.name)
    if date_match:
        result['report_date'] = date_match.group(1)

    ncfd_match = re.search(r'NCFD.*?:\s*([\d.]+)%', content)
    mmfi_match = re.search(r'MMFI.*?:\s*([\d.]+)%', content)
    mmtw_match = re.search(r'MMTW.*?:\s*([\d.]+)%', content)
    mmth_match = re.search(r'MMTH.*?:\s*([\d.]+)%', content)
    if ncfd_match:
        result['ncfd'] = float(ncfd_match.group(1))
    if mmfi_match:
        result['mmfi'] = float(mmfi_match.group(1))
    if mmtw_match:
        result['mmtw'] = float(mmtw_match.group(1))
    if mmth_match:
        result['mmth'] = float(mmth_match.group(1))

    # Parse themes
    lines = content.split('\n')
    theme_section_lines = []
    in_themes = False

    for line in lines:
        if '## 🌍 Market Themes' in line or '## Market Themes' in line:
            in_themes = True
            continue
        if in_themes and (
            '## 📈 Trading Performance' in line or
            '## Trading Performance' in line or
            '## Executive Summary' in line or
            (line.startswith('---') and len(line.strip()) <= 5)
        ):
            if '## 📊 Market Context' not in line:
                break
            continue
        if in_themes:
            theme_section_lines.append(line)

    theme_text = '\n'.join(theme_section_lines)

    theme_pattern = re.compile(
        r'###\s+(\d+)\.\s+(.+?)(?:\s+[🔥⚡💫🌟])?\s*\n'
        r'\*\*Theme Score\*\*:\s*([\d.]+)\s*\|\s*\*\*Avg RS\*\*:\s*([\d.]+)%',
        re.MULTILINE
    )

    # Collect ALL ### header positions (numbered themes AND uncategorized/other)
    # so we never bleed one section's tickers into another.
    all_section_starts = [m.start() for m in re.finditer(r'^### ', theme_text, re.MULTILINE)]

    for match in theme_pattern.finditer(theme_text):
        rank = int(match.group(1))
        name = match.group(2).strip()
        score = float(match.group(3))
        avg_rs = float(match.group(4))

        start = match.end()
        next_starts = [s for s in all_section_starts if s > match.start()]
        end = next_starts[0] if next_starts else len(theme_text)
        section = theme_text[start:end]

        tickers = parse_ticker_table(section)

        result['themes'].append({
            'rank': rank,
            'name': name,
            'score': score,
            'avg_rs': avg_rs,
            'tickers': tickers
        })

    return result


def parse_ticker_table(section):
    """Parse a markdown table of tickers from a theme section."""
    tickers = []
    lines = section.strip().split('\n')
    in_table = False
    headers = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('###'):
            break
        if line.startswith('**Tickers:**'):
            continue
        if '|' in line:
            cells = [c.strip() for c in line.split('|')]
            cells = [c for c in cells if c]
            if not in_table:
                if 'Ticker' in cells[0] or 'ticker' in cells[0].lower():
                    headers = cells
                    in_table = True
                continue
            if cells and ('Ticker' in cells[0] or 'ticker' in cells[0].lower()):
                break
            if all(c.replace('-', '').replace(':', '') == '' for c in cells):
                continue
            if len(cells) >= 7 and headers:
                ticker_data = {
                    'ticker': cells[0].strip(),
                    'rs': safe_float(cells[1]),
                    'price': safe_float(cells[2]),
                    'float': cells[4].strip() if len(cells) > 4 else None,
                    'eps': cells[5].strip() if len(cells) > 5 else None,
                    'sales': cells[6].strip() if len(cells) > 6 else None,
                    'inst': cells[7].strip() if len(cells) > 7 else None,
                    'short': safe_float(cells[8]) if len(cells) > 8 else None,
                }
                tickers.append(ticker_data)
    return tickers


def safe_float(val):
    if val is None:
        return None
    val = str(val).strip().replace(',', '')
    if val in ('N/A', '—', '-', ''):
        return None
    try:
        return float(val)
    except ValueError:
        return None


def fetch_yahoo_macro_data():
    """Fetch macro data from Yahoo Finance using yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        print("  Warning: yfinance not installed. Run: pip install yfinance")
        return None

    all_tickers = []
    for group in MACRO_TICKERS.values():
        for item in group:
            all_tickers.append(item['yahoo'])

    print(f"  Fetching {len(all_tickers)} tickers from Yahoo Finance...")

    try:
        data_map = {}

        for ticker_info in [item for group in MACRO_TICKERS.values() for item in group]:
            sym = ticker_info['yahoo']
            try:
                t = yf.Ticker(sym)
                hist = t.history(period='1y')
                if hist.empty:
                    data_map[sym] = None
                    continue

                current = float(hist['Close'].iloc[-1])
                prev_close = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else current

                # 1D%
                d1_pct = ((current - prev_close) / prev_close) * 100

                # 1W% (5 trading days)
                w1_close = float(hist['Close'].iloc[-6]) if len(hist) >= 6 else current
                w1_pct = ((current - w1_close) / w1_close) * 100

                # 52W High%
                high_52w = float(hist['High'].max())
                hi_pct = ((current - high_52w) / high_52w) * 100

                # YTD%
                ytd_start = hist[hist.index.year == hist.index[-1].year]
                if not ytd_start.empty:
                    ytd_open = float(ytd_start['Open'].iloc[0])
                    ytd_pct = ((current - ytd_open) / ytd_open) * 100
                else:
                    ytd_pct = 0

                data_map[sym] = {
                    'price': round(current, 2),
                    'd1': round(d1_pct, 2),
                    'w1': round(w1_pct, 2),
                    'hi52w': round(hi_pct, 2),
                    'ytd': round(ytd_pct, 2),
                }
            except Exception as e:
                print(f"    Warning: Failed to fetch {sym}: {e}")
                data_map[sym] = None

        # Build structured output
        result = {}
        for group_key, group_items in MACRO_TICKERS.items():
            result[group_key] = []
            for item in group_items:
                entry = {
                    'label': item['label'],
                    'name': item['name'],
                    'tv': item['tv'],
                }
                ydata = data_map.get(item['yahoo'])
                if ydata:
                    entry.update(ydata)
                else:
                    entry['price'] = None
                result[group_key].append(entry)

        result['timestamp'] = datetime.now().isoformat()
        return result

    except Exception as e:
        print(f"  Error fetching Yahoo data: {e}")
        return None


def update_breadth_history():
    """Update market breadth history with the latest reading."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    current = {}
    if BREADTH_FILE.exists():
        with open(BREADTH_FILE, 'r') as f:
            current = json.load(f)

    history = {
        'ncfd': {'current': 0, 'history': []},
        'mmfi': {'current': 0, 'history': []},
        'mmtw': {'current': 0, 'history': []},
        'mmth': {'current': 0, 'history': []},
    }
    if BREADTH_HISTORY_FILE.exists():
        with open(BREADTH_HISTORY_FILE, 'r') as f:
            history = json.load(f)

    # Update each breadth indicator from the daily breadth file
    for key in ['ncfd', 'mmfi', 'mmtw', 'mmth']:
        if key in current:
            val = current[key]
            hist = history.get(key, {}).get('history', [])
            if not hist or hist[-1] != val:
                hist.append(val)
            history[key] = {'current': val, 'history': hist[-5:]}

    # Fetch NCFD/MMFI/MMTW/MMTH from barchart.com (Playwright required)
    barchart_data = fetch_barchart_breadth()
    if barchart_data:
        for key in ['ncfd', 'mmfi', 'mmtw', 'mmth']:
            if key in barchart_data and barchart_data[key] is not None:
                val = barchart_data[key]
                hist = history.get(key, {}).get('history', [])
                if not hist or hist[-1] != val:
                    hist.append(val)
                history[key] = {'current': val, 'history': hist[-5:]}

    # CNN Fear & Greed
    fng = fetch_cnn_fear_greed()
    if fng is not None:
        history['fear_greed'] = fng

    # NAAIM Exposure
    naaim = fetch_naaim_exposure()
    if naaim is not None:
        history['naaim'] = naaim

    history['timestamp'] = current.get('timestamp', datetime.now().isoformat())

    with open(BREADTH_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

    ncfd_val = history.get('ncfd', {}).get('current', 'N/A')
    mmfi_val = history.get('mmfi', {}).get('current', 'N/A')
    mmtw_val = history.get('mmtw', {}).get('current', 'N/A')
    mmth_val = history.get('mmth', {}).get('current', 'N/A')
    print(f"  Market breadth updated: NCFD={ncfd_val}, MMFI={mmfi_val}, MMTW={mmtw_val}, MMTH={mmth_val}")
    return history


def fetch_barchart_breadth():
    """Fetch NCFD, MMFI, MMTW and MMTH from barchart.com using Playwright (JS-rendered pages)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  Warning: Playwright not installed. Run: pip install playwright && playwright install chromium")
        return None

    result = {}
    symbols = {
        'ncfd': 'https://www.barchart.com/stocks/quotes/$NCFD/overview',
        'mmfi': 'https://www.barchart.com/stocks/quotes/$MMFI/overview',
        'mmtw': 'https://www.barchart.com/stocks/quotes/$MMTW/overview',
        'mmth': 'https://www.barchart.com/stocks/quotes/$MMTH/overview',
    }

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers({
                'User-Agent': CONFIG["market_breadth"]["user_agent"]
            })

            for key, url in symbols.items():
                try:
                    page.goto(url, timeout=60000, wait_until='domcontentloaded')
                    page.wait_for_selector('span.last-change', timeout=20000)
                    import time
                    time.sleep(3)

                    price_text = page.evaluate('''() => {
                        const el = document.querySelector('span.last-change[data-ng-class*="lastPrice"]');
                        if (el && el.textContent.trim()) return el.textContent.trim();
                        const first = document.querySelector('span.last-change');
                        if (first && first.textContent.trim()) return first.textContent.trim();
                        return null;
                    }''')

                    if price_text:
                        val = float(price_text.replace('%', '').replace(',', '').strip())
                        result[key] = round(val, 2)
                        print(f"    {key.upper()} = {val}%")
                    else:
                        print(f"    Warning: Could not extract {key.upper()} price from barchart")
                except Exception as e:
                    print(f"    Warning: Failed to scrape {key.upper()}: {e}")

            browser.close()
    except Exception as e:
        print(f"  Error with Playwright scraping: {e}")
        return None

    return result if result else None


def fetch_cnn_fear_greed():
    """Fetch CNN Fear & Greed Index using Playwright to bypass bot blocking."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  Warning: Playwright not installed. Cannot fetch CNN Fear & Greed.")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            url = 'https://production.dataviz.cnn.io/index/fearandgreed/graphdata'
            response = page.goto(url, timeout=30000)
            if response and response.ok:
                data = response.json()
                score = data.get('fear_and_greed', {}).get('score', None)
                rating = data.get('fear_and_greed', {}).get('rating', None)
                if score is not None:
                    browser.close()
                    return {'score': round(float(score), 1), 'rating': rating}
            browser.close()
    except Exception as e:
        print(f"  Warning: Could not fetch CNN Fear & Greed via Playwright: {e}")
    return None


def fetch_naaim_exposure():
    """Fetch NAAIM Exposure Index from their website."""
    try:
        url = 'https://www.naaim.org/programs/naaim-exposure-index/'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode('utf-8')

        matches = re.findall(r'\[new Date\(\d{4},\s*\d+,\s*\d+\),\s*([-\d.]+)\s*\]', html)
        if matches:
            naaim_values = [float(m) for m in matches if float(m) < 1000]
            if naaim_values:
                return {'value': naaim_values[-1]}
    except Exception as e:
        print(f"  Warning: Could not fetch NAAIM: {e}")
    return None


def fetch_sheet_csv(url):
    """Fetch CSV data from Google Sheets."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode('utf-8')
    except Exception as e:
        print(f"  Warning: Could not fetch sheet: {e}")
        return None


def fetch_etf_data():
    """Fetch leverage ETF data from Google Sheets."""
    text = fetch_sheet_csv(ETF_SHEET_URL)
    if not text:
        return None

    reader = csv.DictReader(io.StringIO(text))
    etf_list = []
    seen = set()
    for row in reader:
        ticker = row.get('Ticker', '').strip()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        etf_list.append({
            'ticker': ticker,
            'name': row.get('Name', '').strip(),
            'rs': row.get('Relative Strength', '').strip(),
            'rs_sts': safe_float(row.get('RS_STS%', '').replace('%', '')),
            'intraday': safe_float(row.get('Intraday %', '').replace('%', '')),
            'daily': safe_float(row.get('Daily %', '').replace('%', '')),
            'monthly': safe_float(row.get('Monthly %', '').replace('%', '')),
        })
    return etf_list


def fetch_industry_etf_data():
    """Fetch industry ETF data from Google Sheets — only the 'Industry' section."""
    text = fetch_sheet_csv(INDUSTRY_ETF_SHEET_URL)
    if not text:
        return None

    lines = text.strip().split('\n')
    etf_list = []
    seen = set()
    current_section = ''
    in_industry_section = False

    for line in lines:
        row = list(csv.reader(io.StringIO(line)))[0]
        if len(row) < 7:
            continue

        col0 = row[0].strip()
        col2 = row[2].strip()

        if col0 == '1 Month RS':
            current_section = col2
            in_industry_section = current_section == 'Industry'
            continue

        if not in_industry_section:
            continue

        if col0 == 'Reference Index' or col2 == '':
            continue

        ticker = col2
        if not ticker or ticker in seen:
            continue

        clean_ticker = ticker.split(':')[-1] if ':' in ticker else ticker

        seen.add(ticker)
        rs_sts = safe_float(row[1].replace('%', '')) if row[1] else None
        name = row[3].strip() if len(row) > 3 else ''
        intraday = safe_float(row[4].replace('%', '')) if len(row) > 4 and row[4] else None
        daily = safe_float(row[5].replace('%', '')) if len(row) > 5 and row[5] else None
        monthly = safe_float(row[6].replace('%', '')) if len(row) > 6 and row[6] else None
        lev_long = row[7].strip() if len(row) > 7 else ''
        lev_short = row[8].strip() if len(row) > 8 else ''

        etf_list.append({
            'ticker': clean_ticker,
            'display_ticker': ticker,
            'name': name,
            'rs_sts': rs_sts,
            'intraday': intraday,
            'daily': daily,
            'monthly': monthly,
            'lev_long': lev_long,
            'lev_short': lev_short,
        })

    return etf_list


def enrich_themes_from_db(theme_data):
    """Enrich theme ticker data with fresh inst% and short% from the fundamentals DB."""
    import sqlite3
    if not FUNDAMENTALS_DB.exists():
        print("   No fundamentals DB found, skipping enrichment")
        return

    all_tickers = set()
    for theme in theme_data.get('themes', []):
        for t in theme.get('tickers', []):
            if t.get('ticker'):
                all_tickers.add(t['ticker'])

    if not all_tickers:
        return

    conn = sqlite3.connect(FUNDAMENTALS_DB)
    cursor = conn.cursor()
    placeholders = ','.join(['?'] * len(all_tickers))
    cursor.execute(f'''
        SELECT ticker, inst_transactions, short_interest
        FROM fundamentals
        WHERE ticker IN ({placeholders})
    ''', list(all_tickers))

    db_data = {}
    for row in cursor.fetchall():
        db_data[row[0]] = {
            'inst_trans': row[1],
            'short_interest': row[2],
        }
    conn.close()

    enriched_count = 0
    for theme in theme_data.get('themes', []):
        for t in theme.get('tickers', []):
            ticker = t.get('ticker')
            if ticker and ticker in db_data:
                db = db_data[ticker]
                if db['inst_trans'] is not None:
                    val = db['inst_trans']
                    t['inst'] = f"+{val:.1f}" if val > 0 else f"{val:.1f}"
                    enriched_count += 1
                if db['short_interest'] is not None:
                    t['short'] = round(db['short_interest'], 1)

    print(f"   Enriched {enriched_count} tickers with finviz inst%/short% from DB")


def load_ticker_color_flags():
    """Load tight/inside_day + close_to_ma flags, return {ticker: 'green'|'blue'}."""
    try:
        daily_price = su.load_object_from_pickle(PRICE_DATA_TA_FILE)
    except Exception as e:
        print(f"   Warning: Could not load price data for ticker colors: {e}")
        return {}

    flags = {}
    for ticker, df in daily_price.items():
        if ticker.startswith('^') or df.empty:
            continue
        last = df.iloc[-1]
        tight = bool(last.get('tight_day', False)) or bool(last.get('inside_day', False))
        if not tight:
            continue
        close_to_ma = bool(last.get('close_to_ma', False))
        flags[ticker] = 'green' if close_to_ma else 'blue'
    return flags


def enrich_with_ticker_color(data_list, flags, ticker_key='ticker'):
    """Add ticker_color ('green'|'blue') to a list of ticker dicts."""
    count = 0
    for item in data_list:
        tk = item.get(ticker_key, '')
        if tk in flags:
            item['ticker_color'] = flags[tk]
            count += 1
    return count


def fetch_etf_ticker_colors(tickers):
    """Download recent OHLC for ETF tickers and compute ticker colors.

    This is a standalone fetch that does NOT pollute the main price data pipeline.
    Returns a dict of {ticker: 'green'|'blue'} for tight/inside-day tickers.
    """
    if not tickers:
        return {}
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        print("   Warning: yfinance not installed, skipping ETF ticker colors")
        return {}

    # Download 2 months to securely have enough for rolling windows
    unique_tickers = list(set(tickers))
    print(f"   Fetching OHLC for {len(unique_tickers)} ETF tickers from Yahoo Finance...")
    try:
        data = yf.download(unique_tickers, period='2mo', progress=False, group_by='ticker')
    except Exception as e:
        print(f"   Warning: yfinance download failed: {e}")
        return {}

    flags = {}
    for ticker in unique_tickers:
        try:
            if len(unique_tickers) == 1:
                df = data
            else:
                df = data[ticker]
            df = df.dropna(subset=['Close'])
            if len(df) < 2:
                continue

            high = pd.to_numeric(df['High'])
            low = pd.to_numeric(df['Low'])
            close = pd.to_numeric(df['Close'])

            # ADR% (20-day rolling avg of high/low ratio - 1)
            adr_pct = (high / low).rolling(window=20, min_periods=1).mean() - 1

            # EMA10, EMA20
            ema10 = close.ewm(span=10, adjust=False).mean()
            ema20 = close.ewm(span=20, adjust=False).mean()

            # ATR14
            high_low = high - low
            high_prev = (high - close.shift(1)).abs()
            low_prev = (low - close.shift(1)).abs()
            tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
            atr14 = tr.rolling(window=14, min_periods=1).mean()

            last = df.iloc[-1]
            prev = df.iloc[-2]
            last_close = float(last['Close'])
            last_open = float(last['Open'])

            # Inside Day
            inside = float(last['High']) < float(prev['High']) and float(last['Low']) > float(prev['Low'])

            # Tight Day (0.25 * ADR% * open)
            tight = abs(last_close - last_open) < 0.25 * float(adr_pct.iloc[-1]) * last_open

            if not (inside or tight):
                continue

            # Close to MAs
            close_to_ema10 = abs(last_close - float(ema10.iloc[-1])) < 0.5 * float(atr14.iloc[-1])
            close_to_ema20 = abs(last_close - float(ema20.iloc[-1])) < 0.5 * float(atr14.iloc[-1])

            flags[ticker] = 'green' if (close_to_ema10 or close_to_ema20) else 'blue'
        except Exception as e:
            print(f"     Error processing {ticker}: {e}")
            continue

    print(f"   Found {len(flags)} ETFs with tight/inside day colors")
    return flags


THEMES_HISTORY_MAX = 5  # Keep last N trading sessions


def _update_history_file(history_file, report_date, entry):
    """Append an entry to a history JSON file, keeping last N sessions.

    Each entry in the history array must have a 'report_date' key.
    """
    history = []
    if history_file.exists():
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except (json.JSONDecodeError, ValueError):
            history = []

    # Replace existing entry for same date, or append
    history = [h for h in history if h.get('report_date') != report_date]
    history.append(entry)

    # Sort descending by date, keep only last N
    history.sort(key=lambda x: x.get('report_date', ''), reverse=True)
    history = history[:THEMES_HISTORY_MAX]

    with open(history_file, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=2)
    dates = [h['report_date'] for h in history]
    print(f"   -> {history_file} (history: {', '.join(dates)})")


def update_themes_history(theme_data):
    """Append current theme snapshot to history."""
    _update_history_file(
        OUTPUT_DIR / "themes_history.json",
        theme_data.get('report_date', ''),
        theme_data,
    )


def update_etf_history(report_date, etf_data, industry_data):
    """Append current ETF snapshots to their history files."""
    if etf_data:
        _update_history_file(
            OUTPUT_DIR / "etf_data_history.json",
            report_date,
            {'report_date': report_date, 'data': etf_data},
        )
    if industry_data:
        _update_history_file(
            OUTPUT_DIR / "industry_etf_history.json",
            report_date,
            {'report_date': report_date, 'data': industry_data},
        )


def _fmt_float_m(val):
    """Format raw share count as float-in-millions string (e.g. 132300000 -> '132.3')."""
    if val is None:
        return None
    try:
        return f"{float(val) / 1e6:.1f}"
    except (ValueError, TypeError):
        return None


def _fmt_growth(val):
    """Format an EPS/sales growth percent value with no decimals (e.g. 15.4 -> '15')."""
    if val is None:
        return None
    try:
        return f"{float(val):.0f}"
    except (ValueError, TypeError):
        return None


def _fmt_inst(val):
    """Format institutional transaction percentage with explicit sign (e.g. 0.9 -> '+0.9')."""
    if val is None:
        return None
    try:
        v = float(val)
    except (ValueError, TypeError):
        return None
    return f"+{v:.1f}" if v > 0 else f"{v:.1f}"


def export_momentum_136(day_flags):
    """Export momentum_136 screener results grouped by theme to docs/data/momentum_136.json.

    Singletons preserved (passes empty groups dict to build_theme_to_tickers).
    Themes ordered by ticker count desc, alphabetical tiebreaker.
    Also appends to docs/data/momentum_136_history.json (last 5 sessions).
    """
    import sqlite3
    import pandas as pd
    from src.themes.theme_registry import load_ticker_themes
    from src.themes.theme_taxonomy import build_theme_to_tickers

    # Locate latest screener output CSV
    try:
        csv_file = su.get_latest_file(
            SCREENING_OUTPUT_DIR / 'momentum_136', 'momentum_136_*.csv', 1
        )
    except Exception as e:
        print(f"   No momentum_136 CSV found, skipping: {e}")
        return

    df = pd.read_csv(csv_file).fillna(0)
    if df.empty:
        print("   momentum_136 CSV is empty, skipping export")
        return

    csv_date = str(df['date'].iloc[0]) if 'date' in df.columns else ''

    # Build ticker -> themes filtered map (Uncategorized fallback so untagged tickers still appear)
    ticker_themes = load_ticker_themes()
    screener_tickers = df['ticker'].astype(str).str.upper().tolist()
    filtered_map = {
        t: ticker_themes.get(t) or ['Uncategorized']
        for t in screener_tickers
    }

    # Reverse to theme -> tickers; empty groups dict skips consolidation/removal (preserves singletons)
    theme_to_tickers = build_theme_to_tickers(filtered_map, {})

    # Single-shot fundamentals lookup
    fundamentals = {}
    if FUNDAMENTALS_DB.exists() and screener_tickers:
        conn = sqlite3.connect(FUNDAMENTALS_DB)
        placeholders = ','.join(['?'] * len(screener_tickers))
        rows = conn.execute(f'''
            SELECT ticker, shares_float, eps_growth_yoy, sales_growth_yoy,
                   short_interest, inst_transactions
            FROM fundamentals
            WHERE ticker IN ({placeholders})
        ''', screener_tickers).fetchall()
        conn.close()
        for r in rows:
            fundamentals[r[0]] = {
                'shares_float': r[1],
                'eps_growth_yoy': r[2],
                'sales_growth_yoy': r[3],
                'short_interest': r[4],
                'inst_transactions': r[5],
            }

    # Per-ticker dict from master CSV row + fundamentals
    per_ticker = {}
    for _, row in df.iterrows():
        t = str(row['ticker']).upper()
        f = fundamentals.get(t, {})
        short_val = f.get('short_interest')
        per_ticker[t] = {
            'ticker': t,
            'rs': round(float(row.get('rs_sts_pct', 0) or 0), 1),
            'price': round(float(row.get('close', 0) or 0), 2),
            'float': _fmt_float_m(f.get('shares_float')),
            'eps': _fmt_growth(f.get('eps_growth_yoy')),
            'sales': _fmt_growth(f.get('sales_growth_yoy')),
            'inst': _fmt_inst(f.get('inst_transactions')),
            'short': round(float(short_val), 1) if short_val is not None else None,
        }

    # Build theme list (sort tickers within theme by RS desc, themes by count desc)
    themes_list = []
    for theme_name, tickers in theme_to_tickers.items():
        ticker_dicts = [per_ticker[t] for t in tickers if t in per_ticker]
        if not ticker_dicts:
            continue
        ticker_dicts.sort(key=lambda x: -x['rs'])
        themes_list.append({'name': theme_name, 'tickers': ticker_dicts})

    # Catch-all buckets (mirrors _remove_noise in config/theme_groups.yaml) sort to the
    # bottom regardless of count — they're overflow, not thematic concentration.
    catchall_themes = {
        'Uncategorized',
        'Individual Episodic Pivots / Singletons',
        'Meme Stocks',
    }
    themes_list.sort(key=lambda th: (
        th['name'] in catchall_themes,
        -len(th['tickers']),
        th['name'],
    ))

    # Apply ticker_color flags
    if day_flags:
        for th in themes_list:
            enrich_with_ticker_color(th['tickers'], day_flags)

    momentum_data = {
        'report_date': csv_date,
        'themes': themes_list,
    }

    out = OUTPUT_DIR / "momentum_136.json"
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump(momentum_data, fh, indent=2)
    total_tickers = sum(len(th['tickers']) for th in themes_list)
    print(f"   -> {out} ({len(themes_list)} themes, {total_tickers} tickers)")

    _update_history_file(
        OUTPUT_DIR / "momentum_136_history.json",
        csv_date,
        momentum_data,
    )


def export_all():
    """Main export function."""
    print("=" * 60)
    print("EXPORTING DASHBOARD DATA")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    day_flags = None  # Lazy-loaded day pattern flags

    # 1. Parse latest report
    report_file = find_latest_report()
    theme_data = None
    if report_file:
        print(f"\n1. Parsing report: {report_file.name}")
        theme_data = parse_report(report_file)

        # Enrich theme tickers with fresh finviz data from fundamentals DB
        enrich_themes_from_db(theme_data)

        # Enrich theme tickers with ticker color flags (tight/inside + close_to_ma)
        day_flags = load_ticker_color_flags()
        theme_pattern_count = 0
        for theme in theme_data.get('themes', []):
            theme_pattern_count += enrich_with_ticker_color(theme.get('tickers', []), day_flags)
        print(f"   Enriched {theme_pattern_count} theme tickers with ticker color flags")

        theme_output = OUTPUT_DIR / "themes.json"
        with open(theme_output, 'w', encoding='utf-8') as f:
            json.dump(theme_data, f, indent=2)
        print(f"   -> {theme_output} ({len(theme_data['themes'])} themes)")

        # Update themes history (keep last 5 trading sessions)
        update_themes_history(theme_data)
    else:
        print("\n1. No report found, skipping themes export")

    # 1b. Export Momentum 1/3/6 screener (independent of daily report)
    print("\n1b. Exporting momentum_136 data")
    if day_flags is None:
        day_flags = load_ticker_color_flags()
    export_momentum_136(day_flags)

    # 2. Update market breadth history
    print("\n2. Updating market breadth history")
    update_breadth_history()

    # 3. Fetch leverage ETF data
    print("\n3. Fetching leverage ETF data")
    etf_data = fetch_etf_data()
    if etf_data:
        etf_output = OUTPUT_DIR / "etf_data.json"
        with open(etf_output, 'w', encoding='utf-8') as f:
            json.dump(etf_data, f, indent=2)
        print(f"   -> {etf_output} ({len(etf_data)} ETFs)")

    # 4. Fetch Industry ETF data
    print("\n4. Fetching industry ETF data")
    industry_data = fetch_industry_etf_data()
    if industry_data:
        ind_output = OUTPUT_DIR / "industry_etf.json"
        with open(ind_output, 'w', encoding='utf-8') as f:
            json.dump(industry_data, f, indent=2)
        print(f"   -> {ind_output} ({len(industry_data)} industry ETFs)")

    # 4b. Enrich ETFs with inside_day / tight_day via standalone yfinance fetch
    all_etf_tickers = []
    if etf_data:
        all_etf_tickers += [e['ticker'] for e in etf_data]
    if industry_data:
        all_etf_tickers += [e['ticker'] for e in industry_data]
    if all_etf_tickers:
        print("\n   Computing ticker colors for ETFs...")
        etf_day_flags = fetch_etf_ticker_colors(all_etf_tickers)
        if etf_day_flags:
            if etf_data:
                etf_pc = enrich_with_ticker_color(etf_data, etf_day_flags)
                print(f"   Enriched {etf_pc} leverage ETFs with ticker color flags")
                with open(OUTPUT_DIR / "etf_data.json", 'w', encoding='utf-8') as f:
                    json.dump(etf_data, f, indent=2)
            if industry_data:
                ind_pc = enrich_with_ticker_color(industry_data, etf_day_flags)
                print(f"   Enriched {ind_pc} industry ETFs with ticker color flags")
                with open(OUTPUT_DIR / "industry_etf.json", 'w', encoding='utf-8') as f:
                    json.dump(industry_data, f, indent=2)

    # 4c. Update ETF history (use theme report_date as session date)
    report_date = theme_data.get('report_date', '') if theme_data else ''
    if report_date and (etf_data or industry_data):
        update_etf_history(report_date, etf_data, industry_data)

    # 5. Fetch Yahoo Finance macro data
    print("\n5. Fetching Yahoo Finance macro data")
    macro_data = fetch_yahoo_macro_data()
    if macro_data:
        macro_output = OUTPUT_DIR / "macro_data.json"
        with open(macro_output, 'w', encoding='utf-8') as f:
            json.dump(macro_data, f, indent=2)
        print(f"   -> {macro_output}")
    else:
        print("   -> Macro data fetch failed, charts will show TradingView data only")

    # 6. Fetch upcoming macro events from Forex Factory
    print("\n6. Fetching upcoming macro events")
    try:
        macro_events = fetch_macro_events()
        if macro_events:
            write_events_json(macro_events, OUTPUT_DIR / "events.json")
        else:
            print("   -> Macro events fetch returned no data, keeping existing events.json")
    except Exception as e:
        print(f"   -> Macro events fetch failed: {e} -- keeping existing events.json")

    # 7. Report meta
    print("\n7. Writing report meta")
    meta = {
        'export_timestamp': datetime.now().isoformat(),
        'report_date': theme_data.get('report_date') if theme_data else None,
        'theme_count': len(theme_data.get('themes', [])) if theme_data else 0,
        'etf_count': len(etf_data) if etf_data else 0,
        'industry_etf_count': len(industry_data) if industry_data else 0,
    }
    with open(OUTPUT_DIR / "report_meta.json", 'w') as f:
        json.dump(meta, f, indent=2)

    print(f"\n{'=' * 60}")
    print("EXPORT COMPLETE")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    export_all()
