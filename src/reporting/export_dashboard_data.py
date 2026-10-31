"""
Export dashboard data from the daily stock screening output.

Reads the latest daily report and market breadth data, then exports
JSON files for the web dashboard in docs/data/.
"""

import json
import math
import re
import csv
import io
import traceback
import urllib.request
from pathlib import Path
from datetime import datetime, date, timedelta, timezone

from config.settings import (
    CONFIG, REPORTS_DIR, BREADTH_FILE, BREADTH_HISTORY_FILE,
    DOCS_DATA_DIR, FUNDAMENTALS_DB, GOOGLE_SHEET_ID, PRICE_DATA_TA_FILE,
    PROJECT_ROOT, SCREENING_OUTPUT_DIR, SHORT_INTEREST_FILE
)
import src.stock_utils as su
# Re-exported: update_breadth_history calls it through this module's namespace,
# which is what the breadth tests patch.
from src.data_collection.barchart_breadth import (
    BREADTH_KEYS, breadth_reading_is_fresh, fetch_barchart_breadth,
)
from src.data_collection.fetch_macro_events import fetch_macro_events, write_events_json
from src.indicators.create_technical_indicators import (
    compute_atr14,
    compute_ema_pair,
    compute_highlight_tier,
    compute_inside_day,
    compute_sma50_full,
    compute_spy_cum_norm_100,
    compute_weekly_ema,
    weekly_ema_config,
)
from src.screening.screeners.parabolic import MIN_ATR_MULTI_50SMA, MIN_AVG_DOLLAR_VOL
from src.themes.theme_taxonomy import resolve_l1, split_path

OUTPUT_DIR = DOCS_DATA_DIR

#: The two overflow buckets, which are not trading narratives. The flat-table
#: tabs sink them to the bottom; the L1-clustered tabs and the zone maps hide them.
CATCHALL_THEMES = frozenset({'Uncategorized', 'Singleton'})


def _attach_hierarchy(themes_list):
    """Attach l1/l2/l3 fields to each theme entry by splitting the path name.

    Uses the strict :func:`split_path` for L2/L3 (these only make sense for
    canonical hierarchical paths) but falls back to :func:`resolve_l1` for L1
    so legacy labels like ``"AI - Memory & Storage"`` or bare ``"Drones"``
    still bucket into the right narrative hub. Mutates the list in place and
    returns it.
    """
    for entry in themes_list:
        name = entry.get('name', '')
        _, l2, l3 = split_path(name)
        entry['l1'] = resolve_l1(name) or split_path(name)[0]
        entry['l2'] = l2
        entry['l3'] = l3
    return themes_list


VARS_ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "vars"

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


def latest_report_date():
    """Session date of the newest daily report, read from its file name, or None.

    The report body is not parsed: `report_meta.json` carries only this date
    and the export timestamp, and the ETF tabs use it as their session date.
    """
    for report in sorted(REPORTS_DIR.glob("daily_report_*.md"), reverse=True):
        match = re.search(r'daily_report_(\d{4}-\d{2}-\d{2})', report.name)
        if match:
            return match.group(1)
    print("No daily reports found.")
    return None


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
        print("  Warning: yfinance not installed. Run: uv sync")
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


def update_breadth_history(session_date=None):
    """Update market breadth history with the latest reading.

    ``session_date`` keys the NCFD/MMFI/MMTW/MMTH histories: each session holds
    one reading, and a re-run of the same session replaces it. Defaults to
    today.
    """
    session = session_date or date.today().isoformat()
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

    # One reading per indicator per session. Step 3 of the daily workflow
    # scrapes barchart.com into BREADTH_FILE; when that file is fresh and
    # complete it is the reading, and no second browser is opened. Otherwise
    # (a standalone export, or step 3 missed a key) barchart is scraped here
    # and the file fills only the keys that scrape missed. `as_of` lets a
    # re-run of the same session replace its reading rather than append a
    # second one: winner_research's backtests read these histories.
    if breadth_reading_is_fresh(current):
        barchart_data = current
    else:
        barchart_data = fetch_barchart_breadth() or {}
    for key in BREADTH_KEYS:
        val = barchart_data.get(key)
        if val is None:
            val = current.get(key)
        if val is None:
            continue
        entry = history.get(key, {})
        hist = list(entry.get('history', []))
        if hist and entry.get('as_of') == session:
            hist[-1] = val
        else:
            hist.append(val)
        history[key] = {'current': val, 'history': hist[-5:], 'as_of': session}

    # CNN Fear & Greed
    fng = fetch_cnn_fear_greed()
    if fng is not None:
        history['fear_greed'] = fng

    # AAII Sentiment Survey (weekly, published Thursdays)
    aaii = fetch_aaii_sentiment()
    if aaii is not None:
        history['aaii'] = aaii

    # NAAIM Exposure Index (weekly, surveyed Wednesdays). Restored 2026-09 from
    # StockCharts after naaim.org walled its current reading.
    #
    # Overwrite-on-success only, deliberately: a failed fetch leaves the previous
    # reading in place carrying its own survey date, which then visibly ages.
    # That is the AAII tile's behaviour too. The 2026-08 failure this tile is
    # infamous for was not the surviving value -- it was that the value carried
    # no date, so a dead scraper and an unchanged market looked identical.
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
    naaim_val = history.get('naaim', {}).get('value', 'N/A')
    naaim_as_of = history.get('naaim', {}).get('as_of', '?')
    print(f"  Market breadth updated: NCFD={ncfd_val}, MMFI={mmfi_val}, MMTW={mmtw_val}, MMTH={mmth_val}, "
          f"NAAIM={naaim_val} (survey {naaim_as_of})")
    return history


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


# Month names are matched explicitly rather than through strptime('%B'), which
# reads LC_TIME and would fail on a runner with a non-English locale.
_AAII_MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11,
    'december': 12,
}

# The current-week figures hang off `ssv2-snum`; the historical averages beside
# them use `ssv2-savg` and also sum to 100, so the class is the only thing that
# separates the two. `ssv2` is a redesign version prefix -- a v3 rename breaks
# both patterns at once, which is what the split warning below is for.
_AAII_FIGURE_RE = re.compile(r'ssv2-snum\s+(bull|neut|bear)"\s*>\s*([\d.]+)\s*%')
_AAII_WEEK_RE = re.compile(
    r'ssv2-gauge-week"\s*>\s*Week ending\s+([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})'
)


def parse_aaii_sentiment(html):
    """Extract the current week's AAII figures from the survey page markup.

    Returns ``{'bullish', 'neutral', 'bearish', 'week_ending'}`` or ``None``.
    All three percentages or nothing: a two-of-three reading renders as a real
    survey result rather than as a broken parse. ``week_ending`` alone may be
    ``None`` -- losing the date is not a reason to drop three good figures, and
    the tile names that gap on screen.
    """
    figures = {}
    for key, raw in _AAII_FIGURE_RE.findall(html):
        try:
            value = float(raw)
        except ValueError:
            # `[\d.]+` matches strings float() rejects ("3.2.9", "."). Returning
            # None keeps the documented contract: this function reports a bad
            # page, it never raises. An escaping ValueError would not stop at
            # the tile -- fetch_aaii_sentiment calls this outside its request
            # try, and neither update_breadth_history nor export_all guards the
            # chain, so a malformed figure would abort the whole daily export.
            return None

        # A responsive layout can render the same gauge twice (desktop + mobile
        # copy), so a repeat is only a problem when the copies disagree --
        # then there is no basis for picking one and the page shape has really
        # moved. Agreeing copies are the same reading and stay usable.
        if key in figures and figures[key] != value:
            return None
        figures[key] = value

    if set(figures) != {'bull', 'neut', 'bear'}:
        return None
    if any(not 0 <= v <= 100 for v in figures.values()):
        return None
    # Catches a shape change that still matches the hooks but grabs the wrong
    # numbers. AAII rounds to 0.1, so the three rarely sum to exactly 100.
    if abs(sum(figures.values()) - 100) > 1.5:
        return None

    week_ending = None
    week = _AAII_WEEK_RE.search(html)
    if week:
        month = _AAII_MONTHS.get(week.group(1).lower())
        if month:
            week_ending = f"{int(week.group(3)):04d}-{month:02d}-{int(week.group(2)):02d}"

    return {
        'bullish': figures['bull'],
        'neutral': figures['neut'],
        'bearish': figures['bear'],
        'week_ending': week_ending,
    }


def fetch_aaii_sentiment():
    """Fetch the latest AAII Sentiment Survey reading.

    The two failure paths log differently on purpose. A transport error or non-200
    means the request did not land. A 200 that parses to nothing means the page
    shape changed -- that is an upstream-redesign signal, and reporting it as a
    fetch problem is what let the NAAIM tile sit stale for weeks.
    """
    cfg = CONFIG["market_breadth"]
    try:
        req = urllib.request.Request(
            cfg["aaii_url"], headers={'User-Agent': cfg["aaii_user_agent"]}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode('utf-8', 'replace')
    except Exception as e:
        print(f"  Warning: Could not fetch AAII sentiment: {e}")
        return None

    reading = parse_aaii_sentiment(html)
    if reading is None:
        print("  Warning: AAII page fetched but no reading parsed -- the page "
              "shape likely changed (see the ssv2- hooks in parse_aaii_sentiment)")
        return None

    print(f"    AAII = {reading['bullish']}% bull / {reading['neutral']}% neut "
          f"/ {reading['bearish']}% bear (week ending {reading['week_ending']})")
    return reading


# Wrong-series defence, in two legs. If the symbol stops resolving and the
# endpoint serves some other well-formed daily series, every structural check
# here still passes -- these are what refuse it, the same job the range and sum
# checks do in parse_aaii_sentiment above.
#
# The identity leg is the load-bearing one. The response opens with a title line
# naming the symbol ("!NAAIM, Daily") above the column header, so the instrument
# IS in the body and checking it beats inferring identity from value shape.
#
# The range leg is a weak backstop and must not be relied on alone: -200..200
# spans the ordinary price of most listed equities and every 0-100 breadth
# percentage on this same dashboard, so it rejects almost nothing a quote
# service would plausibly serve.
NAAIM_SYMBOL = 'NAAIM'
NAAIM_RANGE = (-200.0, 200.0)

# Roughly eight surveys. Wide enough that the current run's start is always
# inside the window, narrow enough that a series which stopped moving upstream
# shows up as a change-free window rather than a plausible old reading.
NAAIM_WINDOW_DAYS = 60


def parse_naaim_series(text):
    """Parse the StockCharts daily CSV into ``[(ISO date, close), ...]``.

    Rows come back oldest-first, the order the endpoint sends. Returns ``None``
    for any malformed response rather than a partial series: a half-read series
    still yields a survey date, just a confidently wrong one.

    Never raises. This runs outside ``fetch_naaim_exposure``'s request try, and
    neither ``update_breadth_history`` nor ``export_all`` guards the chain, so
    an escaping exception would abort the whole daily export over one tile.
    """
    if not text:
        return None

    lines = [ln for ln in text.splitlines() if ln.strip()]

    # A title line carrying the symbol precedes the column header, and the
    # column count alone would not tell them apart.
    header_at = None
    for i, line in enumerate(lines):
        cols = [c.strip().lower() for c in line.split(',')]
        if 'date' in cols and 'close' in cols:
            header_at = i
            close_at = cols.index('close')
            break
    if header_at is None:
        return None

    # Identity leg: the symbol is right there above the header, so use it rather
    # than inferring the instrument from the shape of its values.
    preamble = ' '.join(lines[:header_at]).upper()
    if NAAIM_SYMBOL not in preamble:
        return None

    rows = []
    for line in lines[header_at + 1:]:
        cols = [c.strip() for c in line.split(',')]
        if len(cols) <= close_at:
            return None
        try:
            close = float(cols[close_at])
            stamp = datetime.strptime(cols[0], '%m/%d/%Y').date()
        except ValueError:
            return None
        # float() accepts "nan", "inf" and "-inf" without raising, so the
        # ValueError guard above does not catch them. A NaN close is worse than
        # a rejected one: NaN != anything is True, so the run-walk in
        # derive_naaim_reading would break at the NaN row and report a date
        # NEWER than the true run start -- erring fresh, which is the one
        # direction that function promises it never errs.
        # See docs/solutions/logic-errors/nan-defeats-numeric-guard-chains.md.
        if not math.isfinite(close):
            return None
        rows.append((stamp.isoformat(), close))

    if not rows:
        return None

    # Ordering leg. Every other malformed shape here fails closed; this one
    # would fail OPEN -- on a newest-first response `rows[-1]` is the oldest bar,
    # so the tile would publish a two-month-old reading that passes the header,
    # identity, range and change-free checks cleanly. The dates are already
    # parsed, so refusing costs one comparison. Refuse rather than sort: a
    # reordering means the endpoint's contract moved, and quietly sorting would
    # paper over that while the next shape change goes unnoticed.
    if any(rows[i][0] > rows[i + 1][0] for i in range(len(rows) - 1)):
        return None

    return rows


def derive_naaim_reading(rows):
    """Recover ``{'value', 'as_of'}`` from the daily series, or ``None``.

    The survey date is derived, never read off the feed. StockCharts copies one
    weekly reading onto every trading day and stamps each copy with that day's
    own date, so the newest bar's timestamp says "today" all week -- trusting it
    would rebuild the frozen-number failure that retired this tile in 2026-08.
    Walking back from the newest row while the close is unchanged finds the step
    the reading actually belongs to.

    Two documented behaviours, both deliberate:

    * A week that exactly repeats the previous week's reading is
      indistinguishable from a week that did not publish, so the walk spans both
      and the date reads one week old. That errs stale, never fresh, which is
      the safe direction for a staleness signal. Measured rate: 6 exact repeats
      in 1,053 weeks (0.57%).
    * A window with no step anywhere yields ``None``. The series has stopped
      moving upstream; dating the reading to the window's first day would invent
      a survey that never happened.
    """
    if not rows or len(rows) < 2:
        return None

    value = rows[-1][1]
    if not NAAIM_RANGE[0] <= value <= NAAIM_RANGE[1]:
        return None

    as_of = None
    for stamp, close in reversed(rows):
        if close != value:
            break
        as_of = stamp
    else:
        # Fell off the oldest row without finding a change: no step in window.
        return None

    return {'value': value, 'as_of': as_of}


def fetch_naaim_exposure():
    """Fetch the latest NAAIM Exposure Index reading, or ``None``.

    One request to the daily-history endpoint rather than the quote endpoint.
    The quote endpoint returns the value in cleaner JSON but its ``time`` field
    is the forward-filled bar date, so it cannot answer "which survey is this?"
    at all; the history endpoint answers both in one call.

    Three failure modes, logged apart on purpose -- collapsing them is what let
    the previous NAAIM breakage read as an ordinary quiet week for weeks.
    """
    cfg = CONFIG["market_breadth"]

    # The URL build is inside the try, not above it. A missing `naaim_url` key
    # or a stray brace in the template raises from `.format()`, and this
    # function is called unguarded -- neither update_breadth_history nor
    # export_all wraps it -- so that exception would abort the whole daily
    # export, losing the ETF, radar, VARS and volume tabs over one tile.
    try:
        start = (date.today() - timedelta(days=NAAIM_WINDOW_DAYS)).isoformat()
        url = cfg["naaim_url"].format(
            start=start, cachebust=int(datetime.now().timestamp() * 1000)
        )
        req = urllib.request.Request(
            url, headers={'User-Agent': cfg["user_agent"]}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode('utf-8', 'replace')
    except Exception as e:
        # A wrong User-Agent returns 404 here, not 403, so a blocked request
        # looks exactly like a delisted symbol. Name it so the next reader
        # checks the header before hunting for a renamed ticker.
        print(f"  Warning: Could not fetch NAAIM exposure: {e} "
              f"(a 404 here may be the User-Agent, not the symbol)")
        return None

    rows = parse_naaim_series(body)
    if rows is None:
        print("  Warning: NAAIM endpoint fetched but no series parsed -- the "
              "response shape likely changed (see parse_naaim_series)")
        return None

    reading = derive_naaim_reading(rows)
    if reading is None:
        print("  Warning: NAAIM series parsed but no reading derived -- the "
              "value has not moved in the whole window, or is outside the "
              "plausible exposure range")
        return None

    print(f"    NAAIM = {reading['value']}% (survey {reading['as_of']})")
    return reading


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


DAY_PATTERN_COLUMNS = ('tight_day', 'inside_day', 'close_to_ma')


def _flag_on(value):
    """True only for a real True. `bool(float('nan'))` is True, so NaN and
    other missing readings must be tested before the truth test."""
    import pandas as pd
    return bool(pd.notna(value)) and bool(value)


def day_pattern_green(bar):
    """The day-pattern rule for one bar: (tight_day OR inside_day) AND close_to_ma.

    A missing column reads as False, so a session without the indicators
    colours nothing rather than everything.
    """
    return ((_flag_on(bar.get('tight_day')) or _flag_on(bar.get('inside_day')))
            and _flag_on(bar.get('close_to_ma')))


def day_flags_from_master(master_df):
    """Return {ticker: 'green'} for the master rows that meet the day-pattern rule.

    Only green is emitted; tickers that fail are omitted (no blue tier).
    """
    flags = {}
    if master_df is None or master_df.empty or 'ticker' not in master_df.columns:
        return flags
    cols = ['ticker'] + [c for c in DAY_PATTERN_COLUMNS if c in master_df.columns]
    for bar in master_df[cols].to_dict('records'):
        if day_pattern_green(bar):
            flags[str(bar['ticker']).upper()] = 'green'
    return flags


def load_day_flags_by_date(root=None):
    """Return {session_date: {ticker: 'green'}} from each per-day master parquet.

    Each session reads its OWN bar. The flags were once read from the newest
    bar of `price_daily_ta.pkl` and handed to every session in the time-travel
    window, so a back-dated session showed today's colouring. For the newest
    session the master row is the ticker's last bar (`create_master_table`
    slices `[:run_date].tail(1)`), so that session's result is unchanged.

    Only the ticker and the three flag columns are read: this runs once per
    retained session (~124), and the radar export reads each file in full
    anyway. A date with no master file is absent from the result, so its
    exporter colours nothing for that session.
    """
    import pandas as pd
    import pyarrow.parquet as pq

    root = Path(root) if root is not None else SCREENING_OUTPUT_DIR
    files = sorted((root / 'master').glob('master_*.parquet'))
    dates = [f.stem.replace('master_', '') for f in files]
    cutoff = _history_cutoff(dates)

    by_date = {}
    for path, date_str in zip(files, dates):
        if cutoff is not None and date_str < cutoff:
            continue
        present = set(pq.read_schema(path).names)
        if 'ticker' not in present:
            by_date[date_str] = {}
            continue
        cols = ['ticker'] + [c for c in DAY_PATTERN_COLUMNS if c in present]
        master_df = pd.read_parquet(path, engine='pyarrow', columns=cols)
        by_date[date_str] = day_flags_from_master(master_df)
    return by_date


def enrich_with_ticker_color(data_list, flags, ticker_key='ticker'):
    """Add ticker_color ('green') to a list of ticker dicts."""
    count = 0
    for item in data_list:
        tk = item.get(ticker_key, '')
        if tk in flags:
            item['ticker_color'] = flags[tk]
            count += 1
    return count


def enrich_etf_with_metrics(data_list, metrics, ticker_key='ticker'):
    """Attach vars, ticker_color and the highlight tier to ETF row dicts.

    Returns (vars_count, color_count). The tier rides along beside the other
    two; `fetch_etf_metrics` prints its own count.
    """
    vars_count = 0
    color_count = 0
    for item in data_list:
        tk = item.get(ticker_key, '')
        m = metrics.get(tk)
        if not m:
            continue
        if m.get('vars') is not None:
            item['vars'] = m['vars']
            vars_count += 1
        if m.get('color'):
            item['ticker_color'] = m['color']
            color_count += 1
        if m.get('highlight'):
            item['highlight'] = m['highlight']
    return vars_count, color_count


def fetch_etf_metrics(tickers, spy_cum_norm_100):
    """Download recent OHLC for ETF tickers and compute VARS, colour and tier.

    Standalone fetch that does NOT pollute the main price data pipeline.
    Returns ``{ticker: {'vars': float|None, 'color': 'green'|None,
    'highlight': str|None, 'zone': dict}}``. `zone` holds the ETF Viz inputs
    from the last bar: `close`, `ema20`, `ema20_w`, `atr14`, `dollar_vol` and
    `adr_pct` (None where a value is missing). Only the moving-average rungs can reach the tier
    here: an ETF carries neither short interest nor a tight-base column.
    VARS uses the same formula as :mod:`create_technical_indicators` so values
    are comparable to the stock VARS leaderboard; the SPY baseline series is
    passed in by the caller (sourced from price_daily_ta.pkl's SPY).
    """
    if not tickers:
        return {}
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        print("   Warning: yfinance not installed, skipping ETF metrics")
        return {}

    # Two years, for the weekly EMA20 of the ETF Viz tab. That EMA seeds on its
    # first week: over 8 months (~34 weeks) the seed keeps ~3% weight, which
    # moves a 2x ETF's weekly distance visibly; over ~100 weeks it keeps less
    # than 0.01%, as on the pipeline's 500-day history. VARS, ATR14, the EMAs,
    # the SMA50 and ADR read only recent windows, so the longer fetch leaves
    # them unchanged.
    unique_tickers = list(set(tickers))
    print(f"   Fetching OHLC for {len(unique_tickers)} ETF tickers from Yahoo Finance...")
    try:
        data = yf.download(unique_tickers, period='2y', progress=False, group_by='ticker')
    except Exception as e:
        print(f"   Warning: yfinance download failed: {e}")
        return {}

    metrics = {}
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

            # Shared with the indicator pipeline, so an ETF's tier means what a
            # stock's does. The SMA50 here is the full-window one the ladder
            # needs, not the pipeline's 25-bar `sma50` column.
            ema10, ema20 = compute_ema_pair(close)
            sma50 = compute_sma50_full(close)

            atr14 = compute_atr14(high, low, close)

            # VARS — gated on ≥50 bars so recent IPOs don't produce garbage.
            vars_value = None
            if len(df) >= 50:
                norm_change = (close - close.shift(1)) / atr14
                cum_norm_100 = norm_change.rolling(window=100, min_periods=1).sum()
                spy_aligned = spy_cum_norm_100.reindex(df.index)
                vars_series = cum_norm_100 - spy_aligned
                last_vars = vars_series.iloc[-1]
                if pd.notna(last_vars):
                    vars_value = float(last_vars)

            # Day-pattern green flag (tight/inside-day on the EMA10/20).
            color = None
            last = df.iloc[-1]
            last_close = float(last['Close'])
            last_open = float(last['Open'])
            # Same helper the indicator pipeline uses, so the two definitions
            # cannot drift. yfinance hands back capitalized OHLC columns.
            inside = bool(compute_inside_day(
                df['Open'], df['High'], df['Low'], df['Close']).iloc[-1])
            tight = abs(last_close - last_open) / last_close < 0.2 * float(adr_pct.iloc[-1])
            if inside or tight:
                close_to_ema10 = abs(last_close - float(ema10.iloc[-1])) < 0.5 * float(atr14.iloc[-1])
                close_to_ema20 = abs(last_close - float(ema20.iloc[-1])) < 0.5 * float(atr14.iloc[-1])
                if close_to_ema10 or close_to_ema20:
                    color = 'green'

            # Highlight tier. An ETF has no short interest and no tight base
            # here, so only the two moving-average rungs can fire. Below the
            # SMA50 minimum period the average is NaN, which the tier function
            # reads as absent and answers None.
            highlight = compute_highlight_tier(
                ema10=ema10.iloc[-1],
                ema20=ema20.iloc[-1],
                sma50=sma50.iloc[-1],
            )

            # Zone-map inputs for the ETF Viz tab. Dollar volume uses the
            # pipeline's `avg_dollar_vol` definition; a frame with no volume
            # has none, and the zone map's floor then drops the ETF.
            dollar_vol = None
            if 'Volume' in df:
                dv = (close * pd.to_numeric(df['Volume'])).rolling(
                    window=20, min_periods=10).mean().iloc[-1]
                dollar_vol = _finite(dv)
            zone = {
                'close': _finite(last_close),
                'ema20': _finite(ema20.iloc[-1]),
                'ema20_w': _finite(compute_weekly_ema(close, **weekly_ema_config()).iloc[-1]),
                'atr14': _finite(atr14.iloc[-1]),
                'dollar_vol': dollar_vol,
                'adr_pct': _finite(adr_pct.iloc[-1]),
            }

            # A row carrying only a tier, or only zone inputs, still reaches
            # the payload — the skip asks whether this ETF earned any metric
            # at all.
            if (vars_value is None and color is None and highlight is None
                    and zone['close'] is None):
                continue
            metrics[ticker] = {
                'vars': vars_value, 'color': color, 'highlight': highlight,
                'zone': zone,
            }
        except Exception as e:
            print(f"     Error processing {ticker}: {e}")
            continue

    vars_n = sum(1 for m in metrics.values() if m.get('vars') is not None)
    color_n = sum(1 for m in metrics.values() if m.get('color'))
    tier_n = sum(1 for m in metrics.values() if m.get('highlight'))
    print(
        f"   Computed VARS for {vars_n} ETFs; {color_n} have tight/inside-day "
        f"color; {tier_n} carry a highlight tier"
    )
    return metrics


def load_spy_cum_norm_100():
    """Load SPY from price_daily_ta.pkl and compute the VARS baseline series."""
    try:
        daily_price = su.load_object_from_pickle(PRICE_DATA_TA_FILE)
    except Exception as e:
        print(f"   Warning: Could not load price data for SPY VARS baseline: {e}")
        return None
    spy = daily_price.get('SPY')
    if spy is None or spy.empty:
        print("   Warning: SPY not found in price_daily_ta.pkl; ETF VARS will be skipped")
        return None
    return compute_spy_cum_norm_100(spy)


THEMES_HISTORY_DAYS = 180  # Time-travel retention window in calendar days.
# Every session whose date falls within the last 180 calendar days is retained
# and offered in the session bar. (Generating the back-dated source parquet
# that far back is driven by the `--days` argument in run_daily_workflow.py.)


def _history_cutoff(date_strings):
    """Inclusive lower-bound ``YYYY-MM-DD`` for the time-travel retention window.

    Anchored to the newest date present (not wall-clock today) so the window is
    reproducible and robust to the export running on a stale/holiday day.
    Returns ``None`` when there are no valid dates. Callers keep entries whose
    date is ``>=`` the returned cutoff.
    """
    valid = [d for d in date_strings if d]
    if not valid:
        return None
    anchor = date.fromisoformat(max(valid))
    return (anchor - timedelta(days=THEMES_HISTORY_DAYS)).isoformat()


def _update_history_file(history_file, report_date, entry):
    """Append an entry to a history JSON file, keeping last N sessions.

    Each entry in the history array must have a 'report_date' key.
    """
    history = []
    if history_file.exists():
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except (json.JSONDecodeError, ValueError) as e:
            # These files grow forward only and cannot be rebuilt (SI, ETF
            # Viz, the ETF tabs). Replacing an unreadable one with a one-entry
            # file would publish the loss, so leave it untouched for repair.
            print(f"   !! WARNING: {history_file} is unreadable ({e}); NOT updated. "
                  "Restore it from git history.")
            return

    # Replace existing entry for same date, or append
    history = [h for h in history if h.get('report_date') != report_date]
    history.append(entry)

    # Sort descending by date, keep only sessions within the retention window
    history.sort(key=lambda x: x.get('report_date', ''), reverse=True)
    cutoff = _history_cutoff([h.get('report_date', '') for h in history])
    if cutoff is not None:
        history = [h for h in history if h.get('report_date', '') >= cutoff]

    # Write-then-rename, so an interrupted run cannot leave a truncated file.
    tmp = history_file.with_name(history_file.name + '.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=2)
    tmp.replace(history_file)
    dates = [h['report_date'] for h in history]
    span = f"{len(dates)} sessions, {dates[-1]} -> {dates[0]}" if dates else "empty"
    print(f"   -> {history_file} (history: {span})")


def _export_session_history(sessions, build, out_dir, name, describe,
                            history_view=None, compact_history=False):
    """Write one tab's current snapshot and rebuild its whole history file.

    `sessions` is ``[(date_str, source), ...]`` in any order. Sessions older
    than the retention window are skipped; the rest are built newest first by
    ``build(date_str, source, newest_session)``, and a None snapshot is dropped.

    ⛔ The first snapshot to land becomes `<name>.json`, the one the tab renders
    as current, so it is the only one built with ``newest_session=True`` — the
    only one allowed today's short interest (see `_highlight_from_row`). This
    is the one place that rule lives.

    Writes `<name>.json` and `<name>_history.json` (every snapshot, newest
    first, passed through `history_view` when given) from scratch, so a re-run
    that regenerated back-dated parquet republishes every session. Returns the
    current snapshot, or None when nothing was built.
    """
    sessions = sorted(sessions, key=lambda s: s[0], reverse=True)
    cutoff = _history_cutoff([date_str for date_str, _ in sessions])

    history = []
    for date_str, source in sessions:
        if cutoff is not None and date_str < cutoff:
            continue
        snap = build(date_str, source, not history)
        if snap is not None:
            history.append(snap)

    if not history:
        print(f"   Every {name} session was empty, skipping {name} export")
        return None

    current = history[0]
    out = out_dir / f"{name}.json"
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump(current, fh, indent=2)
    print(f"   -> {out} ({describe(current)}, date {current['report_date']})")

    written = history_view(history) if history_view else history
    history_out = out_dir / f"{name}_history.json"
    with open(history_out, 'w', encoding='utf-8') as fh:
        if compact_history:
            json.dump(written, fh, separators=(',', ':'))
        else:
            json.dump(written, fh, indent=2)
    dates = [h['report_date'] for h in written]
    print(f"   -> {history_out} (history: {len(written)} sessions, {dates[-1]} -> {dates[0]})")
    return current


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


def filter_metrics(row):
    """Return ``(dollar_vol, adr_pct)`` for the dashboard's V/A cutoff filters.

    `dollar_vol` is `avg_dollar_vol` (20-day average dollar volume) as a whole
    number and `adr_pct` is the fractional 20-day ADR (0.04 == 4%). Both are read
    from the same per-day row as the rest of the payload, so a time-travel session
    filters against *its own* values rather than today's.

    The V lens reads dollar volume, not `vol_sma50`. It measured share volume
    while the control was a fixed 1M-share toggle; as a user-chosen cutoff it
    reads the same column every screener and the radar universe floor already
    gate on, because 1M shares means something different at $4 than at $400.

    A missing value returns None and the filter treats the ticker as passing
    (fail-open, matching `build_radar_universe`'s NaN handling). The snapshot
    builders call ``.fillna(0)`` on their frames, so a genuinely absent metric
    arrives as 0.0 rather than NaN — zero is therefore also mapped to None, or
    every such ticker would be dimmed as if it were illiquid. That matters more
    now than it did under the toggles: the filters are always on, so a metric
    reading as 0 would dim the ticker on first paint with no user action to undo.
    """
    def _positive_or_none(value, caster):
        cast = caster(value)
        return cast if cast else None

    return (
        _positive_or_none(row.get('avg_dollar_vol'), _int_or_none),
        _positive_or_none(row.get('adr_pct'), lambda v: _round_or_none(v, 4)),
    )


#: The only columns `_highlight_from_row` reads. A caller that needs nothing
#: else passes this to `_bars_by_ticker` so a per-session row dict stays narrow.
#: `sma50_full`, not `sma50` — see `_highlight_from_row`.
HIGHLIGHT_ROW_COLUMNS = ('tight_range', 'ema10', 'ema20', 'sma50_full')


def _bars_by_ticker(master_df, columns=None):
    """Index a master frame by upper-case ticker, as ``{ticker: row dict}``.

    `columns` narrows the row dicts to the fields the caller reads. An absent
    column is simply dropped rather than raising, so a back-dated parquet that
    predates an indicator answers "missing" for it — which every consumer here
    already treats as absent. `ticker` is always kept.
    """
    if columns is None:
        frame = master_df
    else:
        keep = ['ticker'] + [c for c in columns if c in master_df.columns]
        frame = master_df[keep]
    return frame.set_index(
        master_df['ticker'].astype(str).str.upper()
    ).to_dict('index')


def _highlight_from_row(row, short_interest=None):
    """Return the one highlight tier a payload row earns, or None.

    Reads `tight_range`, `ema10`, `ema20` and `sma50` off the session's own
    master bar and hands them to `compute_highlight_tier` raw. That function
    already maps missing, zero and non-finite inputs to "absent", so a second
    copy of that rule here could only drift from it. Some builders load their
    frame with `.fillna(0)` and `_build_radar_snapshot` deliberately does not,
    which is exactly why the reading belongs in one place.

    ⛔ **Pass `short_interest` only for the session a tab renders as current.**
    Finviz publishes today's figure with no per-session history, so handing it
    to an older session pins today's crowding onto an old price bar. The short
    rung outranks the tight and moving-average rungs, which *are* computed from
    that session's own parquet, so today's number would erase that session's
    real signal across the whole retention window. CLAUDE.md's SI section calls
    that shape "a fabricated number, not a stale one". A historical session
    therefore renders only the rungs its own parquet supports.
    """
    return compute_highlight_tier(
        tight_range=row.get('tight_range'),
        short_interest=short_interest,
        ema10=row.get('ema10'),
        ema20=row.get('ema20'),
        # `sma50_full`, never `sma50`. The pipeline's `sma50` settles for 25 bars,
        # so a young listing carries a 30-bar mean under a 50-day name — finite
        # and positive, which is the one shape the zero-as-missing rule cannot
        # catch. A parquet predating the column answers None and skips both
        # moving-average rungs: the fail-closed direction, and it heals on the
        # next workflow run, since every run rebuilds 130 sessions.
        sma50=row.get('sma50_full'),
    )


# ── Zone map (the five Viz tabs) ────────────────────────────────────────────
#
# Each ticker sits at (distance from the daily EMA20, distance from the weekly
# EMA20), both in daily ATR14. The server decides every zone and state; the
# browser only draws (`renderZoneMap` in docs/app.js, whose ZONE_RULES mirror
# is pinned to the `zone_map:` config block).
#
# ⛔ The distances read the session's MASTER parquet, unfilled. The screener
# parquets are saved from a frame that run_screener.py loads with
# `.fillna(0)`, so a screener row holds `ema20_w = 0` where the master holds
# NaN. A zero-filled weekly EMA gives `close / atr14`, a large positive
# number, and the name plots far up the extended zone with nothing on screen
# to show it.

ZONE_MAP_DEFAULTS = {
    'min_avg_dollar_vol': 100_000_000,
    'min_adr_pct': 0.04,
    'top_leaves': 20,
    'band_low': -0.5,
    'band_high': 2.0,
    'weekly_low': 0.0,
    'weekly_high': 4.0,
    'x_range': [-2.0, 5.0],
    'y_range': [-4.0, 8.0],
}

#: The seven states, best first. docs/app.js ZONE_STATES lists the same ids in
#: the same order (pinned by tests/test_dashboard_zone_map_markup.py).
ZONE_STATES = ('buy_area', 'buy_stretched', 'above', 'extended',
               'under_daily', 'weekly_broken', 'under_both')

#: Every (daily, weekly) cell maps to exactly one state.
_ZONE_STATE_BY_CELL = {
    ('band', 'normal'): 'buy_area',
    ('band', 'extended'): 'buy_stretched',
    ('above', 'normal'): 'above',
    ('above', 'extended'): 'extended',
    ('under', 'normal'): 'under_daily',
    ('under', 'extended'): 'under_daily',
    ('band', 'below'): 'weekly_broken',
    ('above', 'below'): 'weekly_broken',
    ('under', 'below'): 'under_both',
}

ZONE_MASTER_COLUMNS = ('close', 'ema20', 'ema20_w', 'atr14', 'tight_range', 'tight_len')


def zone_map_config():
    """The `zone_map:` config block, with the defaults filling any gap."""
    cfg = CONFIG.get('zone_map') or {}
    return {key: cfg.get(key, default) for key, default in ZONE_MAP_DEFAULTS.items()}


def zone_distance(close, ma, atr):
    """``(close - ma) / atr`` to 2 decimals, or None.

    A missing, non-finite or non-positive input returns None. A zero ATR
    must not become an infinite distance, and a zero average is a filled
    absence, not a price.
    """
    close, ma, atr = _finite(close), _finite(ma), _finite(atr)
    if close is None or ma is None or atr is None:
        return None
    if close <= 0 or ma <= 0 or atr <= 0:
        return None
    return round((close - ma) / atr, 2)


def classify_zone(d_daily, d_weekly, cfg):
    """Return ``(zone_daily, zone_weekly, state)``. All four limits are inclusive."""
    if d_daily < cfg['band_low']:
        daily = 'under'
    elif d_daily > cfg['band_high']:
        daily = 'above'
    else:
        daily = 'band'
    if d_weekly < cfg['weekly_low']:
        weekly = 'below'
    elif d_weekly > cfg['weekly_high']:
        weekly = 'extended'
    else:
        weekly = 'normal'
    return daily, weekly, _ZONE_STATE_BY_CELL[(daily, weekly)]


def _load_zone_master(date_str):
    """The session's master parquet, UNFILLED, or None when it is absent.

    Only `ticker` and ZONE_MASTER_COLUMNS are read, the same narrow read as
    `load_day_flags_by_date`: three exporters call this once per retained
    session. A file that predates `ema20_w` returns None without a read.
    """
    import pandas as pd
    import pyarrow.parquet as pq

    if not date_str:
        return None
    path = SCREENING_OUTPUT_DIR / 'master' / f'master_{date_str}.parquet'
    if not path.exists():
        return None
    present = set(pq.read_schema(path).names)
    if 'ticker' not in present or 'ema20_w' not in present:
        return None
    cols = ['ticker'] + [c for c in ZONE_MASTER_COLUMNS if c in present]
    return pd.read_parquet(path, engine='pyarrow', columns=cols)


def build_zone_map(rows, master_df, leaves_by_ticker, cfg=None):
    """Build one tab's `zone_map` block from its ticker rows.

    `rows` are the tab's payload ticker dicts. The floors read their
    `dollar_vol` and `adr_pct`; the display reads `vars`, `rs`,
    `highlight` and `ticker_color`. The distances and the tight flag read
    `master_df`, which must be the session's unfilled master frame.

    A missing liquidity metric FAILS the floor. The floor is a universe gate,
    like `si_gate`, not a view lens like the V/A cutoffs, so an unknown must
    not pass it.

    Returns None when the frame is absent or predates `ema20_w`: an empty map
    would read as a quiet market. Every name left off is counted.
    """
    cfg = cfg or zone_map_config()
    if master_df is None or 'ema20_w' not in master_df.columns:
        return None
    rows = list(rows)
    wanted = {str(row.get('ticker') or '').upper() for row in rows}
    in_rows = master_df['ticker'].astype(str).str.upper().isin(wanted)
    bars = _bars_by_ticker(master_df[in_rows], ZONE_MASTER_COLUMNS)
    counts = {'n_candidates': 0, 'n_floor_dropped': 0, 'n_no_weekly': 0, 'n_no_daily': 0}
    seen = set()
    out = []
    for row in rows:
        ticker = str(row.get('ticker') or '').upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        counts['n_candidates'] += 1
        dollar_vol = _finite(row.get('dollar_vol'))
        adr = _finite(row.get('adr_pct'))
        if (dollar_vol is None or adr is None
                or dollar_vol < cfg['min_avg_dollar_vol'] or adr < cfg['min_adr_pct']):
            counts['n_floor_dropped'] += 1
            continue
        bar = bars.get(ticker) or {}
        if _finite(bar.get('ema20_w')) is None:
            counts['n_no_weekly'] += 1
            continue
        d_daily = zone_distance(bar.get('close'), bar.get('ema20'), bar.get('atr14'))
        d_weekly = zone_distance(bar.get('close'), bar.get('ema20_w'), bar.get('atr14'))
        if d_daily is None or d_weekly is None:
            counts['n_no_daily'] += 1
            continue
        zone_daily, zone_weekly, state = classify_zone(d_daily, d_weekly, cfg)
        tight = _flag_on(bar.get('tight_range'))
        out.append({
            'ticker': ticker,
            'd_daily': d_daily,
            'd_weekly': d_weekly,
            'zone_daily': zone_daily,
            'zone_weekly': zone_weekly,
            'state': state,
            'vars': _round_or_none(row.get('vars'), 2),
            'rs': _round_or_none(row.get('rs'), 1),
            'tight': tight,
            'tight_len': _int_or_none(bar.get('tight_len')) if tight else None,
            'highlight': row.get('highlight'),
            'ticker_color': row.get('ticker_color'),
            'leaves': [p for p in (leaves_by_ticker.get(ticker) or [])
                       if p not in CATCHALL_THEMES],
        })
    out.sort(key=lambda t: t['ticker'])
    params = {key: cfg[key] for key in ZONE_MAP_DEFAULTS}
    return {'params': params, **counts, 'tickers': out}


# ── ETF Viz (the long leveraged ETFs on the zone map) ──────────────────────
#
# config/etf_themes.yaml gives each Lev ETF sheet ticker a side and a theme.
# Only long ETFs plot. A sheet ticker with no entry does not plot, because its
# side is unknown and it could be inverse; the snapshot lists it instead.

ETF_THEMES_FILE = PROJECT_ROOT / 'config' / 'etf_themes.yaml'
ETF_SIDES = ('long', 'short', 'exclude')


def load_etf_theme_map(path=ETF_THEMES_FILE):
    """``{TICKER: entry}`` from the ETF theme map, or ``{}`` when it is absent."""
    import yaml

    path = Path(path)
    if not path.exists():
        return {}
    with open(path, encoding='utf-8') as f:
        data = yaml.safe_load(f) or {}
    return {str(ticker).upper(): dict(entry or {}) for ticker, entry in data.items()}


def etf_leaves(entry, ticker_themes):
    """The theme paths of one ETF entry, or None when it has none to give.

    Explicit `themes` win, and an empty list is a real answer (plot with no
    cluster). Otherwise the underlying's tags are read live, so a retag of
    the stock moves the ETF with no edit to the map.
    """
    if 'themes' in entry:
        return list(entry['themes'] or [])
    underlying = str(entry.get('underlying') or '').upper()
    tags = ticker_themes.get(underlying) if underlying else None
    return list(tags) if tags else None


def etf_viz_config():
    """`zone_map_config()` with the ETF Viz dollar floor in place of the shared one.

    Zone limits and the ADR floor stay shared. At the shared $100M floor only
    15 of 47 long ETFs pass (measured 2026-10-20).
    """
    cfg = zone_map_config()
    etf_cfg = CONFIG.get('etf_viz') or {}
    cfg['min_avg_dollar_vol'] = etf_cfg.get('min_avg_dollar_vol', 20_000_000)
    return cfg


def build_etf_viz_snapshot(etf_rows, metrics, theme_map, ticker_themes, report_date, cfg=None):
    """Build the ETF Viz payload, ``{'report_date', 'zone_map'}``, or None.

    `etf_rows` are the Lev ETF sheet rows; `metrics` is `fetch_etf_metrics`
    output, whose `zone` dicts supply the distances. Only long ETFs reach
    `build_zone_map`. Short and excluded ETFs are counted; a ticker with no
    map entry, or a long one with no theme source, is listed in `unmapped`.
    A long ETF the fetch returned no bars for counts in `n_no_bars`, never
    as under the floors: a failed download is not an illiquid fund.

    Returns None when no sheet row has bars (a failed fetch): an empty map
    would read as a quiet market, so the caller keeps the last snapshot. The
    test reads the sheet rows only, because one fetch also serves the
    Industry tab, and Industry bars must not stand in for Lev bars.
    """
    import pandas as pd

    metrics = metrics or {}
    sheet = {str(row.get('ticker') or '').upper() for row in etf_rows}
    bars = [{'ticker': t.upper(), **{k: m['zone'].get(k) for k in ('close', 'ema20', 'ema20_w', 'atr14')}}
            for t, m in metrics.items() if t.upper() in sheet and m.get('zone')]
    if not bars:
        return None
    frame = pd.DataFrame(bars).astype({'ema20_w': float})

    rows, leaves_by_ticker, unmapped = [], {}, []
    n_short = n_excluded = n_no_bars = 0
    for row in etf_rows:
        ticker = str(row.get('ticker') or '').upper()
        if not ticker:
            continue
        entry = theme_map.get(ticker)
        side = entry.get('side') if entry else None
        if side == 'short':
            n_short += 1
            continue
        if side == 'exclude':
            n_excluded += 1
            continue
        leaves = etf_leaves(entry, ticker_themes) if side == 'long' else None
        if leaves is None:
            unmapped.append(ticker)
            continue
        m = metrics.get(row.get('ticker')) or metrics.get(ticker) or {}
        zone = m.get('zone')
        if not zone:
            n_no_bars += 1
            continue
        leaves_by_ticker[ticker] = leaves
        rows.append({
            'ticker': ticker,
            'vars': m.get('vars'),
            'rs': row.get('rs_sts'),
            'highlight': m.get('highlight'),
            'ticker_color': m.get('color'),
            'dollar_vol': zone.get('dollar_vol'),
            'adr_pct': zone.get('adr_pct'),
        })

    zone_map = build_zone_map(rows, frame, leaves_by_ticker, cfg or etf_viz_config())
    if zone_map is None:
        return None
    unmapped = sorted(set(unmapped))
    if unmapped:
        print(f"   Warning: no theme in config/etf_themes.yaml for: {', '.join(unmapped)}")
    zone_map.update({'n_no_bars': n_no_bars, 'n_short': n_short,
                     'n_excluded': n_excluded, 'unmapped': unmapped})
    return {'report_date': report_date, 'zone_map': zone_map}


def export_etf_viz(etf_data, etf_metrics, report_date):
    """Write etf_viz.json and append etf_viz_history.json.

    No bars writes nothing, so the tab keeps the last snapshot under its own
    date. The tab is not critical: an error is logged and the rest of
    `export_all` (macro data, report meta, the prune) still runs.
    """
    try:
        from src.themes.theme_registry import load_ticker_themes
        etf_viz = build_etf_viz_snapshot(etf_data, etf_metrics, load_etf_theme_map(),
                                         load_ticker_themes(), report_date)
        if etf_viz is None:
            print("   Warning: no Lev ETF bars; keeping the last etf_viz.json")
            return
        with open(OUTPUT_DIR / "etf_viz.json", 'w', encoding='utf-8') as f:
            json.dump(etf_viz, f, indent=2)
        print(f"   -> {OUTPUT_DIR / 'etf_viz.json'} "
              f"({len(etf_viz['zone_map']['tickers'])} ETFs on the map)")
        # Forward only, like etf_data_history.json: the roster and the theme
        # map are today's, so a rebuild would pin them on old bars.
        _update_history_file(OUTPUT_DIR / "etf_viz_history.json", report_date, etf_viz)
    except Exception as e:
        print(f"   Warning: ETF Viz export failed: {e}")


# ── Screener-backed tabs (Momentum, VARS, Volume) ───────────────────────────
#
# The three builders share the per-ticker row tail, the theme bucketing and the
# fundamentals join; each adds its own leading fields and its own grouping.


def _screener_theme_buckets(tickers):
    """``(ticker_themes, theme_to_tickers)`` for one screener's tickers.

    An untagged ticker falls back to Uncategorized so it still appears. The
    full tag map comes back too, because the zone map reads it for its cluster
    rows. The import stays function-local: tests patch
    `src.themes.theme_registry.load_ticker_themes`.
    """
    from src.themes.theme_registry import load_ticker_themes
    from src.themes.theme_taxonomy import build_theme_to_tickers

    ticker_themes = load_ticker_themes()
    filtered_map = {t: ticker_themes.get(t) or ['Uncategorized'] for t in tickers}
    return ticker_themes, build_theme_to_tickers(filtered_map)


def _vars_fields(row):
    """The two VARS columns the VARS and Volume rows lead with."""
    return {
        'vars': round(float(row.get('vars', 0) or 0), 2),
        'vars_20ema': round(float(row.get('vars_20ema', 0) or 0), 2),
    }


def _screener_ticker_rows(df, fundamentals, newest_session, lead=None):
    """``{TICKER: payload row}`` for one zero-filled screener frame.

    `lead(row, ticker)` returns the tab's own fields, which sit between
    `ticker` and the shared tail. Key order is the JSON's field order, so
    the tail is spelled out once here. `newest_session` gates the highlight
    ladder's short rung — see `_highlight_from_row`.
    """
    per_ticker = {}
    for _, row in df.iterrows():
        t = str(row['ticker']).upper()
        f = fundamentals.get(t, {})
        short_val = f.get('short_interest')
        dollar_vol, adr_pct = filter_metrics(row)
        per_ticker[t] = {
            'ticker': t,
            **(lead(row, t) if lead else {}),
            'rs': round(float(row.get('rs_sts_pct', 0) or 0), 1),
            'price': round(float(row.get('close', 0) or 0), 2),
            'dollar_vol': dollar_vol,
            'adr_pct': adr_pct,
            'float': _fmt_float_m(f.get('shares_float')),
            'eps': _fmt_growth(f.get('eps_growth_qq')),
            'sales': _fmt_growth(f.get('sales_growth_qq')),
            'inst': _fmt_inst(f.get('inst_transactions')),
            'short': round(float(short_val), 1) if short_val is not None else None,
            'highlight': _highlight_from_row(
                row, short_val if newest_session else None),
        }
    return per_ticker


def _theme_tables(theme_to_tickers, per_ticker, sort_field, skip=()):
    """Yield ``(theme, rows)`` per non-empty theme, rows sorted by `sort_field` desc."""
    for theme_name, tickers in theme_to_tickers.items():
        if theme_name in skip:
            continue
        rows = [per_ticker[t] for t in tickers if t in per_ticker]
        if not rows:
            continue
        rows.sort(key=lambda x: -x[sort_field])
        yield theme_name, rows


def _avg_vars_table(name, rows):
    return {
        'name': name,
        'avg_vars': round(sum(td['vars'] for td in rows) / len(rows), 2),
        'tickers': rows,
    }


def _color_tables(tables, day_flags):
    """Apply the session's day-pattern flags to every table's ticker rows."""
    if day_flags:
        for table in tables:
            enrich_with_ticker_color(table['tickers'], day_flags)


def _build_momentum_136_snapshot(path, day_flags, newest_session=False, fundamentals=None):
    """Build a single momentum_136 snapshot dict from one screener parquet.

    Returns the JSON-serializable {'report_date', 'themes', 'zone_map'}
    structure, or None if the parquet is empty.

    `newest_session` says whether this snapshot is the one the tab renders as
    current. Only then does short interest reach the highlight ladder — see
    `_highlight_from_row`. It defaults to False so a new history loop that
    forgets to pass it drops a rung rather than fabricating one.

    `fundamentals` is the table an exporter loads once per run; None looks this
    session's tickers up in `fundamentals.db` (a missing file reads as empty).
    """
    df = su.load_df_from_parquet(path).fillna(0)
    if df.empty:
        return None

    session_date = str(df['date'].iloc[0]) if 'date' in df.columns else ''
    screener_tickers = df['ticker'].astype(str).str.upper().tolist()
    ticker_themes, theme_to_tickers = _screener_theme_buckets(screener_tickers)
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers(screener_tickers)
    per_ticker = _screener_ticker_rows(df, fundamentals, newest_session)

    # Tickers within a theme by RS desc; themes by count desc.
    themes_list = [{'name': name, 'tickers': rows}
                   for name, rows in _theme_tables(theme_to_tickers, per_ticker, 'rs')]
    # Catch-all buckets sort to the bottom regardless of count — they're
    # overflow, not thematic concentration.
    themes_list.sort(key=lambda th: (
        th['name'] in CATCHALL_THEMES,
        -len(th['tickers']),
        th['name'],
    ))
    _color_tables(themes_list, day_flags)
    _attach_hierarchy(themes_list)

    return {
        'report_date': session_date,
        'themes': themes_list,
        'zone_map': build_zone_map(
            per_ticker.values(), _load_zone_master(session_date), ticker_themes),
    }


def export_momentum_136(day_flags_by_date, fundamentals=None):
    """Export momentum_136 — rebuilds the full retention-window history every run."""
    files = (SCREENING_OUTPUT_DIR / 'momentum_136').glob('momentum_136_*.parquet')
    sessions = [(f.stem.replace('momentum_136_', ''), f) for f in files]
    if not sessions:
        print("   No momentum_136 parquet found, skipping momentum export")
        return None
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers()

    def build(date_str, path, newest_session):
        return _build_momentum_136_snapshot(
            path, day_flags_by_date.get(date_str, {}),
            newest_session=newest_session, fundamentals=fundamentals)

    return _export_session_history(
        sessions, build, OUTPUT_DIR, 'momentum_136',
        lambda cur: (f"{len(cur['themes'])} themes, "
                     f"{sum(len(th['tickers']) for th in cur['themes'])} tickers"))


def _build_vars_snapshot(path, day_flags, newest_session=False, fundamentals=None):
    """Build a single VARS snapshot dict from one screener parquet.

    Tickers are grouped by leaf theme path, then leaves are clustered under
    their taxonomy L1: `themes` is a list of L1 sections, each carrying its
    `leaves` (per-leaf ticker tables). L1s with fewer than
    `vars_tab.min_tickers_per_l1` unique members and Uncategorized/
    Singleton leaves are dropped. L1 score = mean of the top
    `vars_tab.top_k_vars` member VARS values; L1s sort by score desc,
    then L1-avg (vars - vars_20ema) acceleration desc. Leaves within an
    L1 sort by avg VARS desc; tickers within a leaf by VARS desc. An
    L1 is `hot` when avg member RS >= `vars_tab.hot_rs_threshold` and it
    has >= 3 members. `zone_map` covers the tickers the tab renders.
    Returns the JSON dict or None if the parquet is empty.

    `newest_session` and `fundamentals` are as in `_build_momentum_136_snapshot`.
    """
    df = su.load_df_from_parquet(path).fillna(0)
    if df.empty:
        return None

    session_date = str(df['date'].iloc[0]) if 'date' in df.columns else ''
    screener_tickers = df['ticker'].astype(str).str.upper().tolist()
    ticker_themes, theme_to_tickers = _screener_theme_buckets(screener_tickers)
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers(screener_tickers)
    per_ticker = _screener_ticker_rows(
        df, fundamentals, newest_session, lead=lambda row, _t: _vars_fields(row))

    # Leaf tables — drop Uncategorized/Singleton; no per-leaf minimum (L1
    # breadth decides below, so a 2-member leaf like Cybersecurity / Endpoint
    # still renders inside its L1 section).
    leaf_list = [_avg_vars_table(name, rows) for name, rows in _theme_tables(
        theme_to_tickers, per_ticker, 'vars', skip=CATCHALL_THEMES)]
    _color_tables(leaf_list, day_flags)
    _attach_hierarchy(leaf_list)

    # Cluster leaves under their L1; score, flag and sort each L1 section.
    tab_cfg = CONFIG.get('vars_tab') or {}
    min_l1 = int(tab_cfg.get('min_tickers_per_l1', 3))
    top_k = int(tab_cfg.get('top_k_vars', 5))
    hot_rs = float(tab_cfg.get('hot_rs_threshold', 70.0))

    leaves_by_l1 = {}
    for leaf in leaf_list:
        leaves_by_l1.setdefault(leaf['l1'], []).append(leaf)

    l1_list = []
    for l1, leaves in leaves_by_l1.items():
        # Unique members — a dual-tagged ticker may sit in two leaves of one L1
        members = list({td['ticker']: td for leaf in leaves for td in leaf['tickers']}.values())
        if len(members) < min_l1:
            continue
        leaves.sort(key=lambda lf: -lf['avg_vars'])
        top_vars = sorted((m['vars'] for m in members), reverse=True)[:top_k]
        avg_rs = sum(m['rs'] for m in members) / len(members)
        l1_list.append({
            'name': l1,
            'score': round(sum(top_vars) / len(top_vars), 2),
            'avg_vars': round(sum(m['vars'] for m in members) / len(members), 2),
            'avg_rs': round(avg_rs, 1),
            'accel': round(sum(m['vars'] - m['vars_20ema'] for m in members) / len(members), 2),
            'n': len(members),
            'hot': bool(avg_rs >= hot_rs and len(members) >= 3),
            'leaves': leaves,
        })

    l1_list.sort(key=lambda grp: (-grp['score'], -grp['accel'], grp['name']))

    # The zone map covers exactly the tickers this tab renders.
    rendered = [td for grp in l1_list for leaf in grp['leaves'] for td in leaf['tickers']]

    return {
        'report_date': session_date,
        'themes': l1_list,
        'zone_map': build_zone_map(rendered, _load_zone_master(session_date), ticker_themes),
    }


def write_vars_artifact(snapshot, artifact_dir=None):
    """Write the latest VARS tab snapshot as a date-stamped workflow artifact."""
    if not snapshot:
        return None

    out_dir = Path(artifact_dir) if artifact_dir is not None else VARS_ARTIFACT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    report_date = str(snapshot.get('report_date') or 'latest')
    safe_date = re.sub(r'[^0-9A-Za-z_.-]+', '-', report_date).strip('-') or 'latest'
    out = out_dir / f"vars_{safe_date}.json"
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump(snapshot, fh, indent=2)
    return out


def export_vars(day_flags_by_date, fundamentals=None):
    """Export VARS — rebuilds the full retention-window history every run,
    then writes the current snapshot as a date-stamped workflow artifact."""
    files = (SCREENING_OUTPUT_DIR / 'vars').glob('vars_*.parquet')
    sessions = [(f.stem.replace('vars_', ''), f) for f in files]
    if not sessions:
        print("   No vars parquet found, skipping vars export")
        return None
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers()

    def build(date_str, path, newest_session):
        return _build_vars_snapshot(
            path, day_flags_by_date.get(date_str, {}),
            newest_session=newest_session, fundamentals=fundamentals)

    current = _export_session_history(
        sessions, build, OUTPUT_DIR, 'vars',
        lambda cur: (f"{len(cur['themes'])} L1s, "
                     f"{sum(grp.get('n', len(grp.get('tickers', []))) for grp in cur['themes'])} tickers"))
    if current is None:
        return None

    artifact_out = write_vars_artifact(current)
    if artifact_out:
        print(f"   -> {artifact_out} (VARS tab artifact)")
    return current


# ── Trending tab (runs of higher lows / lower highs) ─────────────────────────

def _trending_side(rows, theme_to_tickers, ascending):
    """L1 sections for one side of the Trending tab.

    Every ticker gets a rank by 1-week return across the whole side: best
    first on the up side (`ascending` False), worst first on the down side.
    Leaves and L1 sections then sort by their best-ranked member, so the
    rank 1 ticker always heads the first section. Uncategorized and
    Singleton sort last. There is no breadth minimum: the screener is the
    whole filter, and a one-name L1 is still a trending stock.
    """
    ranked = sorted(rows.values(), key=lambda td: td['perf_1w'], reverse=not ascending)
    for rank, td in enumerate(ranked, 1):
        td['rank'] = rank

    leaf_list = []
    for name, tickers in theme_to_tickers.items():
        members = sorted((rows[t] for t in tickers if t in rows), key=lambda td: td['rank'])
        if members:
            leaf_list.append({
                'name': name,
                'avg_perf_1w': round(sum(td['perf_1w'] for td in members) / len(members), 1),
                'tickers': members,
            })
    _attach_hierarchy(leaf_list)

    leaves_by_l1 = {}
    for leaf in leaf_list:
        leaves_by_l1.setdefault(leaf['l1'], []).append(leaf)

    l1_list = []
    for l1, leaves in leaves_by_l1.items():
        leaves.sort(key=lambda lf: lf['tickers'][0]['rank'])
        # Unique members — a dual-tagged ticker may sit in two leaves of one L1
        members = list({td['ticker']: td for leaf in leaves for td in leaf['tickers']}.values())
        l1_list.append({
            'name': l1,
            'best_rank': leaves[0]['tickers'][0]['rank'],
            'avg_perf_1w': round(sum(td['perf_1w'] for td in members) / len(members), 1),
            'n': len(members),
            'leaves': leaves,
        })
    l1_list.sort(key=lambda grp: (grp['name'] in CATCHALL_THEMES, grp['best_rank']))
    return l1_list


def _build_trending_snapshot(path, day_flags, newest_session=False, fundamentals=None):
    """Build a single Trending snapshot dict from one screener parquet.

    `down` and `up` each hold L1 sections of leaf tables, laid out like the
    VARS tab; see `_trending_side` for the order. A ticker in both a run of
    higher lows and a run of lower highs is listed on both sides.
    Returns the JSON dict or None if the parquet is empty.

    `newest_session` and `fundamentals` are as in `_build_momentum_136_snapshot`.
    """
    from src.screening.screeners.trending import trend_sides

    df = su.load_df_from_parquet(path).fillna(0)
    if df.empty:
        return None

    session_date = str(df['date'].iloc[0]) if 'date' in df.columns else ''
    screener_tickers = df['ticker'].astype(str).str.upper().tolist()
    _, theme_to_tickers = _screener_theme_buckets(screener_tickers)
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers(screener_tickers)

    def lead_for(streak_col):
        return lambda row, _t: {
            'perf_1w': round(float(row.get('perf_1w', 0) or 0) * 100, 1),
            'streak': int(row.get(streak_col, 0) or 0),
            **_vars_fields(row),
        }

    up, down = trend_sides(df)
    snapshot = {'report_date': session_date}
    for side, mask, streak_col, ascending in (
            ('down', down, 'lower_high_streak', True),
            ('up', up, 'higher_low_streak', False)):
        rows = _screener_ticker_rows(
            df[mask], fundamentals, newest_session, lead=lead_for(streak_col))
        if day_flags:
            enrich_with_ticker_color(list(rows.values()), day_flags)
        snapshot[side] = _trending_side(rows, theme_to_tickers, ascending)
    return snapshot


def export_trending(day_flags_by_date, fundamentals=None):
    """Export Trending — rebuilds the full retention-window history every run."""
    files = (SCREENING_OUTPUT_DIR / 'trending').glob('trending_*.parquet')
    sessions = [(f.stem.replace('trending_', ''), f) for f in files]
    if not sessions:
        print("   No trending parquet found, skipping trending export")
        return None
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers()

    def build(date_str, path, newest_session):
        return _build_trending_snapshot(
            path, day_flags_by_date.get(date_str, {}),
            newest_session=newest_session, fundamentals=fundamentals)

    def count(side):
        return len({td['ticker'] for grp in side for leaf in grp['leaves']
                    for td in leaf['tickers']})

    return _export_session_history(
        sessions, build, OUTPUT_DIR, 'trending',
        lambda cur: f"{count(cur['down'])} down, {count(cur['up'])} up")


# ── SI tab (heavily shorted names that have already sold off) ───────────────

def _finite(value):
    """Return ``value`` as a float, or None when it is missing or not finite.

    `float()` accepts 'nan' and 'inf' without raising and `NaN != NaN`, so an
    explicit finite check is the only thing that stops a NaN flowing into a
    comparison and quietly answering False.
    """
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def si_gate(short_interest, drawdown_60, drop_15d, cfg):
    """Does this ticker belong on the SI tab?

        short interest > floor AND (60-day drawdown <= limit OR 15-day drop <= limit)

    ⛔ Fails CLOSED on a missing leg, which is the opposite of `filter_metrics`.
    The difference is what each one asserts. The V/A cutoffs hide a row from a
    view, so a missing metric must not hide it. This gate asserts that a
    selloff happened, and a ticker with no price history has proved nothing.
    Failing open here would publish untested names as confirmed breakdowns.

    Either selloff leg alone is enough. A name that slid 30% over three months
    and one that dropped 30% in a fortnight are both broken charts, and
    requiring both would surface neither.
    """
    si_value = _finite(short_interest)
    if si_value is None or si_value <= float(cfg.get('min_short_interest', 12.0)):
        return False

    dd_limit = float(cfg.get('max_drawdown_60d', -0.25))
    drop_limit = float(cfg.get('max_drop_15d', -0.25))

    dd = _finite(drawdown_60)
    drop = _finite(drop_15d)
    return (dd is not None and dd <= dd_limit) or (drop is not None and drop <= drop_limit)


def _load_radar_ranks(radar_file=None):
    """Return ``{L1 name: rank}`` from the radar export, or ``{}``.

    The radar is the right source for "is this sector hot". It scores every
    tagged ticker with no screener gate, so it reports that semiconductors are
    strong while a broken name inside them is not. That name's own RS cannot
    say so — it is low by construction, which is why it is on this tab.

    A missing or malformed file yields no ranks. Every badge then disappears
    and nothing reorders, because the badge is decoration and the ranking is
    short interest.
    """
    path = Path(radar_file) if radar_file is not None else (OUTPUT_DIR / 'radar.json')
    try:
        with open(path, 'r', encoding='utf-8') as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError, ValueError):
        return {}

    # `l1s` since the 2026-07 consolidation; `ecosystems` in older files.
    entries = payload.get('l1s') or payload.get('ecosystems') or []
    ranks = {}
    for entry in entries:
        name = entry.get('name')
        rank = entry.get('rank')
        if name and rank is not None:
            ranks[name] = rank
    return ranks


def _top_k_mean(values, k):
    """Mean of the k largest values. Fewer than k averages what is there."""
    top = sorted(values, reverse=True)[:max(int(k), 1)]
    return sum(top) / len(top) if top else 0.0


def _build_si_snapshot(si_rows, master_df, day_flags, ticker_themes, radar_ranks,
                       cfg, si_date=None):
    """Build one SI snapshot: L1 sections, each holding its leaf tables.

    L1 is the ranked unit and the leaf is not. Measured 2026-09-10, the leaf
    level gave 67 leaves of which only 24 held 2 or more members, so
    single-name leaves filled the top of the tab and the "several shorted
    names lift the theme" requirement never fired. The same data at L1 level
    with a 3-member minimum gives 9 sections covering all 105 tickers.

    An L1 scores as the mean of its top `top_k_si` member short-interest
    values, and breadth breaks ties. Summing members instead was considered
    and rejected: CLAUDE.md records that choice failing on the tape-pressure
    board, where breadth swamped intensity and 0 of 67 single-name groups ever
    reached the screen.

    Returns None when nothing survives, so the caller can skip the session.
    """
    if master_df is None or master_df.empty or not si_rows:
        return None

    bars = _bars_by_ticker(master_df)

    report_date = ''
    if 'date' in master_df.columns and len(master_df):
        report_date = str(master_df['date'].iloc[0])

    # 1. Gate, and build the per-ticker payload once.
    per_ticker = {}
    for row in si_rows:
        ticker = str(row.get('ticker', '')).strip().upper()
        bar = bars.get(ticker)
        if not ticker or bar is None:
            continue

        close = _finite(bar.get('close'))
        max60 = _finite(bar.get('max60'))
        drawdown_60 = (close / max60 - 1) if (close is not None and max60) else None
        drop_15d = _finite(bar.get('drop_15d'))
        si_value = _finite(row.get('si'))

        if not si_gate(si_value, drawdown_60, drop_15d, cfg):
            continue

        streak = _finite(bar.get('max_down_streak'))
        per_ticker[ticker] = {
            'ticker': ticker,
            'si': round(si_value, 2),
            'dd60': round(drawdown_60 * 100, 1) if drawdown_60 is not None else None,
            'drop15': round(drop_15d * 100, 1) if drop_15d is not None else None,
            'streak': int(streak) if streak is not None else None,
            'price': round(close, 2) if close is not None else None,
            'float': _fmt_float_m(row.get('float_shares')),
            'inst': _fmt_inst(row.get('inst_trans')),
            'ticker_color': day_flags.get(ticker),
            # `export_si` builds only the newest master parquet, so this
            # snapshot is always the current session and the roster's own short
            # interest may reach the ladder. Its age travels in `si_stale`.
            'highlight': _highlight_from_row(bar, si_value),
        }

    if not per_ticker:
        return None

    # 2. Group surviving tickers into leaves, then cluster leaves under their L1.
    leaves_by_l1 = {}
    members_by_l1 = {}
    for ticker in per_ticker:
        for path in ticker_themes.get(ticker) or []:
            if path in CATCHALL_THEMES:
                continue
            l1 = resolve_l1(path)
            if not l1 or l1 in CATCHALL_THEMES:
                continue
            leaves_by_l1.setdefault(l1, {}).setdefault(path, []).append(ticker)
            # A set, so a name tagged into two leaves of one L1 counts once
            # toward that L1's breadth.
            members_by_l1.setdefault(l1, set()).add(ticker)

    min_members = int(cfg.get('min_tickers_per_l1', 3))
    top_k = int(cfg.get('top_k_si', 3))
    hot_rank = int(cfg.get('hot_radar_rank', 10))

    themes = []
    for l1, leaves in leaves_by_l1.items():
        members = members_by_l1[l1]
        if len(members) < min_members:
            continue

        leaf_list = []
        for path, tickers in leaves.items():
            rows = sorted((per_ticker[t] for t in tickers), key=lambda r: -r['si'])
            leaf_list.append({
                'name': path,
                'score': round(_top_k_mean([r['si'] for r in rows], top_k), 2),
                'n': len(rows),
                'tickers': rows,
            })
        leaf_list.sort(key=lambda lf: (-lf['score'], -lf['n'], lf['name']))

        rank = radar_ranks.get(l1)
        themes.append({
            'name': l1,
            'score': round(_top_k_mean([per_ticker[t]['si'] for t in members], top_k), 2),
            'n': len(members),
            'n_leaves': len(leaf_list),
            'radar_rank': rank,
            'hot': rank is not None and rank <= hot_rank,
            'leaves': leaf_list,
        })

    if not themes:
        return None

    # Breadth breaks a score tie; it never scales the score.
    themes.sort(key=lambda t: (-t['score'], -t['n'], t['name']))

    # ⛔ The roster's own date travels with the snapshot, and an unknown date
    # reads as stale. Step 7b is non-critical and short_interest.json is
    # committed, so a failed Finviz fetch leaves the PREVIOUS roster on disk.
    # Joined to today's prices and stamped with today's master date it would
    # look fresh — the frozen-NAAIM-tile shape. Keep the reading, carry its
    # age, let the tab say so: a one-session-old roster is still useful, an
    # undated one is not.
    roster_date = str(si_date) if si_date else None
    return {
        'report_date': report_date,
        'si_date': roster_date,
        'si_stale': roster_date != report_date,
        'n_tickers': len(per_ticker),
        'n_themes': len(themes),
        'themes': themes,
    }


def export_si(day_flags_by_date, root=None, out_dir=None, si_file=None):
    """Export the SI tab — current snapshot plus a forward-growing history.

    ⛔ History only grows forward, unlike every parquet-backed tab. Finviz
    publishes current short interest, not a per-session series. Rebuilding
    history from the master parquet would pin today's short interest onto a
    three-month-old price bar and publish it as that session's reading — a
    fabricated number, not a stale one. So this appends one entry per run,
    like the ETF tabs, pruned to the same retention window.
    """
    from src.themes.theme_registry import load_ticker_themes

    root = Path(root) if root is not None else SCREENING_OUTPUT_DIR
    out_dir = Path(out_dir) if out_dir is not None else OUTPUT_DIR
    si_path = Path(si_file) if si_file is not None else SHORT_INTEREST_FILE

    try:
        with open(si_path, 'r', encoding='utf-8') as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError, ValueError):
        print("   No short_interest.json found, skipping SI export")
        return None

    si_rows = payload.get('rows') or []
    if not si_rows:
        print("   short_interest.json holds no rows, skipping SI export")
        return None

    master_files = sorted((root / 'master').glob('master_*.parquet'), reverse=True)
    if not master_files:
        print("   No master parquet found, skipping SI export")
        return None

    master_df = su.load_df_from_parquet(master_files[0])
    master_date = master_files[0].stem.replace('master_', '')
    snapshot = _build_si_snapshot(
        si_rows, master_df, day_flags_by_date.get(master_date, {}),
        load_ticker_themes(),
        _load_radar_ranks(out_dir / 'radar.json'), CONFIG.get('si_tab', {}),
        si_date=payload.get('date'),
    )
    if snapshot is None:
        print("   No SI tickers survived the gate, skipping SI export")
        return None

    if snapshot['si_stale']:
        # Loud, because the tab is otherwise indistinguishable from a fresh one.
        # A stale roster means step 7b failed: Finviz blocked us, the Ownership
        # view changed shape, or the run happened before the close.
        print(
            f"   Warning: short interest is STALE — roster dated "
            f"{snapshot['si_date'] or 'unknown'} against session "
            f"{snapshot['report_date']}. Step 7b likely failed. Publishing it "
            f"anyway with the age marked; check the Finviz fetch."
        )

    out = out_dir / "si.json"
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump(snapshot, fh, indent=2)
    print(
        f"   -> {out} ({snapshot['n_themes']} L1s, "
        f"{snapshot['n_tickers']} tickers, date {snapshot['report_date']})"
    )

    _update_history_file(out_dir / "si_history.json", snapshot['report_date'], snapshot)

    return snapshot


def _build_radar_snapshot(master_file, screened_set, day_flags, tickers_per_leaf=None,
                          fundamentals=None, newest_session=False):
    """Build one L1 Radar session snapshot from a master parquet.

    Unlike the retired screened-themes backfill, the radar scores ALL tagged
    tickers above the liquidity floor (no screener gate) — the screened union
    is only used to flag which members are actionable setups today. NaNs are
    left intact:
    the scoring module maps missing legs to its neutral default rather than 0.
    Ticker theme tags are point-in-time (today's tags on past sessions), the
    same trade-off as every other backfill.

    `tickers_per_leaf=None` ships every scored member (what `radar.json` wants,
    so the dashboard can expand a leaf to its full roster); an int caps the
    chips per leaf, which is how history entries stay small.

    `fundamentals` is the short-interest table the caller loads once per run,
    and `newest_session` says whether this session may read it. See
    `_highlight_from_row` for why an older session must not.
    """
    from src.themes.l1_score import compute_radar

    master_df = su.load_df_from_parquet(master_file)
    if master_df.empty:
        return None
    session_date = str(master_df['date'].iloc[0]) if 'date' in master_df.columns else ''
    if not session_date:
        return None

    # `compute_radar`'s member dicts carry no moving averages — only ticker,
    # composite, rs, vars, price, liquidity, the tight fields and screened. So
    # the highlight ladder reads the session's own master row through this
    # lookup, the same join `_build_si_snapshot` uses. `l1_score` stays as it is.
    # Only the ladder's own columns: this runs once per retained session, and a
    # full-width row dict would carry forty-odd columns nothing here reads.
    bars = _bars_by_ticker(master_df, HIGHLIGHT_ROW_COLUMNS)
    short_rows = fundamentals if (fundamentals and newest_session) else {}

    def _member_highlight(ticker):
        key = str(ticker).upper()
        # A member with no master row earns no tier. An empty dict answers
        # every rung with "absent", which is what the ladder wants.
        return _highlight_from_row(
            bars.get(key) or {},
            (short_rows.get(key) or {}).get('short_interest'),
        )

    body = compute_radar(master_df, screened_tickers=screened_set)
    if body is None:
        return None

    # A back-dated parquet predating the indicator has no tight-range columns,
    # so every n_tight would be a truthful 0 — and the dashboard cannot tell that
    # from "the market has no tight themes today", which is a claim about the
    # market made from an absence of data. Publish null instead so the strip
    # hides rather than asserting. Same failure shape as the frozen NAAIM tile
    # and the undated SI roster.
    has_tight = 'tight_range' in master_df.columns and 'tightness' in master_df.columns

    # Theme Viz: the members of the top `top_leaves` leaves, UNCAPPED. The
    # history file caps chips at `tickers_per_leaf`, and a map built from the
    # capped chips would show fewer names on a back-dated session than on
    # the newest one.
    zone_cfg = zone_map_config()
    top_leaves = int(zone_cfg['top_leaves'])
    zone_rows = []
    leaves_by_ticker = {}

    l1s = []
    for l1_entry in body['l1s']:
        leaves = []
        for leaf in l1_entry['leaves']:
            members = [{
                'ticker': m['ticker'],
                'score': round(m['composite'], 1),
                'rs': round(m['rs'], 1),
                'vars': round(m['vars'], 2) if m['vars'] is not None else None,
                'price': round(m['price'], 2) if m['price'] is not None else None,
                'dollar_vol': _int_or_none(m.get('avg_dollar_vol')),
                'adr_pct': _round_or_none(m.get('adr_pct'), 4),
                # NaN is not valid JSON and would break the page rather than
                # degrade it, so an unmeasurable figure serializes as null.
                'tightness': _round_or_none(m.get('tightness'), 3),
                'tight': bool(m.get('tight', False)),
                'tight_len': m.get('tight_len'),
                'tight_pctile': _round_or_none(m.get('tight_pctile'), 1),
                'tight_gate': m.get('tight_gate'),
                'is_screened': bool(m.get('is_screened', False)),
                'highlight': _member_highlight(m['ticker']),
            } for m in leaf['members']]
            if day_flags:
                enrich_with_ticker_color(members, day_flags)
            for td in members:
                leaves_by_ticker.setdefault(str(td['ticker']).upper(), []).append(leaf['theme'])
            if leaf['global_rank'] <= top_leaves:
                zone_rows.extend(members)
            ticker_dicts = members[:tickers_per_leaf]
            leaves.append({
                'global_rank': leaf['global_rank'],
                'name': leaf['theme'],
                'l2': leaf['l2'],
                'l3': leaf['l3'],
                'raw': round(leaf['raw'], 3),
                'boosted': round(leaf['boosted'], 3),
                'n': leaf['breadth'],
                # Count over ALL scored members, not just the chips that
                # survived tickers_per_leaf — a capped history entry must not
                # report a smaller tight count than the same session's radar.json.
                'n_tight': leaf.get('n_tight') if has_tight else None,
                'tickers': ticker_dicts,
            })
        l1s.append({
            'rank': l1_entry['rank'],
            'name': l1_entry['name'],
            'raw': round(l1_entry['raw'], 4),
            'boosted': round(l1_entry['boosted'], 4),
            'delta': round(l1_entry['delta'], 4),
            'n_leaves': l1_entry['n_leaves'],
            'n_members': l1_entry['n_members'],
            'n_screened': l1_entry['n_screened'],
            'n_tight': l1_entry.get('n_tight') if has_tight else None,
            'leaves': leaves,
        })

    return {
        'report_date': session_date,
        'params': body['params'],
        'universe_size': body['universe_size'],
        'n_leaves_scored': body['n_leaves_scored'],
        'l1s': l1s,
        'zone_map': build_zone_map(zone_rows, master_df, leaves_by_ticker, zone_cfg),
    }


def export_radar(day_flags_by_date, root=None, out_dir=None, fundamentals=None):
    """Export the L1 Radar (dashboard Themes tab) — full
    retention-window history every run.

    Iterates the per-day master parquet files within the window, derives each
    date's screened union from the per-screener parquet, and rewrites both
    files from scratch. `radar.json` keeps every scored L1 and leaf (no top-N
    burial) *and* every member ticker; history entries cap L1s at
    `radar.history_l1_limit` and chips at `radar.tickers_per_leaf`, and are
    written compact to bound size.
    """
    root = Path(root) if root is not None else SCREENING_OUTPUT_DIR
    out_dir = Path(out_dir) if out_dir is not None else OUTPUT_DIR

    files = (root / 'master').glob('master_*.parquet')
    sessions = [(f.stem.replace('master_', ''), f) for f in files]
    if not sessions:
        print("   No master parquet found, skipping radar export")
        return None

    radar_cfg = CONFIG.get('radar', {})
    chip_cap = int(radar_cfg.get('tickers_per_leaf', 10))
    l1_limit = int(radar_cfg.get('history_l1_limit', 20))

    # Short interest loads once, outside the session loop — the loop runs about
    # 124 times and Finviz publishes one current figure, not a series. The
    # radar's universe is every tagged ticker, so that is the key set to ask
    # for; `fundamentals.db` holds the screened union, and the rest miss.
    if fundamentals is None:
        from src.themes.theme_registry import load_ticker_themes
        fundamentals = _load_fundamentals_for_tickers(load_ticker_themes().keys())

    def build(date_str, master_file, newest_session):
        screened_set = su.union_tickers_for_date(
            date_str, CONFIG['screeners'], root=root
        )
        # The newest session becomes radar.json and ships every member; older
        # sessions stay capped so radar_history.json does not balloon — it
        # already carries ~124 sessions.
        return _build_radar_snapshot(
            master_file, screened_set, day_flags_by_date.get(date_str, {}),
            tickers_per_leaf=None if newest_session else chip_cap,
            fundamentals=fundamentals,
            newest_session=newest_session,
        )

    def trim(history):
        return [
            {**snap, 'l1s': [
                # history[0] is the uncapped current snapshot — re-cap its chips
                # so every history entry carries the same top-N roster.
                {**l1, 'leaves': [
                    {**lf, 'tickers': lf['tickers'][:chip_cap]} for lf in l1['leaves']
                ]} if i == 0 else l1
                for l1 in snap['l1s'][:l1_limit]
            ]}
            for i, snap in enumerate(history)
        ]

    return _export_session_history(
        sessions, build, out_dir, 'radar',
        lambda cur: (f"{len(cur['l1s'])} L1s, {cur['n_leaves_scored']} leaves, "
                     f"universe {cur['universe_size']}"),
        history_view=trim, compact_history=True)


#: The two scans the Volume tab unions, in the order their `scan` tag reads.
VOLUME_SCANS = ('volspike', 'denvol')


def _build_volume_snapshot(date_str, day_flags, newest_session=False, fundamentals=None):
    """Build a single Volume snapshot from the volspike + denvol parquet for one date.

    Unions the tickers from both scans (tagging each with which scan matched),
    groups them by theme, and sorts within each theme by VARS desc and themes by
    avg VARS desc. ALL themes are shown (no min-ticker filter); Uncategorized and
    Singleton sink to the bottom. Returns the JSON dict, or None if neither scan
    has rows for the date.

    `newest_session` and `fundamentals` are as in `_build_momentum_136_snapshot`.
    """
    import pandas as pd

    # Union the two scans for this date; tag each ticker with its source scan(s).
    frames = []
    scan_for_ticker = {}
    for scan_name in VOLUME_SCANS:
        path = SCREENING_OUTPUT_DIR / scan_name / f'{scan_name}_{date_str}.parquet'
        if not path.exists():
            continue
        sdf = su.load_df_from_parquet(path).fillna(0)
        if sdf.empty:
            continue
        frames.append(sdf)
        for t in sdf['ticker'].astype(str).str.upper():
            prev = scan_for_ticker.get(t)
            scan_for_ticker[t] = 'both' if (prev and prev != scan_name) else scan_name

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset='ticker')
    if df.empty:
        return None

    session_date = str(df['date'].iloc[0]) if 'date' in df.columns else date_str
    screener_tickers = df['ticker'].astype(str).str.upper().tolist()
    ticker_themes, theme_to_tickers = _screener_theme_buckets(screener_tickers)
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers(screener_tickers)
    per_ticker = _screener_ticker_rows(
        df, fundamentals, newest_session,
        lead=lambda row, t: {
            'scan': scan_for_ticker.get(t, ''),
            'days_since_hv': _int_or_none(row.get('days_since_highest_volume')),
            **_vars_fields(row),
        })

    # Show ALL themes (no min-ticker filter); sort by avg VARS desc.
    # Uncategorized/Singleton sink to the bottom regardless of avg VARS.
    themes_list = [_avg_vars_table(name, rows)
                   for name, rows in _theme_tables(theme_to_tickers, per_ticker, 'vars')]
    themes_list.sort(key=lambda th: (
        th['name'] in CATCHALL_THEMES,
        -th['avg_vars'],
        th['name'],
    ))
    _color_tables(themes_list, day_flags)
    _attach_hierarchy(themes_list)

    return {
        'report_date': session_date,
        'themes': themes_list,
        'zone_map': build_zone_map(
            per_ticker.values(), _load_zone_master(session_date), ticker_themes),
    }


def export_volume(day_flags_by_date, fundamentals=None):
    """Export the Volume tab — union of the volspike + denvol scans.

    Rebuilds the full retention-window history every run from every date for
    which either scan wrote a parquet.
    """
    dates = set()
    for scan_name in VOLUME_SCANS:
        for path in (SCREENING_OUTPUT_DIR / scan_name).glob(f'{scan_name}_*.parquet'):
            # filename pattern: <scan>_YYYY-MM-DD.parquet
            dates.add(path.stem[len(scan_name) + 1:])
    if not dates:
        print("   No volspike/denvol parquet found, skipping volume export")
        return None
    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers()

    def build(date_str, _source, newest_session):
        return _build_volume_snapshot(
            date_str, day_flags_by_date.get(date_str, {}),
            newest_session=newest_session, fundamentals=fundamentals)

    return _export_session_history(
        [(d, d) for d in dates], build, OUTPUT_DIR, 'volume',
        lambda cur: (f"{len(cur['themes'])} themes, "
                     f"{sum(len(th['tickers']) for th in cur['themes'])} tickers"))


def _numeric_series(df, column):
    """Return a numeric Series for a DataFrame column, or NaN values if absent."""
    import pandas as pd

    if column not in df.columns:
        return pd.Series(float('nan'), index=df.index)
    return pd.to_numeric(df[column], errors='coerce')


def _round_or_none(value, digits=1):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return round(number, digits)


def _int_or_none(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return int(number)


#: The `fundamentals.db` columns the payload rows read, in SELECT order.
FUNDAMENTALS_COLUMNS = ('shares_float', 'eps_growth_qq', 'sales_growth_qq',
                        'short_interest', 'inst_transactions')


def _load_fundamentals_for_tickers(tickers=None):
    """``{TICKER: {column: value}}`` from `fundamentals.db`, or ``{}``.

    `tickers=None` reads the whole table, which is how an exporter loads it
    once per run: the table holds the screened union only, and Finviz
    publishes one current figure per ticker rather than a series.

    Guarded: a missing, locked or malformed database logs a warning and reads
    as empty, so every tab still publishes with blank fundamentals columns
    instead of aborting the export.
    """
    import sqlite3

    if not FUNDAMENTALS_DB.exists():
        return {}
    columns = ', '.join(('ticker',) + FUNDAMENTALS_COLUMNS)
    if tickers is None:
        query, params = f'SELECT {columns} FROM fundamentals', []
    else:
        params = sorted({str(t).upper() for t in tickers if str(t).strip()})
        if not params:
            return {}
        placeholders = ','.join(['?'] * len(params))
        query = f'SELECT {columns} FROM fundamentals WHERE ticker IN ({placeholders})'

    try:
        conn = sqlite3.connect(FUNDAMENTALS_DB)
        try:
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()
    except Exception as e:
        print(f"   Warning: fundamentals lookup failed: {e}")
        return {}

    return {
        str(row[0]).upper(): dict(zip(FUNDAMENTALS_COLUMNS, row[1:]))
        for row in rows
    }


def add_parabolic_derived_columns(master_df, previous_df=None):
    """Add atr_multi_50sma and previous-session fields needed by parabolic."""
    df = master_df.copy()
    df['ticker'] = df['ticker'].astype(str).str.upper()

    if 'atr_multi_50sma' in df.columns:
        df['atr_multi_50sma'] = _numeric_series(df, 'atr_multi_50sma')
    else:
        close = _numeric_series(df, 'close')
        sma50 = _numeric_series(df, 'sma50')
        if 'atr_pct' in df.columns:
            atr_pct = _numeric_series(df, 'atr_pct')
        else:
            atr_pct = _numeric_series(df, 'atr14') / close
        df['atr_multi_50sma'] = (close / sma50 - 1) / atr_pct

    previous_columns = {
        'previous_session_high': 'high',
        'previous_session_low': 'low',
        'previous_session_volume': 'volume',
    }

    missing_previous = [
        col for col in previous_columns
        if col not in df.columns
    ]
    if missing_previous and previous_df is not None and not previous_df.empty:
        prev = previous_df.copy()
        prev['ticker'] = prev['ticker'].astype(str).str.upper()
        prev = prev.drop_duplicates(subset='ticker').set_index('ticker')
        for out_col, source_col in previous_columns.items():
            if out_col in df.columns:
                df[out_col] = _numeric_series(df, out_col)
            elif source_col in prev.columns:
                df[out_col] = df['ticker'].map(prev[source_col])
                df[out_col] = _numeric_series(df, out_col)
    else:
        for out_col in previous_columns:
            df[out_col] = _numeric_series(df, out_col)

    return df


def filter_parabolic_candidates(master_df, previous_df=None):
    """Return parabolic candidates sorted by atr_multi_50sma descending."""
    df = add_parabolic_derived_columns(master_df, previous_df)

    avg_dollar_vol = _numeric_series(df, 'avg_dollar_vol')
    adr_pct = _numeric_series(df, 'adr_pct')
    close = _numeric_series(df, 'close')
    high = _numeric_series(df, 'high')
    low = _numeric_series(df, 'low')
    volume = _numeric_series(df, 'volume')
    atr_multi = _numeric_series(df, 'atr_multi_50sma')
    previous_high = _numeric_series(df, 'previous_session_high')
    previous_volume = _numeric_series(df, 'previous_session_volume')

    mask = (
        (avg_dollar_vol >= MIN_AVG_DOLLAR_VOL) &
        (adr_pct >= 0.04) &
        (close >= 5.0) &
        (atr_multi >= MIN_ATR_MULTI_50SMA) &
        (low >= previous_high) &
        (high > low) &
        (volume > previous_volume)
    )

    return (
        df[mask]
        .copy()
        .sort_values('atr_multi_50sma', ascending=False, na_position='last')
    )


def _parabolic_item_from_row(row, fundamentals, newest_session=False):
    """One parabolic payload row.

    `newest_session` gates the highlight ladder's short rung, as in
    `_build_momentum_136_snapshot`.
    """
    ticker = str(row.get('ticker', '')).upper()
    f = fundamentals.get(ticker, {})
    short_val = f.get('short_interest')

    return {
        'ticker': ticker,
        'price': _round_or_none(row.get('close'), 2),
        'float': _fmt_float_m(f.get('shares_float')),
        'inst': _fmt_inst(f.get('inst_transactions')),
        'short': _round_or_none(short_val, 1),
        'atr_multi_50sma': _round_or_none(row.get('atr_multi_50sma'), 1),
        # One dollar-volume key, shared with every other filtered tab. This item
        # used to carry a separate `avg_dollar_vol` that no renderer read; now
        # that the V lens reads dollar volume, the filter's key is the only one.
        'dollar_vol': filter_metrics(row)[0],
        'adr_pct': _round_or_none(row.get('adr_pct'), 4),
        'current_session_low': _round_or_none(row.get('low'), 2),
        'current_session_high': _round_or_none(row.get('high'), 2),
        'previous_session_high': _round_or_none(row.get('previous_session_high'), 2),
        'current_session_volume': _int_or_none(row.get('volume')),
        'previous_session_volume': _int_or_none(row.get('previous_session_volume')),
        'highlight': _highlight_from_row(
            row, short_val if newest_session else None),
    }


def _parabolic_snapshot(report_date, candidates, fundamentals, newest_session=False):
    tickers = [
        _parabolic_item_from_row(row, fundamentals, newest_session=newest_session)
        for _, row in candidates.iterrows()
    ]
    return {
        'report_date': report_date,
        'count': len(tickers),
        'tickers': tickers,
    }


def export_parabolic(fundamentals=None):
    """Export parabolic scanner data and the retention-window session history."""
    master_dir = SCREENING_OUTPUT_DIR / 'master'
    master_files = sorted(master_dir.glob('master_*.parquet'), reverse=True)
    if not master_files:
        print("   No master parquet found, skipping parabolic export")
        return None

    cutoff = _history_cutoff([f.stem.replace('master_', '') for f in master_files])
    # Newest first, so the window is a prefix of `master_files` and each
    # session's previous file is the next entry.
    current_files = [
        f for f in master_files
        if cutoff is None or f.stem.replace('master_', '') >= cutoff
    ]

    candidate_sets = []
    all_tickers = set()
    master_df = su.load_df_from_parquet(current_files[0])
    for idx, master_file in enumerate(current_files):
        previous_file = master_files[idx + 1] if idx + 1 < len(master_files) else None
        previous_df = su.load_df_from_parquet(previous_file) if previous_file else None
        report_date = (
            str(master_df['date'].iloc[0])
            if 'date' in master_df.columns and not master_df.empty
            else master_file.stem.replace('master_', '')
        )

        candidates = filter_parabolic_candidates(master_df, previous_df)
        all_tickers.update(candidates['ticker'].astype(str).str.upper().tolist())
        candidate_sets.append((report_date, candidates))
        # This session's previous frame is the next session's own: read once.
        master_df = previous_df

    if fundamentals is None:
        fundamentals = _load_fundamentals_for_tickers(all_tickers)

    history = []
    for report_date, candidates in candidate_sets:
        # `candidate_sets` runs newest first, so the first snapshot becomes
        # parabolic.json and is the only one allowed the short rung.
        snapshot = _parabolic_snapshot(
            report_date, candidates, fundamentals, newest_session=not history)
        history.append(snapshot)

    current = history[0]

    out = OUTPUT_DIR / "parabolic.json"
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump(current, fh, indent=2)
    print(f"   -> {out} ({current['count']} tickers)")

    history_out = OUTPUT_DIR / "parabolic_history.json"
    with open(history_out, 'w', encoding='utf-8') as fh:
        json.dump(history, fh, indent=2)
    dates = [h['report_date'] for h in history]
    print(f"   -> {history_out} (history: {', '.join(dates)})")

    return current


def _write_json(path, payload):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)


def export_etf_tabs(report_date):
    """The Lev ETF and Industry tabs, their histories, and ETF Viz.

    Both sheets are fetched and enriched with VARS, colour and tier from one
    standalone yfinance download, and only then written — once each.
    """
    print("\n3. Fetching leverage ETF data")
    etf_data = fetch_etf_data()
    print("\n4. Fetching industry ETF data")
    industry_data = fetch_industry_etf_data()

    # 4b. VARS + inside_day/tight_day colour via a standalone yfinance fetch.
    all_etf_tickers = [e['ticker'] for e in (etf_data or []) + (industry_data or [])]
    etf_metrics = {}
    if all_etf_tickers:
        print("\n   Computing VARS + ticker colors for ETFs...")
        spy_baseline = load_spy_cum_norm_100()
        etf_metrics = fetch_etf_metrics(all_etf_tickers, spy_baseline) if spy_baseline is not None else {}
        if etf_metrics and etf_data:
            etf_v, etf_c = enrich_etf_with_metrics(etf_data, etf_metrics)
            print(f"   Enriched leverage ETFs: {etf_v} with VARS, {etf_c} with ticker color")
        if etf_metrics and industry_data:
            ind_v, ind_c = enrich_etf_with_metrics(industry_data, etf_metrics)
            print(f"   Enriched industry ETFs: {ind_v} with VARS, {ind_c} with ticker color")

    if etf_data:
        _write_json(OUTPUT_DIR / "etf_data.json", etf_data)
        print(f"   -> {OUTPUT_DIR / 'etf_data.json'} ({len(etf_data)} ETFs)")
    if industry_data:
        _write_json(OUTPUT_DIR / "industry_etf.json", industry_data)
        print(f"   -> {OUTPUT_DIR / 'industry_etf.json'} ({len(industry_data)} industry ETFs)")

    # 4c. ETF history, keyed by the daily report's session date.
    if report_date and (etf_data or industry_data):
        update_etf_history(report_date, etf_data, industry_data)

    # 4d. ETF Viz: the long Lev ETFs on the zone map.
    if report_date and etf_data:
        print("\n4d. Exporting ETF Viz zone map")
        export_etf_viz(etf_data, etf_metrics, report_date)


def export_macro_data():
    print("\n5. Fetching Yahoo Finance macro data")
    macro_data = fetch_yahoo_macro_data()
    if macro_data:
        macro_output = OUTPUT_DIR / "macro_data.json"
        _write_json(macro_output, macro_data)
        print(f"   -> {macro_output}")
    else:
        print("   -> Macro data fetch failed, charts will show TradingView data only")


def export_macro_events():
    print("\n6. Fetching upcoming macro events")
    macro_events = fetch_macro_events()
    if macro_events:
        write_events_json(macro_events, OUTPUT_DIR / "events.json")
    else:
        print("   -> Macro events fetch returned no data, keeping existing events.json")


def export_all():
    """Main export function.

    Each step runs under its own guard: a step that raises prints its traceback
    and the remaining steps still run, down to report_meta.json and the prune.
    The failures are then raised together at the end, so the process still
    exits non-zero and CI still fails loudly — the guard only stops one broken
    tab from blanking every other one.
    """
    print("=" * 60)
    print("EXPORTING DASHBOARD DATA")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    failures = []

    def step(label, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            traceback.print_exc()
            print(f"   !! {label} FAILED; continuing with the remaining steps")
            failures.append(label)
            return None

    # 1. The newest daily report's date: report_meta.json and the ETF-history
    # session date. The Themes tab is fed by the radar export below.
    report_date = latest_report_date()
    print(f"\n1. Latest report session: {report_date or 'none found'}")

    # Day-pattern colour flags (tight/inside + close_to_ma), one set per
    # session, each read from that session's own master parquet.
    day_flags_by_date = step('day-pattern flags', load_day_flags_by_date) or {}
    if day_flags_by_date:
        newest = max(day_flags_by_date)
        print(f"   Day-pattern flags: {len(day_flags_by_date)} sessions, "
              f"{len(day_flags_by_date[newest])} green on {newest}")

    # One fundamentals read serves every tab below (guarded: empty on failure).
    fundamentals = _load_fundamentals_for_tickers()

    print("\n1b. Exporting momentum_136 data")
    step('momentum_136', export_momentum_136, day_flags_by_date, fundamentals=fundamentals)

    print("\n1c. Exporting volume data")
    step('volume', export_volume, day_flags_by_date, fundamentals=fundamentals)

    print("\n1d. Exporting vars data")
    step('vars', export_vars, day_flags_by_date, fundamentals=fundamentals)

    print("\n1d2. Exporting trending data")
    step('trending', export_trending, day_flags_by_date, fundamentals=fundamentals)

    print("\n1e. Exporting parabolic data")
    step('parabolic', export_parabolic, fundamentals=fundamentals)

    # 1f. L1 Radar (screener-independent; must run before the screening_output
    # prune at the tail so the full master window is on disk)
    if CONFIG.get('radar', {}).get('enabled', True):
        print("\n1f. Exporting L1 radar data")
        step('radar', export_radar, day_flags_by_date, fundamentals=fundamentals)

    # 1g. SI tab. Must run AFTER the radar export: the HOT badge reads
    # radar.json, and reading it earlier would badge every session against the
    # previous run's ranks.
    print("\n1g. Exporting SI data")
    step('si', export_si, day_flags_by_date)

    print("\n2. Updating market breadth history")
    step('market breadth', update_breadth_history, report_date)

    step('ETF tabs', export_etf_tabs, report_date)
    step('macro data', export_macro_data)
    step('macro events', export_macro_events)

    # 7. Report meta. winner_research finds dashboard commits by the
    # `Daily report <date>` commits that touch this file and copies it whole.
    print("\n7. Writing report meta")
    step('report meta', _write_json, OUTPUT_DIR / "report_meta.json", {
        'export_timestamp': datetime.now(timezone.utc).isoformat(),
        'report_date': report_date,
    })

    # Retention prune: screening_output/ is local scratch regenerated each run.
    # Runs LAST — after every history JSON above has read the full window — so it
    # only trims what the next run will regenerate (no-op in CI; nothing persists).
    from src.screening.prune_screening_output import prune_screening_output
    pruned = step('prune', prune_screening_output)
    if pruned:
        print(f"Pruned {pruned} old screening_output parquet file(s)")

    print(f"\n{'=' * 60}")
    if failures:
        print(f"EXPORT FINISHED WITH {len(failures)} FAILED STEP(S): {', '.join(failures)}")
        print(f"{'=' * 60}")
        raise RuntimeError(f"dashboard export steps failed: {', '.join(failures)}")
    print("EXPORT COMPLETE")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    export_all()
