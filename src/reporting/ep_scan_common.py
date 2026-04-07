"""
EP Scanner Common Utilities

Shared helpers for morning (BMO) and afternoon (AMC) EP scans:
- Finviz screener + fundamentals + news
- Alpaca 5-min bars for RVol at time (extended hours)
- Yahoo Finance for AH/PM prices and technicals
- Discord webhook notification
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, time as dt_time, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

try:
    from finvizfinance.screener.overview import Overview
    from finvizfinance.quote import finvizfinance as FinvizQuote
    FINVIZ_AVAILABLE = True
except ImportError:
    FINVIZ_AVAILABLE = False
    print("Warning: finvizfinance not installed")

ET = ZoneInfo("America/New_York")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOCS_DATA_DIR = PROJECT_ROOT / "docs" / "data"

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_EP_SCAN_WEBHOOK_URL", "")

ALPACA_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_API_SECRET", "")
ALPACA_DATA_URL = "https://data.alpaca.markets/v2"

# Extended hours time boundaries (ET)
PREMARKET_START = dt_time(4, 0)
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)
AFTERHOURS_END = dt_time(20, 0)

RVOL_LOOKBACK_SESSIONS = 10


# ── Finviz helpers ───────────────────────────────────────────────────────────

def scan_finviz_tickers(earnings_filter: str) -> List[str]:
    """Use Finviz screener to find tickers matching earnings + short + volume."""
    if not FINVIZ_AVAILABLE:
        return []
    try:
        overview = Overview()
        overview.set_filter(filters_dict={
            'Earnings Date': earnings_filter,
            'Short Float': 'Over 10%',
            'Average Volume': 'Over 1M',
        })
        df = overview.screener_view()
        if df is None or df.empty:
            return []
        return df['Ticker'].tolist()
    except Exception as e:
        print(f"  Finviz scan failed ({earnings_filter}): {e}")
        return []


def get_fundamentals(ticker: str) -> Optional[Dict]:
    """Fetch float, short%, avg volume from finviz."""
    if not FINVIZ_AVAILABLE:
        return None
    try:
        stock = FinvizQuote(ticker)
        f = stock.ticker_fundament()

        float_str = f.get('Shs Float', 'N/A')
        stock_float = None
        if 'M' in str(float_str):
            stock_float = float(float_str.replace('M', ''))
        elif 'B' in str(float_str):
            stock_float = float(float_str.replace('B', '')) * 1000

        short_str = f.get('Short Float', 'N/A')
        short_pct = None
        if '%' in str(short_str):
            short_pct = float(short_str.replace('%', ''))

        vol_str = f.get('Avg Volume', 'N/A')
        avg_vol = _parse_volume_str(vol_str)

        return {'float': stock_float, 'short': short_pct, 'avg_volume': avg_vol}
    except Exception as e:
        print(f"  [{ticker}] fundamentals failed: {e}")
        return None


def _parse_volume_str(vol_str: str) -> Optional[float]:
    """Parse Finviz volume strings like '1.5M', '300K'."""
    s = str(vol_str).replace(',', '')
    if 'K' in s:
        return float(s.replace('K', '')) * 1_000
    if 'M' in s:
        return float(s.replace('M', '')) * 1_000_000
    if 'B' in s:
        return float(s.replace('B', '')) * 1_000_000_000
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def get_ticker_news(ticker: str, max_items: int = 3) -> List[Dict]:
    """Fetch recent news headlines for a ticker from Finviz."""
    if not FINVIZ_AVAILABLE:
        return []
    try:
        stock = FinvizQuote(ticker)
        news_df = stock.ticker_news()
        if news_df is None or news_df.empty:
            return []
        items = []
        for _, row in news_df.head(max_items).iterrows():
            items.append({
                'title': str(row.get('Title', '')),
                'link': str(row.get('Link', '')),
                'source': str(row.get('Source', '')),
                'date': str(row.get('Date', '')),
            })
        return items
    except Exception as e:
        print(f"  [{ticker}] news fetch failed: {e}")
        return []


# ── Alpaca helpers (RVol) ────────────────────────────────────────────────────

def _alpaca_get(path: str) -> dict:
    """Make an authenticated GET request to Alpaca data API."""
    url = f"{ALPACA_DATA_URL}{path}"
    resp = requests.get(url, headers={
        'APCA-API-KEY-ID': ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
        'accept': 'application/json',
    }, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _fetch_alpaca_bars(symbol: str, start_iso: str, end_iso: str) -> List[dict]:
    """Fetch 5-min bars from Alpaca with pagination."""
    all_bars: List[dict] = []
    page_token = None
    while True:
        path = (f"/stocks/{symbol}/bars"
                f"?timeframe=5Min&start={start_iso}&end={end_iso}"
                f"&limit=10000&feed=sip")
        if page_token:
            path += f"&page_token={page_token}"
        data = _alpaca_get(path)
        all_bars.extend(data.get('bars') or [])
        page_token = data.get('next_page_token')
        if not page_token:
            break
    return all_bars


def calculate_rvol_at_time(
    ticker: str,
    entry_time: datetime,
    lookback_sessions: int = RVOL_LOOKBACK_SESSIONS,
) -> float:
    """
    Calculate Relative Volume at Time using Alpaca 5-min bars.

    Treats the full extended day (4 AM – 8 PM ET) as one continuous session.
    RVol = cumulative volume from 4 AM to *entry_time*
           / historical average of the same cumulative window.
    """
    if not ALPACA_KEY or not ALPACA_SECRET:
        print(f"  [{ticker}] Alpaca credentials not configured, skipping RVol")
        return 0.0

    entry_et = entry_time
    if entry_et.tzinfo is None:
        entry_et = entry_et.replace(tzinfo=ET)
    else:
        entry_et = entry_et.astimezone(ET)

    entry_date = entry_et.date()
    entry_tod = entry_et.time()

    # Fetch enough history: lookback + 10 extra days for weekends/holidays
    start_date = entry_date - timedelta(days=lookback_sessions * 2 + 10)
    start_iso = f"{start_date}T08:00:00Z"  # 4 AM ET ≈ 08/09 UTC
    end_iso = f"{entry_date}T23:59:59Z"

    try:
        bars = _fetch_alpaca_bars(ticker, start_iso, end_iso)
    except Exception as e:
        print(f"  [{ticker}] Alpaca bars fetch failed: {e}")
        return 0.0

    if not bars:
        return 0.0

    # Group by session date, keep bars within 4 AM – 8 PM ET
    sessions: Dict[date, List[dict]] = defaultdict(list)
    for b in bars:
        ts = datetime.fromisoformat(b['t'].replace('Z', '+00:00')).astimezone(ET)
        t = ts.time()
        if PREMARKET_START <= t < AFTERHOURS_END:
            sessions[ts.date()].append({'time': t, 'volume': b['v']})

    if entry_date not in sessions:
        return 0.0

    # Cumulative volume from 4 AM to entry_tod (inclusive) for each session
    def cum_vol(bar_list: List[dict]) -> int:
        return sum(b['volume'] for b in bar_list if b['time'] <= entry_tod)

    hist_dates = sorted(d for d in sessions if d < entry_date)[-lookback_sessions:]
    if not hist_dates:
        return 0.0

    hist_vols = [cum_vol(sessions[d]) for d in hist_dates]
    expected = np.mean(hist_vols)
    current = cum_vol(sessions[entry_date])

    return current / expected if expected > 0 else 0.0


# ── Yahoo Finance helpers ────────────────────────────────────────────────────

def get_after_hours_price(ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Get after-hours price and the regular session close for today.
    Returns (ah_price, close_price).
    """
    try:
        t = yf.Ticker(ticker)
        data = t.history(period='5d', interval='1m', prepost=True)
        if data.empty:
            return None, None

        if data.index.tz is None:
            data.index = data.index.tz_localize("UTC").tz_convert(ET)
        else:
            data.index = data.index.tz_convert(ET)

        today = datetime.now(ET).date()

        today_reg = data[
            (data.index.date == today) &
            (data.index.time >= MARKET_OPEN) &
            (data.index.time < MARKET_CLOSE)
        ]
        close_price = float(today_reg['Close'].iloc[-1]) if not today_reg.empty else None

        today_ah = data[
            (data.index.date == today) &
            (data.index.time >= MARKET_CLOSE)
        ]
        ah_price = float(today_ah['Close'].iloc[-1]) if not today_ah.empty else None

        if ah_price is None:
            ah_price = float(data['Close'].iloc[-1])
        if close_price is None:
            daily = t.history(period='5d')
            if not daily.empty:
                close_price = float(daily['Close'].iloc[-1])

        return ah_price, close_price
    except Exception as e:
        print(f"  [{ticker}] AH price failed: {e}")
        return None, None


def get_premarket_price(ticker: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Get pre-market price and previous session close.
    Returns (pm_price, prev_close).
    """
    try:
        t = yf.Ticker(ticker)
        data = t.history(period='5d', interval='1m', prepost=True)
        if data.empty:
            return None, None

        if data.index.tz is None:
            data.index = data.index.tz_localize("UTC").tz_convert(ET)
        else:
            data.index = data.index.tz_convert(ET)

        today = datetime.now(ET).date()

        today_pm = data[
            (data.index.date == today) &
            (data.index.time >= PREMARKET_START) &
            (data.index.time < MARKET_OPEN)
        ]
        pm_price = float(today_pm['Close'].iloc[-1]) if not today_pm.empty else None

        prev_dates = sorted(set(d for d in data.index.date if d < today))
        prev_close = None
        if prev_dates:
            prev_date = prev_dates[-1]
            prev_reg = data[
                (data.index.date == prev_date) &
                (data.index.time >= MARKET_OPEN) &
                (data.index.time < MARKET_CLOSE)
            ]
            if not prev_reg.empty:
                prev_close = float(prev_reg['Close'].iloc[-1])

        if prev_close is None:
            daily = t.history(period='10d')
            if not daily.empty and len(daily) >= 2:
                prev_close = float(daily['Close'].iloc[-2])
            elif not daily.empty:
                prev_close = float(daily['Close'].iloc[-1])

        return pm_price, prev_close
    except Exception as e:
        print(f"  [{ticker}] PM price failed: {e}")
        return None, None


def calculate_technicals(ticker: str, ref_price: float) -> Optional[Dict]:
    """Calculate 52W high distance and ATR multiple from daily data."""
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period='1y')
        if hist.empty or len(hist) < 14:
            return None

        high_52w = float(hist['High'].max())
        if high_52w == 0:
            return None
        dist_52w_high = (ref_price - high_52w) / high_52w * 100

        sma50 = (float(hist['Close'].iloc[-50:].mean())
                 if len(hist) >= 50 else float(hist['Close'].mean()))

        high_low = hist['High'] - hist['Low']
        high_prev = (hist['High'] - hist['Close'].shift(1)).abs()
        low_prev = (hist['Low'] - hist['Close'].shift(1)).abs()
        tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])

        last_close = float(hist['Close'].iloc[-1])
        if sma50 == 0 or last_close == 0 or atr == 0:
            return None

        atr_pct = atr / last_close * 100
        price_dist_pct = (ref_price - sma50) / sma50 * 100
        atr_multiple = price_dist_pct / atr_pct

        return {
            'dist_52w_high': round(dist_52w_high, 2),
            'atr_multiple': round(atr_multiple, 2),
            'sma50': round(sma50, 2),
            'atr': round(atr, 2),
            'close': round(last_close, 2),
        }
    except Exception as e:
        print(f"  [{ticker}] technicals failed: {e}")
        return None


# ── Discord notification ─────────────────────────────────────────────────────

def send_discord_notification(
    scan_type: str,
    scan_date: str,
    tickers: List[Dict],
) -> None:
    """Send EP scan results to Discord via webhook."""
    if not tickers:
        content = f"**EP Scan - {scan_type} ({scan_date})**\nNo qualifying tickers found."
    else:
        lines = [
            f"**EP Scan - {scan_type} ({scan_date})**",
            f"Found {len(tickers)} qualifying ticker(s)",
            "",
        ]
        for t in tickers:
            float_str = f"{t['float']:.1f}M" if t.get('float') else 'N/A'
            short_str = f"{t['short']:.1f}%" if t.get('short') is not None else 'N/A'
            dist_str = (f"{t['dist_52w_high']:+.1f}%"
                        if t.get('dist_52w_high') is not None else 'N/A')
            atr_str = (f"{t['atr_multiple']:.1f}x"
                       if t.get('atr_multiple') is not None else 'N/A')

            chg_key = 'ah_chg_pct' if 'ah_chg_pct' in t else 'pm_chg_pct'
            chg_label = 'AH CHG' if 'ah_chg_pct' in t else 'BM CHG'
            chg_val = t.get(chg_key)
            chg_str = f"{chg_val:+.1f}%" if chg_val is not None else 'N/A'

            news_items = t.get('news', [])
            news_str = news_items[0]['title'] if news_items else 'No news'

            lines.append(f"- **{t['ticker']}**")
            lines.append(f"{float_str} ({short_str})")
            lines.append(f"52W High: {dist_str}")
            lines.append(f"ATR: {atr_str}")
            lines.append(f"{chg_label}: {chg_str}")
            lines.append(f"News: {news_str}")
            lines.append("")

        content = '\n'.join(lines)

    if len(content) > 1990:
        content = content[:1987] + '...'

    if not DISCORD_WEBHOOK_URL:
        print("  Discord notification skipped: DISCORD_EP_SCAN_WEBHOOK_URL not set")
        return

    try:
        resp = requests.post(
            DISCORD_WEBHOOK_URL,
            json={'content': content},
            timeout=10,
        )
        if resp.status_code >= 300:
            print(f"  Discord notification failed ({resp.status_code}): {resp.text[:300]}")
        else:
            print(f"  Discord notification sent (HTTP {resp.status_code})")
    except Exception as e:
        print(f"  Discord notification failed: {e}")


# ── JSON export helper ───────────────────────────────────────────────────────

def export_scan_results(
    results: List[Dict],
    scan_type: str,
    output_filename: str,
) -> Path:
    """Write scan results to docs/data/<output_filename>."""
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    output = {
        'timestamp': datetime.now().isoformat(),
        'scan_date': datetime.now(ET).strftime('%Y-%m-%d'),
        'scan_type': scan_type,
        'count': len(results),
        'tickers': results,
    }

    out_path = DOCS_DATA_DIR / output_filename
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f"\n-> Exported {len(results)} tickers to {out_path}")
    return out_path
