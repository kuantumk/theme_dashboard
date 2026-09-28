"""
EP Scanner Export — Earnings Pivot (AMC) Scanner

Scans for stocks reporting earnings After Market Close (today OR yesterday AMC),
computes technical metrics using Yahoo Finance (including after-hours price),
and exports the results to docs/data/ep_scan.json for the dashboard.

Schedule: Run at ~1:30 PM Pacific daily (after-hours price has stabilized).
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import pytz

# Output path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Both Finviz helpers come from the live EP scans, which carry the fixes for
# Finviz's 2026 redesigns: the shared fundamentals lookup (finvizfinance's
# ticker_fundament() raises on every ticker since late June) and the ticker
# repair view (the screener doubles each ticker's first letter since July 15).
from src.reporting.ep_scan_common import (  # noqa: E402
    FINVIZ_AVAILABLE,
    get_fundamentals,
    make_ticker_repair_view,
)

if FINVIZ_AVAILABLE:
    from finvizfinance.screener.overview import Overview
    _EarningsOverview = make_ticker_repair_view(Overview)

DOCS_DATA_DIR = PROJECT_ROOT / "docs" / "data"

EASTERN = pytz.timezone('US/Eastern')

# ── FINVIZ helpers ────────────────────────────────────────────────────────────

def get_earnings_tickers(schedule_filter: str) -> list:
    """Fetch tickers with earnings on the given finviz date filter."""
    if not FINVIZ_AVAILABLE:
        return []
    try:
        overview = _EarningsOverview()
        overview.set_filter(filters_dict={'Earnings Date': schedule_filter})
        df = overview.screener_view()
        if df is None or df.empty:
            return []
        return df['Ticker'].tolist()
    except Exception as e:
        print(f"  Warning: finviz earnings fetch failed ({schedule_filter}): {e}")
        return []


# ── Yahoo Finance helpers ─────────────────────────────────────────────────────

def get_after_hours_price(ticker: str) -> float | None:
    """
    Get the latest after-hours (post-market) price from Yahoo Finance.
    Falls back to the regular-session close if post-market isn't available.
    """
    try:
        t = yf.Ticker(ticker)
        # Fetch intraday 1m data including pre/post market
        data = t.history(period='2d', interval='1m', prepost=True)
        if data.empty:
            return None

        eastern = pytz.timezone('US/Eastern')
        # Latest known price
        latest_price = float(data['Close'].iloc[-1])

        # Determine if the latest bar is in after-hours territory
        latest_ts = data.index[-1]
        if latest_ts.tzinfo is None:
            latest_ts = eastern.localize(latest_ts)
        else:
            latest_ts = latest_ts.astimezone(eastern)

        hour = latest_ts.hour
        # After-hours: 16:00 – 20:00 ET
        if hour >= 16 or hour < 4:
            return latest_price

        # Market is open — return the current price anyway (best effort)
        return latest_price

    except Exception as e:
        print(f"  Warning [{ticker}]: after-hours price fetch failed: {e}")
        return None


def calculate_technicals(ticker: str, ah_price: float) -> dict | None:
    """
    Calculate 52-week high distance and ATR-normalised distance from 50 SMA.
    Uses only regular-session (daily) OHLCV for the indicators.
    """
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period='1y')  # ~252 trading days

        if hist.empty or len(hist) < 14:
            return None

        # 52-week high (from daily OHLCV)
        high_52w = float(hist['High'].max())
        if high_52w == 0:
            return None

        dist_52w_high = (ah_price - high_52w) / high_52w * 100  # negative means below high

        # 50-day SMA (exclude after-hours — we use the last 50 daily closes)
        if len(hist) < 50:
            sma50 = float(hist['Close'].mean())
        else:
            sma50 = float(hist['Close'].iloc[-50:].mean())

        # 14-day ATR on daily data
        high_low = hist['High'] - hist['Low']
        high_prev = (hist['High'] - hist['Close'].shift(1)).abs()
        low_prev = (hist['Low'] - hist['Close'].shift(1)).abs()
        tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])

        last_close = float(hist['Close'].iloc[-1])
        if sma50 == 0 or last_close == 0:
            return None

        atr_pct = atr / last_close * 100          # ATR as % of last close
        if atr_pct == 0:
            return None

        price_dist_pct = (ah_price - sma50) / sma50 * 100   # % above/below 50 SMA
        atr_multiple = price_dist_pct / atr_pct              # dist in ATR units

        return {
            'dist_52w_high': round(dist_52w_high, 2),
            'atr_multiple': round(atr_multiple, 2),
            'sma50': round(sma50, 2),
            'atr': round(atr, 2),
            'close': round(last_close, 2),
        }

    except Exception as e:
        print(f"  Warning [{ticker}]: technicals failed: {e}")
        return None


# ── Main scan ─────────────────────────────────────────────────────────────────

def run_ep_scan() -> list:
    """
    Run the full EP (Earnings Pivot) scan for AMC stocks.
    Checks both 'Today After Market Close' and 'Yesterday After Market Close'.
    Returns a list of dicts ready for JSON export.
    """
    print("Running EP scan (AMC)...")

    # Collect tickers from today AND yesterday AMC (some AH moves take time)
    tickers_today = get_earnings_tickers('Today After Market Close')
    tickers_yesterday = get_earnings_tickers('Yesterday After Market Close')

    all_tickers = list(dict.fromkeys(tickers_today + tickers_yesterday))  # dedup, preserve order
    print(f"  Found {len(tickers_today)} today-AMC + {len(tickers_yesterday)} yesterday-AMC = {len(all_tickers)} unique tickers")

    results = []
    for ticker in all_tickers:
        print(f"  Processing {ticker}...")
        time.sleep(0.5)  # be polite to APIs

        # 1. Get fundamentals
        fundamentals = get_fundamentals(ticker)
        if not fundamentals:
            continue
        if fundamentals['float'] is None or fundamentals['short'] is None:
            print(f"    Skip {ticker}: missing float/short data")
            continue
        
        avg_vol = fundamentals.get('avg_volume')
        if avg_vol is not None and avg_vol < 300000:
            print(f"    Skip {ticker}: volume too low ({avg_vol:,.0f} < 300k)")
            continue

        # 2. Get after-hours price
        ah_price = get_after_hours_price(ticker)
        if not ah_price:
            print(f"    Skip {ticker}: no after-hours price")
            continue

        time.sleep(0.5)

        # 3. Calculate technicals
        technicals = calculate_technicals(ticker, ah_price)
        if not technicals:
            print(f"    Skip {ticker}: technicals failed")
            continue

        close_price = technicals['close']
        ah_chg_pct = (ah_price - close_price) / close_price * 100 if close_price else None

        results.append({
            'ticker': ticker,
            'float': fundamentals['float'],
            'short': fundamentals['short'],
            'ah_chg_pct': round(ah_chg_pct, 2) if ah_chg_pct is not None else None,
            'dist_52w_high': technicals['dist_52w_high'],
            'atr_multiple': technicals['atr_multiple'],
            'sma50': technicals['sma50'],
            'atr': technicals['atr'],
        })
        vol_fmt = f"{fundamentals['avg_volume']/1000000:.2f}M" if fundamentals.get('avg_volume') else "N/A"
        print(f"    OK: float={fundamentals['float']}M, short={fundamentals['short']}%, vol={vol_fmt}, "
              f"AH={ah_price:.2f} ({ah_chg_pct:+.1f}%), dist52w={technicals['dist_52w_high']:.1f}%, "
              f"ATR×={technicals['atr_multiple']:.1f}")

    # Sort by float ASC (default dashboard sort)
    results.sort(key=lambda x: x['float'] if x['float'] is not None else float('inf'))
    return results


def export_ep_scan():
    """Export EP scan results to docs/data/ep_scan.json."""
    print("=" * 60)
    print("EP SCAN EXPORT")
    print("=" * 60)

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    results = run_ep_scan()

    output = {
        'timestamp': datetime.now().isoformat(),
        'scan_date': datetime.now(EASTERN).strftime('%Y-%m-%d'),
        'tickers': results,
        'count': len(results),
    }

    out_path = DOCS_DATA_DIR / 'ep_scan.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f"\n→ Exported {len(results)} tickers to {out_path}")
    print("=" * 60)
    return results


if __name__ == '__main__':
    export_ep_scan()
