"""
EP Scanner — Afternoon (AMC) Earnings

Scans for stocks reporting earnings Today After Market Close that meet:
  - Short Float > 10%
  - Average Volume > 1M
  - After-hours price >= today's close

Schedule: 2:00 PM Pacific daily (5:00 PM ET — 1 hour into after-hours).
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Allow running as `python src/reporting/ep_scan_afternoon.py` from repo root
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(_ROOT / ".env")

from src.reporting.ep_scan_common import (  # noqa: E402
    scan_finviz_tickers,
    get_fundamentals,
    get_ticker_news,
    get_after_hours_price,
    calculate_technicals,
    calculate_rvol_at_time,
    send_discord_notification,
    export_scan_results,
    ET,
)


def run_afternoon_scan() -> list:
    """Run the afternoon EP scan for AMC stocks."""
    print("=" * 60)
    print("EP SCAN — AFTERNOON (AMC)")
    print("=" * 60)

    tickers = scan_finviz_tickers('Today After Market Close')
    print(f"  Finviz found {len(tickers)} tickers matching AMC + Short>10% + Vol>1M")

    if not tickers:
        print("  No qualifying tickers from screener.")
        return []

    now_et = datetime.now(ET)
    results = []

    for ticker in tickers:
        print(f"  Processing {ticker}...")
        time.sleep(0.5)

        # 1. After-hours price check: AH price >= close
        ah_price, close_price = get_after_hours_price(ticker)
        if ah_price is None or close_price is None:
            print(f"    Skip {ticker}: no AH/close price")
            continue
        if ah_price < close_price:
            print(f"    Skip {ticker}: AH {ah_price:.2f} < close {close_price:.2f}")
            continue

        time.sleep(0.3)

        # 2. Fundamentals for display values
        fundamentals = get_fundamentals(ticker)
        if not fundamentals or fundamentals['float'] is None:
            print(f"    Skip {ticker}: missing fundamental data")
            continue

        time.sleep(0.3)

        # 3. Technicals
        technicals = calculate_technicals(ticker, ah_price)
        if not technicals:
            print(f"    Skip {ticker}: technicals failed")
            continue

        # 4. RVol at time (using Alpaca)
        rvol = calculate_rvol_at_time(ticker, now_et)

        time.sleep(0.3)

        # 5. News
        news = get_ticker_news(ticker)

        ah_chg_pct = (ah_price - close_price) / close_price * 100

        result = {
            'ticker': ticker,
            'float': fundamentals['float'],
            'short': fundamentals['short'],
            'ah_price': round(ah_price, 2),
            'close': close_price,
            'ah_chg_pct': round(ah_chg_pct, 2),
            'dist_52w_high': technicals['dist_52w_high'],
            'atr_multiple': technicals['atr_multiple'],
            'sma50': technicals['sma50'],
            'atr': technicals['atr'],
            'rvol': round(rvol, 2),
            'news': news,
        }
        results.append(result)

        print(f"    OK: float={fundamentals['float']}M, short={fundamentals['short']}%, "
              f"AH={ah_price:.2f} ({ah_chg_pct:+.1f}%), RVol={rvol:.1f}x")

    results.sort(key=lambda x: x['float'] if x['float'] is not None else float('inf'))
    return results


def main():
    results = run_afternoon_scan()
    scan_date = datetime.now(ET).strftime('%Y-%m-%d')

    export_scan_results(results, 'afternoon', 'ep_scan_afternoon.json')
    send_discord_notification('Afternoon Earnings', scan_date, results)

    print(f"\nDone. {len(results)} tickers exported.")


if __name__ == '__main__':
    main()
