"""
EP Scanner — Morning (BMO) Earnings

Scans for stocks reporting earnings Today Before Market Open that meet:
  - Short Float > 10%
  - Average Volume > 1M
  - Pre-market price >= previous session's close

Schedule: 5:45 AM Pacific daily (8:45 AM ET — near end of pre-market).
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(_ROOT / ".env")

from src.reporting.ep_scan_common import (  # noqa: E402
    scan_finviz_tickers,
    get_fundamentals,
    get_ticker_news,
    get_premarket_price,
    calculate_technicals,
    calculate_rvol_at_time,
    send_discord_notification,
    export_scan_results,
    ET,
)


def run_morning_scan() -> list:
    """Run the morning EP scan for BMO stocks."""
    print("=" * 60)
    print("EP SCAN — MORNING (BMO)")
    print("=" * 60)

    tickers = scan_finviz_tickers('Today Before Market Open')
    print(f"  Finviz found {len(tickers)} tickers matching BMO + Short>10% + Vol>1M")

    if not tickers:
        print("  No qualifying tickers from screener.")
        return []

    now_et = datetime.now(ET)
    results = []

    for ticker in tickers:
        print(f"  Processing {ticker}...")
        time.sleep(0.5)

        # 1. Pre-market price check: PM price >= prev close
        pm_price, prev_close = get_premarket_price(ticker)
        if pm_price is None or prev_close is None:
            print(f"    Skip {ticker}: no PM/prev close price")
            continue
        if pm_price < prev_close:
            print(f"    Skip {ticker}: PM {pm_price:.2f} < prev close {prev_close:.2f}")
            continue

        time.sleep(0.3)

        # 2. Fundamentals
        fundamentals = get_fundamentals(ticker)
        if not fundamentals or fundamentals['float'] is None:
            print(f"    Skip {ticker}: missing fundamental data")
            continue

        time.sleep(0.3)

        # 3. Technicals (using PM price as reference)
        technicals = calculate_technicals(ticker, pm_price)
        if not technicals:
            print(f"    Skip {ticker}: technicals failed")
            continue

        # 4. RVol at time (using Alpaca)
        rvol = calculate_rvol_at_time(ticker, now_et)

        time.sleep(0.3)

        # 5. News
        news = get_ticker_news(ticker)

        pm_chg_pct = (pm_price - prev_close) / prev_close * 100

        result = {
            'ticker': ticker,
            'float': fundamentals['float'],
            'short': fundamentals['short'],
            'pm_price': round(pm_price, 2),
            'prev_close': round(prev_close, 2),
            'pm_chg_pct': round(pm_chg_pct, 2),
            'dist_52w_high': technicals['dist_52w_high'],
            'atr_multiple': technicals['atr_multiple'],
            'sma50': technicals['sma50'],
            'atr': technicals['atr'],
            'rvol': round(rvol, 2),
            'news': news,
        }
        results.append(result)

        print(f"    OK: float={fundamentals['float']}M, short={fundamentals['short']}%, "
              f"PM={pm_price:.2f} ({pm_chg_pct:+.1f}%), RVol={rvol:.1f}x")

    results.sort(key=lambda x: x['float'] if x['float'] is not None else float('inf'))
    return results


def main():
    results = run_morning_scan()
    scan_date = datetime.now(ET).strftime('%Y-%m-%d')

    export_scan_results(results, 'morning', 'ep_scan_morning.json')
    send_discord_notification('Morning Earnings', scan_date, results)

    print(f"\nDone. {len(results)} tickers exported.")


if __name__ == '__main__':
    main()
