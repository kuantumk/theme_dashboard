"""
Market breadth readings from barchart.com (headless Chromium via Playwright).

- NCFD: % of Nasdaq stocks above their 5-day average
- MMFI: % of stocks above their 50-day moving average
- MMTW: % of stocks above their 20-day moving average
- MMTH: % of stocks above their 200-day moving average

Step 3 of the daily workflow scrapes once and writes ``BREADTH_FILE``
(``data/market_breadth_latest.json``). The daily report reads NCFD/MMFI from
it, and the dashboard export's ``update_breadth_history`` reuses it rather
than opening a second browser.
"""

import json
import time
from datetime import datetime, timedelta

from config.settings import BREADTH_FILE, CONFIG

BREADTH_KEYS = ('ncfd', 'mmfi', 'mmtw', 'mmth')
# The export runs right after the workflow, hours after step 3 on a slow day.
# A reading older than this (a standalone export the next morning) is
# scraped again rather than published as the session's.
MAX_READING_AGE = timedelta(hours=12)


def fetch_barchart_breadth():
    """Fetch NCFD, MMFI, MMTW and MMTH from barchart.com using Playwright (JS-rendered pages).

    Returns ``{key: value}`` for the readings found, or None when none were.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  Warning: Playwright not installed. Run: uv sync && uv run playwright install chromium")
        return None

    cfg = CONFIG["market_breadth"]
    result = {}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers({'User-Agent': cfg["user_agent"]})

            for key in BREADTH_KEYS:
                try:
                    page.goto(cfg[f"{key}_url"], timeout=60000, wait_until='domcontentloaded')
                    page.wait_for_selector('span.last-change', timeout=20000)
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


def get_market_breadth():
    """Scrape the four readings once: ``{ncfd, mmfi, mmtw, mmth, timestamp}`` (None where missed)."""
    found = fetch_barchart_breadth() or {}
    breadth = {key: found.get(key) for key in BREADTH_KEYS}
    breadth['timestamp'] = datetime.now().isoformat()
    return breadth


def breadth_reading_is_fresh(reading, now=None):
    """True when a step-3 reading carries every key and is at most MAX_READING_AGE old."""
    if any(reading.get(key) is None for key in BREADTH_KEYS):
        return False
    try:
        taken = datetime.fromisoformat(reading['timestamp'])
    except (KeyError, TypeError, ValueError):
        return False
    return timedelta(0) <= (now or datetime.now()) - taken <= MAX_READING_AGE


def write_market_breadth(breadth, path=BREADTH_FILE):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        json.dump(breadth, f)


if __name__ == '__main__':
    reading = get_market_breadth()
    write_market_breadth(reading)
    print(json.dumps(reading, indent=2))
