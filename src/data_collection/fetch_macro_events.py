"""
Fetch upcoming high-importance US macro events from the Forex Factory JSON feed.

Replaces the previous investpy/Investing.com backend which:
  - Used an internal AJAX endpoint not designed for programmatic access
  - Returned 403 from datacenter IPs (GitHub Actions runners)
  - Required fragile HTML/regex parsing

Forex Factory feed (https://nfs.faireconomy.media/ff_calendar_thisweek.json):
  - Public, CDN-hosted JSON — no API key, no bot detection
  - Covers the current FF week (Sunday–Saturday)
  - Dates carry an ET offset so no timezone conversion is needed
  - High-impact US events only when filtered by country="USD" + impact="High"
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FF_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# Only keep events whose name contains one of these keywords (case-insensitive).
# These are the releases that consistently move the S&P and set the macro tone.
MARKET_MOVING_KEYWORDS = [
    # Inflation
    "CPI",
    "PPI",
    "PCE Price",
    # Employment
    "Nonfarm Payrolls",
    "Non-Farm Employment Change",
    "Unemployment Rate",
    "Average Hourly Earnings",
    "ADP Nonfarm",
    "ADP Non-Farm",
    "Initial Jobless Claims",
    "Unemployment Claims",
    # Fed
    "Fed Interest Rate",
    "Federal Funds Rate",
    "FOMC Interest Rate",
    "FOMC Statement",
    "FOMC Press Conference",
    # Growth
    "GDP",
    "Retail Sales",
    "Core Retail Sales",
    # Activity
    "ISM Manufacturing PMI",
    "ISM Manufacturing Prices",
    "ISM Non-Manufacturing PMI",
    "ISM Non-Manufacturing Prices",
    "ISM Services PMI",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_market_moving(event_name: str) -> bool:
    """Return True if the event matches one of the curated market-moving keywords."""
    lower = event_name.lower()
    return any(kw.lower() in lower for kw in MARKET_MOVING_KEYWORDS)


def _fetch_ff_raw() -> Optional[list]:
    """
    Fetch the raw Forex Factory calendar JSON with retry + rate-limit handling.

    Returns the parsed JSON list on success, None on failure.
    """
    headers = {"User-Agent": "macro-events-bot/1.0", "Accept": "application/json"}
    for attempt in range(3):
        try:
            resp = requests.get(_FF_CALENDAR_URL, headers=headers, timeout=20)
            if resp.status_code == 429:
                wait = 2 ** attempt * 5  # 5s, 10s, 20s
                print(f"  Rate limited by FF, retrying in {wait}s ...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"  Error fetching Forex Factory calendar: {e}")
            return None
    print("  Error: FF calendar rate limit exceeded after retries.")
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_macro_events(days_ahead: int = 30) -> Optional[List[Dict[str, str]]]:
    """
    Fetch upcoming US high-importance macro events from the Forex Factory feed.

    The FF feed covers the *current* week (Sunday–Saturday).  We filter to the
    window [today, today + days_ahead], keeping only US/High-impact events that
    match our market-moving keyword list.

    Args:
        days_ahead: Number of calendar days ahead to include.

    Returns:
        List of event dicts with 'date' (DD/MM/YYYY), 'time' (HH:MM ET),
        and 'event' keys, sorted chronologically.  Returns None on failure.
    """
    today = datetime.now(timezone.utc)
    end_date = today + timedelta(days=days_ahead)

    from_str = today.strftime("%Y-%m-%d")
    to_str = end_date.strftime("%Y-%m-%d")

    print(f"  Fetching macro events from Forex Factory "
          f"({today.strftime('%b %d')} – {end_date.strftime('%b %d')})...")

    data = _fetch_ff_raw()
    if data is None:
        return None

    events: List[Dict[str, str]] = []
    seen: set = set()

    for item in data:
        # USD events only
        if item.get("country") != "USD":
            continue
        # High-impact only
        if item.get("impact") != "High":
            continue

        title = (item.get("title") or "").strip()
        if not title or not _is_market_moving(title):
            continue

        date_raw = (item.get("date") or "").strip()
        if not date_raw:
            continue

        try:
            dt = datetime.fromisoformat(date_raw)
        except ValueError:
            continue

        # Filter to requested window
        day_str = dt.strftime("%Y-%m-%d")
        if day_str < from_str or day_str > to_str:
            continue

        # FF dates already carry an ET offset — use wall-clock time directly
        date_fmt = dt.strftime("%d/%m/%Y")   # DD/MM/YYYY (dashboard format)
        time_fmt = dt.strftime("%H:%M")       # HH:MM ET

        key = f"{date_fmt}|{time_fmt}|{title}"
        if key in seen:
            continue
        seen.add(key)

        events.append({
            "date": date_fmt,
            "time": time_fmt,
            "event": title,
        })

    # Sort chronologically: YYYY, MM, DD, then time
    events.sort(key=lambda e: (e["date"].split("/")[::-1], e["time"]))

    print(f"  Found {len(events)} market-moving US events")
    return events if events else None


def write_events_json(events: List[Dict[str, str]], output_path: Path) -> None:
    """Write events to JSON file."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)
    print(f"   → {output_path} ({len(events)} events)")


if __name__ == "__main__":
    from config.settings import DOCS_DATA_DIR

    events = fetch_macro_events()
    if events:
        output = DOCS_DATA_DIR / "events.json"
        write_events_json(events, output)
        print("\nGenerated events:")
        for ev in events:
            print(f"  {ev['date']} {ev['time']}  {ev['event']}")
    else:
        print("Failed to fetch macro events.")
