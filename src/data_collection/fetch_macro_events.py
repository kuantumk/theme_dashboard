"""
Fetch upcoming high-importance US macro events from Investing.com economic calendar.

Uses investpy to retrieve events and outputs them in the format expected by the
dashboard (DD/MM/YYYY dates, HH:MM times in US Eastern).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional


# Eastern Time zone for investpy (EST = GMT-5, but investpy returns correct
# "wall clock" ET values with this setting regardless of DST)
INVESTPY_TIMEZONE = "GMT -5:00"

# Only keep events whose name contains one of these keywords (case-insensitive).
# These are the releases that consistently move the S&P and set the macro tone.
MARKET_MOVING_KEYWORDS = [
    # Inflation
    "CPI",
    "PPI",
    "PCE Price",
    # Employment
    "Nonfarm Payrolls",
    "Unemployment Rate",
    "Average Hourly Earnings",
    "ADP Nonfarm",
    "Initial Jobless Claims",
    # Fed
    "Fed Interest Rate",
    "FOMC Interest Rate",
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


def _is_market_moving(event_name: str) -> bool:
    """Return True if the event matches one of the curated market-moving keywords."""
    lower = event_name.lower()
    return any(kw.lower() in lower for kw in MARKET_MOVING_KEYWORDS)


def fetch_macro_events(days_ahead: int = 30) -> Optional[List[Dict[str, str]]]:
    """
    Fetch upcoming US high-importance macro events from Investing.com.

    Args:
        days_ahead: Number of days ahead to fetch events for.

    Returns:
        List of event dicts with 'date' (DD/MM/YYYY), 'time' (HH:MM ET),
        and 'event' keys, sorted chronologically. Returns None on failure.
    """
    try:
        import investpy
    except ImportError:
        print("  Warning: investpy not installed. Run: pip install investpy")
        return None

    today = datetime.now()
    end_date = today + timedelta(days=days_ahead)

    from_date = today.strftime("%d/%m/%Y")
    to_date = end_date.strftime("%d/%m/%Y")

    print(f"  Fetching macro events from Investing.com "
          f"({today.strftime('%b %d')} – {end_date.strftime('%b %d')})...")

    try:
        df = investpy.economic_calendar(
            from_date=from_date,
            to_date=to_date,
            countries=["united states"],
            importances=["high"],
            time_zone=INVESTPY_TIMEZONE,
        )
    except Exception as e:
        print(f"  Error fetching economic calendar: {e}")
        return None

    if df.empty:
        print("  No high-importance US events found")
        return None

    # Filter out "All Day" events and rows without a valid time
    df = df[df["time"].str.match(r"^\d{1,2}:\d{2}$", na=False)].copy()

    if df.empty:
        print("  No timed events found after filtering")
        return None

    # Build output list — only keep curated market-moving events
    events = []
    seen = set()

    for _, row in df.iterrows():
        date_str = row["date"]     # already DD/MM/YYYY
        time_str = row["time"]     # HH:MM in ET
        event_name = row["event"].strip()

        # Skip events that aren't in our curated whitelist
        if not _is_market_moving(event_name):
            continue

        # Normalize time to zero-padded HH:MM
        parts = time_str.split(":")
        time_str = f"{int(parts[0]):02d}:{parts[1]}"

        # Deduplicate
        key = f"{date_str}|{time_str}|{event_name}"
        if key in seen:
            continue
        seen.add(key)

        events.append({
            "date": date_str,
            "time": time_str,
            "event": event_name,
        })

    # Sort by date (YYYY/MM/DD) then time
    events.sort(key=lambda e: (
        e["date"].split("/")[::-1],  # [YYYY, MM, DD]
        e["time"],
    ))

    print(f"  Found {len(events)} market-moving US events")
    return events


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
