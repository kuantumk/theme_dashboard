"""Prepare batched audit input for the periodic Claude Opus theme audit.

Scans data/ticker_themes.json for tickers that have gone stale — i.e., not in
today's screened pool and not validated/audited within the last 30 days — and
writes per-batch JSON payloads to data/audit/<YYYY-MM-DD>/batch_NNN.json.

The routine that consumes these batches reasons through them with Opus and
writes decisions_NNN.json files alongside; apply_theme_audit.py then merges
high-confidence decisions back into ticker_themes.json.

Pure stdlib for the audit logic itself. ensure_company_profiles may hit
yfinance/Finviz to fill cache misses, but reuses the 90-day profile cache,
so most stale tickers are free.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Set

from config.settings import (
    CONFIG,
    DATA_DIR,
    SCREENING_OUTPUT_DIR,
    THEME_REVIEW_STATE_FILE,
)
from src.themes.company_profiles import ensure_company_profiles
from src.themes.tag_new_tickers import get_existing_theme_taxonomy
from src.themes.theme_registry import load_ticker_themes


THEME_CFG = CONFIG.get("themes", {})
DEFAULT_BATCH_SIZE = THEME_CFG.get("audit_batch_size", 50)
DEFAULT_STALE_DAYS = THEME_CFG.get("audit_stale_days", 30)
THEME_AUDIT_STATE_FILE = DATA_DIR / "theme_audit_state.json"
AUDIT_DIR = DATA_DIR / "audit"

# Generic placeholders that the audit must never propose as a target theme.
GENERIC_THEMES = {
    "Singleton",
    "Uncategorized",
    "Individual Episodic Pivots / Singletons",
}


def load_state(path: Path) -> Dict[str, Dict[str, object]]:
    """Read a state JSON file ({TICKER: {field: value}}); return {} if missing."""
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        return {}
    return {
        str(ticker).strip().upper(): entry
        for ticker, entry in raw.items()
        if isinstance(entry, dict) and str(ticker).strip()
    }


def parse_iso(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def latest_screened_pool() -> Set[str]:
    """Return the ticker set from the most recent _union_*.txt; empty if none."""
    consolidated = SCREENING_OUTPUT_DIR / "consolidated"
    if not consolidated.exists():
        return set()

    pattern = re.compile(r"_union_(\d{8})\.txt$")
    candidates: List[tuple[datetime, Path]] = []
    for path in consolidated.glob("_union_*.txt"):
        match = pattern.search(path.name)
        if not match:
            continue
        try:
            stamp = datetime.strptime(match.group(1), "%m%d%Y")
        except ValueError:
            continue
        candidates.append((stamp, path))

    if not candidates:
        return set()

    _, latest = max(candidates, key=lambda item: item[0])
    pool: Set[str] = set()
    with latest.open(encoding="utf-8") as handle:
        for line in handle:
            ticker = line.strip().upper()
            if ticker:
                pool.add(ticker)
    return pool


def select_stale_tickers(
    ticker_themes: Mapping[str, List[str]],
    review_state: Mapping[str, Mapping[str, object]],
    audit_state: Mapping[str, Mapping[str, object]],
    screened_pool: Set[str],
    stale_days: int,
    *,
    ignore_stale_filter: bool = False,
) -> List[str]:
    """A ticker is stale iff: (not in screened pool) AND (no recent validate/audit)."""
    cutoff = datetime.now() - timedelta(days=stale_days)
    stale: List[str] = []
    for ticker in sorted(ticker_themes.keys()):
        if ticker in screened_pool:
            continue
        if ignore_stale_filter:
            stale.append(ticker)
            continue
        review_ts = parse_iso(review_state.get(ticker, {}).get("last_validated_at"))
        audit_ts = parse_iso(audit_state.get(ticker, {}).get("last_audited_at"))
        timestamps = [t for t in (review_ts, audit_ts) if t is not None]
        most_recent = max(timestamps) if timestamps else None
        if most_recent is None or most_recent < cutoff:
            stale.append(ticker)
    return stale


def build_taxonomy(ticker_themes: Mapping[str, List[str]]) -> List[str]:
    """Valid-themes list for the prompt: existing assignments minus generic placeholders."""
    existing = set(get_existing_theme_taxonomy(ticker_themes)) - GENERIC_THEMES
    return sorted(existing)


def build_batch_payload(
    batch_id: str,
    audit_date: str,
    valid_themes: List[str],
    tickers: List[str],
    ticker_themes: Mapping[str, List[str]],
    profiles: Mapping[str, Mapping[str, str]],
) -> Dict[str, object]:
    return {
        "batch_id": batch_id,
        "audit_date": audit_date,
        "valid_themes": valid_themes,
        "tickers": [
            {
                "ticker": ticker,
                "current_themes": list(ticker_themes.get(ticker, [])),
                "profile": dict(profiles.get(ticker, {})),
            }
            for ticker in tickers
        ],
    }


def write_batches(
    audit_date: str,
    valid_themes: List[str],
    stale_tickers: List[str],
    ticker_themes: Mapping[str, List[str]],
    profiles: Mapping[str, Mapping[str, str]],
    batch_size: int,
) -> List[Path]:
    audit_dir = AUDIT_DIR / audit_date
    audit_dir.mkdir(parents=True, exist_ok=True)

    paths: List[Path] = []
    for index in range(0, len(stale_tickers), batch_size):
        chunk = stale_tickers[index:index + batch_size]
        batch_num = index // batch_size + 1
        batch_id = f"{batch_num:03d}"
        payload = build_batch_payload(
            batch_id, audit_date, valid_themes, chunk, ticker_themes, profiles
        )
        path = audit_dir / f"batch_{batch_id}.json"
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        paths.append(path)
    return paths


def parse_explicit_tickers(raw: str) -> List[str]:
    return [t.strip().upper() for t in raw.split(",") if t.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap stale ticker count after filtering")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated explicit list (overrides stale filter)")
    parser.add_argument("--audit-date", type=str, default=None,
                        help="Override audit date (YYYY-MM-DD); default = today")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--stale-days", type=int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--ignore-stale-filter", action="store_true",
                        help="Audit every non-screened-pool ticker regardless of state")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit_date = args.audit_date or datetime.now().strftime("%Y-%m-%d")

    ticker_themes = load_ticker_themes()
    review_state = load_state(THEME_REVIEW_STATE_FILE)
    audit_state = load_state(THEME_AUDIT_STATE_FILE)

    print(f"Audit date: {audit_date}")
    print(f"Tickers in ticker_themes.json: {len(ticker_themes)}")
    print(f"Entries in theme_review_state.json: {len(review_state)}")
    print(f"Entries in theme_audit_state.json: {len(audit_state)}")

    if args.tickers:
        explicit = parse_explicit_tickers(args.tickers)
        candidates = [t for t in explicit if t in ticker_themes]
        missing = [t for t in explicit if t not in ticker_themes]
        if missing:
            print(f"  Skipping unknown ticker(s) (not in ticker_themes.json): {missing}")
    else:
        screened_pool = latest_screened_pool()
        print(f"Latest screened pool size: {len(screened_pool)}")
        candidates = select_stale_tickers(
            ticker_themes, review_state, audit_state,
            screened_pool, args.stale_days,
            ignore_stale_filter=args.ignore_stale_filter,
        )

    if args.limit is not None:
        candidates = candidates[: args.limit]

    print(f"Stale candidates after filtering: {len(candidates)}")
    if not candidates:
        print("Nothing to audit. Exiting.")
        return 0

    profiles = ensure_company_profiles(candidates)
    valid_themes = build_taxonomy(ticker_themes)

    paths = write_batches(
        audit_date, valid_themes, candidates, ticker_themes, profiles, args.batch_size
    )

    print(f"\nWrote {len(paths)} batch file(s) to {AUDIT_DIR / audit_date}:")
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
