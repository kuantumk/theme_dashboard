"""Explicit re-tag CLI for single tickers when a theme narrative shifts.

Tags committed to ``data/ticker_themes.json`` are git-locked. The auto-
validation loop is disabled so existing tickers never silently change.
When real life changes (a company announces an AI pivot, divests a segment,
etc.) use this tool to re-classify just that ticker. This is also the write
path the weekday audit routine uses for first-time classification of
untagged tickers — the caller (human or Claude) supplies the judgment.

Example::

    python -m src.themes.retag --ticker AMZN --reason "Add cloud after AWS split" \\
        --paths "Software & Internet / E-commerce" "AI / Data Center / Cloud & Hyperscalers"

Every run appends an entry to ``data/theme_review_state.json`` under
``manual_retags`` so the change is auditable.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.settings import THEME_REVIEW_STATE_FILE  # noqa: E402
from src.themes.theme_registry import load_ticker_themes, save_ticker_themes  # noqa: E402
from src.themes.theme_taxonomy import load_taxonomy, validate_path  # noqa: E402


def _validate(paths: List[str]) -> None:
    taxonomy = load_taxonomy()
    invalid = [p for p in paths if not validate_path(p, taxonomy)]
    if invalid:
        raise SystemExit(f"Invalid path(s) not in taxonomy: {invalid}")


def _append_audit_entry(ticker: str, old_paths: List[str], new_paths: List[str], reason: str) -> None:
    state_file = Path(THEME_REVIEW_STATE_FILE)
    state = {}
    if state_file.exists():
        try:
            with open(state_file, encoding="utf-8") as f:
                state = json.load(f)
        except json.JSONDecodeError:
            state = {}
    manual_log = state.setdefault("manual_retags", [])
    manual_log.append({
        "ticker": ticker,
        "old_paths": old_paths,
        "new_paths": new_paths,
        "reason": reason,
        "at": datetime.now().isoformat(timespec="seconds"),
    })
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-tag a single ticker against the canonical taxonomy."
    )
    parser.add_argument("--ticker", required=True, help="Ticker symbol (uppercased)")
    parser.add_argument(
        "--reason",
        required=True,
        help="Why this re-tag is happening (logged for audit)",
    )
    parser.add_argument(
        "--paths",
        nargs="+",
        required=True,
        help=(
            "Canonical taxonomy paths to set. "
            'Provide 1-3 paths, e.g. --paths "AI / Data Center / Memory"'
        ),
    )
    args = parser.parse_args()

    ticker = args.ticker.strip().upper()
    if not ticker:
        raise SystemExit("--ticker is required")

    ticker_themes = load_ticker_themes()
    old_paths = list(ticker_themes.get(ticker, []))

    new_paths = list(args.paths)[:3]
    _validate(new_paths)

    ticker_themes[ticker] = new_paths
    save_ticker_themes(ticker_themes)
    print(f"Saved {ticker}: {old_paths} -> {new_paths}")

    _append_audit_entry(ticker, old_paths, new_paths, args.reason)
    print(f"Audit entry appended to {THEME_REVIEW_STATE_FILE}")


if __name__ == "__main__":
    main()
