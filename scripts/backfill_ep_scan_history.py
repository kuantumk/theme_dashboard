"""Backfill EP scan history JSON files from committed snapshot history.

The EP scanners write one current snapshot per run, and the dashboard reads a
separate *_history.json file for time travel. If older workflow runs committed
only the current snapshot, this script reconstructs the missing history file
from Git's prior versions of that snapshot.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Dict, Iterable, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCAN_FILES = (
    "docs/data/ep_scan_afternoon.json",
    "docs/data/ep_scan_morning.json",
)


def _git(args: Iterable[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout


def _history_path(scan_path: Path) -> Path:
    return scan_path.with_name(f"{scan_path.stem}_history.json")


def _normalize_snapshot(snapshot: object) -> Optional[Dict]:
    if not isinstance(snapshot, dict):
        return None

    report_date = snapshot.get("report_date") or snapshot.get("scan_date")
    if not report_date:
        return None

    normalized = dict(snapshot)
    normalized["report_date"] = report_date
    return normalized


def _load_json(text: str) -> Optional[Dict]:
    try:
        return _normalize_snapshot(json.loads(text))
    except json.JSONDecodeError:
        return None


def build_history(scan_file: str, limit: int) -> list[Dict]:
    scan_path = Path(scan_file)
    by_date: dict[str, Dict] = {}

    commits = [
        line.strip()
        for line in _git(["log", "--format=%H", "--", scan_file]).splitlines()
        if line.strip()
    ]
    for commit in commits:
        snapshot = _load_json(_git(["show", f"{commit}:{scan_file}"]))
        if snapshot is None:
            continue
        by_date.setdefault(snapshot["report_date"], snapshot)

    current_path = PROJECT_ROOT / scan_path
    if current_path.exists():
        current = _load_json(current_path.read_text(encoding="utf-8"))
        if current is not None:
            by_date[current["report_date"]] = current

    return sorted(
        by_date.values(),
        key=lambda item: item.get("report_date", ""),
        reverse=True,
    )[:limit]


def write_history(scan_file: str, limit: int) -> Path:
    scan_path = Path(scan_file)
    history = build_history(scan_file, limit)
    out_path = PROJECT_ROOT / _history_path(scan_path)
    out_path.write_text(json.dumps(history, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} ({len(history)} sessions)")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill EP scan *_history.json files from Git history.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=60,
        help="Maximum snapshots per history file.",
    )
    parser.add_argument(
        "scan_files",
        nargs="*",
        default=DEFAULT_SCAN_FILES,
        help="Current EP snapshot files to backfill.",
    )
    args = parser.parse_args()

    for scan_file in args.scan_files:
        write_history(scan_file, args.limit)


if __name__ == "__main__":
    main()
