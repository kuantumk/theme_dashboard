"""Audit data/ticker_themes.json for tagging quality.

Mechanical checks that don't require AI judgment. Run periodically (weekly
is a reasonable cadence) and before merging taxonomy edits. The companion
skill ./.claude/skills/audit-theme-tags/SKILL.md wraps this with AI-judgment
passes (business-pivot detection, web-verified L2 selection).

Reads:
  - data/ticker_themes.json
  - config/theme_taxonomy.yaml (via src.themes.theme_taxonomy)

Exit code:
  0 -> no [BUG] findings (WARN/INFO may still be present)
  1 -> one or more [BUG] findings (mechanical defects that produce viz
       bugs or break downstream tooling)
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.themes.theme_taxonomy import (  # noqa: E402
    _children,
    load_taxonomy,
    split_path,
    validate_path,
)

TICKER_THEMES_FILE = ROOT / "data" / "ticker_themes.json"


def load_ticker_themes() -> Dict[str, List[str]]:
    with open(TICKER_THEMES_FILE, encoding="utf-8") as f:
        return json.load(f)


def check_bare_l1_with_children(
    themes: Dict[str, List[str]], tax: Dict
) -> List[Tuple[str, str, str]]:
    """Bare-L1 paths are only valid for L1s with no children. Returns
    (ticker, path, l1) for each violation."""
    findings = []
    for ticker, paths in themes.items():
        for p in paths:
            l1, l2, _ = split_path(p)
            if l2 is None and l1 in tax and _children(tax[l1]):
                findings.append((ticker, p, l1))
    return findings


def check_invalid_paths(
    themes: Dict[str, List[str]], tax: Dict
) -> List[Tuple[str, str]]:
    """Paths that fail validate_path (taxonomy may have changed under them)."""
    findings = []
    for ticker, paths in themes.items():
        for p in paths:
            if not validate_path(p, tax):
                findings.append((ticker, p))
    return findings


def check_empty_tags(themes: Dict[str, List[str]]) -> List[str]:
    return [t for t, paths in themes.items() if not paths]


def check_excessive_paths(themes: Dict[str, List[str]]) -> List[Tuple[str, int]]:
    """Taxonomy says 1-3 paths per ticker."""
    return [(t, len(paths)) for t, paths in themes.items() if len(paths) > 3]


def check_duplicate_paths(
    themes: Dict[str, List[str]],
) -> List[Tuple[str, List[str]]]:
    findings = []
    for ticker, paths in themes.items():
        seen = Counter(paths)
        dupes = [p for p, n in seen.items() if n > 1]
        if dupes:
            findings.append((ticker, dupes))
    return findings


def count_generic_only(themes: Dict[str, List[str]]) -> Dict[str, int]:
    """Count tickers whose only tag is Singleton or Uncategorized."""
    out = {"Singleton": 0, "Uncategorized": 0}
    for paths in themes.values():
        if len(paths) == 1 and paths[0] in out:
            out[paths[0]] += 1
    return out


def main() -> int:
    themes = load_ticker_themes()
    tax = load_taxonomy()
    print(f"Auditing {len(themes)} tickers in {TICKER_THEMES_FILE.name}")
    print("=" * 60)

    bug_count = 0
    warn_count = 0

    # BUG: bare-L1 with children
    bare_l1 = check_bare_l1_with_children(themes, tax)
    if bare_l1:
        bug_count += len(bare_l1)
        print(f"\n[BUG] Bare-L1 paths for L1s that have children: {len(bare_l1)}")
        by_l1: Dict[str, List[Tuple[str, str]]] = {}
        for ticker, path, l1 in bare_l1:
            by_l1.setdefault(l1, []).append((ticker, path))
        for l1, items in sorted(by_l1.items()):
            tickers = sorted({t for t, _ in items})
            child_l2s = sorted(_children(tax[l1]).keys())
            print(f"  {l1} (valid L2s: {child_l2s})")
            print(f"    Tickers: {tickers}")
            print(f"    Fix: pick an L2 per ticker, then run")
            print(
                f"      python -m src.themes.retag --ticker <T> "
                f'--reason "..." --paths "{l1} / <L2>"'
            )

    # BUG: invalid paths
    invalid = check_invalid_paths(themes, tax)
    if invalid:
        bug_count += len(invalid)
        print(f"\n[BUG] Paths that don't validate against taxonomy: {len(invalid)}")
        for ticker, path in invalid[:20]:
            print(f"  {ticker}: {path!r}")
        if len(invalid) > 20:
            print(f"  ... and {len(invalid) - 20} more")

    # WARN: empty tag list
    empty = check_empty_tags(themes)
    if empty:
        warn_count += len(empty)
        head = empty[:20]
        suffix = " ..." if len(empty) > 20 else ""
        print(f"\n[WARN] Tickers with empty tag list: {len(empty)}")
        print(f"  {head}{suffix}")

    # WARN: too many paths
    too_many = check_excessive_paths(themes)
    if too_many:
        warn_count += len(too_many)
        print(f"\n[WARN] Tickers with > 3 paths: {len(too_many)}")
        for ticker, count in too_many[:10]:
            print(f"  {ticker}: {count} paths -> {themes[ticker]}")

    # WARN: duplicate paths within ticker
    dupes = check_duplicate_paths(themes)
    if dupes:
        warn_count += len(dupes)
        print(f"\n[WARN] Tickers with duplicate paths: {len(dupes)}")
        for ticker, d in dupes[:10]:
            print(f"  {ticker}: {d}")

    # INFO: generic-only counts
    generic = count_generic_only(themes)
    print(
        f"\n[INFO] Generic-only tags: "
        f"Singleton={generic['Singleton']}, "
        f"Uncategorized={generic['Uncategorized']}"
    )

    print("\n" + "=" * 60)
    print(f"Summary: {bug_count} BUG finding(s), {warn_count} WARN finding(s)")
    return 1 if bug_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
