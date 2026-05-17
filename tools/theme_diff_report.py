"""Generate a markdown diff between pre-migration and post-migration ticker themes.

Reads:
  - data/ticker_themes.pre_migration.json (snapshot taken before migration)
  - data/ticker_themes.json (current/new tags)

Writes:
  - reports/theme_taxonomy_migration_<YYYY-MM-DD>.md
"""
from __future__ import annotations

import json
import random
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _depth(path: str) -> int:
    return len([p for p in path.split(" / ") if p.strip()])


def _l1(path: str) -> str:
    return path.split(" / ")[0].strip()


def _load(filename: str) -> dict:
    with open(ROOT / "data" / filename, encoding="utf-8") as f:
        return json.load(f)


def _summary_stats(themes: dict, label: str) -> list[str]:
    tickers = list(themes.keys())
    total_paths = sum(len(v) for v in themes.values())
    unique_paths = {p for v in themes.values() for p in v}
    paths_per = Counter(len(v) for v in themes.values())
    depths = Counter(_depth(p) for v in themes.values() for p in v)
    lines = [f"### {label}", ""]
    lines.append(f"- Tickers: **{len(tickers)}**")
    lines.append(f"- Distinct theme strings: **{len(unique_paths)}**")
    lines.append(f"- Total path assignments: **{total_paths}**")
    lines.append("- Paths per ticker:")
    for k in sorted(paths_per):
        lines.append(f"  - {k} path(s): {paths_per[k]} tickers")
    lines.append("- Path depth distribution:")
    for k in sorted(depths):
        label_d = "L1" if k == 1 else ("L1/L2" if k == 2 else "L1/L2/L3")
        lines.append(f"  - {label_d}: {depths[k]} assignments")
    lines.append("")
    return lines


def _l1_distribution_table(new_themes: dict) -> list[str]:
    l1_counts: Counter = Counter()
    for paths in new_themes.values():
        for p in paths:
            l1_counts[_l1(p)] += 1
    lines = ["### L1 narrative distribution (new taxonomy)", "", "| L1 | Tickers |", "|----|--------:|"]
    for l1, n in l1_counts.most_common():
        lines.append(f"| {l1} | {n} |")
    lines.append("")
    return lines


def _l1_reshuffle(old: dict, new: dict) -> list[str]:
    """Tickers whose L1 narrative changed (after accounting for flat-string
    legacy themes — we treat the first " / " segment of the old theme as L1)."""

    def old_l1s(themes: list[str]) -> set[str]:
        out: set[str] = set()
        for t in themes:
            head = t.split(" / ")[0].strip()
            # Map a few obvious legacy heads to new L1s for fair comparison
            mapping = {
                "Energy": "Oil & Gas / Clean Energy (split)",
                "Mining": "Metals & Mining",
                "Cryptocurrency": "Fintech & Crypto",
                "Financials": "Fintech & Crypto",
                "Financial Services": "Fintech & Crypto",
                "Fintech": "Fintech & Crypto",
                "Biotechnology": "Biotech",
                "Medical Devices": "MedTech",
                "Software": "Software & Internet",
                "Aerospace & Defense": "Defense & Aerospace",
                "EV & AV": "EV & Autonomous",
                "LiDAR & AV Tech": "EV & Autonomous",
                "Industrial": "Industrials",
                "Retail": "Retail (Multi-Category)",
                "Nuclear": "Nuclear",
            }
            out.add(mapping.get(head, head))
        return out

    rows: list[tuple[str, str, str]] = []
    for ticker in sorted(set(old) & set(new)):
        old_set = old_l1s(old[ticker])
        new_set = {_l1(p) for p in new[ticker]}
        if not (old_set & new_set) and old_set != {"Individual Episodic Pivots / Singletons"}:
            rows.append((ticker, " | ".join(old[ticker]), " | ".join(new[ticker])))

    lines = [
        "### L1 reshuffle — tickers whose narrative moved (capped at 60)",
        "",
        "| Ticker | Before | After |",
        "|--------|--------|-------|",
    ]
    for ticker, before, after in rows[:60]:
        lines.append(f"| {ticker} | {before} | {after} |")
    if len(rows) > 60:
        lines.append(f"\n_…and {len(rows) - 60} more_\n")
    lines.append("")
    return lines


def _granularity_gains(old: dict, new: dict) -> list[str]:
    """Tickers that escaped Singleton / Uncategorized / Meme into real themes."""
    legacy_buckets = {
        "Individual Episodic Pivots / Singletons",
        "Uncategorized",
        "Meme Stocks",
    }
    rows = []
    for ticker in sorted(set(old) & set(new)):
        before = old[ticker]
        after = new[ticker]
        if any(t in legacy_buckets for t in before) and not all(_l1(p) == "Singleton" for p in after):
            rows.append((ticker, " | ".join(before), " | ".join(after)))
    lines = [
        "### Granularity gains — tickers that escaped the catch-all buckets",
        "",
        f"_{len(rows)} tickers gained meaningful themes_",
        "",
        "| Ticker | Before | After |",
        "|--------|--------|-------|",
    ]
    for ticker, before, after in rows[:40]:
        lines.append(f"| {ticker} | {before} | {after} |")
    if len(rows) > 40:
        lines.append(f"\n_…and {len(rows) - 40} more_\n")
    lines.append("")
    return lines


def _spotlight(old: dict, new: dict) -> list[str]:
    """The BE/PUMP fix — show that clean-energy and oil-gas no longer share L1."""
    examples = {
        "Clean Energy (Fuel Cell)": ["BE", "FCEL", "BLDP", "PLUG"],
        "Clean Energy (Batteries)": ["AMPX", "MVST", "QS"],
        "Oil & Gas (Services)":     ["PUMP", "HAL", "SLB", "NOV"],
        "Oil & Gas (E&P)":          ["XOM", "CVX", "FANG", "EOG"],
        "Space cluster":            ["RKLB", "PL", "ASTS", "IRDM"],
        "AI Data Center / Memory":  ["SNDK", "MU", "STX", "WDC"],
    }
    lines = [
        "### Spotlight — the BE/PUMP fix",
        "",
        "Before the migration these tickers all shared the same 'Energy' node on VARS Viz. "
        "After the migration, fuel-cell and oilfield-services tickers belong to different L1 "
        "narratives ('Clean Energy' vs 'Oil & Gas') and connect to different hub nodes in the "
        "Cytoscape graph.",
        "",
        "| Cluster | Ticker | Before | After |",
        "|---------|--------|--------|-------|",
    ]
    for cluster, tickers in examples.items():
        for ticker in tickers:
            before = " | ".join(old.get(ticker, ["—"]))
            after = " | ".join(new.get(ticker, ["—"]))
            lines.append(f"| {cluster} | {ticker} | {before} | {after} |")
    lines.append("")
    return lines


def _random_diffs(old: dict, new: dict, n: int = 50) -> list[str]:
    rng = random.Random(42)
    tickers = sorted(set(old) & set(new))
    sample = rng.sample(tickers, min(n, len(tickers)))
    lines = [
        f"### {len(sample)} random ticker diffs",
        "",
        "| Ticker | Before | After |",
        "|--------|--------|-------|",
    ]
    for ticker in sample:
        lines.append(f"| {ticker} | {' | '.join(old[ticker])} | {' | '.join(new[ticker])} |")
    lines.append("")
    return lines


def main() -> None:
    old = _load("ticker_themes.pre_migration.json")
    new = _load("ticker_themes.json")

    out_path = ROOT / "reports" / f"theme_taxonomy_migration_{date.today().isoformat()}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append(f"# Theme Taxonomy Migration — {date.today().isoformat()}")
    lines.append("")
    lines.append(
        "Migration from flat LLM-emitted themes (consolidated at display time by "
        "`config/theme_groups.yaml`) to a canonical 3-level hierarchy "
        "(`config/theme_taxonomy.yaml`). Every ticker is now stored as one to "
        "three slash-delimited paths whose L1 is a coherent trading narrative."
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Summary statistics")
    lines.append("")
    lines += _summary_stats(old, "Before (pre-migration)")
    lines += _summary_stats(new, "After (canonical taxonomy)")
    lines += _l1_distribution_table(new)
    lines.append("---")
    lines.append("")
    lines += _spotlight(old, new)
    lines.append("---")
    lines.append("")
    lines += _granularity_gains(old, new)
    lines.append("---")
    lines.append("")
    lines += _l1_reshuffle(old, new)
    lines.append("---")
    lines.append("")
    lines += _random_diffs(old, new)

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
