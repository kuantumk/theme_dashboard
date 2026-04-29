"""Apply Claude Opus audit decisions to ticker_themes.json with safety gates.

Reads data/audit/<YYYY-MM-DD>/decisions_*.json files produced by the routine,
validates each decision, and partitions them into:

  applied   — high-confidence change merged into ticker_themes.json
  kept      — explicit "keep" decision (no mutation needed)
  deferred  — change but failed a safety gate (low confidence, invented theme,
              singleton/uncategorized target) — surfaces in audit_summary.md
              for manual review
  invalid   — malformed decision (missing fields, unknown action, etc.)
  skipped   — ticker was in a batch but no decision came back from the routine

Updates data/theme_audit_state.json with last_audited_at for every ticker that
appeared in this run's batches (so the staleness filter excludes them next month).

Pure stdlib. Safe to re-run; --dry-run produces audit_summary.md without mutation.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Set, Tuple

from config.settings import CONFIG, DATA_DIR
from src.themes.theme_registry import (
    load_ticker_themes,
    normalize_theme_list,
    save_ticker_themes,
)


THEME_CFG = CONFIG.get("themes", {})
DEFAULT_CONFIDENCE_THRESHOLD = THEME_CFG.get("audit_confidence_threshold", 0.85)
THEME_AUDIT_STATE_FILE = DATA_DIR / "theme_audit_state.json"
AUDIT_DIR = DATA_DIR / "audit"

GENERIC_THEMES = {
    "Singleton",
    "Uncategorized",
    "Individual Episodic Pivots / Singletons",
}
VALID_ACTIONS = {"keep", "change", "uncertain"}


def load_audit_state() -> Dict[str, Dict[str, object]]:
    if not THEME_AUDIT_STATE_FILE.exists():
        return {}
    with THEME_AUDIT_STATE_FILE.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        return {}
    return {
        str(ticker).strip().upper(): entry
        for ticker, entry in raw.items()
        if isinstance(entry, dict) and str(ticker).strip()
    }


def save_audit_state(state: Mapping[str, Mapping[str, object]]) -> None:
    THEME_AUDIT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with THEME_AUDIT_STATE_FILE.open("w", encoding="utf-8") as handle:
        json.dump(dict(state), handle, indent=2, sort_keys=True)
        handle.write("\n")


def load_json(path: Path) -> Dict[str, object]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def collect_batch_tickers(audit_dir: Path) -> Set[str]:
    """Union of tickers across all batch_*.json files in this audit run."""
    tickers: Set[str] = set()
    for path in sorted(audit_dir.glob("batch_*.json")):
        payload = load_json(path)
        for entry in payload.get("tickers", []):
            ticker = str(entry.get("ticker", "")).strip().upper()
            if ticker:
                tickers.add(ticker)
    return tickers


def collect_valid_themes(audit_dir: Path) -> Set[str]:
    """Union of valid_themes across all batch files (should be identical, but be safe)."""
    themes: Set[str] = set()
    for path in sorted(audit_dir.glob("batch_*.json")):
        payload = load_json(path)
        for theme in payload.get("valid_themes", []):
            cleaned = str(theme).strip()
            if cleaned:
                themes.add(cleaned)
    return themes


def collect_decisions(audit_dir: Path) -> Dict[str, Dict[str, object]]:
    """Merge decisions_*.json files into a single {ticker: decision} map."""
    merged: Dict[str, Dict[str, object]] = {}
    for path in sorted(audit_dir.glob("decisions_*.json")):
        payload = load_json(path)
        decisions = payload.get("decisions", payload)
        if not isinstance(decisions, dict):
            continue
        for ticker, decision in decisions.items():
            if not isinstance(decision, dict):
                continue
            clean_ticker = str(ticker).strip().upper()
            if not clean_ticker:
                continue
            merged[clean_ticker] = decision
    return merged


def classify_decision(
    ticker: str,
    decision: Mapping[str, object],
    current_themes: List[str],
    valid_themes: Set[str],
    confidence_threshold: float,
) -> Tuple[str, Dict[str, object]]:
    """Return (bucket, record). Bucket is one of: applied, kept, deferred, invalid."""
    action_raw = str(decision.get("action", "")).strip().lower()
    if action_raw not in VALID_ACTIONS:
        return "invalid", {"ticker": ticker, "reason": f"unknown action: {action_raw!r}",
                           "decision": dict(decision)}

    try:
        confidence = float(decision.get("confidence", 0.0))
    except (TypeError, ValueError):
        return "invalid", {"ticker": ticker, "reason": "confidence not a number",
                           "decision": dict(decision)}
    if not 0.0 <= confidence <= 1.0:
        return "invalid", {"ticker": ticker, "reason": f"confidence out of range: {confidence}",
                           "decision": dict(decision)}

    rationale = str(decision.get("rationale", "")).strip()

    if action_raw == "keep":
        return "kept", {"ticker": ticker, "themes": list(current_themes),
                        "confidence": confidence, "rationale": rationale}

    if action_raw == "uncertain":
        return "deferred", {"ticker": ticker, "from": list(current_themes),
                            "to": normalize_theme_list(decision.get("themes")),
                            "confidence": confidence, "rationale": rationale,
                            "reason": "uncertain"}

    proposed = normalize_theme_list(decision.get("themes"))
    if not proposed:
        return "invalid", {"ticker": ticker, "reason": "change action with empty themes",
                           "decision": dict(decision)}

    if confidence < confidence_threshold:
        return "deferred", {"ticker": ticker, "from": list(current_themes), "to": proposed,
                            "confidence": confidence, "rationale": rationale,
                            "reason": f"confidence below threshold ({confidence_threshold})"}

    if any(theme in GENERIC_THEMES for theme in proposed):
        return "deferred", {"ticker": ticker, "from": list(current_themes), "to": proposed,
                            "confidence": confidence, "rationale": rationale,
                            "reason": "downgrade to singleton/uncategorized requires manual review"}

    novel = [theme for theme in proposed if theme not in valid_themes]
    if novel:
        return "deferred", {"ticker": ticker, "from": list(current_themes), "to": proposed,
                            "confidence": confidence, "rationale": rationale,
                            "reason": f"proposes theme(s) not in taxonomy: {novel}"}

    if normalize_theme_list(current_themes) == proposed:
        return "kept", {"ticker": ticker, "themes": proposed,
                        "confidence": confidence, "rationale": rationale,
                        "note": "decision matched current themes"}

    return "applied", {"ticker": ticker, "from": list(current_themes), "to": proposed,
                       "confidence": confidence, "rationale": rationale}


def render_summary(
    audit_date: str,
    applied: List[Dict[str, object]],
    kept: List[Dict[str, object]],
    deferred: List[Dict[str, object]],
    invalid: List[Dict[str, object]],
    skipped: List[str],
    dry_run: bool,
) -> str:
    lines: List[str] = []
    lines.append(f"# Theme Audit — {audit_date}")
    if dry_run:
        lines.append("")
        lines.append("**DRY RUN — no files mutated.**")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Applied: {len(applied)}")
    lines.append(f"- Kept (no change): {len(kept)}")
    lines.append(f"- Deferred (manual review): {len(deferred)}")
    lines.append(f"- Invalid decisions: {len(invalid)}")
    lines.append(f"- Skipped (no decision returned): {len(skipped)}")
    lines.append("")

    if applied:
        lines.append("## Applied Changes")
        lines.append("")
        for entry in sorted(applied, key=lambda e: str(e["ticker"])):
            ticker = entry["ticker"]
            from_t = entry["from"] or ["(none)"]
            to_t = entry["to"]
            conf = entry["confidence"]
            lines.append(f"- **{ticker}** {from_t} → {to_t} (confidence {conf:.2f})")
            if entry.get("rationale"):
                lines.append(f"  - {entry['rationale']}")
        lines.append("")

    if deferred:
        lines.append("## Deferred — Manual Review")
        lines.append("")
        for entry in sorted(deferred, key=lambda e: str(e["ticker"])):
            ticker = entry["ticker"]
            from_t = entry.get("from", []) or ["(none)"]
            to_t = entry.get("to", []) or ["(none)"]
            conf = entry.get("confidence", 0.0)
            reason = entry.get("reason", "")
            lines.append(f"- **{ticker}** {from_t} → {to_t} (confidence {conf:.2f}) — _{reason}_")
            if entry.get("rationale"):
                lines.append(f"  - {entry['rationale']}")
        lines.append("")

    if invalid:
        lines.append("## Invalid Decisions")
        lines.append("")
        for entry in sorted(invalid, key=lambda e: str(e.get("ticker", ""))):
            lines.append(f"- **{entry.get('ticker')}** — {entry.get('reason')}")
        lines.append("")

    if skipped:
        lines.append("## Skipped (no decision from routine)")
        lines.append("")
        lines.append(", ".join(sorted(skipped)))
        lines.append("")

    return "\n".join(lines)


def apply_changes(
    ticker_themes: Dict[str, List[str]],
    applied: Iterable[Mapping[str, object]],
) -> Dict[str, List[str]]:
    updated = dict(ticker_themes)
    for entry in applied:
        ticker = str(entry["ticker"]).strip().upper()
        proposed = normalize_theme_list(entry.get("to"))
        if ticker and proposed:
            updated[ticker] = proposed
    return updated


def update_audit_state(
    audit_state: Dict[str, Dict[str, object]],
    audit_timestamp: str,
    decisions_by_ticker: Mapping[str, Mapping[str, object]],
    skipped: Iterable[str],
) -> Dict[str, Dict[str, object]]:
    updated = {ticker: dict(entry) for ticker, entry in audit_state.items()}
    for ticker, decision in decisions_by_ticker.items():
        action = str(decision.get("action", "")).strip().lower()
        if action not in VALID_ACTIONS:
            action = "invalid"
        try:
            confidence = float(decision.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        updated[ticker] = {
            "last_audited_at": audit_timestamp,
            "last_action": action,
            "last_confidence": round(confidence, 4),
        }
    for ticker in skipped:
        updated[ticker] = {
            "last_audited_at": audit_timestamp,
            "last_action": "skipped",
            "last_confidence": 0.0,
        }
    return updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-date", type=str, default=None,
                        help="Audit date directory (YYYY-MM-DD); default = today")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't mutate ticker_themes.json or theme_audit_state.json")
    parser.add_argument("--confidence-threshold", type=float,
                        default=DEFAULT_CONFIDENCE_THRESHOLD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audit_date = args.audit_date or datetime.now().strftime("%Y-%m-%d")
    audit_dir = AUDIT_DIR / audit_date

    if not audit_dir.exists():
        print(f"No audit directory at {audit_dir}; nothing to apply.")
        return 1

    batch_tickers = collect_batch_tickers(audit_dir)
    valid_themes = collect_valid_themes(audit_dir)
    decisions = collect_decisions(audit_dir)
    ticker_themes = load_ticker_themes()

    print(f"Audit date: {audit_date}")
    print(f"Tickers in batches: {len(batch_tickers)}")
    print(f"Decisions returned: {len(decisions)}")
    print(f"Confidence threshold: {args.confidence_threshold}")
    print(f"Dry run: {args.dry_run}")

    applied: List[Dict[str, object]] = []
    kept: List[Dict[str, object]] = []
    deferred: List[Dict[str, object]] = []
    invalid: List[Dict[str, object]] = []

    for ticker, decision in decisions.items():
        if ticker not in ticker_themes:
            invalid.append({"ticker": ticker, "reason": "ticker not in ticker_themes.json",
                            "decision": dict(decision)})
            continue
        bucket, record = classify_decision(
            ticker, decision, ticker_themes[ticker], valid_themes, args.confidence_threshold,
        )
        if bucket == "applied":
            applied.append(record)
        elif bucket == "kept":
            kept.append(record)
        elif bucket == "deferred":
            deferred.append(record)
        else:
            invalid.append(record)

    skipped = sorted(batch_tickers - set(decisions.keys()))

    summary_md = render_summary(audit_date, applied, kept, deferred, invalid, skipped,
                                args.dry_run)
    summary_path = audit_dir / "audit_summary.md"
    summary_path.write_text(summary_md, encoding="utf-8")

    deferred_path = audit_dir / "deferred.json"
    deferred_path.write_text(
        json.dumps({"deferred": deferred, "invalid": invalid, "skipped": skipped},
                   indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if args.dry_run:
        print("\nDRY RUN — no mutations to ticker_themes.json or theme_audit_state.json")
    else:
        if applied:
            updated_themes = apply_changes(ticker_themes, applied)
            save_ticker_themes(updated_themes)
            print(f"\nApplied {len(applied)} change(s) to ticker_themes.json")
        else:
            print("\nNo applied changes; ticker_themes.json unchanged")

        audit_state = load_audit_state()
        audit_state = update_audit_state(
            audit_state,
            datetime.now().isoformat(timespec="seconds"),
            decisions,
            skipped,
        )
        save_audit_state(audit_state)
        print(f"Updated theme_audit_state.json: {len(audit_state)} total entries")

    print(f"\nSummary: {summary_path}")
    print(f"Deferred: {deferred_path}")
    print(f"  applied={len(applied)} kept={len(kept)} deferred={len(deferred)} "
          f"invalid={len(invalid)} skipped={len(skipped)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
