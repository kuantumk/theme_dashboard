"""Daily non-LLM theme sync: Google Sheet ground truth + untagged surfacing.

LLM theme classification is not part of this module anymore. The weekday
Claude Code audit routine (``.claude/routines/theme_tag_audit.md``) runs the
``audit-theme-tags`` skill, which classifies the untagged tickers this sync
surfaces and merges the result back to main. This module keeps the non-LLM
inputs running in CI:

1. **Google Sheet ground truth** — human curation input, translated through
   the legacy alias table, validated against the canonical taxonomy, and
   applied under the ``git_locked_themes`` defence (existing canonical tags
   are never overwritten; only brand-new / generic-only tickers inherit).
2. **Company-profile cache warming** — ``ensure_company_profiles`` for the
   generic-tagged screened set, so the committed cache gives the routine
   company context without needing yfinance access.
3. **Untagged surfacing** — the routine's worklist (see
   ``theme_registry.is_untagged``): screened tickers with no entry, an empty
   list, or ``Uncategorized``-only. ``Singleton``-only is excluded.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Mapping, Set

from config.settings import CONFIG, LOG_DIR
from src.themes.company_profiles import ensure_company_profiles
from src.themes.import_existing_themes import import_google_sheet_themes
from src.themes.theme_registry import (
    filter_untagged,
    load_ticker_themes,
    normalize_theme_list,
    save_ticker_themes,
)


GENERIC_SHEET_THEMES = {"Uncategorized", "Singleton"}
GENERIC_CLASSIFICATION_THEMES = {"Uncategorized", "Singleton"}


@dataclass
class ThemeSyncResult:
    """Outcome of the daily non-LLM theme sync.

    ``untagged_tickers`` is the worklist the weekday audit routine consumes:
    screened tickers with no entry, an empty list, or ``Uncategorized``-only
    (``Singleton``-only excluded — see ``theme_registry.is_untagged``).
    ``profile_candidates`` is the broader generic set (Singleton included)
    whose company profiles were warmed for the routine.
    """

    ticker_themes: Dict[str, List[str]]
    google_sheet_tickers: List[str]
    google_sheet_updates: List[Dict[str, object]]
    profile_candidates: List[str]
    untagged_tickers: List[str]
    audit_report_path: str | None = None


def load_existing_themes() -> Dict[str, List[str]]:
    return load_ticker_themes()


def normalize_tickers(tickers: Iterable[str]) -> List[str]:
    cleaned = {
        str(ticker).strip().upper()
        for ticker in tickers
        if str(ticker).strip()
    }
    return sorted(cleaned)


def themes_match(left: Iterable[str] | None, right: Iterable[str] | None) -> bool:
    return sorted(normalize_theme_list(left)) == sorted(normalize_theme_list(right))


def get_existing_theme_taxonomy(ticker_themes: Mapping[str, List[str]]) -> List[str]:
    themes = set()
    for theme_list in ticker_themes.values():
        themes.update(normalize_theme_list(theme_list))
    return sorted(themes)


def _canonicalize_sheet_themes(raw_themes: Iterable[str]) -> tuple[List[str], List[str]]:
    """Translate sheet labels into canonical taxonomy paths.

    Returns ``(canonical_paths, dropped_labels)``.

    Each raw label is normalised through the legacy alias table; the result is
    then validated against the canonical taxonomy. Labels that resolve to a
    valid taxonomy path are kept (deduplicated, preserving order). Labels that
    are already canonical paths are kept as-is. Everything else is dropped and
    reported back to the caller for logging.
    """
    from src.themes.legacy_aliases import normalize_legacy_theme
    from src.themes.theme_taxonomy import load_taxonomy, validate_path

    taxonomy = load_taxonomy()
    canonical: List[str] = []
    dropped: List[str] = []
    seen: Set[str] = set()

    for raw in normalize_theme_list(raw_themes):
        if raw in GENERIC_SHEET_THEMES:
            continue
        # First try: is the sheet label already a canonical taxonomy path?
        candidate = raw if validate_path(raw, taxonomy) else None
        # Second try: is it a known legacy label?
        if candidate is None:
            mapped = normalize_legacy_theme(raw)
            if mapped and validate_path(mapped, taxonomy):
                candidate = mapped
        if candidate is None:
            dropped.append(raw)
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        canonical.append(candidate)

    return canonical, dropped


def apply_google_sheet_ground_truth(
    ticker_themes: Mapping[str, List[str]],
    tickers: Iterable[str],
    google_sheet_themes: Mapping[str, List[str]],
) -> tuple[Dict[str, List[str]], Set[str], List[Dict[str, object]]]:
    """Apply Google Sheet ground truth, with three layers of defence:

    1. **Legacy alias remap** — stale sheet labels like ``"AI - Memory & Storage"``
       are translated to their canonical taxonomy path before anything else
       happens. The mapping table is sourced from the migration script via
       :mod:`src.themes.legacy_aliases`.
    2. **Taxonomy validation** — any label that does not resolve to a valid
       path in ``config/theme_taxonomy.yaml`` is dropped. If *all* of a
       ticker's sheet labels fail validation the ticker keeps its existing
       canonical tags (no destructive overwrite from bad data).
    3. **Git-locked tags** — when ``themes.git_locked_themes`` is ``true`` in
       ``workflow_config.yaml``, existing tickers whose stored tags are already
       canonical are *never* overwritten by the sheet. Only brand-new tickers
       (no existing tags, or only ``Uncategorized``/``Singleton``) inherit
       from the sheet. Use ``python -m src.themes.retag`` for explicit
       re-classification of an existing ticker.
    """
    git_locked = bool(CONFIG.get("themes", {}).get("git_locked_themes", True))

    merged = {ticker: list(themes) for ticker, themes in ticker_themes.items()}
    ground_truth_tickers: Set[str] = set()
    updates: List[Dict[str, object]] = []

    for ticker in normalize_tickers(tickers):
        if ticker not in google_sheet_themes:
            continue

        canonical, dropped = _canonicalize_sheet_themes(google_sheet_themes[ticker])
        if dropped:
            print(f"  Sheet alias guard dropped non-canonical labels for {ticker}: {dropped}")
        if not canonical:
            continue

        previous = normalize_theme_list(merged.get(ticker))
        previous_is_canonical = bool(previous) and all(
            theme not in GENERIC_SHEET_THEMES for theme in previous
        )

        if git_locked and previous_is_canonical:
            # Existing canonical tags are frozen. Treat the ticker as ground
            # truth (so it isn't surfaced as untagged) but don't mutate its
            # tags. To re-tag, use ``python -m src.themes.retag``.
            ground_truth_tickers.add(ticker)
            if not themes_match(previous, canonical):
                print(
                    f"  Sheet sync skipped for {ticker} (git-locked): "
                    f"keeping {previous} (sheet wanted {canonical})"
                )
            continue

        ground_truth_tickers.add(ticker)
        if themes_match(previous, canonical):
            continue

        print(f"  Google Sheet update {ticker}: {previous} -> {canonical}")
        merged[ticker] = canonical
        updates.append({"ticker": ticker, "previous": previous, "updated": canonical})

    return merged, ground_truth_tickers, updates


def identify_tickers_needing_classification(
    screened_tickers: Iterable[str],
    ticker_themes: Mapping[str, List[str]],
    google_sheet_tickers: Set[str],
) -> List[str]:
    """Screened tickers with only generic tags (or none) and no sheet coverage.

    This is the *broad* generic set — ``Singleton``-only included — used to
    warm the company-profile cache for the audit routine. The routine's
    actual tagging worklist is the narrower ``theme_registry.filter_untagged``
    set, which excludes ``Singleton``-only.
    """
    candidates: List[str] = []
    for ticker in normalize_tickers(screened_tickers):
        if ticker in google_sheet_tickers:
            continue
        current = normalize_theme_list(ticker_themes.get(ticker))
        if not current or all(theme in GENERIC_CLASSIFICATION_THEMES for theme in current):
            candidates.append(ticker)
    return candidates


def write_sync_audit(
    result: ThemeSyncResult,
    *,
    screened_ticker_count: int,
) -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = LOG_DIR / f"theme_sync_audit_{datetime.now().strftime('%Y-%m-%d')}.json"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "screened_ticker_count": screened_ticker_count,
        "google_sheet_tickers": result.google_sheet_tickers,
        "google_sheet_updates": result.google_sheet_updates,
        "profile_candidates": result.profile_candidates,
        "untagged_tickers": result.untagged_tickers,
    }
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return str(audit_path)


def sync_screened_ticker_themes(screener_tickers: Set[str]) -> ThemeSyncResult:
    """Daily non-LLM theme sync: Sheet ground truth + profile warming + untagged surfacing.

    LLM classification of the surfaced untagged tickers happens in the weekday
    Claude Code audit routine (``.claude/routines/theme_tag_audit.md``), not here.
    """
    screened_tickers = normalize_tickers(screener_tickers)
    previous_themes = load_existing_themes()
    ticker_themes = {ticker: list(themes) for ticker, themes in previous_themes.items()}

    google_sheet_tickers: Set[str] = set()
    google_sheet_updates: List[Dict[str, object]] = []
    try:
        google_sheet_themes = import_google_sheet_themes()
        print(f"Loaded {len(google_sheet_themes)} ticker(s) from Google Sheet")
        ticker_themes, google_sheet_tickers, google_sheet_updates = apply_google_sheet_ground_truth(
            ticker_themes,
            screened_tickers,
            google_sheet_themes,
        )
    except Exception as exc:
        print(f"Warning: Failed to fetch Google Sheet: {exc}")

    # Broader generic set (Singleton included): warm the committed profile
    # cache so the audit routine has company context for both first-time
    # classification and Singleton rescue.
    profile_candidates = identify_tickers_needing_classification(
        screened_tickers,
        ticker_themes,
        google_sheet_tickers,
    )
    if profile_candidates:
        try:
            ensure_company_profiles(profile_candidates)
        except Exception as exc:
            print(f"Warning: profile cache warming failed: {exc}")
    else:
        print("No screened tickers need profile warming")

    save_ticker_themes(ticker_themes)
    persisted_themes = load_ticker_themes()

    untagged_tickers = filter_untagged(screened_tickers, persisted_themes)

    result = ThemeSyncResult(
        ticker_themes=persisted_themes,
        google_sheet_tickers=sorted(google_sheet_tickers),
        google_sheet_updates=google_sheet_updates,
        profile_candidates=profile_candidates,
        untagged_tickers=untagged_tickers,
    )
    result.audit_report_path = write_sync_audit(result, screened_ticker_count=len(screened_tickers))

    print(
        "\nTheme sync summary: "
        f"{len(result.google_sheet_updates)} sheet update(s), "
        f"{len(result.profile_candidates)} profile(s) warmed, "
        f"{len(result.untagged_tickers)} untagged awaiting routine"
    )
    print(f"Sync audit saved to {result.audit_report_path}")
    return result


if __name__ == "__main__":
    test_tickers = {"NVDA", "TSLA", "AAPL", "LUNR", "LMND"}
    result = sync_screened_ticker_themes(test_tickers)
    print(f"\nTotal tickers in database: {len(result.ticker_themes)}")
    print(f"Untagged awaiting routine: {result.untagged_tickers}")
