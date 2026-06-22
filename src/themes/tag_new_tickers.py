"""Theme classification and dashboard validation pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Mapping, Sequence, Set

from config.settings import CONFIG, GOOGLE_API_KEY, LOG_DIR, THEME_REVIEW_STATE_FILE
from src.themes.company_profiles import ensure_company_profiles
from src.themes.import_existing_themes import import_google_sheet_themes
from src.themes.theme_registry import load_ticker_themes, normalize_theme_list, save_ticker_themes


GENERIC_SHEET_THEMES = {"Uncategorized", "Singleton"}
GENERIC_CLASSIFICATION_THEMES = {"Uncategorized", "Singleton"}
CLASSIFICATION_BATCH_SIZE = CONFIG["themes"].get("llm_batch_size", 100)
VALIDATION_BATCH_SIZE = CONFIG["themes"].get("validation_batch_size", 60)
VALIDATION_STALE_DAYS = CONFIG["themes"].get("validation_stale_days", 30)
VALIDATION_CONFIRMATION_THRESHOLD = CONFIG["themes"].get("validation_confirmation_threshold", 2)


class GeminiJSONError(ValueError):
    """Gemini returned text that could not be parsed as a complete JSON object."""

    def __init__(
        self,
        message: str,
        *,
        finish_reason: str = "",
        response_chars: int = 0,
        preview: str = "",
    ) -> None:
        super().__init__(message)
        self.finish_reason = finish_reason
        self.response_chars = response_chars
        self.preview = preview


@dataclass
class ThemeClassificationResult:
    ticker_themes: Dict[str, List[str]]
    google_sheet_tickers: List[str]
    google_sheet_updates: List[Dict[str, object]]
    classification_candidates: List[str]
    classified_tickers: List[str]
    new_tickers: List[str]
    unresolved_tickers: List[str]
    failed_batches: List[Dict[str, object]]
    audit_report_path: str | None = None


@dataclass
class ThemeValidationResult:
    ticker_themes: Dict[str, List[str]]
    google_sheet_tickers: List[str]
    google_sheet_updates: List[Dict[str, object]]
    validated_tickers: List[str]
    confirmed_keeps: List[str]
    pending_mismatches: List[Dict[str, object]]
    applied_retags: List[Dict[str, object]]
    unresolved_tickers: List[str]
    audit_report_path: str | None = None
    review_state_path: str | None = None


@dataclass
class ValidationApplicationResult:
    ticker_themes: Dict[str, List[str]]
    review_state: Dict[str, Dict[str, object]]
    confirmed_keeps: List[str]
    pending_mismatches: List[Dict[str, object]]
    applied_retags: List[Dict[str, object]]
    unresolved_tickers: List[str]


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


def _coerce_theme_list(raw_themes: object) -> List[str]:
    if isinstance(raw_themes, str):
        cleaned = normalize_theme_list([raw_themes])
    elif isinstance(raw_themes, list):
        cleaned = normalize_theme_list(raw_themes)
    else:
        cleaned = []
    return cleaned or ["Uncategorized"]


def _clean_note(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())[:140].strip()


def _response_finish_reason(response: object) -> str:
    candidates = getattr(response, "candidates", None) or []
    if not candidates:
        return ""
    return str(getattr(candidates[0], "finish_reason", "") or "")


def _call_gemini_json(prompt: str) -> Dict[str, object]:
    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY environment variable not set")

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=GOOGLE_API_KEY)
    response = client.models.generate_content(
        model=CONFIG["llm"]["model"],
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=CONFIG["llm"]["temperature"],
            max_output_tokens=CONFIG["llm"]["max_tokens"],
            response_mime_type="application/json",
        ),
    )

    text = (response.text or "").strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        finish_reason = _response_finish_reason(response)
        raise GeminiJSONError(
            (
                "Gemini returned invalid JSON "
                f"(finish_reason={finish_reason or 'unknown'}, chars={len(text)}): {exc}"
            ),
            finish_reason=finish_reason,
            response_chars=len(text),
            preview=text[-500:],
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError("Gemini response must be a JSON object")
    return parsed


def filter_real_sheet_themes(themes: Iterable[str] | None) -> List[str]:
    return [theme for theme in normalize_theme_list(themes) if theme not in GENERIC_SHEET_THEMES]


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
            # truth (so it doesn't fall back into Gemini classification) but
            # don't mutate its tags. To re-tag, use ``python -m src.themes.retag``.
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


def format_profiles_for_prompt(
    tickers: Sequence[str],
    profiles: Mapping[str, Mapping[str, str]],
    *,
    ticker_themes: Mapping[str, List[str]] | None = None,
) -> str:
    lines: List[str] = []
    for ticker in tickers:
        profile = profiles.get(ticker, {})
        company_name = profile.get("company_name") or ticker
        sector = profile.get("sector") or "Unknown"
        industry = profile.get("industry") or "Unknown"
        summary = profile.get("business_summary") or "No cached business summary."
        parts = [
            f"ticker={ticker}",
            f"company={company_name}",
            f"sector={sector}",
            f"industry={industry}",
            f"summary={summary}",
        ]
        if ticker_themes is not None:
            current = normalize_theme_list(ticker_themes.get(ticker))
            parts.insert(1, f"current_themes={', '.join(current) if current else 'None'}")
        lines.append("- " + " | ".join(parts))
    return "\n".join(lines)


def _format_taxonomy_for_prompt() -> str:
    """Render the canonical taxonomy as a numbered list of every valid path."""
    from src.themes.theme_taxonomy import list_all_paths
    paths = list_all_paths()
    return "\n".join(f"- {p}" for p in sorted(paths))


def build_classification_prompt(
    tickers: List[str],
    existing_themes: List[str],  # noqa: ARG001 — replaced by full taxonomy
    profiles: Mapping[str, Mapping[str, str]],
) -> str:
    return f"""<role>
Act as a Momentum & Sector Analysis Specialist.
</role>

<task>
Classify the provided stocks into the canonical hierarchical theme taxonomy.
Use the cached company metadata below instead of guessing from ticker symbols.
Pick the MOST SPECIFIC path available (prefer L3 over L2 when applicable).
If a stock is genuinely idiosyncratic with no peer group, use "Singleton".
</task>

<rules>
1. PATH FORMAT: Each theme is a slash-delimited path with " / " separators:
   - "AI / Data Center / Memory"  (L1 / L2 / L3 — most specific)
   - "Space / Launch"             (L1 / L2)
   - "Quantum Computing"          (L1 only, when the L1 has no children)
   ONLY use paths from <taxonomy> below. Do NOT invent new paths.

2. L1 = NARRATIVE: L1 is the dominant investing narrative. Tickers sharing L1 share a
   trading thesis. Clean Energy and Oil & Gas are SEPARATE L1s — a fuel-cell company
   (BE) must NEVER share L1 with an oilfield-services company (PUMP).

3. SECTOR CONSISTENCY: The chosen L1 must align with the company's sector/industry.
   - An Energy (oil/gas) sector company belongs under "Oil & Gas", not "Clean Energy".
   - A Consumer Cyclical / Internet Retail company is "Software & Internet / E-commerce"
     or "Retail (Multi-Category) / ...", NOT "Logistics".

4. CORE BUSINESS ONLY: Classify by primary revenue source. Do NOT classify by:
   - Headquarters location (use "Geographic / ..." ONLY when geography IS the thesis).
   - Customer segment served (a grocery-delivery app is "Gig Economy / Delivery",
     not "Logistics").

5. MULTI-THEME (max 3 paths): A second/third path is only justified when the company
   has DISTINCT material revenue lines under different L1s (e.g. AMZN has E-commerce
   AND Cloud, so it gets both Software & Internet / E-commerce and AI / Data Center /
   Cloud & Hyperscalers). Do NOT add extra paths for country or customer type.
</rules>

<negative_examples>
WRONG: BE (sector=Industrials, industry=Specialty Industrial Machinery, fuel cells)
       -> ["Oil & Gas / E&P"]
       Fuel cell systems are CLEAN ENERGY (hydrogen), NOT fossil-fuel oil & gas.
       CORRECT: ["Clean Energy / Fuel Cell & Hydrogen"]

WRONG: CART (Maplebear/Instacart, sector=Consumer Cyclical, industry=Internet Retail)
       -> ["Software & Internet / E-commerce", "Logistics / Freight Brokerage & Trucking"]
       Instacart is a retail marketplace, not a freight broker.
       CORRECT: ["Software & Internet / E-commerce"]

WRONG: YPF (sector=Energy, industry=Oil & Gas Integrated, Argentine)
       -> ["Oil & Gas / E&P", "Geographic / Argentina"]
       YPF is an energy company. Being Argentine doesn't make geography the thesis.
       CORRECT: ["Oil & Gas / E&P"]
</negative_examples>

<taxonomy>
{_format_taxonomy_for_prompt()}
</taxonomy>

<ticker_profiles>
{format_profiles_for_prompt(tickers, profiles)}
</ticker_profiles>

<output_format>
Return ONLY valid JSON, max 3 paths per ticker:
{{
  "TICKER": ["L1 / L2 / L3", "L1 / L2 (optional second path)"]
}}
</output_format>"""


def classify_tickers_with_gemini(
    tickers: List[str],
    existing_themes: List[str],
    profiles: Mapping[str, Mapping[str, str]],
) -> Dict[str, List[str]]:
    if not tickers:
        return {}

    print(f"Classifying {len(tickers)} new/unclassified ticker(s) with Gemini...")
    raw_tags = _call_gemini_json(build_classification_prompt(tickers, existing_themes, profiles))

    from src.themes.theme_taxonomy import validate_path, load_taxonomy
    taxonomy = load_taxonomy()

    normalized_tags: Dict[str, List[str]] = {}
    for ticker, themes in raw_tags.items():
        clean_ticker = str(ticker).strip().upper()
        if not clean_ticker:
            continue
        candidate_paths = _coerce_theme_list(themes)[:3]
        valid_paths = [p for p in candidate_paths if validate_path(p, taxonomy)]
        invalid = [p for p in candidate_paths if p not in valid_paths]
        if invalid:
            print(f"  Taxonomy guard: dropped invalid paths for {clean_ticker}: {invalid}")
        normalized_tags[clean_ticker] = valid_paths or ["Singleton"]

    missing = sorted(set(tickers) - set(normalized_tags.keys()))
    if missing:
        print(f"Warning: {len(missing)} classification ticker(s) missing from Gemini response: {missing}")

    return normalized_tags


def _failure_payload(
    *,
    batch: Sequence[str],
    batch_num: int,
    total_batches: int,
    error: Exception,
    retried: bool,
    terminal: bool,
) -> Dict[str, object]:
    payload: Dict[str, object] = {
        "batch_num": batch_num,
        "total_batches": total_batches,
        "size": len(batch),
        "tickers": list(batch),
        "error_type": type(error).__name__,
        "error": str(error),
        "retried": retried,
        "terminal": terminal,
    }
    if isinstance(error, GeminiJSONError):
        payload["finish_reason"] = error.finish_reason
        payload["response_chars"] = error.response_chars
        payload["response_tail"] = error.preview
    return payload


def _missing_response_error(missing: Sequence[str]) -> GeminiJSONError:
    return GeminiJSONError(
        f"Gemini response omitted {len(missing)} ticker(s): {list(missing)}",
        finish_reason="missing_tickers",
    )


def _split_and_retry_classification(
    tickers: List[str],
    existing_themes: List[str],
    profiles: Mapping[str, Mapping[str, str]],
    *,
    batch_num: int,
    total_batches: int,
    failure: Dict[str, object],
) -> tuple[Dict[str, List[str]], List[Dict[str, object]]]:
    midpoint = len(tickers) // 2
    left_tags, left_failures = classify_tickers_with_retries(
        tickers[:midpoint],
        existing_themes,
        profiles,
        batch_num=batch_num,
        total_batches=total_batches,
    )
    right_tags, right_failures = classify_tickers_with_retries(
        tickers[midpoint:],
        existing_themes,
        profiles,
        batch_num=batch_num,
        total_batches=total_batches,
    )
    return {**left_tags, **right_tags}, [failure, *left_failures, *right_failures]


def classify_tickers_with_retries(
    tickers: List[str],
    existing_themes: List[str],
    profiles: Mapping[str, Mapping[str, str]],
    *,
    batch_num: int,
    total_batches: int,
) -> tuple[Dict[str, List[str]], List[Dict[str, object]]]:
    """Classify a batch, splitting retryable failures into smaller batches."""
    if not tickers:
        return {}, []

    try:
        tags = classify_tickers_with_gemini(tickers, existing_themes, profiles)
    except GeminiJSONError as exc:
        if len(tickers) == 1:
            return {}, [
                _failure_payload(
                    batch=tickers,
                    batch_num=batch_num,
                    total_batches=total_batches,
                    error=exc,
                    retried=False,
                    terminal=True,
                )
            ]

        failure = _failure_payload(
            batch=tickers,
            batch_num=batch_num,
            total_batches=total_batches,
            error=exc,
            retried=True,
            terminal=False,
        )
        return _split_and_retry_classification(
            tickers,
            existing_themes,
            profiles,
            batch_num=batch_num,
            total_batches=total_batches,
            failure=failure,
        )

    missing = sorted(set(tickers) - set(tags.keys()))
    if not missing:
        return tags, []

    missing_error = _missing_response_error(missing)
    if len(missing) == len(tickers):
        if len(tickers) == 1:
            return tags, [
                _failure_payload(
                    batch=missing,
                    batch_num=batch_num,
                    total_batches=total_batches,
                    error=missing_error,
                    retried=False,
                    terminal=True,
                )
            ]

        failure = _failure_payload(
            batch=missing,
            batch_num=batch_num,
            total_batches=total_batches,
            error=missing_error,
            retried=True,
            terminal=False,
        )
        retry_tags, retry_failures = _split_and_retry_classification(
            tickers,
            existing_themes,
            profiles,
            batch_num=batch_num,
            total_batches=total_batches,
            failure=failure,
        )
        return {**tags, **retry_tags}, retry_failures

    failure = _failure_payload(
        batch=missing,
        batch_num=batch_num,
        total_batches=total_batches,
        error=missing_error,
        retried=True,
        terminal=False,
    )
    retry_tags, retry_failures = classify_tickers_with_retries(
        missing,
        existing_themes,
        profiles,
        batch_num=batch_num,
        total_batches=total_batches,
    )
    return {**tags, **retry_tags}, [failure, *retry_failures]


# Sector → blocked L1 narratives. L1 names from config/theme_taxonomy.yaml.
# Conservative: only blocks obviously wrong cross-sector assignments.
SECTOR_THEME_BLOCKLIST: Dict[str, Set[str]] = {
    "Energy": {"Fintech & Crypto", "Software & Internet", "Healthcare", "Biotech"},
    "Consumer Cyclical": {"Fintech & Crypto", "Logistics"},
    "Consumer Defensive": {"Fintech & Crypto", "Logistics"},
    "Healthcare": {"Fintech & Crypto", "Oil & Gas", "Clean Energy", "Logistics"},
    "Industrials": {"Fintech & Crypto", "Healthcare", "Biotech"},
    "Basic Materials": {"Fintech & Crypto", "Software & Internet", "Healthcare"},
    "Financial Services": {"Oil & Gas", "Clean Energy", "Healthcare", "Biotech", "Logistics"},
    "Real Estate": {"Oil & Gas", "Clean Energy", "Logistics", "Biotech"},
    "Utilities": {"Fintech & Crypto", "Logistics", "Biotech"},
}


def filter_sector_inconsistent_themes(
    classified_tags: Dict[str, List[str]],
    profiles: Mapping[str, Mapping[str, str]],
) -> Dict[str, List[str]]:
    """Remove themes that are clearly incompatible with a ticker's sector.

    Uses a conservative blocklist so only obviously wrong pairings are caught.
    Never leaves a ticker themeless — keeps the first theme if all would be removed.
    """
    result: Dict[str, List[str]] = {}
    for ticker, themes in classified_tags.items():
        sector = (profiles.get(ticker) or {}).get("sector", "")
        blocked_prefixes = SECTOR_THEME_BLOCKLIST.get(sector, set())
        if not blocked_prefixes:
            result[ticker] = themes
            continue

        kept: List[str] = []
        removed: List[str] = []
        for theme in themes:
            if any(theme.startswith(prefix) for prefix in blocked_prefixes):
                removed.append(theme)
            else:
                kept.append(theme)

        if removed:
            if not kept:
                # Never leave a ticker themeless — keep the first theme
                kept = [themes[0]]
                removed = removed[1:] if len(removed) > 1 else []
                print(f"  Sector guard: all themes blocked for {ticker} (sector={sector}), keeping first: {kept}")
            for theme in removed:
                print(f"  Sector guard removed '{theme}' from {ticker} (sector={sector})")

        result[ticker] = kept or themes
    return result


def identify_tickers_needing_classification(
    screened_tickers: Iterable[str],
    ticker_themes: Mapping[str, List[str]],
    google_sheet_tickers: Set[str],
) -> List[str]:
    candidates: List[str] = []
    for ticker in normalize_tickers(screened_tickers):
        if ticker in google_sheet_tickers:
            continue
        current = normalize_theme_list(ticker_themes.get(ticker))
        if not current or all(theme in GENERIC_CLASSIFICATION_THEMES for theme in current):
            candidates.append(ticker)
    return candidates


def write_classification_audit(
    result: ThemeClassificationResult,
    *,
    screened_ticker_count: int,
) -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = LOG_DIR / f"theme_classification_audit_{datetime.now().strftime('%Y-%m-%d')}.json"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "screened_ticker_count": screened_ticker_count,
        "google_sheet_tickers": result.google_sheet_tickers,
        "google_sheet_updates": result.google_sheet_updates,
        "classification_candidates": result.classification_candidates,
        "classified_tickers": result.classified_tickers,
        "new_tickers": result.new_tickers,
        "unresolved_tickers": result.unresolved_tickers,
        "failed_batches": result.failed_batches,
    }
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return str(audit_path)


def sync_screened_ticker_themes(screener_tickers: Set[str]) -> ThemeClassificationResult:
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

    classification_candidates = identify_tickers_needing_classification(
        screened_tickers,
        ticker_themes,
        google_sheet_tickers,
    )
    classified_tags: Dict[str, List[str]] = {}
    failed_batches: List[Dict[str, object]] = []

    if classification_candidates:
        profiles = ensure_company_profiles(classification_candidates)
        total_batches = (len(classification_candidates) + CLASSIFICATION_BATCH_SIZE - 1) // CLASSIFICATION_BATCH_SIZE
        for start in range(0, len(classification_candidates), CLASSIFICATION_BATCH_SIZE):
            batch = classification_candidates[start:start + CLASSIFICATION_BATCH_SIZE]
            batch_num = start // CLASSIFICATION_BATCH_SIZE + 1
            existing_themes = get_existing_theme_taxonomy({**ticker_themes, **classified_tags})
            try:
                batch_tags, batch_failures = classify_tickers_with_retries(
                    batch,
                    existing_themes,
                    profiles,
                    batch_num=batch_num,
                    total_batches=total_batches,
                )
                batch_tags = filter_sector_inconsistent_themes(batch_tags, profiles)
                classified_tags.update(batch_tags)
                failed_batches.extend(batch_failures)
                for failure in batch_failures:
                    status = "terminal" if failure.get("terminal") else "retried"
                    print(
                        "  Classification batch "
                        f"{failure.get('batch_num')}/{failure.get('total_batches')} {status}: "
                        f"{failure.get('error')}"
                    )
            except Exception as exc:
                failure = _failure_payload(
                    batch=batch,
                    batch_num=batch_num,
                    total_batches=total_batches,
                    error=exc,
                    retried=False,
                    terminal=True,
                )
                failed_batches.append(failure)
                print(
                    f"  Warning: classification batch {batch_num}/{total_batches} failed "
                    f"({len(batch)} tickers): {exc}"
                )

        for ticker, themes in classified_tags.items():
            if not themes_match(ticker_themes.get(ticker), themes):
                print(f"  Classified {ticker}: {ticker_themes.get(ticker)} -> {themes}")
                ticker_themes[ticker] = themes
    else:
        print("No screened tickers need new classification")

    save_ticker_themes(ticker_themes)
    persisted_themes = load_ticker_themes()

    new_tickers = [
        ticker
        for ticker in classification_candidates
        if not normalize_theme_list(previous_themes.get(ticker)) and ticker in classified_tags
    ]
    unresolved_tickers = [
        ticker
        for ticker in classification_candidates
        if ticker not in classified_tags
    ]

    result = ThemeClassificationResult(
        ticker_themes=persisted_themes,
        google_sheet_tickers=sorted(google_sheet_tickers),
        google_sheet_updates=google_sheet_updates,
        classification_candidates=classification_candidates,
        classified_tickers=sorted(classified_tags.keys()),
        new_tickers=sorted(new_tickers),
        unresolved_tickers=unresolved_tickers,
        failed_batches=failed_batches,
    )
    result.audit_report_path = write_classification_audit(result, screened_ticker_count=len(screened_tickers))

    print(
        "\nTheme classification summary: "
        f"{len(result.classified_tickers)} classified, "
        f"{len(result.new_tickers)} new, "
        f"{len(result.unresolved_tickers)} unresolved"
    )
    print(f"Classification audit saved to {result.audit_report_path}")
    return result


def _normalize_review_entry(raw_entry: Mapping[str, object] | None) -> Dict[str, object]:
    raw = dict(raw_entry or {})
    confirmation_count = raw.get("confirmation_count", 0)
    try:
        confirmation_count = int(confirmation_count)
    except (TypeError, ValueError):
        confirmation_count = 0

    return {
        "last_validated_at": str(raw.get("last_validated_at", "")).strip(),
        "last_seen_on_dashboard_at": str(raw.get("last_seen_on_dashboard_at", "")).strip(),
        "pending_source_themes": normalize_theme_list(raw.get("pending_source_themes")),
        "pending_candidate_themes": normalize_theme_list(raw.get("pending_candidate_themes")),
        "pending_note": _clean_note(raw.get("pending_note")),
        "pending_since": str(raw.get("pending_since", "")).strip(),
        "confirmation_count": confirmation_count,
        "last_applied_at": str(raw.get("last_applied_at", "")).strip(),
        "last_applied_themes": normalize_theme_list(raw.get("last_applied_themes")),
    }


def load_theme_review_state() -> Dict[str, Dict[str, object]]:
    if not THEME_REVIEW_STATE_FILE.exists():
        return {}

    with THEME_REVIEW_STATE_FILE.open(encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict):
        raise ValueError(f"Theme review state at {THEME_REVIEW_STATE_FILE} must be a JSON object")

    state: Dict[str, Dict[str, object]] = {}
    for ticker, entry in raw.items():
        clean_ticker = str(ticker).strip().upper()
        if not clean_ticker:
            continue
        state[clean_ticker] = _normalize_review_entry(entry if isinstance(entry, dict) else {})
    return state


def save_theme_review_state(state: Mapping[str, Mapping[str, object]]) -> None:
    normalized = {
        str(ticker).strip().upper(): _normalize_review_entry(entry)
        for ticker, entry in state.items()
        if str(ticker).strip()
    }
    THEME_REVIEW_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with THEME_REVIEW_STATE_FILE.open("w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2, sort_keys=True)
        handle.write("\n")


def prune_theme_review_state(
    state: Mapping[str, Mapping[str, object]],
    *,
    max_age_days: int = VALIDATION_STALE_DAYS,
    reference_time: datetime | None = None,
) -> Dict[str, Dict[str, object]]:
    reference_time = reference_time or datetime.now()
    cutoff = reference_time - timedelta(days=max_age_days)
    pruned: Dict[str, Dict[str, object]] = {}

    for ticker, entry in state.items():
        normalized = _normalize_review_entry(entry)
        if normalized["pending_candidate_themes"]:
            pruned[ticker] = normalized
            continue

        last_validated_at = str(normalized.get("last_validated_at", "")).strip()
        if not last_validated_at:
            continue

        try:
            validated_at = datetime.fromisoformat(last_validated_at)
        except ValueError:
            continue

        if validated_at >= cutoff:
            pruned[ticker] = normalized

    return pruned


def select_validation_tickers(
    dashboard_tickers: Iterable[str],
    review_state: Mapping[str, Mapping[str, object]],
    *,
    reference_time: datetime | None = None,
) -> List[str]:
    candidates = set(normalize_tickers(dashboard_tickers))
    reference_time = reference_time or datetime.now()
    cutoff = reference_time - timedelta(days=VALIDATION_STALE_DAYS)
    for ticker, entry in review_state.items():
        normalized = _normalize_review_entry(entry)
        if normalized["pending_candidate_themes"]:
            candidates.add(str(ticker).strip().upper())
            continue

        last_validated_at = str(normalized.get("last_validated_at", "")).strip()
        if not last_validated_at:
            candidates.add(str(ticker).strip().upper())
            continue

        try:
            validated_at = datetime.fromisoformat(last_validated_at)
        except ValueError:
            candidates.add(str(ticker).strip().upper())
            continue

        if validated_at < cutoff:
            candidates.add(str(ticker).strip().upper())
    return sorted(candidates)


def build_validation_prompt(
    tickers: List[str],
    ticker_themes: Mapping[str, List[str]],
    profiles: Mapping[str, Mapping[str, str]],
) -> str:
    return f"""<role>
Act as a careful theme-tag validator.
</role>

<task>
For each stock below, decide whether the CURRENT theme assignment is a good fit for the company's real business.
Use the cached company metadata below.
Return action="keep" unless the current theme is clearly wrong.
Only propose action="candidate_change" when the mismatch is material.
If the metadata is too weak to judge, return action="uncertain".
</task>

<ticker_profiles>
{format_profiles_for_prompt(tickers, profiles, ticker_themes=ticker_themes)}
</ticker_profiles>

<output_format>
Return ONLY valid JSON:
{{
  "TICKER1": {{"action": "keep"}},
  "TICKER2": {{"action": "candidate_change", "themes": ["Better Theme"], "note": "short reason"}},
  "TICKER3": {{"action": "uncertain"}}
}}
</output_format>"""


def validate_tickers_with_gemini(
    tickers: List[str],
    ticker_themes: Mapping[str, List[str]],
    profiles: Mapping[str, Mapping[str, str]],
) -> Dict[str, Dict[str, object]]:
    if not tickers:
        return {}

    print(f"Validating {len(tickers)} dashboard/pending ticker(s) with Gemini...")
    raw = _call_gemini_json(build_validation_prompt(tickers, ticker_themes, profiles))

    normalized: Dict[str, Dict[str, object]] = {}
    for ticker, decision in raw.items():
        clean_ticker = str(ticker).strip().upper()
        if not clean_ticker or not isinstance(decision, dict):
            continue

        action = str(decision.get("action", "")).strip().lower()
        if action not in {"keep", "candidate_change", "uncertain"}:
            action = "uncertain"

        payload: Dict[str, object] = {"action": action}
        if action == "candidate_change":
            payload["themes"] = _coerce_theme_list(decision.get("themes"))
            payload["note"] = _clean_note(decision.get("note"))
        normalized[clean_ticker] = payload

    missing = sorted(set(tickers) - set(normalized.keys()))
    if missing:
        print(f"Warning: {len(missing)} validation ticker(s) missing from Gemini response: {missing}")

    return normalized


def apply_validation_decisions(
    ticker_themes: Mapping[str, List[str]],
    review_state: Mapping[str, Mapping[str, object]],
    validation_tickers: Iterable[str],
    dashboard_tickers: Iterable[str],
    decisions: Mapping[str, Mapping[str, object]],
    *,
    confirmation_threshold: int = VALIDATION_CONFIRMATION_THRESHOLD,
    validation_time: datetime | None = None,
) -> ValidationApplicationResult:
    validation_time = validation_time or datetime.now()
    now_iso = validation_time.isoformat(timespec="seconds")
    today_str = validation_time.date().isoformat()
    dashboard_set = set(normalize_tickers(dashboard_tickers))
    updated_themes = {ticker: list(themes) for ticker, themes in ticker_themes.items()}
    updated_state = {
        str(ticker).strip().upper(): _normalize_review_entry(entry)
        for ticker, entry in review_state.items()
        if str(ticker).strip()
    }

    confirmed_keeps: List[str] = []
    pending_mismatches: List[Dict[str, object]] = []
    applied_retags: List[Dict[str, object]] = []
    unresolved_tickers: List[str] = []

    for ticker in normalize_tickers(validation_tickers):
        current_themes = normalize_theme_list(updated_themes.get(ticker))
        entry = _normalize_review_entry(updated_state.get(ticker))
        if ticker in dashboard_set:
            entry["last_seen_on_dashboard_at"] = today_str

        decision = decisions.get(ticker)
        if not decision:
            entry["last_validated_at"] = now_iso
            updated_state[ticker] = entry
            unresolved_tickers.append(ticker)
            continue

        action = str(decision.get("action", "")).strip().lower()
        if action == "keep":
            entry.update(
                {
                    "last_validated_at": now_iso,
                    "pending_source_themes": [],
                    "pending_candidate_themes": [],
                    "pending_note": "",
                    "pending_since": "",
                    "confirmation_count": 0,
                }
            )
            updated_state[ticker] = entry
            confirmed_keeps.append(ticker)
            continue

        if action != "candidate_change":
            entry["last_validated_at"] = now_iso
            updated_state[ticker] = entry
            unresolved_tickers.append(ticker)
            continue

        candidate_themes = _coerce_theme_list(decision.get("themes"))
        if themes_match(current_themes, candidate_themes):
            entry.update(
                {
                    "last_validated_at": now_iso,
                    "pending_source_themes": [],
                    "pending_candidate_themes": [],
                    "pending_note": "",
                    "pending_since": "",
                    "confirmation_count": 0,
                }
            )
            updated_state[ticker] = entry
            confirmed_keeps.append(ticker)
            continue

        same_source = themes_match(entry.get("pending_source_themes"), current_themes)
        same_candidate = themes_match(entry.get("pending_candidate_themes"), candidate_themes)
        confirmation_count = entry.get("confirmation_count", 0) + 1 if same_source and same_candidate else 1
        note = _clean_note(decision.get("note"))

        if confirmation_count >= confirmation_threshold:
            updated_themes[ticker] = candidate_themes
            entry.update(
                {
                    "last_validated_at": now_iso,
                    "pending_source_themes": [],
                    "pending_candidate_themes": [],
                    "pending_note": "",
                    "pending_since": "",
                    "confirmation_count": 0,
                    "last_applied_at": now_iso,
                    "last_applied_themes": candidate_themes,
                }
            )
            updated_state[ticker] = entry
            applied_retags.append(
                {
                    "ticker": ticker,
                    "previous": current_themes,
                    "updated": candidate_themes,
                    "confirmations": confirmation_count,
                    "note": note,
                }
            )
            continue

        entry.update(
            {
                "last_validated_at": now_iso,
                "pending_source_themes": current_themes,
                "pending_candidate_themes": candidate_themes,
                "pending_note": note,
                "pending_since": entry.get("pending_since") if same_source and same_candidate else today_str,
                "confirmation_count": confirmation_count,
            }
        )
        updated_state[ticker] = entry
        pending_mismatches.append(
            {
                "ticker": ticker,
                "current": current_themes,
                "candidate": candidate_themes,
                "confirmations": confirmation_count,
                "threshold": confirmation_threshold,
                "note": note,
            }
        )

    updated_state = prune_theme_review_state(updated_state, reference_time=validation_time)
    return ValidationApplicationResult(
        ticker_themes=updated_themes,
        review_state=updated_state,
        confirmed_keeps=sorted(confirmed_keeps),
        pending_mismatches=pending_mismatches,
        applied_retags=applied_retags,
        unresolved_tickers=sorted(unresolved_tickers),
    )


def write_validation_audit(
    result: ThemeValidationResult,
    *,
    requested_ticker_count: int,
) -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    audit_path = LOG_DIR / f"theme_validation_audit_{datetime.now().strftime('%Y-%m-%d')}.json"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "requested_ticker_count": requested_ticker_count,
        "google_sheet_tickers": result.google_sheet_tickers,
        "google_sheet_updates": result.google_sheet_updates,
        "validated_tickers": result.validated_tickers,
        "confirmed_keeps": result.confirmed_keeps,
        "pending_mismatches": result.pending_mismatches,
        "applied_retags": result.applied_retags,
        "unresolved_tickers": result.unresolved_tickers,
        "review_state_path": result.review_state_path,
    }
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return str(audit_path)


def validate_dashboard_ticker_themes(dashboard_tickers: Iterable[str]) -> ThemeValidationResult:
    # Git-locked mode: skip the auto-revalidation loop. Existing tickers stay
    # frozen at their committed paths; only `src.themes.retag` (explicit user
    # command) or `sync_screened_ticker_themes` (new tickers) can mutate them.
    git_locked = CONFIG.get("themes", {}).get("git_locked_themes", True)
    if git_locked:
        ticker_themes = load_existing_themes()
        return ThemeValidationResult(
            ticker_themes=ticker_themes,
            google_sheet_tickers=[],
            google_sheet_updates=[],
            validated_tickers=[],
            confirmed_keeps=[],
            pending_mismatches=[],
            applied_retags=[],
            unresolved_tickers=[],
            review_state_path=str(THEME_REVIEW_STATE_FILE),
        )

    ticker_themes = load_existing_themes()
    review_state = load_theme_review_state()
    validation_tickers = select_validation_tickers(dashboard_tickers, review_state)

    if not validation_tickers:
        pruned_state = prune_theme_review_state(review_state)
        save_theme_review_state(pruned_state)
        result = ThemeValidationResult(
            ticker_themes=ticker_themes,
            google_sheet_tickers=[],
            google_sheet_updates=[],
            validated_tickers=[],
            confirmed_keeps=[],
            pending_mismatches=[],
            applied_retags=[],
            unresolved_tickers=[],
            review_state_path=str(THEME_REVIEW_STATE_FILE),
        )
        result.audit_report_path = write_validation_audit(result, requested_ticker_count=0)
        return result

    google_sheet_tickers: Set[str] = set()
    google_sheet_updates: List[Dict[str, object]] = []
    try:
        google_sheet_themes = import_google_sheet_themes()
        print(f"Loaded {len(google_sheet_themes)} ticker(s) from Google Sheet for validation")
        ticker_themes, google_sheet_tickers, google_sheet_updates = apply_google_sheet_ground_truth(
            ticker_themes,
            validation_tickers,
            google_sheet_themes,
        )
    except Exception as exc:
        print(f"Warning: Failed to fetch Google Sheet during validation: {exc}")

    current_time = datetime.now()
    for ticker in google_sheet_tickers:
        entry = _normalize_review_entry(review_state.get(ticker))
        entry.update(
            {
                "last_validated_at": current_time.isoformat(timespec="seconds"),
                "pending_source_themes": [],
                "pending_candidate_themes": [],
                "pending_note": "",
                "pending_since": "",
                "confirmation_count": 0,
            }
        )
        review_state[ticker] = entry

    tickers_for_gemini = [ticker for ticker in validation_tickers if ticker not in google_sheet_tickers]
    decisions: Dict[str, Dict[str, object]] = {}
    if tickers_for_gemini:
        profiles = ensure_company_profiles(tickers_for_gemini)
        for start in range(0, len(tickers_for_gemini), VALIDATION_BATCH_SIZE):
            batch = tickers_for_gemini[start:start + VALIDATION_BATCH_SIZE]
            decisions.update(validate_tickers_with_gemini(batch, ticker_themes, profiles))
    else:
        print("All validation tickers were resolved via Google Sheet ground truth")

    application = apply_validation_decisions(
        ticker_themes=ticker_themes,
        review_state=review_state,
        validation_tickers=tickers_for_gemini,
        dashboard_tickers=dashboard_tickers,
        decisions=decisions,
        validation_time=current_time,
    )

    save_ticker_themes(application.ticker_themes)
    save_theme_review_state(application.review_state)

    result = ThemeValidationResult(
        ticker_themes=load_ticker_themes(),
        google_sheet_tickers=sorted(google_sheet_tickers),
        google_sheet_updates=google_sheet_updates,
        validated_tickers=tickers_for_gemini,
        confirmed_keeps=application.confirmed_keeps,
        pending_mismatches=application.pending_mismatches,
        applied_retags=application.applied_retags,
        unresolved_tickers=application.unresolved_tickers,
        review_state_path=str(THEME_REVIEW_STATE_FILE),
    )
    result.audit_report_path = write_validation_audit(result, requested_ticker_count=len(validation_tickers))

    print(
        "\nTheme validation summary: "
        f"{len(result.confirmed_keeps)} keeps, "
        f"{len(result.pending_mismatches)} pending mismatches, "
        f"{len(result.applied_retags)} applied retags, "
        f"{len(result.unresolved_tickers)} unresolved"
    )
    print(f"Validation audit saved to {result.audit_report_path}")
    return result


def tag_new_tickers(screener_tickers: Set[str]) -> Dict[str, List[str]]:
    return sync_screened_ticker_themes(screener_tickers).ticker_themes


if __name__ == "__main__":
    test_tickers = {"NVDA", "TSLA", "AAPL", "LUNR", "LMND"}
    result = sync_screened_ticker_themes(test_tickers)
    print(f"\nTotal tickers in database: {len(result.ticker_themes)}")
