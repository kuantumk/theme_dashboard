"""Cached company profile helpers for theme classification and validation."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Mapping

from config.settings import CONFIG, DATA_DIR, TICKER_COMPANY_METADATA_FILE


PROFILE_CACHE_DAYS = CONFIG["themes"].get("company_metadata_cache_days", 90)
SUMMARY_MAX_CHARS = 280
NASDAQ_TICKER_FILE = DATA_DIR / "tickers_from_nasdaq.txt"


def _clean_text(value: object, max_chars: int | None = None) -> str:
    """Normalize whitespace and optionally clamp the length."""
    if value is None:
        return ""

    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text:
        return ""

    if max_chars is not None and len(text) > max_chars:
        text = text[: max_chars - 3].rstrip(" ,;:.") + "..."
    return text


def _normalize_profile(profile: Mapping[str, object] | None) -> Dict[str, str]:
    """Normalize a profile into a stable JSON-friendly structure."""
    raw = dict(profile or {})
    return {
        "company_name": _clean_text(raw.get("company_name")),
        "sector": _clean_text(raw.get("sector")),
        "industry": _clean_text(raw.get("industry")),
        "business_summary": _clean_text(raw.get("business_summary"), SUMMARY_MAX_CHARS),
        "last_updated": _clean_text(raw.get("last_updated")),
        "source": _clean_text(raw.get("source")),
    }


def load_company_profiles() -> Dict[str, Dict[str, str]]:
    """Load cached company profiles from disk."""
    if not TICKER_COMPANY_METADATA_FILE.exists():
        return {}

    with TICKER_COMPANY_METADATA_FILE.open(encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict):
        raise ValueError(f"Company metadata at {TICKER_COMPANY_METADATA_FILE} must be a JSON object")

    normalized: Dict[str, Dict[str, str]] = {}
    for ticker, profile in raw.items():
        clean_ticker = _clean_text(ticker).upper()
        if not clean_ticker:
            continue
        normalized[clean_ticker] = _normalize_profile(profile if isinstance(profile, dict) else {})
    return normalized


def save_company_profiles(profiles: Mapping[str, Mapping[str, object]]) -> None:
    """Persist normalized company profiles to disk."""
    normalized = {
        _clean_text(ticker).upper(): _normalize_profile(profile)
        for ticker, profile in profiles.items()
        if _clean_text(ticker)
    }
    TICKER_COMPANY_METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TICKER_COMPANY_METADATA_FILE.open("w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2, sort_keys=True)
        handle.write("\n")


def load_security_name_map() -> Dict[str, str]:
    """Load local ticker -> security name mappings from the Nasdaq dump."""
    if not NASDAQ_TICKER_FILE.exists():
        return {}

    security_names: Dict[str, str] = {}
    with NASDAQ_TICKER_FILE.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            values = line.rstrip("\n").split("|")
            if len(values) < 3 or values[1] in {"Symbol", ""}:
                continue
            symbol = _clean_text(values[1]).upper()
            if not symbol or symbol in security_names:
                continue
            security_names[symbol] = _clean_text(values[2])
    return security_names


def _is_stale(profile: Mapping[str, str], max_age_days: int) -> bool:
    """Whether a cached profile should be refreshed."""
    if not profile:
        return True

    required_fields = ("company_name", "business_summary")
    if not any(_clean_text(profile.get(field)) for field in required_fields):
        return True

    last_updated = _clean_text(profile.get("last_updated"))
    if not last_updated:
        return True

    try:
        updated_at = datetime.fromisoformat(last_updated)
    except ValueError:
        return True

    return updated_at < datetime.now() - timedelta(days=max_age_days)


def fetch_company_profile(ticker: str, security_names: Mapping[str, str] | None = None) -> Dict[str, str]:
    """Fetch a single company profile from yfinance with local-name fallback."""
    import yfinance as yf

    clean_ticker = _clean_text(ticker).upper()
    fallback_name = _clean_text((security_names or {}).get(clean_ticker)) or clean_ticker

    profile = {
        "company_name": fallback_name,
        "sector": "",
        "industry": "",
        "business_summary": "",
        "last_updated": datetime.now().isoformat(timespec="seconds"),
        "source": "nasdaq",
    }

    try:
        ticker_obj = yf.Ticker(clean_ticker)
        try:
            info = ticker_obj.get_info()
        except Exception:
            info = getattr(ticker_obj, "info", {}) or {}

        if isinstance(info, dict):
            profile["company_name"] = _clean_text(
                info.get("longName") or info.get("shortName") or fallback_name
            ) or fallback_name
            profile["sector"] = _clean_text(info.get("sectorDisp") or info.get("sector"))
            profile["industry"] = _clean_text(info.get("industryDisp") or info.get("industry"))
            profile["business_summary"] = _clean_text(
                info.get("longBusinessSummary") or info.get("description"),
                SUMMARY_MAX_CHARS,
            )
            profile["source"] = "yfinance"
    except Exception as exc:
        print(f"Warning: metadata fetch failed for {clean_ticker}: {exc}")

    return _normalize_profile(profile)


def ensure_company_profiles(
    tickers: Iterable[str],
    *,
    max_age_days: int | None = None,
) -> Dict[str, Dict[str, str]]:
    """Load cached profiles and fetch any missing/stale entries."""
    max_age_days = max_age_days or PROFILE_CACHE_DAYS
    requested_tickers = sorted({_clean_text(ticker).upper() for ticker in tickers if _clean_text(ticker)})
    cache = load_company_profiles()
    security_names = load_security_name_map()

    updated = False
    for ticker in requested_tickers:
        profile = cache.get(ticker, {})
        if not _is_stale(profile, max_age_days):
            continue
        cache[ticker] = fetch_company_profile(ticker, security_names)
        updated = True

    for ticker in requested_tickers:
        cache.setdefault(
            ticker,
            _normalize_profile(
                {
                    "company_name": security_names.get(ticker, ticker),
                    "source": "nasdaq",
                }
            ),
        )

    if updated:
        save_company_profiles(cache)

    return {ticker: cache[ticker] for ticker in requested_tickers}
