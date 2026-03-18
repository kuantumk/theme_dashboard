"""Shared helpers for loading and saving ticker theme mappings."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

from config.settings import TICKER_THEMES_FILE


ThemeMap = Dict[str, List[str]]


def normalize_theme_list(themes: Iterable[str] | None) -> List[str]:
    """Normalize a theme list while preserving order."""
    if themes is None:
        return []

    normalized: List[str] = []
    seen = set()
    for theme in themes:
        if theme is None:
            continue
        clean = str(theme).strip()
        if not clean or clean in seen:
            continue
        normalized.append(clean)
        seen.add(clean)
    return normalized


def normalize_theme_map(theme_map: Mapping[str, Iterable[str] | None]) -> ThemeMap:
    """Normalize ticker/theme mappings into a stable structure."""
    normalized: ThemeMap = {}
    for ticker, themes in theme_map.items():
        clean_ticker = str(ticker).strip().upper()
        if not clean_ticker:
            continue
        normalized[clean_ticker] = normalize_theme_list(themes)
    return normalized


def load_theme_map(path: Path) -> ThemeMap:
    """Load a theme mapping from disk if it exists."""
    if not path.exists():
        return {}

    with path.open(encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict):
        raise ValueError(f"Theme mapping at {path} must be a JSON object")

    return normalize_theme_map(raw)


def load_ticker_themes() -> ThemeMap:
    """Load the persisted ticker theme mapping."""
    return load_theme_map(TICKER_THEMES_FILE)


def save_ticker_themes(ticker_themes: Mapping[str, Iterable[str] | None]) -> None:
    """Persist ticker themes after normalization."""
    merged = normalize_theme_map(ticker_themes)
    TICKER_THEMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TICKER_THEMES_FILE.open("w", encoding="utf-8") as handle:
        json.dump(merged, handle, indent=2, sort_keys=True)
        handle.write("\n")
