"""Theme taxonomy — loads ticker-to-theme mappings and applies theme grouping.

Supports three grouping modes:
  keep    — score both super-theme and sub-themes
  consume — merge sub-themes into super-theme only
  remove  — delete listed themes without creating a super-theme

Consolidation is applied as a view layer: ticker_themes.json is never modified.
"""

from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import yaml

from config.settings import CONFIG


def load_theme_groups(path: str = None) -> Dict[str, dict]:
    """Load theme grouping configuration from YAML."""
    if path is None:
        path = CONFIG["themes"].get("scoring", {}).get(
            "theme_groups_file", "config/theme_groups.yaml"
        )
    p = Path(path)
    if not p.exists():
        return {}
    with open(p) as f:
        config = yaml.safe_load(f)
    groups = {}
    for super_theme, info in config.get("theme_groups", {}).items():
        groups[super_theme] = {
            "members": info.get("members", []),
            "prefix": info.get("prefix", []),
            "mode": info.get("mode", "keep"),
        }
    return groups


def _matches_group(theme_name: str, group_config: dict) -> bool:
    """Check if a theme name matches a group's prefix or member list."""
    for prefix in group_config.get("prefix", []):
        if theme_name.startswith(prefix):
            return True
    return theme_name in group_config.get("members", [])


def build_theme_to_tickers(
    ticker_themes: Dict[str, List[str]],
    theme_groups: Dict[str, dict] = None,
) -> Dict[str, List[str]]:
    """Build theme -> [tickers] mapping, applying grouping.

    Args:
        ticker_themes: {ticker: [theme1, theme2, ...]} from ticker_themes.json
        theme_groups: grouping config (loaded from theme_groups.yaml).
                      If None, loads from the default config path.

    Returns:
        {theme: [ticker1, ticker2, ...]} with consolidation applied.
    """
    # Reverse mapping: ticker -> themes  →  theme -> tickers
    raw = defaultdict(list)
    for ticker, themes in ticker_themes.items():
        for theme in themes:
            raw[theme].append(ticker)

    result = dict(raw)

    if theme_groups is None:
        theme_groups = load_theme_groups()

    if not theme_groups:
        return result

    consumed = set()

    for super_theme, config in theme_groups.items():
        mode = config.get("mode", "keep")

        if mode == "remove":
            for theme_name in list(raw.keys()):
                if _matches_group(theme_name, config):
                    consumed.add(theme_name)
            continue

        # keep or consume mode: build super-theme
        tickers = set()
        for theme_name in list(raw.keys()):
            if _matches_group(theme_name, config):
                tickers.update(raw[theme_name])
                if mode == "consume":
                    consumed.add(theme_name)

        if tickers:
            result[super_theme] = sorted(tickers)

    # Remove consumed/removed sub-themes, but NEVER remove a super-theme
    super_names = set(theme_groups.keys())
    for theme in consumed:
        if theme not in super_names:
            result.pop(theme, None)

    return result
