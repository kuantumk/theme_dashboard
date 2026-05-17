"""Hierarchical theme taxonomy.

Reads ``config/theme_taxonomy.yaml`` and exposes lookups for the L1/L2/L3
narrative hierarchy. Theme strings are slash-delimited paths (separator
``" / "``), e.g. ``"AI / Data Center / Memory"``.

The taxonomy is the single source of truth for what themes exist. Every path
in ``data/ticker_themes.json`` must validate against it.
"""

from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from config.settings import CONFIG

PATH_SEP = " / "


def _config_path() -> Path:
    rel = CONFIG.get("themes", {}).get("scoring", {}).get(
        "theme_taxonomy_file", "config/theme_taxonomy.yaml"
    )
    return Path(rel)


@lru_cache(maxsize=1)
def load_taxonomy(path: Optional[str] = None) -> Dict[str, dict]:
    """Load the taxonomy YAML and return the top-level ``themes`` mapping."""
    p = Path(path) if path else _config_path()
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("themes", {})


def _children(node) -> Dict[str, dict]:
    """Normalize a node's ``children`` to a dict ``{name: subtree}``.

    YAML supports both shapes: ``children: ["A", "B"]`` (flat leaves) and
    ``children: {A: {}, B: {children: [...]}}`` (nested).
    """
    if not isinstance(node, dict):
        return {}
    raw = node.get("children")
    if raw is None:
        return {}
    if isinstance(raw, list):
        return {name: {} for name in raw}
    if isinstance(raw, dict):
        return raw
    return {}


def split_path(path: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Split a theme path into (L1, L2, L3). L2 and L3 may be ``None``."""
    parts = [p.strip() for p in path.split(PATH_SEP)]
    l1 = parts[0] if parts else ""
    l2 = parts[1] if len(parts) > 1 else None
    l3 = parts[2] if len(parts) > 2 else None
    return l1, l2, l3


def get_l1(path: str) -> str:
    return split_path(path)[0]


def get_leaf(path: str) -> str:
    """Return the most specific component of the path."""
    parts = path.split(PATH_SEP)
    return parts[-1].strip() if parts else path


def validate_path(path: str, taxonomy: Optional[Dict[str, dict]] = None) -> bool:
    """Validate a slash-delimited theme path against the taxonomy."""
    if taxonomy is None:
        taxonomy = load_taxonomy()
    if not taxonomy:
        return False
    l1, l2, l3 = split_path(path)
    if l1 not in taxonomy:
        return False
    if l2 is None:
        return True
    l2_children = _children(taxonomy[l1])
    if l2 not in l2_children:
        return False
    if l3 is None:
        return True
    l3_children = _children(l2_children[l2])
    return l3 in l3_children


def list_all_paths(taxonomy: Optional[Dict[str, dict]] = None) -> List[str]:
    """Return every valid leaf path in the taxonomy (deepest available level)."""
    if taxonomy is None:
        taxonomy = load_taxonomy()
    out: List[str] = []
    for l1, l1_node in taxonomy.items():
        l2_children = _children(l1_node)
        if not l2_children:
            out.append(l1)
            continue
        for l2, l2_node in l2_children.items():
            l3_children = _children(l2_node)
            if not l3_children:
                out.append(f"{l1}{PATH_SEP}{l2}")
                continue
            for l3 in l3_children:
                out.append(f"{l1}{PATH_SEP}{l2}{PATH_SEP}{l3}")
    return out


def build_theme_to_tickers(
    ticker_themes: Dict[str, List[str]],
    taxonomy: Optional[Dict[str, dict]] = None,  # noqa: ARG001 — kept for API parity
) -> Dict[str, List[str]]:
    """Build ``{theme_path: [tickers]}`` from ``{ticker: [theme_paths]}``.

    Groups by leaf path (the most specific tag). No prefix-based consolidation —
    the taxonomy IS the consolidation. Singleton stays as its own bucket but
    will typically be filtered out by downstream code.
    """
    out: Dict[str, List[str]] = defaultdict(list)
    for ticker, paths in ticker_themes.items():
        for path in paths:
            out[path].append(ticker)
    return {k: sorted(v) for k, v in out.items()}


def build_l1_to_tickers(
    ticker_themes: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Build ``{L1: [tickers]}`` for hub-node rendering. Tickers appear once
    per distinct L1 they touch."""
    out: Dict[str, set] = defaultdict(set)
    for ticker, paths in ticker_themes.items():
        for path in paths:
            out[get_l1(path)].add(ticker)
    return {k: sorted(v) for k, v in out.items()}


def build_l1_to_leaf_themes(
    ticker_themes: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Build ``{L1: [leaf_paths sorted]}`` — used to render is-a edges between
    leaf nodes and their L1 hub in the network visualization."""
    out: Dict[str, set] = defaultdict(set)
    for paths in ticker_themes.values():
        for path in paths:
            out[get_l1(path)].add(path)
    return {k: sorted(v) for k, v in out.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Backward-compatibility shims for code that still calls the old API
# ─────────────────────────────────────────────────────────────────────────────
def load_theme_groups(path: Optional[str] = None) -> Dict[str, dict]:
    """Deprecated — returns an empty dict. Old prefix/consume rules are gone;
    the taxonomy file is the consolidation."""
    return {}
