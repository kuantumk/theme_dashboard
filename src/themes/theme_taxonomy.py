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

from config.settings import CONFIG, PROJECT_ROOT

PATH_SEP = " / "


def _config_path() -> Path:
    # Anchored to the project root so the taxonomy loads from any working
    # directory: a bare relative path read as {} from tests/, and lru_cache
    # then kept that empty taxonomy for the life of the process.
    rel = CONFIG.get("themes", {}).get("scoring", {}).get(
        "theme_taxonomy_file", "config/theme_taxonomy.yaml"
    )
    return PROJECT_ROOT / rel


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


def resolve_l1(name: str, taxonomy: Optional[Dict[str, dict]] = None) -> str:
    """Return the best-guess L1 narrative for *any* theme label.

    Defense-in-depth helper for the display layer. Recognises three shapes:

    1. **Canonical taxonomy path** — ``"AI / Data Center / Memory"`` →
       ``"AI"`` (just the first slash segment).
    2. **Legacy alias** — ``"AI - Memory & Storage"``, ``"Drones"``,
       ``"Solar"`` → routed through :mod:`src.themes.legacy_aliases`. The
       canonical path's L1 is returned (e.g. ``"AI"``, ``"Defense & Aerospace"``,
       ``"Clean Energy"``).
    3. **Old "L1 - rest" prefix** — ``"AI - Some New Thing"`` → if the segment
       before the first ``" - "`` matches a taxonomy L1, that's used.

    Falls back to the raw first slash segment when nothing else matches, so
    the function is total. Callers should still validate via
    :func:`validate_path` when they need strict semantics.
    """
    if not name:
        return ""
    if taxonomy is None:
        taxonomy = load_taxonomy()

    # 1. Canonical path: first segment is a known L1.
    head = name.split(PATH_SEP, 1)[0].strip()
    if head in taxonomy:
        return head

    # 2. Legacy alias table — covers every label the migration script knew.
    try:
        from src.themes.legacy_aliases import normalize_legacy_theme  # local to avoid cycles
        canonical = normalize_legacy_theme(name)
    except Exception:  # pragma: no cover — keep helper total even if aliases unavailable
        canonical = None
    if canonical:
        canon_head = canonical.split(PATH_SEP, 1)[0].strip()
        if canon_head:
            return canon_head

    # 3. "L1 - rest" pattern — recognise dash-separated legacy labels whose
    #    prefix is still a real taxonomy L1 (e.g. a label not in the alias
    #    table but starting with "AI - ").
    if " - " in name:
        prefix = name.split(" - ", 1)[0].strip()
        if prefix in taxonomy:
            return prefix

    # 4. Last resort — return whatever the first slash segment was.
    return head


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


def is_valid_tag(path: str, taxonomy: Optional[Dict[str, dict]] = None) -> bool:
    """Strict form of :func:`validate_path` for writing tags.

    ``validate_path`` accepts a bare L1 whatever its children. As a stored tag
    a bare L1 is valid only for an L1 with no children (``Quantum Computing``,
    ``Singleton``); for any other L1 it is the bare-L1 tagging bug.
    """
    if taxonomy is None:
        taxonomy = load_taxonomy()
    if not validate_path(path, taxonomy):
        return False
    l1, l2, _ = split_path(path)
    return l2 is not None or not _children(taxonomy[l1])


def build_theme_to_tickers(
    ticker_themes: Dict[str, List[str]],
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
