"""Legacy theme label aliasing.

Bridges historical theme strings (still present in the curated Google Sheet
and in any cached LLM output) to the canonical hierarchical paths defined in
``config/theme_taxonomy.yaml``.

The authoritative mapping table lives in ``tools/migrate_themes.py`` — the
one-shot migration script that produced today's ``data/ticker_themes.json``.
This module re-exposes that table so live code (``apply_google_sheet_ground_truth``,
the dashboard exporter, …) can translate stale labels into the new taxonomy
on the fly without having to re-run the migration.

When a new stale label appears in the Google Sheet that has no mapping here,
``normalize_legacy_theme`` returns ``None`` and the caller drops it. Add the
new entry to ``OLD_TO_NEW`` (in ``tools/migrate_themes.py``) and the
correction propagates automatically.
"""

from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATE_PATH = _REPO_ROOT / "tools" / "migrate_themes.py"


@lru_cache(maxsize=1)
def _load_migration_tables() -> Dict[str, Dict]:
    """Import the migration script as a private module and pull its dicts.

    Cached so we only pay the import cost once per process. Returns empty
    dicts if the migration script is missing (e.g. someone deletes the
    historical record), which makes the system fail-soft rather than crash.
    """
    if not _MIGRATE_PATH.exists():
        return {"OLD_TO_NEW": {}, "TICKER_OVERRIDES": {}, "MULTI_THEME_ADDITIONS": {}}

    # Make sure the repo root is on sys.path so migrate_themes' own
    # `sys.path.insert(0, str(ROOT))` line is a no-op rather than a duplicate.
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))

    spec = importlib.util.spec_from_file_location("_migrate_themes_aliases", _MIGRATE_PATH)
    if spec is None or spec.loader is None:
        return {"OLD_TO_NEW": {}, "TICKER_OVERRIDES": {}, "MULTI_THEME_ADDITIONS": {}}
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {
        "OLD_TO_NEW": dict(getattr(module, "OLD_TO_NEW", {})),
        "TICKER_OVERRIDES": dict(getattr(module, "TICKER_OVERRIDES", {})),
        "MULTI_THEME_ADDITIONS": dict(getattr(module, "MULTI_THEME_ADDITIONS", {})),
    }


def _table() -> Dict[str, str]:
    return _load_migration_tables()["OLD_TO_NEW"]


def ticker_overrides() -> Dict[str, List[str]]:
    """Return the per-ticker override table from the migration (read-only)."""
    return _load_migration_tables()["TICKER_OVERRIDES"]


def multi_theme_additions() -> Dict[str, List[str]]:
    """Return the per-ticker secondary-theme additions from the migration."""
    return _load_migration_tables()["MULTI_THEME_ADDITIONS"]


def normalize_legacy_theme(label: str) -> Optional[str]:
    """Map a legacy label to its canonical path. Returns ``None`` if unknown.

    Whitespace is stripped before lookup. Passing ``None`` or an empty string
    returns ``None``.
    """
    if not label:
        return None
    return _table().get(str(label).strip())


def normalize_legacy_themes(labels) -> List[str]:
    """Map a list of legacy labels to canonical paths.

    Drops labels that have no canonical mapping. Order is preserved and
    duplicates are removed.
    """
    out: List[str] = []
    seen: set[str] = set()
    for raw in labels or []:
        canonical = normalize_legacy_theme(raw)
        if canonical is None:
            continue
        if canonical in seen:
            continue
        seen.add(canonical)
        out.append(canonical)
    return out


def known_l1_prefixes() -> set[str]:
    """Return every distinct L1 narrative the alias table maps to.

    Used by the defensive ``resolve_l1`` helper in ``theme_taxonomy`` to
    recognise old-format labels like "AI - Memory & Storage" whose L1 prefix
    (text before the first " - ") is a real taxonomy narrative.
    """
    prefixes: set[str] = set()
    for canonical in _table().values():
        head = canonical.split(" / ", 1)[0].strip()
        if head:
            prefixes.add(head)
    return prefixes
