"""Group tickers under the L1/L2 theme taxonomy and rank by tape pressure.

Reuses `data/ticker_themes.json` — the whole reason this app lives in this repo
rather than its own. Untagged movers fall back to the feed's industry so nothing
is silently dropped: this app ranges over the whole market while the taxonomy is
curated for roughly 2,300 screened names.
"""

from __future__ import annotations

import json
from typing import Optional

from config.settings import TICKER_THEMES_FILE
from src.bidask.highs import extreme_badge
from src.themes.theme_registry import is_untagged

UNCLASSIFIED_GROUP = "Unclassified"


def load_themes(path: Optional[str] = None) -> dict:
    target = path or TICKER_THEMES_FILE
    try:
        with open(target, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}


def _leaves_for(symbol: str, meta: dict, themes: dict) -> list[tuple[str, str]]:
    """Return [(group name, origin)] for a symbol.

    A tagged ticker yields one entry per theme leaf — a dual-role name genuinely
    belongs to both narratives and contributes to both group scores. An untagged
    one yields a single industry-derived entry.
    """
    tags = themes.get(symbol)
    if not is_untagged(tags):
        return [(leaf, "theme") for leaf in tags]
    industry = meta.get("industry")
    if industry:
        return [(str(industry), "industry")]
    return [(UNCLASSIFIED_GROUP, "industry")]


def build_columns(states, themes: dict, cfg, *, grouped: bool = True) -> dict:
    """Split accumulated states into strong-tape and weak-tape columns.

    Strong tape is ask hits exceeding bid hits. Tickers with a zero margin
    belong to neither column — the tape has not spoken on them.
    """
    strong_rows, weak_rows = [], []
    for state in states:
        if state.total_hits < cfg.min_hits_to_show:
            continue
        payload = state.as_dict()
        payload["badge"] = extreme_badge(state.meta)
        if state.margin > 0:
            strong_rows.append(payload)
        elif state.margin < 0:
            weak_rows.append(payload)

    if not grouped:
        return {
            "strong": _flat(strong_rows, cfg, reverse=True),
            "weak": _flat(weak_rows, cfg, reverse=False),
        }
    return {
        "strong": _grouped(strong_rows, themes, cfg, reverse=True),
        "weak": _grouped(weak_rows, themes, cfg, reverse=False),
    }


def _flat(rows: list[dict], cfg, *, reverse: bool) -> list[dict]:
    """Crypto path: one pseudo-group, ranked. The taxonomy is equities-only."""
    ordered = sorted(rows, key=lambda r: r["margin"], reverse=reverse)
    ordered = ordered[: cfg.max_rows_per_column]
    if not ordered:
        return []
    return [{
        "name": "All",
        "origin": "flat",
        "score": sum(r["margin"] for r in ordered),
        "members": ordered,
    }]


def _grouped(rows: list[dict], themes: dict, cfg, *, reverse: bool) -> list[dict]:
    buckets: dict[str, dict] = {}
    for payload in rows:
        for name, origin in _leaves_for(payload["symbol"], payload, themes):
            bucket = buckets.setdefault(name, {"name": name, "origin": origin,
                                               "score": 0, "members": []})
            bucket["score"] += payload["margin"]
            bucket["members"].append(payload)

    groups = list(buckets.values())
    for bucket in groups:
        bucket["members"].sort(key=lambda r: r["margin"], reverse=reverse)
    groups.sort(key=lambda b: b["score"], reverse=reverse)

    # Cap by rendered rows rather than group count, so one huge group cannot
    # crowd out every other narrative in the column.
    capped, budget = [], cfg.max_rows_per_column
    for bucket in groups:
        if budget <= 0:
            break
        bucket["members"] = bucket["members"][:budget]
        budget -= len(bucket["members"])
        capped.append(bucket)
    return capped
