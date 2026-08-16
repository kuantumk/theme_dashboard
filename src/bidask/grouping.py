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


def _tally(total_groups: int, total_tickers: int) -> dict:
    """How much there was to show, so the page can say what it is not showing.

    The cap is deliberate — an industry fallback can hold 70 tickers and would
    otherwise consume the column. Reporting it is what was missing: on
    2026-08-14 the strong column rendered 13 of 124 groups and the page gave no
    hint that the other 111 existed, so a theme that was genuinely bid looked
    identical to one that was not being tracked at all.

    Only the totals are published. The matching "shown" half is the browser's
    to count, because it applies its own sliders after this cap — a count sent
    from here would be pre-slider and would disagree with the screen.
    """
    return {"groups_total": total_groups, "tickers_total": total_tickers}


def build_columns(states, themes: dict, cfg, *, grouped: bool = True) -> dict:
    """Split accumulated states into strong-tape and weak-tape columns.

    Strong tape is ask hits exceeding bid hits. Tickers with a zero margin
    belong to neither column — the tape has not spoken on them.

    `truncated` carries per-side counts of what the display caps dropped.
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

    split = _flat if not grouped else _grouped
    strong, strong_meta = split(strong_rows, themes, cfg, reverse=True)
    weak, weak_meta = split(weak_rows, themes, cfg, reverse=False)
    return {
        "strong": strong,
        "weak": weak,
        "truncated": {"strong": strong_meta, "weak": weak_meta},
    }


def _flat(rows: list[dict], _themes: dict, cfg, *, reverse: bool) -> tuple[list[dict], dict]:
    """Crypto path: one pseudo-group, ranked. The taxonomy is equities-only.

    `_themes` is unused and named so; it exists only to match `_grouped`, which
    `build_columns` selects between and calls with one argument list.
    """
    ordered = sorted(rows, key=lambda r: r["margin"], reverse=reverse)
    ordered = ordered[: cfg.max_rows_per_column]
    total_tickers = len({r["symbol"] for r in rows})
    if not ordered:
        return [], _tally(0, total_tickers)
    shown = [{
        "name": "All",
        "origin": "flat",
        "score": sum(r["margin"] for r in ordered),
        "members": ordered,
    }]
    return shown, _tally(1, total_tickers)


def _grouped(rows: list[dict], themes: dict, cfg, *, reverse: bool) -> tuple[list[dict], dict]:
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

    # Cap per group *before* spending the column budget. Without the per-group
    # limit the top bucket takes as many slots as it has members and every other
    # narrative is dropped — an industry fallback can easily hold 70 tickers.
    capped, budget = [], cfg.max_rows_per_column
    for bucket in groups:
        if budget <= 0:
            break
        take = min(budget, cfg.max_rows_per_group)
        bucket["members"] = bucket["members"][:take]
        budget -= len(bucket["members"])
        capped.append(bucket)
    return capped, _tally(len(groups), len({r["symbol"] for r in rows}))
