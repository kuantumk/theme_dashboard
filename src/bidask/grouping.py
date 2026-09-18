"""Group tickers under the L1/L2 theme taxonomy and rank by relative volume.

Reuses `data/ticker_themes.json` — the whole reason this app lives in this repo
rather than its own. Untagged movers fall back to the feed's industry so nothing
is silently dropped: this app ranges over the whole market while the taxonomy is
curated for roughly 2,300 screened names.

A group's score has two separate halves, and they are separate on purpose. The
score this replaced was the SUM of its members' classified margins, which made
breadth swamp intensity: Spearman(member count, |score|) ran +0.72, 0 of 67
single-name groups ever reached the screen, and a two-name theme was
structurally unreachable however hard its tape ran. Here intensity is a MEAN of
the leaders — insensitive to roster size — and breadth is an additive term that
is bounded by its own coefficient and can be read off the config.
"""

from __future__ import annotations

import json
import math
from typing import Optional

from config.settings import TICKER_THEMES_FILE
from src.bidask.highs import extreme_badge
from src.themes.theme_registry import is_untagged

UNCLASSIFIED_GROUP = "Unclassified"

# The per-member figure the score ranks on: Relative Volume at Time — this
# ticker's volume since its session anchor over its own mean by the same point
# of day. Deliberately NOT `rvol`, which the feed fills from the screener's
# `relative_volume_10d_calc`. That one divides by a full-day average and is a
# different quantity at every hour; keeping both names apart is what stops the
# two being read as interchangeable.
RVOL_FIELD = "rvol_at_time"

# Which column or columns a row earned. Membership is an `in` test, so a list, a
# tuple, a set, or a dict mapping each side to the reference price that placed
# it there all work — and the mapping is what R5's per-side marker needs.
SIDES_FIELD = "sides"
SIDE_STRONG = "strong"
SIDE_WEAK = "weak"

# How many members carry the intensity half. Fixed at three by the requirement
# itself, so it is a constant rather than a config key: a tunable here would let
# two boards disagree about what "this theme's leaders" means.
TOP_MEMBERS = 3

# Defaults for the three scoring knobs, mirroring the `bidask:` block of
# `config/workflow_config.yaml`. `BidAskConfig` carries all three, so `_tunable`
# reads them off the config object; these defaults are the fallback for a config
# double that predates them, which is what the tests build. `tests/test_bidask_grouping.py` pins these equal to the
# shipped YAML so the two cannot drift apart unnoticed.
DEFAULT_RVOL_CAP = 5.0
DEFAULT_BREADTH_COEF = 0.5
DEFAULT_BREADTH_MIN_MEMBERS = 2


def load_themes(path: Optional[str] = None) -> dict:
    target = path or TICKER_THEMES_FILE
    try:
        with open(target, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}


def _tunable(cfg, name: str, default: float) -> float:
    """Read one scoring knob from the config object, else its named default."""
    value = getattr(cfg, name, None)
    return default if value is None else float(value)


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


def _member_value(payload: dict) -> float:
    """Relative Volume at Time for one row, or 0.0 when it is unusable.

    The gate already excludes a ticker whose figure could not be computed, so
    this is a backstop rather than a path. It matters anyway: a NaN would poison
    the mean, and the state payload is serialized with `allow_nan=False`, so one
    bad row would cost the whole document rather than one field. 0.0 never
    fabricates strength — an unknown cannot lift a theme.
    """
    try:
        value = float(payload.get(RVOL_FIELD))
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) and value > 0 else 0.0


def _sort_key(payload: dict):
    """Members run highest first, ties broken by symbol so order is stable.

    Sorted on the RAW reading, not the capped one: the cap is a scoring device,
    and a reader wants the 40x name above the 6x name even though the score
    treats them alike.
    """
    return (-_member_value(payload), str(payload.get("symbol") or ""))


def _dedupe(members: list[dict]) -> list[dict]:
    """One ticker, one vote within a group.

    A leaf repeated in a ticker's tag list would otherwise count twice toward
    the top-three mean and twice toward the breadth share, and render as a
    duplicate row against the per-group cap.
    """
    seen, unique = set(), []
    for payload in members:
        symbol = payload.get("symbol")
        if symbol in seen:
            continue
        seen.add(symbol)
        unique.append(payload)
    return unique


def _intensity(members: list[dict], cap: float) -> float:
    """Mean of the top `TOP_MEMBERS` members, each capped BEFORE the mean.

    The cap is what lets a mean survive the tail. DLXY read 2090.8 in the
    measured session; uncapped it hands its theme a score three orders of
    magnitude above every rival and the ranking stops responding to anything
    else. Capping the mean instead is not the same operation — it lets one
    extreme member carry two quiet ones to the ceiling.

    A group with fewer than `TOP_MEMBERS` members scores on the members it has.
    Zero-padding to three would punish a small roster inside the intensity half,
    which is the breadth term's job and is bounded there.
    """
    values = sorted((min(_member_value(m), cap) for m in members), reverse=True)
    leaders = values[:TOP_MEMBERS]
    return (sum(leaders) / len(leaders)) if leaders else 0.0


def _breadth(qualifying: int, roster: int, coef: float, minimum: int) -> float:
    """Additive term: coefficient x the share of the roster that cleared the gate.

    Share, not count: rosters run from 2 to 214 members, so a count ranks by
    roster size and buries the small, densely participating theme — the same
    lesson the coil strip and the SI tab already record.

    Below `minimum` qualifying members the term is zero. Share alone hands a
    one-member group the maximum, which is the opposite of what breadth means.

    The term only ever adds. A theme with no other qualifying members is not
    worse for it, merely not broad, so the result is floored at zero and a
    negative coefficient cannot turn it into a penalty.
    """
    if qualifying < minimum or roster <= 0:
        return 0.0
    share = min(1.0, qualifying / roster)
    return max(0.0, coef * share)


def _roster_sizes(universe: list[dict], themes: dict) -> dict[str, int]:
    """Distinct members per group in the poll's liquidity-filtered universe.

    Taken BEFORE the relative-volume gate. After the gate every surviving row
    qualifies by definition, so a post-gate denominator is 1.0 for every group
    and the breadth term ranks nothing at all. It is also the only denominator
    the industry-fallback groups can have: an untagged bucket has no taxonomy
    roster to count against.
    """
    seen: dict[str, set] = {}
    for payload in universe:
        symbol = payload.get("symbol")
        if not symbol:
            continue
        for name, _origin in _leaves_for(symbol, payload, themes):
            seen.setdefault(name, set()).add(symbol)
    return {name: len(symbols) for name, symbols in seen.items()}


def build_columns(rows, themes: dict, cfg, *, grouped: bool = True,
                  universe=None) -> dict:
    """Split one poll's qualifying rows into strong-tape and weak-tape columns.

    `rows` are the rows that cleared the relative-volume gate. Each carries
    `symbol`, `RVOL_FIELD`, and `SIDES_FIELD` naming the side or sides it
    earned. A row can carry both: a stock above today's open and below
    yesterday's close is genuinely being accumulated and distributed against two
    different references, so the two sides are independent tests here and never
    an if/else.

    `universe` is the same poll's liquidity-filtered rows BEFORE the gate, and
    supplies the breadth denominator. Omitting it falls back to the qualifying
    rows, where every counted member qualified by construction — the share then
    sits at or near 1.0 for every group and the term stops separating anything.
    That is degenerate rather than wrong, and it is why the caller should pass
    the pre-gate rows.

    Both columns sort descending. The score is a capped relative volume and is
    never negative, so an ascending weak column would lead with the quietest
    theme and spend its whole budget before reaching the ones being distributed.
    There is no `reverse` parameter left to pass the wrong way round.

    `truncated` carries per-side counts of what the display caps dropped.
    """
    strong_rows, weak_rows = [], []
    for source in rows:
        sides = source.get(SIDES_FIELD) or ()
        if SIDE_STRONG not in sides and SIDE_WEAK not in sides:
            continue
        # A copy, so the badge never lands on the caller's row — the same list
        # is usually also the pre-gate `universe`.
        payload = dict(source)
        payload["badge"] = extreme_badge(payload)
        if SIDE_STRONG in sides:
            strong_rows.append(payload)
        if SIDE_WEAK in sides:
            weak_rows.append(payload)

    pool = list(rows) if universe is None else list(universe)
    split = _flat if not grouped else _grouped
    strong, strong_meta = split(strong_rows, themes, cfg, pool)
    weak, weak_meta = split(weak_rows, themes, cfg, pool)
    return {
        "strong": strong,
        "weak": weak,
        "truncated": {"strong": strong_meta, "weak": weak_meta},
    }


def _score(members: list[dict], roster: int, cfg) -> float:
    """Capped top-three mean plus the additive breadth term, rounded for display.

    Rounded here rather than at render time so the number the board ranks on is
    the number it shows. A sort on an unrounded value beside a rounded label is
    a board whose order cannot be checked against its own figures.
    """
    total = _intensity(members, _tunable(cfg, "group_rvol_cap", DEFAULT_RVOL_CAP))
    total += _breadth(
        len(members),
        roster,
        _tunable(cfg, "group_breadth_coef", DEFAULT_BREADTH_COEF),
        int(_tunable(cfg, "group_breadth_min_members",
                     DEFAULT_BREADTH_MIN_MEMBERS)),
    )
    return round(total, 4)


def _flat(rows: list[dict], _themes: dict, cfg, universe: list[dict]
          ) -> tuple[list[dict], dict]:
    """Crypto path: one pseudo-group, ranked. The taxonomy is equities-only.

    `_themes` is unused and named so; it exists only to match `_grouped`, which
    `build_columns` selects between and calls with one argument list.
    """
    members = _dedupe(rows)
    members.sort(key=_sort_key)
    total_tickers = len({r["symbol"] for r in rows})
    if not members:
        return [], _tally(0, total_tickers)
    roster = len({r.get("symbol") for r in universe if r.get("symbol")})
    shown = [{
        "name": "All",
        "origin": "flat",
        # Scored before the column cap, so the ranking does not depend on how
        # many rows happen to fit.
        "score": _score(members, roster, cfg),
        "members": members[: cfg.max_rows_per_column],
    }]
    return shown, _tally(1, total_tickers)


def _grouped(rows: list[dict], themes: dict, cfg, universe: list[dict]
             ) -> tuple[list[dict], dict]:
    roster = _roster_sizes(universe, themes)

    buckets: dict[str, dict] = {}
    for payload in rows:
        for name, origin in _leaves_for(payload["symbol"], payload, themes):
            bucket = buckets.setdefault(name, {"name": name, "origin": origin,
                                               "score": 0.0, "members": []})
            bucket["members"].append(payload)

    groups = list(buckets.values())
    for bucket in groups:
        bucket["members"] = _dedupe(bucket["members"])
        bucket["members"].sort(key=_sort_key)
        bucket["score"] = _score(bucket["members"],
                                 roster.get(bucket["name"], 0), cfg)
    groups.sort(key=lambda b: (-b["score"], b["name"]))

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
