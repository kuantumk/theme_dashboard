"""Session state and reference-price resolution for the tape board.

One poll's response becomes a session state, and that state decides which
reference price the board measures against. The state also decides the
relative-volume floor (`src/bidask/universe.py`), so the two choices travel
together: a state that acquired a reference without a floor, or the reverse,
would gate half the board by one set of rules and half by another.

| state         | Reference for strong / weak            |
|---------------|----------------------------------------|
| `pre_market`  | previous session close                 |
| `market`      | session open **or** previous close     |
| `post_market` | the regular session close              |
| `crypto`      | the price 24 hours ago                 |
| anything else | none — the board declares itself shut  |

**Strong and weak are two independent tests, never a branch.** A stock that
closed at $10, opened at $9 and now trades at $9.50 is genuinely strong against
its open and genuinely weak against yesterday, and the board shows both
readings rather than picking one. An `if strong ... elif weak` shape passes
every single-sided case and silently drops exactly the case this design is for,
which is why each side is built in its own tuple and why the tests exercise
them separately.

Each side carries the name of the reference that placed it there. The UI has to
print that name — a tint cannot say *which* comparison made a ticker weak, and
with two references live in the regular session the answer is not inferable
from the column alone.

**⛔ Direction is read from the feed's own change fields, never reconstructed
from prices.** Deriving the previous close as `close / (1 + change)` adds a
division whose failure mode on a null or zero field is a WRONG SIGN rather than
an absent reading — a ticker placed in the opposite column with nothing on
screen to show it. The vendor already publishes `change`, `change_from_open`,
`premarket_change` and `postmarket_change`, all measured against exactly the
references above, and all non-null on 100% of rows through the regular session.

**⛔ An unmapped `current_session` is closed, not open.** The feed sends
`market` for the regular session — verified live, after `regular` had been
assumed for months and never observed. Both spellings map here. A value not in
`SESSIONS` resolves to a closed board, because the alternative is applying a
reference price and a volume floor that were written for a different state.

**`crypto` is a state the feed never sends.** The crypto scanner carries no
`current_session` at all: that market has no open and no close, only one
rolling 24-hour reference. `feed.fetch_crypto` stamps the state itself, which is
why `crypto` is a key of `REFERENCE_FIELDS` and of the floor schedules but never
a value in `SESSIONS`. Its reference is labelled `24h ago` (R15) so a reader
switching tabs does not carry an equity session meaning across.

This module imports nothing from the package. `config.py` and `rvol_at_time.py`
both read the state names from here, which is what lets one definition serve
both without the import cycle (`rvol_at_time` -> `tvbars` -> `tvsocket` ->
`config`) that used to force a second copy.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple

# The states the board recognises. The three trading values are spelled as the
# feed spells them, so a config block keyed by state reads the same way the
# payload does.
PRE_MARKET = "pre_market"
MARKET = "market"
POST_MARKET = "post_market"
CLOSED = "closed"
# The crypto board's state. Not a session — see the module docstring.
CRYPTO = "crypto"

# The states `bidask.in_play_rvol_schedules` may be keyed by: every state that
# has a floor. `closed` is absent on purpose — it has none, and admits nothing.
SCHEDULE_STATES = (PRE_MARKET, MARKET, POST_MARKET, CRYPTO)

# What each side is measured against. These strings reach the screen, so they
# read as a trader would name the comparison rather than as a column name.
REF_OPEN = "open"
REF_PREV_CLOSE = "prev close"
REF_SESSION_CLOSE = "session close"
# Deliberately unlike the three above, all of which name a session boundary the
# crypto market does not have.
REF_24H = "24h ago"

# Every `current_session` value the feed is known to send, mapped to the state
# the board acts on AND the label the banner prints. One table, so a spelling
# cannot reach the banner without also reaching the rules, or the reverse.
SESSIONS = {
    # `market` is what the feed actually sends while the regular session runs
    # — verified live, 2026-08-12. `regular` was assumed and never observed.
    "market": (MARKET, "market open"),
    "regular": (MARKET, "market open"),
    "pre_market": (PRE_MARKET, "pre-market"),
    "premarket": (PRE_MARKET, "pre-market"),
    "post_market": (POST_MARKET, "after hours"),
    "postmarket": (POST_MARKET, "after hours"),
    "out_of_session": (CLOSED, "market closed"),
    "holiday": (CLOSED, "holiday"),
    # `extended` is listed here to record a decision rather than to leave one
    # to a fall-through. It was not observed during the after-hours probe and
    # no reference price or volume floor has been assigned to it, so it reads
    # as closed until one is. Borrowing the post-market pair would be a guess
    # about which side of 16:00 the value means.
    "extended": (CLOSED, "extended hours"),
}

# Per state, the change fields that answer it, paired with the reference each
# one measures against. Order is the order the UI prints them in.
#
# The regular session carries two references because a ticker can be up on the
# day and down from its open, or the reverse, and both facts belong on the
# board. Pre-market and after hours carry one each: before the bell there is no
# open yet, and after it `close` still holds the 16:00 print — measured, not
# assumed — so any reference derived from `close` reports the session that has
# already ended rather than the window now trading. Crypto carries one:
# `24h_close_change|5`, which `feed.fetch_crypto` maps onto `change_pct`.
REFERENCE_FIELDS = {
    MARKET: (("change_from_open", REF_OPEN), ("change", REF_PREV_CLOSE)),
    PRE_MARKET: (("premarket_change", REF_PREV_CLOSE),),
    POST_MARKET: (("postmarket_change", REF_SESSION_CLOSE),),
    CRYPTO: (("change_pct", REF_24H),),
    CLOSED: (),
}


@dataclass(frozen=True)
class Sides:
    """The side or sides one row earns, and the reference behind each.

    Both tuples can be populated at once. That is the intended reading of a
    gapped-down recovering stock, not a conflict to resolve downstream.
    """

    strong: Tuple[str, ...] = ()
    weak: Tuple[str, ...] = ()


def resolve_session(current_session) -> Tuple[str, str]:
    """`(state, banner label)` for one value of the feed's own session field.

    The local clock is never consulted: a holiday, an early close and a feed
    outage all look like an ordinary afternoon from here. An unknown value is
    closed, and its label is the raw value made readable — the banner may name
    a state the board has no rules for, but the board never applies any.
    """
    try:
        raw = str(current_session).strip().lower()
    except Exception:  # noqa: BLE001 — an unprintable value is still just closed
        return CLOSED, ""
    return SESSIONS.get(raw, (CLOSED, raw.replace("_", " ")))


def resolve_state(current_session) -> str:
    """Map the feed's own session field to the state the board acts on."""
    return resolve_session(current_session)[0]


def direction_of(row, field: str) -> int:
    """+1 above the reference, -1 below it, 0 when the field cannot answer.

    Non-finite first, for the reason the classifier's guards gave: pandas
    yields NaN rather than None for a null cell, and every comparison against
    NaN is False. Without the explicit test a null field would fall through
    both branches to the same 0 — right today, and one refactor away from a
    default that reads a missing column as a direction. On the crypto board a
    dead `24h_close_change|5` therefore empties the columns rather than filling
    one with the whole universe.

    An exact zero is 0 as well. A stock sitting on its open has not moved
    against it, and a zero move is not a direction.
    """
    try:
        value = float(row.get(field))
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(value):
        return 0
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def sides_for(row, state: str) -> Sides:
    """The side or sides this row earns in `state`.

    Pure: `state` is passed in rather than read off the row, because one poll
    resolves one state for the whole response. Reading it per row would let two
    tickers in the same response be judged against different references.

    A state with no references — closed, or a value the feed has never sent —
    returns neither side however far the price has moved. That is R16 holding
    at the other end: price alone admits nothing.
    """
    strong = []
    weak = []
    for field, reference in REFERENCE_FIELDS.get(state, ()):
        direction = direction_of(row, field)
        # This branch is per reference, where it is exhaustive — one field
        # cannot be above and below at once. The independence R5 needs lives in
        # the two separate lists: one reference can fill `strong` while the
        # next fills `weak`. A single `if strong ... elif weak` over the whole
        # row is the shape that drops the gapped-down case.
        if direction > 0:
            strong.append(reference)
        elif direction < 0:
            weak.append(reference)
    return Sides(strong=tuple(strong), weak=tuple(weak))
