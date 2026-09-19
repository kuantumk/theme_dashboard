"""The crypto board's price reference — that market's own 24 hours.

`src/bidask/session_state.py` is the equity path and stays that way. Each of
its four states selects a reference price **and** a volume floor, and both
choices come from `current_session`, a field the crypto scanner does not send.
Crypto has no open to measure against and no close to measure against; it has
one rolling 24-hour reference, published as `24h_close_change|5` and mapped to
`change_pct` by `src/bidask/feed.py`. Putting that in the session table would
invent a fifth session state for a market that has none.

**R15: the reference is labelled, not implied.** `REF_24H` reaches the screen
beside `open`, `prev close` and `session close`, and a reader switching tabs
must not carry the equity meaning across. A chip reading "24h" says what it
was measured against; an unlabelled tint would leave the crypto column looking
like an equity session measure taken at a strange hour.

The volume half of the crypto rules is not here. It lives with every other
relative-volume rule in `src/bidask/rvol_at_time.py`, keyed by the same
`crypto` string this module carries — one flat floor and a UTC-day anchor.
`tests/test_bidask_universe.py` pins the two spellings equal, the way this
package pins `feed.SESSION_LABELS` against `session_state.SESSION_STATES`.
"""

from __future__ import annotations

from src.bidask.session_state import Sides, direction_of

# Kept equal to `rvol_at_time.CRYPTO` by a test rather than by an import: that
# module reaches `tvbars` and then `tvsocket`, and this one is read by the poll
# path where a socket import buys nothing.
CRYPTO = "crypto"

# The field `feed.fetch_crypto` maps `24h_close_change|5` onto. Named here so
# the reference and the column that answers it sit in one place.
CHANGE_FIELD = "change_pct"

# What the side was measured against, as it reaches the screen. Deliberately
# unlike every `REF_*` in `session_state.py`, all of which name a session
# boundary this market does not have.
REF_24H = "24h ago"


def crypto_sides(row) -> Sides:
    """The side or sides one crypto row earns against its 24-hour reference.

    One reference, so unlike the regular session a row earns at most one side.
    The pair of tuples is kept anyway because `build_columns` reads the same
    `sides` shape for both markets, and a second shape would need a second
    branch in the column builder to go wrong in.

    An unreadable or exactly flat change earns neither side. A coin sitting on
    its 24-hour-ago price has not moved against it, and a null field is not a
    direction — the same fail-closed rule the volume gate obeys one module
    over, and the reason a dead `24h_close_change|5` empties the board instead
    of filling one column with the whole universe.
    """
    direction = direction_of(row, CHANGE_FIELD)
    return Sides(
        state=CRYPTO,
        strong=(REF_24H,) if direction > 0 else (),
        weak=(REF_24H,) if direction < 0 else (),
    )
