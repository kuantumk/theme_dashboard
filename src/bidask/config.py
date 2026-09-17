"""Configuration for the bid/ask tape-pressure dashboard.

Every tunable lives in the ``bidask:`` block of ``config/workflow_config.yaml``
so thresholds are never hardcoded in the pipeline modules.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import FrozenSet, Optional

from config.settings import CONFIG, TRADINGVIEW_SESSION_SIGN, TRADINGVIEW_SESSIONID

# Windows TradingView actually populates. `average_volume_20d_calc` is accepted
# by the screener but returns null for every row, so a 20-day average — the
# window a trader would naturally ask for — is not obtainable from this feed.
VALID_AVG_WINDOWS = (10, 30, 60, 90)

# The states `in_play_rvol_schedules` may be keyed by. Three come from
# `src/bidask/session_state.py` and the fourth is the crypto board, which has
# no session state at all.
#
# This module cannot import `rvol_at_time` to read `SCHEDULE_STATES` from it:
# that module reaches `tvquote`, which imports `cookie_jar` from here, so the
# import would be a cycle. `tests/test_bidask_rvol_at_time.py` pins the two
# tuples equal instead — the same arrangement `feed.SESSION_LABELS` and
# `session_state.SESSION_STATES` already live under.
RVOL_SCHEDULE_STATES = ("pre_market", "market", "post_market", "crypto")

# Keys that no longer mean anything, each naming what replaced it. Ignoring one
# would silently drop a leg of the gate — which is precisely how the previous
# gate's volume leg went missing — so `load_config` raises instead.
RETIRED_KEYS = {
    "in_play_min_rvol":
        "floored the screener's raw `relative_volume_10d_calc`, which is "
        "session-to-date volume over a FULL-DAY average and therefore a "
        "different filter every hour",
    "in_play_min_volume_pace":
        "divided that raw figure by a market-wide intraday volume curve, which "
        "still assumes every ticker shares the market's shape",
    "in_play_rvol_schedule":
        "carried ONE stepped schedule, written in minutes since 09:30. The "
        "board now runs pre-market and after hours too, and each state has its "
        "own floor",
    "in_play_min_change_pct":
        "admitted a ticker on an absolute price move alone. No ticker reaches "
        "either column on price now: relative volume is the only admission "
        "path, however far a stock has run",
}


@dataclass(frozen=True)
class BidAskConfig:
    poll_seconds: int
    min_today_dollar_vol: float
    min_avg_dollar_vol: float
    min_avg_volume: float
    avg_window_days: int
    # Stepped floors on Relative Volume at Time, one schedule per session
    # state: ((state, ((minutes, floor), ...)), ...). A tuple rather than a
    # dict so the frozen config stays hashable; `threshold_for` takes either
    # shape. Each schedule is written in minutes since ITS OWN state began —
    # the regular session's bands count from 09:30, not from the 04:00 anchor
    # the measure itself uses. NOT the screener's raw
    # `relative_volume_10d_calc`, which divides by a full-day average. See
    # `src/bidask/rvol_at_time.py`.
    in_play_rvol_schedules: tuple
    in_play_rvol_sessions: int
    band_frac: float
    max_spread_pct: float
    open_auction_minutes: int
    close_auction_minutes: int
    winsor_multiple: float
    # Minutes of tape each hit counter covers. 0 disables the window.
    hit_window_minutes: float
    min_hits_to_show: int
    max_rows_per_column: int
    max_rows_per_group: int
    # Theme scoring. The cap bounds one extreme member so it cannot decide the
    # whole ranking; the breadth coefficient and its minimum member count set
    # how much a theme gains for having more than one name in play. See
    # `src/bidask/grouping.py`.
    group_rvol_cap: float
    group_breadth_coef: float
    group_breadth_min_members: int
    min_poll_seconds: int
    max_poll_seconds: int
    crypto_exclude: FrozenSet[str] = frozenset()

    def clamp_poll_seconds(self, seconds) -> int:
        """Bound a requested cadence. The floor protects the vendor endpoint."""
        try:
            value = int(seconds)
        except (TypeError, ValueError):
            return self.poll_seconds
        return max(self.min_poll_seconds, min(self.max_poll_seconds, value))

    @property
    def avg_volume_field(self) -> str:
        """The screener column supplying the average-volume figure."""
        return f"average_volume_{self.avg_window_days}d_calc"


def load_config(overrides: Optional[dict] = None) -> BidAskConfig:
    """Build the config from YAML, applying optional overrides.

    Raises ValueError on an averaging window the feed cannot serve, rather than
    letting a silently-null column poison every downstream liquidity filter, and
    on any retired key, malformed schedule band, or unknown session state. Every
    one of those, ignored, removes part of the gate with nothing on screen to
    show it — which reads as a quiet market rather than as a broken config.
    """
    raw = dict(CONFIG.get("bidask") or {})
    if overrides:
        raw.update({k: v for k, v in overrides.items() if v is not None})

    # Every retired key raises and names its replacement. A leftover key that
    # is merely ignored disables part of the gate with nothing on screen to
    # show it, and the board then looks like a quiet market.
    for retired, why in RETIRED_KEYS.items():
        if retired in raw:
            raise ValueError(
                f"bidask.{retired} {why}. It is replaced by "
                "bidask.in_play_rvol_schedules, one stepped floor on Relative "
                "Volume at Time per session state (this ticker's volume since "
                "the 04:00 anchor over its own average by the same time of "
                "day). Update the key in config/workflow_config.yaml."
            )

    window = int(raw.get("avg_window_days", 30))
    if window not in VALID_AVG_WINDOWS:
        raise ValueError(
            f"avg_window_days={window} is not available from the TradingView "
            f"screener. Valid windows: {', '.join(map(str, VALID_AVG_WINDOWS))}. "
            "(A 20-day window is accepted by the API but returns null.)"
        )

    def _window_minutes(value) -> float:
        """Minutes of tape per hit counter. 0 disables the window.

        Raises rather than coercing. This value reaches the state payload, which
        is serialized with `allow_nan=False`, and a NaN there costs the whole
        document rather than one field. A negative value would prune every
        observation the moment it was recorded, emptying the board with no
        visible cause.
        """
        if value is None:
            return 0.0
        # `float(True)` is 1.0, so YAML `hit_window_minutes: true` — a natural
        # way to write "yes, enable it" against a key documented as "0
        # disables" — would silently become a one-minute horizon. At a 10s
        # cadence that is ~6 observations per ticker, below `min_hits_to_show`,
        # so the columns empty with no error. `_schedule` raises on malformed
        # input for the same reason.
        if isinstance(value, bool):
            raise ValueError(
                f"bidask.hit_window_minutes={value!r} is a boolean. Give a "
                "number of minutes (0 disables the window)."
            )
        try:
            minutes = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"bidask.hit_window_minutes={value!r} is not a number."
            ) from exc
        if not math.isfinite(minutes) or minutes < 0:
            raise ValueError(
                f"bidask.hit_window_minutes={value!r} must be a finite, "
                "non-negative number of minutes (0 disables the window)."
            )
        return minutes

    def _bands(state: str, value) -> tuple:
        """Normalise one state's stepped floors, sorted by minute mark.

        A malformed entry raises rather than being skipped: a silently dropped
        band is a hole in the gate at exactly one time of day, which is close to
        impossible to notice from the board.

        An empty schedule raises too. The gate has no off switch — relative
        volume is the only admission path to either column — so an empty list
        is not "no floor here", it is a state that admits nothing all window.
        That reads on screen as a dead market rather than as a config mistake.
        """
        if not value:
            raise ValueError(
                f"bidask.in_play_rvol_schedules.{state} is empty. Relative "
                "volume is the only admission path to either column, so a "
                "state with no floor admits nothing for its whole window. "
                "Give it at least one [minutes, floor] band."
            )
        bands = []
        for entry in value:
            try:
                minutes, floor = entry
                bands.append((float(minutes), float(floor)))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"bidask.in_play_rvol_schedules.{state} entry {entry!r} is "
                    "not a [minutes, floor] pair."
                ) from exc
        return tuple(sorted(bands))

    def _schedules(value) -> tuple:
        """Normalise the per-state schedules into a hashable tuple of pairs.

        An unknown state key raises. A typo is a state with no floor, and a
        state with no floor is invisible from the board until that window comes
        round — the pre-market block misspelled `premarket` would look perfect
        all day and admit nothing at 07:00.
        """
        if not value:
            return ()
        try:
            items = sorted(dict(value).items())
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "bidask.in_play_rvol_schedules must map a session state to its "
                f"[[minutes, floor], ...] bands; got {value!r}."
            ) from exc
        unknown = [state for state, _ in items if state not in RVOL_SCHEDULE_STATES]
        if unknown:
            raise ValueError(
                f"bidask.in_play_rvol_schedules has no such state(s): "
                f"{', '.join(map(str, unknown))}. Valid keys are "
                f"{', '.join(RVOL_SCHEDULE_STATES)}."
            )
        return tuple((state, _bands(state, bands)) for state, bands in items)

    return BidAskConfig(
        poll_seconds=int(raw.get("poll_seconds", 10)),
        min_today_dollar_vol=float(raw.get("min_today_dollar_vol", 1_000_000)),
        min_avg_dollar_vol=float(raw.get("min_avg_dollar_vol", 10_000_000)),
        min_avg_volume=float(raw.get("min_avg_volume", 750_000)),
        avg_window_days=window,
        in_play_rvol_schedules=_schedules(raw.get("in_play_rvol_schedules")),
        in_play_rvol_sessions=int(raw.get("in_play_rvol_sessions", 10)),
        band_frac=float(raw.get("band_frac", 0.30)),
        max_spread_pct=float(raw.get("max_spread_pct", 2.0)),
        open_auction_minutes=int(raw.get("open_auction_minutes", 15)),
        close_auction_minutes=int(raw.get("close_auction_minutes", 5)),
        winsor_multiple=float(raw.get("winsor_multiple", 10.0)),
        hit_window_minutes=_window_minutes(raw.get("hit_window_minutes", 30.0)),
        min_hits_to_show=int(raw.get("min_hits_to_show", 3)),
        max_rows_per_column=int(raw.get("max_rows_per_column", 60)),
        max_rows_per_group=int(raw.get("max_rows_per_group", 12)),
        group_rvol_cap=float(raw.get("group_rvol_cap", 5.0)),
        group_breadth_coef=float(raw.get("group_breadth_coef", 0.5)),
        group_breadth_min_members=int(raw.get("group_breadth_min_members", 2)),
        min_poll_seconds=int(raw.get("min_poll_seconds", 3)),
        max_poll_seconds=int(raw.get("max_poll_seconds", 120)),
        crypto_exclude=frozenset(
            str(s).strip().upper() for s in (raw.get("crypto_exclude") or []) if str(s).strip()
        ),
    )


def cookie_jar() -> dict:
    """Cookies for the screener request; empty when unconfigured.

    An empty jar is a normal degraded state, not an error: the scan still
    returns rows, just on the 15-minute delayed feed.
    """
    if not TRADINGVIEW_SESSIONID:
        return {}
    jar = {"sessionid": TRADINGVIEW_SESSIONID}
    if TRADINGVIEW_SESSION_SIGN:
        jar["sessionid_sign"] = TRADINGVIEW_SESSION_SIGN
    return jar
