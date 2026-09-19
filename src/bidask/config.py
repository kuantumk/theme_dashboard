"""Configuration for the bid/ask tape-pressure dashboard.

Every tunable lives in the ``bidask:`` block of ``config/workflow_config.yaml``
so thresholds are never hardcoded in the pipeline modules.
"""

from __future__ import annotations

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
# that module reaches `tvbars` and then `tvsocket`, which imports `cookie_jar`
# from here, so the import would be a cycle. `tests/test_bidask_rvol_at_time.py` pins the two
# tuples equal instead — the same arrangement `feed.SESSION_LABELS` and
# `session_state.SESSION_STATES` already live under.
RVOL_SCHEDULE_STATES = ("pre_market", "market", "post_market", "crypto")

# Keys that no longer mean anything, each naming what replaced it. Ignoring one
# would silently drop a leg of the gate — which is precisely how the previous
# gate's volume leg went missing — so `load_config` raises instead.
#
# The second block retires the trade classifier and its accumulator. Those keys
# are inert rather than dangerous, but a config key with no reader is a claim
# that the board still does something it stopped doing, and a reader who tunes
# one gets no feedback at all. Raising is the only answer that reaches them.
RETIRED_KEYS = {
    "in_play_min_rvol":
        "floored the screener's raw `relative_volume_10d_calc`, which is "
        "session-to-date volume over a FULL-DAY average and therefore a "
        "different filter every hour. Use bidask.in_play_rvol_schedules, one "
        "stepped floor on Relative Volume at Time per session state.",
    "in_play_min_volume_pace":
        "divided that raw figure by a market-wide intraday volume curve, which "
        "still assumes every ticker shares the market's shape. Use "
        "bidask.in_play_rvol_schedules, which compares a ticker against its "
        "own history at the same time of day.",
    "in_play_rvol_schedule":
        "carried ONE stepped schedule, written in minutes since 09:30. The "
        "board now runs pre-market and after hours too, and each state has its "
        "own floor. Use bidask.in_play_rvol_schedules, keyed by session state.",
    "in_play_min_change_pct":
        "admitted a ticker on an absolute price move alone. No ticker reaches "
        "either column on price now: bidask.in_play_rvol_schedules is the only "
        "admission path, however far a stock has run.",
    "band_frac":
        "sized the CLNV band a print was classified against. The board no "
        "longer classifies trades: a ticker's side is its price against a "
        "session-appropriate reference, and nothing replaces this key.",
    "max_spread_pct":
        "rejected a quote wider than this share of the mid as stale. The board "
        "reads no quotes at all now — the equity quote socket went with the "
        "classifier — and nothing replaces this key.",
    "open_auction_minutes":
        "excluded the opening auction, whose crosses have no meaningful "
        "contemporaneous quote to classify against. A price-direction test has "
        "no such problem, so the window is gone and nothing replaces this key.",
    "close_auction_minutes":
        "excluded the closing auction, for the same reason as "
        "bidask.open_auction_minutes. Nothing replaces this key.",
    "winsor_multiple":
        "capped a poll's volume delta against its running median, bounding one "
        "misclassified print. The board sums no observations now, so there is "
        "no tail to cap and nothing replaces this key.",
    "hit_window_minutes":
        "bounded how much tape the per-ticker hit counters accumulated. The "
        "board is stateless — it recomputes from price and relative volume "
        "every poll — so nothing accumulates and nothing replaces this key.",
    "min_hits_to_show":
        "hid a ticker below this many classified observations. There are no "
        "hit counts now; a ticker reaches a column by clearing "
        "bidask.in_play_rvol_schedules, and the display caps are "
        "bidask.max_rows_per_column and bidask.max_rows_per_group.",
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
                f"bidask.{retired} {why} Remove the key from "
                "config/workflow_config.yaml."
            )

    window = int(raw.get("avg_window_days", 30))
    if window not in VALID_AVG_WINDOWS:
        raise ValueError(
            f"avg_window_days={window} is not available from the TradingView "
            f"screener. Valid windows: {', '.join(map(str, VALID_AVG_WINDOWS))}. "
            "(A 20-day window is accepted by the API but returns null.)"
        )

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
