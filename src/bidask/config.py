"""Configuration for the bid/ask tape-pressure dashboard.

Every tunable lives in the ``bidask:`` block of ``config/workflow_config.yaml``
so thresholds are never hardcoded in the pipeline modules.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import FrozenSet, Optional

from config.settings import CONFIG, TRADINGVIEW_SESSION_SIGN, TRADINGVIEW_SESSIONID

# Windows TradingView actually populates. `average_volume_20d_calc` is accepted
# by the screener but returns null for every row, so a 20-day average — the
# window a trader would naturally ask for — is not obtainable from this feed.
VALID_AVG_WINDOWS = (10, 30, 60, 90)


@dataclass(frozen=True)
class BidAskConfig:
    poll_seconds: int
    min_today_dollar_vol: float
    min_avg_dollar_vol: float
    min_avg_volume: float
    avg_window_days: int
    # Stepped floors on Relative Volume at Time, as [minutes since 09:30, floor].
    # NOT the screener's raw `relative_volume_10d_calc`, which is divided by a
    # full-day average. See `src/bidask/rvol_at_time.py`.
    in_play_rvol_schedule: tuple
    in_play_rvol_sessions: int
    in_play_min_change_pct: Optional[float]
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
    letting a silently-null column poison every downstream liquidity filter.
    """
    raw = dict(CONFIG.get("bidask") or {})
    if overrides:
        raw.update({k: v for k, v in overrides.items() if v is not None})

    # The volume leg used to floor the screener's raw relative-volume figure,
    # which is session-to-date volume over a FULL-DAY average and therefore a
    # different filter every hour. Ignoring a leftover key would silently
    # disable the leg — the exact failure this replaced — so say so instead.
    for retired in ("in_play_min_rvol", "in_play_min_volume_pace"):
        if retired in raw:
            raise ValueError(
                f"bidask.{retired} has been replaced by "
                "bidask.in_play_rvol_schedule, a stepped floor on Relative "
                "Volume at Time (this ticker's volume since 09:30 over its own "
                "average by the same time of day). Update the key in "
                "config/workflow_config.yaml."
            )

    window = int(raw.get("avg_window_days", 30))
    if window not in VALID_AVG_WINDOWS:
        raise ValueError(
            f"avg_window_days={window} is not available from the TradingView "
            f"screener. Valid windows: {', '.join(map(str, VALID_AVG_WINDOWS))}. "
            "(A 20-day window is accepted by the API but returns null.)"
        )

    def _opt(key: str) -> Optional[float]:
        value = raw.get(key)
        return None if value is None else float(value)

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

    def _schedule(value) -> tuple:
        """Normalise the stepped floors, sorted by minute mark.

        An empty list disables the volume leg, matching how a null disables the
        change leg. A malformed entry raises rather than being skipped: a
        silently dropped band is a hole in the gate at exactly one time of day,
        which is close to impossible to notice from the board.
        """
        if not value:
            return ()
        bands = []
        for entry in value:
            try:
                minutes, floor = entry
                bands.append((float(minutes), float(floor)))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"bidask.in_play_rvol_schedule entry {entry!r} is not a "
                    "[minutes, floor] pair."
                ) from exc
        return tuple(sorted(bands))

    return BidAskConfig(
        poll_seconds=int(raw.get("poll_seconds", 10)),
        min_today_dollar_vol=float(raw.get("min_today_dollar_vol", 1_000_000)),
        min_avg_dollar_vol=float(raw.get("min_avg_dollar_vol", 10_000_000)),
        min_avg_volume=float(raw.get("min_avg_volume", 750_000)),
        avg_window_days=window,
        in_play_rvol_schedule=_schedule(raw.get("in_play_rvol_schedule")),
        in_play_rvol_sessions=int(raw.get("in_play_rvol_sessions", 10)),
        in_play_min_change_pct=_opt("in_play_min_change_pct"),
        band_frac=float(raw.get("band_frac", 0.30)),
        max_spread_pct=float(raw.get("max_spread_pct", 2.0)),
        open_auction_minutes=int(raw.get("open_auction_minutes", 15)),
        close_auction_minutes=int(raw.get("close_auction_minutes", 5)),
        winsor_multiple=float(raw.get("winsor_multiple", 10.0)),
        hit_window_minutes=_window_minutes(raw.get("hit_window_minutes", 30.0)),
        min_hits_to_show=int(raw.get("min_hits_to_show", 3)),
        max_rows_per_column=int(raw.get("max_rows_per_column", 60)),
        max_rows_per_group=int(raw.get("max_rows_per_group", 12)),
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
