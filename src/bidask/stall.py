"""Detect a vendor volume column that is populated but no longer advancing.

⛔ This covers the one failure the rest of the board cannot see. Every other
guard here keys off ABSENCE: `RvolGate.source_unavailable` needs every reading
to be zero, `_drop_non_finite` needs a NaN, the warm-up guard needs a raise or
an empty result. A column that keeps serving plausible numbers and simply stops
moving trips none of them.

It is not a hypothetical shape for this vendor. Two instances are measured:

* `volume` holds the PREVIOUS completed session before the bell — median
  2,213,074 against a `premarket_volume` median of 6,448 — which is why
  `universe.VOLUME_FIELDS` is a table rather than one column.
* `relative_volume_intraday|5` stayed non-null on 2,805 of 2,805 rows after the
  close while changing for 0 of 250 symbols across a 240-second gap.

Both were found by running a two-poll movement check by hand. This is that
check, moved into the server, because the next instance will not announce
itself either.

⛔ A frozen numerator DRAINS the board rather than holding it still.
`rvol_at_time.baseline_at` reads the baseline at the current minute, so the
denominator keeps growing with the clock while the numerator does not. Every
reading falls, names slip back under the floor one at a time, and the columns
empty over the morning — ending at `emptyReason`'s last branch, which says the
source is working and the market is quiet.

⛔ DIAGNOSTIC ONLY. Nothing here may change a score, a side, or an admission.
The engine is stateless by design: every side and every score is recomputed
from the current poll, so there is no counter to age out and no accumulated
bias to bound. This module is the one deliberate exception, and it earns it by
touching only what the page SAYS. If a later edit reads a `StallReading` to
filter, rank, or gate a row, that exception is gone and the design property
with it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Optional

# How far apart two readings must be before they are worth comparing.
#
# ⛔ Not the poll cadence. Polls land every 10 seconds, and a thin pre-market
# genuinely prints nothing across the whole universe for that long, so
# comparing consecutive polls would accuse the vendor every quiet minute before
# 05:00. 120s matches the order of the gap the recorded probes used (240s and
# 201s), where zero movement was conclusive.
DEFAULT_WINDOW_SECONDS = 120.0

# Consecutive still windows before the page says anything. Two windows is four
# minutes of a completely motionless universe. One window alone would publish a
# vendor accusation off a single unlucky sample.
DEFAULT_STRIKES = 2

# Symbols that must be carrying volume before a verdict is possible. A handful
# of names trading at 04:02 is a thin morning, not a dead column, and the whole
# point of this module is to stop the board blaming the wrong thing.
DEFAULT_MIN_ACTIVE = 25


@dataclass(frozen=True)
class StallReading:
    """One poll's verdict on whether the numerator column is still advancing.

    `watched` is 0 whenever no comparison happened — inside the window, on the
    first reading of a state, or with too few active symbols to judge. That is
    a distinct outcome from "compared and found moving", and the page must not
    render silence as evidence either way.
    """

    stalled: bool       # two or more consecutive still windows
    seconds: float      # how long the column has been still, 0 when moving
    moved: int          # symbols whose volume changed in the last comparison
    watched: int        # symbols compared, or 0 when nothing was compared


@dataclass
class _Watch:
    """One market's reference reading."""

    state: Optional[str] = None
    values: dict = field(default_factory=dict)
    taken_at: float = 0.0
    last_moved_at: float = 0.0
    strikes: int = 0
    stalled: bool = False

    def anchor(self, state: str, values: dict, now: float) -> None:
        self.state = state
        self.values = values
        self.taken_at = now
        self.last_moved_at = now
        self.strikes = 0
        self.stalled = False


class StallWatch:
    """Per-market memory of whether the volume column is still moving.

    The only state carried between polls in this application besides the
    baselines, and deliberately so — see the module docstring.
    """

    def __init__(self, window_seconds: float = DEFAULT_WINDOW_SECONDS,
                 strikes: int = DEFAULT_STRIKES,
                 min_active: int = DEFAULT_MIN_ACTIVE):
        self.window_seconds = float(window_seconds)
        self.strikes = int(strikes)
        self.min_active = int(min_active)
        self._markets: dict = {}

    def observe(self, market: str, state: str,
                volumes: Mapping[str, float], now: float) -> StallReading:
        """Record this poll's numerators and say whether the column has frozen.

        `volumes` maps symbol to that row's volume since the anchor — the exact
        quantity the gate divides. Symbols reading zero are dropped: zero
        equals zero on every comparison, so counting them would convict the
        vendor of the market being shut.
        """
        active = {str(symbol): float(volume)
                  for symbol, volume in volumes.items()
                  if _positive(volume)}
        watch = self._markets.get(market)
        if watch is None:
            watch = self._markets[market] = _Watch()

        # ⛔ A state change swaps the column being read — `premarket_volume`
        # for `volume` at the bell. Comparing across that boundary measures the
        # swap, not the feed, so the reference is abandoned rather than carried.
        if watch.state != state:
            watch.anchor(state, active, now)
            return StallReading(False, 0.0, 0, 0)

        if not watch.values:
            watch.anchor(state, active, now)
            return StallReading(False, 0.0, 0, 0)

        if now - watch.taken_at < self.window_seconds:
            # Too soon to judge. The standing verdict persists so the pill does
            # not flicker between polls, but nothing was compared this time.
            return StallReading(watch.stalled, self._age(watch, now), 0, 0)

        common = [s for s in active if s in watch.values]
        if len(common) < self.min_active:
            # Not enough of the market is trading to tell a dead column from a
            # quiet one. Re-anchor rather than hold a reference that will age
            # into a false conviction the moment volume arrives.
            watch.anchor(state, active, now)
            return StallReading(False, 0.0, 0, 0)

        moved = sum(1 for s in common if active[s] != watch.values[s])
        watch.values = active
        watch.taken_at = now
        if moved:
            watch.strikes = 0
            watch.stalled = False
            watch.last_moved_at = now
            return StallReading(False, 0.0, moved, len(common))

        watch.strikes += 1
        watch.stalled = watch.strikes >= self.strikes
        return StallReading(watch.stalled, self._age(watch, now), 0, len(common))

    @staticmethod
    def _age(watch: _Watch, now: float) -> float:
        return max(0.0, now - watch.last_moved_at) if watch.stalled else 0.0


def _positive(volume) -> bool:
    try:
        return float(volume) > 0.0
    except (TypeError, ValueError):
        return False
