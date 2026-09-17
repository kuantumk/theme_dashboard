"""Relative Volume at Time — per ticker, against its own history.

The question this answers is TradingView's "Relative Volume at Time" in
**Cumulative** mode: how does the volume this ticker has traded since the
session anchor compare with the volume *it* had usually traded by this same
point in the day?

    rvol_at_time(t) = volume today from 04:00 to t
                      / mean over the last N sessions of that same 04:00-to-t sum

Both legs are cumulative from the anchor and both are cut at the same time of
day, so the comparison is like for like at every moment.

Why the anchor is 04:00 and not 09:30
-------------------------------------
The board runs pre-market and after hours, so the measure has to mean something
in all three windows. `ta.relativeVolume(10, "1D", cumulative)` on an
extended-hours chart anchors its 1D period at **04:00**, and that is the figure
the user reads off the app and wrote the floors against. The regular session is
not a special case — it is the same running total, seen later in the day.

Confirmed against the app for GNRC on 2026-09-17: the plot climbs through
pre-market to roughly 100 and then **falls** to 33.50 at 09:30, because the
numerator keeps accruing while the denominator jumps as the regular-session
open enters the 10-day average. That discontinuity is the signature of a
time-of-day denominator. Our own computation gave 31.19 at 10:21 ET against the
33.50 read off the chart minutes earlier on a declining curve.

Why no screener column answers this
-----------------------------------
`relative_volume_10d_calc` is `volume / average_volume_10d_calc` in every
session state — measured to a 3.10% median error against the published inputs.
Mid-session the numerator is a partial day, so a fixed floor on it is a
different filter every hour. Before the open it is still *yesterday's*
completed day: across 300 symbols over a 201-second gap, pre-market volume rose
for 262 while that field and every `relative_volume_intraday|N` variant changed
for **0 of 300**. `premarket_volume / average_volume_10d_calc` is not a
substitute either — it divides by a normal full day rather than by this
ticker's usual pre-market, and for GNRC it read 0.267 where the app read ~100,
a factor of 374 that differs per ticker and so cannot be recalibrated away.

Why the bars come from the chart socket
---------------------------------------
yfinance serves the extended-hours bars with **zero volume on every one**, so a
baseline built from it is a zero denominator rather than a quiet ticker. See
`src/bidask/tvbars.py`, which is the only route to real extended-hours volume
here.

Cost
----
The historical leg depends only on completed sessions, so it is computed once
per session and cached — not per poll. A poll then costs one division.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

import numpy as np

from src.bidask.tvbars import fetch_bars

ET = ZoneInfo("America/New_York")

# The extended trading day. 04:00 is the anchor TradingView's own 1D period
# uses on an extended-hours chart; 20:00 closes the post-market window.
SESSION_OPEN_MIN = 4 * 60        # 04:00 ET
SESSION_CLOSE_MIN = 20 * 60      # 20:00 ET
SESSION_MINUTES = SESSION_CLOSE_MIN - SESSION_OPEN_MIN  # 960

# The regular session inside it. Used only to judge whether a historical
# session is complete enough to average in — never as an anchor.
REGULAR_OPEN_MIN = 9 * 60 + 30   # 09:30 ET
REGULAR_CLOSE_MIN = 16 * 60      # 16:00 ET

BAR_MINUTES = 5
BARS_PER_SESSION = SESSION_MINUTES // BAR_MINUTES  # 192

# Sessions of history behind the average. Matches the 10 the screener's own
# relative-volume field uses, so the two figures stay comparable in scale.
DEFAULT_SESSIONS = 10

# A session needs most of its **regular-session** bars present to be averaged
# in. Counting bars over the whole 04:00-20:00 grid instead would drop every
# quiet name's entire history: most extended windows hold no trade, so the feed
# returns no bar for them, and a full session of a normal stock carries well
# under 192 bars. A ticker whose history is all dropped has no baseline, scores
# 0 and is excluded — which would read as a dead universe rather than as a bad
# completeness rule. A half day still fails this, which is the point.
MIN_REGULAR_BARS_FOR_SESSION = 60

# Bumped for the 04:00 anchor. A curve written under the 09:30 anchor has 78
# slots meaning different clock times, so reading one would misdate every
# lookup rather than fail — hence a version check rather than a length check.
CACHE_VERSION = 3


def minutes_since_open(now: Optional[datetime] = None) -> float:
    """Minutes elapsed in the extended session; 0 before 04:00, 960 after 20:00."""
    moment = now or datetime.now(tz=ET)
    moment = moment.astimezone(ET) if moment.tzinfo else moment.replace(tzinfo=ET)
    elapsed = (moment.hour * 60 + moment.minute + moment.second / 60.0) - SESSION_OPEN_MIN
    return max(0.0, min(float(SESSION_MINUTES), elapsed))


def baseline_at(profile, elapsed_minutes: float) -> float:
    """Expected cumulative volume by `elapsed_minutes`, from a ticker's profile.

    `profile` is cumulative volume at each 5-minute boundary from 04:00. Today's
    figure arrives continuously from the screener, so comparing it against a
    step function would swing the ratio across every bar edge. Interpolating
    within the bar keeps both legs on the same footing.
    """
    if profile is None or len(profile) == 0:
        return 0.0
    position = max(0.0, min(float(elapsed_minutes), float(SESSION_MINUTES))) / BAR_MINUTES
    if position <= 0:
        return 0.0
    low = int(math.floor(position))
    if low >= len(profile):
        return float(profile[-1])
    lower = float(profile[low - 1]) if low > 0 else 0.0
    upper = float(profile[low])
    return lower + (upper - lower) * (position - low)


def rvol_at_time(volume_so_far, profile, elapsed_minutes: float) -> float:
    """Today's volume since 04:00 over what this ticker usually had by now.

    Returns 0.0 — never 1.0 — when the reading or the baseline is unusable. An
    unknown must not pass a floor as though it had qualified.

    A genuinely zero baseline also scores 0 rather than infinity. A ticker that
    has never traded before the open and suddenly does is the strongest signal
    this measure could carry, and it is the one case the measure cannot express
    — an accepted cost of the fail-closed rule. It is rare in a liquidity-gated
    universe, because the denominator is a mean over ten sessions and the
    cumulative curve at any pre-market minute is a sum over hours.
    """
    try:
        traded = float(volume_so_far)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(traded) or traded <= 0:
        return 0.0
    expected = baseline_at(profile, elapsed_minutes)
    if expected <= 0:
        return 0.0
    return traded / expected


def _is_extended(minute_of_day: int) -> bool:
    return not (REGULAR_OPEN_MIN <= minute_of_day < REGULAR_CLOSE_MIN)


def extended_volume_is_dead(frame) -> bool:
    """True when extended-hours bars exist and not one carries real volume.

    This is the yfinance shape, and it is the one failure that must never
    become a baseline: a zero denominator makes every pre-market ratio
    infinite, where a rejected symbol merely scores 0 and is excluded.

    A ticker with **no** extended-hours bars at all is not dead — a 5-minute
    window in which nothing traded returns no bar, which is the ordinary shape
    of a quiet pre-market. Only a bar that exists and reads zero is evidence
    the source is broken.
    """
    if frame is None or len(frame) == 0:
        return False
    seen = False
    for stamp, volume in zip(frame.index, frame["Volume"].to_numpy(dtype=float)):
        if not _is_extended(stamp.hour * 60 + stamp.minute):
            continue
        seen = True
        if math.isfinite(volume) and volume > 0:
            return False
    return seen


def build_profiles(bars_by_symbol: dict, sessions: int = DEFAULT_SESSIONS,
                   exclude_date=None) -> dict:
    """Average each symbol's cumulative extended-session volume across sessions.

    `bars_by_symbol` maps a symbol to a DataFrame of 5-minute extended-session
    bars with a timezone-aware ET index and a `Volume` column.

    `exclude_date` drops one session — today's — because the baseline is the
    ten *completed* sessions behind it. Left in, the current session would
    appear in its own denominator and damp exactly the reading the board exists
    to catch.

    A symbol whose extended-hours bars all read zero is omitted entirely rather
    than given a regular-session-only curve; see `extended_volume_is_dead`.
    """
    skip = str(exclude_date) if exclude_date is not None else None
    profiles = {}
    for symbol, frame in bars_by_symbol.items():
        if frame is None or len(frame) == 0:
            continue
        if extended_volume_is_dead(frame):
            continue
        curves = []
        for day, rows in frame.groupby(frame.index.date):
            if skip is not None and str(day) == skip:
                continue
            slots = np.zeros(BARS_PER_SESSION, dtype=float)
            regular_bars = 0
            for stamp, volume in zip(rows.index, rows["Volume"].to_numpy(dtype=float)):
                minute_of_day = stamp.hour * 60 + stamp.minute
                if not _is_extended(minute_of_day):
                    regular_bars += 1
                if not math.isfinite(volume):
                    continue
                index = (minute_of_day - SESSION_OPEN_MIN) // BAR_MINUTES
                if 0 <= index < BARS_PER_SESSION:
                    slots[index] += volume
            if regular_bars < MIN_REGULAR_BARS_FOR_SESSION:
                continue
            curves.append(np.cumsum(slots))
        if not curves:
            continue
        stacked = np.vstack(curves[-sessions:])
        averaged = stacked.mean(axis=0)
        if averaged[-1] <= 0:
            continue
        profiles[symbol] = averaged
    return profiles


# ── threshold schedule ───────────────────────────────────────────

def threshold_for(schedule, elapsed_minutes: float) -> Optional[float]:
    """The floor that applies this many minutes into the session.

    `schedule` is [[minutes, floor], ...] ascending. The floor for a band holds
    from its own minute mark until the next one. Early in the session the
    denominator is small and the ratio is noisy, which is why the floors start
    loose and tighten: an unusual reading at 09:35 is worth less than the same
    reading at 10:30.
    """
    if not schedule:
        return None
    applicable = None
    for entry in schedule:
        minutes, floor = float(entry[0]), float(entry[1])
        if elapsed_minutes >= minutes:
            applicable = floor
        else:
            break
    # Before the first band starts, the first band's floor still applies —
    # a stepped schedule must never leave a window with no floor at all,
    # which would admit the whole universe on the opening print.
    return applicable if applicable is not None else float(schedule[0][1])


# ── on-disk cache ────────────────────────────────────────────────

def cache_path(out_dir: Path, session_date: str) -> Path:
    return Path(out_dir) / f"rvol_baselines_{session_date}.json"


def save_profiles(profiles: dict, out_dir: Path, session_date: str) -> bool:
    """Persist the session's baselines so a restart does not refetch."""
    path = cache_path(out_dir, session_date)
    payload = {
        "version": CACHE_VERSION,
        "session_date": session_date,
        "bar_minutes": BAR_MINUTES,
        "anchor_minutes": SESSION_OPEN_MIN,
        # Rounded to whole shares: the figures are volume averages in the
        # millions, so the decimals are noise and they triple the file size.
        "profiles": {s: [int(round(v)) for v in curve] for s, curve in profiles.items()},
    }
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        tmp.replace(path)
        return True
    except OSError:
        tmp.unlink(missing_ok=True)
        return False


def load_profiles(out_dir: Path, session_date: str) -> dict:
    """Read today's cached baselines. Any mismatch returns empty, never stale.

    A baseline from another session is worse than none: it would be silently
    wrong for every ticker rather than visibly absent for all of them. The
    anchor is checked alongside the version for the same reason — a curve
    written from 09:30 has every slot at the wrong clock time and would read
    without error.
    """
    try:
        raw = json.loads(cache_path(out_dir, session_date).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if raw.get("version") != CACHE_VERSION or raw.get("session_date") != session_date:
        return {}
    if raw.get("bar_minutes") != BAR_MINUTES:
        return {}
    if raw.get("anchor_minutes") != SESSION_OPEN_MIN:
        return {}
    return {s: np.asarray(curve, dtype=float) for s, curve in (raw.get("profiles") or {}).items()}


def prune_cache(out_dir: Path, keep: str) -> None:
    """Drop baselines from previous sessions; only today's is ever read."""
    for path in Path(out_dir).glob("rvol_baselines_*.json"):
        if path.name != f"rvol_baselines_{keep}.json":
            path.unlink(missing_ok=True)


# ── history download ─────────────────────────────────────────────

def bar_count_for(sessions: int) -> int:
    """Bars to request so `sessions` complete extended days fit, with slack.

    Three spare sessions cover today's partial day, a holiday inside the
    window, and the fact that a continuously-traded name fills more of the
    192-slot grid than a quiet one.
    """
    return max(1, (int(sessions) + 3) * BARS_PER_SESSION)


def build_for_symbols(symbols: Iterable[str], sessions: int = DEFAULT_SESSIONS,
                      *, exclude_date=None, **kwargs) -> dict:
    """Fetch history and reduce it to one cumulative-volume profile per symbol.

    Today's session is excluded by default — the baseline is the completed
    sessions behind it. Pass `exclude_date=False` only to score a historical
    day, where "today" is not in the data at all.
    """
    if exclude_date is None:
        exclude_date = datetime.now(tz=ET).date()
    elif exclude_date is False:
        exclude_date = None
    bars = fetch_bars(symbols, bars=bar_count_for(sessions), **kwargs)
    return build_profiles(bars, sessions=sessions, exclude_date=exclude_date)
