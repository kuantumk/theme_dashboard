"""Relative Volume at Time — per ticker, against its own history.

The question this answers is TradingView's "Relative Volume at Time" in
**Cumulative** mode: how does the volume this ticker has traded since the
session opened compare with the volume *it* had usually traded by this same
point in the day?

    rvol_at_time(t) = volume today from 09:30 to t
                      / mean over the last N sessions of that same 09:30-to-t sum

Both legs are cumulative from the session anchor and both are cut at the same
time of day, so the comparison is like for like at every moment of the session.

Why not the screener's own figure
---------------------------------
`relative_volume_10d_calc` divides session-to-date volume by the 10-day average
**full-day** volume, with no time-of-day adjustment — verified, not assumed:
across 59 liquid tickers the implied divisor matched the true 10-day average
daily volume to a median error of 0.02%. A fixed floor on it is a different
filter every hour (16x normal participation demanded at 09:35, 1.8x at 15:00).

An earlier fix divided that figure by a market-wide median volume curve. That
is closer, but it is still not like for like: it assumes every ticker shares
the market's intraday shape, and a ticker drawing unusual interest is exactly
the case where its shape departs from the market's. Dividing by the ticker's
own history removes the assumption entirely.

`relative_volume_intraday|5` exists in the screener metainfo and may be this
figure, but its semantics are undocumented and it did not behave like a
cumulative measure when probed after the close (AVGO read 2.26 against a
full-day 1.75). This repo has already lost a season to a TradingView field
whose behaviour was assumed rather than verified, so it stays unused until
someone validates it against this module during a live session.

Cost
----
The historical leg depends only on completed sessions, so it is computed once
per session and cached — not per poll. A poll then costs one division. Building
the whole liquidity-filtered universe (~1,900 tickers) takes about 1.7 minutes
in batches, which finishes before the opening bell when the dashboard is
launched pre-market, and a same-day restart reads the cache instead.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

import numpy as np

ET = ZoneInfo("America/New_York")

SESSION_OPEN_MIN = 9 * 60 + 30   # 09:30 ET
SESSION_CLOSE_MIN = 16 * 60      # 16:00 ET
SESSION_MINUTES = SESSION_CLOSE_MIN - SESSION_OPEN_MIN  # 390

BAR_MINUTES = 5
BARS_PER_SESSION = SESSION_MINUTES // BAR_MINUTES  # 78

# Sessions of history behind the average. Matches the 10 the screener's own
# relative-volume field uses, so the two figures stay comparable in scale.
DEFAULT_SESSIONS = 10

# A session needs most of its bars present to be averaged in. A half day or a
# partly-downloaded session would otherwise drag the baseline down and inflate
# every pace computed against it.
MIN_BARS_FOR_SESSION = 60

CACHE_VERSION = 2


def minutes_since_open(now: Optional[datetime] = None) -> float:
    """Minutes elapsed in the regular session; 0 before the open, 390 after."""
    moment = now or datetime.now(tz=ET)
    moment = moment.astimezone(ET) if moment.tzinfo else moment.replace(tzinfo=ET)
    elapsed = (moment.hour * 60 + moment.minute + moment.second / 60.0) - SESSION_OPEN_MIN
    return max(0.0, min(float(SESSION_MINUTES), elapsed))


def baseline_at(profile, elapsed_minutes: float) -> float:
    """Expected cumulative volume by `elapsed_minutes`, from a ticker's profile.

    `profile` is cumulative volume at each 5-minute boundary. Today's figure
    arrives continuously from the screener, so comparing it against a step
    function would swing the ratio across every bar edge. Interpolating within
    the bar keeps both legs on the same footing.
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
    """Today's session volume over what this ticker usually had by now.

    Returns 0.0 — never 1.0 — when the reading or the baseline is unusable. An
    unknown must not pass a floor as though it had qualified; this is the same
    fail-closed rule the classifier's quote guards follow.
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


def build_profiles(bars_by_symbol: dict, sessions: int = DEFAULT_SESSIONS) -> dict:
    """Average each symbol's cumulative intraday volume across recent sessions.

    `bars_by_symbol` maps a symbol to a DataFrame of 5-minute regular-session
    bars with a timezone-aware index and a `Volume` column.
    """
    profiles = {}
    for symbol, frame in bars_by_symbol.items():
        curves = []
        for _, day in frame.groupby(frame.index.date):
            if len(day) < MIN_BARS_FOR_SESSION:
                continue
            slots = np.zeros(BARS_PER_SESSION, dtype=float)
            for stamp, volume in zip(day.index, day["Volume"].to_numpy(dtype=float)):
                if not math.isfinite(volume):
                    continue
                offset = (stamp.hour * 60 + stamp.minute) - SESSION_OPEN_MIN
                index = int(offset // BAR_MINUTES)
                if 0 <= index < BARS_PER_SESSION:
                    slots[index] += volume
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
    wrong for every ticker rather than visibly absent for all of them.
    """
    try:
        raw = json.loads(cache_path(out_dir, session_date).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if raw.get("version") != CACHE_VERSION or raw.get("session_date") != session_date:
        return {}
    if raw.get("bar_minutes") != BAR_MINUTES:
        return {}
    return {s: np.asarray(curve, dtype=float) for s, curve in (raw.get("profiles") or {}).items()}


def prune_cache(out_dir: Path, keep: str) -> None:
    """Drop baselines from previous sessions; only today's is ever read."""
    for path in Path(out_dir).glob("rvol_baselines_*.json"):
        if path.name != f"rvol_baselines_{keep}.json":
            path.unlink(missing_ok=True)


# ── history download ─────────────────────────────────────────────

def fetch_bars(symbols: Iterable[str], lookback_days: int = 15, batch: int = 100) -> dict:
    """Download 5-minute regular-session bars for the given symbols.

    yfinance is used rather than Alpaca because it needs no credentials — the
    tape dashboard's `.env` carries only TradingView keys — and because this
    runs once per session rather than per poll, so throughput matters more than
    latency. Extended-hours bars are excluded to match the screener's `volume`,
    which is regular-session-to-date.
    """
    import pandas as pd
    import yfinance as yf

    symbols = [s for s in dict.fromkeys(symbols) if s]
    collected = {}
    for start in range(0, len(symbols), batch):
        chunk = symbols[start:start + batch]
        try:
            data = yf.download(chunk, period=f"{lookback_days}d", interval="5m",
                               progress=False, prepost=False, group_by="ticker",
                               auto_adjust=False, threads=True)
        except Exception:  # noqa: BLE001 — one bad batch must not end the warm-up
            continue
        if data is None or data.empty:
            continue
        for symbol in chunk:
            try:
                frame = data[symbol] if isinstance(data.columns, pd.MultiIndex) else data
            except KeyError:
                continue
            frame = frame.dropna(subset=["Volume"])
            if frame.empty:
                continue
            index = frame.index
            if index.tz is None:
                index = index.tz_localize("UTC")
            frame = frame.copy()
            frame.index = index.tz_convert(ET)
            frame = frame.between_time("09:30", "15:59")
            if not frame.empty:
                collected[symbol] = frame
    return collected


def build_for_symbols(symbols: Iterable[str], sessions: int = DEFAULT_SESSIONS) -> dict:
    """Fetch history and reduce it to one cumulative-volume profile per symbol."""
    lookback = max(10, sessions * 3 // 2 + 5)
    return build_profiles(fetch_bars(symbols, lookback_days=lookback), sessions=sessions)
