"""`build_profiles` must stay bit-exact while it gets faster.

Measured 2026-09-18 at full universe scale — 1,800 symbols x 2,496 bars, the
real shape of a warm-up — the reduction took **33.25s**, of which
`extended_volume_is_dead` was 2.47s. That was a tenth of the warm-up when the
fetch cost ~195s. Once batching and a smaller incremental fetch bring the
download near 49s, a 33s reduction is most of what is left.

⛔ This is a REWRITE of arithmetic the whole board depends on, so the tests
compare against a reference written from the SPECIFICATION rather than copied
from the implementation. A reference copied from the code under test agrees
with it by construction, including where both are wrong.

The spec, from `build_profiles`' own contract:

1. Cut days in the GRID's timezone, not the frame's.
2. Bucket each bar's volume into slot `(minute_of_day - anchor) // 5`.
3. Drop a bar outside the grid's day, and a non-finite volume.
4. Drop a day carrying fewer than `min_core_bars` bars inside the core window.
5. Drop `exclude_date` — today must not appear in its own denominator.
6. Average the LAST `sessions` surviving days, elementwise, after a cumulative
   sum within each day.
7. Drop a symbol whose averaged curve ends at zero, and one whose
   extended-hours bars all read zero.
"""

import math
import unittest

import numpy as np
import pandas as pd

from src.bidask.rvol_at_time import (BAR_MINUTES, CRYPTO_GRID, EQUITY_GRID,
                                     build_profiles, extended_volume_is_dead)


def reference_profiles(bars_by_symbol, sessions=10, exclude_date=None,
                       grid=EQUITY_GRID):
    """The contract above, written plainly. Slow on purpose."""
    skip = str(exclude_date) if exclude_date is not None else None
    out = {}
    for symbol, frame in bars_by_symbol.items():
        if frame is None or len(frame) == 0:
            continue
        if reference_dead(frame, grid):
            continue
        local = frame.index.tz_convert(grid.tz) if frame.index.tz else frame.index
        curves = []
        for day in sorted({stamp.date() for stamp in local}):
            if skip is not None and str(day) == skip:
                continue
            slots = np.zeros(grid.slots, dtype=float)
            core = 0
            for stamp, local_stamp, volume in zip(frame.index, local,
                                                  frame["Volume"]):
                if local_stamp.date() != day:
                    continue
                minute = local_stamp.hour * 60 + local_stamp.minute
                if grid.core_open_min <= minute < grid.core_close_min:
                    core += 1
                value = float(volume)
                if not math.isfinite(value):
                    continue
                slot = (minute - grid.anchor_min) // BAR_MINUTES
                if 0 <= slot < grid.slots:
                    slots[slot] += value
            if core < grid.min_core_bars:
                continue
            curves.append(np.cumsum(slots))
        if not curves:
            continue
        averaged = np.vstack(curves[-sessions:]).mean(axis=0)
        if averaged[-1] <= 0:
            continue
        out[symbol] = averaged
    return out


def reference_dead(frame, grid=EQUITY_GRID):
    local = frame.index.tz_convert(grid.tz) if frame.index.tz else frame.index
    seen = False
    for stamp, volume in zip(local, frame["Volume"]):
        minute = stamp.hour * 60 + stamp.minute
        if grid.core_open_min <= minute < grid.core_close_min:
            continue
        seen = True
        value = float(volume)
        if math.isfinite(value) and value > 0:
            return False
    return seen


def frame_for(stamps, volumes, tz="America/New_York"):
    index = pd.DatetimeIndex(pd.to_datetime(stamps)).tz_localize(tz)
    return pd.DataFrame({"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
                         "Volume": list(volumes)}, index=index)


def synthetic(rng, *, days, tz="America/New_York", start="04:00",
              periods=192, nan_rate=0.0, zero_extended=False):
    frames = []
    for day in range(days):
        date = pd.Timestamp("2026-09-01") + pd.Timedelta(days=day)
        index = pd.date_range(f"{date.date()} {start}", periods=periods,
                              freq=f"{BAR_MINUTES}min", tz=tz)
        volume = rng.integers(1, 90_000, periods).astype(float)
        if nan_rate:
            volume[rng.random(periods) < nan_rate] = np.nan
        if zero_extended:
            minutes = index.hour * 60 + index.minute
            volume[(minutes < 570) | (minutes >= 960)] = 0.0
        frames.append(pd.DataFrame(
            {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
             "Volume": volume}, index=index))
    return pd.concat(frames)


class TestTheRewriteMatchesTheSpecification(unittest.TestCase):
    """Randomised frames, compared elementwise against the plain reference."""

    def assert_same(self, bars, **kwargs):
        fast = build_profiles(bars, **kwargs)
        slow = reference_profiles(bars, **kwargs)
        self.assertEqual(sorted(fast), sorted(slow), "different symbols kept")
        for symbol in slow:
            np.testing.assert_allclose(
                fast[symbol], slow[symbol], rtol=0, atol=0,
                err_msg=f"{symbol} curve differs from the specification")

    def test_ordinary_equity_frames(self):
        rng = np.random.default_rng(11)
        self.assert_same({f"S{i}": synthetic(rng, days=13) for i in range(6)})

    def test_frames_carrying_absent_volume(self):
        """A withdrawn cell must be skipped, never folded in as a number."""
        rng = np.random.default_rng(12)
        self.assert_same({f"S{i}": synthetic(rng, days=12, nan_rate=0.15)
                          for i in range(6)})

    def test_short_days_are_dropped_the_same_way(self):
        """A half day carries too few core bars and must not be averaged."""
        rng = np.random.default_rng(13)
        bars = {"S0": pd.concat([synthetic(rng, days=11),
                                 synthetic(rng, days=1, start="04:00",
                                           periods=20)])}
        self.assert_same(bars)

    def test_the_excluded_day_is_dropped_the_same_way(self):
        rng = np.random.default_rng(14)
        self.assert_same({"S0": synthetic(rng, days=13)},
                         exclude_date="2026-09-07")

    def test_only_the_last_sessions_are_averaged(self):
        rng = np.random.default_rng(15)
        self.assert_same({"S0": synthetic(rng, days=20)}, sessions=10)
        self.assert_same({"S0": synthetic(rng, days=20)}, sessions=3)

    def test_the_crypto_grid_cuts_days_in_utc(self):
        """⛔ The frame arrives ET-indexed and the day must still be cut in UTC.

        `tvbars.bars_to_frame` returns an America/New_York index for BOTH
        markets, so this is the real shape of a crypto warm-up — not a UTC
        frame. Grouping on the raw index would split each crypto day at 00:00
        ET, every curve would straddle two UTC days, and the result would
        disagree with the `volume` numerator by four or five hours with nothing
        on screen to show it. A UTC-indexed fixture cannot catch that: the
        conversion is a no-op there.
        """
        rng = np.random.default_rng(16)
        bars = {}
        for i in range(4):
            frame = synthetic(rng, days=13, tz="UTC", start="00:00", periods=288)
            frame.index = frame.index.tz_convert("America/New_York")
            bars[f"C{i}"] = frame
        self.assert_same(bars, grid=CRYPTO_GRID)
        self.assertTrue(bars["C0"].index.tz.key == "America/New_York",
                        "the fixture must keep the ET index tvbars produces")

    def test_a_dst_boundary_does_not_shift_a_curve(self):
        """US clocks move on 2026-11-01; a day is still one local day."""
        rng = np.random.default_rng(17)
        index = pd.date_range("2026-10-28 04:00", periods=192 * 8,
                              freq=f"{BAR_MINUTES}min", tz="America/New_York")
        volume = rng.integers(1, 50_000, len(index)).astype(float)
        bars = {"S0": pd.DataFrame(
            {"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
             "Volume": volume}, index=index)}
        self.assert_same(bars)


class TestTheFailClosedRulesSurvive(unittest.TestCase):
    """The rejections matter more than the arithmetic — each one is a symbol
    that will score 0 and stay off the board rather than be admitted wrong."""

    def test_a_symbol_whose_extended_bars_all_read_zero_is_rejected(self):
        """The yfinance shape. A zero denominator makes every ratio infinite."""
        rng = np.random.default_rng(18)
        frame = synthetic(rng, days=12, zero_extended=True)
        self.assertTrue(extended_volume_is_dead(frame, EQUITY_GRID))
        self.assertEqual(build_profiles({"S0": frame}), {})

    def test_a_quiet_pre_market_is_not_a_dead_one(self):
        """No bar at all is ordinary thinness; a bar reading zero is evidence."""
        frame = frame_for(
            [f"2026-09-0{d} 09:{m:02d}" for d in range(1, 9) for m in (35, 40)],
            [5_000.0] * 16)
        self.assertFalse(extended_volume_is_dead(frame, EQUITY_GRID))

    def test_the_dead_check_agrees_with_the_specification(self):
        rng = np.random.default_rng(19)
        for zero in (True, False):
            for nan_rate in (0.0, 0.2):
                frame = synthetic(rng, days=4, nan_rate=nan_rate,
                                  zero_extended=zero)
                self.assertEqual(extended_volume_is_dead(frame, EQUITY_GRID),
                                 reference_dead(frame, EQUITY_GRID),
                                 f"zero={zero} nan_rate={nan_rate}")

    def test_an_empty_frame_is_neither_dead_nor_a_curve(self):
        empty = pd.DataFrame({"Volume": []},
                             index=pd.DatetimeIndex([], tz="America/New_York"))
        self.assertFalse(extended_volume_is_dead(empty, EQUITY_GRID))
        self.assertEqual(build_profiles({"S0": empty}), {})


class TestHandCheckedArithmetic(unittest.TestCase):
    """One curve small enough to verify by hand, so the reference is anchored."""

    def test_two_bars_accumulate_into_the_right_slots(self):
        grid = EQUITY_GRID
        # 04:00 is slot 0, 04:05 slot 1, 09:30 slot 66.
        stamps, volumes = [], []
        for day in range(1, 12):
            stamps += [f"2026-09-{day:02d} 04:00", f"2026-09-{day:02d} 04:05"]
            volumes += [100.0, 200.0]
            # Enough core bars for the day to count.
            for step in range(grid.min_core_bars):
                minute = 570 + step * BAR_MINUTES
                stamps.append(f"2026-09-{day:02d} "
                              f"{minute // 60:02d}:{minute % 60:02d}")
                volumes.append(0.0)
        curve = build_profiles({"S0": frame_for(stamps, volumes)})["S0"]
        self.assertEqual(curve[0], 100.0)
        self.assertEqual(curve[1], 300.0, "the curve is cumulative")
        self.assertEqual(curve[65], 300.0, "nothing traded until the open")
        self.assertEqual(curve[-1], 300.0)


if __name__ == "__main__":
    unittest.main()
