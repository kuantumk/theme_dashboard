"""Relative Volume at Time tests.

The measure compares a ticker's volume since the **04:00** extended-session
anchor against the mean of its own 04:00-to-now volume over recent sessions, so
both legs are cut at the same time of day. The screener's
`relative_volume_10d_calc` divides by a full-day average instead, which is why a
fixed floor on it means something different every hour — and why it replays
yesterday's session entirely before the open.

`tests/test_bidask_rvol_extended.py` covers the extended-hours behaviours the
04:00 anchor exists for. This file covers the arithmetic they rest on.
"""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.bidask.rvol_at_time import (
    BARS_PER_SESSION,
    SESSION_MINUTES,
    baseline_at,
    build_profiles,
    load_profiles,
    minutes_since_open,
    prune_cache,
    rvol_at_time,
    save_profiles,
    threshold_for,
)

ET = ZoneInfo("America/New_York")
SCHEDULE = ((5.0, 0.8), (15.0, 1.0), (30.0, 1.2), (60.0, 1.5))

# Minutes from the 04:00 anchor to the regular open and close.
OPEN_AT = 330.0
CLOSE_AT = 720.0

# Divides evenly across the 192-slot grid, so the cache's whole-share rounding
# is exact and a round-trip test measures serialization rather than rounding.
DAY_VOLUME = 192_000


def at(hour, minute):
    return datetime(2026, 8, 14, hour, minute, tzinfo=ET)


def flat_profile(total=DAY_VOLUME):
    """A ticker that trades evenly through the whole extended day."""
    return np.cumsum(np.full(BARS_PER_SESSION, total / BARS_PER_SESSION))


class TestElapsed(unittest.TestCase):
    def test_counts_from_the_extended_anchor(self):
        self.assertEqual(minutes_since_open(at(4, 0)), 0.0)
        self.assertEqual(minutes_since_open(at(9, 30)), OPEN_AT)
        self.assertEqual(minutes_since_open(at(10, 0)), OPEN_AT + 30.0)
        self.assertEqual(minutes_since_open(at(15, 0)), OPEN_AT + 330.0)

    def test_clamped_outside_the_extended_day(self):
        self.assertEqual(minutes_since_open(at(3, 0)), 0.0)
        self.assertEqual(minutes_since_open(at(20, 0)), float(SESSION_MINUTES))

    def test_naive_datetime_is_read_as_eastern(self):
        self.assertEqual(minutes_since_open(datetime(2026, 8, 14, 10, 0)),
                         OPEN_AT + 30.0)


class TestBaselineLookup(unittest.TestCase):
    def test_interpolates_inside_a_bar(self):
        """Today's volume arrives continuously, so the baseline must too.

        Against a step function the ratio would jump at every bar edge.
        """
        profile = flat_profile()
        self.assertAlmostEqual(baseline_at(profile, 5), 1000.0)
        self.assertAlmostEqual(baseline_at(profile, 2.5), 500.0)
        self.assertAlmostEqual(baseline_at(profile, 10), 2000.0)

    def test_zero_at_the_anchor_and_full_at_the_extended_close(self):
        profile = flat_profile()
        self.assertEqual(baseline_at(profile, 0), 0.0)
        self.assertAlmostEqual(baseline_at(profile, SESSION_MINUTES), float(DAY_VOLUME))

    def test_past_20_00_holds_the_full_day(self):
        self.assertAlmostEqual(baseline_at(flat_profile(), 9999), float(DAY_VOLUME))

    def test_missing_profile_is_zero(self):
        self.assertEqual(baseline_at(None, 60), 0.0)
        self.assertEqual(baseline_at(np.array([]), 60), 0.0)


class TestRvolAtTime(unittest.TestCase):
    def test_matching_its_own_history_reads_one(self):
        profile = flat_profile()
        for minutes in (15, 60, OPEN_AT, 500, CLOSE_AT, SESSION_MINUTES):
            self.assertAlmostEqual(
                rvol_at_time(baseline_at(profile, minutes), profile, minutes), 1.0)

    def test_twice_the_usual_reads_two_at_every_hour(self):
        """The property the raw screener figure could not deliver.

        Against `relative_volume_10d_calc` the same 2x participation read 0.18
        at 09:35 and 1.63 at 15:00 — and nothing at all before the open.
        """
        profile = flat_profile()
        for minutes in (5, 15, 60, OPEN_AT, 400, CLOSE_AT, 900):
            traded = 2.0 * baseline_at(profile, minutes)
            self.assertAlmostEqual(rvol_at_time(traded, profile, minutes), 2.0)

    def test_unknown_readings_fail_closed(self):
        """An unknown must never clear a floor as if it had qualified."""
        profile = flat_profile()
        for bad in (None, float("nan"), 0, -5, "x"):
            self.assertEqual(rvol_at_time(bad, profile, 400), 0.0)

    def test_no_baseline_fails_closed(self):
        """A fresh listing or a download miss scores 0, not 1."""
        self.assertEqual(rvol_at_time(5_000_000, None, 400), 0.0)
        self.assertEqual(rvol_at_time(5_000_000, flat_profile(), 0), 0.0)


class TestThresholdSchedule(unittest.TestCase):
    """Pure minute-to-floor lookup, so it is independent of the anchor."""

    def test_each_band_holds_until_the_next(self):
        for minutes, expected in ((5, 0.8), (14, 0.8), (15, 1.0), (29, 1.0),
                                  (30, 1.2), (59, 1.2), (60, 1.5), (390, 1.5)):
            self.assertEqual(threshold_for(SCHEDULE, minutes), expected,
                             f"wrong floor at t={minutes}")

    def test_before_the_first_band_the_first_floor_still_applies(self):
        """A window with no floor would admit the whole universe."""
        self.assertEqual(threshold_for(SCHEDULE, 0), 0.8)
        self.assertEqual(threshold_for(SCHEDULE, 4.9), 0.8)

    def test_empty_schedule_disables_the_leg(self):
        self.assertIsNone(threshold_for((), 60))

    def test_floors_tighten_through_the_session(self):
        floors = [threshold_for(SCHEDULE, m) for m in (0, 5, 15, 30, 60, 300)]
        self.assertEqual(floors, sorted(floors))


class TestBuildProfiles(unittest.TestCase):
    """Sessions are laid onto the 04:00 grid and averaged.

    Each fixture session holds only regular-session bars, which is the ordinary
    shape of a name nobody trades before the open — and the case that would
    break if completeness were judged over the whole 192-slot grid.
    """

    REGULAR_BARS = 78

    def _session(self, day, per_bar, bars=None):
        index = pd.date_range(f"2026-08-{day} 09:30", periods=bars or self.REGULAR_BARS,
                              freq="5min", tz=ET)
        return pd.DataFrame({"Volume": [per_bar] * len(index)}, index=index)

    def test_averages_across_sessions(self):
        frame = pd.concat([self._session(10, 100), self._session(11, 300)])
        profile = build_profiles({"AAA": frame}, sessions=10)["AAA"]
        # Mean per bar is 200, so cumulative ten minutes into the session is 400.
        self.assertAlmostEqual(baseline_at(profile, minutes_since_open(at(9, 40))), 400.0)
        self.assertAlmostEqual(profile[-1], 200.0 * self.REGULAR_BARS)

    def test_bars_land_at_their_own_clock_time(self):
        """The whole point of the re-anchor: slot 0 is 04:00, not 09:30."""
        profile = build_profiles({"AAA": self._session(10, 100)}, sessions=10)["AAA"]
        self.assertEqual(baseline_at(profile, minutes_since_open(at(9, 30))), 0.0)
        self.assertAlmostEqual(baseline_at(profile, minutes_since_open(at(9, 35))), 100.0)

    def test_short_sessions_are_excluded(self):
        """A half day would drag the baseline down and inflate every ratio."""
        frame = pd.concat([self._session(10, 100), self._session(12, 100, bars=30)])
        profile = build_profiles({"AAA": frame}, sessions=10)["AAA"]
        self.assertAlmostEqual(profile[-1], 100.0 * self.REGULAR_BARS)

    def test_only_the_most_recent_sessions_count(self):
        frames = [self._session(10 + i, 100 if i < 2 else 500) for i in range(4)]
        profile = build_profiles({"AAA": pd.concat(frames)}, sessions=2)["AAA"]
        self.assertAlmostEqual(baseline_at(profile, minutes_since_open(at(9, 35))), 500.0)

    def test_a_symbol_with_no_usable_session_is_omitted(self):
        profiles = build_profiles({"AAA": self._session(10, 100, bars=5)}, sessions=10)
        self.assertNotIn("AAA", profiles)


class TestCache(unittest.TestCase):
    def test_round_trip(self):
        profiles = {"AAA": flat_profile()}
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(save_profiles(profiles, Path(tmp), "2026-08-14"))
            loaded = load_profiles(Path(tmp), "2026-08-14")
        self.assertIn("AAA", loaded)
        np.testing.assert_allclose(loaded["AAA"], profiles["AAA"], rtol=1e-6)

    def test_another_session_is_never_reused(self):
        """A stale baseline is worse than none — silently wrong for every ticker."""
        with tempfile.TemporaryDirectory() as tmp:
            save_profiles({"AAA": flat_profile()}, Path(tmp), "2026-08-13")
            self.assertEqual(load_profiles(Path(tmp), "2026-08-14"), {})

    def test_prune_keeps_only_today(self):
        with tempfile.TemporaryDirectory() as tmp:
            for date in ("2026-08-12", "2026-08-13", "2026-08-14"):
                save_profiles({"AAA": flat_profile(1920)}, Path(tmp), date)
            prune_cache(Path(tmp), "2026-08-14")
            left = sorted(p.name for p in Path(tmp).glob("rvol_baselines_*.json"))
        self.assertEqual(left, ["rvol_baselines_2026-08-14.json"])


class TestRegressionBEandFCEL(unittest.TestCase):
    """2026-08-14, the session that motivated this measure.

    FCEL ran +14% on genuinely heavy participation and never reached the board
    because the raw screener figure could not clear 1.5 before mid-afternoon.
    BE ran to +5% on thin tape and was correctly excluded. Real cumulative
    volumes, real 10-session baselines at the same clock times.

    The assertions are on the ratios, not on a stepped floor: the schedule's
    bands are minutes from the anchor and the gate owns their re-anchoring.
    What this file pins is that the measure separates the two names at all.
    """

    # (clock time, volume today, that ticker's own average by then)
    FCEL = [((9, 45), 1_184_773, 900_000), ((10, 0), 2_441_174, 1_500_000),
            ((10, 30), 4_784_937, 2_300_000)]
    BE = [((9, 45), 1_119_644, 1_900_000), ((10, 0), 1_694_070, 2_900_000),
          ((10, 30), 3_004_897, 4_400_000)]

    def _scored(self, rows):
        """Score each observation against a profile worth `expected` by then."""
        out = []
        for clock, traded, expected in rows:
            elapsed = minutes_since_open(at(*clock))
            per_bar = expected / (elapsed / 5.0)
            profile = np.cumsum(np.full(BARS_PER_SESSION, per_bar))
            out.append((clock, rvol_at_time(traded, profile, elapsed)))
        return out

    def test_fcel_reads_heavy_from_the_first_observation(self):
        for clock, ratio in self._scored(self.FCEL):
            self.assertGreater(ratio, 1.3, f"FCEL read {ratio:.2f} at {clock}")

    def test_be_reads_thin_throughout(self):
        for clock, ratio in self._scored(self.BE):
            self.assertLess(ratio, 0.7, f"BE read {ratio:.2f} at {clock}")

    def test_the_two_never_overlap(self):
        heaviest_be = max(r for _, r in self._scored(self.BE))
        lightest_fcel = min(r for _, r in self._scored(self.FCEL))
        self.assertGreater(lightest_fcel, heaviest_be)


if __name__ == "__main__":
    unittest.main()
