"""Relative Volume at Time tests.

The measure compares a ticker's volume since 09:30 against the mean of its own
09:30-to-now volume over recent sessions, so both legs are cut at the same time
of day. The screener's `relative_volume_10d_calc` divides by a full-day average
instead, which is why a fixed floor on it means something different every hour.
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


def at(hour, minute):
    return datetime(2026, 8, 14, hour, minute, tzinfo=ET)


def flat_profile(total):
    """A ticker that trades evenly through the session."""
    return np.cumsum(np.full(BARS_PER_SESSION, total / BARS_PER_SESSION))


class TestElapsed(unittest.TestCase):
    def test_counts_from_the_open(self):
        self.assertEqual(minutes_since_open(at(9, 30)), 0.0)
        self.assertEqual(minutes_since_open(at(10, 0)), 30.0)
        self.assertEqual(minutes_since_open(at(15, 0)), 330.0)

    def test_clamped_outside_the_session(self):
        self.assertEqual(minutes_since_open(at(4, 0)), 0.0)
        self.assertEqual(minutes_since_open(at(20, 0)), float(SESSION_MINUTES))

    def test_naive_datetime_is_read_as_eastern(self):
        self.assertEqual(minutes_since_open(datetime(2026, 8, 14, 10, 0)), 30.0)


class TestBaselineLookup(unittest.TestCase):
    def test_interpolates_inside_a_bar(self):
        """Today's volume arrives continuously, so the baseline must too.

        Against a step function the ratio would jump at every bar edge.
        """
        profile = flat_profile(78_000)
        self.assertAlmostEqual(baseline_at(profile, 5), 1000.0)
        self.assertAlmostEqual(baseline_at(profile, 2.5), 500.0)
        self.assertAlmostEqual(baseline_at(profile, 10), 2000.0)

    def test_zero_at_the_open_and_full_at_the_close(self):
        profile = flat_profile(78_000)
        self.assertEqual(baseline_at(profile, 0), 0.0)
        self.assertAlmostEqual(baseline_at(profile, SESSION_MINUTES), 78_000.0)

    def test_past_the_close_holds_the_full_session(self):
        profile = flat_profile(78_000)
        self.assertAlmostEqual(baseline_at(profile, 999), 78_000.0)

    def test_missing_profile_is_zero(self):
        self.assertEqual(baseline_at(None, 60), 0.0)
        self.assertEqual(baseline_at(np.array([]), 60), 0.0)


class TestRvolAtTime(unittest.TestCase):
    def test_matching_its_own_history_reads_one(self):
        profile = flat_profile(78_000)
        for minutes in (15, 60, 195, 390):
            self.assertAlmostEqual(
                rvol_at_time(baseline_at(profile, minutes), profile, minutes), 1.0)

    def test_twice_the_usual_reads_two_at_every_hour(self):
        """The property the old raw figure could not deliver.

        Against `relative_volume_10d_calc` the same 2x participation read 0.18
        at 09:35 and 1.63 at 15:00.
        """
        profile = flat_profile(78_000)
        for minutes in (5, 15, 30, 60, 200, 380):
            traded = 2.0 * baseline_at(profile, minutes)
            self.assertAlmostEqual(rvol_at_time(traded, profile, minutes), 2.0)

    def test_unknown_readings_fail_closed(self):
        """An unknown must never clear a floor as if it had qualified."""
        profile = flat_profile(78_000)
        for bad in (None, float("nan"), 0, -5, "x"):
            self.assertEqual(rvol_at_time(bad, profile, 60), 0.0)

    def test_no_baseline_fails_closed(self):
        """A fresh listing or a download miss scores 0, not 1."""
        self.assertEqual(rvol_at_time(5_000_000, None, 60), 0.0)
        self.assertEqual(rvol_at_time(5_000_000, flat_profile(78_000), 0), 0.0)


class TestThresholdSchedule(unittest.TestCase):
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
    def _session(self, day, per_bar):
        index = pd.date_range(f"2026-08-{day} 09:30", periods=BARS_PER_SESSION,
                              freq="5min", tz=ET)
        return pd.DataFrame({"Volume": [per_bar] * BARS_PER_SESSION}, index=index)

    def test_averages_across_sessions(self):
        frame = pd.concat([self._session(10, 100), self._session(11, 300)])
        profiles = build_profiles({"AAA": frame}, sessions=10)
        # mean per bar is 200, so cumulative by the second bar is 400
        self.assertAlmostEqual(profiles["AAA"][1], 400.0)
        self.assertAlmostEqual(profiles["AAA"][-1], 200.0 * BARS_PER_SESSION)

    def test_short_sessions_are_excluded(self):
        """A half day would drag the baseline down and inflate every ratio."""
        half = self._session(12, 100).head(30)
        frame = pd.concat([self._session(10, 100), half])
        profiles = build_profiles({"AAA": frame}, sessions=10)
        self.assertAlmostEqual(profiles["AAA"][-1], 100.0 * BARS_PER_SESSION)

    def test_only_the_most_recent_sessions_count(self):
        frames = [self._session(10 + i, 100 if i < 2 else 500) for i in range(4)]
        profiles = build_profiles({"AAA": pd.concat(frames)}, sessions=2)
        self.assertAlmostEqual(profiles["AAA"][0], 500.0)

    def test_a_symbol_with_no_usable_session_is_omitted(self):
        profiles = build_profiles({"AAA": self._session(10, 100).head(5)}, sessions=10)
        self.assertNotIn("AAA", profiles)


class TestCache(unittest.TestCase):
    def test_round_trip(self):
        profiles = {"AAA": flat_profile(78_000)}
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(save_profiles(profiles, Path(tmp), "2026-08-14"))
            loaded = load_profiles(Path(tmp), "2026-08-14")
        self.assertIn("AAA", loaded)
        np.testing.assert_allclose(loaded["AAA"], profiles["AAA"], rtol=1e-6)

    def test_another_session_is_never_reused(self):
        """A stale baseline is worse than none — silently wrong for every ticker."""
        with tempfile.TemporaryDirectory() as tmp:
            save_profiles({"AAA": flat_profile(78_000)}, Path(tmp), "2026-08-13")
            self.assertEqual(load_profiles(Path(tmp), "2026-08-14"), {})

    def test_prune_keeps_only_today(self):
        with tempfile.TemporaryDirectory() as tmp:
            for date in ("2026-08-12", "2026-08-13", "2026-08-14"):
                save_profiles({"AAA": flat_profile(1000)}, Path(tmp), date)
            prune_cache(Path(tmp), "2026-08-14")
            left = sorted(p.name for p in Path(tmp).glob("rvol_baselines_*.json"))
        self.assertEqual(left, ["rvol_baselines_2026-08-14.json"])


class TestRegressionBEandFCEL(unittest.TestCase):
    """2026-08-14, the session that motivated this.

    FCEL ran +14% on genuinely heavy participation and never reached the board
    because the raw figure could not clear 1.5 before mid-afternoon. BE ran to
    +5% on thin tape and was correctly excluded. Real cumulative volumes, real
    10-session baselines at the same clock times.
    """

    # (elapsed minutes, volume today, that ticker's own average by then)
    FCEL = [(15, 1_184_773, 900_000), (30, 2_441_174, 1_500_000),
            (60, 4_784_937, 2_300_000)]
    BE = [(15, 1_119_644, 1_900_000), (30, 1_694_070, 2_900_000),
          (60, 3_004_897, 4_400_000)]

    def _admitted(self, rows):
        """Score each observation against a profile worth `expected` by then."""
        out = []
        for minutes, traded, expected in rows:
            per_bar = expected / (minutes / 5.0)
            profile = np.cumsum(np.full(BARS_PER_SESSION, per_bar))
            ratio = rvol_at_time(traded, profile, minutes)
            out.append((minutes, round(ratio, 2),
                        ratio >= threshold_for(SCHEDULE, minutes)))
        return out

    def test_fcel_is_admitted_from_the_first_band(self):
        for minutes, ratio, admitted in self._admitted(self.FCEL):
            self.assertTrue(admitted,
                            f"FCEL rejected at t={minutes} with rvol_at_time {ratio}")
            self.assertGreater(ratio, 1.0)

    def test_be_is_rejected_throughout(self):
        for minutes, ratio, admitted in self._admitted(self.BE):
            self.assertFalse(admitted,
                             f"BE admitted at t={minutes} with rvol_at_time {ratio}")
            self.assertLess(ratio, 1.0)


if __name__ == "__main__":
    unittest.main()
