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

from src.bidask.config import RVOL_SCHEDULE_STATES, load_config
from src.bidask.rvol_at_time import (
    BARS_PER_SESSION,
    CRYPTO,
    SCHEDULE_ORIGIN_MIN,
    SCHEDULE_STATES,
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
from src.bidask.session_state import CLOSED, MARKET, POST_MARKET, PRE_MARKET

ET = ZoneInfo("America/New_York")

# A stepped schedule with four distinct floors, so a band mix-up shows up as a
# wrong number rather than as a coincidence. Deliberately NOT the shipped
# market schedule — these tests exercise the lookup, not the shipped floors,
# which `tests/test_bidask_universe.py` owns.
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
    """One schedule per session state, looked up on the 04:00 clock.

    The bands are written in minutes since each state's OWN start, while the
    caller's `elapsed_minutes` counts from 04:00. `threshold_for` converts, and
    that conversion is the load-bearing part of this class.
    """

    SCHEDULES = {MARKET: SCHEDULE,
                 PRE_MARKET: ((0.0, 3.0),),
                 POST_MARKET: ((0.0, 1.5),),
                 CRYPTO: ((0.0, 1.2),)}

    def market_floor(self, minutes_in):
        return threshold_for(self.SCHEDULES, MARKET, OPEN_AT + minutes_in)

    def test_each_band_holds_until_the_next(self):
        for minutes, expected in ((5, 0.8), (14, 0.8), (15, 1.0), (29, 1.0),
                                  (30, 1.2), (59, 1.2), (60, 1.5), (390, 1.5)):
            self.assertEqual(self.market_floor(minutes), expected,
                             f"wrong floor at {minutes} minutes into the session")

    def test_before_the_first_band_the_first_floor_still_applies(self):
        """A window with no floor would admit the whole universe."""
        self.assertEqual(self.market_floor(0), 0.8)
        self.assertEqual(self.market_floor(4.9), 0.8)

    def test_the_market_bands_are_read_against_09_30_not_the_anchor(self):
        """The conversion in one case, because nothing on screen shows it.

        09:35 is five minutes into the regular session and 335 into the
        extended day. Reading the raw figure against the schedule would apply
        the last band at the opening print and the first band all afternoon.
        """
        self.assertEqual(threshold_for(self.SCHEDULES, MARKET, OPEN_AT + 5), 0.8)
        self.assertNotEqual(threshold_for(self.SCHEDULES, MARKET, OPEN_AT + 5),
                            threshold_for(self.SCHEDULES, MARKET, OPEN_AT + 390))

    def test_extended_states_read_against_their_own_origins(self):
        # Pre-market is anchored at 04:00 and after hours at 16:00; both are
        # flat, so the same floor must come back anywhere in the window.
        for minutes in (0, 120, 320):
            self.assertEqual(threshold_for(self.SCHEDULES, PRE_MARKET, minutes), 3.0)
        for minutes in (0, 60, 240):
            self.assertEqual(
                threshold_for(self.SCHEDULES, POST_MARKET, CLOSE_AT + minutes), 1.5)

    def test_crypto_is_flat_at_every_hour(self):
        for minutes in (0, OPEN_AT, CLOSE_AT, 960):
            self.assertEqual(threshold_for(self.SCHEDULES, CRYPTO, minutes), 1.2)

    def test_a_state_with_no_schedule_has_no_floor(self):
        """The gate reads None as *admit nothing*, never as *admit all*."""
        self.assertIsNone(threshold_for(self.SCHEDULES, CLOSED, 60))
        self.assertIsNone(threshold_for({}, MARKET, 60))
        self.assertIsNone(threshold_for((), MARKET, 60))

    def test_the_tuple_of_pairs_the_config_carries_is_accepted(self):
        # `BidAskConfig` stores the schedules as a tuple so the frozen config
        # stays hashable; the lookup must take that shape as readily as a dict.
        self.assertEqual(threshold_for(tuple(self.SCHEDULES.items()), MARKET,
                                       OPEN_AT + 30), 1.2)

    def test_floors_tighten_through_the_session(self):
        floors = [self.market_floor(m) for m in (0, 5, 15, 30, 60, 300)]
        self.assertEqual(floors, sorted(floors))


class TestScheduleStatesMatchTheConfigGuard(unittest.TestCase):
    """`config.py` cannot import this module — `tvquote` imports `config` for
    its cookie jar, so the edge would be a cycle — and so carries its own copy
    of the valid state keys. Pinned here, exactly as `feed.SESSION_LABELS` and
    `session_state.SESSION_STATES` are pinned to each other.

    A key in one table and not the other is a state the gate can be configured
    for and never looks up, or one it looks up and no config may name.
    """

    def test_the_two_tables_carry_the_same_states(self):
        self.assertEqual(sorted(SCHEDULE_STATES), sorted(RVOL_SCHEDULE_STATES))

    def test_every_shipped_schedule_names_a_state_with_an_origin(self):
        for state, _bands in load_config().in_play_rvol_schedules:
            self.assertIn(state, SCHEDULE_ORIGIN_MIN,
                          f"{state} has a schedule but no origin minute")


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
