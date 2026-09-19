"""Universe filter and relative-volume gate tests.

The gate is the **only** admission path to either column. A price move admits
nothing on its own, however far it has run — the absolute-change leg that used
to do that is retired, and `TestRetiredConfigKeys` pins that its key raises
rather than being ignored.

Two anchors are in play and confusing them is the expensive mistake here.
`elapsed_minutes` counts from the anchor of the state's OWN market grid — the
**04:00 ET** extended anchor for the three equity states, **00:00 UTC** for
crypto. Each state's floor schedule is then written in minutes since **its own**
state began, so a regular-session band at 15 minutes means 09:45, which is
`OPEN_AT + 15` on the 04:00 clock. Reading one as the other shifts every
regular-session band by five and a half hours.

The crypto anchor is measured rather than chosen: the crypto scanner's `volume`
column is itself a sum from 00:00 UTC. See `TestCryptoAnchor`.
"""

import json
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.bidask.config import load_config
from src.bidask.feed import _bare_ticker
from src.bidask.crypto_state import REF_24H, crypto_sides
from src.bidask.rvol_at_time import (
    BARS_PER_SESSION,
    CRYPTO,
    CRYPTO_GRID,
    EQUITY_GRID,
    GRID_FOR_STATE,
    bar_count_for,
    baseline_at,
    build_profiles,
    load_profiles,
    minutes_since_open,
    prune_cache,
    save_profiles,
)
from src.bidask.session_state import (
    CLOSED,
    MARKET,
    POST_MARKET,
    PRE_MARKET,
    REF_OPEN,
    REF_PREV_CLOSE,
    REF_SESSION_CLOSE,
)
from src.bidask.universe import (
    VOLUME_FIELDS,
    apply_liquidity,
    apply_rvol_gate,
    build_universe,
    volume_since_anchor,
)

CFG = load_config()

# Minutes from the 04:00 relative-volume anchor to the regular session's own
# boundaries. A schedule written for `market` counts from OPEN_AT.
OPEN_AT = 330.0
CLOSE_AT = 720.0


def frame(rows):
    return pd.DataFrame(rows)


def flat_profile(total=192_000.0):
    """A ticker trading evenly across the whole 04:00-20:00 extended day.

    Unrealistic on purpose: an even curve makes "1.4x its usual by now" one
    multiplication instead of a session-shape argument.
    """
    return np.cumsum(np.full(BARS_PER_SESSION, total / BARS_PER_SESSION))


PROFILES = {"AAA": flat_profile(), "BBB": flat_profile()}


def volume_for(ratio, elapsed):
    """Today's cumulative volume that reads exactly `ratio` at `elapsed`."""
    return ratio * baseline_at(PROFILES["AAA"], elapsed)


def row(state, ratio, elapsed, *, symbol="AAA", change_pct=0.0):
    """One screener row trading at `ratio` times its usual by `elapsed`.

    The volume lands in the field that state's numerator reads, so a test says
    what it means: before the bell that is `premarket_volume`, because `volume`
    still holds the previous completed session. Once the session is open
    `volume` is the running extended-day total and is read alone, through the
    close and on into after hours.
    """
    volume = volume_for(ratio, elapsed)
    payload = {"symbol": symbol, "change_pct": change_pct,
               "premarket_volume": 0.0, "volume": 0.0, "postmarket_volume": 0.0}
    if state == PRE_MARKET:
        payload["premarket_volume"] = volume
    else:
        payload["volume"] = volume
    return payload


def gate(state, rows, elapsed, profiles=PROFILES, cfg=CFG):
    return apply_rvol_gate(frame(rows), cfg, state=state,
                           profiles=profiles, elapsed_minutes=elapsed)


class TestTickerNormalization(unittest.TestCase):
    def test_exchange_prefix_is_stripped(self):
        self.assertEqual(_bare_ticker("NASDAQ:WOLF"), "WOLF")
        self.assertEqual(_bare_ticker("NYSE:OC"), "OC")
        self.assertEqual(_bare_ticker("AMEX:BTG"), "BTG")

    def test_bare_ticker_passes_through(self):
        self.assertEqual(_bare_ticker("WOLF"), "WOLF")


class TestLiquidity(unittest.TestCase):
    def test_avg_dollar_volume_is_derived_and_filtered(self):
        # The screener has no avg-dollar-volume column and the library rejects
        # column arithmetic, so this floor can only be applied client-side.
        df = frame([
            {"symbol": "RICH", "close": 100.0, "avg_volume": 1_000_000},   # $100M
            {"symbol": "THIN", "close": 2.0, "avg_volume": 1_000_000},     # $2M
        ])
        out = apply_liquidity(df, CFG)
        self.assertEqual(out["symbol"].tolist(), ["RICH"])
        self.assertEqual(out.iloc[0]["avg_dollar_vol"], 100_000_000)

    def test_missing_avg_volume_skips_the_floor(self):
        # Crypto carries no average-volume field; rows must not be dropped as if
        # they had failed the floor.
        df = frame([{"symbol": "BTC", "close": 65000.0, "avg_volume": None}])
        out = apply_liquidity(df, CFG)
        self.assertEqual(len(out), 1)
        self.assertIsNone(out.iloc[0]["avg_dollar_vol"])


class TestVolumeNumerator(unittest.TestCase):
    """The numerator is cumulative volume since 04:00, assembled per state.

    `volume` is a running EXTENDED-day total once a session is under way, so
    from the open onward it already starts at 04:00 and is read alone. Before
    the bell it is not today's figure at all: it still holds the previous
    completed day, and it does not move — 0 of 300 symbols changed over a
    201-second pre-market gap while 262 of them saw `premarket_volume` rise.
    Reading it in the pre-market would therefore divide a whole previous
    session by this morning's expected few minutes and admit the entire
    universe at once.
    """

    def test_pre_market_reads_only_pre_market_volume(self):
        df = frame([{"symbol": "AAA", "premarket_volume": 40_000.0,
                     "volume": 9_000_000.0, "postmarket_volume": 0.0}])
        self.assertEqual(volume_since_anchor(df, PRE_MARKET).tolist(), [40_000.0])

    def test_the_regular_session_reads_volume_alone_and_does_not_re_add_the_morning(self):
        # `volume` already spans the extended day from 04:00 once the session is
        # open. Measured 2026-09-17, 8 of 8 rows: CTNT read 1,151,094,276
        # against pre-market bars of 805,266,601 plus regular bars of
        # 325,117,028. Adding `premarket_volume` back counts the morning twice
        # and inflated that row's numerator by about 70%.
        df = frame([{"symbol": "AAA", "premarket_volume": 40_000.0,
                     "volume": 200_000.0, "postmarket_volume": 0.0}])
        self.assertEqual(volume_since_anchor(df, MARKET).tolist(), [200_000.0])

    def test_after_hours_keeps_accumulating_past_the_close(self):
        # One running extended-day total, so the close is not a boundary the
        # numerator has to re-cross.
        df = frame([{"symbol": "AAA", "premarket_volume": 40_000.0,
                     "volume": 225_000.0, "postmarket_volume": 25_000.0}])
        self.assertEqual(volume_since_anchor(df, POST_MARKET).tolist(), [225_000.0])

    def test_a_missing_column_contributes_nothing_rather_than_raising(self):
        # A withdrawn vendor column must cost the leg, not the poll.
        df = frame([{"symbol": "AAA", "volume": 160_000.0}])
        self.assertEqual(volume_since_anchor(df, MARKET).tolist(), [160_000.0])

    def test_an_unparseable_reading_is_zero_not_a_crash(self):
        df = frame([{"symbol": "AAA", "premarket_volume": "n/a",
                     "volume": 160_000.0, "postmarket_volume": None}])
        self.assertEqual(volume_since_anchor(df, MARKET).tolist(), [160_000.0])

    def test_a_state_with_no_numerator_scores_nothing(self):
        # Closed, and crypto until its own 24-hour numerator is wired. An
        # all-zero numerator fails every row closed and surfaces as an
        # unavailable source rather than as a quiet market.
        df = frame([{"symbol": "AAA", "premarket_volume": 40_000.0,
                     "volume": 160_000.0, "postmarket_volume": 25_000.0}])
        self.assertEqual(volume_since_anchor(df, CLOSED).tolist(), [0.0])


class TestRegularSessionFloors(unittest.TestCase):
    """0.7 from the open, 1.0 at 15 minutes, 1.2 at 30, and 1.2 onward."""

    def admitted(self, ratio, minutes_in):
        elapsed = OPEN_AT + minutes_in
        return len(gate(MARKET, [row(MARKET, ratio, elapsed)], elapsed).rows)

    def test_the_opening_floor_also_covers_the_minutes_before_its_mark(self):
        self.assertEqual(self.admitted(0.75, 0), 1)
        self.assertEqual(self.admitted(0.75, 5), 1)
        self.assertEqual(self.admitted(0.65, 5), 0)

    def test_fifteen_minutes_in_the_floor_is_one(self):
        self.assertEqual(self.admitted(1.05, 15), 1)
        self.assertEqual(self.admitted(0.95, 15), 0)

    def test_thirty_minutes_in_the_floor_is_one_point_two(self):
        self.assertEqual(self.admitted(1.25, 30), 1)
        self.assertEqual(self.admitted(1.15, 30), 0)

    def test_the_last_band_holds_for_the_remainder_of_the_session(self):
        self.assertEqual(self.admitted(1.25, 200), 1)
        self.assertEqual(self.admitted(1.15, 200), 0)

    def test_the_bands_are_read_against_the_open_not_the_04_00_anchor(self):
        """The whole anchor trap in one case.

        At 09:35 the ticker is five minutes into the regular session and the
        0.7 floor applies. Reading the same moment as 335 minutes into the
        schedule would apply the last band instead and reject it.
        """
        self.assertEqual(self.admitted(0.75, 5), 1)
        # ... and the converse: 0.75x an hour in is genuinely below the floor.
        self.assertEqual(self.admitted(0.75, 60), 0)

    def test_floors_never_loosen_through_the_session(self):
        edge = [self.admitted(1.15, m) for m in (0, 5, 15, 30, 60, 200)]
        self.assertEqual(edge, sorted(edge, reverse=True))


class TestExtendedHoursFloors(unittest.TestCase):
    def test_pre_market_admits_above_three_and_excludes_below(self):
        """AE3. The floors are the user's numbers, used exactly as written."""
        self.assertEqual(len(gate(PRE_MARKET, [row(PRE_MARKET, 2.5, 120)], 120).rows), 0)
        self.assertEqual(len(gate(PRE_MARKET, [row(PRE_MARKET, 3.1, 120)], 120).rows), 1)

    def test_after_hours_admits_above_one_point_five(self):
        at = CLOSE_AT + 30
        self.assertEqual(len(gate(POST_MARKET, [row(POST_MARKET, 1.4, at)], at).rows), 0)
        self.assertEqual(len(gate(POST_MARKET, [row(POST_MARKET, 1.6, at)], at).rows), 1)

    def test_the_extended_floors_are_flat_across_their_window(self):
        for minutes in (0, 60, 180, 320):
            self.assertEqual(
                len(gate(PRE_MARKET, [row(PRE_MARKET, 2.9, minutes)], minutes).rows), 0,
                f"2.9x admitted pre-market at t={minutes}")

    def test_crypto_carries_a_flat_floor_the_gate_can_express(self):
        """R14. The call site is a later unit's; the gate must express it now."""
        self.assertEqual(dict(CFG.in_play_rvol_schedules)[CRYPTO], ((0.0, 1.2),))


class TestPriceAloneAdmitsNothing(unittest.TestCase):
    """R16. The absolute-change leg is removed, not retained as an alternative."""

    def test_a_twelve_percent_move_on_thin_volume_is_excluded(self):
        """AE2."""
        elapsed = OPEN_AT + 90
        thin = row(MARKET, 0.4, elapsed, change_pct=12.0)
        self.assertEqual(len(gate(MARKET, [thin], elapsed).rows), 0)

    def test_a_ticker_with_no_baseline_is_excluded_however_far_it_moved(self):
        elapsed = OPEN_AT + 90
        unknown = row(MARKET, 50.0, elapsed, symbol="ZZZ", change_pct=30.0)
        self.assertEqual(len(gate(MARKET, [unknown], elapsed).rows), 0)

    def test_a_closed_market_admits_nothing(self):
        elapsed = OPEN_AT + 90
        heavy = row(MARKET, 25.0, elapsed, change_pct=9.0)
        result = gate(CLOSED, [heavy], elapsed)
        self.assertEqual(len(result.rows), 0)
        self.assertIsNone(result.floor)

    def test_a_closed_market_is_not_a_broken_source(self):
        # Both empty the board, and they must not render as the same sentence.
        elapsed = OPEN_AT + 90
        result = gate(CLOSED, [row(MARKET, 25.0, elapsed)], elapsed)
        self.assertFalse(result.source_unavailable)


class TestGateCoverage(unittest.TestCase):
    """KTD2's honesty pair: what could be scored against what was polled.

    An empty board with no coverage figure is indistinguishable from a quiet
    market, which is the failure this whole redesign was written around.
    """

    def test_qualifying_rows_carry_their_reading(self):
        elapsed = OPEN_AT + 90
        result = gate(MARKET, [row(MARKET, 2.0, elapsed)], elapsed)
        self.assertAlmostEqual(result.rows.iloc[0]["rvol_at_time"], 2.0, places=6)

    def test_the_pair_counts_usable_readings_against_rows_polled(self):
        elapsed = OPEN_AT + 90
        rows = [row(MARKET, 2.0, elapsed),
                row(MARKET, 0.5, elapsed, symbol="BBB"),
                row(MARKET, 9.0, elapsed, symbol="ZZZ")]  # no baseline
        result = gate(MARKET, rows, elapsed)
        self.assertEqual(result.polled, 3)
        self.assertEqual(result.scored, 2)
        self.assertEqual(result.floor, 1.2)

    def test_partial_nulls_keep_publishing(self):
        elapsed = OPEN_AT + 90
        rows = [row(MARKET, 2.0, elapsed),
                row(MARKET, 9.0, elapsed, symbol="ZZZ")]  # no baseline
        result = gate(MARKET, rows, elapsed)
        self.assertFalse(result.source_unavailable)
        self.assertEqual(result.rows["symbol"].tolist(), ["AAA"])

    def test_no_usable_reading_anywhere_reports_an_unavailable_source(self):
        elapsed = OPEN_AT + 90
        rows = [row(MARKET, 9.0, elapsed, symbol="ZZZ"),
                row(MARKET, 9.0, elapsed, symbol="YYY")]
        result = gate(MARKET, rows, elapsed, profiles={})
        self.assertTrue(result.source_unavailable)
        self.assertEqual((result.scored, result.polled), (0, 2))

    def test_an_empty_response_is_not_an_unavailable_source(self):
        # Nothing was polled, so nothing failed. Reporting a dead source here
        # would blame the vendor for our own upstream floors.
        result = gate(MARKET, [], OPEN_AT + 90)
        self.assertFalse(result.source_unavailable)
        self.assertEqual((result.scored, result.polled), (0, 0))

    def test_missing_metrics_do_not_crash(self):
        elapsed = OPEN_AT + 90
        rows = [{"symbol": "AAA", "volume": None, "change_pct": None}]
        self.assertEqual(len(gate(MARKET, rows, elapsed).rows), 0)

    def test_omitted_elapsed_falls_back_to_the_clock(self):
        """Callers may omit it; the gate must not then admit everything."""
        rows = [{"symbol": "AAA", "volume": 0, "premarket_volume": 0}]
        result = apply_rvol_gate(frame(rows), CFG, state=MARKET, profiles=PROFILES)
        self.assertEqual(len(result.rows), 0)


class TestRetiredConfigKeys(unittest.TestCase):
    """Silently ignoring a retired key would disable a leg with no signal —
    the exact failure this module exists to prevent."""

    def test_raw_rvol_key_is_rejected_with_a_migration_message(self):
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_min_rvol": 1.5})
        self.assertIn("in_play_rvol_schedules", str(caught.exception))

    def test_the_interim_pace_key_is_also_rejected(self):
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_min_volume_pace": 1.5})
        self.assertIn("in_play_rvol_schedules", str(caught.exception))

    def test_the_single_schedule_key_names_its_per_state_replacement(self):
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_rvol_schedule": [[5, 0.8]]})
        self.assertIn("in_play_rvol_schedules", str(caught.exception))

    def test_the_change_leg_key_names_what_replaced_it(self):
        """R16: no ticker reaches a column on a price move alone."""
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_min_change_pct": 3.0})
        message = str(caught.exception)
        self.assertIn("in_play_min_change_pct", message)
        self.assertIn("in_play_rvol_schedules", message)

    def test_a_malformed_schedule_entry_raises(self):
        """A skipped band is a hole in the gate at one time of day only."""
        with self.assertRaises(ValueError):
            load_config({"in_play_rvol_schedules":
                         {"market": [[5, 0.8], "nonsense"]}})

    def test_an_unknown_session_key_raises_rather_than_being_ignored(self):
        # A typo is a state with no floor, which is invisible from the board
        # until that window comes round.
        with self.assertRaises(ValueError) as caught:
            load_config({"in_play_rvol_schedules": {"premarket": [[0, 3.0]]}})
        self.assertIn("premarket", str(caught.exception))

    def test_an_empty_schedule_for_a_state_raises(self):
        """The gate has no off switch — R6 admits nothing without a floor."""
        with self.assertRaises(ValueError):
            load_config({"in_play_rvol_schedules": {"market": []}})


class TestGroupingKnobsReachTheConfig(unittest.TestCase):
    """`grouping.py` read these through a getattr fallback; they are real keys
    now, so the shipped YAML and the running board cannot drift apart."""

    def test_the_three_scoring_knobs_are_typed_fields(self):
        self.assertAlmostEqual(CFG.group_rvol_cap, 5.0)
        self.assertAlmostEqual(CFG.group_breadth_coef, 0.5)
        self.assertEqual(CFG.group_breadth_min_members, 2)


class TestStablecoinExclusion(unittest.TestCase):
    def test_stablecoins_are_dropped_from_crypto(self):
        # Pegged assets accumulate hits from micro-oscillation around $1 and
        # float to the top of the column carrying no information.
        df = frame([
            {"symbol": "USDC", "close": 1.0, "avg_volume": None},
            {"symbol": "USDT", "close": 1.0, "avg_volume": None},
            {"symbol": "BTC", "close": 65000.0, "avg_volume": None},
        ])
        out = build_universe(df, CFG, market="crypto")
        self.assertEqual(out["symbol"].tolist(), ["BTC"])

    def test_exclusion_is_case_insensitive(self):
        df = frame([{"symbol": "usdc", "close": 1.0, "avg_volume": None},
                    {"symbol": "BTC", "close": 65000.0, "avg_volume": None}])
        out = build_universe(df, CFG, market="crypto")
        self.assertEqual(out["symbol"].tolist(), ["BTC"])

    def test_gold_backed_tokens_are_kept(self):
        # PAXG/XAUT track a real moving asset; they are not pegged.
        df = frame([{"symbol": "PAXG", "close": 2600.0, "avg_volume": None},
                    {"symbol": "XAUT", "close": 2600.0, "avg_volume": None}])
        out = build_universe(df, CFG, market="crypto")
        self.assertEqual(sorted(out["symbol"].tolist()), ["PAXG", "XAUT"])

    def test_equity_market_is_unaffected(self):
        # "USDC" as an equity ticker must not be filtered by a crypto rule.
        df = frame([{"symbol": "USDC", "close": 50.0, "avg_volume": 1_000_000}])
        out = build_universe(df, CFG, market="equity")
        self.assertEqual(len(out), 1)


class TestMarketStatus(unittest.TestCase):
    """Market state and feed entitlement are different questions.

    A real-time entitlement on a closed market is still a closed market, so the
    UI must not read `update_mode` as "the market is live".
    """

    def test_session_field_maps_to_a_human_label(self):
        from src.bidask.feed import _market_status
        # `market` is the value the feed actually sends during the regular
        # session (verified live 2026-08-12). The original `regular` spelling was
        # assumed, never observed, and left the UI styling an open market as
        # delayed; both map now so neither vintage regresses.
        self.assertEqual(_market_status(frame([{"current_session": "market"}])),
                         "market open")
        self.assertEqual(_market_status(frame([{"current_session": "regular"}])),
                         "market open")
        self.assertEqual(_market_status(frame([{"current_session": "out_of_session"}])),
                         "market closed")
        self.assertEqual(_market_status(frame([{"current_session": "pre_market"}])),
                         "pre-market")
        self.assertEqual(_market_status(frame([{"current_session": "post_market"}])),
                         "after hours")

    def test_unknown_session_value_is_passed_through_readably(self):
        from src.bidask.feed import _market_status
        self.assertEqual(_market_status(frame([{"current_session": "some_new_state"}])),
                         "some new state")

    def test_missing_session_column_yields_empty(self):
        from src.bidask.feed import _market_status
        self.assertEqual(_market_status(frame([{"close": 10.0}])), "")

    def test_payload_market_open_only_for_trading_states(self):
        from src.bidask.feed import Payload
        empty = pd.DataFrame()
        self.assertTrue(Payload(rows=empty, market_status="market open").market_open)
        self.assertTrue(Payload(rows=empty, market_status="pre-market").market_open)
        self.assertFalse(Payload(rows=empty, market_status="market closed").market_open)
        self.assertFalse(Payload(rows=empty, market_status="").market_open)


class TestCadenceClamp(unittest.TestCase):
    def test_requested_cadence_is_bounded(self):
        self.assertEqual(CFG.clamp_poll_seconds(1), CFG.min_poll_seconds)
        self.assertEqual(CFG.clamp_poll_seconds(9999), CFG.max_poll_seconds)
        self.assertEqual(CFG.clamp_poll_seconds(15), 15)

    def test_garbage_cadence_falls_back_to_the_configured_value(self):
        self.assertEqual(CFG.clamp_poll_seconds(None), CFG.poll_seconds)
        self.assertEqual(CFG.clamp_poll_seconds("fast"), CFG.poll_seconds)


class TestBuildUniverse(unittest.TestCase):
    """`build_universe` is the pre-gate half and never gates.

    Its output is what `build_columns` counts for the breadth denominator, so
    folding the gate into it would leave every group's share at 1.0.
    """

    def test_liquidity_only_and_no_relative_volume_leg(self):
        df = frame([{"symbol": "AAA", "close": 100.0, "avg_volume": 1_000_000,
                     "volume": 1.0, "change_pct": 0.1}])
        self.assertEqual(len(build_universe(df, CFG)), 1)

    def test_empty_frame_survives(self):
        self.assertTrue(build_universe(frame([]), CFG).empty)


class TestConfigValidation(unittest.TestCase):
    def test_twenty_day_window_is_rejected_with_a_clear_message(self):
        with self.assertRaises(ValueError) as ctx:
            load_config({"avg_window_days": 20})
        self.assertIn("20", str(ctx.exception))
        self.assertIn("null", str(ctx.exception))

    def test_avg_volume_field_tracks_the_window(self):
        self.assertEqual(load_config({"avg_window_days": 30}).avg_volume_field,
                         "average_volume_30d_calc")


class _StubQuery:
    """Stands in for the screener builder and records what was asked for.

    The fetchers build their query as one long method chain, so every builder
    method returns `self` and only `get_scanner_data` produces anything. The
    recorded `selected` / `filters` / `ordered` are what let a test assert on
    the request rather than only on the response.
    """

    last = None

    def __init__(self, matched, df):
        self._matched, self._df = matched, df
        self.selected, self.filters, self.ordered = (), (), None
        type(self).last = self

    def set_markets(self, *args):
        return self

    def select(self, *columns):
        self.selected = columns
        return self

    def where(self, *conditions):
        self.filters = conditions
        return self

    def order_by(self, field, **kwargs):
        self.ordered = field
        return self

    def limit(self, count):
        return self

    def get_scanner_data(self, **kwargs):
        return self._matched, self._df


def stub_screener(rows, matched=None):
    """Patch the screener with a canned response for one fetch."""
    df = frame(rows)
    return mock.patch("src.bidask.feed.Query",
                      lambda: _StubQuery(len(df) if matched is None else matched, df))


# One screener row, shaped as the live feed shapes it. `Value.Traded` is absent
# on purpose — that is the whole point of these tests.
def screener_row(**overrides):
    row = {"ticker": "NASDAQ:AAA", "name": "AAA", "close": 20.0, "bid": None,
           "ask": None, "change": 4.0, "volume": 5_000_000,
           "relative_volume_10d_calc": 1.4, "market_cap_basic": 1e9,
           "sector": "Technology", "industry": "Software",
           "update_mode": "streaming", "current_session": "pre_market",
           CFG.avg_volume_field: 4_000_000}
    row.update(overrides)
    return row


class TestTradedValueFieldIsUnreliable(unittest.TestCase):
    """`Value.Traded` is an unlisted alias, not a supported scanner field.

    It has never appeared in the 3,771-field metainfo, which lists every other
    column selected here — yet the scanner resolves it, which is what makes it
    dangerous. Selecting an unpublished field does NOT error; it returns null
    whenever the vendor has no value, the same trap
    `docs/solutions/logic-errors/api-returns-null-for-fields-it-does-not-have.md`
    records for `bid`/`ask`. Verified 2026-08-21: null for every row through pre-market and
    for at least the first four minutes of the session (`Value.Traded >= $1M`
    matched 0 of 13,661 at 09:34 ET with `current_session` reading `market` and
    `close`/`volume` live, against 2,806 for the average-volume leg alone), and
    4,231 by 17:21 ET. A server-side floor on it therefore empties the universe
    through the open, and an empty frame is indistinguishable from a closed
    market.
    """

    def test_the_query_never_names_the_unpublished_field(self):
        from src.bidask.feed import fetch_equities
        with stub_screener([screener_row()]):
            fetch_equities(CFG)
        query = _StubQuery.last
        self.assertNotIn("Value.Traded", query.selected)
        self.assertNotIn("Value.Traded", [c.get("left") for c in query.filters])
        self.assertNotEqual(query.ordered, "Value.Traded")

    def test_a_frame_without_the_field_still_yields_a_universe(self):
        # The regression: with the floor pushed server-side against a null
        # column, this came back empty on every poll and the board went dark.
        from src.bidask.feed import fetch_equities
        with stub_screener([screener_row()]):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.rows["symbol"].tolist(), ["AAA"])
        self.assertEqual(payload.rows.iloc[0]["dollar_vol"], 100_000_000)

    def test_todays_traded_value_floor_still_cuts(self):
        # The replacement must be a real floor, not a no-op that admits the
        # whole screener. close x volume is this file's own long-standing
        # definition of the same quantity.
        from src.bidask.feed import fetch_equities
        thin = screener_row(ticker="NASDAQ:BBB", name="BBB", close=0.10,
                            volume=1_000)  # $100 traded
        with stub_screener([screener_row(), thin]):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.rows["symbol"].tolist(), ["AAA"])

    def test_an_unusable_reading_fails_closed(self):
        # A missing price or volume is an unknown, and an unknown must not
        # clear a floor as though it had qualified.
        from src.bidask.feed import fetch_equities
        with stub_screener([screener_row(volume=np.nan)]):
            self.assertTrue(fetch_equities(CFG).rows.empty)

    def test_a_text_typed_column_cannot_kill_the_poll(self):
        # The derivation sits OUTSIDE the fetcher's try block, so a raise here
        # escapes `poll_once` to the poll loop, which never writes the state
        # file — the page then freezes on the last good poll while the console
        # scrolls one line. On a text column `"20.0" * 5_000_000` builds a
        # 35MB string and comparing it against the floor raises TypeError.
        # Coercing both legs keeps the poll alive: a parseable figure is
        # recovered, an unparseable one becomes NaN and fails the floor.
        from src.bidask.feed import fetch_equities
        rows = [
            screener_row(close="20.0"),                      # $100M, parseable
            screener_row(ticker="NASDAQ:CCC", name="CCC", close="n/a"),
        ]
        with stub_screener(rows):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.rows["symbol"].tolist(), ["AAA"])
        self.assertEqual(payload.rows.iloc[0]["dollar_vol"], 100_000_000)

    def test_a_withdrawn_column_surfaces_as_a_feed_error(self):
        # The derivation reads df["close"] directly, so the next withdrawn
        # column raises KeyError rather than nulling. Nothing above catches it
        # — not poll_once — so the poll loop would swallow it and never write
        # the state file, freezing every field on the page including the
        # clock. A visible feed error is the whole point of the guard.
        from src.bidask.feed import fetch_equities
        row = screener_row()
        row.pop("close")
        with stub_screener([row]):
            payload = fetch_equities(CFG)
        self.assertEqual(payload.error, "KeyError")
        self.assertTrue(payload.rows.empty)
        # The reading the response did carry survives the failure, so the
        # session pill keeps telling the truth while the feed pill reports.
        self.assertEqual(payload.market_status, "pre-market")


class TestEmptyResultStillReportsTheFeed(unittest.TestCase):
    """An empty board must not be reported as an unknown market on a dead feed.

    `market_status=""` renders as the literal word "unknown" and `feed=""`
    renders as "delayed feed", so dropping both on the empty path turned a
    real-time pre-market session into two false claims on screen. That is what
    made the missing-field failure above unreadable for a full session.
    """

    def test_equity_feed_and_session_survive_a_fully_filtered_frame(self):
        from src.bidask.feed import fetch_equities
        thin = screener_row(close=0.10, volume=1_000)  # below every floor
        with stub_screener([thin]):
            payload = fetch_equities(CFG)
        self.assertTrue(payload.rows.empty)
        self.assertEqual(payload.market_status, "pre-market")
        self.assertEqual(payload.feed, "streaming")
        self.assertFalse(payload.delayed)

    def test_crypto_feed_survives_a_frame_of_unnamed_rows(self):
        from src.bidask.feed import fetch_crypto
        with mock.patch("src.bidask.feed.screeners") as screeners:
            df = frame([{"base_currency": None, "close": 1.0, "volume": 1.0,
                         "update_mode": "streaming"}])
            screeners.crypto = lambda: _StubQuery(1, df)
            payload = fetch_crypto(CFG)
        self.assertTrue(payload.rows.empty)
        self.assertEqual(payload.feed, "streaming")
        self.assertEqual(payload.market_status, "24/7")


# ── crypto ───────────────────────────────────────────────────────

CRYPTO_DAY = 1440.0        # minutes in a UTC day
CRYPTO_SLOTS = 288         # 5-minute bars in it


def crypto_profile(total=288_000.0):
    """A coin trading evenly across the whole UTC day.

    Evenly on purpose, for the same reason `flat_profile` is: it makes "1.4x
    its usual by now" one multiplication rather than an argument about the
    shape of a trading day.
    """
    return np.cumsum(np.full(CRYPTO_SLOTS, total / CRYPTO_SLOTS))


CRYPTO_PROFILES = {"BTC": crypto_profile(), "ETH": crypto_profile()}


def crypto_row(ratio, elapsed, *, symbol="BTC", change_pct=0.0):
    """One crypto row trading at `ratio` times its usual by `elapsed`.

    MEASURED 2026-09-17 19:03 UTC, 13 of 13 rows: the crypto scanner's
    `volume` is the cumulative figure since 00:00 UTC, matching a 5-minute bar
    sum from that anchor at a median ratio of 1.00026. It is NOT the 24-hour
    rolling figure — that one is `24h_vol|5`, which read 1.25x `volume` at the
    same moment. So the numerator is `volume`, exactly as it is for an open
    equity session, and the anchor is the UTC day.
    """
    usual = baseline_at(CRYPTO_PROFILES[symbol], elapsed, grid=CRYPTO_GRID)
    return {"symbol": symbol, "change_pct": change_pct, "volume": ratio * usual}


def crypto_gate(rows, elapsed, profiles=CRYPTO_PROFILES, cfg=CFG):
    return apply_rvol_gate(frame(rows), cfg, state=CRYPTO,
                           profiles=profiles, elapsed_minutes=elapsed)


class TestCryptoAnchor(unittest.TestCase):
    """Q4, answered by measurement: the UTC day.

    Crypto has no session open, so Relative Volume at Time needed an anchor
    chosen rather than inherited. The vendor picked one for us and it is
    checkable: `volume` on the crypto scanner equals a bar sum from 00:00 UTC.
    Anchoring anywhere else puts the numerator and the denominator on
    different clocks, which is the one error this measure cannot survive.
    """

    def test_the_day_is_the_utc_day_not_the_equity_extended_day(self):
        self.assertEqual(CRYPTO_GRID.anchor_min, 0)
        self.assertEqual(CRYPTO_GRID.minutes, int(CRYPTO_DAY))
        self.assertEqual(CRYPTO_GRID.slots, CRYPTO_SLOTS)
        self.assertEqual(str(CRYPTO_GRID.tz), "UTC")

    def test_elapsed_counts_from_utc_midnight(self):
        utc = ZoneInfo("UTC")
        self.assertEqual(
            minutes_since_open(datetime(2026, 9, 17, 12, 0, tzinfo=utc),
                               grid=CRYPTO_GRID), 720.0)
        self.assertEqual(
            minutes_since_open(datetime(2026, 9, 17, 0, 0, tzinfo=utc),
                               grid=CRYPTO_GRID), 0.0)

    def test_the_equity_anchor_is_untouched(self):
        """04:00 ET stays the equity anchor; the crypto grid is a sibling."""
        self.assertEqual(EQUITY_GRID.anchor_min, 4 * 60)
        self.assertEqual(EQUITY_GRID.slots, BARS_PER_SESSION)

    def test_a_crypto_curve_is_read_across_the_whole_day(self):
        """The equity clamp stops at 960 minutes; a UTC day runs to 1440.

        Left on the equity grid, `baseline_at` reports the same expected
        volume for every minute after 16:00 ET, so every evening reading rises
        without bound and the whole evening clears any floor.
        """
        profile = crypto_profile()
        late = baseline_at(profile, 1380.0, grid=CRYPTO_GRID)
        mid = baseline_at(profile, 960.0, grid=CRYPTO_GRID)
        self.assertGreater(late, mid)
        self.assertAlmostEqual(late / mid, 1380.0 / 960.0, places=6)


class TestCryptoFloor(unittest.TestCase):
    """R14: one flat floor of 1.2, at every hour of the day."""

    def test_one_point_one_is_excluded_and_one_point_three_admitted(self):
        for elapsed in (5.0, 60.0, 360.0, 720.0, 1439.0):
            self.assertEqual(
                len(crypto_gate([crypto_row(1.1, elapsed)], elapsed).rows), 0,
                f"1.1x admitted at {elapsed} minutes into the UTC day")
            self.assertEqual(
                len(crypto_gate([crypto_row(1.3, elapsed)], elapsed).rows), 1,
                f"1.3x excluded at {elapsed} minutes into the UTC day")

    def test_nothing_is_judged_at_the_anchor_instant_itself(self):
        """00:00 UTC exactly: no volume is expected yet, so nothing qualifies.

        The same property the equity path has at 04:00, and it fails in the
        safe direction — a zero denominator scores 0 rather than infinity, so
        the board is briefly empty rather than briefly showing everything.
        """
        result = crypto_gate([crypto_row(50.0, 0.0)], 0.0)
        self.assertEqual(len(result.rows), 0)

    def test_no_elapsed_value_moves_the_crypto_floor(self):
        """R14 from the other side: there is no schedule here to step."""
        floors = {crypto_gate([crypto_row(1.3, m)], m).floor
                  for m in (0.0, 15.0, 30.0, 60.0, 330.0, 720.0, 1439.0)}
        self.assertEqual(floors, {1.2})

    def test_the_gate_reads_volume_as_the_utc_day_numerator(self):
        self.assertEqual(dict(VOLUME_FIELDS)[CRYPTO], ("volume",))
        rows = frame([{"symbol": "BTC", "volume": 1_000.0}])
        self.assertEqual(volume_since_anchor(rows, CRYPTO).tolist(), [1_000.0])


class TestCryptoFailsClosed(unittest.TestCase):
    """R11 on the crypto tab. An unusable reading scores 0 and is excluded."""

    def test_a_coin_with_no_baseline_is_excluded_however_far_it_moved(self):
        unknown = {"symbol": "ZZZ", "volume": 1e12, "change_pct": 40.0}
        self.assertEqual(len(crypto_gate([unknown], 720.0).rows), 0)

    def test_a_null_volume_column_excludes_every_row(self):
        """The `Value.Traded` shape: the field resolves and answers null."""
        rows = [{"symbol": "BTC", "volume": None, "change_pct": 5.0},
                {"symbol": "ETH", "volume": None, "change_pct": -5.0}]
        result = crypto_gate(rows, 720.0)
        self.assertEqual(len(result.rows), 0)
        self.assertEqual((result.scored, result.polled), (0, 2))

    def test_a_null_column_reports_an_unavailable_source_not_a_quiet_market(self):
        """R18. The crypto board must be able to name its own cause."""
        rows = [{"symbol": "BTC", "volume": None}, {"symbol": "ETH", "volume": None}]
        self.assertTrue(crypto_gate(rows, 720.0).source_unavailable)

    def test_a_warm_up_still_running_is_not_a_quiet_market_either(self):
        rows = [crypto_row(9.0, 720.0), crypto_row(9.0, 720.0, symbol="ETH")]
        result = crypto_gate(rows, 720.0, profiles={})
        self.assertEqual(len(result.rows), 0)
        self.assertTrue(result.source_unavailable)

    def test_an_empty_crypto_response_is_not_a_broken_source(self):
        self.assertFalse(crypto_gate([], 720.0).source_unavailable)


class TestCryptoPriceReference(unittest.TestCase):
    """R15. The side test is that market's own 24-hour reference, and it says so.

    Kept out of `session_state.py` deliberately: each of that module's four
    states selects a reference price AND a volume floor, and crypto has
    neither an open nor a close to measure against. A crypto entry in that
    table would be a session state the feed never sends.
    """

    def test_up_over_24_hours_is_strong_against_the_24_hour_reference(self):
        sides = crypto_sides({"change_pct": 3.4})
        self.assertEqual(sides.strong, (REF_24H,))
        self.assertEqual(sides.weak, ())

    def test_down_over_24_hours_is_weak(self):
        sides = crypto_sides({"change_pct": -3.4})
        self.assertEqual(sides.weak, (REF_24H,))
        self.assertEqual(sides.strong, ())

    def test_the_reference_is_labelled_as_24_hour_not_as_a_session(self):
        """It renders beside the equity labels, so it must not read as one."""
        self.assertIn("24h", REF_24H)
        self.assertNotIn(REF_24H, (REF_OPEN, REF_PREV_CLOSE, REF_SESSION_CLOSE))

    def test_an_unreadable_or_flat_change_earns_no_side(self):
        for value in (None, float("nan"), "n/a", 0.0):
            sides = crypto_sides({"change_pct": value})
            self.assertEqual((sides.strong, sides.weak), ((), ()),
                             f"{value!r} earned a side")

    def test_a_missing_field_earns_no_side(self):
        sides = crypto_sides({})
        self.assertEqual((sides.strong, sides.weak), ((), ()))


class TestCryptoIsNotASessionState(unittest.TestCase):
    """The two vocabularies stay apart, the way the repo pins every other pair."""

    def test_crypto_is_absent_from_the_equity_session_table(self):
        from src.bidask.session_state import SESSION_STATES
        self.assertNotIn(CRYPTO, SESSION_STATES.values())

    def test_the_schedule_key_and_the_side_module_agree_on_one_spelling(self):
        from src.bidask.crypto_state import CRYPTO as CRYPTO_SIDE_KEY
        self.assertEqual(CRYPTO_SIDE_KEY, CRYPTO)

    def test_the_crypto_grid_is_the_one_the_gate_looks_up_for_that_state(self):
        self.assertIs(GRID_FOR_STATE[CRYPTO], CRYPTO_GRID)
        for state in (PRE_MARKET, MARKET, POST_MARKET):
            self.assertIs(GRID_FOR_STATE[state], EQUITY_GRID)


class TestCryptoBaselines(unittest.TestCase):
    """Baselines are built on the UTC day, from the same bars the equity path uses.

    MEASURED 2026-09-17 across 60 BINANCE symbols: every one resolved on the
    chart socket, a complete UTC day carries 288 five-minute bars, and 719 of
    778 completed days reached 216. The 58-bar days are the truncated edge of
    the request window, which is what the completeness floor exists to drop.
    """

    def _day(self, date, total, bars=CRYPTO_SLOTS):
        stamps = pd.date_range(f"{date} 00:00", periods=bars, freq="5min", tz="UTC")
        return pd.DataFrame({"Volume": np.full(bars, total / bars)}, index=stamps)

    def test_a_utc_day_is_averaged_onto_288_slots(self):
        frames = pd.concat([self._day("2026-09-15", 288_000.0),
                            self._day("2026-09-16", 288_000.0)])
        profile = build_profiles({"BTC": frames}, sessions=10, grid=CRYPTO_GRID)["BTC"]
        self.assertEqual(len(profile), CRYPTO_SLOTS)
        self.assertAlmostEqual(baseline_at(profile, 720.0, grid=CRYPTO_GRID),
                               144_000.0, places=3)

    def test_a_truncated_edge_day_is_not_averaged_in(self):
        """A 58-bar day is the request window's edge, not a quiet session."""
        frames = pd.concat([self._day("2026-09-15", 58_000.0, bars=58),
                            self._day("2026-09-16", 288_000.0)])
        profile = build_profiles({"BTC": frames}, sessions=10, grid=CRYPTO_GRID)["BTC"]
        self.assertAlmostEqual(baseline_at(profile, 1440.0, grid=CRYPTO_GRID),
                               288_000.0, places=3)

    def test_todays_partial_day_is_excluded_from_its_own_baseline(self):
        frames = pd.concat([self._day("2026-09-16", 288_000.0),
                            self._day("2026-09-17", 20_000.0, bars=229)])
        profile = build_profiles({"BTC": frames}, sessions=10,
                                 exclude_date="2026-09-17", grid=CRYPTO_GRID)["BTC"]
        self.assertAlmostEqual(baseline_at(profile, 1440.0, grid=CRYPTO_GRID),
                               288_000.0, places=3)

    def test_the_bar_budget_covers_ten_complete_utc_days(self):
        self.assertGreaterEqual(bar_count_for(10, grid=CRYPTO_GRID), 13 * CRYPTO_SLOTS)

    def test_a_baseline_cache_is_never_shared_between_the_two_markets(self):
        """Both markets name a session date; only the prefix keeps them apart."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            save_profiles({"BTC": crypto_profile()}, out, "2026-09-17",
                          market="crypto", grid=CRYPTO_GRID)
            self.assertEqual(load_profiles(out, "2026-09-17", market="equity"), {})
            self.assertIn("BTC", load_profiles(out, "2026-09-17", market="crypto",
                                               grid=CRYPTO_GRID))

    def test_pruning_one_market_leaves_the_other_alone(self):
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            save_profiles({"BTC": crypto_profile()}, out, "2026-09-17",
                          market="crypto", grid=CRYPTO_GRID)
            save_profiles({"AAA": flat_profile()}, out, "2026-09-17", market="equity")
            prune_cache(out, "2026-09-17", market="equity")
            self.assertIn("BTC", load_profiles(out, "2026-09-17", market="crypto",
                                               grid=CRYPTO_GRID))


class TestCryptoFeedColumns(unittest.TestCase):
    """What `fetch_crypto` publishes, and what it must survive losing.

    The crypto post-processing runs outside a try block, so every column read
    here is a column whose withdrawal freezes the poll. New reads are kept
    guarded for that reason.
    """

    def payload(self, row):
        from src.bidask.feed import fetch_crypto
        with mock.patch("src.bidask.feed.screeners") as screeners:
            screeners.crypto = lambda: _StubQuery(1, frame([row]))
            return fetch_crypto(CFG)

    def base(self, **extra):
        row = {"ticker": "BINANCE:BTCUSDT.P", "base_currency": "BTC",
               "close": 76_000.0, "volume": 1_000.0, "24h_vol|5": 9.4e8,
               "24h_close_change|5": 3.2, "update_mode": "streaming"}
        row.update(extra)
        return row

    def test_the_instrument_behind_the_display_symbol_is_published(self):
        """`BTC` on screen is `BINANCE:BTCUSDT.P` to the chart socket.

        The baseline warm-up fetches bars for this column; a bare `BTC`
        resolves on no exchange, so every crypto row would lose its baseline.
        """
        rows = self.payload(self.base()).rows
        self.assertEqual(rows.iloc[0]["symbol"], "BTC")
        self.assertEqual(rows.iloc[0]["feed_symbol"], "BINANCE:BTCUSDT.P")

    def test_the_24_hour_price_reference_lands_in_change_pct(self):
        """R15's input. `24h_close_change|5` is the tab's only price test."""
        self.assertEqual(self.payload(self.base()).rows.iloc[0]["change_pct"], 3.2)

    def test_the_utc_day_volume_is_the_gate_numerator_not_the_rolling_figure(self):
        # MEASURED 2026-09-17: `volume` is the sum since 00:00 UTC and
        # `24h_vol|5` is the rolling 24-hour figure, 1.25x it at 19:03 UTC.
        # They are carried as separate columns so neither can stand in for the
        # other.
        row = self.payload(self.base()).rows.iloc[0]
        self.assertEqual(row["volume"], 1_000.0)
        self.assertEqual(row["vol_24h"], 9.4e8)
        self.assertEqual(row["dollar_vol"], 76_000.0 * 1_000.0)

    def test_an_unreported_24h_volume_is_null_never_nan(self):
        """`write_state` serializes with `allow_nan=False`.

        One NaN costs the whole document rather than one field, and the page
        then freezes on its last good poll with nothing on screen to say why.
        """
        row = self.payload(self.base(**{"24h_vol|5": None})).rows.iloc[0]
        self.assertIsNone(row["vol_24h"])
        json.dumps({"vol_24h": row["vol_24h"]}, allow_nan=False)

    def test_a_withdrawn_24h_column_does_not_kill_the_poll(self):
        row = self.base()
        row.pop("24h_vol|5")
        payload = self.payload(row)
        self.assertEqual(payload.error, "")
        self.assertEqual(len(payload.rows), 1)


if __name__ == "__main__":
    unittest.main()
