"""Universe filter and relative-volume gate tests.

The gate is the **only** admission path to either column. A price move admits
nothing on its own, however far it has run — the absolute-change leg that used
to do that is retired, and `TestRetiredConfigKeys` pins that its key raises
rather than being ignored.

Two anchors are in play and confusing them is the expensive mistake here.
`elapsed_minutes` everywhere in this package counts from the **04:00** extended
anchor that Relative Volume at Time uses. Each state's floor schedule is written
in minutes since **its own** state began, so a regular-session band at 15
minutes means 09:45, which is `OPEN_AT + 15` on the 04:00 clock. Reading one as
the other shifts every regular-session band by five and a half hours.
"""

import unittest
from unittest import mock

import numpy as np
import pandas as pd

from src.bidask.config import load_config
from src.bidask.feed import _bare_ticker
from src.bidask.rvol_at_time import BARS_PER_SESSION, CRYPTO, baseline_at
from src.bidask.session_state import CLOSED, MARKET, POST_MARKET, PRE_MARKET
from src.bidask.universe import (
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


if __name__ == "__main__":
    unittest.main()
