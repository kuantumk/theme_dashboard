"""The `highlight` field, as the dashboard producers publish it.

`tests/test_highlight_tier.py` pins the ladder itself. This module pins the
wiring: every list-tab producer emits the tier under one key, each producer
reads the session's own master bar, and the short rung reaches the newest
session only.

⛔ **Short interest reaches the newest session and no other.** Finviz publishes
one current figure with no per-session history. The short rung outranks the coil
and moving-average rungs, which *are* computed from that session's own parquet,
so today's crowding pinned onto an old price bar would erase that session's real
signal across the whole 180-day window. CLAUDE.md's SI section calls that shape
"a fabricated number, not a stale one".

⛔ **The radar reads its moving averages from its own master-row lookup.**
`compute_radar`'s member dicts carry no averages at all, so a tier built from
them would be silently absent on every chip. The equality test against the VARS
snapshot is what proves the lookup found the same row.

⛔ **The tier changes nothing else.** The radar's scores, ordering, `coiled`
booleans and `n_coiled` counts must be byte-identical with and without the tier
inputs, or a display marker has moved the board.
"""

import contextlib
import io
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data as ex

DATE = '2026-09-23'
OLDER = '2026-09-22'

# ticker, tight_base, ema10, ema20, sma50 — one row per ladder outcome.
LADDER_ROWS = [
    # Coil outranks a crowded short and a stacked stack.
    ('COILER', True, 12.0, 11.0, 10.0),
    # 34% short interest: 'short' on the newest session, 'ma_up' on older ones.
    ('CROWDED', False, 12.0, 11.0, 10.0),
    # No fundamentals row at all, so the short rung is unanswerable.
    ('STACKED', False, 12.0, 11.0, 10.0),
    # EMA20 under SMA50.
    ('SPLITTER', False, 12.0, 11.0, 13.0),
    # No SMA50. `.fillna(0)` turns it into 0.0, which must read as absent.
    ('NOBARS', False, 12.0, 11.0, float('nan')),
    # EMA10 under EMA20: no moving-average rung fires.
    ('QUIET', False, 10.0, 11.0, 12.0),
]

THEMES = {t: ['Cybersecurity / Network'] for t, *_ in LADDER_ROWS}

# Only CROWDED and COILER are crowded. STACKED is deliberately absent.
SHORT_ROWS = {
    'COILER': 34.0,
    'CROWDED': 34.0,
}


def _master_frame(date_str=DATE):
    """One master session carrying every column the six producers read."""
    rows = []
    for i, (ticker, tight, ema10, ema20, sma50) in enumerate(LADDER_ROWS):
        rows.append({
            'date': date_str,
            'ticker': ticker,
            'close': 50.0,
            'high': 51.0,
            'low': 49.0,
            'volume': 2_000_000,
            'avg_dollar_vol': 50_000_000.0,
            'adr_pct': 0.05,
            'rs_sts_pct': 90.0 - i,
            'vars': 10.0 - i,
            'vars_20ema': 9.0 - i,
            'rela_perf_1mo_rank': 90 - i,
            'tightness': 0.10 + 0.05 * i,
            'tight_base': tight,
            'ema10': ema10,
            'ema20': ema20,
            # Both columns, because a real master parquet carries both and the
            # ladder must read the full-window one. `sma50` settles for 25 bars,
            # so it holds a partial mean on a young listing; the deliberately
            # wrong value here fails every test if the ladder ever reads it.
            'sma50': 999.0,
            'sma50_full': sma50,
            'days_since_highest_volume': 3,
        })
    return pd.DataFrame(rows)


def _fundamentals_db(path):
    """A fundamentals.db holding short interest for the crowded names only."""
    conn = sqlite3.connect(path)
    conn.execute(
        'CREATE TABLE fundamentals ('
        'ticker TEXT, shares_float REAL, eps_growth_yoy REAL, '
        'sales_growth_yoy REAL, short_interest REAL, inst_transactions REAL)'
    )
    conn.executemany(
        'INSERT INTO fundamentals VALUES (?, ?, ?, ?, ?, ?)',
        [(t, None, None, None, si, None) for t, si in SHORT_ROWS.items()],
    )
    conn.commit()
    conn.close()
    return path


def _radar_tiers(master_file, fundamentals=None, newest_session=False):
    """Ticker -> tier from one radar snapshot."""
    with patch('src.themes.l1_score.load_ticker_themes', return_value=THEMES):
        snap = ex._build_radar_snapshot(
            master_file, set(), {},
            fundamentals=fundamentals, newest_session=newest_session,
        )
    return snap, {
        chip['ticker']: chip.get('highlight')
        for l1 in snap['l1s'] for leaf in l1['leaves'] for chip in leaf['tickers']
    }


def _vars_tiers(parquet_file):
    """Ticker -> tier from one VARS snapshot, with no fundamentals reachable."""
    missing_db = parquet_file.parent / 'no-such-fundamentals.db'
    with (
        patch('src.themes.theme_registry.load_ticker_themes', return_value=THEMES),
        patch.object(ex, 'FUNDAMENTALS_DB', missing_db),
    ):
        snap = ex._build_vars_snapshot(parquet_file, {})
    return snap, {
        row['ticker']: row.get('highlight')
        for l1 in snap['themes'] for leaf in l1['leaves'] for row in leaf['tickers']
    }


class RadarHighlightTests(unittest.TestCase):
    """The Themes tab resolves its averages through a master-row lookup."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.master = self.tmp / 'master_2026-09-23.parquet'
        su.save_df_to_parquet(_master_frame(), self.master)
        self.addCleanup(self._tmp.cleanup)

    def test_the_ladder_reaches_every_rung_through_the_row_lookup(self):
        _, tiers = _radar_tiers(
            self.master, fundamentals={
                t: {'short_interest': si} for t, si in SHORT_ROWS.items()},
            newest_session=True,
        )
        self.assertEqual(tiers['COILER'], 'coil')
        self.assertEqual(tiers['CROWDED'], 'short')
        self.assertEqual(tiers['STACKED'], 'ma_up')
        self.assertEqual(tiers['SPLITTER'], 'ma_split')

    def test_a_ticker_absent_from_fundamentals_still_earns_a_moving_average_tier(self):
        # The radar scores every tagged ticker while fundamentals.db holds the
        # screened union only, so most chips reach the ladder with no short
        # interest. They must fall through to the averages, not to nothing.
        _, tiers = _radar_tiers(
            self.master, fundamentals={
                t: {'short_interest': si} for t, si in SHORT_ROWS.items()},
            newest_session=True,
        )
        self.assertNotIn('STACKED', SHORT_ROWS)
        self.assertEqual(tiers['STACKED'], 'ma_up')

    def test_an_absent_sma50_earns_no_tier(self):
        _, tiers = _radar_tiers(self.master)
        self.assertIsNone(tiers['NOBARS'])
        self.assertIsNone(tiers['QUIET'])

    def test_a_member_with_no_master_row_earns_no_tier(self):
        # compute_radar cannot return a member the master frame lacks, so this
        # pins the guard directly: an empty row answers every rung "absent".
        self.assertIsNone(ex._highlight_from_row({}))

    def test_the_tier_changes_nothing_else_in_the_snapshot(self):
        # Scores, ordering, `coiled` and `n_coiled` must be identical with and
        # without the tier inputs. A display marker may not move the board.
        plain, _ = _radar_tiers(self.master)
        tinted, _ = _radar_tiers(
            self.master, fundamentals={
                t: {'short_interest': si} for t, si in SHORT_ROWS.items()},
            newest_session=True,
        )

        def _strip(snapshot):
            return [
                {**l1, 'leaves': [
                    {**leaf, 'tickers': [
                        {k: v for k, v in chip.items() if k != 'highlight'}
                        for chip in leaf['tickers']
                    ]} for leaf in l1['leaves']
                ]} for l1 in snapshot['l1s']
            ]

        self.assertEqual(_strip(plain), _strip(tinted))
        coiled = sum(1 for t, tight, *_ in LADDER_ROWS if tight)
        self.assertEqual(tinted['l1s'][0]['n_coiled'], coiled)
        self.assertEqual(tinted['l1s'][0]['leaves'][0]['n_coiled'], coiled)


class RadarAgainstVarsTests(unittest.TestCase):
    """The radar's row join must find the row the VARS builder reads."""

    def test_the_two_producers_agree_on_every_ticker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            frame = _master_frame()
            master = root / 'master_2026-09-23.parquet'
            vars_file = root / 'vars_2026-09-23.parquet'
            su.save_df_to_parquet(frame, master)
            su.save_df_to_parquet(frame, vars_file)

            # Neither side sees short interest, so any disagreement is the
            # radar's lookup missing a row rather than a freshness rule.
            _, radar = _radar_tiers(master)
            _, vars_snap = _vars_tiers(vars_file)

            self.assertEqual(set(radar), set(vars_snap))
            self.assertEqual(radar, vars_snap)
            self.assertEqual(radar['COILER'], 'coil')


class FilledZeroTests(unittest.TestCase):
    """`.fillna(0)` must not read as a stacked trend."""

    def test_a_zero_filled_sma50_earns_no_tier(self):
        with tempfile.TemporaryDirectory() as tmp:
            vars_file = Path(tmp) / 'vars_2026-09-23.parquet'
            su.save_df_to_parquet(_master_frame(), vars_file)
            snap, tiers = _vars_tiers(vars_file)

            # Prove the builder really filled the hole rather than dropping it.
            row = next(
                r for l1 in snap['themes'] for leaf in l1['leaves']
                for r in leaf['tickers'] if r['ticker'] == 'NOBARS'
            )
            self.assertIsNotNone(row)
            self.assertIsNone(tiers['NOBARS'])


class NewestSessionOnlyTests(unittest.TestCase):
    """AE9. A historical session renders only the rungs its own parquet holds."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.db = _fundamentals_db(self.tmp / 'fundamentals.db')
        self.addCleanup(self._tmp.cleanup)

    def test_the_radar_drops_the_short_rung_on_an_older_session(self):
        master = self.tmp / 'master_2026-09-22.parquet'
        su.save_df_to_parquet(_master_frame(OLDER), master)
        table = {t: {'short_interest': si} for t, si in SHORT_ROWS.items()}

        _, newest = _radar_tiers(master, fundamentals=table, newest_session=True)
        _, older = _radar_tiers(master, fundamentals=table, newest_session=False)

        self.assertEqual(newest['CROWDED'], 'short')
        self.assertEqual(older['CROWDED'], 'ma_up')
        # The rungs computed from the session's own parquet are untouched.
        self.assertEqual(older['COILER'], 'coil')
        self.assertEqual(older['SPLITTER'], 'ma_split')
        self.assertIsNone(older['QUIET'])

    def _screener_tiers(self, builder, arg, newest_session):
        with (
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEMES),
            patch.object(ex, 'FUNDAMENTALS_DB', self.db),
        ):
            snap = builder(arg, {}, newest_session=newest_session)
        rows = []
        for theme in snap['themes']:
            rows += theme.get('tickers') or [
                r for leaf in theme.get('leaves', []) for r in leaf['tickers']
            ]
        return {r['ticker']: r.get('highlight') for r in rows}

    def test_the_vars_builder_drops_the_short_rung_on_an_older_session(self):
        parquet = self.tmp / 'vars_2026-09-22.parquet'
        su.save_df_to_parquet(_master_frame(OLDER), parquet)
        newest = self._screener_tiers(ex._build_vars_snapshot, parquet, True)
        older = self._screener_tiers(ex._build_vars_snapshot, parquet, False)
        self.assertEqual(newest['CROWDED'], 'short')
        self.assertEqual(older['CROWDED'], 'ma_up')
        self.assertEqual(older['COILER'], 'coil')

    def test_the_momentum_builder_drops_the_short_rung_on_an_older_session(self):
        parquet = self.tmp / 'momentum_136_2026-09-22.parquet'
        su.save_df_to_parquet(_master_frame(OLDER), parquet)
        newest = self._screener_tiers(ex._build_momentum_136_snapshot, parquet, True)
        older = self._screener_tiers(ex._build_momentum_136_snapshot, parquet, False)
        self.assertEqual(newest['CROWDED'], 'short')
        self.assertEqual(older['CROWDED'], 'ma_up')
        self.assertEqual(older['SPLITTER'], 'ma_split')

    def test_the_volume_builder_drops_the_short_rung_on_an_older_session(self):
        for scan in ('volspike', 'denvol'):
            su.save_df_to_parquet(
                _master_frame(OLDER), self.tmp / scan / f'{scan}_{OLDER}.parquet')
        with patch.object(ex, 'SCREENING_OUTPUT_DIR', self.tmp):
            newest = self._screener_tiers(ex._build_volume_snapshot, OLDER, True)
            older = self._screener_tiers(ex._build_volume_snapshot, OLDER, False)
        self.assertEqual(newest['CROWDED'], 'short')
        self.assertEqual(older['CROWDED'], 'ma_up')

    def test_the_parabolic_row_drops_the_short_rung_on_an_older_session(self):
        row = _master_frame(OLDER).set_index('ticker').loc['CROWDED']
        row['ticker'] = 'CROWDED'
        table = {t: {'short_interest': si} for t, si in SHORT_ROWS.items()}
        self.assertEqual(
            ex._parabolic_item_from_row(row, table, newest_session=True)['highlight'],
            'short')
        self.assertEqual(
            ex._parabolic_item_from_row(row, table, newest_session=False)['highlight'],
            'ma_up')


class PartialWindowTests(unittest.TestCase):
    """The ladder reads `sma50_full`, never the 25-bar `sma50`.

    A master row carries both. `sma50` settles for 25 bars because
    `atr_multi_50sma` and the screeners want it that way, so on a young listing
    it holds a partial mean — finite, positive, and invisible to the
    zero-as-missing rule. Reading it would state a stacked trend on a stock with
    no 50-day trend to read.
    """

    def test_a_row_carrying_only_the_partial_average_earns_no_tier(self):
        row = {'ema10': 12.0, 'ema20': 11.0, 'sma50': 10.0}
        self.assertIsNone(ex._highlight_from_row(row))

    def test_the_full_window_column_is_what_answers(self):
        row = {'ema10': 12.0, 'ema20': 11.0, 'sma50_full': 10.0}
        self.assertEqual(ex._highlight_from_row(row), 'ma_up')

    def test_the_partial_average_cannot_override_the_full_one(self):
        # Both present and disagreeing: the full window decides.
        row = {'ema10': 12.0, 'ema20': 11.0, 'sma50': 99.0, 'sma50_full': 10.0}
        self.assertEqual(ex._highlight_from_row(row), 'ma_up')

    def test_a_frame_missing_the_column_entirely_does_not_raise(self):
        """The case the next workflow run after this ships will actually hit.

        The local parquet cache keeps the newest ten sessions, so it can still
        hold sessions written before `sma50_full` existed. `_bars_by_ticker` drops
        an absent column instead of raising, and the ladder then answers nothing.
        Narrowing that filter to a plain `master_df[['ticker', *columns]]` would
        turn the same input into a KeyError, and no other test would see it.
        """
        frame = _master_frame().drop(columns=['sma50_full', 'tight_base'])
        bars = ex._bars_by_ticker(frame, ex.HIGHLIGHT_ROW_COLUMNS)
        self.assertIn('COILER', bars)
        self.assertNotIn('sma50_full', bars['COILER'])
        for ticker in bars:
            self.assertIsNone(
                ex._highlight_from_row(bars[ticker]),
                f'{ticker} earned a tier from a frame with no full-window SMA50')

    def test_the_narrowed_lookup_carries_the_full_window_column(self):
        # A column list that forgot `sma50_full` would blank the two
        # moving-average rungs on every tab with nothing on screen to say why.
        self.assertIn('sma50_full', ex.HIGHLIGHT_ROW_COLUMNS)
        self.assertNotIn('sma50', ex.HIGHLIGHT_ROW_COLUMNS)


class SiHighlightTests(unittest.TestCase):
    """The SI tab builds the current session only, so it always holds the rung."""

    def test_the_roster_short_interest_reaches_the_ladder(self):
        master = _master_frame()
        master['max60'] = 100.0          # close 50 -> -50% drawdown, gate passes
        master['drop_15d'] = -0.40
        master['max_down_streak'] = 4
        # Every SI row clears the tab's own 12% gate, but the ladder's rung
        # needs 20%. QUIET sits in that gap, so it reaches the tab and earns
        # no tier — the rung is stricter than the roster it reads.
        si_rows = [
            {'ticker': t, 'si': 15.0 if t == 'QUIET' else 34.0}
            for t, *_ in LADDER_ROWS
        ]
        cfg = {
            'min_short_interest': 12.0, 'max_drawdown_60d': -0.25,
            'max_drop_15d': -0.25, 'min_tickers_per_l1': 3,
            'top_k_si': 3, 'hot_radar_rank': 10,
        }
        snap = ex._build_si_snapshot(
            si_rows, master, {}, THEMES, {}, cfg, si_date=DATE)
        tiers = {
            r['ticker']: r.get('highlight')
            for l1 in snap['themes'] for leaf in l1['leaves'] for r in leaf['tickers']
        }
        self.assertEqual(tiers['COILER'], 'coil')
        self.assertEqual(tiers['CROWDED'], 'short')
        self.assertIsNone(tiers['QUIET'])


def _etf_frame(closes):
    """An OHLC frame in the capitalized shape yfinance returns."""
    index = pd.date_range('2026-01-01', periods=len(closes), freq='D')
    return pd.DataFrame({
        'Open': [c - 0.9 for c in closes],
        'High': [c + 0.5 for c in closes],
        'Low': [c - 0.5 for c in closes],
        'Close': list(closes),
    }, index=index)


def _etf_metrics(frame, spy=None):
    baseline = spy if spy is not None else pd.Series(dtype=float)
    with patch('yfinance.download', return_value=frame):
        with contextlib.redirect_stdout(io.StringIO()):
            return ex.fetch_etf_metrics(['TQQQ'], baseline)


class EtfHighlightTests(unittest.TestCase):
    """AE6. The ETF fetch can reach the moving-average rungs and no others."""

    def test_a_rising_series_earns_the_stacked_tier_and_nothing_higher(self):
        metrics = _etf_metrics(_etf_frame([100.0 + i for i in range(120)]))
        self.assertEqual(metrics['TQQQ']['highlight'], 'ma_up')
        # No short interest and no tight base reach this producer at all.
        self.assertNotIn(metrics['TQQQ']['highlight'], ('short', 'coil'))

    def test_a_ticker_whose_only_metric_is_the_tier_still_ships(self):
        # A 120-bar rising series with no SPY baseline: VARS is unanswerable and
        # the last bar is neither tight nor an inside day, so the tier is the
        # only thing this ETF earned. The early skip must let it through.
        metrics = _etf_metrics(_etf_frame([100.0 + i for i in range(120)]))
        self.assertIn('TQQQ', metrics)
        self.assertIsNone(metrics['TQQQ']['vars'])
        self.assertIsNone(metrics['TQQQ']['color'])
        self.assertEqual(metrics['TQQQ']['highlight'], 'ma_up')

    def test_a_series_below_the_sma50_minimum_period_earns_no_tier(self):
        # 20 bars against `min_periods=25`, so SMA50 is NaN. The row survives on
        # its day-pattern colour, which is what lets the assertion see the tier.
        closes = [100.0 + 0.02 * i for i in range(20)]
        frame = _etf_frame(closes)
        frame['Open'] = [c - 0.01 for c in closes]   # tight body, near the EMAs
        metrics = _etf_metrics(frame)
        self.assertEqual(metrics['TQQQ']['color'], 'green')
        self.assertIsNone(metrics['TQQQ']['highlight'])

    def test_a_partial_window_earns_no_tier_however_plainly_it_rises(self):
        """The one input shape zero-as-missing cannot catch.

        Between 25 and 49 bars a `min_periods=25` average returns a number — a
        30-bar mean wearing a 50-day label, finite and positive. Measured: such a
        series scored `ma_up` before this producer moved to a full window, which
        is a stacked-trend claim about a stock with no 50-day trend to read. The
        20-bar case above never reached the ladder at all, so it did not cover
        this. The EP scans refuse the same shape through their own `sma50_full`.
        """
        for bars in (25, 30, 49):
            with self.subTest(bars=bars):
                metrics = _etf_metrics(
                    _etf_frame([100.0 + i for i in range(bars)]))
                tier = metrics.get('TQQQ', {}).get('highlight')
                self.assertIsNone(
                    tier,
                    f'{bars} bars scored {tier!r} off a partial 50-day mean')

    def test_fifty_bars_is_the_first_window_the_ladder_will_read(self):
        """The boundary the test above stops at, from the other side."""
        metrics = _etf_metrics(_etf_frame([100.0 + i for i in range(50)]))
        self.assertEqual(metrics['TQQQ']['highlight'], 'ma_up')


if __name__ == '__main__':
    unittest.main()
