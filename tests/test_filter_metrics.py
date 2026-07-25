"""avg_vol / adr_pct payload coverage for the dashboard's V/A filter toggles.

Every stock tab that carries the toggles must ship both metrics per ticker, for
the session being displayed. The fail-open contract is the fragile half: a
missing metric must serialize as null so the toggle passes the ticker through
rather than dimming it as illiquid. The screener-backed builders `.fillna(0)`
their frames, so "missing" reaches the payload as 0.0 — every builder is
asserted against that path, not just the happy one.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from config.settings import CONFIG
from src.reporting import export_dashboard_data
from src.reporting.export_dashboard_data import export_radar, filter_metrics
from src.themes.l1_score import build_radar_universe, compute_leaf_scores, radar_config

DATE = '2026-07-24'

# LIQUID passes both floors; THIN fails both; NOVOL has no volume column value
# at all (the recent-IPO shape the radar universe admits on dollar-vol alone).
THEME_MAP = {
    'LIQUID': ['Cybersecurity / Network'],
    'THIN': ['Cybersecurity / Network'],
    'NOVOL': ['Cybersecurity / Network'],
}

ROWS = [
    # ticker,   vol_sma50,  adr_pct
    ('LIQUID', 5_000_000.0, 0.0812),
    ('THIN', 400_000.0, 0.0231),
    ('NOVOL', 0.0, 0.0),  # post-fillna(0) shape of a missing metric
]


def _screener_df():
    return pd.DataFrame([
        {
            'date': DATE,
            'ticker': ticker,
            'close': 100.0,
            'rs_sts_pct': 88.0,
            'vars': 7.0,
            'vars_20ema': 6.0,
            'days_since_highest_volume': 3,
            'vol_sma50': vol,
            'adr_pct': adr,
        }
        for ticker, vol, adr in ROWS
    ])


def _by_ticker(ticker_dicts):
    return {t['ticker']: t for t in ticker_dicts}


class FilterMetricsHelperTests(unittest.TestCase):
    def test_reads_both_metrics_off_the_row(self):
        self.assertEqual(
            filter_metrics({'vol_sma50': 5_000_000.0, 'adr_pct': 0.08123}),
            (5_000_000, 0.0812),
        )

    def test_volume_is_a_whole_number(self):
        avg_vol, _ = filter_metrics({'vol_sma50': 1_234_567.89, 'adr_pct': 0.05})
        self.assertIsInstance(avg_vol, int)
        self.assertEqual(avg_vol, 1_234_567)

    def test_absent_keys_fail_open(self):
        self.assertEqual(filter_metrics({}), (None, None))

    def test_zero_fails_open_rather_than_reading_as_illiquid(self):
        # The builders fillna(0), so a genuinely missing metric arrives as 0.0.
        # Emitting 0 would dim every such ticker under the 1M / 4% floors.
        self.assertEqual(filter_metrics({'vol_sma50': 0.0, 'adr_pct': 0.0}), (None, None))

    def test_nan_fails_open(self):
        self.assertEqual(
            filter_metrics({'vol_sma50': float('nan'), 'adr_pct': float('nan')}),
            (None, None),
        )


class ScreenerBackedSnapshotTests(unittest.TestCase):
    """momentum_136, VARS, and Volume all read from a per-screener parquet."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        root = Path(cls._tmp.name)
        momentum = root / 'momentum_136' / f'momentum_136_{DATE}.parquet'
        su.save_df_to_parquet(_screener_df(), momentum)
        su.save_df_to_parquet(_screener_df(), root / 'vars' / f'vars_{DATE}.parquet')
        su.save_df_to_parquet(_screener_df(), root / 'volspike' / f'volspike_{DATE}.parquet')

        with (
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEME_MAP),
            patch.object(export_dashboard_data, 'FUNDAMENTALS_DB', root / 'missing.db'),
            patch.object(export_dashboard_data, 'SCREENING_OUTPUT_DIR', root),
        ):
            cls.momentum = export_dashboard_data._build_momentum_136_snapshot(
                momentum, day_flags={})
            cls.vars_snap = export_dashboard_data._build_vars_snapshot(
                root / 'vars' / f'vars_{DATE}.parquet', day_flags={})
            cls.volume = export_dashboard_data._build_volume_snapshot(DATE, day_flags={})

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _assert_payload(self, ticker_dicts):
        rows = _by_ticker(ticker_dicts)
        self.assertEqual(rows['LIQUID']['avg_vol'], 5_000_000)
        self.assertEqual(rows['LIQUID']['adr_pct'], 0.0812)
        self.assertEqual(rows['THIN']['avg_vol'], 400_000)
        self.assertEqual(rows['THIN']['adr_pct'], 0.0231)
        self.assertIsNone(rows['NOVOL']['avg_vol'])
        self.assertIsNone(rows['NOVOL']['adr_pct'])

    def test_momentum_snapshot_carries_both_metrics(self):
        self._assert_payload(self.momentum['themes'][0]['tickers'])

    def test_vars_snapshot_carries_both_metrics(self):
        leaf = self.vars_snap['themes'][0]['leaves'][0]
        self._assert_payload(leaf['tickers'])

    def test_volume_snapshot_carries_both_metrics(self):
        self._assert_payload(self.volume['themes'][0]['tickers'])

    def test_metrics_survive_json_serialization(self):
        # numpy scalars leaking through would break json.dump at export time.
        json.dumps(self.vars_snap)


class ParabolicItemTests(unittest.TestCase):
    def test_gains_avg_vol_without_disturbing_existing_adr(self):
        item = export_dashboard_data._parabolic_item_from_row(
            {'ticker': 'liquid', 'close': 100.0, 'vol_sma50': 5_000_000.0,
             'adr_pct': 0.0812, 'atr_multi_50sma': 12.0},
            fundamentals={},
        )
        self.assertEqual(item['avg_vol'], 5_000_000)
        self.assertEqual(item['adr_pct'], 0.0812)

    def test_missing_volume_fails_open(self):
        item = export_dashboard_data._parabolic_item_from_row(
            {'ticker': 'novol', 'close': 100.0, 'adr_pct': 0.0812},
            fundamentals={},
        )
        self.assertIsNone(item['avg_vol'])


def _radar_master_df():
    return pd.DataFrame([
        {
            'date': DATE,
            'ticker': ticker,
            'close': 100.0,
            'avg_dollar_vol': 50_000_000.0,
            'rs_sts_pct': 60.0,
            'vars': 5.0,
            'rela_perf_1mo_rank': 60,
            'vol_sma50': vol if vol else float('nan'),
            'adr_pct': adr if adr else float('nan'),
        }
        for ticker, vol, adr in ROWS
    ])


class RadarMemberTests(unittest.TestCase):
    """The Themes tab is screener-independent, so its chips need the metrics too."""

    def _members(self):
        cfg = radar_config()
        universe = build_radar_universe(_radar_master_df(), set(THEME_MAP), cfg)
        leaves = compute_leaf_scores(
            universe, {'Cybersecurity / Network': list(THEME_MAP)}, cfg)
        return {m['ticker']: m for m in leaves[0]['members']}

    def test_members_carry_both_metrics(self):
        members = self._members()
        self.assertEqual(members['LIQUID']['vol_sma50'], 5_000_000.0)
        self.assertEqual(members['LIQUID']['adr_pct'], 0.0812)

    def test_nan_metrics_pass_through_as_none(self):
        # NOVOL has NaN vol_sma50 — the radar admits it on the dollar-vol floor
        # alone, and the toggle must not dim what it cannot measure.
        members = self._members()
        self.assertIn('NOVOL', members)
        self.assertIsNone(members['NOVOL']['vol_sma50'])
        self.assertIsNone(members['NOVOL']['adr_pct'])

    def test_metrics_are_payload_only_and_do_not_move_scores(self):
        members = self._members()
        # Identical rs/vars across the fixture -> identical composites. If either
        # new column had leaked into scoring, these would diverge.
        composites = {round(m['composite'], 6) for m in members.values()}
        self.assertEqual(len(composites), 1)

    def test_chips_carry_both_metrics_through_the_export(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, out_dir = Path(tmp) / 'screening', Path(tmp) / 'docs'
            out_dir.mkdir()
            su.save_df_to_parquet(_radar_master_df(),
                                  root / 'master' / f'master_{DATE}.parquet')
            screener = CONFIG['screeners'][0]
            su.save_df_to_parquet(pd.DataFrame({'ticker': ['LIQUID']}),
                                  root / screener / f'{screener}_{DATE}.parquet')

            with patch('src.themes.l1_score.load_ticker_themes', return_value=THEME_MAP):
                export_radar({}, root=root, out_dir=out_dir)

            radar = json.loads((out_dir / 'radar.json').read_text())
            chips = _by_ticker(radar['l1s'][0]['leaves'][0]['tickers'])
            self.assertEqual(chips['LIQUID']['avg_vol'], 5_000_000)
            self.assertEqual(chips['LIQUID']['adr_pct'], 0.0812)
            self.assertIsNone(chips['NOVOL']['avg_vol'])


if __name__ == '__main__':
    unittest.main()
