"""The VARS, Momentum and Volume builders publish one `zone_map` block each.

The fixture writes the screener parquet the way run_screener.py does — from
a zero-filled master frame — so the fillna trap is real here: the screener
row holds ema20_w = 0 while the master holds NaN.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data as ex

DATE = '2026-10-08'
TICKERS = ['AAA', 'BBB', 'CCC', 'DDD']
THEMES = {t: ['Cybersecurity / Network'] for t in TICKERS}


def _master():
    rows = []
    for i, t in enumerate(TICKERS):
        rows.append({
            'date': DATE, 'ticker': t, 'close': 100.0, 'high': 101.0, 'low': 98.0,
            'volume': 2_000_000, 'vol_sma50': 2_000_000,
            'ema10': 101.0, 'ema20': 99.0, 'ema20_w': 95.0, 'sma50_full': 90.0,
            'atr14': 2.0, 'avg_dollar_vol': 150e6, 'adr_pct': 0.05,
            'vars': 6.0 - i, 'vars_20ema': 5.0 - i, 'rs_sts_pct': 90.0 - i,
            'tight_range': False, 'tight_len': 0, 'tightness': 0.5,
            'days_since_highest_volume': 3, 'rela_perf_1mo_rank': 50,
        })
    df = pd.DataFrame(rows)
    df.loc[df['ticker'] == 'DDD', 'ema20_w'] = float('nan')   # a young listing
    return df


class ZoneMapBuilderTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        master = _master()
        self.master_file = self.root / 'master' / f'master_{DATE}.parquet'
        files = {
            self.master_file: master,
            # run_screener.py saves rows from a zero-filled frame.
            self.root / 'vars' / f'vars_{DATE}.parquet': master.fillna(0),
            self.root / 'momentum_136' / f'momentum_136_{DATE}.parquet': master.fillna(0),
            self.root / 'volspike' / f'volspike_{DATE}.parquet': master.fillna(0),
        }
        for path, frame in files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            su.save_df_to_parquet(frame, path)
        for p in (
            patch.object(ex, 'SCREENING_OUTPUT_DIR', self.root),
            patch.object(ex, 'FUNDAMENTALS_DB', self.root / 'no-such.db'),
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEMES),
        ):
            p.start()
            self.addCleanup(p.stop)

    def _snaps(self):
        return {
            'vars': ex._build_vars_snapshot(self.root / 'vars' / f'vars_{DATE}.parquet', {}),
            'momentum': ex._build_momentum_136_snapshot(
                self.root / 'momentum_136' / f'momentum_136_{DATE}.parquet', {}),
            'volume': ex._build_volume_snapshot(DATE, {}),
        }

    def test_each_builder_publishes_the_map_and_no_network(self):
        for name, snap in self._snaps().items():
            self.assertNotIn('network', snap, name)
            zm = snap['zone_map']
            self.assertEqual({'AAA', 'BBB', 'CCC'}, {t['ticker'] for t in zm['tickers']}, name)

    def test_the_filled_screener_row_does_not_reach_the_map(self):
        for name, snap in self._snaps().items():
            self.assertEqual(1, snap['zone_map']['n_no_weekly'], name)

    def test_a_ticker_reads_its_state_numbers_and_leaf(self):
        for name, snap in self._snaps().items():
            aaa = next(t for t in snap['zone_map']['tickers'] if t['ticker'] == 'AAA')
            self.assertEqual(('band', 'normal', 'buy_area'),
                             (aaa['zone_daily'], aaa['zone_weekly'], aaa['state']), name)
            # The Momentum payload carries no `vars` field, so its map has none.
            expected_vars = None if name == 'momentum' else 6.0
            self.assertEqual((expected_vars, 90.0), (aaa['vars'], aaa['rs']), name)
            self.assertEqual(['Cybersecurity / Network'], aaa['leaves'], name)

    def test_a_missing_master_parquet_publishes_none(self):
        self.master_file.unlink()
        for name, snap in self._snaps().items():
            self.assertIsNone(snap['zone_map'], name)

    def test_all_three_blocks_share_one_shape(self):
        shapes = {name: set(snap['zone_map']) for name, snap in self._snaps().items()}
        self.assertEqual(1, len({frozenset(s) for s in shapes.values()}), shapes)


if __name__ == '__main__':
    unittest.main()
