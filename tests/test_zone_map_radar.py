"""Theme Viz: members of the top radar leaves, read UNCAPPED.

radar_history.json caps chips at `radar.tickers_per_leaf` (10). The map must
still see every member of a top leaf, or a back-dated session would show
fewer names than the newest one.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data as ex

DATE = '2026-10-08'
OLDER = '2026-10-07'
CYBER = [f'T{i:02d}' for i in range(1, 13)]           # 12 members, one leaf
OPTICS = ['O1', 'O2', 'O3']
THEMES = {t: ['Cybersecurity / Network'] for t in CYBER}
THEMES.update({t: ['AI / Data Center / Optics'] for t in OPTICS})
THEMES['O1'] = ['AI / Data Center / Optics', 'Cybersecurity / Network']   # dual-tagged


def _master(date_str):
    rows = []
    for i, t in enumerate(CYBER + OPTICS):
        rows.append({
            'date': date_str, 'ticker': t, 'close': 100.0, 'high': 101.0, 'low': 98.0,
            'volume': 2_000_000, 'vol_sma50': 2_000_000,
            'ema10': 101.0, 'ema20': 99.0, 'ema20_w': 95.0, 'sma50_full': 90.0,
            'atr14': 2.0, 'avg_dollar_vol': 150e6, 'adr_pct': 0.05,
            'vars': 8.0 - 0.1 * i, 'vars_20ema': 7.0, 'rs_sts_pct': 95.0 - i,
            'rela_perf_1mo_rank': 50, 'tight_range': False, 'tight_len': 0,
            'tightness': 0.5, 'tight_pctile': 50.0, 'tight_gate': None,
        })
    return pd.DataFrame(rows)


class RadarZoneMapTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        (self.root / 'master').mkdir()
        for d in (DATE, OLDER):
            su.save_df_to_parquet(_master(d), self.root / 'master' / f'master_{d}.parquet')
        self.master_file = self.root / 'master' / f'master_{DATE}.parquet'
        for p in (
            patch('src.themes.l1_score.load_ticker_themes', return_value=THEMES),
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEMES),
        ):
            p.start()
            self.addCleanup(p.stop)

    def _snap(self, cap=10):
        return ex._build_radar_snapshot(self.master_file, set(), {}, tickers_per_leaf=cap)

    def test_a_capped_snapshot_still_maps_every_member(self):
        snap = self._snap(cap=10)
        cyber = next(lf for l1 in snap['l1s'] for lf in l1['leaves']
                     if lf['name'] == 'Cybersecurity / Network')
        self.assertEqual(10, len(cyber['tickers']))
        mapped = {t['ticker'] for t in snap['zone_map']['tickers']}
        self.assertTrue(set(CYBER) <= mapped, set(CYBER) - mapped)

    def test_only_members_of_the_top_leaves_are_mapped(self):
        cfg = dict(ex.CONFIG.get('zone_map') or {}, top_leaves=1)
        with patch.dict(ex.CONFIG, {'zone_map': cfg}):
            snap = self._snap(cap=None)
        top = next(lf for l1 in snap['l1s'] for lf in l1['leaves'] if lf['global_rank'] == 1)
        self.assertEqual({t['ticker'] for t in top['tickers']},
                         {t['ticker'] for t in snap['zone_map']['tickers']})

    def test_a_dual_tagged_member_is_one_dot_with_both_leaves(self):
        snap = self._snap(cap=None)
        o1 = [t for t in snap['zone_map']['tickers'] if t['ticker'] == 'O1']
        self.assertEqual(1, len(o1))
        self.assertEqual({'AI / Data Center / Optics', 'Cybersecurity / Network'},
                         set(o1[0]['leaves']))

    def test_every_history_entry_carries_the_map(self):
        out = self.root / 'out'
        out.mkdir()
        with patch.object(ex, '_load_fundamentals_for_tickers', return_value={}):
            ex.export_radar({}, root=self.root, out_dir=out)
        history = json.loads((out / 'radar_history.json').read_text(encoding='utf-8'))
        self.assertEqual(2, len(history))
        for entry in history:
            self.assertIsNotNone(entry['zone_map'], entry['report_date'])
            self.assertEqual(15, len(entry['zone_map']['tickers']), entry['report_date'])


if __name__ == '__main__':
    unittest.main()
