import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data


# 2026-07-17 regression fixture: cybersecurity leaders (RS 88-100, accelerating)
# vs stale AI/Data-Center leaders (higher 100-session VARS, collapsed RS,
# decelerating). vars_20ema sits 1.0 below vars for cyber (accel +1.0) and
# 1.0 above for AI (accel -1.0).
CYBER = [
    ('PANW', 14.07, 100.0), ('FTNT', 12.14, 88.5), ('CRWD', 11.94, 92.3),
    ('S', 3.93, 96.2), ('OKTA', 8.70, 96.2), ('DDOG', 9.92, 69.2),
    ('NET', 4.57, 96.2),
]
AI = [
    ('DELL', 15.81, 19.2), ('HPE', 13.85, 34.6), ('PENG', 12.78, 11.5),
    ('AMD', 11.95, 11.5), ('MRVL', 11.07, 3.8),
]

THEME_MAP = {
    'PANW': ['Cybersecurity / Network'],
    'FTNT': ['Cybersecurity / Network'],
    'CRWD': ['Cybersecurity / Endpoint'],
    'S': ['Cybersecurity / Endpoint'],
    'OKTA': ['Cybersecurity / Identity'],
    # Dual-tagged across families: the Software & Internet family (n=1) is
    # dropped by the family minimum, the cyber leaf keeps DDOG.
    'DDOG': ['Software & Internet / DevOps & Data', 'Cybersecurity / Data Security'],
    # Dual-tagged within ONE family: must count once in family n / avg_rs.
    'NET': ['Cybersecurity / Generalist', 'Cybersecurity / Data Security'],
    'DELL': ['AI / Data Center / Cloud & Hyperscalers'],
    'HPE': ['AI / Data Center / Cloud & Hyperscalers'],
    'PENG': ['AI / Data Center / Components'],
    'AMD': ['AI / Data Center / Chips & Processors'],
    'MRVL': ['AI / Data Center / Chips & Processors'],
    # Two-member family: below the 3-member family minimum, must not render.
    'RKLB': ['Space / Launch'],
    'ASTS': ['Space / Satellites'],
    # Hidden buckets.
    'JUNK2': ['Singleton'],
}


def _fixture_df():
    rows = []
    for ticker, vars_val, rs in CYBER:
        rows.append((ticker, vars_val, vars_val - 1.0, rs))
    for ticker, vars_val, rs in AI:
        rows.append((ticker, vars_val, vars_val + 1.0, rs))
    rows += [
        ('RKLB', 6.0, 5.0, 90.0),
        ('ASTS', 5.0, 4.0, 90.0),
        ('JUNK1', 4.0, 3.0, 50.0),   # untagged -> Uncategorized, hidden
        ('JUNK2', 4.0, 3.0, 50.0),   # Singleton, hidden
    ]
    return pd.DataFrame([
        {
            'ticker': t, 'date': '2026-07-17', 'vars': v, 'vars_20ema': e,
            'rs_sts_pct': r, 'close': 100.0,
        }
        for t, v, e, r in rows
    ])


class VarsFamilyGroupingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        parquet = Path(cls._tmp.name) / 'vars_2026-07-17.parquet'
        su.save_df_to_parquet(_fixture_df(), parquet)
        with (
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEME_MAP),
            patch.object(export_dashboard_data, 'FUNDAMENTALS_DB',
                         Path(cls._tmp.name) / 'missing.db'),
        ):
            cls.snap = export_dashboard_data._build_vars_snapshot(parquet, day_flags={})

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _family(self, name):
        return next(f for f in self.snap['themes'] if f['name'] == name)

    def test_only_families_meeting_minimum_render(self):
        names = [f['name'] for f in self.snap['themes']]
        self.assertEqual(sorted(names), ['AI', 'Cybersecurity'])
        # Space (n=2) and Software & Internet (n=1) fail the family min;
        # Uncategorized/Singleton are hidden outright.

    def test_family_ranking_score_is_top5_pooled_vars(self):
        ai = self._family('AI')
        cyber = self._family('Cybersecurity')
        # AI top-5: 15.81+13.85+12.78+11.95+11.07 = 65.46 -> 13.09
        self.assertEqual(ai['score'], 13.09)
        # Cyber top-5: 14.07+12.14+11.94+9.92+8.70 = 56.77 -> 11.35
        self.assertEqual(cyber['score'], 11.35)
        # Pure-VARS ranking keeps the stale-but-higher family first; rotation
        # is flagged by hot/accel instead (asserted below).
        self.assertEqual([f['name'] for f in self.snap['themes']], ['AI', 'Cybersecurity'])

    def test_hot_flag_and_acceleration_split_new_vs_stale_leadership(self):
        ai = self._family('AI')
        cyber = self._family('Cybersecurity')
        self.assertTrue(cyber['hot'])          # avg RS 91.2 >= 70
        self.assertEqual(cyber['avg_rs'], 91.2)
        self.assertFalse(ai['hot'])            # avg RS 16.1
        self.assertEqual(ai['avg_rs'], 16.1)
        self.assertEqual(cyber['accel'], 1.0)  # vars above 20EMA -> rotating in
        self.assertEqual(ai['accel'], -1.0)    # vars below 20EMA -> cooling off

    def test_two_member_leaf_renders_inside_family(self):
        cyber = self._family('Cybersecurity')
        endpoint = next(lf for lf in cyber['leaves'] if lf['name'] == 'Cybersecurity / Endpoint')
        self.assertEqual([t['ticker'] for t in endpoint['tickers']], ['CRWD', 'S'])
        # Leaves sort by avg VARS desc within the family.
        leaf_avgs = [lf['avg_vars'] for lf in cyber['leaves']]
        self.assertEqual(leaf_avgs, sorted(leaf_avgs, reverse=True))
        # Every leaf resolves to its family hub.
        for fam in self.snap['themes']:
            for leaf in fam['leaves']:
                self.assertEqual(leaf['l1'], fam['name'])

    def test_dual_tagged_ticker_counts_once_per_family(self):
        cyber = self._family('Cybersecurity')
        self.assertEqual(cyber['n'], 7)  # NET dual-leafed within cyber, counted once
        members = [t['ticker'] for lf in cyber['leaves'] for t in lf['tickers']]
        self.assertEqual(members.count('NET'), 2)  # still shown in both leaf tables

    def test_network_stays_leaf_level_for_rendered_families_only(self):
        nodes = {(n['id'], n['kind']) for n in self.snap['network']['nodes']}
        self.assertIn(('Cybersecurity', 'l1'), nodes)
        self.assertIn(('Cybersecurity / Endpoint', 'leaf'), nodes)
        self.assertIn(('AI / Data Center / Chips & Processors', 'leaf'), nodes)
        self.assertNotIn(('Space / Launch', 'leaf'), nodes)
        edges = {(e['source'], e['target']) for e in self.snap['network']['edges']}
        self.assertIn(('Cybersecurity / Endpoint', 'Cybersecurity'), edges)


if __name__ == '__main__':
    unittest.main()
