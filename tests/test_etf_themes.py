"""The ETF Viz theme map: one side and one theme source per sheet ETF."""

import json
import unittest
from pathlib import Path

from src.reporting import export_dashboard_data as ex
from src.themes.theme_registry import load_ticker_themes
from src.themes.theme_taxonomy import validate_path

ROOT = Path(__file__).resolve().parent.parent


class EtfThemeMapTests(unittest.TestCase):
    def test_every_sheet_etf_has_an_entry(self):
        # docs/data/etf_data.json is the committed copy of the sheet.
        rows = json.loads((ROOT / 'docs' / 'data' / 'etf_data.json').read_text(encoding='utf-8'))
        sheet = {r['ticker'].upper() for r in rows}
        self.assertEqual(set(), sheet - set(ex.load_etf_theme_map()))

    def test_every_side_is_known(self):
        for t, e in ex.load_etf_theme_map().items():
            self.assertIn(e.get('side'), ex.ETF_SIDES, t)

    def test_every_explicit_path_is_in_the_taxonomy(self):
        for t, e in ex.load_etf_theme_map().items():
            for p in e.get('themes') or []:
                self.assertTrue(validate_path(p), f'{t}: {p}')

    def test_every_long_entry_resolves_to_a_theme_list(self):
        tt = load_ticker_themes()
        for t, e in ex.load_etf_theme_map().items():
            if e['side'] == 'long':
                self.assertIsNotNone(ex.etf_leaves(e, tt), t)

    def test_explicit_themes_win_over_the_underlying(self):
        e = {'side': 'long', 'underlying': 'BABA', 'themes': ['Geographic / China']}
        self.assertEqual(['Geographic / China'],
                         ex.etf_leaves(e, {'BABA': ['AI / Software & Analytics']}))

    def test_an_empty_theme_list_is_kept_not_unmapped(self):
        self.assertEqual([], ex.etf_leaves({'side': 'long', 'themes': []}, {}))

    def test_an_untagged_underlying_with_no_themes_is_unmapped(self):
        self.assertIsNone(ex.etf_leaves({'side': 'long', 'underlying': 'ZZZZ'}, {}))

    def test_an_underlying_retag_flows_through(self):
        e = {'side': 'long', 'underlying': 'NVDA'}
        self.assertEqual(['Robotics / Humanoid'],
                         ex.etf_leaves(e, {'NVDA': ['Robotics / Humanoid']}))

    def test_a_missing_file_is_an_empty_map(self):
        self.assertEqual({}, ex.load_etf_theme_map(Path('no/such/file.yaml')))


if __name__ == '__main__':
    unittest.main()
