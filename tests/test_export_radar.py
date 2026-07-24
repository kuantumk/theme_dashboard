import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from config.settings import CONFIG
from src.reporting.export_dashboard_data import export_radar

THEMES = {
    **{f'CY{i:02d}': ['Cybersecurity / Network'] for i in range(12)},
    'IDA': ['Cybersecurity / Identity'],
    'IDB': ['Cybersecurity / Identity'],
    'BIA': ['Biotech / Immunology'],
    'BIB': ['Biotech / Immunology'],
}


def _master_rows(date_str):
    rows = []
    for i, ticker in enumerate(sorted(THEMES)):
        rows.append({
            'date': date_str,
            'ticker': ticker,
            'close': 50.0,
            'avg_dollar_vol': 50_000_000.0,
            'rs_sts_pct': 30.0 + i * 4,
            'vars': float(i),
            'rela_perf_1mo_rank': 30 + i * 4,
        })
    return pd.DataFrame(rows)


class ExportRadarTests(unittest.TestCase):
    def _write_inputs(self, root: Path, dates):
        for ds in dates:
            su.save_df_to_parquet(_master_rows(ds), root / 'master' / f'master_{ds}.parquet')
            screener = CONFIG['screeners'][0]
            su.save_df_to_parquet(
                pd.DataFrame({'ticker': ['CY00', 'IDA']}),
                root / screener / f'{screener}_{ds}.parquet',
            )

    def test_exports_current_and_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, out_dir = Path(tmp) / 'screening', Path(tmp) / 'docs'
            out_dir.mkdir()
            dates = ['2026-07-10', '2026-07-13']
            self._write_inputs(root, dates)
            # A session whose master is empty must be skipped, not crash
            su.save_df_to_parquet(
                pd.DataFrame({'date': [], 'ticker': []}),
                root / 'master' / 'master_2026-07-09.parquet',
            )

            with patch('src.themes.l1_score.load_ticker_themes', return_value=THEMES):
                current = export_radar({'CY11': 'green'}, root=root, out_dir=out_dir)

            self.assertIsNotNone(current)
            radar = json.loads((out_dir / 'radar.json').read_text())
            history = json.loads((out_dir / 'radar_history.json').read_text())

            self.assertEqual(radar['report_date'], '2026-07-13')
            self.assertEqual([h['report_date'] for h in history],
                             ['2026-07-13', '2026-07-10'])

            self.assertEqual([e['rank'] for e in radar['l1s']],
                             list(range(1, len(radar['l1s']) + 1)))
            cyber = next(e for e in radar['l1s'] if e['name'] == 'Cybersecurity')
            self.assertEqual(cyber['n_leaves'], 2)
            self.assertEqual(cyber['n_screened'], 2)

            network = next(lf for lf in cyber['leaves']
                           if lf['name'] == 'Cybersecurity / Network')
            # radar.json ships every scored member so the Themes tab can expand
            # a leaf to its full roster...
            self.assertEqual(network['n'], 12)
            self.assertEqual(len(network['tickers']), 12)
            self.assertIn('global_rank', network)
            self.assertIn('raw', network)
            self.assertIn('boosted', network)

            # ...while the same session in history stays capped at
            # radar.tickers_per_leaf, which bounds radar_history.json.
            cap = int(CONFIG.get('radar', {}).get('tickers_per_leaf', 10))
            hist_cyber = next(e for e in history[0]['l1s']
                              if e['name'] == 'Cybersecurity')
            hist_network = next(lf for lf in hist_cyber['leaves']
                                if lf['name'] == 'Cybersecurity / Network')
            self.assertEqual(hist_network['n'], 12)
            self.assertEqual(len(hist_network['tickers']), cap)
            self.assertEqual([t['ticker'] for t in hist_network['tickers']],
                             [t['ticker'] for t in network['tickers']][:cap])

            for l1_entry in radar['l1s']:
                for leaf in l1_entry['leaves']:
                    for td in leaf['tickers']:
                        self.assertEqual(td['is_screened'],
                                         td['ticker'] in {'CY00', 'IDA'})

            flagged = [td for l1_entry in radar['l1s'] for lf in l1_entry['leaves']
                       for td in lf['tickers'] if td.get('ticker_color') == 'green']
            self.assertEqual({td['ticker'] for td in flagged}, {'CY11'})

    def test_no_masters_is_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, out_dir = Path(tmp) / 'screening', Path(tmp) / 'docs'
            out_dir.mkdir()
            self.assertIsNone(export_radar({}, root=root, out_dir=out_dir))
            self.assertFalse((out_dir / 'radar.json').exists())


if __name__ == '__main__':
    unittest.main()
