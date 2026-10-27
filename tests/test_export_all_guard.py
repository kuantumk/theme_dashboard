"""`export_all` isolates each step, then still fails loudly.

One tab raising must not blank every other tab, the report meta or the
prune — but the run must still end in an exception so CI goes red.
"""

import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from src.reporting import export_dashboard_data as ex


class ExportAllGuardTests(unittest.TestCase):
    def test_one_failing_step_does_not_stop_the_rest_and_the_run_still_fails(self):
        calls = []

        def ok(name):
            def fn(*args, **kwargs):
                calls.append(name)
            return fn

        def boom(*args, **kwargs):
            calls.append('vars')
            raise ValueError('broken vars parquet')

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            import src.screening.prune_screening_output as prune_mod
            with (
                patch.object(ex, 'OUTPUT_DIR', out),
                patch.object(ex, 'latest_report_date', lambda: '2026-10-01'),
                patch.object(ex, 'load_day_flags_by_date', lambda: {}),
                patch.object(ex, '_load_fundamentals_for_tickers', lambda *a: {}),
                patch.object(ex, 'export_momentum_136', ok('momentum_136')),
                patch.object(ex, 'export_volume', ok('volume')),
                patch.object(ex, 'export_vars', boom),
                patch.object(ex, 'export_parabolic', ok('parabolic')),
                patch.object(ex, 'export_radar', ok('radar')),
                patch.object(ex, 'export_si', ok('si')),
                patch.object(ex, 'update_breadth_history', ok('breadth')),
                patch.object(ex, 'export_etf_tabs', ok('etf')),
                patch.object(ex, 'export_macro_data', ok('macro')),
                patch.object(ex, 'export_macro_events', ok('events')),
                patch.object(prune_mod, 'prune_screening_output', ok('prune')),
                redirect_stdout(io.StringIO()),
                redirect_stderr(io.StringIO()) as err,
            ):
                with self.assertRaisesRegex(RuntimeError, 'vars'):
                    ex.export_all()
            meta_written = (out / 'report_meta.json').exists()

        self.assertEqual(calls, ['momentum_136', 'volume', 'vars', 'parabolic', 'radar', 'si',
                                 'breadth', 'etf', 'macro', 'events', 'prune'])
        self.assertTrue(meta_written)
        self.assertIn('broken vars parquet', err.getvalue())  # the traceback is printed


class FundamentalsLookupTests(unittest.TestCase):
    def test_a_missing_database_reads_as_empty(self):
        with patch.object(ex, 'FUNDAMENTALS_DB', Path('/nonexistent/fundamentals.db')):
            self.assertEqual(ex._load_fundamentals_for_tickers(), {})
            self.assertEqual(ex._load_fundamentals_for_tickers(['AAA']), {})

    def test_a_malformed_database_warns_instead_of_raising(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / 'fundamentals.db'
            db.write_bytes(b'not a sqlite file at all' * 100)
            with patch.object(ex, 'FUNDAMENTALS_DB', db), redirect_stdout(io.StringIO()) as out:
                self.assertEqual(ex._load_fundamentals_for_tickers(), {})
        self.assertIn('fundamentals lookup failed', out.getvalue())

    def test_reads_every_payload_column_keyed_by_upper_ticker(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / 'fundamentals.db'
            conn = sqlite3.connect(db)
            conn.execute('CREATE TABLE fundamentals (ticker TEXT, shares_float REAL, '
                         'eps_growth_yoy REAL, sales_growth_yoy REAL, short_interest REAL, '
                         'inst_transactions REAL)')
            conn.executemany('INSERT INTO fundamentals VALUES (?, ?, ?, ?, ?, ?)', [
                ('AAA', 1e7, 15.0, 20.0, 25.0, 1.5),
                ('BBB', None, None, None, 3.0, None),
            ])
            conn.commit()
            conn.close()
            with patch.object(ex, 'FUNDAMENTALS_DB', db):
                everything = ex._load_fundamentals_for_tickers()
                some = ex._load_fundamentals_for_tickers(['aaa'])
        self.assertEqual(everything['AAA'], {
            'shares_float': 1e7, 'eps_growth_yoy': 15.0, 'sales_growth_yoy': 20.0,
            'short_interest': 25.0, 'inst_transactions': 1.5,
        })
        self.assertEqual(set(everything), {'AAA', 'BBB'})
        self.assertEqual(set(some), {'AAA'})


if __name__ == '__main__':
    unittest.main()
