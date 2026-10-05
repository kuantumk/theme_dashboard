"""Day-pattern green flags come from each session's own bar.

The flags used to be read once from the newest bar of `price_daily_ta.pkl`
and stamped onto every session in the 180-day time-travel window, so a
back-dated session showed today's colouring. Measured on the published
`vars_history.json` before the fix: of 983 tickers present in two or more of
126 sessions, 0 changed colour between sessions, and 170 were green in every
session they appeared in. A one-day candle pattern cannot do that.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data
from src.reporting.export_dashboard_data import (
    day_flags_from_master,
    day_pattern_green,
    export_radar,
    load_day_flags_by_date,
)

OLD, NEW = '2026-07-10', '2026-07-13'

# Two small leaves, so every member survives the history chip cap.
THEMES = {
    **{f'NW{i}': ['Cybersecurity / Network'] for i in range(4)},
    **{f'ID{i}': ['Cybersecurity / Identity'] for i in range(4)},
}

# NW0 is green on the old session only; ID0 on the newest only.
GREEN_ON = {OLD: {'NW0'}, NEW: {'ID0'}}


def _master_rows(date_str):
    rows = []
    for i, ticker in enumerate(sorted(THEMES)):
        green = ticker in GREEN_ON[date_str]
        rows.append({
            'date': date_str,
            'ticker': ticker,
            'close': 50.0,
            'avg_dollar_vol': 50_000_000.0,
            'rs_sts_pct': 40.0 + i * 5,
            'vars': float(i),
            'rela_perf_1mo_rank': 40 + i * 5,
            'tightness': 0.5,
            'tight_range': False,
            'tight_day': green,
            'inside_day': False,
            'close_to_ma': green,
        })
    return pd.DataFrame(rows)


def _write_masters(root):
    for ds in (OLD, NEW):
        su.save_df_to_parquet(_master_rows(ds), root / 'master' / f'master_{ds}.parquet')


def _old_rule(last):
    """The deleted `load_ticker_color_flags` test, verbatim, as the reference."""
    tight_or_inside = bool(last.get('tight_day', False)) or bool(last.get('inside_day', False))
    return tight_or_inside and bool(last.get('close_to_ma', False))


class DayPatternRuleTests(unittest.TestCase):
    def test_rule_truth_table(self):
        for tight in (True, False):
            for inside in (True, False):
                for near in (True, False):
                    bar = {'tight_day': tight, 'inside_day': inside, 'close_to_ma': near}
                    self.assertEqual(day_pattern_green(bar), (tight or inside) and near, bar)

    def test_nan_and_missing_never_fire(self):
        # bool(float('nan')) is True, so a bare truth test would paint a
        # ticker green on an absent reading.
        nan = float('nan')
        self.assertFalse(day_pattern_green({'tight_day': nan, 'close_to_ma': True}))
        self.assertFalse(day_pattern_green({'tight_day': True, 'close_to_ma': nan}))
        self.assertFalse(day_pattern_green({'tight_day': None, 'inside_day': pd.NA,
                                            'close_to_ma': True}))
        self.assertFalse(day_pattern_green({}))

    def test_newest_session_matches_the_old_newest_bar_rule(self):
        """The master row for the newest session IS the ticker's last bar
        (`create_master_table` takes `df[:run_date].tail(1)` and run_date is the
        last date), so the new path must agree with the old one on that bar."""
        bars = []
        for tight in (True, False):
            for inside in (True, False):
                for near in (True, False):
                    bars.append({'ticker': f'T{len(bars)}', 'tight_day': tight,
                                 'inside_day': inside, 'close_to_ma': near})
        master = pd.DataFrame(bars)
        expected = {b['ticker']: 'green' for b in bars if _old_rule(b)}
        self.assertEqual(day_flags_from_master(master), expected)
        self.assertTrue(expected)


class PerSessionFlagTests(unittest.TestCase):
    def test_each_date_reads_its_own_bar(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_masters(root)
            by_date = load_day_flags_by_date(root)
        self.assertEqual(by_date, {OLD: {'NW0': 'green'}, NEW: {'ID0': 'green'}})

    def test_master_without_the_columns_flags_nothing(self):
        # A parquet older than the indicators fails closed, not open.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            su.save_df_to_parquet(
                pd.DataFrame({'date': [NEW], 'ticker': ['AAA'], 'close': [1.0]}),
                root / 'master' / f'master_{NEW}.parquet')
            self.assertEqual(load_day_flags_by_date(root), {NEW: {}})

    def test_back_dated_radar_session_uses_its_own_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, out_dir = Path(tmp) / 'screening', Path(tmp) / 'docs'
            out_dir.mkdir()
            _write_masters(root)
            with patch('src.themes.l1_score.load_ticker_themes', return_value=THEMES):
                export_radar(load_day_flags_by_date(root), root=root, out_dir=out_dir)
            history = json.loads((out_dir / 'radar_history.json').read_text())

        green = {
            snap['report_date']: {
                td['ticker'] for l1 in snap['l1s'] for lf in l1['leaves']
                for td in lf['tickers'] if td.get('ticker_color') == 'green'
            }
            for snap in history
        }
        self.assertEqual(green, {NEW: {'ID0'}, OLD: {'NW0'}})

    def test_exporter_hands_each_builder_its_own_session(self):
        """`export_vars` stands for the three screener-backed exporters: each
        looks its session's flags up by the date in the parquet's file name."""
        with tempfile.TemporaryDirectory() as tmp:
            screening = Path(tmp) / 'screening_output'
            (screening / 'vars').mkdir(parents=True)
            for ds in (OLD, NEW):
                (screening / 'vars' / f'vars_{ds}.parquet').write_text('x')
            seen = {}

            def fake_build(csv_file, day_flags, newest_session=False):
                ds = csv_file.stem.replace('vars_', '')
                seen[ds] = day_flags
                return {'report_date': ds, 'themes': []}

            by_date = {OLD: {'NW0': 'green'}, NEW: {'ID0': 'green'}}
            with (
                patch.object(export_dashboard_data, 'SCREENING_OUTPUT_DIR', screening),
                patch.object(export_dashboard_data, 'OUTPUT_DIR', Path(tmp)),
                patch.object(export_dashboard_data, 'VARS_ARTIFACT_DIR', Path(tmp) / 'a'),
                patch.object(export_dashboard_data, '_build_vars_snapshot', fake_build),
            ):
                export_dashboard_data.export_vars(by_date)

        self.assertEqual(seen, by_date)


if __name__ == '__main__':
    unittest.main()
