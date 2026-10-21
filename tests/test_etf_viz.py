"""The ETF Viz tab: zone inputs from the ETF fetch, and the snapshot builder."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.indicators.create_technical_indicators import compute_weekly_ema
from src.reporting import export_dashboard_data as ex


def _etf_frame(closes, volume=None):
    """An OHLC frame in the capitalized shape yfinance returns."""
    index = pd.date_range('2026-01-01', periods=len(closes), freq='D')
    frame = pd.DataFrame({
        'Open': [c - 0.9 for c in closes],
        'High': [c + 0.5 for c in closes],
        'Low': [c - 0.5 for c in closes],
        'Close': list(closes),
    }, index=index)
    if volume is not None:
        frame['Volume'] = float(volume)
    return frame


def _etf_metrics(frame, spy=None):
    baseline = spy if spy is not None else pd.Series(dtype=float)
    with patch('yfinance.download', return_value=frame):
        with contextlib.redirect_stdout(io.StringIO()):
            return ex.fetch_etf_metrics(['TQQQ'], baseline)


class EtfZoneInputTests(unittest.TestCase):
    def test_a_long_series_carries_all_zone_inputs(self):
        frame = _etf_frame([100.0 + i * 0.1 for i in range(400)], volume=1_000_000)
        z = _etf_metrics(frame)['TQQQ']['zone']
        for key in ('close', 'ema20', 'ema20_w', 'atr14', 'dollar_vol', 'adr_pct'):
            self.assertIsNotNone(z[key], key)

    def test_dollar_vol_is_the_20_session_mean_of_close_times_volume(self):
        closes = [100.0] * 380 + [200.0] * 20
        z = _etf_metrics(_etf_frame(closes, volume=1_000))['TQQQ']['zone']
        self.assertAlmostEqual(200_000.0, z['dollar_vol'])

    def test_a_frame_without_volume_has_no_dollar_vol(self):
        z = _etf_metrics(_etf_frame([100.0 + i for i in range(400)]))['TQQQ']['zone']
        self.assertIsNone(z['dollar_vol'])

    def test_fewer_than_20_weeks_has_no_weekly_ema(self):
        # 120 calendar days is about 17 completed weeks.
        z = _etf_metrics(_etf_frame([100.0 + i for i in range(120)]))['TQQQ']['zone']
        self.assertIsNone(z['ema20_w'])

    def test_the_weekly_ema_matches_the_pipeline_helper(self):
        frame = _etf_frame([100.0 + (i % 30) for i in range(400)], volume=1_000)
        want = compute_weekly_ema(frame['Close'], 20, 20).iloc[-1]
        self.assertAlmostEqual(want, _etf_metrics(frame)['TQQQ']['zone']['ema20_w'])

    def test_the_fetch_asks_for_two_years(self):
        frame = _etf_frame([100.0 + i for i in range(60)])
        with patch('yfinance.download', return_value=frame) as download:
            with contextlib.redirect_stdout(io.StringIO()):
                ex.fetch_etf_metrics(['TQQQ'], pd.Series(dtype=float))
        self.assertEqual('2y', download.call_args.kwargs['period'])


DATE = '2026-10-20'
TICKER_THEMES = {'NVDA': ['AI / Data Center / Chips & Processors']}
THEME_MAP = {
    'NVDL': {'side': 'long', 'underlying': 'NVDA'},
    'TQQQ': {'side': 'long', 'themes': []},
    'SQQQ': {'side': 'short'},
    'CORD': {'side': 'exclude'},
}
VARS_IN = 3.456


def _zone(dollar_vol=30e6, adr_pct=0.05):
    # close 100, EMA20 99, weekly EMA20 95, ATR14 2: daily +0.5, weekly +2.5,
    # inside the buy area.
    return {'close': 100.0, 'ema20': 99.0, 'ema20_w': 95.0, 'atr14': 2.0,
            'dollar_vol': dollar_vol, 'adr_pct': adr_pct}


def _metrics(tickers, **zone_kw):
    return {t: {'vars': VARS_IN, 'color': None, 'highlight': 'ma_up', 'zone': _zone(**zone_kw)}
            for t in tickers}


def _snap(tickers=('NVDL', 'TQQQ', 'SQQQ', 'CORD'), metrics=None, **zone_kw):
    rows = [{'ticker': t, 'rs_sts': 80.0} for t in tickers]
    metrics = _metrics(tickers, **zone_kw) if metrics is None else metrics
    with contextlib.redirect_stdout(io.StringIO()):
        return ex.build_etf_viz_snapshot(rows, metrics, THEME_MAP, TICKER_THEMES, DATE)


def _tickers(snap):
    return {t['ticker']: t for t in snap['zone_map']['tickers']}


class EtfVizSnapshotTests(unittest.TestCase):
    def test_a_long_etf_plots_with_its_underlying_theme(self):
        t = _tickers(_snap())['NVDL']
        self.assertEqual('buy_area', t['state'])
        self.assertEqual(['AI / Data Center / Chips & Processors'], t['leaves'])

    def test_a_long_etf_with_no_theme_still_plots(self):
        self.assertEqual([], _tickers(_snap())['TQQQ']['leaves'])

    def test_short_and_excluded_etfs_stay_off_and_are_counted(self):
        snap = _snap()
        self.assertNotIn('SQQQ', _tickers(snap))
        self.assertNotIn('CORD', _tickers(snap))
        self.assertEqual(1, snap['zone_map']['n_short'])
        self.assertEqual(1, snap['zone_map']['n_excluded'])

    def test_an_unknown_etf_is_listed_not_plotted(self):
        snap = _snap(tickers=('NVDL', 'NEWX'))
        self.assertNotIn('NEWX', _tickers(snap))
        self.assertEqual(['NEWX'], snap['zone_map']['unmapped'])

    def test_the_etf_floor_is_20m_and_is_published(self):
        self.assertNotIn('NVDL', _tickers(_snap(dollar_vol=15e6)))
        snap = _snap(dollar_vol=25e6)
        self.assertIn('NVDL', _tickers(snap))
        self.assertEqual(20_000_000, snap['zone_map']['params']['min_avg_dollar_vol'])

    def test_the_adr_floor_is_the_shared_one(self):
        snap = _snap(tickers=('NVDL',), adr_pct=0.035)
        self.assertEqual([], snap['zone_map']['tickers'])
        self.assertEqual(1, snap['zone_map']['n_floor_dropped'])

    def test_the_row_number_is_vars_and_rs_reads_rs_sts(self):
        t = _tickers(_snap())['NVDL']
        self.assertEqual(round(VARS_IN, 2), t['vars'])
        self.assertEqual(80.0, t['rs'])
        self.assertEqual('ma_up', t['highlight'])

    def test_a_missing_weekly_ema_is_counted_not_plotted(self):
        metrics = _metrics(('NVDL',))
        metrics['NVDL']['zone']['ema20_w'] = None
        snap = _snap(tickers=('NVDL',), metrics=metrics)
        self.assertEqual(1, snap['zone_map']['n_no_weekly'])

    def test_the_payload_carries_its_date(self):
        self.assertEqual(DATE, _snap()['report_date'])

    def test_no_bars_builds_nothing(self):
        self.assertIsNone(_snap(metrics={}))

    def test_bars_for_other_etfs_only_build_nothing(self):
        # One fetch serves the Lev and the Industry tabs. Industry bars alone
        # must not stand in for a failed Lev fetch.
        self.assertIsNone(_snap(tickers=('NVDL', 'TQQQ'), metrics=_metrics(('SMH', 'XBI'))))

    def test_a_long_etf_with_no_bars_is_counted_apart_from_the_floor(self):
        snap = _snap(tickers=('NVDL', 'TQQQ'), metrics=_metrics(('NVDL',)))
        self.assertNotIn('TQQQ', _tickers(snap))
        self.assertEqual(1, snap['zone_map']['n_no_bars'])
        self.assertEqual(0, snap['zone_map']['n_floor_dropped'])


class ExportEtfVizTests(unittest.TestCase):
    def _run(self, metrics, out, **kw):
        rows = [{'ticker': t, 'rs_sts': 80.0} for t in ('NVDL', 'TQQQ')]
        with patch.object(ex, 'OUTPUT_DIR', out),                 patch.object(ex, 'load_etf_theme_map', return_value=THEME_MAP),                 patch('src.themes.theme_registry.load_ticker_themes', return_value=TICKER_THEMES),                 contextlib.redirect_stdout(io.StringIO()):
            ex.export_etf_viz(rows, metrics, DATE, **kw)

    def test_a_good_snapshot_writes_both_files(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            self._run(_metrics(('NVDL', 'TQQQ')), out)
            self.assertEqual(DATE, json.loads((out / 'etf_viz.json').read_text())['report_date'])
            self.assertEqual([DATE], [h['report_date'] for h in
                                      json.loads((out / 'etf_viz_history.json').read_text())])

    def test_no_bars_leaves_the_last_snapshot_alone(self):
        with tempfile.TemporaryDirectory() as d:
            out = Path(d)
            (out / 'etf_viz.json').write_text('{"report_date": "2026-10-19"}')
            self._run({}, out)
            self.assertEqual('2026-10-19', json.loads((out / 'etf_viz.json').read_text())['report_date'])
            self.assertFalse((out / 'etf_viz_history.json').exists())

    def test_an_error_is_logged_not_raised(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.object(ex, 'build_etf_viz_snapshot', side_effect=ValueError('bad yaml')):
                self._run(_metrics(('NVDL',)), Path(d))   # must not raise
            self.assertFalse((Path(d) / 'etf_viz.json').exists())


if __name__ == '__main__':
    unittest.main()
