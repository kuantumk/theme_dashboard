"""The ETF Viz tab: zone inputs from the ETF fetch, and the snapshot builder."""

import contextlib
import io
import unittest
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


if __name__ == '__main__':
    unittest.main()
