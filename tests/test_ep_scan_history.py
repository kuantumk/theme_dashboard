import json
from datetime import date, timedelta

from src.reporting.ep_scan_common import SCAN_HISTORY_DAYS, update_scan_history


def _snapshot(report_date, *, count=0):
    return {
        "timestamp": f"{report_date}T12:00:00-04:00",
        "scan_date": report_date,
        "scan_type": "morning",
        "count": count,
        "tickers": [{"ticker": "TEST"}] if count else [],
    }


def test_update_scan_history_creates_and_normalizes_history(tmp_path):
    history_path = update_scan_history(
        _snapshot("2026-05-20", count=1),
        "ep_scan_morning.json",
        out_dir=tmp_path,
    )

    assert history_path == tmp_path / "ep_scan_morning_history.json"
    history = json.loads(history_path.read_text(encoding="utf-8"))
    assert history == [
        {
            "timestamp": "2026-05-20T12:00:00-04:00",
            "scan_date": "2026-05-20",
            "scan_type": "morning",
            "count": 1,
            "tickers": [{"ticker": "TEST"}],
            "report_date": "2026-05-20",
        }
    ]


def test_update_scan_history_drops_sessions_outside_calendar_window(tmp_path):
    newest = date(2026, 6, 1)
    window_start = newest - timedelta(days=SCAN_HISTORY_DAYS)  # inclusive lower bound
    just_outside = window_start - timedelta(days=1)
    way_outside = newest - timedelta(days=400)

    # Insert oldest-first so we exercise sort + prune on every append.
    for scan_date in (way_outside, just_outside, window_start, newest):
        update_scan_history(
            _snapshot(scan_date.isoformat()), "ep_scan_morning.json", out_dir=tmp_path
        )

    history = json.loads(
        (tmp_path / "ep_scan_morning_history.json").read_text(encoding="utf-8")
    )
    dates = [item["report_date"] for item in history]

    assert dates == sorted(dates, reverse=True)
    # The window boundary is inclusive; everything older is pruned.
    assert dates == [newest.isoformat(), window_start.isoformat()]
    assert just_outside.isoformat() not in dates
    assert way_outside.isoformat() not in dates


def test_update_scan_history_deduplicates_by_date(tmp_path):
    update_scan_history(_snapshot("2026-05-20", count=0), "ep_scan_morning.json", out_dir=tmp_path)
    update_scan_history(_snapshot("2026-05-20", count=1), "ep_scan_morning.json", out_dir=tmp_path)

    history = json.loads(
        (tmp_path / "ep_scan_morning_history.json").read_text(encoding="utf-8")
    )

    assert len(history) == 1
    assert history[0]["report_date"] == "2026-05-20"
    assert history[0]["count"] == 1  # later snapshot for the same date wins
