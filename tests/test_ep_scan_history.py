import json
from datetime import date, timedelta

from src.reporting.ep_scan_common import SCAN_HISTORY_MAX, update_scan_history


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


def test_update_scan_history_deduplicates_and_keeps_last_60_sessions(tmp_path):
    start = date(2026, 1, 1)
    for offset in range(SCAN_HISTORY_MAX + 2):
        scan_date = (start + timedelta(days=offset)).isoformat()
        update_scan_history(_snapshot(scan_date), "ep_scan_morning.json", out_dir=tmp_path)

    update_scan_history(
        _snapshot((start + timedelta(days=SCAN_HISTORY_MAX + 1)).isoformat(), count=1),
        "ep_scan_morning.json",
        out_dir=tmp_path,
    )

    history = json.loads(
        (tmp_path / "ep_scan_morning_history.json").read_text(encoding="utf-8")
    )
    dates = [item["report_date"] for item in history]

    assert len(history) == SCAN_HISTORY_MAX
    assert dates == sorted(dates, reverse=True)
    assert dates[0] == (start + timedelta(days=SCAN_HISTORY_MAX + 1)).isoformat()
    assert dates[-1] == (start + timedelta(days=2)).isoformat()
    assert history[0]["count"] == 1
