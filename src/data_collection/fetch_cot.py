"""
Fetch CFTC Commitments of Traders (COT) positioning for the dashboard COT tab.

Source: the CFTC public reporting API, legacy report, futures only. One request
returns every configured market for the plotted window. The CFTC publishes on
Friday afternoon for the Tuesday before, so most daily runs rewrite the same
weeks; the tab prints the report's age so a stopped fetch cannot pass as fresh.

Each market publishes three net positions (long minus short): non-commercial
(large speculators), commercial, and non-reportable (small traders). Spread
positions cancel, so the three nets add to zero.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.settings import CONFIG, DOCS_DATA_DIR  # noqa: E402

OUTPUT_PATH = DOCS_DATA_DIR / "cot.json"
ROW_LIMIT = 50_000
# A previous report further back than this is a gap, not last week. The CFTC
# skips weeks for thin contracts (the sector E-minis report about half of them),
# and a change measured across a gap would read as one week's move.
MAX_WEEK_GAP_DAYS = 10

FIELDS = {
    "date": "report_date_as_yyyy_mm_dd",
    "oi": "open_interest_all",
    "nc_long": "noncomm_positions_long_all",
    "nc_short": "noncomm_positions_short_all",
    "cm_long": "comm_positions_long_all",
    "cm_short": "comm_positions_short_all",
    "sm_long": "nonrept_positions_long_all",
    "sm_short": "nonrept_positions_short_all",
}


def fetch_rows(url: str, codes: list[str], since: date) -> list[dict]:
    """Every report row for `codes` on or after `since`, oldest first."""
    quoted = ",".join(f"'{c}'" for c in codes)
    params = {
        "$select": ",".join(["cftc_contract_market_code", *FIELDS.values()]),
        "$where": (f"cftc_contract_market_code in({quoted}) AND "
                   f"report_date_as_yyyy_mm_dd >= '{since.isoformat()}'"),
        "$order": "report_date_as_yyyy_mm_dd",
        "$limit": str(ROW_LIMIT),
    }
    with urllib.request.urlopen(f"{url}?{urllib.parse.urlencode(params)}", timeout=60) as resp:
        rows = json.loads(resp.read().decode("utf-8"))
    if len(rows) >= ROW_LIMIT:
        raise RuntimeError(f"CFTC response hit the {ROW_LIMIT}-row limit; it is truncated")
    return rows


def parse_rows(rows: list[dict]) -> dict[str, list[dict]]:
    """Group raw API rows by contract code as `{date, oi, nc, cm, sm}` weeks."""
    by_code: dict[str, dict[str, dict]] = {}
    for raw in rows:
        values = {k: raw.get(v) for k, v in FIELDS.items()}
        if any(v is None for v in values.values()):
            continue
        oi = int(values["oi"])
        if oi <= 0:
            continue
        week = {
            "date": values["date"][:10],
            "oi": oi,
            "nc": int(values["nc_long"]) - int(values["nc_short"]),
            "cm": int(values["cm_long"]) - int(values["cm_short"]),
            "sm": int(values["sm_long"]) - int(values["sm_short"]),
        }
        # Keyed by date so a duplicated row cannot plot one week twice.
        by_code.setdefault(raw["cftc_contract_market_code"], {})[week["date"]] = week
    return {code: [weeks[d] for d in sorted(weeks)] for code, weeks in by_code.items()}


def percentile_rank(values: list[float], current: float) -> float:
    """Share of `values` at or below `current`, 0-100."""
    return 100.0 * sum(v <= current for v in values) / len(values)


def week_change(weeks: list[dict], key: str) -> int | None:
    """Change from the previous report, or None when that report is a gap away."""
    if len(weeks) < 2:
        return None
    gap = date.fromisoformat(weeks[-1]["date"]) - date.fromisoformat(weeks[-2]["date"])
    if gap.days > MAX_WEEK_GAP_DAYS:
        return None
    return weeks[-1][key] - weeks[-2][key]


def crowding_signal(weeks: list[dict], *, min_weeks: int, extreme_pct: float) -> dict:
    """Crowding score and tags for the newest week of `weeks` (the plotted window).

    The score ranks the non-commercial net as a share of open interest inside
    the window, so a market whose open interest doubled is not called crowded
    on contract count alone. Checked against a third-party COT screen on the
    Nasdaq E-mini, 2026-10-27: this reads 96.2, the screen printed 96.
    """
    nc_change = week_change(weeks, "nc")
    if len(weeks) < min_weeks:
        return {"score": None, "crowd": None, "state": None, "commercials": None,
                "summary": "Too few reported weeks to score positioning."}

    nc_share = [w["nc"] / w["oi"] for w in weeks]
    cm_share = [w["cm"] / w["oi"] for w in weeks]
    score = percentile_rank(nc_share, nc_share[-1])
    cm_score = percentile_rank(cm_share, cm_share[-1])

    crowd = None
    if score >= 100 - extreme_pct:
        crowd = "long"
    elif score <= extreme_pct:
        crowd = "short"

    state = None
    if crowd and nc_change is not None:
        against = nc_change < 0 if crowd == "long" else nc_change > 0
        state = "fade" if against else "extended"

    commercials = None
    if cm_score <= extreme_pct:
        commercials = "bearish"
    elif cm_score >= 100 - extreme_pct:
        commercials = "bullish"

    if state == "fade":
        summary = "Speculative positioning is fading, creating crowd unwind risk."
    elif state == "extended":
        summary = "Speculative positioning is still building at an extreme."
    elif crowd:
        summary = "Speculative positioning is at an extreme."
    else:
        summary = "Speculative positioning is not at an extreme."
    if commercials:
        summary += f" Commercials are {commercials} relative to speculators."

    return {"score": round(score), "crowd": crowd, "state": state,
            "commercials": commercials, "summary": summary}


def build_payload(weeks_by_code: dict[str, list[dict]], cfg: dict) -> dict:
    """The cot.json payload: configured markets in dropdown order, one window."""
    if not weeks_by_code:
        raise RuntimeError("CFTC returned no rows for any configured market")
    newest = max(w[-1]["date"] for w in weeks_by_code.values() if w)
    window_start = (date.fromisoformat(newest)
                    - timedelta(weeks=cfg["window_weeks"] - 1)).isoformat()

    markets = []
    for market in cfg["markets"]:
        code = str(market["code"])
        weeks = [w for w in weeks_by_code.get(code, []) if w["date"] >= window_start]
        if not weeks:
            print(f"  no CFTC rows for {market['name']} ({code}); left out of the dropdown")
            continue
        markets.append({
            "code": code,
            "name": market["name"],
            "dates": [w["date"] for w in weeks],
            "oi": [w["oi"] for w in weeks],
            "nc": [w["nc"] for w in weeks],
            "cm": [w["cm"] for w in weeks],
            "sm": [w["sm"] for w in weeks],
            "signal": crowding_signal(weeks, min_weeks=cfg["min_weeks_for_score"],
                                      extreme_pct=cfg["extreme_pct"]),
        })
    if not markets:
        raise RuntimeError("no configured market has a report inside the window")

    return {
        "updated": datetime.now().isoformat(timespec="seconds"),
        "report_date": newest,
        "window_start": window_start,
        "default_market": str(cfg["default_market"]),
        "markets": markets,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch CFTC COT positioning for the COT tab")
    parser.add_argument("--out", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    cfg = CONFIG["cot"]
    codes = [str(m["code"]) for m in cfg["markets"]]
    # Two spare weeks: the newest report trails today by up to ten days, and
    # build_payload trims to the window anchored on that report.
    since = date.today() - timedelta(weeks=cfg["window_weeks"] + 2)
    rows = fetch_rows(cfg["url"], codes, since)
    payload = build_payload(parse_rows(rows), cfg)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(f"COT {payload['report_date']}: {len(payload['markets'])} markets -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
