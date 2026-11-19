"""Fetch heavily shorted tickers from the Finviz Ownership screener.

Feeds the dashboard's SI tab. Writes ``data/short_interest.json``.

The Ownership view (``v=131``) carries short float, float, market cap and
institutional transactions in the screener itself, so one screen answers the
whole question. Reading the same figures from per-ticker quote pages, the way
``fetch_fundamental_data`` does, would cost about 250 requests at the rate
limit and add nothing.

⛔ Read the JSON the page embeds, not its HTML grid. By 2026-11-18 Finviz
served the screener as a React island: the rows are ``<div>`` cells and the
page has no ``<table>`` and no ``pageSelect``. finvizfinance 1.3.0 parses that
page as "No ticker found." The same page carries every row as numbers in
``<script type="application/json" data-ssr-props=...>``, with clean tickers,
so the doubled-first-letter avatar repair that the HTML once needed is gone.

⛔ Finviz answers 403 to an old Chrome User-Agent. finvizfinance's Chrome/81
agent blocked this step from 2026-09-29. Send ``fundamental_data.user_agent``.

⛔ ``Current Volume`` makes this screen time-of-day dependent. Before the open
no ticker has 750K of current volume, so the screen is empty. The daily
workflow runs after the close, where current volume is the full session. Keep
this step inside that workflow.
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import date
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional

import requests

from config.settings import CONFIG, SCREENING_OUTPUT_DIR, SHORT_INTEREST_FILE

logger = logging.getLogger(__name__)

SCREENER_URL = "https://finviz.com/screener/stocks"

# Average volume > 1M, current volume > 750K, price > $10, short float > 10%,
# monthly volatility > 4%. Sorted by short float, highest first.
SI_FILTERS = "sh_avgvol_o1000,sh_curvol_o750,sh_price_o10,sh_short_o10,ta_volatility_mo4"
SI_ORDER = "-shortinterestshare"

# Stops a page that never ends from looping forever. The screen returned 252
# rows on 2026-11-18, 13 pages of 20.
MAX_PAGES = 50

_PROPS_RE = re.compile(
    r'<script type="application/json" data-ssr-props="[^"]*">(.*?)</script>', re.S
)

# Payload key -> (field in the row's ``numbers``, scale). Finviz gives float
# and market cap in millions. Short float and Inst Trans are already percents.
# A missing field becomes None rather than failing the fetch.
_COLUMNS = {
    "float_shares": ("sharesFloat", 1e6),
    "market_cap": ("marketCap", 1e6),
    "inst_trans": ("instTrans", 1.0),
    "price": ("price", 1.0),
}


def parse_screener_page(html: str) -> Dict:
    """Return the ``data`` block (``rows``, ``totalCount``, ``pageSize``).

    Raises ``ValueError`` when the page carries no screener data. That is a
    change in page shape, and it must not read as a quiet market.
    """
    for match in _PROPS_RE.finditer(html):
        try:
            props = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        data = props.get("data") if isinstance(props, dict) else None
        if isinstance(data, dict) and isinstance(data.get("rows"), list):
            return data
    raise ValueError(
        "Finviz screener page has no data-ssr-props rows; the page changed shape"
    )


def _number(numbers: Dict, field: str) -> Optional[float]:
    value = numbers.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value == value else None


def build_rows(raw_rows: List[Dict]) -> List[Dict]:
    """Turn screener rows into payload rows, sorted by short interest.

    Rows with no short float are dropped: that is the one figure this tab
    cannot do without.
    """
    rows = []
    for raw in raw_rows:
        ticker = str((raw.get("text") or {}).get("ticker") or "").strip().upper()
        numbers = raw.get("numbers") or {}
        si_value = _number(numbers, "shortInterestShare")
        if not ticker or si_value is None:
            continue
        entry = {"ticker": ticker, "si": round(si_value, 2)}
        for key, (field, scale) in _COLUMNS.items():
            value = _number(numbers, field)
            entry[key] = None if value is None else value * scale
        rows.append(entry)

    rows.sort(key=lambda r: -r["si"])
    return rows


def _get_page(first_row: int) -> str:
    """Fetch one screener page. ``first_row`` is 1-based, like Finviz's ``r``."""
    response = requests.get(
        SCREENER_URL,
        params={"v": "131", "f": SI_FILTERS, "o": SI_ORDER, "r": str(first_row)},
        headers={"User-Agent": CONFIG["fundamental_data"]["user_agent"]},
        timeout=20,
    )
    response.raise_for_status()
    return response.text


def fetch_short_interest() -> Dict:
    """Read every page of the screen and return ``{date, filters, rows}``.

    ``date`` is left empty here and stamped by :func:`write_short_interest`,
    so the caller decides which session this belongs to.
    """
    pause = CONFIG["fundamental_data"].get("rate_limit_seconds", 2)
    raw_rows: List[Dict] = []
    total = None
    for page in range(MAX_PAGES):
        if page:
            time.sleep(pause)
        data = parse_screener_page(_get_page(len(raw_rows) + 1))
        total = data.get("totalCount")
        raw_rows.extend(data["rows"])
        if not data["rows"] or (isinstance(total, int) and len(raw_rows) >= total):
            break

    if isinstance(total, int) and len(raw_rows) < total:
        logger.warning("SI screen returned %d of %d rows", len(raw_rows), total)

    rows = build_rows(raw_rows)
    if not rows:
        logger.warning(
            "Finviz returned no rows for the SI screen. Either the market has "
            "no qualifying names, or the row fields changed. Check the "
            "screener before assuming a quiet day."
        )
    else:
        logger.info("SI screen: %d rows", len(rows))

    return {"date": "", "filters": SI_FILTERS, "rows": rows}


def latest_session_date() -> str:
    """Date of the newest master parquet, else today.

    Anchoring to the master table keeps the SI snapshot on the same session as
    every other tab, so the time-travel bars agree.
    """
    masters = sorted(glob(str(SCREENING_OUTPUT_DIR / "master" / "master_*.parquet")))
    if masters:
        return Path(masters[-1]).stem.replace("master_", "")
    return date.today().isoformat()


def write_short_interest(payload: Dict, report_date: str, out_file=None) -> Path:
    """Stamp the payload with its session date, in place, and write it.

    The stamp lands on the caller's dict rather than a copy so the caller can
    report the date it just wrote.

    Raises ``ValueError`` on an empty roster and writes nothing. A fresh-dated
    empty file would blank the SI tab with no stale banner; keeping the last
    roster lets its own older date trip ``si_stale`` instead.
    """
    if not payload.get("rows"):
        raise ValueError("SI screen returned no rows; keeping the previous roster")
    out = Path(out_file) if out_file is not None else SHORT_INTEREST_FILE
    out.parent.mkdir(parents=True, exist_ok=True)
    payload["date"] = report_date
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return out


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default=None, help="output path override")
    args = parser.parse_args(argv)

    try:
        payload = fetch_short_interest()
    except Exception as exc:
        logger.error("Short interest fetch failed: %s", exc)
        return 1

    try:
        out = write_short_interest(payload, latest_session_date(), args.out)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1
    print(f"   -> {out} ({len(payload['rows'])} tickers, date {payload['date']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
