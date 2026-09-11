"""Fetch heavily shorted tickers from the Finviz Ownership screener.

Feeds the dashboard's SI tab. Writes ``data/short_interest.json``.

The Ownership view (``v=131``) carries ``Short Float`` in the screener table
itself, alongside float, market cap and institutional transactions. So one
screener call answers the whole question. Reading the same figures from
per-ticker quote pages, the way ``fetch_fundamental_data`` does, would cost
231 requests at the rate limit and add nothing.

⛔ The ticker column arrives corrupted and must be repaired. Finviz's
2026-07-15 redesign put a logo avatar in the ticker cell whose one-letter
fallback span sits before the symbol, so ``OKLO`` parses as ``OOKLO``.
Measured 2026-09-10 on this exact screener: **231 of 231 rows** were wrong.
Unrepaired, every master-table join misses and the tab publishes nothing,
which is indistinguishable from a market holding no shorted names. Do not
un-double the first character — see CLAUDE.md.

⛔ ``Current Volume`` makes this screen time-of-day dependent. Before the open
no ticker has 750K of current volume, so the call returns an empty table. The
daily workflow runs at 1:30 PM Pacific, after the close, where current volume
is the full session. Keep this step inside that workflow.
"""

import argparse
import json
import logging
import sys
from datetime import date
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from config.settings import SCREENING_OUTPUT_DIR, SHORT_INTEREST_FILE
from src.reporting.ep_scan_common import make_ticker_repair_view

logger = logging.getLogger(__name__)

try:
    from finvizfinance.screener.ownership import Ownership
    FINVIZ_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the dependency
    FINVIZ_AVAILABLE = False

# The five legs of the source screener URL:
#   f=sh_avgvol_o1000,sh_curvol_o750,sh_price_o10,sh_short_o10,ta_volatility_mo4
SI_FILTERS = {
    "Average Volume": "Over 1M",
    "Current Volume": "Over 750K",
    "Price": "Over $10",
    "Float Short": "Over 10%",
    "Volatility": "Month - Over 4%",
}

# `o=-shortinterestshare` in the URL. "Float Short" is the COLUMN name and is
# not a valid sort key — finvizfinance raises ValueError listing the real ones,
# which would abort the step.
SI_ORDER = "Short Interest Share"

# Screener columns -> payload keys. Anything absent becomes None rather than
# failing the whole fetch: a view gaining or losing a column must not blank
# the tab.
_COLUMNS = {
    "float_shares": "Float",
    "market_cap": "Market Cap",
    "inst_trans": "Inst Trans",
    "price": "Price",
}


def parse_short_float(value) -> Optional[float]:
    """Parse a Finviz short-float cell (``'70.54%'``) to a float percent.

    Returns None for the placeholder dash, blanks and anything unparseable, so
    a row with no reading is dropped rather than scored as zero.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-" or text.lower() in ("nan", "n/a"):
        return None
    try:
        return float(text.rstrip("%"))
    except ValueError:
        return None


def _optional(row, column) -> Optional[float]:
    if column not in row:
        return None
    value = row[column]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_rows(table: pd.DataFrame) -> List[Dict]:
    """Turn the screener frame into payload rows, sorted by short interest.

    Rows with no parseable short interest are dropped — that is the one column
    this tab cannot do without.
    """
    if table is None or table.empty or "Ticker" not in table.columns:
        return []

    rows = []
    for _, row in table.iterrows():
        si_value = parse_short_float(row.get("Short Float"))
        if si_value is None:
            continue
        entry = {"ticker": str(row["Ticker"]).strip().upper(), "si": si_value}
        for key, column in _COLUMNS.items():
            entry[key] = _optional(row, column)
        rows.append(entry)

    rows.sort(key=lambda r: -r["si"])
    return rows


def _make_view():
    """Build the repair-wrapped Ownership view. Patched in tests."""
    if not FINVIZ_AVAILABLE:
        raise RuntimeError("finvizfinance is not installed")
    return make_ticker_repair_view(Ownership)()


def fetch_short_interest() -> Dict:
    """Run the screener once and return ``{date, filters, rows}``.

    ``date`` is left empty here and stamped by :func:`write_short_interest`,
    so the caller decides which session this belongs to.
    """
    view = _make_view()
    view.set_filter(filters_dict=SI_FILTERS)
    table = view.screener_view(order=SI_ORDER, ascend=False, verbose=0)

    rows = build_rows(table)
    repaired = getattr(view, "tickers_repaired", 0)

    if not rows:
        # A 200 that parses to nothing is an upstream-shape signal, not a quiet
        # market. Conflating the two is what let the NAAIM tile sit stale.
        logger.warning(
            "Finviz returned no rows for the SI screen. Either the market has "
            "no qualifying names, or the Ownership view changed shape. Check "
            "the screener before assuming a quiet day."
        )
    elif not repaired:
        # Every row was corrupted when this was measured. Zero repairs against
        # a populated table means the ticker cell changed again.
        logger.warning(
            "Repaired 0 of %d tickers. Every row needed repair when this was "
            "measured (2026-09-10), so Finviz has likely changed the ticker "
            "cell. Verify the symbols before trusting this run.",
            len(rows),
        )
    else:
        logger.info("SI screen: %d rows, %d tickers repaired", len(rows), repaired)

    return {"date": "", "filters": dict(SI_FILTERS), "rows": rows}


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
    """
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

    out = write_short_interest(payload, latest_session_date(), args.out)
    print(f"   -> {out} ({len(payload['rows'])} tickers, date {payload['date']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
