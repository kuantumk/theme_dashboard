"""Repair the ticker column of Finviz screener tables.

Finviz's 2026-07-15 screener redesign put a company-logo avatar inside the
ticker cell. The avatar carries a one-letter fallback <span> that renders when
the logo image is missing, and finvizfinance's flat `col.text` parse
concatenates it with the ticker itself — "OKLO" comes back as "OOKLO". Every
symbol arrives with its first character doubled, so every downstream lookup
misses and the caller exports nothing.

The unmangled symbol is still in the markup twice: as the cell's
data-boxover-ticker attribute, and as the text of its <a class="tab-link">.
We re-read it from there rather than un-doubling the first character, which
would be a guess that silently corrupts if Finviz changes the avatar again.
finvizfinance 1.3.0 (Jan 2026) is the latest release and predates the
redesign, so there is no upstream fix to upgrade to.

This lived in ``src/reporting/ep_scan_common.py`` until the EP scans were
retired; the SI collector (``fetch_short_interest``) is its one caller now.
"""

from typing import List, Optional

import pandas as pd


def _ticker_from_row(row) -> Optional[str]:
    """Read the unmangled ticker out of a Finviz screener <tr>."""
    cell = row.find('td', attrs={'data-boxover-ticker': True})
    if cell is not None:
        symbol = (cell.get('data-boxover-ticker') or '').strip()
        if symbol:
            return symbol

    cells = row.find_all('td')
    if len(cells) < 2:
        return None
    link = cells[1].find('a', class_='tab-link')
    if link is None:
        return None
    return link.get_text(strip=True) or None


def repair_ticker_column(table: pd.DataFrame, data_rows: List) -> int:
    """Overwrite mangled Ticker values with the symbols carried in the markup.

    ``table`` is finvizfinance's cumulative frame across pages; ``data_rows``
    are the current page's <tr> elements, which map to its trailing rows.
    Returns the number of values actually corrected.
    """
    if table is None or 'Ticker' not in getattr(table, 'columns', []):
        return 0
    start = len(table) - len(data_rows)
    if start < 0:
        return 0

    col = table.columns.get_loc('Ticker')
    repaired = 0
    for offset, row in enumerate(data_rows):
        symbol = _ticker_from_row(row)
        if not symbol:
            continue
        if table.iat[start + offset, col] != symbol:
            table.iat[start + offset, col] = symbol
            repaired += 1
    return repaired


def make_ticker_repair_view(base_view_cls):
    """Build a screener view that re-reads tickers from the markup each page.

    The avatar corruption is a property of the screener's ticker cell, so it
    hits every view. The SI collector reads Ownership, which carries Short
    Float directly.
    """

    class _TickerRepairView(base_view_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.tickers_repaired = 0

        def _get_table(self, rows, df, num_col_index, table_header, limit=-1):
            table = super()._get_table(rows, df, num_col_index, table_header, limit)
            # Mirror the parent's row slicing so offsets line up.
            data_rows = rows[1:]
            if limit != -1:
                data_rows = data_rows[0:limit]
            self.tickers_repaired += repair_ticker_column(table, data_rows)
            return table

    _TickerRepairView.__name__ = f"_TickerRepair{base_view_cls.__name__}"
    _TickerRepairView.__qualname__ = _TickerRepairView.__name__
    return _TickerRepairView
