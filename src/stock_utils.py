"""
Stock utilities module.

Provides:
- pickle_object_to_file / load_object_from_pickle
- save_df_to_parquet / load_df_from_parquet
- get_tickers_from_nasdaq
- get_latest_file
- SCREENING_OUTPUT_DIR (via config)
"""

import re
import pickle
from ftplib import FTP
from io import StringIO
from pathlib import Path

import pandas as pd

from config.settings import PROJECT_ROOT, SCREENING_OUTPUT_DIR, DATA_DIR


def pickle_object_to_file(obj, file_path):
    """Save Python object to pickle file."""
    with open(file_path, 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_object_from_pickle(pickle_path):
    """Load Python object from pickle file."""
    with open(pickle_path, 'rb') as handle:
        obj = pickle.load(handle)
    return obj


def save_df_to_parquet(df, file_path):
    """Save a DataFrame to parquet (pyarrow engine), creating parent dirs.

    Screening numeric outputs (master tables, per-screener results) use this
    instead of CSV — parquet is columnar, ~5-10x smaller on disk, preserves
    dtypes exactly, and reads faster across the per-day time-travel window.
    The index is not written (all callers use a default RangeIndex).
    """
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path, engine='pyarrow', index=False)


def load_df_from_parquet(parquet_path):
    """Load a DataFrame from parquet (pyarrow engine).

    On pandas 3.x, string columns come back as the default ``str`` dtype —
    exactly what ``pd.read_csv`` produces — so the CSV->parquet swap is
    transparent to the screening consumers (which already run on pandas 3.x).
    """
    return pd.read_parquet(parquet_path, engine='pyarrow')


def union_tickers_for_date(date_str, screeners, root=SCREENING_OUTPUT_DIR):
    """Union of screened tickers across per-screener parquet outputs for a date.

    Replaces the removed ``_union_<date>.txt``: each screener writes its passing
    rows to ``<screener>/<screener>_<date>.parquet``, so the day's screened
    union is the distinct ``ticker`` values across the given screeners. Missing
    or empty per-screener files contribute nothing (a 0-match screener still
    writes a readable empty parquet). ``date_str`` is ``YYYY-MM-DD``.
    """
    root = Path(root)
    tickers = set()
    for screener in screeners:
        parquet_file = root / screener / f'{screener}_{date_str}.parquet'
        if not parquet_file.exists():
            continue
        try:
            df = load_df_from_parquet(parquet_file)
        except Exception:
            continue
        if 'ticker' in df.columns and len(df):
            tickers.update(str(t) for t in df['ticker'].tolist())
    return tickers


def get_latest_file(file_dir, keyword, file_index=1):
    """
    Get the latest file in a folder matching a keyword pattern.

    Args:
        file_dir: Directory to search
        keyword: Glob pattern to match
        file_index: Which file to return (1 = most recent)

    Returns:
        Path object of the matched file
    """
    sorted_files = sorted([x for x in file_dir.glob(keyword)])
    return sorted_files[-file_index]


def exchange_from_symbol(symbol):
    """Convert exchange symbol to full name."""
    exchanges = {
        "Q": "NASDAQ",
        "A": "NYSE MKT",
        "N": "NYSE",
        "P": "NYSE ARCA",
        "Z": "BATS",
        "V": "IEXG"
    }
    return exchanges.get(symbol, "n/a")


def get_tickers_from_nasdaq():
    """
    Get ticker list from NASDAQ FTP.

    Returns:
        tickers: List of ticker symbols
        exchanges: Dict mapping ticker to exchange
    """
    ticker_file = DATA_DIR / 'tickers_from_nasdaq.txt'
    filename = "nasdaqtraded.txt"
    ticker_column = 1
    company_name_column = 2
    etf_column = 5
    exchange_column = 3
    test_column = 7

    try:
        ftp = FTP('ftp.nasdaqtrader.com')
        ftp.login()
        ftp.cwd('SymbolDirectory')
        lines = StringIO()
        ftp.retrlines('RETR ' + filename, lambda x: lines.write(str(x) + '\n'))
        ftp.quit()
        lines.seek(0)
        results = lines.readlines()
        with open(ticker_file, 'w') as f:
            f.write('\n'.join(results))
    except Exception as e:
        print(f"{e} - Nasdaq FTP connection failed. Using last downloaded ticker list...")
        with open(ticker_file, 'r') as f:
            results = f.readlines()
            results = [r for r in results if r != '\n']

    tickers = []
    exchanges = []
    for entry in results:
        values = entry.split('|')
        ticker = values[ticker_column]
        exchange = exchange_from_symbol(values[exchange_column])

        # Filter out warrants, notes, ETFs, ETNs
        company_name_lower = values[company_name_column].lower()
        if (re.match(r'^[A-Z]+$', ticker)
                and 'warrant' not in company_name_lower
                and 'fixed-to-floating-rate' not in company_name_lower
                and 'fixed rate' not in company_name_lower
                and '- unit' not in company_name_lower
                and '- right' not in company_name_lower
                and ' etn' not in company_name_lower
                and values[etf_column] == "N"
                and values[test_column] == "N"):
            tickers.append(ticker)
            exchanges.append(exchange)

    # Return exchanges as a dict
    exchanges = {t: e for t, e in zip(tickers, exchanges)}
    return tickers, exchanges
