"""
Stock utilities module.

Provides:
- pickle_object_to_file / load_object_from_pickle
- get_tickers_from_nasdaq
- get_latest_file
- SCREENING_OUTPUT_DIR (via config)
"""

import re
import pickle
from ftplib import FTP
from io import StringIO

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
