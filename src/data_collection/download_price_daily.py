"""
Download price history data from Yahoo Finance.

Robustly handles rate limits and ensures Benchmark (SPY) is valid.
"""

import time
import datetime as dt
import pandas as pd
import yfinance as yf

import src.stock_utils as su
from config.settings import CONFIG, PRICE_DATA_FILE

TODAY = dt.date.today()
END_DATE = TODAY + dt.timedelta(days=1)  # yf.download end_date is not inclusive
# market_history reads the same key as this download's length (its warm-up
# must cover the 365-day volume-spike window), so the two cannot drift.
START_DATE = TODAY - dt.timedelta(days=int(CONFIG["price_data"]["lookback_days"]))


BATCH_SIZE = 200
BATCH_RETRY_WAIT_S = 30


class DownloadCoverageError(RuntimeError):
    """Too few of the requested tickers came back to publish a run from."""


def _download_batch(tickers, escaped, start_date, end_date):
    """``{ticker: frame}`` for one batch. Raises if the request itself failed."""
    batch_df = yf.download(
        tickers=escaped,
        start=start_date,
        end=end_date,
        group_by='ticker',
        threads=True,
        progress=False
    )
    found = {}
    if batch_df.empty or not isinstance(batch_df.columns, pd.MultiIndex):
        return found
    symbols = set(batch_df.columns.get_level_values(0))
    for ticker, escaped_ticker in zip(tickers, escaped):
        if escaped_ticker not in symbols:
            continue
        try:
            t_df = batch_df[escaped_ticker].copy()
            t_df.columns = [c.lower() for c in t_df.columns]
            if not t_df['close'].dropna().empty:
                found[ticker] = t_df
        except Exception:
            continue
    return found


def download_yf_price_data(start_date, end_date, min_coverage=None):
    """
    Download price history data from Yahoo Finance.
    Robustly handles rate limits and ensures Benchmark (SPY) is valid.

    A batch that fails outright (an exception or an empty frame) is retried
    once. With ``min_coverage`` set, the download raises
    ``DownloadCoverageError`` -- before writing anything -- when fewer than
    that share of the requested tickers came back, so a partial Yahoo outage
    cannot replace the price file with a thinned universe that every tab
    then publishes. market_history calls this without ``min_coverage``: it
    retries rate-limited tickers itself after this returns, and a back-dated
    range legitimately lacks tickers listed since.
    """
    price_data_dict = {}

    # ---------------------------------------------------------
    # 1. Download INDICES first (Critical for RS calculation)
    # ---------------------------------------------------------
    indices = ['SPY', 'MDY', 'QQQ', 'IWM', '^GSPC']
    print(f"\nDownloading Critical Indices: {indices}")

    for index in indices:
        for attempt in range(3):
            try:
                print(f"  Fetching {index} (Attempt {attempt+1})...")
                df = yf.download(index, start=start_date, end=end_date, progress=False, threads=False)

                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns]
                else:
                    df = df.rename(columns=lambda x: x.lower())

                if not df.empty and not df['close'].dropna().empty:
                    last_date = df.index[-1].date()
                    if (TODAY - last_date).days < 5:
                        price_data_dict[index] = df
                        print(f"    OK {index} Success (Last date: {last_date})")
                        break
                    else:
                        print(f"    Warning: {index} Stale data (Last: {last_date})")
                else:
                    print(f"    Warning: {index} Empty or All-NaN")

            except Exception as e:
                print(f"    Error: {e}")

            time.sleep(10)

        if index not in price_data_dict:
            print(f"CRITICAL WARNING: Failed to download {index} after 3 attempts!")

    # ---------------------------------------------------------
    # 2. Download Market Tickers (Batched)
    # ---------------------------------------------------------
    tickers, _ = su.get_tickers_from_nasdaq()
    tickers = [t for t in tickers if t not in price_data_dict]

    print(f"\nDownloading {len(tickers)} market tickers in batches...")

    escaped_tickers = [t.replace('.', '-') for t in tickers]
    success_count = 0
    failed_batches = 0

    for i in range(0, len(tickers), BATCH_SIZE):
        batch_tickers = tickers[i:i+BATCH_SIZE]
        batch_escaped = escaped_tickers[i:i+BATCH_SIZE]

        if i % (BATCH_SIZE * 5) == 0:
            print(f"  Processing {i}/{len(tickers)}...")

        found = None
        for attempt in range(2):
            try:
                found = _download_batch(batch_tickers, batch_escaped, start_date, end_date)
            except Exception as e:
                print(f"  Batch {i // BATCH_SIZE} failed (attempt {attempt + 1}): {e}")
                found = None
            if found:
                break
            if attempt == 0:
                print(f"  Batch {i // BATCH_SIZE} returned nothing; retrying in {BATCH_RETRY_WAIT_S}s")
                time.sleep(BATCH_RETRY_WAIT_S)
        if not found:
            failed_batches += 1
            continue

        price_data_dict.update(found)
        success_count += len(found)
        time.sleep(1.0)

    print(f"\nDownload complete. Success: {success_count}/{len(tickers)} tickers "
          f"({failed_batches} batches returned nothing after a retry).")
    print(f"Indices status: SPY={'YES' if 'SPY' in price_data_dict else 'NO'}")

    if 'SPY' not in price_data_dict:
        raise ValueError("CRITICAL ERROR: SPY data failed to download. Cannot proceed.")
    if '^GSPC' not in price_data_dict:
        raise ValueError("CRITICAL ERROR: ^GSPC data failed to download. Cannot proceed.")
    if min_coverage is not None and tickers and success_count / len(tickers) < min_coverage:
        raise DownloadCoverageError(
            f"Only {success_count}/{len(tickers)} tickers downloaded "
            f"({success_count / len(tickers):.1%}, minimum {min_coverage:.0%}); "
            f"{PRICE_DATA_FILE} left unchanged.")

    su.pickle_object_to_file(price_data_dict, PRICE_DATA_FILE)


if __name__ == '__main__':
    download_yf_price_data(START_DATE, END_DATE,
                           min_coverage=float(CONFIG["price_data"]["min_coverage"]))
