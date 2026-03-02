"""
Download price history data from Yahoo Finance.

Robustly handles rate limits and ensures Benchmark (SPY) is valid.
"""

import time
import datetime as dt
import pandas as pd
import yfinance as yf

import src.stock_utils as su
from config.settings import PRICE_DATA_FILE

TODAY = dt.date.today()
END_DATE = TODAY + dt.timedelta(days=1)  # yf.download end_date is not inclusive
START_DATE = TODAY - dt.timedelta(days=500)


def download_yf_price_data(start_date, end_date):
    """
    Download price history data from Yahoo Finance.
    Robustly handles rate limits and ensures Benchmark (SPY) is valid.
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

    BATCH_SIZE = 200
    escaped_tickers = [t.replace('.', '-') for t in tickers]

    success_count = 0

    for i in range(0, len(tickers), BATCH_SIZE):
        batch_tickers = tickers[i:i+BATCH_SIZE]
        batch_escaped = escaped_tickers[i:i+BATCH_SIZE]

        if i % (BATCH_SIZE * 5) == 0:
            print(f"  Processing {i}/{len(tickers)}...")

        try:
            batch_df = yf.download(
                tickers=batch_escaped,
                start=start_date,
                end=end_date,
                group_by='ticker',
                threads=True,
                progress=False
            )

            if batch_df.empty:
                continue

            for ticker, escaped_ticker in zip(batch_tickers, batch_escaped):
                try:
                    if isinstance(batch_df.columns, pd.MultiIndex):
                        if escaped_ticker in batch_df.columns.get_level_values(0):
                            t_df = batch_df[escaped_ticker].copy()
                            t_df.columns = [c.lower() for c in t_df.columns]

                            if not t_df['close'].dropna().empty:
                                price_data_dict[ticker] = t_df
                                success_count += 1
                    else:
                        pass

                except Exception:
                    continue

            time.sleep(1.0)

        except Exception as e:
            print(f"  Batch failed: {e}")
            time.sleep(5)

    print(f"\nDownload complete. Success: {success_count}/{len(tickers)} tickers.")
    print(f"Indices status: SPY={'YES' if 'SPY' in price_data_dict else 'NO'}")

    if 'SPY' not in price_data_dict:
        raise ValueError("CRITICAL ERROR: SPY data failed to download. Cannot proceed.")
    if '^GSPC' not in price_data_dict:
        raise ValueError("CRITICAL ERROR: ^GSPC data failed to download. Cannot proceed.")

    su.pickle_object_to_file(price_data_dict, PRICE_DATA_FILE)


if __name__ == '__main__':
    download_yf_price_data(START_DATE, END_DATE)
