"""
Main daily workflow orchestrator.

Runs the complete stock screening pipeline:
1. Download price data
2. Calculate technical indicators
3. Scrape market breadth (3b: Nasdaq McClellan Summation Index; 3c: CFTC COT)
4. Create master tables (with RS_STS%)
5. Run all screeners (5b: EMA20 cross setups)
6. Consolidate results
7. Fetch fundamentals for screened tickers (7b: short interest)
8. Sync Google Sheet ground truth + surface untagged tickers
9. Analyze theme strength (9b: L1 radar)
10. Generate daily report

A critical step that fails stops the workflow (exit 1); a non-critical one
logs a warning and the workflow carries on without its output.

LLM theme classification is NOT part of this pipeline: the weekday Claude Code
audit routine (.claude/routines/theme_tag_audit.md) tags the untagged tickers
surfaced by step 8 and merges the result back to main.
"""

import json
import logging
import os
import subprocess
import sys
from datetime import datetime

from config.settings import (
    BREADTH_FILE, CONFIG, DATA_DIR, LOG_DIR, PROJECT_ROOT, SCREENING_OUTPUT_DIR,
)
import src.stock_utils as su
from src.data_collection.barchart_breadth import get_market_breadth, write_market_breadth
from src.data_collection.fetch_fundamental_data import batch_fetch_fundamentals
from src.data_collection.fetch_short_interest import (
    fetch_short_interest,
    write_short_interest,
)
from src.screening.run_screener import master_files_by_date
from src.themes.tag_new_tickers import sync_screened_ticker_themes
from src.themes.analyze_theme_strength import analyze_theme_strength
from src.themes.l1_score import compute_radar
from src.reporting.generate_daily_report import generate_daily_report, save_report

# Setup logging
logging.basicConfig(
    level=getattr(logging, CONFIG["logging"]["level"]),
    format=CONFIG["logging"]["format"],
    handlers=[
        logging.FileHandler(LOG_DIR / f"daily_workflow_{datetime.now().strftime('%Y-%m-%d')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# `--days 130` (~180 calendar days of trading sessions) so the back-dated
# master and screener parquet always carry today's full indicator schema AND
# span the full time-travel window (THEMES_HISTORY_DAYS = 180). The exporter
# prunes anything older than 180 calendar days, so the few extra sessions are
# padding that keeps the window full.
HISTORY_SESSIONS = '130'


class StepFailed(RuntimeError):
    """A critical step failed; the workflow stops."""


def run_script(script_path, args=()):
    """Run a pipeline script as a subprocess, streaming its output into the log.

    stdout and stderr are merged and logged line by line as they arrive, so a
    long step shows progress and a failure keeps its traceback. Raises
    ``subprocess.CalledProcessError`` on a non-zero exit.
    """
    cmd = [sys.executable, '-u', script_path, *args]
    logger.info(f"Running: {' '.join(cmd)}")
    env = os.environ.copy()
    env['PYTHONPATH'] = str(PROJECT_ROOT)
    # tqdm redraws with '\r', which a line reader turns into one line per
    # redraw; one progress line per 30s is plenty in a log.
    env.setdefault('TQDM_MININTERVAL', '30')
    with subprocess.Popen(cmd, cwd=PROJECT_ROOT, env=env, text=True, bufsize=1,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                logger.info(f"  | {line}")
        returncode = proc.wait()
    if returncode:
        raise subprocess.CalledProcessError(returncode, cmd)


def run_step(description, action, *, critical=True, default=None):
    """Run one workflow step under a banner.

    A critical step that raises stops the workflow (``StepFailed``). A
    non-critical one logs a warning and returns ``default`` instead.
    """
    logger.info('=' * 80)
    logger.info(f"STEP: {description}")
    logger.info('=' * 80)
    try:
        result = action()
    except Exception as e:
        if critical:
            logger.error(f"FAILED {description}: {e}")
            raise StepFailed(description) from e
        logger.warning(f"Non-critical step failed, continuing: {description}: {e}")
        return default
    logger.info(f"OK {description}\n")
    return result


def consolidate_screener_results(date_str: str):
    """Union screened tickers from the per-screener parquet outputs for ``date_str``.

    The per-screener ``.txt`` files were removed; each screener writes its
    passing rows to ``<screener>/<screener>_<date>.parquet``, so the day's union
    is the distinct ``ticker`` values across ``CONFIG['screeners']``. The latest
    union is written to ``data/screened_union.json`` (committed, tiny) so the
    weekday tag-audit routine can read the worklist from a clone that no longer
    carries ``screening_output/``. The set is returned for the in-process theme
    and report steps.
    """
    logger.info("Consolidating screener results...")

    # The 20EMA setups are not a master-table filter, so they are not in
    # `screeners`, but their names need fundamentals and theme tags too.
    all_tickers = su.union_tickers_for_date(
        date_str, CONFIG['screeners'] + ['ema20'], root=SCREENING_OUTPUT_DIR
    )

    union_file = DATA_DIR / 'screened_union.json'
    union_file.parent.mkdir(exist_ok=True, parents=True)
    with union_file.open('w', encoding='utf-8') as f:
        json.dump({'date': date_str, 'tickers': sorted(all_tickers)}, f, indent=2)

    logger.info(
        f"OK Consolidated {len(CONFIG['screeners'])} screeners -> "
        f"{len(all_tickers)} unique tickers"
    )
    logger.info(f"  Union written to {union_file}")

    return all_tickers


def scrape_breadth():
    """Step 3: one barchart.com scrape, written to BREADTH_FILE for the report and the export."""
    breadth = get_market_breadth()
    write_market_breadth(breadth, BREADTH_FILE)
    logger.info(f"Market breadth: {breadth}")
    return breadth


def latest_master():
    """``(date_str, path)`` of the newest master table."""
    masters = master_files_by_date(SCREENING_OUTPUT_DIR / 'master')
    if not masters:
        raise FileNotFoundError("No master files found")
    date_str = list(masters)[-1]
    return date_str, masters[date_str]


def fetch_and_write_short_interest(date_str):
    # The screen carries a Current Volume leg, so it must run after the
    # close: before the open it returns an empty table.
    payload = fetch_short_interest()
    write_short_interest(payload, date_str)
    logger.info(f"Short interest: {len(payload['rows'])} tickers")


def run_daily_workflow():
    """Execute the complete daily workflow. Returns True on success."""
    start_time = datetime.now()
    logger.info(f"\n{'#'*80}")
    logger.info(f"# DAILY STOCK SCREENING WORKFLOW - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'#'*80}\n")

    try:
        run_step("1. Download daily price data from Yahoo Finance",
                 lambda: run_script('src/data_collection/download_price_daily.py'))
        run_step("2. Calculate technical indicators",
                 lambda: run_script('src/indicators/create_technical_indicators.py'))
        market_breadth = run_step("3. Scrape market breadth (barchart.com)",
                                  scrape_breadth, critical=False, default={})
        # Its own ticker universe (breadth needs the ETFs that
        # `get_tickers_from_nasdaq` drops) and its own cached A/D history, so
        # a failure here cannot affect screening.
        run_step("3b. Compute Nasdaq McClellan Summation Index + RSI(14)",
                 lambda: run_script('src/data_collection/compute_nasi.py'), critical=False)
        # The CFTC reports weekly, so most runs rewrite the same weeks. A failed
        # fetch leaves the last cot.json, whose report date the tab prints.
        run_step("3c. Fetch CFTC Commitments of Traders (COT tab)",
                 lambda: run_script('src/data_collection/fetch_cot.py'), critical=False)
        run_step(f"4. Create master tables with RS_STS% ({HISTORY_SESSIONS} sessions)",
                 lambda: run_script('src/screening/create_master_table.py',
                                    ['--days', HISTORY_SESSIONS]))
        run_step(f"5. Run all screeners ({HISTORY_SESSIONS} sessions)",
                 lambda: run_script('src/screening/run_screener.py',
                                    ['--all', '--days', HISTORY_SESSIONS]))
        # Non-critical: only the 20EMA tab reads it, and a missing session
        # adds nothing to the screened union.
        run_step(f"5b. Find EMA20 cross setups ({HISTORY_SESSIONS} sessions)",
                 lambda: run_script('src/screening/ema20_setups.py',
                                    ['--days', HISTORY_SESSIONS]), critical=False)

        date_str, master_path = latest_master()
        all_tickers = run_step("6. Consolidate screener results",
                               lambda: consolidate_screener_results(date_str))

        run_step(f"7. Fetch fundamental data for {len(all_tickers)} screened tickers",
                 lambda: batch_fetch_fundamentals(list(all_tickers)), critical=False)
        # Nothing downstream reads the SI file except the SI export, which
        # skips cleanly when it is absent.
        run_step("7b. Fetch short interest (SI tab)",
                 lambda: fetch_and_write_short_interest(date_str), critical=False)

        # LLM classification happens in the weekday audit routine, not here.
        sync_result = run_step("8. Sync Sheet ground truth + surface untagged tickers",
                               lambda: sync_screened_ticker_themes(all_tickers),
                               critical=False)
        untagged_tickers = sync_result.untagged_tickers if sync_result else []
        if sync_result:
            logger.info(
                f"Theme sync: {len(sync_result.google_sheet_updates)} sheet updates, "
                f"{len(sync_result.profile_candidates)} profiles warmed, "
                f"{len(untagged_tickers)} untagged awaiting routine"
            )

        master_df = su.load_df_from_parquet(master_path)
        theme_df = run_step("9. Analyze theme strength",
                            lambda: analyze_theme_strength(master_df, market_breadth,
                                                           screened_tickers=all_tickers))
        logger.info(f"Analyzed {len(theme_df)} themes")

        radar = None
        if CONFIG.get('radar', {}).get('enabled', True):
            radar = run_step("9b. Compute L1 radar",
                             lambda: compute_radar(master_df, screened_tickers=all_tickers),
                             critical=False)
            if radar:
                logger.info(f"Radar scored {len(radar['l1s'])} L1s")

        report_file = run_step("10. Generate daily report", lambda: save_report(
            generate_daily_report(
                date_str=date_str,
                master_df=master_df,
                theme_df=theme_df,
                market_breadth=market_breadth,
                screened_tickers=all_tickers,
                untagged_tickers=untagged_tickers,
                radar=radar,
            ),
            date_str,
        ))

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"\n{'#'*80}")
        logger.info("# WORKFLOW COMPLETE")
        logger.info(f"{'#'*80}")
        logger.info(f"Duration: {duration:.1f} seconds")
        logger.info(f"Report: {report_file}")
        logger.info(f"Total tickers: {len(master_df)}")
        logger.info(f"Hot themes: {theme_df['is_hot'].sum() if 'is_hot' in theme_df.columns else 0}")
        logger.info(f"Untagged awaiting routine: {len(untagged_tickers)}")
        if sync_result:
            logger.info(f"Sync audit: {sync_result.audit_report_path}")
        logger.info(f"{'#'*80}\n")
        return True

    except Exception as e:
        logger.error(f"\n{'='*80}")
        logger.error("WORKFLOW FAILED")
        logger.error(f"{'='*80}")
        logger.error(f"Error: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = run_daily_workflow()
    sys.exit(0 if success else 1)
