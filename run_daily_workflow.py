"""
Main daily workflow orchestrator.

Runs the complete stock screening pipeline:
1. Download price data
2. Calculate technical indicators
3. Scrape market breadth
4. Create master table (with RS_STS%)
5. Run all screeners
6. Consolidate results
7. Fetch fundamentals for screened tickers
8. Sync Google Sheet ground truth + surface untagged tickers
9. Analyze theme strength
10. Generate daily report

LLM theme classification is NOT part of this pipeline: the weekday Claude Code
audit routine (.claude/routines/theme_tag_audit.md) tags the untagged tickers
surfaced by step 8 and merges the result back to main.
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import logging
import json
from glob import glob
import pandas as pd

from config.settings import CONFIG, PROJECT_ROOT, LOG_DIR, SCREENING_OUTPUT_DIR
from src.data_collection.scrape_market_breadth import get_market_breadth
from src.data_collection.fetch_fundamental_data import batch_fetch_fundamentals
from src.themes.tag_new_tickers import sync_screened_ticker_themes
from src.themes.analyze_theme_strength import analyze_theme_strength
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


def run_script(script_path: str, args: list = None, description: str = None):
    """Run a Python script as a subprocess."""
    if description:
        logger.info(f"{'='*80}")
        logger.info(f"STEP: {description}")
        logger.info(f"{'='*80}")

    cmd = [sys.executable, '-u', script_path]
    if args:
        cmd.extend(args)

    logger.info(f"Running: {' '.join(cmd)}")

    try:
        env = os.environ.copy()
        env['PYTHONPATH'] = str(PROJECT_ROOT)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=PROJECT_ROOT,
            env=env
        )

        if result.stdout:
            logger.info(f"STDOUT: {result.stdout}")

        logger.info(f"OK {script_path} completed\n")
        return result

    except subprocess.CalledProcessError as e:
        logger.error(f"FAILED {script_path} (exit code {e.returncode})")
        if e.stdout:
            logger.error(f"STDOUT: {e.stdout}")
        if e.stderr:
            logger.error(f"STDERR: {e.stderr}")

        # Non-critical steps can fail without aborting
        if script_path in ['src/data_collection/fetch_fundamental_data.py', 'src/data_collection/scrape_market_breadth.py']:
            logger.warning(f"Non-critical step failed, continuing...")
            return False
        else:
            logger.error(f"Critical step failed, aborting workflow")
            raise
    return False


def consolidate_screener_results(date_str: str):
    """Consolidate all screener txt files into union file."""
    logger.info("Consolidating screener results...")

    consolidated_dir = SCREENING_OUTPUT_DIR / 'consolidated'
    consolidated_dir.mkdir(exist_ok=True, parents=True)

    # Find all screener txt files for today
    txt_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%m%d%Y')
    screener_files = sorted(consolidated_dir.glob(f'_*_{txt_date}.txt'))

    if not screener_files:
        logger.warning(f"No screener files found for {txt_date}")
        return set()

    # Combine all tickers (skip 0-match screeners that produced empty files)
    all_tickers = set()
    for f in screener_files:
        if f.stat().st_size == 0:
            logger.info(f"  {f.name}: 0 matches (empty file, skipping)")
            continue
        df = pd.read_csv(f, header=None)
        all_tickers.update(df[0].tolist())

    # Save union file
    union_file = consolidated_dir / f'_union_{txt_date}.txt'
    pd.DataFrame(sorted(all_tickers)).to_csv(union_file, index=False, header=False)

    logger.info(f"OK Consolidated {len(screener_files)} screeners -> {len(all_tickers)} unique tickers")
    logger.info(f"  Saved to {union_file}")

    return all_tickers


def run_daily_workflow():
    """Execute the complete daily workflow."""
    start_time = datetime.now()
    logger.info(f"\n{'#'*80}")
    logger.info(f"# DAILY STOCK SCREENING WORKFLOW - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'#'*80}\n")

    try:
        # Step 1: Download price data
        run_script(
            'src/data_collection/download_price_daily.py',
            description="Download daily price data from Yahoo Finance"
        )

        # Step 2: Calculate technical indicators
        run_script(
            'src/indicators/create_technical_indicators.py',
            description="Calculate technical indicators"
        )

        # Step 3: Scrape market breadth
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Scrape market breadth indicators")
        logger.info(f"{'='*80}")

        market_breadth = get_market_breadth()

        # Save market breadth to temp file for report generator
        temp_breadth_file = PROJECT_ROOT / 'data' / 'market_breadth_latest.json'
        temp_breadth_file.parent.mkdir(exist_ok=True)
        with temp_breadth_file.open('w') as f:
            json.dump(market_breadth, f)

        logger.info(f"OK Market breadth saved\n")

        # Step 4: Create master table (includes RS_STS% calculation).
        # `--days 130` (~180 calendar days of trading sessions) so historical
        # master CSVs always carry today's full indicator schema AND span the
        # full time-travel retention window (THEMES_HISTORY_DAYS = 180). When a
        # new column like `vars` is added, every dropdown session gets backfilled
        # in one workflow run rather than accumulating naturally. The exporter
        # prunes anything older than 180 calendar days, so a few extra sessions
        # here are harmless padding that guarantees the window is always full.
        run_script(
            'src/screening/create_master_table.py',
            args=['--days', '130'],
            description="Create master table with RS_STS% (130 sessions ~ 180 calendar days for time-travel history)"
        )

        # Step 5: Run all screeners (`--days 130` so dashboard time-travel
        # has a full ~180-calendar-day history per screener every run, not just today).
        for screener in CONFIG['screeners']:
            run_script(
                'src/screening/run_screener.py',
                args=['--screener', screener, '--days', '130'],
                description=f"Run {screener} screener (130 sessions ~ 180 calendar days)"
            )

        # Step 6: Consolidate screener results
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Consolidate screener results")
        logger.info(f"{'='*80}")

        # Get today's date from latest master file
        master_files = sorted(glob(str(SCREENING_OUTPUT_DIR / 'master' / 'master_*.parquet')))
        if not master_files:
            raise FileNotFoundError("No master files found")

        latest_master = Path(master_files[-1])
        date_str = latest_master.stem.replace('master_', '')

        all_tickers = consolidate_screener_results(date_str)

        # Step 7: Fetch fundamentals for screened tickers
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Fetch fundamental data for screened tickers")
        logger.info(f"{'='*80}")

        screened_list = list(all_tickers)
        logger.info(f"Fetching fundamentals for {len(screened_list)} screened tickers...")
        try:
            batch_fetch_fundamentals(screened_list)
            logger.info(f"OK Fundamental data fetch complete\n")
        except Exception as e:
            logger.warning(f"Fundamental data fetch failed: {e}")
            logger.warning("Continuing workflow without fundamentals...")

        # Step 8: Sync Google Sheet ground truth + surface untagged tickers
        # (LLM classification happens in the weekday audit routine, not here.)
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Sync Sheet ground truth + surface untagged tickers")
        logger.info(f"{'='*80}")

        sync_result = None
        untagged_tickers = []
        try:
            sync_result = sync_screened_ticker_themes(all_tickers)
            untagged_tickers = sync_result.untagged_tickers
            logger.info(
                "Theme sync complete "
                f"({len(sync_result.google_sheet_updates)} sheet updates, "
                f"{len(sync_result.profile_candidates)} profiles warmed, "
                f"{len(untagged_tickers)} untagged awaiting routine)\n"
            )
        except Exception as e:
            logger.error(f"Theme sync FAILED: {e}")
            logger.warning("Continuing workflow with existing themes...")

        # Step 9: Analyze theme strength
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Analyze theme strength")
        logger.info(f"{'='*80}")

        master_df = su.load_df_from_parquet(latest_master)
        theme_df = analyze_theme_strength(master_df, market_breadth, screened_tickers=all_tickers)

        regime = theme_df['regime'].iloc[0] if not theme_df.empty and 'regime' in theme_df.columns else 'N/A'
        logger.info(f"OK Analyzed {len(theme_df)} themes (regime: {regime})\n")

        # Step 10: Generate daily report
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Generate daily report")
        logger.info(f"{'='*80}")

        report = generate_daily_report(
            date_str=date_str,
            master_df=master_df,
            theme_df=theme_df,
            market_breadth=market_breadth,
            screened_tickers=all_tickers,
            untagged_tickers=untagged_tickers
        )

        report_file = save_report(report, date_str)

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"\n{'#'*80}")
        logger.info(f"# WORKFLOW COMPLETE")
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
        logger.error(f"WORKFLOW FAILED")
        logger.error(f"{'='*80}")
        logger.error(f"Error: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = run_daily_workflow()
    sys.exit(0 if success else 1)
