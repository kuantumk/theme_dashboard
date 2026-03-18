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
8. Classify new/unclassified screened tickers
9. Analyze theme strength
10. Validate dashboard-visible ticker tags
11. Generate daily report
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
import src.stock_utils as su
from src.data_collection.scrape_market_breadth import get_market_breadth
from src.data_collection.fetch_fundamental_data import batch_fetch_fundamentals
from src.themes.tag_new_tickers import (
    load_existing_themes,
    sync_screened_ticker_themes,
    validate_dashboard_ticker_themes,
)
from src.themes.analyze_theme_strength import analyze_theme_strength
from src.reporting.generate_daily_report import (
    generate_daily_report,
    save_report,
    select_dashboard_theme_tickers,
)

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

    # Combine all tickers
    all_tickers = set()
    for f in screener_files:
        df = pd.read_csv(f, header=None)
        all_tickers.update(df[0].tolist())

    # Save union file
    union_file = consolidated_dir / f'_union_{txt_date}.txt'
    pd.DataFrame(sorted(all_tickers)).to_csv(union_file, index=False, header=False)

    logger.info(f"✓ Consolidated {len(screener_files)} screeners -> {len(all_tickers)} unique tickers")
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
            description="Calculate technical indicators (OPTIMIZED: 25 indicators)"
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

        # Step 4: Create master table (includes RS_STS% calculation)
        run_script(
            'src/screening/create_master_table.py',
            args=['--days', '1'],
            description="Create master table with RS_STS%"
        )

        # Step 5: Run all screeners
        for screener in CONFIG['screeners']:
            run_script(
                'src/screening/run_screener.py',
                args=['--screener', screener, '--days', '1'],
                description=f"Run {screener} screener"
            )

        # Step 6: Consolidate screener results
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Consolidate screener results")
        logger.info(f"{'='*80}")

        # Get today's date from latest master file
        master_files = sorted(glob(str(SCREENING_OUTPUT_DIR / 'master' / 'master_*.csv')))
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
            logger.info(f"✓ Fundamental data fetch complete\n")
        except Exception as e:
            logger.warning(f"Fundamental data fetch failed: {e}")
            logger.warning("Continuing workflow without fundamentals...")

        # Step 8: Classify new/unclassified screened tickers
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Classify new/unclassified screened tickers")
        logger.info(f"{'='*80}")

        classification_result = None
        try:
            classification_result = sync_screened_ticker_themes(all_tickers)
            ticker_themes = classification_result.ticker_themes
            logger.info(
                "Theme classification complete "
                f"({len(classification_result.classified_tickers)} classified, "
                f"{len(classification_result.new_tickers)} new, "
                f"{len(classification_result.unresolved_tickers)} unresolved)\n"
            )
        except Exception as e:
            logger.error(f"Theme classification FAILED: {e}")
            logger.warning("Continuing workflow with existing themes only...")
            ticker_themes = load_existing_themes()

        # Step 9: Analyze theme strength
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Analyze theme strength")
        logger.info(f"{'='*80}")

        master_df = pd.read_csv(latest_master)
        theme_df = analyze_theme_strength(master_df, market_breadth, screened_tickers=all_tickers)

        logger.info(f"✓ Analyzed {len(theme_df)} themes\n")

        # Step 10: Validate dashboard-visible ticker themes
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Validate dashboard-visible ticker themes")
        logger.info(f"{'='*80}")

        dashboard_tickers = select_dashboard_theme_tickers(theme_df, master_df, all_tickers)
        validation_result = None
        if dashboard_tickers:
            try:
                validation_result = validate_dashboard_ticker_themes(dashboard_tickers)
                ticker_themes = validation_result.ticker_themes
                logger.info(
                    "Theme validation complete "
                    f"({len(validation_result.confirmed_keeps)} keeps, "
                    f"{len(validation_result.pending_mismatches)} pending, "
                    f"{len(validation_result.applied_retags)} applied, "
                    f"{len(validation_result.unresolved_tickers)} unresolved)\n"
                )
                if validation_result.applied_retags or validation_result.google_sheet_updates:
                    theme_df = analyze_theme_strength(master_df, market_breadth, screened_tickers=all_tickers)
                    logger.info("Re-ran theme strength after confirmed dashboard retags\n")
            except Exception as e:
                logger.error(f"Theme validation FAILED: {e}")
                logger.warning("Continuing workflow with current upstream themes...")

        # Step 11: Generate daily report
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Generate daily report")
        logger.info(f"{'='*80}")

        new_tickers_list = classification_result.new_tickers if classification_result else []

        report = generate_daily_report(
            date_str=date_str,
            master_df=master_df,
            theme_df=theme_df,
            market_breadth=market_breadth,
            screened_tickers=all_tickers,
            new_tickers=new_tickers_list
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
        logger.info(f"New tickers tagged: {len(new_tickers_list)}")
        if classification_result:
            logger.info(f"Classification audit: {classification_result.audit_report_path}")
        if validation_result:
            logger.info(f"Confirmed retags applied: {len(validation_result.applied_retags)}")
            logger.info(f"Pending tag mismatches: {len(validation_result.pending_mismatches)}")
            logger.info(f"Validation audit: {validation_result.audit_report_path}")
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
