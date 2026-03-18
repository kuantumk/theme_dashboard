"""
Centralized configuration for the theme_dashboard project.

Loads environment variables from .env and workflow settings from workflow_config.yaml.
All modules should import config from here instead of loading individually.
"""

import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

# ─── Project Root ───────────────────────────────────────────────
# config/ is one level below project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ─── Environment Variables ──────────────────────────────────────
load_dotenv(PROJECT_ROOT / ".env")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
IBKR_FLEX_TOKEN = os.getenv("IBKR_FLEX_TOKEN", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

# ─── YAML Config ────────────────────────────────────────────────
CONFIG_FILE = PROJECT_ROOT / "config" / "workflow_config.yaml"
with CONFIG_FILE.open() as _f:
    CONFIG = yaml.safe_load(_f)

# ─── Derived Paths ──────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data"
DOCS_DIR = PROJECT_ROOT / "docs"
DOCS_DATA_DIR = DOCS_DIR / "data"
REPORTS_DIR = PROJECT_ROOT / CONFIG["report"]["output_dir"]
LOG_DIR = PROJECT_ROOT / CONFIG["logging"]["dir"]
SCREENING_OUTPUT_DIR = PROJECT_ROOT / "screening_output"

PRICE_DATA_FILE = DATA_DIR / "price_daily.pkl"
PRICE_DATA_TA_FILE = DATA_DIR / "price_daily_ta.pkl"
FUNDAMENTALS_DB = DATA_DIR / "fundamentals.db"
TICKER_THEMES_FILE = PROJECT_ROOT / CONFIG["themes"]["metadata_file"]
TICKER_COMPANY_METADATA_FILE = PROJECT_ROOT / CONFIG["themes"]["company_metadata_file"]
THEME_REVIEW_STATE_FILE = PROJECT_ROOT / CONFIG["themes"]["review_state_file"]
BREADTH_FILE = DATA_DIR / "market_breadth_latest.json"
BREADTH_HISTORY_FILE = DOCS_DATA_DIR / "market_breadth.json"

# ─── Ensure directories exist ───────────────────────────────────
for d in [DATA_DIR, DOCS_DATA_DIR, REPORTS_DIR, LOG_DIR, SCREENING_OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)
