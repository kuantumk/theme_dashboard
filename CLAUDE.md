# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Theme Dashboard is a momentum trading stock screening platform that identifies correlated stock themes ("group moves") for Qullamaggie-style trading. It runs an automated daily workflow: download prices → compute indicators → screen stocks → group into themes → score and report.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run complete daily workflow (all 10 steps)
python run_daily_workflow.py

# Run individual pipeline steps
python src/data_collection/download_price_daily.py
python src/indicators/create_technical_indicators.py
python src/screening/create_master_table.py --days 1
python src/screening/run_screener.py --screener steady_trend --days 1
python src/themes/analyze_theme_strength.py
python src/reporting/generate_daily_report.py
python src/reporting/export_dashboard_data.py

# Run EP scans standalone (requires ALPACA_API_KEY/SECRET in .env)
python src/reporting/ep_scan_afternoon.py
python src/reporting/ep_scan_morning.py

# Test a screener against a single ticker
python src/screening/run_screener.py --screener steady_trend --test --ticker AAPL

# Run scoring backtests
cd tests && python backtest_theme_scoring.py
```

## Architecture

### Data Flow (10-Step Daily Pipeline)

`run_daily_workflow.py` orchestrates the pipeline by spawning each step as a subprocess:

1. **Download** ~8000 stocks × 500 days OHLCV via yfinance → `data/price_daily.pkl`
2. **Indicators** 25 pandas-based technicals (no TA-Lib) → `data/price_daily_ta.pkl`
3. **Market Breadth** NCFD/MMFI scraped from barchart.com via Selenium → `docs/data/market_breadth.json`
4. **Master Table** cross-sectional percentile ranks + RS_STS% → `screening_output/master/`
5. **Screeners** 5 pattern filters run in parallel → per-screener CSVs
6. **Consolidate** union all screener tickers → `screening_output/consolidated/`
7. **Fundamentals** float/EPS/short% from Finviz → `data/fundamentals.db` (SQLite, 7-day cache)
8. **AI Tagging** Gemini 3 Flash classifies new tickers into themes → `data/ticker_themes.json`
9. **Theme Scoring** dual-metric (strength + confirmation) with actionability overlay
10. **Report** markdown daily report → `reports/`

### EP Scan Pipeline (Earnings Pivot Scanner)

Separate from the daily theme pipeline, two workflows scan for earnings-driven setups:

- **Afternoon scan** (`ep-scan-afternoon.yml`, 2:00 PM Pacific) — Finviz screener for Today AMC earnings (short float >10%, avg vol >1M), filters for AH price ≥ close, enriches with RVol/technicals/news → `docs/data/ep_scan_afternoon.json`
- **Morning scan** (`ep-scan-morning.yml`, 5:45 AM Pacific) — same filters for Today BMO earnings, filters for PM price ≥ prev close → `docs/data/ep_scan_morning.json`

Shared logic lives in `src/reporting/ep_scan_common.py`. Key details:
- **RVol at time**: uses Alpaca Market Data API (SIP feed) for 5-min extended-hours bars. Treats 4 AM–8 PM ET as one continuous session, computes cumulative volume ratio vs 10-session historical average. yfinance does NOT provide usable extended-hours volume at 5m intervals.
- **Discord notification**: sends webhook alert with ticker summaries on scan completion.

### Key Data Stores

| File | Format | Content |
|------|--------|---------|
| `data/price_daily.pkl` | Pickle (dict of DataFrames) | Raw OHLCV history |
| `data/price_daily_ta.pkl` | Pickle | Price data + 25 technical indicators |
| `data/fundamentals.db` | SQLite | Finviz fundamentals with 7-day TTL |
| `data/ticker_themes.json` | JSON | `{ticker: [theme1, theme2]}` mapping |
| `config/workflow_config.yaml` | YAML | All tunable parameters |
| `docs/data/ep_scan_afternoon.json` | JSON | Afternoon EP scan results |
| `docs/data/ep_scan_morning.json` | JSON | Morning EP scan results |

### Module Layout

- **`config/settings.py`** — centralized paths and env var loading
- **`src/stock_utils.py`** — shared pickle/ticker/file helpers used across modules
- **`src/data_collection/`** — external data: yfinance prices, Finviz fundamentals, barchart breadth
- **`src/indicators/`** — technical indicator calculation and RS_STS% (PERCENTRANK vs SPY)
- **`src/screening/`** — master table generation + 5 screeners in `screeners/` subdir
- **`src/themes/`** — Gemini AI tagging, theme strength scoring, Google Sheets import
- **`src/reporting/`** — daily markdown reports, dashboard JSON export, earnings pivot scanner
- **`docs/`** — GitHub Pages web dashboard (index.html, app.js, style.css + data JSONs)

### Five Screeners (`src/screening/screeners/`)

| Screener | Pattern | ADR | Key Filter |
|----------|---------|-----|------------|
| `steady_trend` | Low-vol uptrend | 2-4% | RS ≥ 90, Close > SMA50 > SMA200 |
| `topdog` | High-ADR momentum | >4% | 96+ percentile from 30-252 lows |
| `gamma` | Short-term burst | ≥4% | 20%+ gain in 30 days |
| `htf` | High Tight Flag | >4% | 150-day 2x range, tight close |
| `darvas` | Extended recovery | ≥4% | 252-day 2x range, near high |

### Theme Scoring Formula

Scoring depends on market regime (bull when MMFI > 50%):
- **Strength** (0-100) = median RS + leader concentration
- **Confirmation** (0-100) = structural health + near-highs + breadth quality
- **Score** = 0.5 × Strength + 0.5 × Confirmation
- **Actionability** = extension penalty × volume bonus
- **Hot threshold**: avg RS_STS% > 70% and breadth ≥ 3 stocks

## Configuration

All workflow parameters live in `config/workflow_config.yaml` (lookback windows, RS thresholds, screener list, scoring coefficients, LLM settings).

Environment variables (`.env`): `GOOGLE_API_KEY` (Gemini), `GOOGLE_SHEET_ID` (theme taxonomy), `ALPACA_API_KEY` + `ALPACA_API_SECRET` (extended-hours volume for RVol), `IBKR_FLEX_TOKEN` (optional).

## CI/CD

Three GitHub Actions workflows, all DST-agnostic via dual-cron + schedule guard (`src/ci/daily_screening_schedule_guard.py`):

| Workflow | Schedule (Pacific) | What it does |
|----------|-------------------|--------------|
| `daily-screening.yml` | 1:15 PM | Full 10-step theme pipeline + Pages deploy |
| `ep-scan-afternoon.yml` | 2:00 PM | AMC earnings scan → JSON + Discord alert + Pages deploy |
| `ep-scan-morning.yml` | 5:45 AM | BMO earnings scan → JSON + Discord alert + Pages deploy |

Each workflow uses two UTC cron entries (PDT + PST) and passes the expected cron pair to the schedule guard via `SCHEDULE_GUARD_PDT_CRON` / `SCHEDULE_GUARD_PST_CRON` env vars. The guard skips the run if the triggering cron doesn't match the current Pacific offset. All workflows support `workflow_dispatch` for manual trigger.

## Tech Stack

Python 3.11+, pandas/numpy/scipy, yfinance, Selenium (breadth scraping), google-genai (Gemini 3 Flash), finvizfinance + BeautifulSoup (fundamentals), Alpaca Market Data API (extended-hours volume). No TA-Lib — all indicators are pure pandas.
