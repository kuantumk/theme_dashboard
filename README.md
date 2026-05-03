# Theme Dashboard

Momentum trading stock screening platform that identifies correlated stock themes ("group moves") for Qullamaggie-style trading. Runs an automated daily workflow: download prices → compute indicators → screen stocks → group into themes → score and report.

Live dashboard: **https://kuantumk.github.io/theme_dashboard/**

## What it does

- **Daily 10-step pipeline** scans ~8000 US stocks, runs 7 technical screeners, classifies tickers into themes via Gemini, scores theme strength + confirmation, and emits a markdown report plus dashboard JSON.
- **Two earnings-pivot scans** (BMO morning, AMC afternoon) flag earnings setups with extended-hours volume confirmation and post Discord alerts.
- **Public dashboard** on GitHub Pages with tabs for macro, themes, momentum, industry/leverage ETFs, EP scanner, and parabolic shorts.

## Quickstart

Requires Python 3.11+.

```bash
# Install
pip install -r requirements.txt

# Configure
cp .env.example .env
# fill in GOOGLE_API_KEY (Gemini), GOOGLE_SHEET_ID (theme taxonomy)
# optional: ALPACA_API_KEY + ALPACA_API_SECRET (extended-hours volume for EP scan RVol)
# optional: IBKR_FLEX_TOKEN (trade log fetching)

# Run the full daily workflow
python run_daily_workflow.py
```

Run individual steps:

```bash
python src/data_collection/download_price_daily.py
python src/indicators/create_technical_indicators.py
python src/screening/create_master_table.py --days 1
python src/screening/run_screener.py --screener steady_trend --days 1
python src/themes/analyze_theme_strength.py
python src/reporting/generate_daily_report.py
python src/reporting/export_dashboard_data.py
```

EP scans (require Alpaca credentials):

```bash
python src/reporting/ep_scan_afternoon.py
python src/reporting/ep_scan_morning.py
```

Test a screener against a single ticker:

```bash
python src/screening/run_screener.py --screener steady_trend --test --ticker AAPL
```

## Pipeline (10 steps)

| # | Step | Output |
|---|------|--------|
| 1 | Download ~8000 stocks × 500d OHLCV via yfinance | `data/price_daily.pkl` |
| 2 | Compute pandas-based technicals (no TA-Lib) | `data/price_daily_ta.pkl` |
| 3 | Scrape NCFD/MMFI breadth from barchart.com via Selenium | `docs/data/market_breadth.json` |
| 4 | Build cross-sectional percentile ranks + RS_STS% | `screening_output/master/` |
| 5 | Run 7 pattern screeners in parallel | per-screener CSVs |
| 6 | Consolidate union of screener tickers | `screening_output/consolidated/` |
| 7 | Pull float / EPS / short% from Finviz (7d cache) | `data/fundamentals.db` |
| 8 | Tag new tickers into themes via Gemini 3 Flash | `data/ticker_themes.json` |
| 9 | Score theme strength + confirmation with regime-aware weighting | dashboard data |
| 10 | Emit markdown daily report | `reports/` |

## Screeners

Seven pattern filters live in `src/screening/screeners/`:

| Screener | Pattern | ADR | Key filter |
|----------|---------|-----|------------|
| `steady_trend` | Low-vol uptrend | 2-4% | RS ≥ 90, Close > SMA50 > SMA200 |
| `topdog` | High-ADR momentum | >4% | 96+ percentile from 30-252 lows |
| `gamma` | Short-term burst | ≥4% | 20%+ gain in 30 days |
| `htf` | High Tight Flag | >4% | 150-day 2x range, tight close |
| `darvas` | Extended recovery | ≥4% | 252-day 2x range, near high |
| `momentum_136` | 1/3/6-mo leaders | ≥4% | 25%/50%/100% gains, $15M dollar vol |
| `parabolic` | Parabolic short watch | ≥4% | $10M dollar vol, ATR multiple ≥ 10 vs 50SMA, volume expansion |

## Theme scoring

Scoring depends on market regime (bull when MMFI > 50%):

- **Strength** (0-100) = median RS + leader concentration
- **Confirmation** (0-100) = structural health + near-highs + breadth quality
- **Score** = 0.5 × Strength + 0.5 × Confirmation
- **Actionability** = extension penalty × volume bonus
- **Hot threshold**: avg RS_STS% > 70% and breadth ≥ 3 stocks

## EP scans (earnings pivot)

Separate from the daily theme pipeline. Two scheduled scans surface earnings-driven setups with short interest and pre/after-hours strength:

- **Afternoon** (2:00 PM Pacific) — Today AMC earnings, short float >10%, avg vol >1M, AH price ≥ close → `docs/data/ep_scan_afternoon.json`
- **Morning** (5:45 AM Pacific) — Today BMO earnings, same filters, PM price ≥ prev close → `docs/data/ep_scan_morning.json`

Shared logic in `src/reporting/ep_scan_common.py`. RVol-at-time uses the Alpaca Market Data API (SIP feed) for 5-min extended-hours bars and computes a cumulative volume ratio vs a 10-session historical baseline; yfinance does not provide usable extended-hours volume at 5m intervals.

## Architecture

```
config/         workflow_config.yaml + theme_groups.yaml + paths
data/           pickles, SQLite fundamentals, JSON metadata
docs/           GitHub Pages dashboard (HTML/CSS/JS + data JSONs)
reports/        daily markdown reports
screening_output/  master tables and per-screener CSVs
src/
  data_collection/  yfinance, Finviz, barchart breadth
  indicators/       technical indicators + RS_STS%
  screening/        master table + screeners/ subdir
  themes/           Gemini tagging, scoring, Sheets import
  reporting/        markdown report, dashboard export, EP scans
  ci/               schedule guard for DST-agnostic CI
tests/          test suite
```

Key data stores:

| File | Format | Content |
|------|--------|---------|
| `data/price_daily.pkl` | pickle | Raw OHLCV history |
| `data/price_daily_ta.pkl` | pickle | Price + technical indicators |
| `data/fundamentals.db` | SQLite | Finviz fundamentals (7-day TTL) |
| `data/ticker_themes.json` | JSON | `{ticker: [theme1, theme2]}` mapping |
| `config/workflow_config.yaml` | YAML | All tunable parameters |
| `docs/data/ep_scan_*.json` | JSON | EP scan results |

## CI/CD

Three GitHub Actions workflows, all DST-agnostic via dual-cron + a Pacific-time schedule guard (`src/ci/daily_screening_schedule_guard.py`):

| Workflow | Pacific schedule | Action |
|----------|------------------|--------|
| `daily-screening.yml` | 1:15 PM | Full 10-step theme pipeline + Pages deploy |
| `ep-scan-afternoon.yml` | 2:00 PM | AMC earnings scan + Discord alert + Pages deploy |
| `ep-scan-morning.yml` | 5:45 AM | BMO earnings scan + Discord alert + Pages deploy |

Each workflow declares two UTC cron entries (PDT + PST) and passes the expected pair to the schedule guard via `SCHEDULE_GUARD_PDT_CRON` / `SCHEDULE_GUARD_PST_CRON`. The guard skips runs whose triggering cron doesn't match the current Pacific offset, so exactly one cron fires per day across DST boundaries. All workflows accept `workflow_dispatch` for manual trigger.

## Configuration

All workflow parameters live in [`config/workflow_config.yaml`](config/workflow_config.yaml) — lookback windows, RS thresholds, screener list, scoring coefficients, LLM settings, EP scan filters.

Environment variables (`.env`):

- `GOOGLE_API_KEY` — Gemini API key for theme tagging
- `GOOGLE_SHEET_ID` — sheet holding the theme taxonomy
- `ALPACA_API_KEY` + `ALPACA_API_SECRET` — extended-hours volume for EP scan RVol
- `IBKR_FLEX_TOKEN` — optional, for IBKR trade log fetching

## Tech stack

Python 3.11, pandas / numpy / scipy, yfinance, Selenium (breadth scraping), google-genai (Gemini 3 Flash), finvizfinance + BeautifulSoup (fundamentals), Alpaca Market Data API (extended-hours volume). No TA-Lib — all indicators are pure pandas.

## License

[MIT](LICENSE)
