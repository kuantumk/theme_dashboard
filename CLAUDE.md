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
2. **Indicators** pandas-based technicals (no TA-Lib) → `data/price_daily_ta.pkl`
3. **Market Breadth** NCFD/MMFI scraped from barchart.com via Selenium → `docs/data/market_breadth.json`
4. **Master Table** cross-sectional percentile ranks + RS_STS% → `screening_output/master/`
5. **Screeners** 7 pattern filters run in parallel → per-screener CSVs
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
| `data/price_daily_ta.pkl` | Pickle | Price data + technical indicators |
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
- **`src/screening/`** — master table generation + screeners in `screeners/` subdir
- **`src/themes/`** — Gemini AI tagging, theme strength scoring, Google Sheets import
- **`src/reporting/`** — daily markdown reports, dashboard JSON export, earnings pivot scanner
- **`docs/`** — GitHub Pages web dashboard (index.html, app.js, style.css + data JSONs)

### Eight Screeners (`src/screening/screeners/`)

| Screener | Pattern | ADR | Key Filter |
|----------|---------|-----|------------|
| `steady_trend` | Low-vol uptrend | 2-4% | RS ≥ 90, Close > SMA50 > SMA200 |
| `topdog` | High-ADR momentum | >4% | 96+ percentile from 30-252 lows |
| `gamma` | Short-term burst | ≥4% | 20%+ gain in 30 days |
| `htf` | High Tight Flag | >4% | 150-day 2x range, tight close |
| `darvas` | Extended recovery | ≥4% | 252-day 2x range, near high |
| `momentum_136` | 1/3/6-mo leaders | ≥4% | 25%+/50%+/100%+ over 1/3/6mo, $15M dollar vol, 750k shares |
| `parabolic` | Parabolic short watch | ≥4% | $10M dollar vol, price ≥ $5, ATR multiple from 50SMA ≥ 10, no-overlap up candle, volume expansion |
| `vars` | Volatility-adjusted RS leaders | ≥3.3% | $40M dollar vol, 1M shares, price > $2, VARS > 2, VARS 20EMA > 1 |

### VARS — Volatility-Adjusted Relative Strength

`vars` and `vars_20ema` are computed in `create_technical_indicators.py` (Pine Script-derived):
- For each ticker: `norm_change = (close - close[1]) / atr14`
- `vars(ticker) = sum(norm_change[ticker], 100) - sum(norm_change[SPY], 100)` (rolling 100-session sums, `min_periods=1` so recent IPOs still get a value)
- `vars_20ema = ewm(vars, span=20)`

Both legs are normalized by their own ATR before summing, so VARS values are comparable across tickers regardless of underlying volatility. SPY's cumulative series is computed once before the per-ticker loop and reindexed into each ticker.

### Theme Scoring Formula

Scoring depends on market regime (bull when MMFI > 50%):
- **Strength** (0-100) = median RS + leader concentration
- **Confirmation** (0-100) = structural health + near-highs + breadth quality
- **Score** = 0.5 × Strength + 0.5 × Confirmation
- **Actionability** = extension penalty × volume bonus
- **Hot threshold**: avg RS_STS% > 70% and breadth ≥ 3 stocks

### Theme Taxonomy (hierarchical)

`config/theme_taxonomy.yaml` is the canonical taxonomy — a 3-level hierarchy (`L1 / L2 / L3`) where L1 is the trading narrative (e.g. `AI`, `Clean Energy`, `Oil & Gas`). Each ticker in `data/ticker_themes.json` stores 1–3 slash-delimited paths like `"AI / Data Center / Memory"` or `"Space / Launch"`. L2 must be a child of L1; L3 a child of L2.

**Trading-narrative separation**: `Clean Energy` and `Oil & Gas` are sibling L1s, never children of a generic `Energy` node — fuel-cell stocks (BE, FCEL) never share L1 with oilfield-services stocks (PUMP, HAL). The Theme Viz / VARS Viz networks render each L1 as a yellow hexagonal hub; leaf themes connect to their L1 via dashed `is_a` edges.

**Three sources, one canonical form** — and which one wins matters:

| Source | Format today | Authority |
|--------|--------------|-----------|
| `config/theme_taxonomy.yaml` | Tree of `L1 → L2 → L3` | **Schema** — `validate_path` rejects anything not here |
| `data/ticker_themes.json` | `{ticker: ["L1 / L2 / L3"]}` slash-delimited | **Ground truth** for tags |
| Google Sheet (`google_sheet_gid`) | Free-form labels, often legacy (`"AI - Memory & Storage"`, `"Drones"`, `"Solar"`) | **Human curation input only** — never overrides existing canonical tags |

The Sheet is *not* synced to the new taxonomy. Live code translates its labels through `src/themes/legacy_aliases.py` (which re-exports the migration's `OLD_TO_NEW` map) on every import, then validates each result against the taxonomy. Anything that fails to resolve to a valid path is dropped with a `Sheet alias guard dropped` log line. When you spot one of those, add the new alias to `OLD_TO_NEW` in `tools/migrate_themes.py` — don't edit the Sheet to match.

**`apply_google_sheet_ground_truth` defences** (`src/themes/tag_new_tickers.py`):
1. Alias-remap every incoming label via `legacy_aliases.normalize_legacy_theme`.
2. Validate the result against `theme_taxonomy.validate_path`; drop on failure.
3. Honour `git_locked_themes: true` — never overwrite a ticker that already has canonical tags. Brand-new tickers and ones whose only existing tag is `Uncategorized`/`Singleton` *do* accept the sheet's value (so onboarding still works). To re-tag an existing canonical ticker, use the explicit CLI:
   ```bash
   python -m src.themes.retag --ticker NVDA --reason "Announced AI infra pivot"
   ```

**Display-layer defensive parsing** — `theme_taxonomy.resolve_l1(name)` is a total helper used by `export_dashboard_data.py:_attach_hierarchy` / `_build_network` and mirrored in `docs/app.js`'s `l1Of`. It recognises canonical paths, known legacy aliases, and unknown `"L1 - rest"` prefixes where the prefix is still a real taxonomy L1 — so a stray legacy label like `"AI - Some New Concept"` still buckets into the AI hub instead of becoming an orphan node. Strict callers (Gemini validation, retag CLI) should still use `validate_path`; `resolve_l1` is for rendering only.

**Auto-tagging vs locking** — `git_locked_themes: true` originally only short-circuited the 30-day Gemini revalidation loop. After the May 2026 regression (the Sheet sync silently re-introduced 114 legacy labels), the lock now also applies to `apply_google_sheet_ground_truth`. Any future code that mutates `data/ticker_themes.json` (new screener, audit job, etc.) MUST consult this flag before overwriting an existing ticker. The retag CLI is the only sanctioned bypass.

The old `config/theme_groups.yaml` consolidator is archived as `theme_groups.legacy.yaml` and no longer loaded.

### Day-pattern Ticker Coloring

Per-ticker green highlighting in dashboard tables marks entry-ready setups. A ticker turns green only when **(`tight_day` OR `inside_day`) AND `close_to_ma`** all hold for the latest bar:
- `inside_day` = today's high < yesterday's high AND today's low > yesterday's low
- `tight_day` = `|close − open| / close < 0.2 × adr_pct` (fractional body smaller than 20% of ADR%)
- `close_to_ma` = `|close − EMA10| < 0.5 × ATR14` OR `|close − EMA20| < 0.5 × ATR14`

Tickers that fail any condition stay default-colored. Logic lives in `src/reporting/export_dashboard_data.py:load_ticker_color_flags` (main universe) and `fetch_etf_ticker_colors` (ETF tabs, recomputes on-the-fly from yfinance OHLC).

### Dashboard Time Travel

Each tab's session bar shows the last 5 trading days as clickable date buttons plus a `+ older sessions…` dropdown that exposes up to 35 additional days (40 total). `THEMES_HISTORY_MAX = 40` in `export_dashboard_data.py` controls retention.

**Every workflow run produces a fresh 40-session history**: `run_daily_workflow.py` calls `create_master_table.py --days 40` and each `run_screener.py --days 40`, so back-dated master + screener CSVs always carry today's full indicator schema (e.g. when `vars` was added, all 40 dropdown sessions get the new column on the very next workflow run instead of having to wait 40 days). On the export side, `export_momentum_136` / `export_vars` / `export_parabolic` all iterate up to 40 per-day CSVs and rewrite `*_history.json` from scratch each run — no append-only drift. Tabs without per-day source data (themes, industry/leverage ETFs, EP scans) still accumulate one entry per workflow run and reach the 40-cap naturally.

## Configuration

All workflow parameters live in `config/workflow_config.yaml` (lookback windows, RS thresholds, screener list, scoring coefficients, LLM settings).

Environment variables (`.env`): `GOOGLE_API_KEY` (Gemini), `GOOGLE_SHEET_ID` (theme taxonomy), `ALPACA_API_KEY` + `ALPACA_API_SECRET` (extended-hours volume for RVol), `IBKR_FLEX_TOKEN` (optional).

## CI/CD

Three GitHub Actions workflows:

| Workflow | Schedule (Pacific) | What it does |
|----------|-------------------|--------------|
| `daily-screening.yml` | 1:30 PM | Full 10-step theme pipeline + Pages deploy |
| `ep-scan-afternoon.yml` | 2:00 PM | AMC earnings scan → JSON + Discord alert + Pages deploy |
| `ep-scan-morning.yml` | 5:45 AM | BMO earnings scan → JSON + Discord alert + Pages deploy |

`daily-screening.yml` intentionally uses one timezone-aware cron entry (`America/Los_Angeles`) so GitHub creates only one scheduled workflow run at 1:30 PM Pacific. The EP scan workflows still use two UTC cron entries (PDT + PST) and pass the expected cron pair to the schedule guard via `SCHEDULE_GUARD_PDT_CRON` / `SCHEDULE_GUARD_PST_CRON` env vars. The guard skips EP scan runs whose triggering cron doesn't match the current Pacific offset. All workflows support `workflow_dispatch` for manual trigger.

## Tech Stack

Python 3.11+, pandas/numpy/scipy, yfinance, Selenium (breadth scraping), google-genai (Gemini 3 Flash), finvizfinance + BeautifulSoup (fundamentals), Alpaca Market Data API (extended-hours volume). No TA-Lib — all indicators are pure pandas.
