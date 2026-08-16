# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Theme Dashboard is a momentum trading stock screening platform that identifies correlated stock themes ("group moves") for Qullamaggie-style trading. It runs an automated daily workflow: download prices → compute indicators → screen stocks → group into themes → score and report.

## Commands

Dependencies are managed with **[uv](https://docs.astral.sh/uv/)**: `pyproject.toml` is the source of truth and `uv.lock` is committed for reproducible installs. Python 3.11 is pinned via `.python-version` and provisioned by uv itself — no system Python required. CI installs with `uv sync --locked`.

```bash
# One-time setup: provision Python 3.11 + create .venv from the lockfile
uv sync
uv run playwright install chromium   # browser binary for market-breadth scraping

# Manage dependencies (each updates pyproject.toml + uv.lock and re-syncs .venv)
uv add <package>            # add a dependency
uv remove <package>         # remove a dependency
uv lock --upgrade           # bump all deps to latest allowed versions, refresh the lock

# Run complete daily workflow (all 10 steps)
uv run python run_daily_workflow.py

# Run individual pipeline steps
uv run python src/data_collection/download_price_daily.py
uv run python src/indicators/create_technical_indicators.py
uv run python src/screening/create_master_table.py --days 1
uv run python src/screening/run_screener.py --screener steady_trend --days 1
uv run python src/themes/analyze_theme_strength.py
uv run python src/reporting/generate_daily_report.py
uv run python src/reporting/export_dashboard_data.py

# Run EP scans standalone (requires ALPACA_API_KEY/SECRET in .env)
uv run python src/reporting/ep_scan_afternoon.py
uv run python src/reporting/ep_scan_morning.py

# Test a screener against a single ticker
uv run python src/screening/run_screener.py --screener steady_trend --test --ticker AAPL

# Mechanical theme-tag audit (exit 1 on [BUG]; also the tag-audit routine's first step)
uv run python tools/audit_theme_tags.py

# Run the unit tests
uv run python -m unittest discover -s tests

# Run scoring backtests
cd tests && uv run python backtest_theme_scoring.py
```

**PR convention** — code-fix PRs do NOT include regenerated `docs/data/*.json` files (the daily workflow rewrites them). After running `uv run python -m src.reporting.export_dashboard_data` locally to verify a fix, reset the noise with `git checkout -- docs/data/` before committing. `screening_output/` is **never committed** — it is per-run local scratch (parquet), regenerated every run and pruned to the newest `screening_output.retention_sessions` (10) per subdir at the tail of `export_all()`.

## Architecture

### Data Flow (10-Step Daily Pipeline)

`run_daily_workflow.py` orchestrates the pipeline by spawning each step as a subprocess:

1. **Download** ~8000 stocks × 500 days OHLCV via yfinance → `data/price_daily.pkl`
2. **Indicators** pandas-based technicals (no TA-Lib) → `data/price_daily_ta.pkl`
3. **Market Breadth** NCFD/MMFI scraped from barchart.com via Selenium → `docs/data/market_breadth.json`; then **step 3b** computes the Nasdaq McClellan Summation Index + RSI(14) → `docs/data/nasi.json` (see NASI below). Both are non-critical — a failure logs a warning and the workflow continues.
4. **Master Table** cross-sectional percentile ranks + RS_STS% → `screening_output/master/*.parquet`
5. **Screeners** pattern filters (listed in `config/workflow_config.yaml`) run in parallel → per-screener `*.parquet`
6. **Consolidate** union all screener tickers (derived from the per-screener parquet) → committed `data/screened_union.json` (`{date, tickers}`, the tag-audit routine's worklist). No `.txt` is written.
7. **Fundamentals** float/EPS/short% from Finviz → `data/fundamentals.db` (SQLite, 7-day cache)
8. **Theme Sync** Google Sheet ground truth + profile-cache warming + untagged surfacing → `data/ticker_themes.json` (no LLM here — classification happens in the weekday tag-audit routine, see Theme Taxonomy below)
9. **Theme Scoring** dual-metric (strength + confirmation) with actionability overlay
10. **Report** markdown daily report → `reports/` (includes "Untagged tickers awaiting audit")

### EP Scan Pipeline (Earnings Pivot Scanner)

Separate from the daily theme pipeline, two workflows scan for earnings-driven setups:

- **Afternoon scan** (`ep-scan-afternoon.yml`, 2:00 PM Pacific) — Finviz screener for Today AMC earnings (short float >10%, avg vol >1M), filters for AH price ≥ close, enriches with RVol/technicals/news → `docs/data/ep_scan_afternoon.json`
- **Morning scan** (`ep-scan-morning.yml`, 5:45 AM Pacific) — same filters for Today BMO earnings, filters for PM price ≥ prev close → `docs/data/ep_scan_morning.json`

Shared logic lives in `src/reporting/ep_scan_common.py`. Key details:
- **RVol at time**: uses Alpaca Market Data API (SIP feed) for 5-min extended-hours bars. Treats 4 AM–8 PM ET as one continuous session, computes cumulative volume ratio vs 10-session historical average. yfinance does NOT provide usable extended-hours volume at 5m intervals.
- **Finviz screener ticker parsing is patched locally** — `scan_finviz_tickers` uses `_TickerRepairOverview`, a `finvizfinance.Overview` subclass that re-reads each ticker from the row markup (`data-boxover-ticker` attribute, falling back to `<a class="tab-link">`). Finviz's **2026-07-15** redesign put a company-logo avatar in the ticker cell whose one-letter fallback `<span>` sits before the symbol; finvizfinance's flat `col.text` parse concatenates the two, so `OKLO` arrives as `OOKLO` — **every ticker gets its first character doubled**. Every downstream Yahoo lookup then 404s and the scan exports zero tickers, which is indistinguishable from a quiet earnings day. This silently blanked the EP tab for the whole Q2 season (68 candidates dropped on 2026-08-07 alone). **Do not "fix" this by un-doubling the first character** — that is a guess that re-corrupts silently the next time Finviz touches the avatar. finvizfinance 1.3.0 (Jan 2026) is the newest release and predates the redesign, so upgrading does not help; re-check on any future release.
- **All-dropped alarm**: when the screener returns candidates but enrichment drops every one, both scans log a `WARNING` and `send_discord_notification` posts a warning instead of "No qualifying tickers found". A 100% drop rate is an upstream-breakage signal, not a quiet day — that ambiguity is what hid the bug above for three weeks.
- **Discord notification**: sends webhook alert with ticker summaries on scan completion.
- **Local diagnostic runs**: both scan scripts accept `--out-dir <path>` (defaults to `docs/data/`) and `--no-discord`. The Windows Task Scheduler launcher `scripts/ep_scan_morning_local.bat` passes `--out-dir scripts/local_runs --no-discord` so local runs write to a gitignored sandbox and never dirty the CI-published `docs/data/ep_scan_*.json` files.

### Tape Pressure Dashboard (`src/bidask/`, local only)

A standalone local app — `scripts/launch_tape_pressure.bat` → `src/bidask/server.py` — that polls quotes, classifies each observation as buyer- or seller-initiated (CLNV-shaped band + tick rule, see `classify.py`), accumulates pressure per ticker since session start, and splits the market into strong-tape / weak-tape columns grouped under the L1/L2 theme taxonomy. Nothing is published to Pages; state goes to the gitignored `scripts/local_runs/`. Requires `TRADINGVIEW_SESSIONID` + `TRADINGVIEW_SESSION_SIGN` in `.env`.

**⛔ The TradingView screener has no `bid`/`ask` for US equities, and asking for them succeeds.** `scanner.tradingview.com/america/metainfo` publishes **3,771 fields and not one is a quote field** (`bid`, `ask`, `bid_size`, `ask_size`, `spread` are all absent). Select one anyway and the API does **not** error — it returns `null` for every row, which is indistinguishable from a missing data entitlement. It is not an entitlement problem: verified on an authenticated `streaming` (real-time) session during market hours, 2064/2064 rows null. The *crypto* scanner genuinely does expose `bid`/`ask` in its metainfo, which is the only reason that tab ever worked.

Equity quotes therefore come from the service TradingView's own web and desktop apps use: the **quote websocket** (`src/bidask/tvquote.py`) at `wss://data.tradingview.com/socket.io/websocket`, authenticated with a JWT minted from the same `sessionid` cookie via `https://www.tradingview.com/quote_token/`. Measured: 296 symbols subscribed in 6 frames, all quoting within **0.4s**, 99% two-sided (the misses are OTC ADRs, which genuinely have no quote).

- **Last price and volume come from the socket too**, not the screener — `merge_quotes` overwrites `close`/`volume` alongside `bid`/`ask`. The classifier compares a trade price against its prevailing quote, so both legs must share one clock; mixing sources adds a second skew term on top of the poll-interval skew `classify.py` already documents as its dominant error.
- **Rows without a quote are kept, never dropped.** They surface as `no_quote` rejections. Dropping them would hide a dead socket behind a quietly shrinking universe.
- **The screener still owns the universe** — liquidity floors, the in-play gate, sector/industry, period highs. It is good at that and it is one request.
- **An empty column must name its own cause** (`emptyReason` in `web/app.js` + the `quotes` block in the state payload). The original bug hid for a full session because the UI said *"No tickers above the current thresholds yet"* — blaming the user's sliders — while 100% of observations were being rejected for want of a quote. A `quotes N/M` pill and the rejection-reason fallback exist so that can't recur.
- `current_session` reports **`market`** during the regular session, not `regular`. Both are mapped in `SESSION_LABELS`; an unmapped value falls through to the raw string and then fails the UI's equality test, styling an open market as delayed.

**⛔ `relative_volume_10d_calc` is not time-of-day adjusted, so a fixed floor on it is a different filter every hour.** It is session-to-date volume divided by the 10-day average **full-day** volume — verified, not assumed: across 59 liquid tickers the implied divisor (`volume / rvol`) matched the true 10-day average daily volume to a median error of **0.02%** (the 30-day average was off by 13.8%). Flooring the raw figure at 1.5 therefore demands **16.3x** normal participation at 09:35, 7.5x at 10:00, 4.8x at 10:30 and 1.8x at 15:00 — strictest exactly when a momentum trader needs it loosest. On 2026-08-14 the raw leg admitted **20 rows out of 2,191**, and FCEL ran +14% on genuinely heavy participation from the open without the volume leg ever firing.

**The gate uses Relative Volume at Time instead — per ticker, against its own history.** `src/bidask/rvol_at_time.py` implements TradingView's Cumulative mode: this ticker's volume since 09:30 over the mean of **its own** 09:30-to-now volume across the last 10 sessions. Both legs are cut at the same time of day, so the comparison is like for like at every moment. An intermediate fix divided the raw figure by a market-wide median volume curve; that is closer but still assumes every ticker shares the market's intraday shape, and a ticker drawing unusual interest is exactly where that assumption fails. Measured on 2026-08-14, the three approaches admit FCEL on the volume leg at **never / 10:00 / 09:35** respectively, while all three correctly reject BE (which ran 0.50–0.73x its usual participation all session despite touching +5%).

- **Floors step up through the session** (`bidask.in_play_rvol_schedule`, `[minutes since 09:30, floor]`): 0.8 from the open, 1.0 at 15 min, 1.2 at 30 min, 1.5 from 60 min on. Early denominators are small and the ratio is noisy, so an unusual reading at 09:35 is worth less than the same reading at 10:30. The first band also covers the minutes before it — a window with no floor would admit the whole universe on the opening print. Both `in_play_min_rvol` and the interim `in_play_min_volume_pace` **raise** rather than being ignored, and a malformed schedule entry raises too: a silently skipped band is a hole in the gate at one time of day only, which is close to invisible from the board.
- **Baselines are built once per session, not per poll.** The historical leg depends only on completed sessions, so `TapeEngine.ensure_profiles` warms it in a background thread (~1.7 min for the ~1,900-ticker universe via yfinance 5-minute bars, no credentials needed) and caches it to `scripts/local_runs/rvol_baselines_<date>.json`. A same-day restart reuses the cache; a cache from another session is **discarded, never reused** — a stale baseline is silently wrong for every ticker rather than visibly absent. Launching pre-open means the warm-up finishes before the bell.
- **A ticker with no baseline scores 0, never 1** — fresh listing, download miss, or warm-up still running. It reaches the board only on the change leg. The `rvol` block in the state payload carries the warm-up status for the same reason the `quotes` block exists: a thin board during warm-up must not read as a quiet market.
- **`relative_volume_intraday|5` exists in the screener metainfo and is deliberately unused.** It may be this figure, but probed after the close it did not behave like a cumulative measure (AVGO read 2.26 against a full-day 1.75, consistent with per-bar "Regular" mode). Validate it against this module during a live session before trusting it. `ep_scan_common.calculate_rvol_at_time` answers the same question from Alpaca at one request per ticker — right for ~30 earnings candidates, wrong for ~1,900 rows.

- **The column cap drops most of the board, so it must say so.** `max_rows_per_column` (60) and `max_rows_per_group` (12) are deliberate, but they are severe: measured 2026-08-14, the strong column rendered **13 of 124** themes and 111 of 367 in-play tickers. `build_columns` returns a `truncated` block per side and the column head shows `shown/total themes · shown/total tickers`, amber once anything is hidden. The server publishes **only the totals**; the `shown` half is counted in the browser after its own sliders, because a server-side count would be pre-slider and would disagree with what is on screen. The no-data branch of `renderColumnMeta` must reset `className` — the tabs share one element, so crypto inherited equity's amber state. `tests/test_bidask_column_meta_markup.py` pins the joins across `index.html` / `app.js` / `style.css`.
- **Group score is the SUM of member margins, so breadth beats intensity.** Spearman(member count, |group score|) = **+0.72**; on 2026-08-14 **0 of 67** single-name strong groups reached the screen, and `Clean Energy / Fuel Cell & Hydrogen` (FCEL, BE) ranked 105th of 124 against a cutoff of 82. A one- or two-name theme is structurally hard to surface however strong its tape. This is a known limitation, not a bug — the truncation counter exists so it is visible rather than silent.
- **The classifier core itself is validated.** Contemporaneous test over 108 polls x 337 in-play tickers: Spearman(imbalance, same-window return) = **+0.305** (p = 1.1e-08), sign agreement 67.7%, monotone quintiles from −0.06% to +0.30%. When the board looks wrong, suspect the gate, the ranking or the cap before the CLNV logic.
- **Raw `margin` is dwell-biased across a full session.** It counts polls, so a ticker in play since 09:35 accumulates ~1,300 observations against ~100 for one admitted at 15:00. On the live all-day state Spearman(total_hits, |margin|) = +0.61; over a uniform-dwell window it drops to −0.15, which is what identifies dwell as the cause. `imbalance` is the scale-free counterpart and is already computed. Accumulation is also cumulative since the open with no decay, so by mid-afternoon a morning move is washed out.

### Key Data Stores

| File | Format | Content |
|------|--------|---------|
| `data/price_daily.pkl` | Pickle (dict of DataFrames) | Raw OHLCV history |
| `data/price_daily_ta.pkl` | Pickle | Price data + technical indicators |
| `data/fundamentals.db` | SQLite | Finviz fundamentals with 7-day TTL |
| `data/ticker_themes.json` | JSON | `{ticker: [theme1, theme2]}` mapping |
| `data/screened_union.json` | JSON | `{date, tickers}` — latest screened union; the tag-audit routine's worklist (committed) |
| `screening_output/**/*.parquet` | Parquet | Per-run master + per-screener numeric outputs (local scratch; regenerated each run, never committed, pruned to newest 10) |
| `config/workflow_config.yaml` | YAML | All tunable parameters |
| `data/nasdaq_ad_history.json` | JSON | `{date: {advances, declines}}` — cached Nasdaq A/D counts (committed; each run refreshes a trailing **90 calendar days** (~62 sessions, `REFRESH_DAYS`) so late-reported sessions self-correct and the multi-year history is paid for once) |
| `docs/data/nasi.json` | JSON | Nasdaq McClellan Summation Index + RSI(14), current + 378-session (~18 month) retained history. The chart plots only the newest 252 (~1 year); the rest is headroom (see NASI below). Seeded in git so the panel renders before the first workflow run; rewritten every run thereafter |
| `docs/data/ep_scan_afternoon.json` | JSON | Afternoon EP scan results |
| `docs/data/ep_scan_morning.json` | JSON | Morning EP scan results |

### Module Layout

- **`config/settings.py`** — centralized paths and env var loading
- **`src/stock_utils.py`** — shared pickle/ticker/file helpers used across modules
- **`src/data_collection/`** — external data: yfinance prices, Finviz fundamentals, barchart breadth
- **`src/indicators/`** — technical indicator calculation and RS_STS% (PERCENTRANK vs SPY)
- **`src/screening/`** — master table generation + screeners in `screeners/` subdir
- **`src/themes/`** — Sheet ground-truth sync (`tag_new_tickers.py`), retag CLI, theme strength scoring, Google Sheets import
- **`src/reporting/`** — daily markdown reports, dashboard JSON export, earnings pivot scanner
- **`docs/`** — GitHub Pages web dashboard (index.html, app.js, style.css + data JSONs)
- **`docs/solutions/`** — documented solutions to past problems (bugs, architecture patterns, conventions), organized by category with YAML frontmatter (`module`, `tags`, `problem_type`). Relevant when implementing or debugging in an area one of them covers — several record vendor-API traps that cost a full session to diagnose the first time.
- **`CONCEPTS.md`** — shared domain vocabulary (themes and taxonomy levels, screening artifacts, metrics, breadth, tape pressure). Relevant when orienting to the codebase or when a term's project-specific meaning is load-bearing.

### Screeners (`src/screening/screeners/`)

The daily workflow runs the screeners listed under `screeners:` in `config/workflow_config.yaml`:

| Screener | Pattern | ADR | Key Filter |
|----------|---------|-----|------------|
| `steady_trend` | Low-vol uptrend | 2-4% | RS ≥ 90, Close > SMA50 > SMA200 |
| `topdog` | High-ADR momentum | >4% | 96+ percentile from 30-252 lows |
| `gamma` | Short-term burst | ≥4% | 20%+ gain in 30 days |
| `htf` | High Tight Flag | >4% | 150-day 2x range, tight close |
| `darvas` | Extended recovery | ≥4% | 252-day 2x range, near high |
| `momentum_136` | 1/3/6-mo leaders | ≥4% | 25%+/50%+/100%+ over 1/3/6mo, $15M dollar vol, 750k shares |
| `parabolic` | Parabolic short watch | ≥4% | $10M dollar vol, price ≥ $5, ATR multiple from 50SMA ≥ 10, no-overlap up candle, volume expansion |
| `vars` | Volatility-adjusted RS leaders | ≥2% | Config-driven (`vars_screener:` block): $40M dollar vol, 1M shares, price > $2, VARS > 2 |
| `volspike` | Highest-volume spike | ≥4% | `days_since_highest_volume` ≤ 30, `up_dollar_vol_max` ≥ $40M, `vol_sma50` ≥ 1M, price ≥ $2, close ≥ SMA200 |
| `denvol` | Dense up-volume breakout | ≥4% | `up_dollar_vol_max` ≥ $40M, volume ≥ 300k, price ≥ $2, close ≥ SMA200, and the highest-volume day (within 30 days) is also the highest up-dollar-volume day |

**Volume-spike indicators** (`create_technical_indicators.py`, **trailing 365-calendar-day** rolling window via `VOL_SPIKE_WINDOW = '365D'`, point-in-time safe): `highest_volume` (today's volume is the highest in the trailing year), `days_since_highest_volume` / `days_since_vol_max`, `up_dollar_vol_max` (trailing-1yr max of signed dollar volume), `days_since_up_vol_max`. **Do not use a full-download / expanding window** — a stock's record bar from >1 year ago would suppress a fresh in-window spike (the FLNC bug). The 365-day window + the ≤30-day recency gate in the screeners surfaces both fresh all-time highs and recent 1-year highs (e.g. NVDA `days_since` went 403→8 after the fix).

**Volume / Volume Viz dashboard tabs** show the **union of `volspike` + `denvol`**, grouped by leaf theme and ranked by avg VARS (flat per-leaf tables — deliberately NOT the L1-clustered layout of the VARS tab; each ticker carries a `scan` tag of `volspike`/`denvol`/`both` + `days_since_hv`). They replaced the former **Coiled / Coiled Viz** tabs. Export: `export_volume` / `_build_volume_snapshot` in `export_dashboard_data.py` → `docs/data/volume.json` + `volume_history.json`.

**VARS / VARS Viz dashboard tabs** show the `vars` screener's survivors as **leaf tables clustered under taxonomy L1 sections** (one section per trading narrative, e.g. all five Cybersecurity leaves inside one "Cybersecurity" block). L1 score = mean of the top-`vars_tab.top_k_vars` (5) member VARS values; L1 sections sort by (score desc, avg `vars − vars_20ema` acceleration desc); an L1 gets a **HOT** badge when avg member RS ≥ `vars_tab.hot_rs_threshold` (70) with ≥ 3 members. The `vars_tab.min_tickers_per_l1` (3) minimum applies at the **L1** level with **no per-leaf minimum** — a 2-member leaf like `Cybersecurity / Endpoint` (CRWD, S) renders inside its L1 section (the pre-2026-07 flat layout's per-leaf min-3 structurally hid such leaves). Screener gates live in the `vars_screener:` config block; the ADR floor is **2%**, not the momentum screeners' 3.3–4%, because VARS is already ATR-normalized and a higher floor ejects liquid large-cap leaders exactly when post-run tightening compresses their ADR (PANW/FTNT, July 2026). Ticker rows carry ▲/▼ acceleration badges (vars vs 20EMA). The snapshot's `network` payload stays **leaf-level**; `varsVizSnap` in `docs/app.js` flattens L1-clustered snapshots for the shared network viz, and `renderVARS` falls back gracefully on legacy flat history entries. Export: `export_vars` / `_build_vars_snapshot` → `docs/data/vars.json` + `vars_history.json`.

`coiled_theme` (`src/screening/coiled_theme.py` scoring module + `screeners/coiled_theme.py`) is **retained as a standalone screener but is no longer in the daily workflow** (dropped from the `screeners` config list, and `add_coiled_theme_metrics` is no longer called in `create_master_table.py`, so `coiled_theme_score`/`coiled_is_candidate`/`coiled_flags` are not in the master table). Run it manually via `run_screener.py --screener coiled_theme`; it computes its score on demand from the precomputed setup-feature columns.

### VARS — Volatility-Adjusted Relative Strength

`vars` and `vars_20ema` are computed in `create_technical_indicators.py` (Pine Script-derived):
- For each ticker: `norm_change = (close - close[1]) / atr14`
- `vars(ticker) = sum(norm_change[ticker], 100) - sum(norm_change[SPY], 100)` (rolling 100-session sums, `min_periods=1` so recent IPOs still get a value)
- `vars_20ema = ewm(vars, span=20)`

Both legs are normalized by their own ATR before summing, so VARS values are comparable across tickers regardless of underlying volatility. SPY's cumulative series is computed once before the per-ticker loop and reindexed into each ticker.

### NASI — Nasdaq McClellan Summation Index + RSI(14)

`src/indicators/nasdaq_mcclellan.py` (pure math) + `src/data_collection/compute_nasi.py` (collection). Rendered in the Overview tab's **Market Breadth & Sentiment** card, under the breadth tiles. RSI(14) of the summation index reaching ≈10 has marked major Nasdaq lows (2024-04-17, 2025-11-17, 2026-07-30).

```
RANA       = 1000 * (advances - declines) / (advances + declines)
oscillator = EMA19(RANA) - EMA39(RANA)      # recursive, adjust=False
summation  = cumsum(oscillator)
rsi        = Wilder RSI(14) of summation
```

**There is no free feed for `$NASI`, and no canonical one either.** The exchanges do not publish advance/decline data at all — every vendor computes its own from a private issue list, so values legitimately differ between providers. StockCharts is JS-walled; Barchart serves `$NCFD` but 404s on `$NASI`/`$NAMO`/`$NAAD`; stooq blocks. Computing our own is the only sustainable path. Do not add a scraper for this.

**⛔ The issue universe must include ETFs — this is the whole ballgame.** `select_universe` deliberately does **not** use `stock_utils.get_tickers_from_nasdaq()`, which drops ETFs, warrants, units, rights and ETNs because the *screeners* must never trade them. Breadth is the opposite problem: Nasdaq's own market diary counts every listed security. Excluding ETFs under-counts decliners in a selloff and biases the series upward — measured on 2026-07-30, an operating-companies-only universe reads RSI **13.06** where an all-issues universe reads **9.97** (StockCharts: 8.85). Since the oversold line is 10, that one exclusion is the difference between seeing the signal and missing it entirely. Consequently `price_daily.pkl` **cannot** be reused as the breadth source, and step 3b keeps its own universe and its own cached A/D history.

**The summation *level* is meaningless; only its shape and its RSI are portable.** The index is a running total with an arbitrary origin, so ours reads ≈ −6,400 where StockCharts reads −139 for the same session. The dashboard therefore shows the **oscillator** and the **RSI** as figures (both vendor-comparable: ours +29.03 / 9.89 vs their +27.91 / 8.85 on those dates) and plots the summation for shape only. Never surface the summation level as a headline number — it will never match any chart a user compares against. RSI is immune to the origin because `diff(summation) == oscillator`; `tests/test_nasdaq_mcclellan.py` pins this invariance.

**The rule covers the crosshair too, which is where it is easiest to break.** Hovering the chart draws a vertical line at the nearest session and reports that session's date, oscillator and RSI beneath it — the same pair the header shows, at the hovered date. Printing the value of the white summation line you are hovering is the obvious-looking change and it is wrong for the reason above; `tests/test_nasi_crosshair_markup.py` pins it, because nothing on screen tells the next editor. That test also pins the two non-obvious mechanics: the crosshair `<line>` is created **inside `renderNasiChart`** (which resets `svg.innerHTML`, so a line appended anywhere else dies on the next panel drag or window resize), and the SVG needs `pointer-events: all` because the plotted paths are `fill: none` and the two panes are separated by a 14px gap.

**The hovered oscillator is derived client-side, not read from the file.** `docs/data/nasi.json` history points carry only `date` / `summation` / `summation_ma` / `rsi`; `oscillator` exists on `current` alone, so `pt.oscillator` is silently `undefined`. `deriveNasiOscillator` in `docs/app.js` recovers it as `summation[i] − summation[i−1]` — exact by construction, since the summation is its running total — rounded to 2dp, or float noise shows `24.849999…` where the header shows `24.85`. Deriving rather than adding a field to `compute_nasi.py` is a **deployment-lag** decision, not a correctness one: code PRs reset `docs/data/`, so a new field would render as an em dash until the next daily workflow run. The derivation runs on the **whole payload** and only then gets sliced to the plotted window, so every plotted session keeps a real oscillator; the `—` at index 0 appears only when the payload is no longer than the window itself.

**RSI near the floor is provider-specific and hypersensitive.** At a trough the average-gain term is only a Wilder-decayed remnant of the *previous rally* (in July 2026 every contributing up-day fell in 6/26–7/09; from 7/13 there were 13 straight down days), while the average-loss term is large. Small coverage differences therefore swing RSI by points — issue-universe choice alone moved RSI(2026-07-30) across 12.30–22.97 before the ETF fix. Treat `NASI_OVERSOLD = 10` in `docs/app.js` as calibrated to *our* series, not as a universal constant, and re-verify it if the universe rule changes.

**The pane marks both ends, and both are *level* tests — never crossing tests.** An amber rail sits at `NASI_OVERSOLD` (10) and another at `NASI_OVERBOUGHT` (80); every session at or beyond either level gets a marker, green below and red above. Marking only the bar that crosses into a band is the obvious-looking change and it is wrong: the panel reports a **phase**, so a band has to read as a run. In the plotted year 18 consecutive sessions sat at or above 80 where a crossing test draws one marker. Adjacent sessions are ~1.35 CSS px apart against a 4 CSS px marker, so a run renders as one band with rounded ends — that is the intended read, not a rendering bug. **Rails are amber on both sides and never the marker colour**: the 80 rail lands at y 116 and the markers there cover y 110.9–117.5 (centres 112.9–115.5, plus `ry: 2`), so a red rail would sit under the red markers it labels and the pair would read as one thickened line. `tests/test_nasi_crosshair_markup.py` pins the level rule, the rail colour, and the single-loop structure.

**`NASI_OVERBOUGHT = 80` is a convention, not a calibrated level.** Unlike 10 — checked against StockCharts `$NASI` in August 2026 — 80 has never been compared against an external reference. It came in as a header tint and is now a plotted rail, which lends it visual authority it has not earned. The hypersensitivity above cuts both ways; if the issue universe changes, re-verify 80 the same way, and do not treat it as portable from a vendor chart.

**The chart plots 252 sessions; the export keeps 378.** The window is applied client-side in `loadNasiData` (`NASI_CHART_SESSIONS`), not in `compute_nasi.py` — same deployment-lag reasoning as the derived oscillator: code PRs reset `docs/data/`, so trimming `EXPORT_SESSIONS` would leave the deployed chart at 18 months until the next daily workflow run. The extra sessions are retention headroom. **Slice in exactly one place, before `nasiHistory` is assigned.** Four call sites share that index space — `renderNasiChart` plots at it, `nasiIndexAt` and `initNasiCrosshair` divide by it, `showNasiReadout` indexes into it — and the first and last take arguments rather than reading the module variable, so leaving either on the fetched payload plots 378 sessions against 252-session pointer math. Nothing on screen shows it; the crosshair just names a neighbouring session. Two consequences of the narrower window are accepted: the summation pane's y-range rescales (it is computed from the visible points), and four of the five overbought runs plus two of the five dated `NASI_LOW_BAND` lows fall outside it. Of the three lows that remain, only 2026-03-30 (10.95) bottoms inside the 10–12 band, so the dashed 12 rail now labels one visible low.

### Theme Scoring Formula (screened lens — daily report only)

Two-track, implemented in `src/themes/analyze_theme_strength.py` (the earlier strength/confirmation/actionability/regime formula no longer exists in code). This lens feeds only the report's "Market Themes" section — the dashboard Themes tab is the L1 Radar below, and the legacy `themes.json`/`themes_history.json` exports are retired:
- **Universe per theme** = tagged members ∩ that day's screened union ∩ master table; min scored breadth 2
- **Composite (per ticker)** = mean(`rs_sts_pct` clipped 0-100, raw VARS as-is) — missing legs → 50
- **Theme score** = mean of the top-10 member composites
- **Demand (per ticker)** = mean(short-interest score, float score via config anchors) — controls within-theme display order only, never the score
- **Hot threshold**: avg RS_STS% > 70% and breadth ≥ 3 stocks

### L1 Radar (screener-independent lens)

`src/themes/l1_score.py` scores fixed theme baskets daily over **all** tagged tickers (no screener gate) and rolls leaves up to their taxonomy L1, so slowly-strengthening L1s (e.g. cybersecurity on 2026-07-13, one session pre-breakout) surface before members pass momentum screeners. Tunables live in the `radar:` block of `config/workflow_config.yaml`:
- **Universe**: every `data/ticker_themes.json` ticker present in the master table with close ≥ `min_close` ($3), 20d avg dollar vol ≥ `min_avg_dollar_vol` ($10M), and 50d avg share volume `vol_sma50` ≥ `min_avg_volume` (750k) — the share floor is **waived** at ≥ `min_avg_volume_dollar_exempt` ($40M) avg dollar vol, because a share floor stacked on the dollar floor can only eject high-priced names and must not blind the radar to STRL/CRS/ARGX-class liquid leaders (same lesson as the VARS ADR floor; assessment 2026-07-24: 1M rejected — it flipped the #1 L1 and killed 11 leaves). NaN `vol_sma50` (IPOs <25 sessions) passes through on dollar-floor-only gating. No fundamentals (unscreened members have none)
- **Composite (per ticker)** = 0.5·`rs_sts_pct` + 0.5·(VARS percentile across **all tagged tickers** in the master table, anchored before the price/liquidity floors, invariant to floor changes) — both legs 0-100, missing → 50. The former 0.2 `rela_perf_1mo_rank` fast leg is zero-weighted since the 2026-07 backtest (Spearman +0.81 vs the rs leg, no incremental IC — tests/RADAR_BACKTEST_FINDINGS.md §3-4). Raw VARS is never averaged with 0-100 legs here (scale mismatch)
- **Leaf raw** = mean of top-5 member composites (min breadth 2), then **z-scored across all scored leaves** (session-relative, decompresses ranks)
- **L1 raw** = mean of its top-5 leaf z-scores. L1s with ≥ 2 scored leaves earn **boost = β·l1_raw** (β = 0.3), added to every leaf (`boosted = z + boost`) and to the L1 itself (`boosted = l1_raw + boost`) — co-firing sub-themes lift the whole L1 (sibling confirmation). Single-leaf L1s get no self-boost; negative l1_raw boosts negatively (symmetric)
- **Ranks**: leaves ranked globally by boosted score across all L1s; L1s by boosted score. No top-N cap at scoring level
- **Flows**: step 9b of `run_daily_workflow.py` computes the radar in-process → "📡 L1 Radar" report section (above Market Themes; ignored by `parse_report`). `export_radar` in `export_dashboard_data.py` rebuilds `docs/data/radar.json` (current, uncapped — every scored L1, leaf, **and member ticker**) + `radar_history.json` (180-day window, per-entry L1s capped at `history_l1_limit` and chips at `tickers_per_leaf`, compact JSON) from the per-day master parquet — it runs inside `export_all()` **before** `prune_screening_output`. Only the newest session is built uncapped (`_build_radar_snapshot(..., tickers_per_leaf=None)`); history entries stay capped because that file already carries ~124 sessions at ~15 MB. The dashboard **Themes** tab renders the radar as one block per leaf — a metadata line (global rank, name, `N=`, raw → boosted) above a **full-width wrapping chip row** (screened members highlighted, unscreened dimmed), clamped to `--radar-chip-rows` (2) lines with a measured `+N more` toggle. It is deliberately **not** a table: five columns in a ~250px list panel pushed raw/boosted off-screen and clipped the ticker list. `syncRadarClamps` in `docs/app.js` measures chip `offsetTop` against the clamp — it must run whenever the panel's width changes or the tab first gains a box, so it is called from the tab switch, the resize-handle drag, `window.resize`, and a `ResizeObserver` (the observer alone is not sufficient: it only delivers inside the rendering lifecycle). The **Theme Viz** network is fed from the same radar snapshots via the client-side `radarVizSnap` adapter in `docs/app.js` (leaf `score` = mean member composite, `avg_rs` = mean member RS; hot filter avg RS ≥ 70, breadth ≥ 3 unchanged) — it averages only the leading `RADAR_VIZ_MEMBERS` (10, matching `tickers_per_leaf`) members so uncapped current snapshots and capped history entries score identically
- **Acceptance check**: `uv run python tools/validate_radar.py --episodes tools/radar_episodes.yaml` — three forward-return-verified early-call episodes (single-date form: `--date ... --expect-l1 ... --max-rank N`; add `--sweep` with a local master parquet to grid-search β/top-K/top-M/fast-weight). The original 2026-07-13 Cybersecurity≤3 case is retired — it fails (rank 6) under every tag vintage, see tests/RADAR_BACKTEST_FINDINGS.md §5

### Theme Taxonomy (hierarchical)

`config/theme_taxonomy.yaml` is the canonical taxonomy — a 3-level hierarchy (`L1 / L2 / L3`) where L1 is the trading narrative (e.g. `AI`, `Clean Energy`, `Oil & Gas`). Each ticker in `data/ticker_themes.json` stores 1–3 slash-delimited paths like `"AI / Data Center / Memory"` or `"Space / Launch"`. L2 must be a child of L1; L3 a child of L2.

**Vocabulary — one concept, one name.** **L1** = top hierarchy level (trading narrative, e.g. `AI`); **L2** = sub-segment (`Data Center`); **L3** = specialty (`Memory`); a **leaf** = a full slash path as stored in `ticker_themes.json` (the scoring unit). The radar's per-L1 aggregate is the **L1 score**. The retired synonyms "ecosystem" (radar layer, inherited from the competitor implementation the radar was decoded from) and "family" (VARS-tab layer) must not be reintroduced in code, config, UI, or docs. Persisted-schema note: `radar.json`/`radar_history.json` carry the L1 list under the `l1s` key; entries written before the 2026-07 consolidation used `ecosystems`, and `docs/app.js` + `tools/validate_radar.py` keep a one-line legacy shim until the daily workflow has rewritten both files.

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
3. Honour `git_locked_themes: true` — never overwrite a ticker that already has canonical tags. Brand-new tickers and ones whose only existing tag is `Uncategorized`/`Singleton` *do* accept the sheet's value (so onboarding still works). To re-tag an existing canonical ticker, use the explicit CLI (`--paths` is required — the caller supplies the judgment):
   ```bash
   uv run python -m src.themes.retag --ticker NVDA --reason "Announced AI infra pivot" \
     --paths "AI / Data Center / Cloud & Hyperscalers"
   ```

**Display-layer defensive parsing** — `theme_taxonomy.resolve_l1(name)` is a total helper used by `export_dashboard_data.py:_attach_hierarchy` / `_build_network` and mirrored in `docs/app.js`'s `l1Of`. It recognises canonical paths, known legacy aliases, and unknown `"L1 - rest"` prefixes where the prefix is still a real taxonomy L1 — so a stray legacy label like `"AI - Some New Concept"` still buckets into the AI hub instead of becoming an orphan node. Strict callers (the retag CLI, audit tooling) should still use `validate_path`; `resolve_l1` is for rendering only.

**Bare-L1 paths: when valid** — A one-segment path like `"Quantum Computing"` is valid *only* for L1s with no children in `theme_taxonomy.yaml` (currently `Quantum Computing` and `Singleton`). For any L1 with children — `Space`, `Cybersecurity`, `Nuclear`, `AI`, etc. — a bare-L1 path is a **tagging bug**, even though `validate_path` accepts it (the validator returns True whenever L2 is `None`, regardless of whether the L1 has children). Symptom: the network viz renders an orphan L2 circle with the same label as the L1 hexagon hub (two "Space" nodes). Cytoscape doesn't error because front-end IDs are prefixed (`l1::Space` vs `theme::Space`).

When auto-tagging, retagging, or hand-editing an L1-with-children ticker, the classifier MUST pick an L2. If no existing L2 fits, add one to `theme_taxonomy.yaml` or fall back to `Singleton`. **Never write a bare-L1 path for an L1 with children.** Audit existing tags with:

```python
from src.themes.theme_taxonomy import load_taxonomy, _children, split_path
tax = load_taxonomy()
for ticker, paths in ticker_themes.items():
    for p in paths:
        l1, l2, _ = split_path(p)
        if l2 is None and _children(tax.get(l1, {})):
            print(f"BUG: {ticker} bare-L1 {p!r} but {l1} has children")
```

`_build_network` (`src/reporting/export_dashboard_data.py`) and the matching loop in `docs/app.js` drop the duplicate leaf as a defensive backstop — but it's just rendering hygiene. Fix the tag, don't rely on the guard.

**Weekday tag-audit routine (all LLM tagging lives here)** — the daily pipeline does no LLM classification. A Claude Code cloud routine (weekdays 5:30 PM Pacific, Sonnet 5) executes `.claude/routines/theme_tag_audit.md`: sync main → run the `audit-theme-tags` skill in full (fix `[BUG]`s, web-verified narrative-shift corrections, classify every `[UNTAGGED]` ticker, capped Singleton rescue, capped basket densification of the top radar L1s — cross-listing dual-role names + filling pure-play roster gaps) → if tag files changed, branch `theme-tags/YYYY-MM-DD` → commit → PR → squash-merge → delete branch; otherwise report no-op. The 5:30 PM slot sits after the daily workflow's results commit (observed landing 4:01–4:57 PM PT), so the routine tags the same day's discoveries and the next 1:30 PM run scores them — a new ticker is themeless in the report for at most one session. All writes go through the retag CLI.

**Audit tooling** — `uv run python tools/audit_theme_tags.py` (exit 1 on `[BUG]` findings only) covers the mechanical checks plus the `[UNTAGGED]` worklist (`data/screened_union.json` — the committed `{date, tickers}` the daily workflow writes — diffed against `data/ticker_themes.json` via `theme_registry.is_untagged`: missing/empty/`Uncategorized`-only; `Singleton`-only excluded). The `audit-theme-tags` skill (`.claude/skills/audit-theme-tags/SKILL.md`) wraps it with the AI-judgment phases — it's both the routine's playbook and the interactive one.

**Auto-tagging vs locking** — `git_locked_themes: true` originally short-circuited the (since-removed) 30-day Gemini revalidation loop. After the May 2026 regression (the Sheet sync silently re-introduced 114 legacy labels), the lock also applies to `apply_google_sheet_ground_truth`. Any future code that mutates `data/ticker_themes.json` (new screener, audit job, etc.) MUST consult this flag before overwriting an existing ticker. The retag CLI is the only sanctioned bypass — used by humans and by the tag-audit routine alike.

The old `config/theme_groups.yaml` consolidator is archived as `theme_groups.legacy.yaml` and no longer loaded.

### Day-pattern Ticker Coloring

Per-ticker green highlighting in dashboard tables marks entry-ready setups. A ticker turns green only when **(`tight_day` OR `inside_day`) AND `close_to_ma`** all hold for the latest bar:
- `inside_day` = **candle engulfed OR body engulfed** — `(high ≤ prev_high AND low ≥ prev_low)` OR `(max(open, close) ≤ max(prev_open, prev_close) AND min(open, close) ≥ min(prev_open, prev_close))`
- `tight_day` = `|close − open| / close < 0.2 × adr_pct` (fractional body smaller than 20% of ADR%)
- `close_to_ma` = `|close − EMA10| < 0.5 × ATR14` OR `|close − EMA20| < 0.5 × ATR14`

Tickers that fail any condition stay default-colored. Logic lives in `src/reporting/export_dashboard_data.py:load_ticker_color_flags` (main universe) and `fetch_etf_metrics` (ETF tabs, recomputes color + VARS on-the-fly from yfinance OHLC).

**Both `inside_day` clauses are inclusive, and there is exactly one implementation.** The earlier strict form (`high < prev_high AND low > prev_low`) rejected a bar that *ties* the prior high or low, and rejected a tight-bodied bar whose wicks poke outside the prior range — both of which are the coiled setups this colouring exists to surface. Body extremes use `max`/`min` of (open, close) so the comparison is direction-agnostic: a red previous bar reads the same as a green one, and comparing open-to-open instead would flip that case. The definition lived in two places — vectorized in the indicator pipeline and scalar in the ETF recompute — and was one edit away from drifting; both now call `create_technical_indicators.compute_inside_day`, the same sharing pattern `compute_spy_cum_norm_100` uses across those two modules. `tests/test_inside_day.py` pins a property that the old definition implies the new one, so the rule can never silently narrow. The widening is strictly looser, so it colours *more* tickers green; `coiled_theme` (standalone, not in the daily workflow) consumes the same column and scores slightly differently as a result — that is intended, not drift to "fix".

### Dashboard Panel Layout

Every tab except the Viz networks is a two-pane split: **chart on the left, ticker list on the right**, with a drag handle between them.

**`.left-panel` and `.right-panel` name DOM position, not screen position.** The DOM order is still list → handle → chart; the visual swap is three `order` declarations in `style.css` (`.right-panel` 1, `.resize-handle` 2, `.left-panel` 3). Read `.left-panel` as *the list pane* and `.right-panel` as *the chart pane* — including in older comments in this file that predate the swap. Renaming them means editing 13 near-identical tab blocks in `index.html` plus every CSS rule and JS query, so it is a deliberate follow-up, not drift.

Two consequences fail silently and are pinned by `tests/test_dashboard_panel_layout.py`:

- **`initResizablePanels` subtracts the pointer delta** (`startWidth - dx`). The handler sizes the list pane; with it left of the handle `+ dx` was right, but from the right, dragging right must *shrink* it. A `+` makes the divider run away from the pointer — it reads as a broken handle rather than a sign error, and no screenshot shows it.
- **The `max-width: 1100px` block resets `order` on all three children.** Stacked vertically, a left/right decision would become chart-above-list and bury the list you scroll to pick a ticker from. Mobile keeps list-above-chart, and the handle stays hidden.

### V / A Filter Cutoffs

Two dropdowns sit at the right edge of the time-travel bar on the five stock-list tabs — **Themes, VARS, Momentum, Volume, Parabolic**. `V` dims tickers whose 20-day average dollar volume (`avg_dollar_vol`) is below the selected cutoff — **$10M / $50M / $100M, default $50M**. `A` dims tickers whose 20-day `adr_pct` is below **2.5% / 3% / 3.5% / 4% / 4.5% / 5%, default 4%**. A ticker failing either is dimmed (union). The Viz, Industry/Lev ETF, and EP tabs deliberately carry no cutoffs (network rendering path; ETF rows come from an on-the-fly yfinance fetch, not the screening parquet; EP already screens on avg vol > 1M upstream).

- **There is no off position.** These are cutoffs, not toggles: the dashboard dims against $50M / 4% from first paint and the only choice is how tight. State is two numbers in `tickerFilters` (`docs/app.js`), shared across tabs like `activeSessionDate`, and resets to the defaults on reload — the repo uses no browser storage.
- **`V` reads dollar volume, not share volume.** It cut `vol_sma50` at a fixed 1M while it was an on/off toggle; as a user-chosen cutoff it reads the column every screener and the L1 Radar universe floor already gate on, because 1M shares means something different at $4 than at $400. This deliberately reverses the older "straight share-volume cut, do not harmonize" rule. The radar's universe floor is still a **separate** decision and keeps its own 750k share leg with the ≥ $40M dollar-volume waiver — that waiver exists so *scoring* is not blinded to high-priced liquid leaders, which is not what a view lens does. Don't collapse the two.
- **View-level only.** Filtering never re-scores, re-ranks, re-sorts, or removes anything — it toggles a `filtered-out` class on already-rendered elements (`applyTickerFilters` in `docs/app.js`). Leaf `N=`, L1 scores, row order, and the radar's `+N more` count are identical at every cutoff.
- **Never give `.filtered-out` a layout-affecting property** (notably `font-weight`). Chip width drives how radar chip rows wrap, and `syncRadarClamps` measures that wrapping to derive the `+N more` count — a width change would silently move that count whenever a cutoff changes.
- **Missing metric fails open** (not dimmed), matching `build_radar_universe`'s NaN handling. The snapshot builders `.fillna(0)`, so `filter_metrics` maps 0 to `None` as well — otherwise every ticker missing the metric would read as illiquid. This matters more than it did under the toggles: the filters are always on, so a metric reading as 0 would dim on first paint with no user action to undo it.
- **Arrow keys skip dimmed tickers.** `ArrowUp`/`ArrowDown` travel between tickers that pass the cutoffs, stepping over any number of consecutive dimmed ones, and stay put rather than selecting a dimmed ticker at either end. The scan walks the *full* link array (`nextVisibleIndex` in `docs/app.js`) so `navIndices` keeps meaning the same thing at any cutoff — the click-sync handler writes into that same index space with `links.indexOf(link)`, and filtering the array instead would silently repoint the selection whenever a cutoff changed. `link.closest('.filtered-out')` is the single dimmed test: the class lands on the `<tr>` for table tabs and on the chip itself for radar chips, and `closest` matches self-or-ancestor. A dimmed ticker stays clickable — only keyboard travel skips it.
- **Deployment lag applies to `V` only.** The metrics ship as `dollar_vol` / `adr_pct` per ticker in `radar.json`, `vars.json`, `volume.json`, `momentum_136.json`, and `parabolic.json` (plus their `*_history.json`). `adr_pct` predates this change and is already published, so **the `A` cutoff bites the moment the code deploys**; `dollar_vol` is new, and since code-fix PRs reset `docs/data/`, `V` dims nothing until the next daily workflow run republishes the data. The fail-open rule is what makes that safe — `V` dims nothing for one run rather than dimming everything.
- `.time-travel-dates` is `display: contents` so the date button, the `+ more` dropdown, and the cutoffs share one wrapping flex flow — without it the cutoffs reserve a column and push the dates onto an extra line in a narrow list panel. `.tt-filters` carries `margin-left: auto`, which is what puts dates left and cutoffs right.
- **The list panel's shared default width is derived from this bar.** `.left-panel { width: max(20%, 400px) }` — the floor is the measured 385px the widest bar needs to stay on one row (1 date button 75px + the 98px `+ more` dropdown + the 132px cutoff pair + 4px gaps + the bar's and the panel's 14px padding + the 6px scrollbar), rounded up for slack. Adding a control, widening the date label, or bumping `VISIBLE` in `renderTimeTravelBar` re-wraps it — re-measure and raise the floor. Measure by **vertical centre**, not `offsetTop`: a `<select>` is taller than a `<button>` and `align-items: center` gives same-row controls different tops. `min-width` deliberately stays at 256px so the resize handle can still trade list width for chart width; the floor is the default, not a hard minimum.
- **Six tabs override that default with a width measured from their own table** — `#volume-left` 610px, `#etf-left` 585px, `#industry-left` 570px, `#ep-left` 500px, `#vars-left` 490px, `#momentum-left` 440px. Each is the tab's natural table width (measured with the table at `width: max-content`) plus 28px panel padding, ~6px scrollbar and 2px card border. Five of the six were clipped at the shared 400px. They are **per tab, not one shared value**, because column counts run 5–9 and a single width sized for Volume costs Momentum ~170px of chart. All six clear the 385px bar floor, so the bar never re-wraps. Natural width is data-dependent (`td` is `white-space: nowrap`), so these are a floor with slack, not an exact fit — re-measure when a column is added. Themes, Parabolic and the four Viz tabs stay on the shared default. `tests/test_dashboard_panel_layout.py` pins the values and the floor.

### Selected-ticker Highlight

The selected ticker renders **yellow** (`--yellow`, `#ffd700`) in text and underline, with `--ydim` background and a yellow outline on its table row. The value matches the Cytoscape selected-node colour in `docs/app.js`, so the DOM tabs and the network viz agree on what "selected" looks like, and it stays distinct from `--amber`, which the V/A cutoff dropdowns own in the same panel.

Two rules exist because the obvious one-line change is not enough, and both will break again if someone "simplifies" them:
- `.tn-link.active-ticker:hover` re-asserts yellow because `.tn-link:hover` sets `--accent` with `!important` — without it the selected ticker flips to blue under the cursor. That was invisible back when selection was also blue.
- `.radar-chip.active-ticker` must sit **after** `.chip-screened`, `.chip-quiet`, and `.filtered-out` in `style.css`. `.radar-chip.chip-screened` sets the whole `border-color` at equal specificity, so on source order alone a selected screened chip keeps its blue outline; `.filtered-out` blanks the border and drops opacity, so a selected dimmed chip would show no selection at all. The rule restores full opacity and sets colours only — never a layout-affecting property, for the `syncRadarClamps` reason above.

### Dashboard Time Travel

Each tab's session bar (every tab except Overview) shows the newest trading day as a clickable date button plus a `+ more` dropdown that exposes every remaining session within the last **180 calendar days**. `VISIBLE` in `renderTimeTravelBar` is **1**, on every bar including the tabs with no V/A cutoffs — the cutoff dropdowns are much wider than the toggles they replaced, and one shared constant beats threading a per-bar count through eleven call sites to make six tabs look different from five. Raising it means re-measuring the `.left-panel` floor. Retention is calendar-day-based, not a fixed session count: `THEMES_HISTORY_DAYS = 180` in `export_dashboard_data.py` (mirrored by `SCAN_HISTORY_DAYS` in `ep_scan_common.py` and `SESSION_HISTORY_DAYS` in `docs/app.js`) controls the window. The shared `_history_cutoff` helper anchors the cutoff to the newest available session date (not wall-clock today) so the window is reproducible and robust to stale/holiday export runs.

**Every workflow run produces a fresh 180-calendar-day history**: `run_daily_workflow.py` calls `create_master_table.py --days 130` and each `run_screener.py --days 130` (~180 calendar days of trading sessions, with a few sessions of padding so the window is always full), so back-dated master + screener **parquet** files always carry today's full indicator schema (e.g. when `vars` was added, every dropdown session gets the new column on the very next workflow run instead of accumulating naturally). On the export side, `export_momentum_136` / `export_vars` / `export_parabolic` / `export_radar` / `export_volume` all iterate the per-day **parquet** files whose date falls within the 180-day window (and `export_radar` derives each date's screened union from the per-screener parquet via `stock_utils.union_tickers_for_date`) and rewrite `*_history.json` from scratch each run — no append-only drift; sessions older than 180 calendar days are pruned from the JSON. The on-disk parquet is separately pruned to the newest 10 sessions per subdir after export (`prune_screening_output`), so the docs JSON keeps the full window while local scratch stays small. Tabs without per-day source data (industry/leverage ETFs, EP scans) still accumulate one entry per workflow run and are pruned to the same window.

### Dashboard Chart (TradingView Free Embed Widget)

The right-hand chart in [docs/app.js](docs/app.js)'s `openChart()` function uses the **free** TradingView embed widget loaded from `https://s3.tradingview.com/tv.js`. This is NOT the paid Charting Library — it is severely limited and several documented overrides silently no-op. Past mistakes to avoid:

**⛔ HARD RULE — the volume study is never removed, and never leaves slot 0.** `{ "id": "STD;Volume" }` MUST be the **first** entry of `studies`, and `studies` MUST NOT exceed **5 entries**. The free embed applies only the first 5 studies and **silently discards the rest** — no console error, no exception, the pane simply isn't there. Index 0 is the only slot the cap can never reach, so that is where volume lives. **To add a study, replace an existing one — never append a 6th.** `tests/test_dashboard_chart_config.py` enforces both halves; if it fails, fix the config, not the test.

The 5-study cap has cost the volume pane twice. [#23](https://github.com/kuantumk/theme_dashboard/pull/23) was the type bug below; [#72](https://github.com/kuantumk/theme_dashboard/pull/72) appended `Earnings@tv-basicstudies` as a 6th entry, which pushed `STD;Volume` past the cap and silently killed volume **and** average volume — reverted, and the plan at `docs/plans/2026-07-31-001-feat-chart-earnings-markers-plan.md` is superseded. Nothing is wrong with the Earnings study itself; a 6-entry array with *no* Earnings study loses volume identically. Earnings markers are only possible by giving up one moving average, and `mainSeriesProperties.esdShowEarnings: true` **alone is inert** — verified to draw nothing without the study occupying a slot.

**No runtime chart API.** The widget instance exposes only `create / ready / render / generateUrl / image / imageCanvas / subscribeToQuote / getSymbolInfo / remove / reload`. `widget.activeChart()`, `getPanes()`, `setHeight()`, `applyOverrides()`, `setInputValues()`, `onChartReady()` are all undefined. Anything you want must be expressible in the constructor object. Do not write `onChartReady` callbacks — they fail silently. Pane heights cannot be controlled; the volume pane settles at ~1/3 of total chart height.

**The `studies` array is type-sensitive.** When any entry is an object (e.g. `{ id, inputs }` to set a per-instance length), **every** entry must be object form. Mixing `{ id: "MAExp@tv-basicstudies", inputs: { length: 10 } }` with a trailing `"STD;Volume"` (string) silently drops the string and that study never registers. Always wrap bare IDs as `{ id: "STD;Volume" }`. This was the root cause of the volume-pane disappearance in [#23](https://github.com/kuantumk/theme_dashboard/pull/23).

**Per-instance study styles are ignored.** `studies_overrides` is global per study type — two `MAExp@tv-basicstudies` instances both pick up `"moving average exponential.ma.color"`. The per-study `styles` field inside study objects (e.g. `{ id, inputs, styles: { plot_0: { color } } }`) is silently ignored by the free widget — that pattern only works in the paid Charting Library's `createStudy()`. To differentiate two EMA lines by color, you'd need to swap one for a different study type (e.g. WMA) and accept the formula change, or accept grouped colors.

**Volume study identifier matters.** Use `STD;Volume`, not `Volume@tv-basicstudies`. `STD;Volume` renders volume bars **and** the Volume MA overlay (blue gradient area); `Volume@tv-basicstudies` only renders bars in the free embed. `hide_volume: true` hides the built-in candle-overlay volume — it is independent of the separate Volume study. **`hide_volume: false` is not a fallback for a lost `STD;Volume`**: it draws bars squashed into the price pane with **no average-volume line at all**, which is the whole reason the separate study exists.

**`hide_legend` is all-or-nothing — do not use it.** Setting `hide_legend: true` removes the entire upper-left panel: the OHLC values, the daily change %, the volume readout, AND the study legend rows. Swing traders need OHLC + change % + volume at a glance, so leave `hide_legend: false` and use the per-element legend overrides instead:

```js
"overrides": {
  "paneProperties.legendProperties.showStudyTitles": false,
  "paneProperties.legendProperties.showStudyValues": false,
  "paneProperties.legendProperties.showStudyArguments": false
}
```

These suppress only the EMA/SMA rows while keeping the main series title, OHLC, change %, and volume value intact. Other useful legend toggles in the same group: `showSeriesOHLC`, `showVolume`, `showBarChange`, `showLastDayChange`, `showSeriesTitle` — see [TradingView's legend overrides docs](https://www.tradingview.com/charting-library-docs/latest/customization/overrides/chart-overrides).

**Other safe constructor options:** `hide_volume: true` (only affects the candle-overlay volume; the separate Volume study still renders), `overrides: { "scalesProperties.scaleSeriesOnly": true }` (auto-scales the price axis to candles instead of stretching to fit the lowest MA).

## Configuration

All workflow parameters live in `config/workflow_config.yaml` (lookback windows, RS thresholds, screener list, scoring coefficients, theme settings).

Environment variables (`.env`): `GOOGLE_SHEET_ID` (theme taxonomy sheet), `ALPACA_API_KEY` + `ALPACA_API_SECRET` (extended-hours volume for RVol), `IBKR_FLEX_TOKEN` (optional).

## CI/CD

Three GitHub Actions workflows:

| Workflow | Schedule (Pacific) | What it does |
|----------|-------------------|--------------|
| `daily-screening.yml` | 1:30 PM | Full 10-step theme pipeline + Pages deploy |
| `ep-scan-afternoon.yml` | 2:00 PM | AMC earnings scan → JSON + Discord alert + Pages deploy |
| `ep-scan-morning.yml` | 5:45 AM | BMO earnings scan → JSON + Discord alert + Pages deploy |

`daily-screening.yml` intentionally uses one timezone-aware cron entry (`America/Los_Angeles`) so GitHub creates only one scheduled workflow run at 1:30 PM Pacific. The EP scan workflows still use two UTC cron entries (PDT + PST) and pass the expected cron pair to the schedule guard via `SCHEDULE_GUARD_PDT_CRON` / `SCHEDULE_GUARD_PST_CRON` env vars. The guard skips EP scan runs whose triggering cron doesn't match the current Pacific offset. All workflows support `workflow_dispatch` for manual trigger.

## Tech Stack

Python 3.11+, pandas/numpy/scipy, pyarrow (parquet screening outputs), yfinance, Selenium (breadth scraping), finvizfinance + BeautifulSoup (fundamentals), Alpaca Market Data API (extended-hours volume). No TA-Lib — all indicators are pure pandas. Theme tagging uses no API-based LLM: the weekday Claude Code routine does the classification.
