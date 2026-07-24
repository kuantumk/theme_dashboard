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
3. **Market Breadth** NCFD/MMFI scraped from barchart.com via Selenium → `docs/data/market_breadth.json`
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
- **Discord notification**: sends webhook alert with ticker summaries on scan completion.
- **Local diagnostic runs**: both scan scripts accept `--out-dir <path>` (defaults to `docs/data/`) and `--no-discord`. The Windows Task Scheduler launcher `scripts/ep_scan_morning_local.bat` passes `--out-dir scripts/local_runs --no-discord` so local runs write to a gitignored sandbox and never dirty the CI-published `docs/data/ep_scan_*.json` files.

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
- **Flows**: step 9b of `run_daily_workflow.py` computes the radar in-process → "📡 L1 Radar" report section (above Market Themes; ignored by `parse_report`). `export_radar` in `export_dashboard_data.py` rebuilds `docs/data/radar.json` (current, uncapped — every scored L1, leaf, **and member ticker**) + `radar_history.json` (180-day window, per-entry L1s capped at `history_l1_limit` and chips at `tickers_per_leaf`, compact JSON) from the per-day master parquet — it runs inside `export_all()` **before** `prune_screening_output`. Only the newest session is built uncapped (`_build_radar_snapshot(..., tickers_per_leaf=None)`); history entries stay capped because that file already carries ~124 sessions at ~15 MB. The dashboard **Themes** tab renders the radar as one block per leaf — a metadata line (global rank, name, `N=`, raw → boosted) above a **full-width wrapping chip row** (screened members highlighted, unscreened dimmed), clamped to `--radar-chip-rows` (2) lines with a measured `+N more` toggle. It is deliberately **not** a table: five columns in a ~250px left panel pushed raw/boosted off-screen and clipped the ticker list. `syncRadarClamps` in `docs/app.js` measures chip `offsetTop` against the clamp — it must run whenever the panel's width changes or the tab first gains a box, so it is called from the tab switch, the resize-handle drag, `window.resize`, and a `ResizeObserver` (the observer alone is not sufficient: it only delivers inside the rendering lifecycle). The **Theme Viz** network is fed from the same radar snapshots via the client-side `radarVizSnap` adapter in `docs/app.js` (leaf `score` = mean member composite, `avg_rs` = mean member RS; hot filter avg RS ≥ 70, breadth ≥ 3 unchanged) — it averages only the leading `RADAR_VIZ_MEMBERS` (10, matching `tickers_per_leaf`) members so uncapped current snapshots and capped history entries score identically
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
- `inside_day` = today's high < yesterday's high AND today's low > yesterday's low
- `tight_day` = `|close − open| / close < 0.2 × adr_pct` (fractional body smaller than 20% of ADR%)
- `close_to_ma` = `|close − EMA10| < 0.5 × ATR14` OR `|close − EMA20| < 0.5 × ATR14`

Tickers that fail any condition stay default-colored. Logic lives in `src/reporting/export_dashboard_data.py:load_ticker_color_flags` (main universe) and `fetch_etf_metrics` (ETF tabs, recomputes color + VARS on-the-fly from yfinance OHLC).

### Dashboard Time Travel

Each tab's session bar (every tab except Overview) shows the last 5 trading days as clickable date buttons plus a `+ more` dropdown that exposes every remaining session within the last **180 calendar days**. Retention is calendar-day-based, not a fixed session count: `THEMES_HISTORY_DAYS = 180` in `export_dashboard_data.py` (mirrored by `SCAN_HISTORY_DAYS` in `ep_scan_common.py` and `SESSION_HISTORY_DAYS` in `docs/app.js`) controls the window. The shared `_history_cutoff` helper anchors the cutoff to the newest available session date (not wall-clock today) so the window is reproducible and robust to stale/holiday export runs.

**Every workflow run produces a fresh 180-calendar-day history**: `run_daily_workflow.py` calls `create_master_table.py --days 130` and each `run_screener.py --days 130` (~180 calendar days of trading sessions, with a few sessions of padding so the window is always full), so back-dated master + screener **parquet** files always carry today's full indicator schema (e.g. when `vars` was added, every dropdown session gets the new column on the very next workflow run instead of accumulating naturally). On the export side, `export_momentum_136` / `export_vars` / `export_parabolic` / `export_radar` / `export_volume` all iterate the per-day **parquet** files whose date falls within the 180-day window (and `export_radar` derives each date's screened union from the per-screener parquet via `stock_utils.union_tickers_for_date`) and rewrite `*_history.json` from scratch each run — no append-only drift; sessions older than 180 calendar days are pruned from the JSON. The on-disk parquet is separately pruned to the newest 10 sessions per subdir after export (`prune_screening_output`), so the docs JSON keeps the full window while local scratch stays small. Tabs without per-day source data (industry/leverage ETFs, EP scans) still accumulate one entry per workflow run and are pruned to the same window.

### Dashboard Chart (TradingView Free Embed Widget)

The right-hand chart in [docs/app.js](docs/app.js)'s `openChart()` function uses the **free** TradingView embed widget loaded from `https://s3.tradingview.com/tv.js`. This is NOT the paid Charting Library — it is severely limited and several documented overrides silently no-op. Past mistakes to avoid:

**No runtime chart API.** The widget instance exposes only `create / ready / render / generateUrl / image / imageCanvas / subscribeToQuote / getSymbolInfo / remove / reload`. `widget.activeChart()`, `getPanes()`, `setHeight()`, `applyOverrides()`, `setInputValues()`, `onChartReady()` are all undefined. Anything you want must be expressible in the constructor object. Do not write `onChartReady` callbacks — they fail silently. Pane heights cannot be controlled; the volume pane settles at ~1/3 of total chart height.

**The `studies` array is type-sensitive.** When any entry is an object (e.g. `{ id, inputs }` to set a per-instance length), **every** entry must be object form. Mixing `{ id: "MAExp@tv-basicstudies", inputs: { length: 10 } }` with a trailing `"STD;Volume"` (string) silently drops the string and that study never registers. Always wrap bare IDs as `{ id: "STD;Volume" }`. This was the root cause of the volume-pane disappearance in [#23](https://github.com/kuantumk/theme_dashboard/pull/23).

**Per-instance study styles are ignored.** `studies_overrides` is global per study type — two `MAExp@tv-basicstudies` instances both pick up `"moving average exponential.ma.color"`. The per-study `styles` field inside study objects (e.g. `{ id, inputs, styles: { plot_0: { color } } }`) is silently ignored by the free widget — that pattern only works in the paid Charting Library's `createStudy()`. To differentiate two EMA lines by color, you'd need to swap one for a different study type (e.g. WMA) and accept the formula change, or accept grouped colors.

**Volume study identifier matters.** Use `STD;Volume`, not `Volume@tv-basicstudies`. `STD;Volume` renders volume bars **and** the Volume MA overlay (blue gradient area); `Volume@tv-basicstudies` only renders bars in the free embed. `hide_volume: true` hides the built-in candle-overlay volume — it is independent of the separate Volume study.

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
