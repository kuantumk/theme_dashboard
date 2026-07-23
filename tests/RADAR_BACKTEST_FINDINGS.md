# L1 Radar Backtest: Composite Weights & Boost β

**Status**: protocol pre-registered; results pending the sweep run.
**Harness**: `tests/backtest_radar.py` (manual research tool, not in the daily workflow).

---

## 0. Why this backtest exists

The radar's per-ticker composite weights (0.4 rs / 0.4 vars_pct / 0.2 fast) and the
sibling-confirmation boost β=0.3 were **reverse-engineered from a single competitor
screenshot** (2026-07-13 Cybersecurity case, PR #52) and have never been validated
against forward returns. `tools/validate_radar.py --sweep` prints ranks for one date
and optimizes nothing. This harness adds the missing objective function.

## 1. Pre-registered protocol (written BEFORE any sweep numbers were produced)

- **Primary metric**: mean per-session rank IC — Spearman(L1 `boosted` score,
  forward excess return of the L1's **all-members** basket), horizon **H=10**
  sessions, equal-weight close-to-close, excess vs SPY, ≥ 70% member coverage,
  ≥ 8 scored L1s per session.
- **Win rule** (for any config challenging the current one): primary IC improves,
  AND mean day-over-day rank autocorrelation degrades by less than 10% relative,
  AND the IC sign holds at H=5 and H=20. Anything else is "needs more data",
  not a win.
- **Secondary metrics** (supporting, never overriding): top-3 hit rate vs median
  L1, top-3 spread (bps), top-5 day-over-day Jaccard, per-session IC>0 share,
  `top` (dashboard-chip) basket variant, `--skip-day` execution-lag variant.
- **Statistical care**: ~120-130 sessions with overlapping H-day windows ≈ N/H
  independent observations (≈ 6 at H=20). Moving-block bootstrap CI
  (block = max(2H, 10), 2000 resamples, 90%). At H=20 this is direction, not
  significance — treated as such.
- **Grids**: weights {current .4/.4/.2, equal, rs_only, vars_only, fast_only,
  no_fast .5/.5/0, fast_heavy .2/.4/.4, rs_heavy .6/.2/.2} × β {0, 0.15, 0.3,
  0.5} × H {5, 10, 20}. Corners measure each leg's solo predictive power —
  the direct answer to "are the three legs redundant?". β=0 is the
  no-boost baseline the original design never checked.
- **Out of scope by user decision (2026-07-23)**: liquidity-floor sweep (the
  $10M floor stays), fast-leg re-anchoring variant (superseded by the vars-leg
  re-anchoring to the all-tagged pool, which shipped as a production change;
  its effect is quantified descriptively via `--mode anchor-diff`).

## 2. Known biases and caveats (read before trusting any number)

1. **Tag lookahead**: theme tags are point-in-time-today; worse, the weekday
   tag-audit routine densifies baskets of the *top radar L1s*, so historically
   hot L1s have inflated breadth → IC is biased **optimistic**. `--pit-tags`
   (git-history tags, legacy labels normalized) is the partial mitigation and
   is reported alongside.
2. **Survivorship**: the fresh yfinance download lacks delisted tickers.
3. **Single regime**: Jan–Jul 2026 only. A weight/β choice that wins here is
   "supported", not "proven".
4. **Warmup**: all three radar legs (27/100/21 sessions) are fully warmed
   across the whole 130-session window; only non-radar 252-day columns are
   partially warmed in the oldest ~25 sessions.

## 3. Leg redundancy (Q1) — results

_TBD: `--mode legs` pairwise Spearman matrix + solo-corner ICs from
`--mode weights`._

## 4. Weight sweep — results

_TBD: aggregate table (mean IC + CI, hit rates, stability) per weight config
at production β._

## 5. β sweep incl. β=0 baseline (Q2) — results

_TBD: `--mode beta` on the winning (or current) weights; plus the cheap
`--mode history` rank-sensitivity check._

## 6. VARS re-anchoring diff (descriptive) — results

_TBD: `--mode anchor-diff` rank correlation + top-3 overlap between tagged-pool
and floor-survivor anchoring._

## 7. Episode candidates for validate_radar regression

_TBD: `--mode episodes-scan` output, human-verified subset only._

## 8. Recommendations

_TBD: split into adopt / needs-more-data / rejected. Config adoption is gated
on user review of this document — no workflow_config.yaml change ships with
the harness._
