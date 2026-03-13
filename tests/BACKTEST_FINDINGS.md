# Theme Scoring Backtest: Old vs New Formula

**Date**: 2026-03-12
**Data period**: 2026-02-27 to 2026-03-10 (8 trading days)
**Themes scored per day**: 172-214 (avg 201)
**Screened tickers per day**: 308-522

---

## 1. Executive Summary

The new scoring formula produces **measurably better rankings** for momentum trading:

- **23% more stable top-5** day-over-day (Jaccard 0.296 vs 0.240)
- **Better score discrimination**: spread of 93.4 vs 83.8
- **Balanced component contribution**: 49/51 (strength/confirmation) vs 24/71/5 (rs/momentum/breadth)
- **Structural health matters**: themes below 50SMA are properly penalized
- **Extension penalty works**: 133 unique themes penalized across 8 days

However, several **weaknesses were discovered** that need attention before production use.

---

## 2. Old Formula Problems Confirmed

### 2a. Momentum component dominated everything

Old formula top-15 component breakdown:
```
RS component:       19.3  (24%)
Momentum component: 56.3  (71%)
Breadth component:   3.6  ( 5%)
```

The 0.6 weight on `high_momentum_pct` (RS > 80 binary threshold) meant **71% of the score** was determined by a single binary metric. Breadth at 5% is negligible.

### 2b. Score banding / clustering

Old formula histogram shows bimodal clustering:
```
[ 0-10):  485 themes   <-- everything below momentum threshold
[20-30):  501 themes   <-- everything above it with breadth=1-2
```

This is the "momentum cliff" — themes either pass the RS>80 gate or they don't. There's no gradient between RS=79 and RS=30.

### 2c. Breadth was nearly meaningless

| Breadth | Old Avg Score | New Avg Score |
|---------|--------------|--------------|
| 1       | 12.8         | 13.7         |
| 2       | 23.6         | 25.1         |
| 3-4     | 45.9         | **54.3** (+18%) |
| 5-7     | 51.7         | **63.5** (+23%) |
| 8-10    | 53.1         | **66.1** (+24%) |
| 11+     | 72.2         | 70.3         |

Going from 3 to 10 screened stocks only added 7.2 points under the old formula. Under the new formula, it adds 11.8 points — the 3-10 stock range (the critical range for momentum themes) is properly differentiated.

### 2d. Case study: Mining / Gold mispriced

On Mar 10, Mining/Gold had 7 screened stocks, all above 50SMA, 71% near 52-week highs, avg distance from 25SMA of just 1.7%. This is a textbook strong theme with good entry timing.

- Old score: **15.7** (ranked ~80th) — crushed because median RS was 57.9 and no stocks were above RS 80
- New score: **59.5** (ranked 18th) — properly credited for structural health and breadth

The old formula's reliance on RS>80 threshold completely missed a theme where every stock was above its 50SMA and most were near highs.

---

## 3. New Formula Results

### 3a. Component balance achieved

New formula top-15 component breakdown:
```
Strength component:     39.6  (49%)
Confirmation component: 40.7  (51%)
Total score:            80.3
```

Exactly the intended 50/50 balance. Neither RS quality nor breadth/structural health dominates.

### 3b. Rank correlation: r = 0.78-0.86

The two formulas are **correlated but materially different**:

| Date       | Spearman r | Themes |
|-----------|-----------|--------|
| 2026-02-27 | 0.8516    | 205    |
| 2026-03-03 | 0.8354    | 209    |
| 2026-03-05 | 0.7775    | 210    |
| 2026-03-10 | 0.7808    | 172    |

r~0.82 means ~33% of the ranking variance is different. This is enough to materially change which themes appear in the top 10-15.

### 3c. Score distribution is healthier

```
              Old        New
Mean:         28.6       32.6
Median:       24.1       24.3
P25:           5.2       14.6   (+180%)
P75:          40.6       51.4   (+27%)
P90:          66.1       67.8
P99:          83.2       89.8
Max:          86.9       98.7
```

The old formula had 485 themes (30%) crammed into the 0-10 bucket. The new formula spreads these into 10-30, making mid-tier themes distinguishable from dead ones.

### 3d. Top-5 stability improved

Day-over-day Jaccard similarity of the top-5 set:
- Old: **0.240** (on average, only ~1.5 of top-5 themes carry over to the next day)
- New: **0.296** (+23%, ~2 of top-5 carry over)

For a momentum trader tracking themes across days, this is meaningful. The old formula's instability was driven by the RS>80 binary cliff — small RS fluctuations around 80 caused large ranking swings.

---

## 4. What the New Formula Gets Right

### 4a. Structural health as a multiplier works

Themes with 100% above 50SMA consistently scored higher. Examples on Mar 10:

| Theme | Med RS | 50SMA% | NearHi% | B | Score |
|---|---|---|---|---|---|
| Real Estate / Diversified REIT | 100.0 | 100 | 100 | 13 | 98.7 (Mar 3) |
| Nuclear | 94.7 | 100 | 100 | 4 | 82.3 |
| Energy / Oil Refining | 73.7 | 100 | 60 | 5 | 61.4 |
| AI - Data Center & Cloud Services | 71.1 | 100 | 75 | 4 | 60.4 |

All have strong structural health AND high near-highs percentages.

### 4b. Extension penalty correctly identifies overchased themes

| Theme | Dist 25SMA | Extension Factor | Impact |
|---|---|---|---|
| Solar | 98.8% | 0.65 | Parabolic outlier detected |
| AI - Data Center Components | 70.3% | 0.65 | Massively extended |
| AI - Optics | 33.3% | 0.65 | Extended after run-up |
| Cryptocurrency | 15.9% | 0.80 | Moderately stretched |
| Nuclear | 12.5% | 0.92 | Slight extension, still tradeable |

These penalties correctly matched what a momentum trader would want: Nuclear at 12.5% extension is still enterable (0.92x penalty), while Solar at 98.8% is clearly parabolic (0.65x).

### 4c. Themes that correctly moved up

| Theme | Old Rank | New Rank | Why |
|---|---|---|---|
| AI - Memory & Storage | 129 | 31 | 100% above 50SMA, 80-100% near highs |
| Travel & Leisure / Cruise Lines | 146 | 36 | 100% above 50SMA, 100% near highs, b=5 |
| Retail / Specialty Apparel | 161 | 66 | Strong structural health, near highs |
| Energy / Oilfield Services / Proppants | 165 | 90 | 100% above 50SMA and near highs |

All of these were structurally strong themes that the old formula ignored because their median RS wasn't above 80.

### 4d. Themes that correctly moved down

| Theme | Old Rank | New Rank | Why |
|---|---|---|---|
| EdTech / Online Learning | 63 | 161 | 0% above 50SMA, 0% near highs, b=2 |
| MedTech / Diagnostics | 59 | 135 | Extension 0.65x, 0% near highs, b=2 |
| AI - Security & Surveillance | 94 | 167 | Extension 0.65x, 0% near highs, singleton |
| Biotech / Dermatology | 93 | 154 | Extension 0.65x, 0% near highs, singleton |

All are structurally broken themes (below 50SMA, far from highs, low breadth) that the old formula inflated because one stock had RS > 80.

---

## 5. Weaknesses Discovered

### 5a. CRITICAL: avg_dist_25sma uses mean, not median - vulnerable to outliers

Solar on Mar 9 shows **98.8% average distance from 25SMA**. This is almost certainly a single parabolic stock pulling the mean. Using `np.nanmean()` makes the extension metric fragile.

**Fix**: Use `np.nanmedian()` instead of `np.nanmean()` for dist_25sma. A single outlier shouldn't penalize an entire theme.

### 5b. pct_near_highs threshold (0.85 x max252) may be too loose

For Qullamaggie-style breakout trading, "near highs" means within 5-10% of highs, not 15%. At 15%, a stock that peaked at $100 and is now at $85 qualifies. That stock might be in stage 4 decline or a wide consolidation — not necessarily "setting up."

On the other hand, this is an aggregate theme metric, not an individual stock screen. At the theme level, 15% is reasonable for capturing "stocks in the vicinity of breakout zones."

**Recommendation**: Test both 0.85 and 0.90 thresholds. A tighter threshold would reduce confirmation scores for recovering themes that haven't fully rebuilt.

### 5c. Extension penalty has hard step thresholds

The current penalty is:
```
>25%: 0.65
>15%: 0.80
>10%: 0.92
<=10%: 1.00
```

A theme at 10.1% from 25SMA gets penalized (0.92x) while one at 9.9% gets no penalty. This creates small discontinuities at the boundaries.

**Impact**: Low in practice — most themes are either clearly extended or clearly not. But a continuous penalty (e.g., `max(1.0 - (dist_25sma - 8) * 0.02, 0.65)`) would be smoother.

### 5d. Volume factor threshold (> 2.0 for 1.15x) is rarely triggered

Only the `> 1.5` tier (1.08x bonus) fired with any frequency. The `> 2.0` tier (1.15x) was rare. This means the volume bonus is effectively binary: 1.0 or 1.08.

**Recommendation**: Lower the tier-2 threshold from 2.0 to 1.75, or switch to a continuous scale.

### 5e. No momentum/recency signal

Neither the old nor new formula captures **whether a theme is heating up or cooling down**. A theme that was #1 for 5 days and is now fading gets the same treatment as one that just appeared in the top 10 today.

For Qullamaggie, the *acceleration* of a theme (day-over-day rank improvement) is a strong signal. First 2-3 days of a new theme appearing in the top 10 are the highest-conviction entries.

**This is NOT easy to fix in a single-day scoring function** — it would require multi-day state tracking.

### 5f. Theme taxonomy fragmentation dilutes signals

The backtest revealed overlapping themes scoring separately:
- "Precious Metals / Gold Mining" (7 tickers)
- "Mining / Gold" (18 tickers)
- "Precious Metals / Gold & Silver" (4 tickers)
- "Metals - Gold, Silver, Copper, Aluminum" (10 tickers)

These are all the same thematic bet. A gold miner appears in 2-3 of these themes, fragmenting breadth across multiple entries instead of consolidating into one strong signal.

**This is a taxonomy problem, not a scoring problem**, but it meaningfully affects ranking quality.

### 5g. Singleton themes still score and can create noise

Themes with breadth=1 get 0.3x penalty, which brings their max score to ~30. With 670 singleton observations across 8 days, they create a noisy "long tail" of low scores that add clutter to the ranking.

**Recommendation**: Consider a minimum breadth threshold (e.g., breadth >= 2) to exclude singletons from the scored output entirely, or at least from the displayed report.

### 5h. Electricity / Power Generation showed extreme rank volatility

Under the new formula, this theme ranked:
```
84, 86, 2, 1, 5, 1, 1, 48
```

StdDev = 35.9 — the most volatile theme in the top-20 set. This happened because the theme has 16 total tickers but the number passing screeners varied wildly (sometimes 2, sometimes 16). When most tickers pass, the confirmation score is very high (100% above 50SMA, near highs) and it rockets to #1. When only a few pass, it drops.

**Root cause**: Large themes are more sensitive to screener composition changes. This is partially desirable (it reflects real market conditions) but the magnitude of rank swings is jarring.

---

## 6. Bear Market Simulation

With bear weights (strength=0.70, confirmation=0.30), the new formula produced sensible rankings:

| Rank | Old (mkt_rel_score sort) | New (bear weights) |
|------|---|---|
| 1 | Real Estate / Diversified REIT | Real Estate / Diversified REIT |
| 2 | Biotech / Rare Diseases | Biotechnology / Oncology |
| 3 | Healthcare / Data & Analytics | Agriculture / Crop Protection |
| 4 | Biotechnology / Oncology | Biotech / Rare Diseases |
| 5 | Agriculture / Crop Protection | Nuclear |

The old bear sort (just avg_rs x penalty) was dominated by themes that happened to have 100% of their 1-3 screened stocks above RS 90. The new formula with bear weights still considers structural health and near-highs, producing a more defensible ranking.

Key difference: Nuclear at rank 5 (new) vs rank 7 (old) — it has 100% structural health which the old bear sort ignored entirely.

---

## 7. Recommendations

### Implement immediately:
1. **Switch to median for dist_25sma** (fix 5a) — one-line change, eliminates outlier vulnerability
2. **Filter out singletons from scored output** (fix 5g) — reduces noise
3. **Fix RS lookback config mismatch** — create_master_table.py hardcodes 20, config says 27

### Implement after testing:
4. **Test 0.90 threshold for pct_near_highs** (fix 5b) — may be better for breakout focus
5. **Lower volume tier-2 threshold to 1.75** (fix 5d) — makes the bonus more accessible
6. **Smooth the extension penalty** (fix 5c) — continuous function instead of hard steps

### Out of scope / future work:
7. **Theme taxonomy cleanup** (issue 5f) — merge overlapping themes at the tagging level
8. **Multi-day momentum tracking** (issue 5e) — requires architecture change to track rank history
9. **Large-theme stability** (issue 5h) — consider capping screened-ticker contribution per theme

---

## 8. Test Methodology

- **Scoring**: Both formulas run on identical master_df and screened_tickers for each day
- **Regime**: All tests run with bull market weights (MMFI was > 50 during this period)
- **Data**: 8 days, 172-214 themes scored per day, 1609 total theme-day observations
- **Comparison metrics**: Rank correlation (Spearman), Jaccard top-5 stability, score distributions, component breakdowns, per-theme deep dives
- **Bear simulation**: Re-run on latest day with bear weights to verify regime switching
- **Code**: `tests/backtest_theme_scoring.py`
