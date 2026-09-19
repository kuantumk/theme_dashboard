---
title: Tape Pressure Price and Volume Redesign - Plan
type: refactor
date: 2026-09-16
artifact_contract: ce-unified-plan/v1
product_contract_source: ce-plan-bootstrap
execution: code
---

# Tape Pressure Price and Volume Redesign - Plan

## Goal Capsule

**Objective.** At any hour the US market trades — pre-market, regular session, after hours — a trader opening the tape board can name the themes being accumulated and the themes being distributed, and every ticker on it is demonstrably trading on unusual participation for that time of day rather than merely drifting.

**Means.** Replace per-trade buy/sell classification with two stateless tests applied to each poll: the direction of price against a session-appropriate reference, and a floor on Relative Volume at Time (KTD1).

**Authority hierarchy.** Product behavior follows the R-IDs. Implementation mechanism follows the KTDs within those R constraints. Where a measurement in this plan contradicts a claim in `CLAUDE.md`'s tape-pressure section, the measurement wins and R19 requires that section be corrected in the same change.

**Stop conditions.** After hours has a validated live source, so that half no longer gates the work. Implementation waits on the pre-market probe (Q1), which is the last unmeasured session state and the one the projection says is most likely to need a different answer. Beyond that, stop and report if any session state is about to gate on a reading that has not been shown to move while its own volume moves — that is the failure this plan was rewritten around.

**Who finishes it.** The implementer runs the full local verification in the Verification Contract against a live session, then opens a PR. The board is local-only and publishes nothing to Pages, so there is no deploy step.

## Product Contract

### Summary

The strong/weak split stops being an inference about who initiated each trade and becomes a direct reading of two things the feed states plainly: which way the price has gone against the reference that matters in the current session state, and how much volume the stock has traded relative to its own history by this time of day. Four session states each carry their own reference price and their own volume floor. Themes rank by the relative volume of their strongest members rather than by summed hit margins. The quote websocket, the trade classifier, and the trailing hit window all come out.

### Problem Frame

The board's columns rest on `classify.py`, which labels each poll buyer- or seller-initiated from a bid/ask snapshot. Three things undermine that foundation, and they compound.

The data is thin at the source. The TradingView screener publishes no bid or ask for US equities at all — selecting them returns null for every row — so equity quotes arrive over a separate websocket purely to feed this classifier. The classifier's own docstring concedes it is an approximation of Lee-Ready/CLNV rather than the algorithm: it sees one snapshot every ten seconds and infers that a trade happened by watching cumulative volume rise, then emits a single sign for an interval that may have carried fifty thousand prints.

The failure mode is silent. `CLAUDE.md` records that the quote rule was reading the wrong book for months while the board "still looked right", because the tick rule decides roughly two thirds of observations and masked it. A signal whose principal defect was invisible for months is a poor foundation regardless of how carefully the remaining third is tuned.

And the board is dark when it would matter most. Everything is anchored to the 09:30 open, so the tool says nothing during the pre-market and after-hours windows where gaps form and earnings reactions play out.

### Key Decisions

- **Price action and relative volume replace trade-side inference entirely** — the user judges price and volume the more reliable signals. Governs R1, R2, R3, R4, R5, R6, R16.
- **A ticker may occupy both columns at once, and this is intended, not a defect to resolve** — a stock that gapped down and is being bid up off its open is genuinely strong on one reference and weak on the other, and the board should show both readings rather than pick one. Governs R5, R13.
- **The board extends to pre-market and after hours** — the extended-hours monitor is the point of the redesign, not a follow-on. Governs R3, R4, R9, R10.
- **Every prior in-play criterion is discarded**, including the ≥3% absolute-change leg that currently admits tickets on its own. Relative volume becomes mandatory. Governs R6, R16.
- **TradingView is the only data source, and Relative Volume at Time is computed in-app in every session state** — no Alpaca, and no per-state vendor fields. Governs R7, R8, R9, R10, R14.

### Requirements

**Session state and reference price**

- R1. The board resolves exactly one session state per poll from the feed's own `current_session` field, never from the local clock: pre-market, regular session, after hours, or closed.
- R2. In the regular session a ticker is strong when its price is above the session open or above the previous session close, and weak when its price is below either of them.
- R3. In the pre-market a ticker is strong when its price is above the previous session close, and weak when below it.
- R4. In the after-hours window a ticker is strong when its price is above the regular session's closing price, and weak when below it.
- R5. A ticker satisfying both the strong and the weak test appears in both columns, carrying a marker that names which reference each side came from.
- R16. No ticker reaches either column on a price move alone. The absolute-change leg is removed rather than retained as an alternative admission path.

**Relative volume gate**

- R6. A ticker reaches either column only when its Relative Volume at Time meets the floor for the current session state. A ticker that fails the floor is absent from the board regardless of how far its price has moved.
- R7. Relative Volume at Time compares the volume this ticker has traded since its session anchor against the mean volume *it* had traded by the same point of day across recent sessions. A market-wide or full-day divisor does not satisfy this requirement.
- R8. Regular-session floors step with elapsed time: 0.7 from the open, 1.0 at 15 minutes, 1.2 at 30 minutes, and 1.2 for the remainder. The first band also covers the minutes before its own mark.
- R9. The pre-market floor is 3.0.
- R10. The after-hours floor is 1.5.
- R11. A ticker whose Relative Volume at Time cannot be computed scores zero and is excluded. An unknown never clears a floor.

**Ranking**

- R12. A theme's score is the mean Relative Volume at Time of its three highest-scoring members, computed on a bounded value so that one extreme reading cannot decide the ranking.
- R13. A theme whose remaining members also clear the gate scores higher than an otherwise identical theme whose members do not. This term only ever adds.
- R17. Columns keep the existing L1/L2 theme grouping, the industry fallback for untagged tickers, and the published shown-versus-total counts.

**Crypto**

- R14. The crypto tab applies a single relative-volume floor of 1.2 at all times and recognises no pre-market or after-hours state.
- R15. The crypto price test uses that market's own 24-hour reference, and the tab labels it as such so it is not read as the equity session measure.

**Honesty**

- R18. An empty or thin column names its own cause, distinguishing a failed fetch, an unavailable relative-volume source, a warm-up still running, and a genuinely quiet market. Absence of data never renders as a quiet market.
- R19. The tape-pressure section of `CLAUDE.md` and the tape entries in `CONCEPTS.md` describe the shipped behavior. Text describing the retired classifier is removed rather than left standing beside its replacement.

### Acceptance Examples

- AE1. Covers R2, R5. **Given** a stock closed at $10 yesterday, opened at $9 today, and now trades at $9.50, **when** the board polls during the regular session and the stock clears its volume floor, **then** it appears in the strong column (above open) and in the weak column (below previous close), marked on each side with the reference that placed it there.
- AE2. Covers R6, R16. **Given** a stock is up 12% on the day but has traded 0.4x its usual volume for this time of day, **when** the board polls, **then** the stock appears in neither column.
- AE3. Covers R9. **Given** the feed reports `pre_market`, **when** a stock has traded 2.5x its usual pre-market volume by this hour, **then** it is excluded; at 3.1x it is admitted, and its side is decided against the previous session close alone.
- AE4. Covers R12. **Given** one theme holds a single member reading 40x normal volume and two members at 1.3x, and a second theme holds three members at 3.5x, **then** the second theme ranks higher. The two 1.3x members clear the 1.2 floor deliberately: at 1.0x the gate would remove them, the first theme would score on its capped single member alone, and the example would assert the opposite of what the rules produce.
- AE5. Covers R11, R18. **Given** KTD3 resolved to its second rung and the extended-hours baselines are still warming up, **when** the board renders, **then** the columns are empty and the page states that the warm-up is in progress rather than showing a quiet market. On the first rung there is no warm-up and this state is unreachable.

### Scope Boundaries

**In scope.** The four session states, both reference-price tests, the relative-volume gate and its schedules, the new theme score, removal of the classifier and quote socket, the UI changes those require, and the documentation correction in R19.

**Deferred to follow-up work.**

- Backtesting whether the new board's columns predict forward return. This plan changes the signal on the user's judgment and the data-quality evidence in the Problem Frame; it does not claim a measured improvement, and none of the figures here establish one.
- Any per-ticker persistence notion, such as how long a ticker has held its side.

**Outside this product's identity.** Publishing any of this to GitHub Pages. The board stays local, writing only to the gitignored `scripts/local_runs/`.

### Outstanding Questions

- Q1. **Closed by KTD2's decision rather than by picking a field.** The question was which vendor column serves each session state; the answer is that none of them does, because the app's own figure is a chart-side calculation with a time-of-day denominator and every published column is full-day-anchored. The measurements that produced the per-state answers are kept in Sources because they document the trap, not because a field was chosen.
- Q2. **Answered: yes.** `postmarket_change` measures against the regular session close, reconciled on 8 of 8 sampled names, so R4 is a single field test.
- Q3. **Closed — no Alpaca needed.** The TradingView chart websocket serves extended-hours bars with volume under the session already in `.env`, so the extended-hours baseline needs no second vendor and no new credentials.
- Q9. **Closed: every session state uses the in-app computation, on TradingView data only.** See KTD2.
- Q4. Crypto only, and narrowed by KTD2: the tab now computes its own ratio like every other surface, so the question is no longer which vendor field to trust but **which anchor a market with no open should use**. The natural reading is the UTC day, matching how the crypto session date already rolls. What 1.2 admits on that basis is unmeasured, and one probe against the crypto scanner settles it.
- Q5. **Closed.** All three windows are buildable now, with no missing dependency — the chart websocket supplies the extended-hours bars pre-market needs. Nothing is deferred on feasibility grounds.
- Q6. How large is the breadth coefficient, and may a single strong name top the board? KTD5 now fixes the term's denominator and its minimum-member floor, but not its magnitude. A small value leaves the board intensity-led and singleton-topped; a large one restores the breadth-swamps-intensity failure the retired summed-margin score had. The radar's β and the coil marker's γ are the repo's precedent for fixing such a value in config with its sweep recorded.
- Q7. Does price-move magnitude enter the theme score at all? R12 ranks on relative volume alone, so a theme churning 3.1x on a 0.1% move can outrank one running +9% on 2.9x, and R2's direction test excludes only an exact tie — the measured session put all 401 admitted rows into a column, with no neutral state. The candidates are to leave the score volume-only and say so, or to have a member contribute only when its move against its own reference clears a minimum. The second is a scoring tolerance rather than an admission path, so R16 holds either way.
- Q8. **R9's 3.0 now stands as written and needs no recalibration.** The in-app pre-market computation produces the same quantity the user reads off the app, on the same scale, so 3.0 means what they intended. An earlier revision of this plan proposed rescaling the floor to roughly 0.05 for a full-day-anchored proxy; that proxy is retired and the rescaling with it. R10's 1.5 is likewise unchanged.

### Sources

Every figure below was measured live against the market on 2026-09-16 between 11:43 and 11:52 ET, roughly 140 minutes into the regular session.

- **`relative_volume_intraday|5` is Relative Volume at Time.** Ten liquid names were scored both by the vendor field and by `src/bidask/rvol_at_time.py` computing the definition from the ticker's own 5-minute history. Rank agreement was exact, 10 of 10. Levels differed by 2–6%, and the residual correlates with each ticker's pre-market share of volume at +0.82, so the two series are close but not interchangeable at the level of a floor.
- **The field resolves only at `|5`.** `relative_volume_intraday` bare and at `|1`, `|15`, `|30`, `|60` all return null, as does `relative_volume_at_time`. The `|5` suffix is not a timeframe selector here; it is the only form that exists.
- **The column catalogue is not the metainfo.** `open` is absent from the MCP catalogue and resolves fine. `relative_volume_intraday|5` is listed as stock/etf only and serves real values on the crypto scanner. This is the same unlisted-alias shape as `Value.Traded` in `src/bidask/feed.py`, which is why KTD2 fails closed rather than trusting availability.
- **Reference prices are all available and internally consistent.** For NVDA: `close` 215.795, `open` 214.14, `change` 1.7085%, `change_from_open` 0.7729%, `gap` 0.9285%, `premarket_change` 0.9709%, `premarket_close` 214.23. The implied previous close of 212.17 reconciles `change`, `gap`, and `premarket_change` to four significant figures, confirming that `premarket_change` measures against the previous session close exactly as R3 requires.
- **One request covers the universe.** A single authenticated screener call returned 2,760 rows in 0.355s with `open`, `change_from_open`, and `relative_volume_intraday|5` non-null on 100% of them, and `premarket_volume` non-null on 70.5% — the absentees being names that did not trade pre-market.
- **The session cookie decides the feed.** With `TRADINGVIEW_SESSIONID` and `TRADINGVIEW_SESSION_SIGN` loaded, `update_mode` reads `streaming`. Without them, and on the MCP server, it reads `delayed_streaming_900`.
- **Gate yield at the shipped thresholds.** Of 1,776 liquidity-filtered rows, the 1.2 floor admitted 401 — 244 strong, 207 weak, with 50 in both columns. The 3.0 floor admitted 38.
- **The relative-volume tail is extreme and genuine.** Median 0.841, p90 1.687, p99 4.329, max 2090.8. The maximum is DLXY, up 418% on 199M shares against a 6.8M average — a real move, not a data fault. Only 6 rows market-wide exceeded 10.
- **yfinance cannot serve extended-hours volume.** With `prepost=True` it returns the bars — 66 pre-market and 48 after-hours 5-minute bars per session — carrying zero volume on every one. `CLAUDE.md` already records this for the EP scan; it is confirmed here for all six probe symbols across 15 sessions.
**After-hours measurements, taken live at 18:03 ET on 2026-09-16, two hours past the close.** These answer Q2 and half of Q1, and they overturn the first rung of KTD3 for this session state.

- **`current_session` reads `post_market`.** Already in `SESSION_LABELS`. `extended` was not observed and the table row for it stands unresolved rather than assumed.
- **`postmarket_change` measures against the regular session close, exactly as R4 requires.** `close` holds the 16:00 price through the post-market window rather than tracking the last extended-hours print, and `postmarket_close / (1 + postmarket_change)` reconciles to it on 8 of 8 sampled names. So R4 is one field test, with no arithmetic. Q2 answered.
- **⛔ Two fields look like relative volume after the close. The one named `intraday` is the dead one.** `relative_volume_intraday|5` **freezes at 16:00**: across a 240-second gap it changed for **0 of 250** symbols while post-market volume rose for 111 of them, and again for **0 of 250** on a second run. It stays non-null on 2,805 of 2,805 rows throughout, so nothing in a single response reveals it. **`relative_volume_10d_calc` is the live one** — it changed for **168 of 250** over the same gap and climbs in step with post-market volume (GIPR 2.8839 to 2.8917 as its post-market volume ticked 42,184,271 to 42,252,714).
- **The live field is what TradingView's own app shows as Relative Volume at Time in this window**, confirmed against the user's screen: GNRC read 1.999 here against 2.07 on the app, GIPR 2.884 against 3.11, both climbing, so the gaps are the minutes between the two readings.
- **Why the same field is wrong during the regular session and right after it.** `relative_volume_10d_calc` is cumulative volume over the 10-day average **full day** — the repo already verified that divisor to a 0.02% median error across 59 tickers, which is why a fixed floor on it mid-session is a different filter every hour (NVDA read 0.27 two hours in). Once the session is complete that objection lapses: the numerator is a whole day, so the ratio means what it says. The two fields are therefore not competitors — each is correct in one session state.
- **The field choice decides whether the board sees the earnings pops it exists for.** Of 11 names that traded at least 5% of a normal day's volume after the bell *and* moved at least 5%, a 1.5 floor admits **8 on the live field and 5 on the frozen one**. DAIC ran +89.1% and reads 2.804 live against 0.076 frozen; GNRC ran +35.2% and reads 1.999 against 0.837; FLNC ran −17.4% and reads 1.504 against 1.116. All three are rejected by the frozen field and admitted by the live one.
- **After-hours yield on the live field**, across 1,824 liquidity-filtered rows: median 1.040, p90 1.640, p99 2.976. The 1.5 floor admits **276**; at that floor the price test splits 119 strong, 60 weak, and 96 with no post-market print at all, which land in neither column.
**Pre-market measurements, taken live at 09:10–09:14 ET on 2026-09-17 with `current_session` reading `pre_market` and the feed `streaming`.**

- **⛔ `relative_volume_10d_calc` is `volume / average_volume_10d_calc` in every session state, and that single fact explains all three behaviours.** Measured against the published inputs: median relative error **3.10%**, matching within 5% on **1,353 of 2,000** rows. The pre-market-volume numerator is decisively excluded at 99.69% median error, 2 of 2,000. Mid-session the numerator is a partial day, so the ratio is not time-adjusted — the repo's long-standing finding. After the close it is a complete day that keeps accruing extended-hours prints, so it is live and meaningful. In pre-market it is still *yesterday's* completed day: `volume` reads a median 2,213,074 against a median `premarket_volume` of 6,448.
- **So every relative-volume field replays the prior session during pre-market.** Across 300 symbols over a 201-second gap, pre-market volume rose for **262** while `relative_volume_10d_calc`, `relative_volume_intraday|5`, `relative_volume` and the `|1`/`|5`/`|15`/`|60` variants each changed for **0 of 300**, all non-null on 2,804 of 2,804 rows throughout. A 3.0 pre-market floor admits 18 names and those names are the ones that were busy yesterday, which is worse than an empty board because it looks populated.
- **⛔ What the app actually plots, and why no screener field can supply it before the open.** TradingView's Relative Volume at Time is `ta.relativeVolume(10, "1D", cumulative)` evaluated on the chart's own bars. On an extended-hours chart the 1D anchor is **04:00**, so the numerator is cumulative volume from 04:00 to now and the denominator is the mean cumulative volume to that same clock time across 10 sessions. Confirmed against the user's chart for GNRC on 2026-09-17: the plot climbs through pre-market to roughly 100 and then **falls** to 33.50 at 09:30 — the numerator keeps accruing while the denominator jumps as the regular-session open enters the 10-day average. That discontinuity is the signature of a time-of-day denominator and rules out every full-day-anchored screener column.
- **This is also why the after-hours match was real rather than lucky.** After the close, "cumulative to now" and "the whole day" are nearly the same quantity, so `relative_volume_10d_calc` and the app's figure converge. Before the open they diverge by orders of magnitude, and no amount of recalibration closes that: one is normalised per ticker and the other is not.
- **⛔ `premarket_volume / average_volume_10d_calc` is NOT a usable substitute — it was proposed here in error and is retained only as a warning.** It divides by a normal full day instead of by this ticker's usual pre-market. For GNRC it reads **0.267** where the app reads ~**100**; the implied denominators are 840,640 shares against roughly 2,247, a factor of **374**. The gap is not a scale offset that a recalibrated floor absorbs, because the ratio between the two denominators differs per ticker — it is exactly the per-ticker normalisation the measure exists to provide. A floor on it ranks stocks by pre-market volume relative to their daily size, which systematically favours names that habitually trade before the open.
- **The correct implementation already exists in this repo, twice.** `src/bidask/rvol_at_time.py` computes precisely this definition but anchors at 09:30 over regular-session bars; `src/reporting/ep_scan_common.py:calculate_rvol_at_time` already treats 04:00–20:00 as one continuous session, which is the anchor the app uses. Serving pre-market means reusing that computation, not finding a vendor column.

**The chart websocket supplies the missing bars, and it removes the Alpaca dependency entirely (measured 2026-09-17).**

- **Studies cannot be pulled, but bars can.** Authenticating the chart socket at `data.tradingview.com/socket.io/websocket?from=chart/` with the same `sessionid`-minted JWT the quote socket already uses, `resolve_symbol` with `"session": "extended"` plus `create_series` returns 2,500 five-minute bars covering roughly eleven extended sessions. A built-in `Volume@tv-basicstudies` study also returns values, but the Relative Volume at Time study is refused with `Study not allowed in this connection` on both `data` and `prodata` hosts — it is Pine-based, and Pine studies are blocked on this connection tier. Do not spend time hunting for a study id; the bars are the route.
- **⛔ These bars carry real extended-hours volume, which is the whole reason Alpaca was in this plan.** For GNRC: 53 pre-market bars totalling 239,052 shares today, against 0–14 bars and 200–5,724 shares on each of the ten prior sessions. yfinance returns the same bars with zero volume on every one, which is what made a second vendor look necessary.
- **The computation reproduces the app.** Cumulative from 04:00 over the 10-session mean to the same clock time gives **31.19 at 10:21 ET** for GNRC, against **33.50** read off the user's chart minutes earlier while the plot was declining through the open. The historical pre-market means also confirm the denominator back-solved from the user's ~100 pre-market reading (~2,247 shares) against measured history of 200–5,724.
- **Cost and shape.** One series request per symbol, so this replaces yfinance in the once-per-session baseline warm-up rather than running per poll. The live numerator still comes from the screener's `volume` / `premarket_volume` in the single universe request. **Throughput across the ~1,900-ticker universe is unmeasured** — the yfinance warm-up it replaces takes about 1.7 minutes, and this must be timed before the launch-before-the-bell assumption is relied on.
- **`premarket_volume` and `premarket_change` are live**, rising for 262 of 300 over the gap and present on 70.6% of rows, the absentees being names with no pre-market trade. R3's price test is therefore a direct field test.
- `postmarket_volume` is live and non-null on 86.8% of rows, so a locally-computed numerator also exists if one is ever needed. Post-market volume over a 30-day average full day was tested as a standalone gate and is too weak to ship — median 0.1624, so half the market clears 16% of a normal day after hours, and a 0.25 floor admits 410 names of which only 10 moved 3%. Recorded so the next reader does not re-derive it.

- Prior art for the module being replaced: `docs/plans/2026-08-08-001-feat-bid-ask-tape-dashboard-plan.md`.

## Planning Contract

### Key Technical Decisions

- KTD1. **Both columns come from a stateless per-poll evaluation of price direction and relative volume; trade-side classification is removed.** (session-settled: user-directed — chosen over keeping the CLNV classifier alongside the new gate: the user judges price and volume more reliable, and carrying both would leave the board's columns sourced from a signal the Problem Frame argues against.) Governs R1–R6, R16.

- KTD2. **Relative Volume at Time is computed in-app in every session state, from TradingView data only.** (session-settled: user-directed — chosen over reading a different validated vendor field per session state: one series on one scale, and no second data vendor.) The vendor columns are not wrong where they were validated, but they are three different quantities across the three windows, and the measurements below show how badly that misleads — `relative_volume_10d_calc` agrees with the app to 3% after the close and is off by 374x before the open. One computed series removes that class of error entirely, puts every window on the scale the user's floors were written against, and collapses KTD4. Governs R7.

  What this costs, stated plainly: the regular session and after hours currently need no warm-up and would acquire one. That is the price of the consistency, and it is accepted. **The residual vendor read is the live numerator only** — `volume` and `premarket_volume` from the universe request already being made — which is a raw quantity rather than a derived ratio and carries none of the anchor ambiguity. Chosen over computing it in-repo from `src/bidask/rvol_at_time.py` on every poll, which is the module the repo built precisely because it did not trust this field: the vendor figure arrives inside the one universe request already being made, at zero added latency and no warm-up, where the in-repo route costs a ~1.7-minute download before the board can gate at all. The measurement above is what reverses the repo's standing caution — exact rank agreement across ten names — and the earlier probe that produced that caution was taken after the close, where the two measures are not comparable. The guard is not optional: the field is an unlisted alias in the same class as `Value.Traded`, and a vendor that starts returning null would otherwise empty the board silently. When the column is absent or null for the whole response, the poll reports an unavailable relative-volume source under R18 rather than an empty market. The in-repo module stays in the tree as the cross-check that keeps R7 honest and as KTD3's second rung. Governs R7.

- KTD3. **One definition, applied identically in every session state: cumulative volume from the 04:00 extended-session anchor, over the mean of the same 04:00-to-now sum across the last 10 sessions.** This is what `ta.relativeVolume(10, "1D", cumulative)` plots on an extended-hours chart, and it is what the user's floors mean. The regular session is not a special case — it is the same running total, seen later in the day.

  **Bars come from the TradingView chart websocket, not yfinance and not Alpaca.** Authenticate at `data.tradingview.com/socket.io/websocket?from=chart/` with the JWT the quote socket already mints from `sessionid`, then `resolve_symbol` with `"session": "extended"` and `create_series`. Measured 2026-09-17: 2,500 five-minute bars, roughly eleven extended sessions, carrying **real extended-hours volume** — 53 pre-market bars and 239,052 shares for GNRC today against 200–5,724 on each of the ten prior sessions. yfinance returns those same bars with zero volume on every one, which is the sole reason a second vendor ever appeared in this plan.

  **Verified against the app end to end:** the computation gives 31.19 for GNRC at 10:21 ET where the user's chart read 33.50 minutes earlier on a declining curve.

  **⛔ Do not try to pull the study itself.** `create_study` for Relative Volume at Time is refused with `Study not allowed in this connection` on both the `data` and `prodata` hosts — it is Pine-based and Pine studies are blocked on this connection tier, while a built-in `Volume@tv-basicstudies` study returns values normally. The refusal is about the study class, not the route or the credentials. The bars are the way in.

  **Baselines are built once per session and cached**, exactly as the existing warm-up does, because they depend only on completed sessions. A same-day restart reuses the cache; a cache from another session is discarded, never reused. Each poll then costs one division against the live `volume` / `premarket_volume` already in the universe request. Governs R7, R9, R10, R11, R14.

- KTD4. **The floors R8, R9 and R10 state are used exactly as written, with no per-state recalibration.** KTD2's single series is what makes this true: one definition on one scale across every session state, and it is the same figure the user reads off the TradingView app, so 3.0 pre-market and 1.5 after hours mean what they were intended to mean. Earlier revisions of this plan carried a rule for translating floors between series and a proposal to rescale R9 by three orders of magnitude; both existed only because two different series were in play, and both are retired. Governs R8, R9, R10.

- KTD5. **A theme scores the mean of its top three members' relative volume, after each member's value is capped at a configurable ceiling defaulting to 5.0; a breadth term then adds only.** The cap is what makes the user's top-three mean survive contact with the data: DLXY at 2090.8 would otherwise hand its theme a score three orders of magnitude above every rival and freeze the ranking. 5.0 sits above the measured p99 of 4.329 and binds only 15 rows of 1,776, so it preserves ordering for better than 99% of the board while bounding the tail that would otherwise decide it. Chosen over cross-sectional percentile ranking, which the L1 Radar uses for VARS and which would also defuse the tail: percentile discards the readable units — "this theme's leaders are running 2.4x normal" — and a trader reading this board is comparing against a remembered sense of normal volume, not against today's cross-section. The breadth term is a share of qualifying members rather than a count, following the same reasoning that governs the coil strip and the SI tab: rosters run from 2 to 214 members, so a count ranks by roster size. Governs R12, R13.

  Three parameters make that share computable, and without them two implementers produce different boards. The **denominator** is the group's members present in the poll's liquidity-filtered universe, measured before the relative-volume gate — the gate removes every non-qualifying row, so a denominator taken after it is 1.0 for every group and the term does nothing. Taking it pre-gate is also what gives the industry-fallback groups R17 preserves a denominator at all, since those have no taxonomy roster. A **minimum qualifying-member count** below which the term is zero, mirroring the coil strip's own floor; share alone hands a one-member group the maximum. And a named **config key with a default**, the way the radar's β and the coil marker's γ are named. How large that coefficient should be is left open below: it sets the balance between breadth and intensity, which decides whether a single strong name can top the board, and that is the user's call rather than a derivable one.

- KTD6. **Price direction is read from the feed's own change fields rather than reconstructed from raw prices.** `change` against the previous close and `change_from_open` against the session open answer R2 directly, `premarket_change` answers R3, and `postmarket_change` is expected to answer R4 pending Q2. Deriving the previous close by dividing `close` by `(1 + change)` would introduce a division whose failure mode on a null or zero field is a wrong sign rather than an absent one. Governs R2, R3, R4.

- KTD7. **Session state comes from `current_session`, and an unmapped value is treated as closed, not as open.** `SESSION_LABELS` in `src/bidask/feed.py` already carries the mapping and already records that the live regular-session value is `market` rather than the `regular` that was once assumed. A new state selects both a reference price and a volume floor, so an unrecognised value falling through to a default would silently apply the wrong pair of rules. Governs R1.

- KTD8. **The trailing hit window, the winsorization of volume deltas, and the per-ticker accumulator state are removed rather than repurposed.** All three exist to bound a bias that arises from summing many classified observations. A stateless evaluation has no accumulation and therefore no such bias, so retaining them would carry complexity that no longer answers a question. Governs R16.

### High-Level Technical Design

Directional only. The implementer decides module boundaries.

**Poll pipeline, before and after.**

```
BEFORE  screener ──┐
                   ├─> merge quotes ─> classify(cur, prev, prior_price)
        websocket ─┘        │                  │
                            │                  v
                            │          accumulate into 30-min window
                            │                  │
                            v                  v
                     in-play gate ────> margin = ask_hits - bid_hits
                                               │
                                               v
                                   group score = SUM(margins)

AFTER   screener ─> session state ─> reference price + rvol floor
                          │                    │
                          │                    v
                          │            gate: rvol >= floor
                          │                    │
                          │                    v
                          └──────────> side(s): strong | weak | both
                                               │
                                               v
                              group score = mean(top-3 capped rvol)
                                             + breadth term
```

**Session state decides two things at once.** Keeping them in one table is what prevents a state from acquiring a reference price without a floor, or the reverse.

| `current_session` | Reference for strong | Volume floor |
|---|---|---|
| `pre_market` | previous session close | 3.0 |
| `market` | session open **or** previous close | 0.7 / 1.0 / 1.2 stepped by elapsed minutes |
| `post_market` | regular session close — confirmed, `postmarket_change > 0` | 1.5, but **not** on the vendor field; it is frozen after the close (KTD3) |
| `extended` | not observed in the after-hours probe; resolved by U1 if it ever appears | resolved by U1 |
| `out_of_session`, holiday, unmapped | none — board declares closed | n/a |
| crypto, always | 24-hour reference | 1.2 |

**Column assignment is two independent tests, not a branch.** Strong and weak are evaluated separately and a ticker carries whichever sides it earns. This is what R5 means by intentional double membership, and it is why the natural `if/elif` shape is wrong here.

### Risks and Dependencies

| Risk | Why it is real here | Mitigation |
|---|---|---|
| `relative_volume_intraday\|5` is an unlisted alias that may return null without notice | The catalogue lists it as stock and ETF only, yet it serves both markets — the same shape as `Value.Traded`, which was never in metainfo, served for months, and then emptied the board for a full session | KTD2's per-poll fail-closed guard, surfaced through R18 as an unavailable source rather than a quiet market |
| Floors calibrated on one series get applied to another | The vendor field and the in-repo computation agree on rank but differ 2–6% in level, and the residual tracks pre-market share at +0.82 | KTD4 binds each floor to its own source and records which in config |
| The right relative-volume field differs by session state, and the wrong one looks identical | Measured: after the close the `intraday` field is frozen while the 10-day field is live; picking by name admits 5 of 11 genuine after-hours movers instead of 8 | KTD3 selects per state from measurement, and KTD4 calibrates each floor against its own series |
| A vendor field can be fully populated, plausible, and static | The frozen figure reads non-null on 2,805 of 2,805 rows and looks healthy at every glance; only a two-poll comparison over 240s exposed it | Freshness is checked, not assumed — a reading that does not move while its own volume does fails closed |
| Pre-market has no vendor relative-volume field | Measured: all seven replay yesterday's session, and a 3.0 floor on one of them surfaces the stocks that were busy yesterday — populated, plausible, and wrong | KTD3 gates that window on a ratio derived from the live `premarket_volume` instead, with its own floor (Q8) |
| A field that matches in one session state can be a different quantity in another | `relative_volume_10d_calc` matched the app to 3% after the close and is off by 374x before the open, because full-day and to-this-minute denominators converge at 16:00 and diverge at 09:00 | Each state's source is validated inside that state; a match in one window is never carried into another |
| The warm-up is untimed and now every window depends on it | One computed series means the regular session and after hours acquire a warm-up they did not previously need, so a slow one darkens the whole board rather than one window. It replaces a ~1.7-minute yfinance warm-up and needs one series request per symbol across ~1,900 tickers | Time it against the full universe as the first task of U5. A slow warm-up is answered by a smaller universe, an earlier launch, or a longer cache life — not by a wrong number, since an absent baseline scores 0 and fails closed |
| The bar source and the quote source share an auth path | Both mint a JWT from the same `sessionid`; a cookie expiry or a TradingView auth change takes out quotes and baselines together | Accepted — it is one vendor by deliberate choice. The failure is loud (auth error, not silence) and the cache means a session already warmed keeps its baselines |
| Deleting three subsystems at once leaves a half-removed state | U6 removes the classifier, the quote socket, and the accumulator together | U6 runs last, behind the units that replace them, and the Definition of Done requires no surviving imports |
| A 1.2 floor is a judgment, not a measured optimum | Measured yield at the shipped thresholds is 401 of 1,776 rows mid-session and 38 at the pre-market floor, but nothing here shows those cutoffs select better tickers | Recorded as such in U9's documentation; the forward-return question is deferred in Scope Boundaries rather than implied to be settled |

**Dependencies.** A valid `TRADINGVIEW_SESSIONID` and `TRADINGVIEW_SESSION_SIGN` pair, without which the feed serves 15-minute-delayed prices and the board is a historical curiosity. Alpaca credentials only if KTD3 resolves to its second rung (Q3). No new Python packages: every field used here comes through the `tradingview_screener` calls the app already makes.

### Assumptions

- The user's "first 5 min / 15 min / 30 min" bands describe floors that take effect at those marks and hold until the next, matching the existing `in_play_rvol_schedule` semantics. The stated 30-minute and rest-of-session floors are both 1.2, so the schedule has three distinct steps, not four.
- "After market" means the post-close extended window, not the overnight closed period. When the feed reports the market fully closed, the board reports closed rather than applying the 1.5 floor to a frozen tape.
- The equity liquidity floors that decide what is worth polling — average volume, average dollar volume, today's traded value — are unchanged. The redesign replaces the in-play display gate, not the universe.

### Sequencing

U1 runs first and gates every other unit. An earlier draft let U2, U3, U4 and U7 proceed alongside it on the grounds that they rest only on regular-session findings; that is true of their content and false of their consequences. U1 also settles the vendor field's volume anchor, the `extended` session label, and the crypto reading — each of which reaches those units — and a stop-and-report outcome after they landed would leave the board half-migrated with the retired classifier still imported. One probe window is cheaper than that state. U8 follows U3 because it reuses the gate U3 builds; U9 comes last because it describes shipped behavior.

## Implementation Units

### U1. Verify extended-hours data availability

**Goal.** Resolve Q1, Q2 and Q4 with live measurements, pick the rung of KTD3's ladder the extended-hours work will stand on, and record the distributions the floors are set against.

**Requirements.** R9, R10, R14; resolves Q1, Q2, Q4. Cited decisions: KTD2, KTD3, KTD4.

**Files.** A throwaway probe script outside the repo tree; no production file changes. Findings land in this plan's Sources and in U9's documentation.

**Approach.** Five measurements, each with a valid session cookie.

1. **Data-source verification — complete.** Both extended-hours windows were measured live, the chart websocket was confirmed to serve extended-hours bars with volume, and the in-app computation was reproduced against the app (see Sources). Q1, Q2, Q3, Q5 and Q9 are closed. What this unit still owes is the crypto reading in step 5 and the warm-up timing that U5 now carries.
2. **Extended-hours distributions.** In each window, run the probe across the whole liquidity-filtered universe and record the median, p90, p99 and the row count admitted at 3.0 and at 1.5. The only yield figures measured so far were taken 140 minutes into the regular session, so nothing yet says what those floors admit in their own window.
3. **The vendor field's volume anchor.** Compare `relative_volume_intraday|5` against `src/bidask/rvol_at_time.py` on names with a large pre-market share of volume. The 2–6% residual already measured correlates with that share at +0.82, which is the signature of an anchor that is not 09:30 — and if it counts pre-market volume, the regular-session gate admits names on participation that happened before the session.
4. **The retired after-close reading.** Re-run the AVGO comparison that produced the repo's standing caution. A cumulative measure converges on the full-day figure at the close, so the old reading was comparable after all and is still unexplained. One request settles whether U9 may delete that caution or must record it beside the new evidence.
5. **The session labels and the crypto reading.** Record the literal `current_session` value the feed emits in each window, including whether `extended` ever appears, and what `relative_volume_intraday|5` anchors to on the crypto scanner along with its value distribution and the count 1.2 admits.

**Execution note.** This is a measurement, not a build. Record what was observed including null results; a field that is null outside the regular session is a finding that selects rung two, not a failure.

**Test scenarios.** None — this unit is investigative and ships no behavior. `Test expectation: none -- investigative spike, findings recorded in the plan and in U9's documentation.`

**Verification.** Q1, Q2 and Q4 each carry a recorded answer with the measurement behind it; KTD3's rung is named per session state; the pre-market, after-hours and crypto distributions are recorded; and the `extended` row of the session-state table is filled or struck. Q2 and the after-hours half of Q1 are already answered in Sources; the pre-market probe and the crypto reading remain.

**Every availability check in this unit is a two-poll freshness test, not a null check.** The after-hours result is the reason: a field reading non-null on 100% of rows was carrying a number that had stopped moving hours earlier, and nothing in a single response distinguishes the two.

### U2. Session state and reference-price resolution

**Goal.** Turn one poll's row into a session state, the reference price or prices that state uses, and the side or sides the ticker earns.

**Requirements.** R1, R2, R3, R4, R5, R16. Cited decisions: KTD6, KTD7.

**Files.** `src/bidask/session_state.py` (new), `src/bidask/feed.py` (add `relative_volume_intraday|5`, `open`, `change_from_open`, `premarket_change`, `premarket_close`, `premarket_volume`, `postmarket_change`, `postmarket_close`, `postmarket_volume` to `EQUITY_COLUMNS`; drop `bid` and `ask` from that same equity list), `tests/test_bidask_session_state.py` (new).

The relative-volume column belongs here because this is the only unit that edits the equity select list, and every floor in U3 reads it. Dropping `bid`/`ask` is scoped to the equity list: the crypto scanner does publish a book, and U6 removes the crypto pair along with the classifier that reads it.

**Approach.** A pure function from a row plus a session state to a pair of booleans, so both column tests are independently exercisable. Strong and weak are evaluated separately per the High-Level Technical Design, never as mutually exclusive branches. Follow `SESSION_LABELS` for the state mapping and treat an unmapped value as closed.

**Test scenarios.**
- Regular session, price above open and above previous close: strong only.
- Regular session, price below open and below previous close: weak only.
- Regular session, gapped down and recovering — above open, below previous close: both sides, each carrying its reference marker. This is AE1.
- Regular session, gapped up and fading — below open, above previous close: both sides.
- Regular session, price exactly at open and exactly at previous close: neither side; a zero move is not a direction.
- Pre-market: only the previous close is consulted; the open is ignored even when present on the row.
- After hours: only the regular session close is consulted.
- `current_session` reads `out_of_session`, a holiday value, and an unmapped string: each yields no side and a closed state.
- A row whose reference field is null or non-finite yields no side rather than a defaulted one.

**Verification.** `uv run python -m unittest tests.test_bidask_session_state`.

### U3. Relative-volume gate

**Goal.** Admit only tickers meeting the floor for the current session state, and reject an unknown rather than defaulting it.

**Requirements.** R6, R7, R8, R11, R16, R18. Cited decisions: KTD2, KTD4.

**Files.** `src/bidask/universe.py` (replace `apply_in_play`), `src/bidask/rvol_at_time.py` (extend `threshold_for` for per-state schedules), `src/bidask/config.py` (new keys; raising guards on `in_play_min_change_pct` and the existing single schedule), `src/bidask/server.py` (KTD2's availability guard and the state-payload block it writes; remove the poll-loop baseline warm-up), `config/workflow_config.yaml`, `tests/test_bidask_universe.py` (rewrite), `tests/test_bidask_rvol_at_time.py` (extend).

**Approach.** Extend the existing stepped-schedule mechanism to carry one schedule per session state rather than one schedule overall. Keep the established convention that the first band also covers the minutes before its mark and that a malformed entry raises. Retire `in_play_min_change_pct` with a raising guard rather than ignoring it, matching how `in_play_min_rvol` was retired — a silently dropped key is how the previous gate's volume leg went missing.

KTD2's guard lives here rather than in a rendering unit, because a browser can only report a cause the state payload carries. The same edit removes the per-session baseline warm-up the poll loop starts: on KTD3's first rung nothing reads those baselines, and a ~1.7-minute download at every launch feeding a gate that no longer consults it is exactly the abandoned code the Definition of Done forbids. `src/bidask/rvol_at_time.py` itself stays — it is KTD2's offline cross-check and KTD3's second rung. If U1 selects that rung, U5 restores the warm-up for the extended-hours baselines alone.

**Test scenarios.**
- A ticker at 12% change and 0.4x relative volume is excluded. This is AE2.
- Regular-session floors apply at 0.7 before and at 5 minutes, 1.0 at 15, 1.2 at 30, and 1.2 at 200 minutes.
- Pre-market at 2.5x is excluded and at 3.1x admitted. This is AE3.
- After hours at 1.4x is excluded and at 1.6x admitted.
- A ticker with no relative-volume value scores zero and is excluded, never admitted as unknown.
- A config carrying `in_play_min_change_pct` raises with a message naming the replacement.
- A malformed schedule entry raises rather than being skipped.
- A response whose relative-volume column is absent, or null on every row, publishes an unavailable-source reason in the state payload instead of an empty board.
- A response whose relative-volume column is null on only some rows keeps publishing, and the payload carries the count of rows that had a usable value against the count polled.
- No baseline warm-up runs on KTD3's first rung, and the payload carries no warm-up status to report.

**Verification.** `uv run python -m unittest tests.test_bidask_universe tests.test_bidask_rvol_at_time`.

### U4. Theme scoring and column assembly

**Goal.** Rank themes by the relative volume of their strongest members, bounded so one extreme reading cannot decide the board.

**Requirements.** R12, R13, R17. Cited decisions: KTD5.

**Files.** `src/bidask/grouping.py`, `config/workflow_config.yaml`, `tests/test_bidask_grouping.py` (rewrite).

**Approach.** Replace the summed-margin score with a capped top-three mean plus an additive breadth term keyed on the share of qualifying members. Keep `_leaves_for`, the industry fallback, the per-group and per-column caps, the new-high/new-low badge `highs.py` supplies, and the `truncated` tally untouched — R17 preserves them and `tests/test_bidask_column_meta_markup.py` pins their markup.

**Both columns now sort descending.** `build_columns` passes `reverse=False` for the weak side, which was right while the score was a signed margin and is backwards once it is a capped relative volume that is never negative: ascending puts the least active theme at the top of the weak column and truncates away the heavily distributed ones that column exists to show. The same applies to the member sort inside each group.

**Test scenarios.**
- A theme with one 40x member and two at 1.0x ranks below a theme with three members at 3.5x. This is AE4.
- A theme with three qualifying members outranks an otherwise identical theme with one, through the breadth term alone.
- The breadth term never lowers a score: a theme with no additional qualifying members scores exactly its capped top-three mean.
- A theme with fewer than three members scores on the members it has rather than being excluded or zero-padded.
- A ticker tagged into two leaves of one theme counts once toward that theme's breadth.
- The cap is applied before the mean, not after it.
- The weak column's top entry is its highest-scoring theme, and members within each group run highest first on both sides.
- The breadth denominator is taken before the relative-volume gate, so an industry-fallback group with no taxonomy roster still gets one.
- A group with fewer qualifying members than the minimum scores no breadth term at all.
- Column and group caps, the industry fallback, the new-high/new-low badge, and the `truncated` totals are unchanged from current behavior.

**Verification.** `uv run python -m unittest tests.test_bidask_grouping tests.test_bidask_column_meta_markup`.

### U5. Extended-hours relative volume

**Goal.** Compute Relative Volume at Time in-app for every session state, from TradingView bars.

**Requirements.** R7, R9, R10, R11, R14. Cited decisions: KTD2, KTD3.

**Files.** `src/bidask/tvbars.py` (new — chart-websocket bar fetcher), `src/bidask/rvol_at_time.py` (re-anchor from 09:30 to 04:00; replace the yfinance `fetch_bars` with the new source), `src/bidask/server.py` (warm-up path), `tests/test_bidask_rvol_extended.py` (new), `tests/test_bidask_rvol_at_time.py` (extend).

**Approach.** The bar fetcher reuses `tvquote.py`'s auth and frame codec rather than restating them — same JWT mint, same `~m~` framing — and differs in the socket path, the `chart_create_session` / `resolve_symbol` / `create_series` sequence, and `"session": "extended"` on the symbol spec. Keep that shared code shared; two copies of the frame parser will drift.

`rvol_at_time.py` keeps its structure and changes its anchor: `SESSION_OPEN_MIN` moves from 09:30 to 04:00 and the bar grid widens to the 04:00–20:00 extended day. The cache shape is unchanged apart from a version bump — versioned, session-dated, and discarded rather than reused across sessions, since a baseline from another session is silently wrong for every ticker rather than visibly absent.

**Execution note.** Validate against the app before wiring it to the gate: pick a name with unusual pre-market participation, compute, and compare with the chart. The first version of this measurement was wrong by a factor of 374 and looked plausible, so a number that merely seems sensible is not evidence.

**Test scenarios.**
- A pre-market reading is computed against the 04:00-anchored baseline, not against a regular-session curve.
- A ticker with unusual pre-market volume against a quiet history scores far above 1, rather than the near-zero a full-day divisor would give.
- The same ticker's reading falls when the regular-session open enters the historical mean, matching the discontinuity the app shows at 09:30.
- An after-hours reading continues accumulating past 16:00 rather than freezing.
- A cache written for a different session date is discarded, not reused.
- A symbol with no baseline scores 0 and is excluded, never admitted as unknown.
- A bar response carrying zero volume on every extended-hours bar is rejected as unusable rather than producing a baseline of zero — that is what yfinance returns, and a zero baseline would make every ratio infinite.

**Test scenarios.**
- A pre-market ratio is computed against a pre-market baseline, never against the regular-session curve.
- An after-hours ratio is computed against an after-hours baseline.
- A cache written for a different session date is discarded, not reused.
- A symbol with no extended-hours baseline scores zero and is excluded.
- With the extended-hours source unavailable, the gate excludes every ticker and the state payload reports the source as unavailable rather than reporting a quiet market.
- The regular-session path is unchanged by the addition.

**Verification.** `uv run python -m unittest tests.test_bidask_rvol_extended tests.test_bidask_rvol_at_time`, then a live pre-market or after-hours run confirming non-empty columns with plausible values.

### U6. Remove the classifier, the quote socket, and the accumulator

**Goal.** Delete the machinery the redesign makes unnecessary, so no second definition of strong and weak survives.

**Requirements.** R16. Cited decisions: KTD1, KTD8. **Depends on U2, U3, U4; and on U5 to avoid deleting a fallback the extended-hours path still needs.**

**Files.** Delete `src/bidask/classify.py` and `tests/test_bidask_classify.py`. **Keep `src/bidask/tvquote.py`'s auth and frame codec** — `_auth_token`, `encode`, `iter_frames` and the header constants, which U5's bar fetcher depends on — and remove only the quote machinery built on top: `QuoteStream`, `Quote`, `merge_quotes`, and the quote-field subscription. Trim `tests/test_bidask_tvquote.py` to the surviving helpers rather than deleting it. Reduce `src/bidask/session.py` to the per-poll snapshot the new board needs, or remove it if U2 and U4 leave it empty. Update `src/bidask/server.py` to drop the quote stream, the auction windows, and the `quotes` state block. Rewrite `tests/test_bidask_session.py` and `tests/test_bidask_server.py`.

If U5 lifted the shared helpers into their own module, this unit deletes what is left of `tvquote.py` instead. Either shape is fine; what must not happen is a second copy of the frame parser.

**Approach.** Delete rather than deprecate. Retiring the quote socket also retires the auction-window exclusion, which existed because auction prints have no meaningful contemporaneous quote — a price-direction test has no such problem. Remove the now-unreachable config keys with raising guards rather than silently ignoring them.

**Execution note.** This is deliberately one unit rather than three. The classifier, the quote socket, and the accumulator are mutually entangled — the socket exists only to feed the classifier, and the accumulator exists only to bound the classifier's bias — so removing them separately leaves intermediate states where the board has two definitions of strong and weak. Land it whole or not at all.

**Test scenarios.**
- The state payload carries no `quotes` block and the server starts with no quote stream.
- No import of `classify` or `tvquote` remains anywhere in `src/` or `tests/`.
- A poll inside the former opening-auction window produces normal output rather than an auction rejection.
- A retired config key raises with a message naming its replacement.
- The server still writes state atomically and still refuses a git-tracked output directory.

**Verification.** `uv run python -m unittest tests.test_bidask_server tests.test_bidask_session`, plus a repository-wide search confirming no surviving references.

### U7. Dashboard rendering

**Goal.** Show the new signal, mark the both-column case, and let a thin board state its own cause.

**Requirements.** R5, R12, R14, R15, R18, R19. Cited decisions: KTD3, KTD5.

**Files.** `src/bidask/web/app.js`, `src/bidask/web/index.html`, `src/bidask/web/style.css`, `src/bidask/server.py` (publish the session state, the floor in force, and the relative-volume coverage pair the browser renders), `tests/test_bidask_column_meta_markup.py` (extend).

**Approach.** Five passes over the board, not one.

1. **Chips.** Replace the ask/bid hit counts with the relative-volume reading and the percentage move.
2. **The R5 marker is a short text label, not a tint.** R5 and its gapped-down acceptance case require the marker to *name* the reference that placed the ticker on each side, and a background tint cannot carry a name — the chips already take a green or red tint from their column, so a third would be unreadable even if naming were not required. The never-a-layout-property rule belongs to the main dashboard, where chip width feeds a wrapping measurement; this app has no such measurement and does not inherit the constraint.
3. **Client-side score and sort.** `renderColumn` recomputes each group's score after the user's sliders by summing `margin`, a field that no longer exists, and sorts the weak side ascending. Recompute it as KTD5's capped top-three mean with the breadth term, and sort both columns descending, for the reason U4 gives.
4. **Remove the surfaces whose data is gone.** The buying/selling pressure bar reads the per-ticker totals KTD8 deletes and would otherwise freeze at a 50/50 split forever, which is absence rendering as a claim of balanced flow. The `min hits` slider and the `hide low-confidence` and `hide divergent` checkboxes read counters that no longer exist and fail open on `undefined`, so they would sit on screen responding to nothing. The window pill goes with the window.
5. **Say what is happening.** Extend `emptyReason` to distinguish a failed fetch, an unavailable relative-volume source, a warm-up in progress on KTD3's second rung, and a quiet market. Add a coverage pill — rows with a usable relative-volume value over rows polled — rendered every poll rather than only when a column empties, replacing the quote pill U6 removes; partial nulls are routine on this feed, and without it a shrinking board looks like a calm one. Add a pill naming the floor in force and the session state that selected it, so the step from 0.7 to 1.0 to 1.2 does not drop a block of tickers with nothing on screen to explain it.

**The board's own copy is part of R19.** The two column headings still read "offers being lifted" and "bids being hit", the footnote paragraph explains hit counts and dashed borders, and the truncation tooltip describes the score as the sum of member margins. All three assert the mechanism KTD1 removes, in the one place a user actually reads, and R19's rule that retired text is removed rather than left beside its replacement applies to them before it applies to any file in the repo.

**Test scenarios.**
- A ticker present in both columns renders a readable label on each side naming the reference that placed it there.
- The weak column's top group is its highest-scoring one, and no surviving code sums a `margin` field.
- An empty column with an unavailable relative-volume source says so, distinctly from a quiet market.
- A partially-null relative-volume column still renders, and the coverage pill shows the usable count against the polled count.
- An empty column with a failed fetch reports the fetch failure.
- On KTD3's second rung, an empty column during warm-up states that the warm-up is running. This is AE5. On the first rung the branch is unreachable and is not shipped.
- The floor pill changes when the schedule steps from one band to the next.
- No surviving control or display reads the retired per-ticker counters, and the pressure bar, the `min hits` slider, the two hide checkboxes and the window pill are gone.
- No column heading, footnote, or tooltip describes trade-side classification, hit counts, or a summed-margin score.
- The crypto tab labels its 24-hour reference and shows no session-state control.
- The no-data branch of the column meta resets its class, so one tab's amber state cannot leak into another.

**Verification.** `uv run python -m unittest tests.test_bidask_column_meta_markup`, then a live board inspected in the browser at the market's current state.

### U8. Crypto path

**Goal.** Apply the flat floor and the 24-hour reference without borrowing the equity session machinery.

**Requirements.** R14, R15. **Depends on U1** for what the vendor field anchors to on a market with no session open, and for the distribution 1.2 is being applied to.

**Files.** `src/bidask/feed.py` (`fetch_crypto`), `src/bidask/universe.py`, `src/bidask/server.py`, `tests/test_bidask_universe.py`.

**Approach.** Route crypto to a single 1.2 floor with no schedule and no session state. The crypto scanner does serve `relative_volume_intraday|5` despite the catalogue listing it as stock and ETF only, so KTD2's fail-closed guard matters more here than on the equity path, not less: this is an unlisted field on an unlisted market, and nothing yet establishes what "relative volume at time" anchors to when there is no session to date from. Keep the existing stablecoin exclusion and the existing 24-hour volume labelling.

`poll_once` currently calls `build_universe` with `in_play` set only for equities, so crypto rows never reach the gate at all. Without that change R14 passes its unit test and does nothing on the running board — a green test beside an unfiltered tab, which is worse than a visible failure. The Problem Frame's two arguments are equity-specific and do not motivate this change: the crypto scanner does publish a book, and the market never closes. Crypto moves for consistency with the equity board, not because its quote data was thin.

**Test scenarios.**
- A crypto row at 1.1x is excluded and at 1.3x admitted, at every hour of the day.
- The crypto poll actually runs the gate: a row below the floor is absent from the rendered board, not merely from a unit-test call.
- No elapsed-minutes value influences the crypto floor.
- A null relative-volume column on the crypto response excludes every row and reports an unavailable source rather than an empty market.
- Excluded stablecoins stay excluded.

**Verification.** `uv run python -m unittest tests.test_bidask_universe`, then a live crypto tab check outside US market hours.

### U9. Documentation correction

**Goal.** Leave the repository describing what it now does.

**Requirements.** R19.

**Files.** `CLAUDE.md` (the Tape Pressure Dashboard section), `CONCEPTS.md` (the tape-pressure entries at lines 127–158 and the feed-state note at 177).

**Approach.** Remove the passages describing the classifier, the quote websocket, the trailing window, position-in-spread, and the summed-margin score, and replace them with the shipped rules and the measurements in this plan's Sources. Follow the existing convention in that file of recording what was measured and what was not. State plainly that the redesign rests on a data-quality argument and the user's judgment, and that no forward-return measurement supports it — the deferred item in Scope Boundaries names that gap and the documentation must not imply otherwise.

The standing caution about `relative_volume_intraday|5` is replaced only if U1's re-run of the retired after-close comparison explains it. If that reading stands unexplained, record both it and the mid-session rank agreement rather than deleting one in favour of the other: this file's own rule is that a superseded validation is worse than none, and deleting a contrary measurement on the strength of a newer one taken under different conditions is how that happens.

**Test scenarios.** `Test expectation: none -- documentation only, no behavior.`

**Verification.** No surviving reference to the removed mechanisms describes them as live behavior; `CONCEPTS.md` entries match the shipped vocabulary.

## Verification Contract

```bash
uv run python -m unittest discover -s tests
```

The full suite must pass, not only the bidask tests — `src/themes/theme_registry` and `config/settings` are shared, and `tests/test_bidask_column_meta_markup.py` pins markup joins across three files.

Live checks, each against a real market state, because none of this is provable from fixtures alone:

1. Regular session. Launch `scripts/launch_tape_pressure.bat`, confirm `update_mode` reads `streaming` and the feed pill agrees, and confirm both columns populate with plausible members.
2. Both-column case. Find a gapped-down recovering name and confirm it renders in both columns, each side labelled with the reference that placed it there.
3. Column ordering. Read the top three groups of each column against the payload and confirm the weak column leads with its highest-scoring theme rather than its lowest. A markup test cannot catch an inverted sort, and this is the one check that does.
4. Pre-market and after hours. One launch in each window, confirming the correct floor applies, the correct reference decides sides, and the state banner names the session.
5. Degraded path. Run once with the extended-hours source unavailable and confirm the board says so rather than showing empty columns.
6. Crypto outside US market hours, confirming the flat floor actually removes rows from the rendered board and that the 24-hour reference is labelled.

The board publishes nothing, so no `docs/data/` reset applies. Confirm `git status` is clean of `scripts/local_runs/` before committing.

## Definition of Done

**Global.**

- Every requirement R1–R19 is implemented or explicitly deferred in Scope Boundaries.
- The full test suite passes.
- All six live checks in the Verification Contract have been run and their results recorded, including any that failed.
- No import of `classify` survives anywhere in the repository, nothing imports the retired quote machinery, and nothing reads the retired per-ticker counters — including the board's own controls and status surfaces. The shared websocket auth and frame codec do survive, in exactly one place.
- Q4, Q6 and Q7 have been answered or explicitly deferred with their consequence named. Q1, Q2, Q3, Q5, Q8 and Q9 are already closed in the plan and need no further action.
- Nothing on screen or in the repository describes trade-side classification as live behavior.
- `CLAUDE.md` and `CONCEPTS.md` describe the shipped behavior, with the retired classifier's documentation removed rather than left beside it.
- No abandoned-approach code remains in the diff. This redesign deletes three subsystems; a half-removed one is worse than either state.

**Per unit.** Each unit's own Verification line passes, and each feature-bearing unit's test scenarios exist as real tests rather than as annotations.
