# Concepts

Shared domain vocabulary for this project — entities, named processes, and status concepts with project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound and ce-compound-refresh process learnings; direct edits are fine. Glossary only, not a spec or catch-all.

---

## Themes and taxonomy

### Group move
Several fundamentally related stocks advancing together, rather than one name moving alone. Detecting group moves is the reason this project exists: a theme confirmed by many members is treated as more tradeable than an isolated breakout.

### Theme
A named trading narrative that a stock can belong to, written as a slash-delimited path through the taxonomy. A stock may carry more than one — a genuinely dual-role company contributes to every narrative it belongs to.

### Taxonomy
The canonical, three-level tree of permitted themes. It is the schema: a theme path is valid only if it exists here, so the taxonomy — not the tag data — decides what may be written.

### L1
The top level of a theme path, naming the trading narrative a reader would recognize as a sector story. L1 is the level at which conviction is judged, because a narrative confirmed across several sub-themes is stronger than one confirmed in a single corner of it.
*Avoid:* ecosystem, family

### L2
The middle level of a theme path, naming a segment within its L1 narrative. An L1 that has any children must never be tagged without an L2 — a bare-L1 path on such an L1 is a tagging defect, not a coarse-but-valid tag.

### L3
The optional deepest level of a theme path, naming a specialty within its L2.

### Leaf
A complete theme path as stored against a ticker, at whatever depth it terminates. The leaf is the unit that gets scored and ranked; L1 figures are aggregates over leaves.

### Singleton
The tag for a stock with no genuine thematic peers in the covered universe — a deliberate terminal classification meaning "no group applies," distinct from not having been classified at all. Because it records a decision, a Singleton stays off the first-time-classification worklist and is revisited only by an evidence-based rescue pass that looks for peers who have since appeared.

### Uncategorized
A placeholder meaning classification has not happened yet. It carries no judgment, so a stock holding only this placeholder remains on the worklist until someone classifies it.

Singleton and Uncategorized differ in intent but are treated alike in one respect: neither counts as a real theme, so both yield to incoming human-curated tags where a genuine theme would be protected.

### Untagged
The status of a screened stock that has no usable theme yet — missing entirely, empty, or holding only the Uncategorized placeholder. Untagged stocks form the tag audit's worklist.

### Tag audit
The recurring review that keeps theme tags honest: repairing mechanical defects, reclassifying companies whose business has shifted, and classifying newly screened stocks. It is the only sanctioned path for changing an existing stock's tags, because tags are otherwise locked against automated overwrite.

---

## Screening

### Screener
One named pattern filter that selects stocks matching a specific setup. Screeners are independent and deliberately overlapping — each encodes a different trade idea, and a stock may pass several.

### Master table
The daily cross-sectional snapshot: every stock's indicators plus its rank relative to all other stocks that session. Screeners read from it, so a metric must be present here before any screener can filter on it.

### Screened union
The set of stocks passing at least one screener on a given session — the day's full list of names worth attention, regardless of which setup surfaced them.

### Universe
The set of stocks under consideration at a given pipeline stage, after that stage's own liquidity and price floors. Different stages deliberately use different universes: what is worth trading, what is worth scoring, and what is worth counting for breadth are three different questions.

---

## Metrics

### RS_STS%
A stock's relative strength expressed as a percentile against the broad-market benchmark — how strongly it has outperformed, ranked against every other stock rather than stated in absolute terms.

### VARS
Volatility-Adjusted Relative Strength: cumulative price change normalized by the stock's own average true range, minus the same figure for the benchmark, over a rolling window. Normalizing each leg by its own volatility before differencing is what makes the result comparable between a quiet large cap and a volatile small cap.

### ADR%
Average daily range as a percentage of price — how much room a stock typically travels in a session. It functions as a tradeability floor: a stock too quiet to reach a profit target is filtered out regardless of how strong its trend is.

### Composite
A per-stock blend of strength measures into one number, used so that ranking depends on several independent signals rather than any single one. Stocks missing an input receive a neutral value for it rather than being dropped.

---

## Radar

### L1 Radar
The screener-independent lens: it scores themes across every tagged stock, not just those passing a screener that day. Its purpose is early warning — a narrative can be strengthening before any member has met a momentum screen, and a screener-gated view is structurally blind to that.

### Boost
The lift applied to a theme when several of its sibling sub-themes score well at the same time. It encodes sibling confirmation — a narrative firing in several places at once is more convincing than one firing in isolation — and it applies symmetrically, so broad weakness pushes a theme down as readily as broad strength lifts it.

---

## Market breadth

### Market breadth
How many stocks are participating in a move, as opposed to how far an index has traveled. Breadth is tracked because an index can rise on a handful of names while most stocks fall, a divergence price alone cannot show.

### Advance/decline
The count of stocks rising versus falling in a session — the raw input to breadth measures. The counted universe must include every listed issue, not just operating companies: narrowing it under-counts decliners in a selloff and biases every derived measure upward.

### McClellan oscillator
The difference between a fast and a slow moving average of net advances minus declines, measuring whether participation is improving or deteriorating.

### NASI
The running cumulative total of the McClellan oscillator, tracked for its shape and its momentum rather than its level. Its absolute value depends on an arbitrary starting point and on how the issue universe was chosen, so it is never comparable against another provider's figure — only its direction and its derived momentum are portable.

---

## Earnings pivot

### Episodic pivot
A setup where an earnings release resets a stock's trajectory, producing a large gap and a sustained move rather than a one-day reaction. Abbreviated EP.

### EP scan
The twice-daily search for stocks reporting earnings imminently that also satisfy the setup's structural preconditions, run once before the open and once before the close. It cannot be backfilled — it depends on live pre- and post-market pricing that no longer exists after the fact.

### RVol at time
Volume so far today compared with volume by the same point in previous sessions, rather than against a whole-day average. The time-of-day alignment is the point: an hour into the session, total daily volume is not yet a meaningful comparison.

---

## Tape pressure

### Tape pressure
A reading of whether buyers or sellers are the aggressors right now, accumulated per stock over a trailing window rather than a whole session. It is an approximation inferred from periodic snapshots, not a measured share of buying volume, and is meaningful only in relative terms — a stock against its own history, or ranked against peers observed on the same cadence.

The window is not a display preference. Each observation carries a small constant bias particular to the stock on top of the directional signal; the signal is bounded by the day's move while the bias grows with the number of observations, so an unbounded accumulation eventually reports the bias instead of the tape.

### Ask hit
An observation classified as buyer-initiated: the trade printed at or near the offer, meaning a buyer crossed the spread to get filled. The mirror case is a bid hit, where a seller crossed the spread downward.

Which offer is load-bearing. The comparison is against the book that prevailed *before* the trade, never the book observed after it — a book seen afterwards has already repriced in response to the trade, and comparing against it inverts the classification.

### Position in spread
Where a trade printed between the prevailing bid and offer, expressed as a fraction of the spread. Each stock has a habitual value that reflects how its order flow is routed and says nothing about direction, so it is the contaminant a tape-pressure reading must be scored against to show that the reading measures flow rather than that habit.

### Imbalance
The margin between a stock's buyer- and seller-initiated observations, normalized so that stocks observed for different lengths of time remain comparable. Raw counts alone partly measure observation cadence rather than genuine flow, which is what the normalization removes.

### Delta
The volume-weighted counterpart to the hit counts: each interval's traded volume signed by its classification and accumulated. It restores cross-stock comparability that counts alone lack, at the cost of letting one large print dominate.

### Divergent
The flag raised when a stock's count-based and volume-weighted readings disagree in direction. It marks the single-print artifact: one outsized trade steering the volume measure while the balance of activity points the other way.

### Coverage
The share of observed trades that produced a usable classification. It is the health signal for the whole reading — a collapse in coverage means the inputs have broken, which is otherwise indistinguishable from a quiet market.

### In-play gate
The filter deciding which stocks are worth displaying, as distinct from which are worth polling. It exists for the reader's attention rather than for throughput, and narrows a liquid universe to the names actually moving.

### Strong tape / weak tape
The two columns a session's stocks are split into: those whose offers are being lifted, and those whose bids are being hit. Stocks with no net direction belong to neither — the tape has not spoken on them.

---

## Dashboard

### Time travel
The ability to view any tab as it stood on an earlier session, selected from a rolling retention window. Every session in the window is rebuilt from source data on each run rather than appended to, so a newly added metric appears across the whole history at once instead of accruing forward.

### Entry-ready
The state marking a stock as being at a low-risk entry point right now — contracted range plus proximity to a moving-average rail. It is a timing signal layered on top of theme and strength, answering "is this actionable today" rather than "is this a good stock."

---

## Flagged ambiguities

- **ecosystem** and **family** were both used for the top level of the theme hierarchy, in the radar and the VARS tab respectively. These are one concept: **L1**. The retired names must not reappear in code, config, UI, or docs.
- **Theme** and **leaf** are related but distinct: a theme is the narrative, a leaf is the full stored path that names it at a given depth. Scoring operates on leaves.
- **Singleton** and **Uncategorized** are not interchangeable. Singleton is a decision that no group applies; Uncategorized means no decision has been made yet. Only the latter keeps a stock on the first-time-classification worklist.
- **Feed state** and **market state** are distinct questions in the tape tools: a real-time data entitlement on a closed market is still a closed market.
