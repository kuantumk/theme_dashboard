---
title: "Benchmark the shipped path, at full scale"
date: 2026-09-18
category: conventions
module: bidask
problem_type: convention
component: tooling
severity: high
applies_when:
  - "Timing a vendor fetch, download, or batch job to decide which optimisation to build"
  - "A prototype or probe script stands in for the shipped code path as the control"
  - "A small sample is being extrapolated to a full run"
  - "Verifying a periodic detector by sampling its published output"
related_components:
  - testing_framework
  - tooling
tags:
  - benchmarking
  - measurement
  - tradingview
  - websocket
  - throughput
  - sampling
  - bidask
---

# Benchmark the shipped path, at full scale

## Context

The relative-volume warm-up downloads eleven sessions of 5-minute bars for the
whole equity universe from the TradingView chart socket, once a session. It was
the slowest thing the tape board does, and the brief was to make it faster.

Three measurements were taken before any code changed. **Each one was wrong, and
they were wrong in different directions**, so no single correction would have
recovered the truth. Two of the three pointed at the wrong optimisation. The
work that followed is in PR #121; this learning is about how the numbers were
obtained, which `CLAUDE.md` records the *results* of but not the method.

That distinction matters here because `CLAUDE.md` now explicitly invites the
next round of this work — it names fetching fewer bars as the remaining lever
and leaves it unbuilt. Whoever takes it will benchmark, and without this they
will build the same three harnesses.

## Guidance

### The control must be the shipped code, not a reimplementation of it

The first probe explored batching by writing a standalone drain loop, then used
that same loop with a batch size of 1 as the "before" number. It measured
**0.469 s/symbol**. The real `fetch_bars` measured **0.106–0.131 s/symbol** on
the same 60 symbols minutes later.

The gap was not batching. The probe opened a fresh socket per chunk, so at chunk
size 1 it paid a connection handshake per symbol, and at 60 attempts it started
throwing `WebSocketBadStatusException` — it had walked into a connection rate
limit the shipped code never approaches, because that reuses one connection per
worker across every symbol it handles.

Taken at face value, that control made batching look like a **4.7x** win — the
same probe's batched arm read 0.100 s/symbol, against its own 0.469 baseline.
The true figure at that sample size was **1.24x**. A reimplemented control
differs from production in every way you did not think about, and the
differences do not announce themselves — this one presented as a spectacular
result.

**Add the shipped entry point as a row in the matrix, interleaved with the
variants rather than run once at the start.** Vendor throughput drifts across
minutes; an A/B separated by ten minutes of other probing is two experiments.

### A small sample does not give a ratio, only a hypothesis

After the control was fixed, the interleaved 60-symbol A/B read:

| configuration | s/symbol |
|---|---|
| one series per connection at a time | 0.097, 0.089 |
| 8 series per connection | 0.078, 0.072 |

That is **1.24x**, which is a modest, unexciting result — easily small enough to
justify skipping the change and reaching for something structural instead.

At the real universe size the same two configurations read:

| configuration | fetch | throughput | resolved |
|---|---|---|---|
| one series at a time | 200.7s | 23,114 bars/s | 1859/1859 |
| batched | 99.8s | 46,479 bars/s | 1859/1859 |

**2.0x**, not 1.24x. Connection setup and whatever warms up behind it amortise
over a long run, and a 60-symbol run charges all of it to 60 symbols. The small
sample understated the win by 60% and would have argued against the change that
turned out to be most of the fix.

The same distortion runs the other way for the optimisation that was *not*
built. At 60 symbols a 768-bar fetch ran 0.027 s/symbol against 0.075 at 2,496
bars, which projects a large further saving. But the full-scale run reached
46,479 bars/s where the small sample saw roughly 33,000, so part of the implied
headroom was the small sample's own overhead. That projection is recorded in
`CLAUDE.md` as a projection, not a measurement, for exactly this reason.

### A correctness bug shows up as a disappointing benchmark

The first batched full run measured **0.112 s/symbol** — barely better than
sequential, which read as "batching does not help much on this account."

It was a bug. A wrong exchange sends `symbol_error` and **then** `series_error`
for the same chart session. The one-at-a-time drain returned on the first frame
and never met the second. A batch keeps reading, so the follow-up overwrote the
reason, and `series_error` on its own means the socket refused
(`REFUSAL_METHODS` in `src/bidask/tvbars.py`) — which discards the entire chunk
and retries it on a new connection. One unlisted ticker cost every symbol
batched with it.

After the fix the same run measured **0.074 s/symbol**.

So the benchmark was not measuring batching; it was measuring batching plus a
silent 34% tax. **When a change measures far below its mechanism's plausible
ceiling, suspect a defect in the change before concluding the mechanism is
weak.** The disappointing number and the dropped symbols had one cause, and the
throughput was the symptom that surfaced first.

### There is no single best parameter value — measure the curve

Batch size does not have an optimum, it has two regimes. The account saturates
near 27,000 bars/s, so at 2,496 bars a batch of 16 measured **worse** than 8
(0.203 against 0.087 s/symbol): past the ceiling a wider batch queues instead of
parallelising. Below the ceiling the bytes stop mattering and a fixed per-series
cost dominates, which only a wider batch amortises — at 768 bars in that same
run, batch 32 read 0.040 against batch 8's 0.075.

A single sweep at one payload size would have produced a constant, and that
constant would have been wrong at the other size. `batch_for` holds
bars-in-flight roughly constant instead of naming a number.

**Those four figures come from the standalone probe, so they are a shape and not
a level** — and writing this learning is what caught the rest of the repo
breaking its own rule. `CLAUDE.md` had paired the probe's 0.075 with the shipped
fetcher's 0.027 as though both were batch 32 at 768 bars; they were two
harnesses and two batch sizes. Both that line and the table in
`src/bidask/tvbars.py` now separate the two, and record that the turning point
has never been re-measured through the shipped fetcher. **Label every figure
with the harness that produced it, at the moment you write it down.** A number
whose provenance is lost cannot be un-mixed later.

### Estimates of your own code are not measurements of it

A code review put the bar-reduction step at "roughly 26s". Measured at real
scale — 1,800 symbols by 2,496 bars — it was **33.25s**. The estimate was in the
right order of magnitude and still 28% low, on code that was sitting in the
repo and could have been timed in two minutes.

### When sampling a periodic observable, check your sample rate against its duty cycle

Live-verifying the stall detector, its published comparison count read zero on
six consecutive samples. That looked like a dead code path, and time went into
suspecting a stale server process.

The detector compares readings 120s apart while the board polls every 10s, so it
publishes a fresh comparison on roughly **one poll in thirteen**. Six independent
samples miss every one of them with probability ≈ **0.62** — the observation was
not evidence of anything.

It did expose a genuine defect, but not the suspected one: a page redrawing every
poll should not watch a figure blink out twelve times in thirteen, so the reading
now carries the most recent comparison rather than only a simultaneous one. The
diagnosis and the fix were both downstream of noticing the sampling ratio.

## Why This Matters

A benchmark's job is to choose what to build. Every error above changed that
choice rather than merely misreporting a number:

- The strawman control argued that batching was a 4.7x win, which would have
  shipped it with no scrutiny of the drain logic — and the drain logic had the
  batch-discarding bug in it.
- The 60-symbol A/B argued the opposite, that batching was worth 1.24x and
  probably not worth the risk. The full-scale figure was 2.0x, and batching plus
  the vectorised reduction took the warm-up from 3.9 minutes to 1.7.
- The same small sample inflated the case for a cross-day incremental cache,
  which is a considerably larger and riskier change, and which remains unbuilt.

The costs are asymmetric and both are real: skipping a cheap 2x, or spending
days on a structural change chosen against a distorted comparison.

## When to Apply

- Before using any timing number to choose between two implementations.
- Whenever the "before" case is produced by anything other than the shipped
  entry point. Reimplementing it for convenience is the default mistake.
- When extrapolating from a sample small enough that fixed setup costs are a
  visible share of it — which, for anything opening network connections, is
  most samples.
- When a result is far better or far worse than the mechanism can plausibly
  deliver. Both directions indicate a harness fault or a defect, not a finding.
- Before concluding that an intermittent observable is never produced.

## Examples

The shipped fetcher takes an explicit `batch` override precisely so a control
run can drive the real code path rather than a copy of it
(`src/bidask/tvbars.py`):

```python
def fetch_bars(symbols, *, interval=DEFAULT_INTERVAL, bars=DEFAULT_BAR_COUNT,
               workers=DEFAULT_WORKERS, token=None, batch=None):
    ...
    size = batch_for(bars) if batch is None else max(1, int(batch))
```

`batch=1` reproduces the previous behaviour inside the current code, so the
control differs from the variant in one parameter and nothing else. That is the
row the 200.7s figure came from. A probe that re-derives the drain loop cannot
offer this, because its "one at a time" is its own, not the product's.

Completeness has to be a column in the table, not an afterthought. Every
configuration above reported resolved-over-requested, and the full-scale rows
read 1859/1859 for both arms. A faster fetcher that silently loses two symbols
is worse than a slow one here: a symbol with no baseline scores 0 and is absent
from the board with nothing on screen to say why.

Protocol behaviour is not testable against a replayed socket. Fourteen tests
passed against the fake socket in `tests/test_bidask_tvbars.py` before the
live run found the `symbol_error` → `series_error` sequence. The fake was then
taught that sequence, so it is a regression test now — but the live run is what
found it, and the fake is what makes the routing and fallback logic testable at
all. Both, in that order.

## Related

- [Never score a derived signal against a series the signal is built from](./self-referential-signal-validation.md) — the other measurement-discipline learning in this module. That one is about a contaminated validation *target*; this one is about a contaminated *harness*. A recorded number can be wrong either way.
- [Authenticating a TradingView session and reading real-time data](../architecture-patterns/tradingview-session-auth-and-real-time-quotes.md) — the chart socket this benchmarking applies to
- [API returns null for fields it does not have](../logic-errors/api-returns-null-for-fields-it-does-not-have.md) — the same vendor answering plausibly instead of erroring, which is why completeness is a column here
- PR #121 — the warm-up work these measurements decided
