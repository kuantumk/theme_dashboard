---
title: "Never score a derived signal against a series the signal is built from"
date: 2026-08-20
category: conventions
module: bidask
problem_type: convention
component: tooling
severity: high
applies_when:
  - "Validating a classifier, score, or ranking against price or any other outcome series"
  - "A recorded correlation is being used to decide where a bug is NOT"
  - "A measure looks correct in aggregate while individual rows look obviously wrong"
related_components:
  - testing_framework
tags:
  - validation
  - spearman
  - backtest
  - bidask
  - microstructure
---

# Never score a derived signal against a series the signal is built from

## Context

The tape-pressure classifier splits the market into strong-tape and weak-tape
columns from polled quote snapshots. For months its recorded validation in
`CLAUDE.md` read:

> Spearman(imbalance, same-window return) = +0.305, sign agreement 67.7%,
> monotone quintiles. When the board looks wrong, suspect the gate, the ranking
> or the cap before the CLNV logic.

The measurement was real and the number was reproducible. The classifier was
also, at that moment, running one of its two rules **backwards** — and that
sentence told every future reader to look somewhere else. It took a live
debugging session and a from-scratch instrumented replica to find, because the
recorded number said the place to look was already cleared.

The measurement's target was contaminated. `classify.py` decides most
observations with the tick rule, which is `sign(last − prior different last)`.
Correlating the classifier's output against a **last-to-last** return therefore
correlates the output partly with itself. Measured on 13,821 live observations:

| classifier sign scored against | Spearman |
|---|---|
| last-to-last return (the old target) | +0.72 |
| mid-to-mid return over the same window | +0.40 |

The inflated figure was not a rounding artifact. It was the self-reference.

## Guidance

**Score against a series the signal does not consume.** Trade prices bounce
between bid and ask, so any rule that reads `last` is partly correlated with a
last-to-last return by construction. Use mid-to-mid returns, or better, an
independent source — this investigation used yfinance 1-minute bars, which
share no code path, no vendor, and no sampling clock with the TradingView quote
socket the classifier reads.

**Score on two axes, not one.** A single "does it track price" number cannot
distinguish a rule that reads flow from a rule that reads a constant. Report
both:

1. **Direction** — correlation with the outcome you claim to predict.
2. **Static bias** — correlation with a per-ticker attribute that carries no
   direction at all.

For this classifier the static attribute was the mean position of a ticker's
prints inside its own spread. That attribute is independent of how the ticker
moved (r = −0.075 with drift) and holds at equal strength inside every drift
tercile, so any correlation with it is contamination by definition. The two
axes separated the rules immediately where one axis had not:

| quote rule scored alone | direction | static bias |
|---|---|---|
| against the current snapshot (the defect) | −0.009 | **+0.896** |
| against the previous poll's book (the fix) | +0.339 | +0.711 |

The shipped rule was measuring *where a ticker's prints habitually sit in its
spread* — almost perfectly — and price direction not at all.

**Prefer a corroborating signal built from different information.** After the
fix, the quote rule's disagreement with the tick rule fell from 14.7% to 7.2%.
The tick rule reads only trade prices and the quote rule only the book, so two
independent constructions agreeing twice as often is evidence neither
correlation alone provides.

**Write down what a validation does *not* establish.** The replacement note in
`CLAUDE.md` records that the window sweep covers 55 minutes and that nothing
measured six hours of accumulation under the corrected rule. A validation that
states its own boundary cannot be read as clearing ground it never covered.

## Why This Matters

A wrong validation is worse than no validation, because it is load-bearing in
the wrong direction. With no recorded number, the next investigator reads the
classifier. With "+0.305, suspect the gate before the CLNV logic," they read the
gate, the ranking and the cap first — which is exactly what happened.

The failure is also self-concealing. The mid band and the drift override routed
67% of decisions to the tick rule, so the board looked plausible over minutes
even while the quote rule was inverted. Aggregate correctness over a short
horizon coexisted with a rule that carried no information at all. Only the
two-axis score against an independent series pulled them apart.

## When to Apply

- Before recording any correlation as evidence that a component is correct.
- Whenever a validation's conclusion would tell a future reader where **not** to
  look. That is the sentence that costs the most when it is wrong.
- When a score looks right in aggregate but individual rows look obviously
  wrong. That combination is the signature of a contaminated target, not of a
  user misreading the board.

## Examples

`tools/probe_bidask.py` (an early prototype, not the shipped classifier) has
both halves of the trap in eight lines:

```python
rate = pd.Series({s: ask_hits[s] / scored[s] for s in syms})
move = pd.Series({s: last_px[s] / first_px[s] - 1 for s in syms})
rho = rate.corr(move, method="spearman")
print(f"\n  corr(ask-hit rate, price move over window): spearman {rho:.3f}")
print("  low correlation => the measure is not a % change proxy;")
print("  high correlation => it is, and adds nothing over price.")
```

The target is a last-to-last move, and the interpretation is inverted: a low
correlation is read as evidence the measure carries independent information,
when a rule that reads a per-ticker constant scores low for exactly the opposite
reason. Applied to the shipped classifier, that reading would have called
`-0.009` a success.

Two validators in this repo already do it correctly and are worth copying
rather than rewriting:

- `tools/validate_radar.py` scores against named episodes with verified forward
  returns, and asserts a rank threshold rather than a correlation.
- `tools/backtest_hl_rule.py` correlates a simulated prediction against an
  external `OBSERVED` dataset that the simulation never reads.

`tests/backtest_theme_scoring.py:689` compares an old and a new formula to each
other. That answers "did the ranking change", not "is either correct", and
should not be read as an accuracy check.

## Related

- [API returns null for fields it does not have](../logic-errors/api-returns-null-for-fields-it-does-not-have.md) — the previous bidask defect that also presented as a plausible-looking board
- [NaN defeats numeric guard chains](../logic-errors/nan-defeats-numeric-guard-chains.md) — same module, same theme of a metric reporting health through a degraded path
- PR #96 — the classifier fix this learning came from, and the commit that retracted the superseded validation note
