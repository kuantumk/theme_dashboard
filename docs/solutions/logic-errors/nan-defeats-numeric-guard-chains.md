---
module: bidask
date: 2026-08-08
problem_type: logic_error
component: tooling
severity: high
symptoms:
  - "Rows with null bid/ask were classified as confident buy/sell signals instead of being rejected"
  - "Coverage metric reported 100% while every observation came from a degraded fallback path"
  - "Dashboard showed 'server unreachable' while the server was running and writing state normally"
root_cause: missing_validation
resolution_type: code_fix
tags:
  - nan
  - pandas
  - json
  - validation
  - guard-clause
related_files:
  - src/bidask/classify.py
  - src/bidask/session.py
  - src/bidask/server.py
---

# NaN defeats numeric guard chains and silently corrupts JSON payloads

## Problem

A validation chain that rejects bad market data (`if bid <= 0: reject`) let rows
with **no quote at all** pass every check, classify as confident buy/sell
signals, and reach the UI. Separately, the same NaN values reached the JSON
payload and made the browser reject the entire document — the page reported the
server as unreachable while the server was healthy.

Both defects came from one root fact and neither was caught by the test suite.

## Symptoms

- Rows whose `bid`/`ask` were null produced `sign=+1, certain=True` — a confident
  directional signal derived from no quote.
- The coverage metric read healthy (100%) precisely when it should have collapsed,
  because rejections never fired.
- One recent-IPO ticker with a null `High.6M` blanked the whole dashboard, both
  tabs, with the misleading banner "server unreachable".

## What Didn't Work

- **Positivity guards.** `if cur.last <= 0 or cur.bid <= 0 or cur.ask <= 0: reject`
  looks exhaustive and is not. Every comparison against NaN evaluates False, so a
  NaN quote satisfies none of the reject conditions and falls through.
- **`is not None` filtering.** `{k: v for k, v in row.items() if v is not None}`
  does not drop NaN. pandas yields `float('nan')` for a null cell, not `None`,
  so the filter passes it straight through.
- **Existing NaN awareness in the same file.** `_clean_symbol` in
  `src/bidask/session.py` already documented this exact hazard for the *symbol*
  field ("`float('nan')` is truthy, so a plain falsiness check lets it through").
  Knowing the trap in one column did not generalize to the numeric ones.
- **A 194-test suite.** No test fed NaN through any numeric field. The suite
  covered the symbol case only, so both defects passed CI cleanly.

## Solution

Guard for non-finiteness **before** the domain preconditions, rather than hoping
the domain checks catch it:

```python
# src/bidask/classify.py — before any <= / > comparison
if not all(math.isfinite(v) for v in (cur.last, cur.bid, cur.ask, cur.volume)):
    return _rejected(REJECT_NO_QUOTE)
```

Filter non-finite values out of anything destined for JSON, and make the
serializer fail loudly rather than emit an unparseable token:

```python
# src/bidask/session.py — _is_finite() rejects None *and* NaN
return {k: row[k] for k in keys if k in row and _is_finite(row[k])}

# src/bidask/server.py — allow_nan=False turns a silent corruption into an error
payload = json.dumps(self.build_state(), separators=(",", ":"), allow_nan=False)
```

Coerce at the boundary too — `_num()` maps None *and* non-finite values to `0.0`,
so a null field trips the positivity guard instead of slipping past it.

## Why This Works

Three independent facts compound into the bug:

1. **IEEE 754**: every comparison involving NaN is False, including `nan <= 0`.
   A guard chain written as "reject the bad values" cannot reject NaN, because
   rejection requires a comparison to be True.
2. **pandas nulls are NaN, not None.** `DataFrame.to_dict("records")` yields
   `float('nan')` for a null cell in a float column. Identity checks against
   `None` miss it entirely, and `float('nan')` is truthy so falsiness checks
   miss it too.
3. **`json.dumps` emits a bare `NaN` token by default.** That is valid Python
   but invalid JSON. `JSON.parse` throws, and a `.catch()` written for network
   failures misattributes it to the server being down.

`math.isfinite` is the only check that returns True for the good case and False
for NaN and both infinities, which is why it belongs ahead of the domain logic
rather than woven into it.

## Prevention

- **Validate finiteness at the ingestion boundary**, before any comparison-based
  guard. Write the check as "accept only what is provably good"
  (`if not all(math.isfinite(...)): reject`) rather than "reject what is
  provably bad" — the inverted form is what NaN slips through.
- **Pass `allow_nan=False` to `json.dumps`** on any payload a browser or another
  strict parser will read. It converts a silent corruption into a loud
  `ValueError` at the write site, next to the code that can fix it.
- **Test NaN explicitly for every numeric field**, not just the one that bit you.
  This codebase had NaN coverage for `symbol` and none for `bid`, `ask`, `last`,
  or `volume`:

```python
def test_nan_quote_is_rejected_not_classified(self):
    nan = float("nan")
    obs = classify(cur=tick(last=10.10, bid=nan, ask=nan, volume=2000), ...)
    self.assertEqual(obs.reason, REJECT_NO_QUOTE)
    self.assertFalse(obs.certain)
```

- **Assert strict JSON, not just round-trippable JSON.** `json.loads` accepts the
  `NaN` token by default, so a naive round-trip test passes where the browser
  fails. Use `parse_constant` to reject it:

```python
json.loads(raw, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))
```

- **Treat "the health metric looks fine" as suspicious when guards exist to make
  it fall.** Coverage reading 100% was itself the tell: a metric that cannot
  degrade is not measuring anything.
