---
module: bidask
date: 2026-08-12
problem_type: logic_error
component: service_object
severity: critical
symptoms:
  - "Equity tab rendered no tickers for an entire trading session while crypto worked normally"
  - "100% of equity observations rejected as no_quote, with the UI blaming user-set thresholds"
  - "bid/ask null on an authenticated real-time feed, indistinguishable from a missing data entitlement"
root_cause: wrong_api
resolution_type: code_fix
tags:
  - vendor-api
  - silent-null
  - tradingview
  - websocket
  - error-surfacing
  - unverified-assumption
related_files:
  - src/bidask/tvsocket.py
  - src/bidask/feed.py
  - src/bidask/web/app.js
---

# An API that returns null for fields it does not have looks exactly like a missing entitlement

## Problem

The tape-pressure dashboard's equity tab showed nothing for a full trading
session. Crypto — same code path, same credentials, same request library —
worked perfectly.

The app selected `bid` and `ask` from TradingView's screener REST API. Both came
back `null` for all 2,064 rows. Because the *crypto* screener returned them
normally, and because US equity real-time quotes genuinely are a paid
entitlement, every available signal pointed at an account or subscription
problem. It was not one. The screener has **no `bid`/`ask` field for US equities
at all** — and asking for a field it does not have returns `null` instead of an
error.

## Symptoms

- `bid`/`ask` null for every equity row, on an authenticated session whose
  `update_mode` read `streaming` (real-time, not delayed).
- 100% of equity observations where a trade actually printed rejected as
  `no_quote`; `classified=0`, `coverage=0%`, indefinitely.
- The UI said **"No tickers above the current thresholds yet"** — pointing the
  user at their own sliders while the data feed was structurally dead.
- Adding valid credentials changed `update_mode` from `delayed_streaming_900` to
  `streaming` and changed nothing else, which *looked* like confirmation that the
  account lacked a quote entitlement.

## Root cause

Two independent facts compounded:

1. **The screener silently nulls unknown fields.**
   `scanner.tradingview.com/america/metainfo` publishes 3,771 fields for the US
   market and not one is a quote field — `bid`, `ask`, `bid_size`, `ask_size`,
   and `spread` are all absent. The crypto scanner's metainfo *does* list `bid`,
   `ask`, and `bid_ask_spread_pct`. Same client, same call shape, different
   schema — and no error on either.

2. **Quotes live on a different service entirely.** TradingView's own web and
   desktop apps read quotes from a websocket at `data.tradingview.com`,
   authenticated with a JWT minted from the same `sessionid` cookie. It serves
   `lp`, `bid`, `ask`, sizes, and `volume` per symbol, pushed on change: 296
   symbols filled in 0.4s, 99% two-sided.

The bug shipped because the equity path was never exercised while the market was
open. The originating PR said so in plain text — *"Equity bid/ask remains
unverified — the fields return null out of session, so Monday is its first real
test"* — and the plan carried a matching stop condition (*"stop and surface if
equity `bid`/`ask` remain null during US market hours"*) that was written as
prose and never implemented as a runtime check. Nothing fired.

## Resolution

At the time (2026-08-12): read quotes from TradingView's own quote websocket and
keep the screener for what it is genuinely good at — universe selection,
liquidity floors, sector/industry, period highs. Unquoted rows were kept rather
than dropped, so they surfaced as `no_quote` rejections instead of hiding a dead
socket behind a quietly shrinking universe, and a `quotes N/M` health pill made
an empty column state its own cause.

**⛔ Superseded in September 2026 — do not reach for the quote socket.** The
board no longer classifies trades, so it reads no quote of any kind: a ticker's
side is its price against a session-appropriate reference, and the only
admission path is Relative Volume at Time. The quote stream was deleted with the
classifier. `tvquote.py` is now `src/bidask/tvsocket.py` and carries only the
shared transport — the JWT minted from the `sessionid` cookie and the
`~m~<len>~m~` frame codec — which `src/bidask/tvbars.py` uses to pull chart bars.
See the Tape Pressure Dashboard section of `CLAUDE.md`.

What survived the redesign is the honesty machinery, and it is the part worth
copying: an empty column still names its own cause, now from the `rvol` block of
the state payload, with an `rvol N/M` coverage pill beside it.

## The generalizable lesson

**A null column is not evidence of a permission problem. Ask the API what fields
it actually has before believing anything about why one is empty.** Most vendor
APIs accept unknown field names and return null rather than erroring, so a
wrong-service call is shaped exactly like an entitlement gap, a stale cookie, or
a quiet market. A metainfo/schema endpoint answers in one request what hours of
credential debugging cannot.

Corollaries:

- **Metainfo settles "can it serve this", not "should I depend on it".** The
  inverse trap also exists: `Value.Traded` has never appeared in the 3,771-field
  catalogue and the scanner serves it anyway — an unlisted alias that was null
  all pre-market, emptied the equity universe for a full session, and errored
  nowhere. `relative_volume_intraday|5` is another, listed stock/ETF only while
  serving the crypto scanner. A field absent from metainfo that returns values is
  not a discovery; it is an undocumented dependency.
- **Never let an empty UI blame user-controlled settings by default.** "No
  results above your thresholds" is a claim about the user's input; it must not
  be the message when the real state is "no input arrived." That single string
  is what made this cost a session instead of minutes.
- **A 100% failure rate is an upstream-breakage signal, not a quiet market.**
  The same lesson the EP scan learned from Finviz's ticker mangling — see
  CLAUDE.md's all-dropped alarm.
- **A stop condition written in a plan is not a control.** If the plan says
  "stop and surface if X," something in the running system has to test X.
- **Contrast against the working sibling.** Crypto worked on the same code, which
  made the difference measurable: comparing the two metainfo schemas took one
  request and settled the question outright.
