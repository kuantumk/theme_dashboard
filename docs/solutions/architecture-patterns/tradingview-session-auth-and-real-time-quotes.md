---
title: "Authenticating a TradingView session and reading real-time data"
date: 2026-08-12
category: architecture-patterns
module: bidask
problem_type: architecture_pattern
component: authentication
severity: high
applies_when:
  - "Reading live prices, session volume, or intraday bars from TradingView"
  - "A TradingView field returns null and it is unclear whether the cause is auth, entitlement, or the wrong endpoint"
  - "Deciding which TradingView surface should own which part of a data pipeline"
related_components:
  - tooling
  - service_object
tags:
  - tradingview
  - websocket
  - real-time
  - authentication
  - session-cookie
  - market-data
---

# Authenticating a TradingView session and reading real-time data

## Context

TradingView exposes several unrelated data services, and the one that is easiest
to reach is not the one that carries quotes. The tape-pressure dashboard was
built entirely on the **screener REST API** (`scanner.tradingview.com`), which is
excellent at selecting a universe and carries **no bid/ask for US equities at
all**. Because the screener returns `null` for fields it does not have rather
than erroring, the resulting empty tab looked exactly like an expired cookie or a
missing market-data subscription. It was neither. The failure analysis lives in
[api-returns-null-for-fields-it-does-not-have.md](../logic-errors/api-returns-null-for-fields-it-does-not-have.md);
this doc is the positive counterpart — how to authenticate once and read
genuinely real-time data.

**One cookie pair opens every socket, and that is the durable part of this
pattern.** The tape board no longer reads quotes at all — its price direction
comes from the screener's own change fields — but the same auth chain and the
same frame codec now carry **chart bars**, which are the only source of real
extended-hours volume in reach. `src/bidask/tvsocket.py` holds both halves and
nothing else; `src/bidask/tvbars.py` is its only consumer. The quote-stream
sections below are kept because the protocol is identical and the discriminators
in §5 are what diagnose either socket.

## Guidance

### 1. Pick the surface by what it actually serves

| Surface | Serves | Use it for |
|---|---|---|
| Screener REST (`scanner.tradingview.com/<market>/scan`) | Fundamentals, technicals, session volume, traded value, sector/industry, period highs, `update_mode`, `current_session`. Crypto **also** gets `bid`/`ask`. | Universe selection and metadata — one request covers thousands of symbols |
| Quote websocket (`wss://data.tradingview.com/socket.io/websocket`) | `lp` (last price), `bid`, `ask`, `bid_size`, `ask_size`, `volume` | Anything quote-shaped, and US equities specifically. **Not used by this repo any more** |
| Chart websocket (same host, `?from=chart/`) | OHLCV bars at any resolution, including **extended hours with real volume** | Per-symbol history: baselines, intraday volume curves. One series request per symbol, so it belongs in a warm-up, never in a poll loop |

⛔ **Studies are not part of that third row.** `create_study` for a Pine-based
study — Relative Volume at Time among them — is refused with `Study not allowed
in this connection` on both the `data` and `prodata` hosts, while the built-in
`Volume@tv-basicstudies` returns values normally. The refusal is about the study
class, not the route or the credentials. Pull the bars and compute the study
yourself.

Do not assume field availability is uniform across markets. Ask the service:
`GET https://scanner.tradingview.com/<market>/metainfo` returns every field that
market can serve. The `america` market lists 3,771 fields and none of them is a
quote field; the `crypto` market lists `bid`, `ask`, and `bid_ask_spread_pct`.
That single request settles in seconds what credential debugging cannot settle at
all.

### 2. The credential is a cookie pair, copied by hand

Two cookies together form the session: `sessionid` and `sessionid_sign`. Both are
required — `sessionid` alone is not accepted for real-time entitlement. They are
copied from a logged-in browser's cookie store, never obtained by programmatic
login (that path triggers CAPTCHA and risks account flagging).

```bash
# .env — see .env.example
TRADINGVIEW_SESSIONID=...
TRADINGVIEW_SESSION_SIGN=...
```

Loaded in `config/settings.py`, which accepts both `TRADINGVIEW_SESSIONID_SIGN`
and `TRADINGVIEW_SESSION_SIGN` spellings, and assembled into a request jar by
`cookie_jar()` in `src/bidask/config.py`.

**Treat this pair as a full account session token.** It is not a scoped API key;
it is revoked only by logging out of TradingView. Never log it, never let it
reach a UI error string, and keep it out of any directory a local HTTP server has
as its document root.

### 3. The auth chain is cookies → JWT → socket

The websocket does not take cookies. It takes a short-lived JWT that the cookies
mint:

```
GET https://www.tradingview.com/quote_token/   (with the cookie jar)
  → a JSON-quoted JWT string (~1,160 chars, starts "eyJ")
  → send as the socket's first message: set_auth_token
```

The response body is a *JSON string*, so strip the surrounding quotes before
using it (`auth_token()` in `src/bidask/tvsocket.py`). Fetch the token **before**
opening the socket — a missing or rejected cookie should not cost a connection.
Without cookies this endpoint fails outright rather than returning an anonymous
token, so there is no unauthenticated fallback for this path. The endpoint is
named `quote_token/`, but the JWT it mints authenticates the chart socket just as
well; there is one token, not one per surface.

### 4. Speak the wire protocol

Messages are length-prefixed: `~m~<byte-length>~m~<payload>`.

```python
def encode(method, params):                # encode(), src/bidask/tvsocket.py
    payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"
```

**Split incoming frames by the declared length, never by matching braces.**
Payloads nest objects, so a non-greedy `\{.*?\}` pattern stops at the first inner
`}` and silently truncates every message (`iter_frames()`, same module).

Handshake order for quotes, then subscriptions:

1. `set_auth_token` — the JWT from step 3
2. `quote_create_session` — an arbitrary unique session name (e.g. `qs_` + random)
3. `quote_set_fields` — session name, then every field you want
4. `quote_add_symbols` / `quote_remove_symbols` — session name, then symbols

For bars the handshake is the same shape on a socket opened with `?from=chart/`:
`set_auth_token`, `chart_create_session`, `resolve_symbol`, `create_series`.

⛔ **`resolve_symbol` must be given `"session": "extended"` to reach pre-market
and after-hours bars.** Without it the series starts at 09:30 and the caller
answers the wrong question with no error anywhere.

Symbols are exchange-qualified (`NASDAQ:AAPL`), which is the form the screener's
own `ticker` column already returns; neither socket resolves a bare ticker.
`quote_add_symbols` accepts many symbols per call, so batch them. A chart session
takes one symbol, so parallelism is per connection — and it has a ceiling:
measured 2026-09-17 over one 60-symbol list, 6 concurrent sockets returned 60 of
60 on both runs, 8 returned 57 and 12 returned 35. Past the ceiling you lose
symbols rather than throughput, which is silent at the consumer.

Two things the protocol requires that are easy to miss:

- **Echo heartbeats verbatim.** Frames matching `~h~<n>` are the keepalive; send
  the identical frame straight back. A connection that stops answering is
  dropped, and from the consumer's side that is indistinguishable from a quiet
  market.
- **Updates are partial.** A `qsd` message carries only the fields that changed,
  so merge into the previous values. A push carrying only `lp` must not blank the
  `bid`/`ask` an earlier push established.

### 5. Verify you are actually on real-time data

`update_mode` is the feed's own report of your entitlement, and it is the fastest
way to confirm the cookies took effect:

| `update_mode` | Meaning |
|---|---|
| `streaming` | Real-time |
| `delayed_streaming_900` | 15-minute delayed (900s) — unauthenticated or unentitled |

Treat any value beginning `delayed` as degraded (`DELAYED_PREFIX` in
`src/bidask/feed.py`). Crypto returns `streaming` even unauthenticated, so
**verify on an equity symbol** — checking crypto proves nothing about the cookies.

`current_session` is a different question: whether the *market* is trading, not
whether your *feed* is live. A real-time entitlement on a closed market is still
a closed market. During the US regular session it reports **`market`** — not
`regular`, a plausible-looking value that never appears (`SESSION_LABELS` in
`src/bidask/feed.py`). Pre-market reads `pre_market` and after hours
`post_market`; `extended` is documented by TradingView but was not observed in
either probe, so treat an unmapped value as closed rather than guessing which
window it names.

## Why This Matters

Every failure mode in this integration presents as the same symptom — empty or
null data — while having a completely different cause: wrong service, missing
cookie, expired cookie, closed market, or delayed entitlement. Without the
discriminators above (metainfo for "can this service serve it at all",
`update_mode` for "am I real-time", `current_session` for "is the market open"),
diagnosis degenerates into guessing at the subscription, which is the one cause
that cannot be tested from the code.

The concrete cost of getting this wrong: the equity tab rendered nothing for a
full trading session, with 100% of observations rejected for want of a quote,
because the code was asking a service that never had the field.

The payoff for getting it right is large, on both sockets. The quote stream is
push-based and cheap at scale — measured 2026-08, 296 symbols subscribed in 6
frames all returned a quote within **0.4 seconds**, 99% of them two-sided, the
~1% being OTC ADRs that genuinely have no quote. The chart socket is the only
route to something no other source here supplies: measured 2026-09-17, 2,500
five-minute bars per symbol across roughly eleven extended sessions, **carrying
real extended-hours volume** — 53 pre-market bars and 239,052 shares for GNRC
that morning. yfinance serves those same bars with **zero volume on every one**,
which reads as a quiet ticker rather than as missing data and silently zeroes any
denominator built from it.

## When to Apply

- Any time a TradingView field comes back null — check `metainfo` before
  suspecting credentials.
- When building a pipeline that needs both a wide universe and per-symbol
  history: use both surfaces, each for its own job. One screener request covers
  thousands of rows; bars cost one request per symbol, so they belong in a
  once-per-session warm-up with its result cached.
- When two figures are compared against each other, take **both legs from one
  surface** so they share a clock. Mixing a screener value with a socket value
  adds a cross-source skew on top of any polling skew.

## Examples

Minimal end-to-end read of a real-time equity quote:

```python
import json, re, requests, websocket
from src.bidask.config import cookie_jar

HEAD = {"User-Agent": "Mozilla/5.0", "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/"}

token = requests.get("https://www.tradingview.com/quote_token/",
                     headers=HEAD, cookies=cookie_jar(), timeout=20).text.strip().strip('"')

def encode(m, p):
    body = json.dumps({"m": m, "p": p}, separators=(",", ":"))
    return f"~m~{len(body)}~m~{body}"

ws = websocket.create_connection(
    "wss://data.tradingview.com/socket.io/websocket?from=screener%2F",
    header=[f"{k}: {v}" for k, v in HEAD.items()],
    origin="https://www.tradingview.com", timeout=20)

ws.send(encode("set_auth_token", [token]))
ws.send(encode("quote_create_session", ["qs_demo"]))
ws.send(encode("quote_set_fields", ["qs_demo", "lp", "bid", "ask", "volume", "update_mode"]))
ws.send(encode("quote_add_symbols", ["qs_demo", "NASDAQ:AAPL"]))

while True:
    raw = ws.recv()
    if re.match(r"^~m~\d+~m~~h~\d+$", raw):
        ws.send(raw)                       # keepalive — echo verbatim
        continue
    print(raw)                             # qsd frames carry the partial updates
```

Expected during market hours, authenticated:

```
lp=300.9323  bid=300.93  ask=300.95  bid_size=80  ask_size=520
update_mode=streaming  current_session=market
```

The same symbol via the screener REST API returns `bid=None, ask=None` — on the
same cookies, on the same `streaming` feed. That contrast is the whole lesson.

## Related

- [An API that returns null for fields it does not have looks exactly like a missing entitlement](../logic-errors/api-returns-null-for-fields-it-does-not-have.md) — the failure analysis this pattern came out of
- [NaN defeats numeric guard chains](../logic-errors/nan-defeats-numeric-guard-chains.md) — why null quote fields must be rejected explicitly rather than falling through numeric guards
- `src/bidask/tvsocket.py` — the shared transport: the token mint and the frame codec, and nothing else
- `src/bidask/tvbars.py` — the production client built on it: chart sessions, the `extended` resolve, worker pool, refusal retry
- `CLAUDE.md` > "Tape Pressure Dashboard" — the repo-level statement of which surface owns what
