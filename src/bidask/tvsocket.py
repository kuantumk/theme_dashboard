"""TradingView websocket auth and framing — shared by every socket client here.

Two things live here and nothing else: the short-lived JWT that
`https://www.tradingview.com/quote_token/` mints from the `sessionid` cookie,
and the `~m~<len>~m~` frame codec every `data.tradingview.com` socket speaks.

`src/bidask/tvbars.py` is the only consumer. It is a module rather than code
inside that file because a second copy of the frame parser would drift from
this one, and the reason this parser slices by declared length instead of
matching braces is the kind of detail a copy loses first.

This file used to be `tvquote.py` and carried a live bid/ask stream on top of
these helpers. The board no longer classifies trades against a quote — a
ticker's side comes from its price against a session-appropriate reference — so
the stream, its `Quote` records and the row overlay were removed. Only the
transport survived, and the module is named for it.
"""

from __future__ import annotations

import json
import re
from typing import Iterable

import requests

from src.bidask.config import cookie_jar

TOKEN_URL = "https://www.tradingview.com/quote_token/"
ORIGIN = "https://www.tradingview.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Origin": ORIGIN,
    "Referer": "https://www.tradingview.com/",
}

_FRAME_HEAD = re.compile(r"~m~(\d+)~m~")


class QuoteAuthError(RuntimeError):
    """The socket could not be authenticated.

    Carried to the UI verbatim, unlike every other failure — the message is
    written here and contains no request detail, so there is no cookie to leak.
    A bare `HTTPError` on the token endpoint is the single most likely thing a
    user will hit, and it says nothing about the cause.
    """


def iter_frames(raw: str) -> Iterable[str]:
    """Split TradingView's `~m~<len>~m~<payload>` framing.

    The declared length is used to slice, rather than matching braces: payloads
    nest objects, so a non-greedy `\\{.*?\\}` pattern splits them in the wrong
    place and silently drops the tail of every message.
    """
    pos = 0
    while pos < len(raw):
        head = _FRAME_HEAD.match(raw, pos)
        if not head:
            return
        length = int(head.group(1))
        start = head.end()
        yield raw[start:start + length]
        pos = start + length


def encode(method: str, params: list) -> str:
    payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


def auth_token() -> str:
    """Mint a short-lived socket JWT from the `sessionid` cookie."""
    jar = cookie_jar()
    if not jar:
        raise QuoteAuthError("no TRADINGVIEW_SESSIONID in .env")
    response = requests.get(TOKEN_URL, headers=HEADERS, cookies=jar, timeout=20)
    if response.status_code in (401, 403):
        raise QuoteAuthError("TradingView session cookie rejected — log in again and re-copy it")
    response.raise_for_status()
    token = response.text.strip().strip('"')
    if not token:
        raise QuoteAuthError("TradingView returned an empty quote token")
    return token
