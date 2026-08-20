"""Trade classification from polled market snapshots.

This is an *approximation* of trade classification, not the real thing, and the
distinction matters enough to state up front. Published algorithms (Lee-Ready
1991, EMO 2000, CLNV 2007) classify each individual trade against the quote
prevailing at that trade, and report 78-90% accuracy. We see only a snapshot
every N seconds — last price, bid, ask, cumulative volume — and infer that a
trade happened at all by observing cumulative volume increase.

Two consequences follow, and both are load-bearing:

1. The quote in the current snapshot is *newer* than the trade that set `last`,
   and has already reacted to it. Lee & Ready's 5-second construction exists to
   prevent misalignment of a few hundred milliseconds; a poll interval
   guarantees a larger one. So the quote rule reads the PREVIOUS poll's book —
   the one that prevailed before this interval's trades — and the tick rule wins
   every disagreement. See `_quote_sign` for the measurements behind both.
2. One observation represents the terminal trade of the interval, not the
   interval's flow. Whether 1 or 50,000 trades printed, we emit one sign.

Use the output relatively — a ticker against its own history, or ranked
cross-sectionally on the same cadence — never as a measured share of buying
volume.

The classification rule is CLNV-shaped: a tolerance band around each quote side,
with the middle of the spread falling to the tick rule. Exact-equality tests
(`last == ask`) are *not* used, because sub-penny price improvement means they
would essentially never fire on US equities.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

# Rejection reasons. Distinct values so the accumulator can report coverage by
# cause rather than a single opaque "rejected" count.
REJECT_WARMUP = "warmup"          # first poll for this ticker; no volume baseline
REJECT_NO_TRADE = "no_trade"      # volume unchanged: a republished quote, not a print
REJECT_NO_QUOTE = "no_quote"      # bid/ask/last missing or non-positive
REJECT_CROSSED = "crossed"        # ask <= bid: locked or crossed, sign is arbitrary
REJECT_WIDE_SPREAD = "wide_spread"  # spread implausible vs mid: stale quote
REJECT_AUCTION = "auction"        # opening/closing window: no valid continuous quote
UNCLASSIFIED = "unclassified"     # mid-spread print with no prior different price


@dataclass(frozen=True)
class Tick:
    """One snapshot of a symbol."""

    last: float
    bid: float
    ask: float
    volume: float


@dataclass(frozen=True)
class Observation:
    """The result of classifying one poll.

    `sign` is +1 buyer-initiated, -1 seller-initiated, 0 when not classified.
    `certain` is False when the drift override fired — the sign is our best
    estimate but the quote and tick rules disagreed.
    """

    sign: int
    certain: bool
    reason: str
    volume_delta: float

    @property
    def classified(self) -> bool:
        return self.sign != 0

    @property
    def is_buy(self) -> bool:
        """The offer was lifted: BUYING pressure."""
        return self.sign > 0

    @property
    def is_sell(self) -> bool:
        """The bid was hit: SELLING pressure."""
        return self.sign < 0


def _rejected(reason: str, volume_delta: float = 0.0) -> Observation:
    return Observation(sign=0, certain=False, reason=reason, volume_delta=volume_delta)


def _tick_sign(last: float, prior_different_price: Optional[float]) -> int:
    """Tick rule: compare against the most recent *different* trade price."""
    if prior_different_price is None:
        return 0
    if last > prior_different_price:
        return 1
    if last < prior_different_price:
        return -1
    return 0


def _quote_sign(last: float, book: Optional[Tick], cfg) -> int:
    """CLNV-shaped band against `book`; 0 when the book cannot answer.

    `book` is the *previous* poll's quote — the one that prevailed before this
    interval's trades. Reading the current snapshot instead inverts the rule,
    because by the time we look the book has already reacted to the trade that
    set `last`. Measured live on 2026-08-20 over 13,821 observations: against
    the current snapshot, Spearman with the same-window mid return is -0.185
    (-0.009 against an independent 1-minute series); against the previous
    poll's book it is +0.339. Prints average 0.353 of the spread when the mid
    rose and 0.633 when it fell.

    The corroborating figure is the disagreement rate with the tick rule, which
    is built from different information entirely: 14.7% of classified
    observations against the current snapshot, 7.2% against the previous poll's
    book. Two independent rules agreeing twice as often is what you expect when
    both are finally measuring the same real thing. It also means `uncertain`
    falls after this change rather than rising, even though the override below
    is now ungated — fewer disagreements to resolve.

    An unusable previous book returns 0 rather than falling back to the current
    one. Falling back would reinstate the inversion on exactly the polls where
    it cannot be detected.

    `max_spread_pct` is applied here as well as to the current snapshot, because
    a poll rejected as `wide_spread` still becomes the next poll's `prev` — the
    accumulator stores every snapshot, rejected or not. Without this test the
    band would be 30% of a stale, absurd spread, which classifies essentially
    any print as a lift or a hit at random.
    """
    if book is None:
        return 0
    bid, ask = book.bid, book.ask
    if not (math.isfinite(bid) and math.isfinite(ask)):
        return 0
    if bid <= 0 or ask <= bid:
        return 0
    mid = (ask + bid) / 2.0
    if mid <= 0 or (ask - bid) / mid * 100.0 > cfg.max_spread_pct:
        return 0
    band = cfg.band_frac * (ask - bid)
    if last >= ask - band:
        return 1
    if last <= bid + band:
        return -1
    return 0


def classify(
    *,
    cur: Tick,
    prev: Optional[Tick],
    prior_different_price: Optional[float],
    cfg,
    in_auction_window: bool = False,
) -> Observation:
    """Classify one polled observation as buyer- or seller-initiated.

    `prior_different_price` is the most recent last-price that differs from
    `cur.last`; the caller tracks it because it can predate `prev`. Pass
    `in_auction_window=True` only for equities inside the opening or closing
    window — crypto trades continuously and has no auction to exclude.
    """
    if prev is None:
        return _rejected(REJECT_WARMUP)

    # Non-finite guard FIRST. pandas yields NaN (not None) for null cells, and
    # every comparison against NaN is False — so without this, `nan <= 0` does
    # not fire and a row with no quote at all falls through to the tick rule and
    # is returned `certain=True`. That is exactly the equity-out-of-session case
    # this app must surface rather than silently classify.
    if not all(math.isfinite(v) for v in (cur.last, cur.bid, cur.ask, cur.volume)):
        return _rejected(REJECT_NO_QUOTE)
    if not math.isfinite(prev.volume):
        return _rejected(REJECT_NO_TRADE)

    # Preconditions, in order. Each exits without producing an observation.
    volume_delta = cur.volume - prev.volume
    if volume_delta <= 0:
        return _rejected(REJECT_NO_TRADE)
    if cur.last <= 0 or cur.bid <= 0 or cur.ask <= 0:
        return _rejected(REJECT_NO_QUOTE, volume_delta)
    if cur.ask <= cur.bid:
        return _rejected(REJECT_CROSSED, volume_delta)

    spread = cur.ask - cur.bid
    mid = (cur.ask + cur.bid) / 2.0
    if mid <= 0 or (spread / mid) * 100.0 > cfg.max_spread_pct:
        return _rejected(REJECT_WIDE_SPREAD, volume_delta)
    if in_auction_window:
        return _rejected(REJECT_AUCTION, volume_delta)

    # Quote rule with a CLNV tolerance band rather than exact equality, applied
    # to the book that prevailed BEFORE this interval's trades.
    quote_sign = _quote_sign(cur.last, prev, cfg)

    tick = _tick_sign(cur.last, prior_different_price)

    # Mid-spread prints have no quote-rule answer; the tick rule decides, and
    # when it cannot, the observation is honestly unclassified. An unusable
    # previous book lands here too, for the same reason: no answer is better
    # than an answer from the wrong book.
    if quote_sign == 0:
        if tick == 0:
            return _rejected(UNCLASSIFIED, volume_delta)
        return Observation(sign=tick, certain=True, reason="", volume_delta=volume_delta)

    # Quote-drift override: the tick rule wins every disagreement.
    #
    # This is deliberately UNGATED. It used to fire only when the book had moved
    # between polls, on the reasoning that a frozen book cannot have stranded
    # `last` on the wrong side. The reasoning is right and the conclusion is
    # backwards: a frozen book means the mid did not move, so the poll carries
    # no directional information at all — yet the quote rule still emits a sign,
    # and that sign is this ticker's habitual position inside its own spread.
    # Those observations are pure bias with no offsetting signal, and the
    # accumulator sums them.
    #
    # Measured live on 2026-08-20: the gate handed 3.4% of observations to the
    # quote rule, and every one of them had a zero mid move. Removing it drops
    # the board's correlation with a static position-in-spread from +0.217 to
    # +0.045 while nudging its correlation with price up from +0.426 to +0.438.
    if tick != 0 and tick != quote_sign:
        return Observation(sign=tick, certain=False, reason="", volume_delta=volume_delta)

    return Observation(sign=quote_sign, certain=True, reason="", volume_delta=volume_delta)
