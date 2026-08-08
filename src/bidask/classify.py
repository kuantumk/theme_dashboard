"""Trade classification from polled market snapshots.

This is an *approximation* of trade classification, not the real thing, and the
distinction matters enough to state up front. Published algorithms (Lee-Ready
1991, EMO 2000, CLNV 2007) classify each individual trade against the quote
prevailing at that trade, and report 78-90% accuracy. We see only a snapshot
every N seconds — last price, bid, ask, cumulative volume — and infer that a
trade happened at all by observing cumulative volume increase.

Two consequences follow, and both are load-bearing:

1. The quote we compare against may be *newer* than the trade that set `last`,
   by up to a full poll interval. Lee & Ready's entire 5-second construction
   exists to prevent misalignment of a few hundred milliseconds; we structurally
   guarantee a larger one. `_drift_override` below is the mitigation.
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

    # Quote rule with a CLNV tolerance band rather than exact equality.
    band = cfg.band_frac * spread
    if cur.last >= cur.ask - band:
        quote_sign = 1
    elif cur.last <= cur.bid + band:
        quote_sign = -1
    else:
        quote_sign = 0

    tick = _tick_sign(cur.last, prior_different_price)

    # Mid-spread prints have no quote-rule answer; the tick rule decides, and
    # when it cannot, the observation is honestly unclassified.
    if quote_sign == 0:
        if tick == 0:
            return _rejected(UNCLASSIFIED, volume_delta)
        return Observation(sign=tick, certain=True, reason="", volume_delta=volume_delta)

    # Quote-drift override. If the book moved between polls, `last` may be
    # stranded on the wrong side of a quote that has already advanced past it —
    # which misclassifies buys as sells exactly during fast up-moves, a
    # systematic anti-momentum bias. The tick rule degrades far less under quote
    # churn, so it wins the disagreement and the result is marked uncertain.
    quote_moved = cur.bid != prev.bid or cur.ask != prev.ask
    if quote_moved and tick != 0 and tick != quote_sign:
        return Observation(sign=tick, certain=False, reason="", volume_delta=volume_delta)

    return Observation(sign=quote_sign, certain=True, reason="", volume_delta=volume_delta)
