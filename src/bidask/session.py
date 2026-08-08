"""Session accumulator — cumulative tape pressure per ticker.

Holds, per symbol and since session start: ask-side hit count, bid-side hit
count, and a volume-weighted signed delta.

Both are kept deliberately. Counts match how the source tool presents its
numbers and bound each misclassification's cost at 1. Volume-weighted delta is
what the industry means by "delta" (Sierra Chart, Bookmap, Jigsaw all define it
as buy volume minus sell volume) and restores cross-ticker comparability — but
it also amplifies each misclassification from one count to a whole interval's
shares, with no within-bar netting to cancel it. Where the two disagree in sign,
a single print is likely driving the reading, and that ticker is flagged.

Raw counts alone are not comparable across tickers: a fixed poll cadence gives
every symbol the same number of observations regardless of how much it trades,
so counts partly measure cadence. The imbalance ratio accompanies them.
"""

from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional

from src.bidask.classify import Tick, classify

# Below this many observed volume deltas the running median is too noisy to
# winsorize against, so no cap is applied yet.
MIN_SAMPLES_FOR_WINSOR = 5


@dataclass
class TickerState:
    symbol: str
    ask_hits: int = 0
    bid_hits: int = 0
    delta: float = 0.0
    uncertain: int = 0
    prev: Optional[Tick] = None
    prior_different_price: Optional[float] = None
    vol_deltas: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)

    @property
    def total_hits(self) -> int:
        return self.ask_hits + self.bid_hits

    @property
    def margin(self) -> int:
        """Ask hits minus bid hits. Positive is a strong tape."""
        return self.ask_hits - self.bid_hits

    @property
    def imbalance(self) -> float:
        """Hit margin normalised to [-1, 1]; 0 when balanced or empty."""
        total = self.total_hits
        return (self.margin / total) if total else 0.0

    @property
    def divergent(self) -> bool:
        """Count-based and volume-based signals disagree in sign.

        The single-print artifact detector: when one large print dominates the
        volume delta while the count margin points the other way.
        """
        if self.margin == 0 or self.delta == 0:
            return False
        return (self.margin > 0) != (self.delta > 0)

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "ask_hits": self.ask_hits,
            "bid_hits": self.bid_hits,
            "total_hits": self.total_hits,
            "margin": self.margin,
            "imbalance": round(self.imbalance, 4),
            "delta": round(self.delta, 2),
            "uncertain": self.uncertain,
            "divergent": self.divergent,
            **self.meta,
        }


class SessionAccumulator:
    """Accumulates classified observations across polls for one market."""

    def __init__(self, cfg, market: str = "equity"):
        self.cfg = cfg
        self.market = market
        self.states: dict[str, TickerState] = {}
        self.reasons: Counter = Counter()
        self.attempted = 0
        self.classified = 0
        self.polls = 0
        self.session_date: Optional[str] = None

    # ── session lifecycle ────────────────────────────────────────

    def _roll_if_needed(self, session_date: Optional[str]) -> bool:
        """Clear counters when the feed's session date advances.

        Keyed off feed data, never the local clock — a machine in another
        timezone, or a session left running overnight, would otherwise roll at
        the wrong moment.
        """
        if session_date is None:
            return False
        if self.session_date is None:
            self.session_date = session_date
            return False
        if session_date != self.session_date:
            self.states.clear()
            self.reasons.clear()
            self.attempted = 0
            self.classified = 0
            self.polls = 0
            self.session_date = session_date
            return True
        return False

    # ── accumulation ─────────────────────────────────────────────

    def _winsor_cap(self, state: TickerState) -> Optional[float]:
        if len(state.vol_deltas) < MIN_SAMPLES_FOR_WINSOR:
            return None
        median = statistics.median(state.vol_deltas)
        return median * self.cfg.winsor_multiple if median > 0 else None

    def apply(
        self,
        rows: list[dict],
        *,
        session_date: Optional[str] = None,
        in_auction_window: bool = False,
    ) -> bool:
        """Fold one poll's rows into the accumulator. Returns True if it rolled."""
        rolled = self._roll_if_needed(session_date)
        self.polls += 1

        for row in rows:
            symbol = row.get("symbol")
            if not symbol:
                continue
            state = self.states.get(symbol)
            if state is None:
                state = TickerState(symbol=symbol)
                self.states[symbol] = state

            cur = Tick(
                last=_num(row.get("close")),
                bid=_num(row.get("bid")),
                ask=_num(row.get("ask")),
                volume=_num(row.get("volume")),
            )
            obs = classify(
                cur=cur,
                prev=state.prev,
                prior_different_price=state.prior_different_price,
                cfg=self.cfg,
                in_auction_window=in_auction_window,
            )

            if state.prev is not None:
                self.attempted += 1
                if obs.reason:
                    self.reasons[obs.reason] += 1

            if obs.classified:
                self.classified += 1
                capped = obs.volume_delta
                cap = self._winsor_cap(state)
                if cap is not None:
                    capped = min(capped, cap)
                state.vol_deltas.append(obs.volume_delta)
                if obs.is_buy:
                    state.ask_hits += 1
                    state.delta += capped
                else:
                    state.bid_hits += 1
                    state.delta -= capped
                if not obs.certain:
                    state.uncertain += 1

            # Track the most recent *different* price for the tick rule. Must be
            # updated regardless of classification outcome.
            if state.prev is not None and cur.last != state.prev.last:
                state.prior_different_price = state.prev.last
            state.prev = cur
            state.meta = _display_meta(row)

        return rolled

    # ── reporting ────────────────────────────────────────────────

    @property
    def coverage(self) -> float:
        """Share of attempted observations that produced a usable classification."""
        return (self.classified / self.attempted) if self.attempted else 0.0

    def active(self, min_hits: int = 0) -> list[TickerState]:
        return [s for s in self.states.values() if s.total_hits >= max(min_hits, 1)]

    def snapshot_stats(self) -> dict:
        return {
            "polls": self.polls,
            "attempted": self.attempted,
            "classified": self.classified,
            "coverage": round(self.coverage, 4),
            "rejections": dict(self.reasons),
            "session_date": self.session_date,
            "tracked": len(self.states),
        }


def _num(value) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _display_meta(row: dict) -> dict:
    """The subset of feed fields the UI renders, carried alongside counters."""
    keys = (
        "close", "change_pct", "sector", "industry", "avg_dollar_vol", "rvol",
        "High.1M", "Low.1M", "High.3M", "Low.3M", "High.6M", "Low.6M",
        "price_52_week_high", "price_52_week_low", "high", "low",
    )
    return {k: row[k] for k in keys if k in row and row[k] is not None}
