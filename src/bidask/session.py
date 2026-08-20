"""Session accumulator — trailing-window tape pressure per ticker.

Holds, per symbol and over a trailing window: ask-side hit count, bid-side hit
count, and a volume-weighted signed delta.

Counters used to run cumulatively from the open, and that is arithmetically
hostile to what they measure. Each poll contributes the
directional signal plus a small constant per-ticker bias — a ticker whose prints
habitually sit high in its own spread earns a sliver of ask-side surplus on
every poll, whether or not it moves. The signal is bounded by the day's net
move; the bias grows with the poll count. Given enough polls the bias wins.

Measured live on 2026-08-20 against the running dashboard: its hit increments
tracked price at Spearman +0.31 over 30 seconds, +0.40 over a minute and +0.47
over three minutes, while its cumulative session margin scored +0.18 over that
same stretch and +0.10 against the day's move. The strong column then averaged
-1.03% since 09:30 against the weak column's -1.28%.

Be precise about how much of that the window fixes, because it is less than it
looks. Most of the constant bias came from the quote rule reading the wrong
book, and `classify.py` fixes that at the source: on a 55-minute tape the
board's correlation with a static position-in-spread is now +0.04 at every
window length from 5 minutes to no window at all. Swept against each window's
OWN horizon, 5/10/15/20/30/45 minutes and cumulative all score +0.30 to +0.40 —
there is no measured optimum in that range. The window is kept for three
reasons, none of which is a peak in that sweep: it bounds whatever bias
survives, it bounds the dwell bias below, and "offers being lifted" is a claim
about now rather than about 09:12. Set the config key to 0 to compare directly.
That sweep does NOT clear a full trading session -- it covers 55 minutes, and
no measurement here tests six hours of accumulation under the corrected rule.

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

import math
import statistics
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Optional

from src.bidask.classify import REJECT_NO_TRADE, Tick, classify

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
    # Poll index when this ticker was last present in the feed. A gap means the
    # ticker left the in-play universe and returned; bridging it would book the
    # whole absence as one signed print.
    last_seen_poll: Optional[int] = None
    # Observations still inside the trailing window, oldest first:
    # (timestamp, sign, signed volume, uncertain). Kept only so the counters
    # above can be *un*-counted as observations age out. Empty when the window
    # is disabled, in which case the counters run cumulatively as before.
    events: deque = field(default_factory=deque)

    def record(self, *, at: float, sign: int, signed_volume: float,
               uncertain: bool, windowed: bool) -> None:
        """Book one classified observation. A zero sign is not an observation.

        The caller guards with `obs.classified`, but this used to be inline
        inside that guard and is now a named method the next caller can reach
        directly. An `else` branch would book an unclassified poll as selling
        pressure and then hand `prune` a bid hit to un-count later.
        """
        if sign == 0:
            return
        if sign > 0:
            self.ask_hits += 1
        else:
            self.bid_hits += 1
        self.delta += signed_volume
        if uncertain:
            self.uncertain += 1
        if windowed:
            self.events.append((at, sign, signed_volume, uncertain))

    def prune(self, cutoff: float) -> None:
        """Drop observations older than `cutoff`, un-counting each one.

        Only called when the window is enabled. An emptied window snaps the
        counters to exact zero rather than leaving float residue behind: `delta`
        is a running sum, and an empty window means no observations, which must
        read as no pressure and not as a rounding artefact.
        """
        while self.events and self.events[0][0] < cutoff:
            _, sign, signed_volume, uncertain = self.events.popleft()
            if sign > 0:
                self.ask_hits -= 1
            else:
                self.bid_hits -= 1
            self.delta -= signed_volume
            if uncertain:
                self.uncertain -= 1
        if not self.events:
            self.ask_hits = self.bid_hits = self.uncertain = 0
            self.delta = 0.0

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
        # Seconds of tape each counter covers. 0 disables the window and
        # restores cumulative-since-open behaviour.
        self.window_seconds = max(0.0, float(cfg.hit_window_minutes or 0) * 60.0)
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
        now: Optional[float] = None,
    ) -> bool:
        """Fold one poll's rows into the accumulator. Returns True if it rolled.

        `now` is the poll's timestamp, injected so the window is testable and so
        one clock read covers the whole poll. The window is measured in seconds,
        never in polls: the in-app cadence control retunes the poll rate at
        runtime, and a poll-counted window would silently change horizon with it.

        The clock is `monotonic`, never `time()`. A wall clock steps backwards
        on an NTP correction or a resume from sleep, and `cutoff` then moves
        further into the past than the window is wide, so nothing prunes until
        wall time catches up — the horizon silently widens while the pill still
        reads "last 30 min". A backwards step also appends an out-of-order
        timestamp, which `prune`'s single head-scan cannot evict on time. Both
        are the same silent-horizon failure this window is counted in seconds to
        avoid. Nothing here is ever compared against a wall-clock value.
        """
        rolled = self._roll_if_needed(session_date)
        self.polls += 1
        at = time.monotonic() if now is None else float(now)

        # Age out first, so this poll's own observations are never pruned. Every
        # state is swept, not just the ones in this poll's rows: a ticker that
        # left the in-play universe must decay off the board like any other,
        # rather than freezing at whatever it last scored.
        if self.window_seconds:
            cutoff = at - self.window_seconds
            for state in self.states.values():
                state.prune(cutoff)

        for row in rows:
            symbol = _clean_symbol(row.get("symbol"))
            if not symbol:
                continue
            state = self.states.get(symbol)
            if state is None:
                state = TickerState(symbol=symbol)
                self.states[symbol] = state

            # A ticker that fell out of the in-play gate and came back would
            # otherwise have its entire absence booked as one signed print:
            # volume_delta spans the whole gap and gets a single sign from a
            # single snapshot. Force a warmup instead of bridging.
            if state.last_seen_poll is not None and self.polls - state.last_seen_poll > 1:
                state.prev = None
                state.prior_different_price = None
            state.last_seen_poll = self.polls

            cur = Tick(
                last=_num(row.get("close")),
                bid=_num(row.get("bid")),
                ask=_num(row.get("ask")),
                volume=_num(row.get("volume")),
            )
            # Refresh the tick-rule reference BEFORE classifying. Updating it
            # afterwards leaves the classifier holding the pre-change value on
            # exactly the poll where the price moved, so mid-spread prints go
            # unclassified and the drift override never fires.
            if state.prev is not None and cur.last != state.prev.last:
                state.prior_different_price = state.prev.last

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
                state.record(
                    at=at,
                    sign=obs.sign,
                    signed_volume=capped if obs.is_buy else -capped,
                    uncertain=not obs.certain,
                    windowed=bool(self.window_seconds),
                )

            state.prev = cur
            state.meta = _display_meta(row)

        return rolled

    # ── reporting ────────────────────────────────────────────────

    @property
    def traded(self) -> int:
        """Attempted observations where a trade actually printed.

        `no_trade` is not a classification failure — most symbols simply do not
        print in any given interval, and that share rises with a broad universe
        or a tight cadence. Counting it against coverage would make a correctly
        working scanner look broken.
        """
        return self.attempted - self.reasons.get(REJECT_NO_TRADE, 0)

    @property
    def coverage(self) -> float:
        """Share of *actual trades* that produced a usable classification.

        Measures classification quality. The denominator excludes polls where
        nothing traded; `trade_rate` reports those separately.
        """
        return (self.classified / self.traded) if self.traded else 0.0

    @property
    def trade_rate(self) -> float:
        """Share of attempted observations where a trade printed."""
        return (self.traded / self.attempted) if self.attempted else 0.0

    def active(self, min_hits: int = 0) -> list[TickerState]:
        return [s for s in self.states.values() if s.total_hits >= max(min_hits, 1)]

    def snapshot_stats(self) -> dict:
        return {
            "polls": self.polls,
            "attempted": self.attempted,
            "traded": self.traded,
            "classified": self.classified,
            "coverage": round(self.coverage, 4),
            "trade_rate": round(self.trade_rate, 4),
            "rejections": dict(self.reasons),
            "session_date": self.session_date,
            "tracked": len(self.states),
        }


def _clean_symbol(value) -> str:
    """Normalise a symbol, rejecting the NaN that pandas yields for null cells.

    `float('nan')` is truthy, so a plain falsiness check lets it through and it
    renders as a literal "nan" ticker.
    """
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _num(value) -> float:
    """Coerce to float, mapping None and non-finite values to 0.0.

    0.0 is deliberate rather than NaN-passthrough: it makes the classifier's
    positivity preconditions fire, so a null field is rejected as a missing
    quote instead of slipping past every comparison.
    """
    try:
        if value is None:
            return 0.0
        result = float(value)
        return result if math.isfinite(result) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _is_finite(value) -> bool:
    """False for None and for the NaN pandas yields on null cells.

    Non-finite floats must never reach the payload: `json.dumps` writes a bare
    `NaN` token, which is valid Python but invalid JSON, so the browser's
    JSON.parse rejects the entire document and the page reports the server as
    unreachable. One recent IPO with a null period-high would do it.
    """
    if value is None:
        return False
    if isinstance(value, float):
        return math.isfinite(value)
    return True


def _display_meta(row: dict) -> dict:
    """The subset of feed fields the UI renders, carried alongside counters."""
    keys = (
        "close", "change_pct", "sector", "industry", "avg_dollar_vol", "rvol",
        # Session-to-date liquidity, carried per ticker so the UI sliders can
        # filter without a refetch.
        "volume", "dollar_vol",
        "High.1M", "Low.1M", "High.3M", "Low.3M", "High.6M", "Low.6M",
        "price_52_week_high", "price_52_week_low", "high", "low",
    )
    return {k: row[k] for k in keys if k in row and _is_finite(row[k])}
