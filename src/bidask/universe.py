"""Universe filtering and the relative-volume gate for the tape board.

Two distinct cuts, deliberately separated:

* **Liquidity floors** decide what is worth *polling*. Most are pushed
  server-side by `feed`; average dollar volume lands here because the screener
  library rejects column arithmetic.
* **The relative-volume gate** decides what reaches a column. One request covers
  the whole universe in under half a second, so this gate exists for the
  reader's attention, not for throughput.

Both halves are published, not just the second. `build_columns` counts the
**pre-gate** universe for its breadth denominator — after the gate every
surviving row qualifies by construction, so a post-gate denominator makes every
group's share 1.0 and the term ranks nothing.

⛔ The gate is the only admission path to either column. A price move admits
nothing on its own, however far it has run. The absolute-change leg that used to
do that is gone and its config key raises: a ticker up 12% on 0.4x its usual
participation is a stock nobody is trading, and the board exists to name the
themes being accumulated rather than the ones that happened to move.

⛔ Two anchors are in play. `elapsed_minutes` counts from the **04:00** extended
anchor Relative Volume at Time uses. Each state's floor schedule counts from
**its own** state's start, so a regular-session band at 15 minutes means 09:45.
`rvol_at_time.threshold_for` converts between them; nothing here should.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

# `RVOL_FIELD` is imported rather than restated: the gate writes that column and
# `grouping.py` ranks on it. The whole point of its name — kept apart from the
# feed's `rvol`, which carries the screener's `relative_volume_10d_calc` — is
# that the two quantities never get read as interchangeable, and two spellings
# of one contract would defeat that on the first typo.
from src.bidask.grouping import RVOL_FIELD
from src.bidask.rvol_at_time import minutes_since_open, rvol_at_time, threshold_for
from src.bidask.session_state import MARKET, POST_MARKET, PRE_MARKET

# The live numerator per session state: today's cumulative volume since 04:00,
# assembled from the fields the feed already returns.
#
# ⛔ `volume` is a REGULAR-SESSION counter, not a running extended-day total,
# and before the bell it still holds YESTERDAY's completed day. The measurement
# behind that is one step removed and worth stating exactly: across 300 symbols
# over a 201-second pre-market gap, `premarket_volume` rose for 262 while
# `relative_volume_10d_calc` changed for 0 of 300. That field is `volume` over a
# daily constant, so an unchanged ratio means an unchanged numerator — `volume`
# did not move while the tape did. Reading it in the pre-market would therefore
# divide a whole previous session by this morning's expected few minutes, which
# admits the entire universe at once and reads as a market of extraordinary
# interest. Hence one tuple per state rather than one column.
#
# The regular session adds the morning to the session-to-date figure because the
# baseline is a sum from 04:00 and both legs have to start in the same place.
#
# ⛔ MEASURED 2026-09-17 14:39 ET, 8 of 8 rows: during the regular session
# `volume` ALREADY spans the extended day from 04:00, so it must be read alone.
# CTNT read 1,151,094,276 against pre-market bars of 805,266,601 plus regular
# bars of 325,117,028 — the 1.8% gap is only the unclosed last bar — and
# `premarket_volume` matched the 04:00-09:29 bar sum exactly on every row.
# Adding `premarket_volume` to it therefore counts the morning twice, which on
# CTNT inflates the numerator by about 70%. An earlier revision summed them as
# the "safe" assumption; it was not safe, it was wrong, and it was wrong in the
# direction that admits too much.
#
# In PRE-MARKET the same field is not today's figure at all: it still holds the
# previous completed session (measured median 2,213,074 against a median
# `premarket_volume` of 6,448), so that state reads `premarket_volume` alone.
#
# POST_MARKET follows the regular session by construction, since `volume` is one
# running extended-day total. The retrospective check against historical bars is
# consistent with that but not clean enough to call verified on its own, so
# Verification Contract check 5 covers it live.
#
# A state absent from this table scores every row 0 and admits nothing. That
# covers `closed` and, for now, `crypto`: crypto `volume` is a 24-hour rolling
# figure rather than a sum from a session anchor, so it needs its own numerator
# and its own baseline before it can be gated. The gate can already express the
# crypto FLOOR; the numerator is the piece still missing, and failing closed
# means the crypto board goes visibly dark rather than quietly wrong.
VOLUME_FIELDS = {
    PRE_MARKET: ("premarket_volume",),
    MARKET: ("volume",),
    POST_MARKET: ("volume",),
}


@dataclass(frozen=True)
class RvolGate:
    """One poll's gate result, with the coverage pair that keeps it honest.

    `scored` against `polled` is what separates "the relative-volume source is
    unusable" from "the market is quiet". An empty board carrying neither figure
    is indistinguishable from a healthy board on a dull morning, and that
    ambiguity is what hid a broken universe for a full session once already.
    """

    rows: pd.DataFrame          # qualifying rows, each carrying RVOL_FIELD
    floor: Optional[float]      # the floor applied, or None where the board is shut
    scored: int                 # rows with a usable reading
    polled: int                 # rows offered to the gate

    @property
    def source_unavailable(self) -> bool:
        """True when there were rules to judge by, rows to judge, and no reading.

        Each clause rules out an honest empty board. An empty response polled
        nothing, so nothing failed — reporting a dead source there blames the
        vendor for our own upstream floors. A closed market has no floor, so
        every row scoring zero is the board being shut rather than the source
        being broken, and the two must not render as the same sentence.
        """
        return self.floor is not None and self.polled > 0 and self.scored == 0


def apply_liquidity(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Apply the floors the feed could not express server-side.

    Average dollar volume is average share volume times price. The screener has
    no such column and the library cannot compose one, so it is derived here.
    """
    if df.empty:
        return df
    out = df.copy()
    if "avg_volume" in out.columns and out["avg_volume"].notna().any():
        out["avg_dollar_vol"] = out["avg_volume"].astype(float) * out["close"].astype(float)
        out = out[out["avg_dollar_vol"] >= cfg.min_avg_dollar_vol]
    else:
        # Crypto carries no average-volume field; the floor cannot be applied
        # and the column is surfaced as absent rather than silently zeroed.
        out["avg_dollar_vol"] = None
    return out


def volume_since_anchor(df: pd.DataFrame, state: str) -> pd.Series:
    """Today's cumulative volume since 04:00, per row, for `state`.

    Every leg is coerced and its nulls filled with zero. A withdrawn or
    text-typed vendor column must cost the leg, not the poll: this runs on the
    poll path, and a raise here escapes to the loop's generic handler, which
    never writes the state file and freezes every field on the page.

    Filling a missing leg with zero understates the numerator, which fails
    closed. Filling it with anything else would invent volume.
    """
    total = pd.Series(0.0, index=df.index)
    for field in VOLUME_FIELDS.get(state, ()):
        if field in df.columns:
            total = total + pd.to_numeric(df[field], errors="coerce").fillna(0.0)
    return total


def score_rvol_at_time(
    df: pd.DataFrame,
    *,
    state: str,
    profiles: Optional[dict] = None,
    elapsed_minutes: Optional[float] = None,
) -> pd.Series:
    """Relative Volume at Time for every row, as a Series aligned to `df`.

    This ticker's volume since 04:00 over the mean of **its own** volume by the
    same point of day across recent sessions. A ticker with no baseline — a
    fresh listing, a download miss, or the warm-up still running — scores 0 and
    is excluded, never admitted as an unknown.

    `profiles` and `elapsed_minutes` are passed in rather than read from the
    cache and the clock here, so the gate stays a pure function of its inputs.
    """
    if df.empty:
        return pd.Series(dtype=float)
    elapsed = minutes_since_open() if elapsed_minutes is None else elapsed_minutes
    table = profiles or {}
    volumes = volume_since_anchor(df, state)
    symbols = df["symbol"] if "symbol" in df.columns else pd.Series("", index=df.index)
    return pd.Series(
        [rvol_at_time(volume, table.get(str(symbol)), elapsed)
         for symbol, volume in zip(symbols, volumes)],
        index=df.index,
        dtype=float,
    )


def apply_rvol_gate(
    df: pd.DataFrame,
    cfg,
    *,
    state: str,
    profiles: Optional[dict] = None,
    elapsed_minutes: Optional[float] = None,
) -> RvolGate:
    """Keep the rows trading on unusual participation for this time of day.

    The floor comes from `state`'s own schedule. A state with no schedule —
    a closed market, or a `current_session` value the feed has never sent —
    admits nothing: the board cannot judge a window whose rules were never
    written, and the alternative is applying another state's floor to it.
    """
    if df.empty:
        return RvolGate(rows=df, floor=None, scored=0, polled=0)

    elapsed = minutes_since_open() if elapsed_minutes is None else elapsed_minutes
    readings = score_rvol_at_time(df, state=state, profiles=profiles,
                                  elapsed_minutes=elapsed)
    scored = int((readings > 0).sum())
    floor = threshold_for(cfg.in_play_rvol_schedules, state, elapsed)

    out = df.copy()
    out[RVOL_FIELD] = readings
    if floor is None:
        out = out.iloc[0:0]
    else:
        out = out[readings >= floor]
    return RvolGate(rows=out, floor=floor, scored=scored, polled=len(df))


def exclude_symbols(df: pd.DataFrame, excluded) -> pd.DataFrame:
    """Drop symbols whose tape carries no information.

    Stablecoins are the motivating case: pegged at $1, so their price never
    answers the strong/weak test while they trade constantly, which keeps them
    near the top of any volume-ranked column carrying nothing.
    """
    if df.empty or not excluded or "symbol" not in df.columns:
        return df
    return df[~df["symbol"].astype(str).str.upper().isin(excluded)]


def build_universe(df: pd.DataFrame, cfg, *, market: str = "equity") -> pd.DataFrame:
    """The poll's liquidity-filtered universe, BEFORE the relative-volume gate.

    Deliberately does not gate. This frame is `build_columns`'s breadth
    denominator as well as the gate's input, and folding the two together would
    leave every group's qualifying share at 1.0.
    """
    out = apply_liquidity(df, cfg)
    if market == "crypto":
        out = exclude_symbols(out, cfg.crypto_exclude)
    return out
