"""
Pydantic schemas for every Redis Streams event type.

Stream topology:
  ingestion  → [raw_bar]       → feature
  feature    → [feature_vec]   → model
  model      → [regime_state]  → api
  model      → [refit_event]   → stress_test
  stress_test→ [gate_result]   → api
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class RawBar(BaseModel):
    """One 1h OHLCV bar with positioning and funding data, published by ingestion."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    taker_buy_base_volume: float  # klines col 9; used to reconstruct CVD (O5)
    oi: float                     # open interest from Vision Metrics (raw level)
    lsr: float                    # long/short ratio from Vision Metrics
    funding_rate: float
    funding_interval_hours: float  # from Binance API; U4 is derived from this, never hardcoded


class FeatureVec(BaseModel):
    """Normalised feature vectors for one 1h bar, published by the feature service."""

    timestamp: datetime
    o: list[float] = Field(..., min_length=8, max_length=8)  # O1–O8 observation vector
    u: list[float] = Field(..., min_length=4, max_length=4)  # U1–U4 input vector


class RegimeState(BaseModel):
    """Viterbi-decoded regime for one 1h bar, published by the model service."""

    timestamp: datetime
    state: int = Field(..., ge=0)   # latent state index, 0 = calmest by vol ordering
    posterior: list[float]          # γ_t(k) for k=0..K-1; sums to 1
    log_likelihood: float
    fold_id: str                    # identifies the refit that produced this decode


class RefitEvent(BaseModel):
    """Signals a completed model refit; consumed by stress_test to trigger gate checks."""

    timestamp: datetime
    fold_id: str
    trigger: str        # "ll_drop" | "degeneracy" | "cusum" | "scheduled"
    train_start: datetime
    train_end: datetime


class GateResult(BaseModel):
    """Stress test outcome for one fold; published by stress_test, cached by api."""

    timestamp: datetime
    fold_id: str
    # Computed by stress_test: all(gates.values()). Do NOT include when inserting
    # into Postgres — gate_results.passed is a GENERATED column derived from gates JSONB.
    passed: bool
    gates: dict[str, bool]          # per-gate pass/fail; keys match tier definitions
    advisories: dict[str, float]    # advisory metric values — never gates
    details: dict                   # per-gate raw values and metadata for diagnostics
