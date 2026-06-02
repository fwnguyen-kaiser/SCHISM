# SCHISM

**S**tate-**C**onditioned **H**idden **I**nput–Output **S**tochastic **M**odel

![Status](https://img.shields.io/badge/status-in_development-orange?style=flat-square)
![Version](https://img.shields.io/badge/spec-v2.1-blue?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)
![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square)
![Cadence](https://img.shields.io/badge/cadence-1h-purple?style=flat-square)
![Gates](https://img.shields.io/badge/stress_test-16_gates-red?style=flat-square)
![NumPy](https://img.shields.io/badge/numpy-%23013243?style=flat-square&logo=numpy&logoColor=white)
![SciPy](https://img.shields.io/badge/scipy-%230C55A5?style=flat-square&logo=scipy&logoColor=white)
![Redis](https://img.shields.io/badge/redis-%23DD0031?style=flat-square&logo=redis&logoColor=white)
![FastAPI](https://img.shields.io/badge/fastapi-%23009688?style=flat-square&logo=fastapi&logoColor=white)

A regime-identification model for BTC perpetual futures. Fits a multivariate Student-*t* IOHMM on 1-hour OHLCV and derivatives data to label the current market regime — not to predict returns.

> *A model that knows what it is not is more useful than one that pretends to be everything.*

---

## What it does

SCHISM fits a **multivariate Student-*t* Input–Output Hidden Markov Model (IOHMM)** on one-hour BTC perpetual-futures data. It outputs a probability distribution over K latent regimes at each bar, plus a hard Viterbi path for downstream consumption.

The central design choice is a strict separation between two roles:

| Vector | Role | Contents |
|--------|------|----------|
| $O_t$ (dim 8) | *Characterize* the current regime | Log return, intrabar range, bar body, volume shock, CVD/volume, illiquidity proxy, detrended OI, smoothed LSR |
| $U_t$ (dim 4) | *Drive* regime switching | ΔOI, ΔLSR, funding rate (EWMA), settlement flag |

This separation — moving derivatives signals out of the emission vector and into the transition input — simultaneously fixes two structural failures of the previous generation: heterogeneous emission tails and a saturated input effect.

---

## Architecture

```
Oₜ ──► Student-t emission  ──► p(Oₜ | Sₜ = k)
                                       │
Uₜ ──► Softmax transition  ──► P(Sₜ₊₁ | Sₜ, Uₜ)
                                       │
                             ┌─────────▼──────────┐
                             │  IOHMM  (K = 4)    │
                             │  EM + aux. weights │
                             └────────────────────┘
```

**Emission:**

$$O_t \mid S_t = k \;\sim\; \mathcal{T}_{\nu}\!\left(\mu_k,\,\Sigma_k\right)$$

with shared degrees-of-freedom $\nu$. The scale-mixture representation furnishes auxiliary weights that downweight outliers in the M-step — robustness without winsorization.

**Transition:**

$$A_t(i,j) \;\propto\; \exp\!\left(\alpha_{ij} + \beta_{ij} \cdot U_t\right)$$

The bias $\alpha$ encodes a sticky-diagonal prior; $\beta$ carries the input-driven departure from persistence.

**Estimation:** Expectation–Maximization with per-bar auxiliary precision weights. The transition parameters $(\alpha, \beta)$ are updated by gradient on the expected complete-data log-likelihood, since the softmax has no closed-form M-step.

---

## Stress test

Every fitted model is subject to a five-tier falsification framework before it is accepted. **16 hard gates** must pass; advisory metrics are reported separately and never block acceptance.

```
Tier 0  Variable falsification   (pre-fit)
Tier 1  Machinery & optimization
Tier 2  Specification adequacy
Tier 3  IOHMM architecture
Tier 4  Semantics & distinctness
```

The stress test is the model's accountability layer. If it passes all 16 gates, the model earned it.

Key design decisions in the test:

- **`emission_ks`** upgraded from advisory to gate — OHLC observation channels are tail-homogeneous, so a single elliptical Student-t is now an appropriate family.
- **`feature_dominance`** uses share-of-total $(\max_d F_d \;/\; \sum_d F_d)$, not max/median — robust when the discriminant structure is sparse.
- **`max_iter_convergence`** distinguishes genuine non-convergence (hit the iteration wall while LL still moving) from acceptable termination (LL already flat at the wall). Tolerance is $10^{-8}$, not zero.
- **`variable_role_consistency`** (advisory) — a formal O/U assignment check using multinomial logistic ΔLL; `role_ratio` $= \text{Score}_U \;/\; (\text{Score}_O + \text{Score}_U)$. Full $K \times K$ multinomial structure retained.

---

## Data contract

| Source | Channels | Floor |
|--------|----------|-------|
| Binance Vision klines 1h | O1–O6, O5 via `taker_buy_base_volume` | 2020-01 |
| Binance Vision funding | U3 (FR EWMA), U4 (settlement flag) | 2020-01 |
| Binance Vision metrics | O7 (OI), O8 (LSR), U1 (ΔOI), U2 (ΔLSR) | **2020-09** |

**Common floor: 2020-09-01.** The Vision-metrics source for OI/LSR begins September 2020, setting the earliest date at which all 12 channels are simultaneously available. This sacrifices the March 2020 COVID crash — a deliberate trade: one event versus one full positioning dimension across the entire sample.

Dropped channels — all for data-source reasons, not modeling: bid-ask spread (no free historical orderbook), cross-exchange funding spread (Bybit floor 2021-01 → ragged), liquidation feed (Binance `liquidationSnapshot` 404s across all years). Zero-fills were rejected in every case; a phantom input is worse than an absent one.

---

## Training

SCHISM is trained **walk-forward** — BTC is non-stationary across years, so a single full-history fit is not an option. All walk-forward parameters (window W, embargo, slide step, max EM iterations, convergence tolerance) are configurable at runtime and logged for reproducibility.

Minimum window guidance: $K{=}4,\; D{=}8,\; \dim U{=}4$ yields ~145 parameters under a diagonal-$\Sigma$ approximation; a window of $\geq 3{,}000$ bars gives ~20 observations per parameter.

Post-refit state alignment uses the **Hungarian algorithm** on pairwise emission KL distance, composed with an ascending-volatility ordering so that state 0 is always the calmest regime and state $K{-}1$ the most volatile.

---

## Repository layout

```
schism/
├── models/
│   ├── emissions/          Student-t log-emission, auxiliary weights
│   ├── transitions/        Softmax IOHMM, logit computation
│   ├── iohmm.py            Forward-backward, Viterbi, EM loop
│   └── stress_test/        Five-tier falsification framework
│       ├── tier0_variable.py
│       ├── tier1_machinery.py
│       ├── tier2_specification.py
│       ├── tier3_architecture.py
│       ├── tier4_semantics.py
│       ├── economic_descriptive.py
│       ├── aggregator.py
│       ├── _helpers.py
│       └── _thresholds.py
├── data/
│   └── crawler/            Binance Vision data pipeline
├── docs/
│   └── schism_v2_spec.pdf  Full technical specification
└── scripts/                Walk-forward runner, alignment, diagnostics
```

---

## Specification

The full technical specification — mathematical derivations, acceptance criteria, stress-test gate definitions, and data-contract details — is in [`docs/schism_v2_spec.pdf`](docs/schism_v2_spec.pdf).

Current version: **v2.1**

---

## Status

| Item | State |
|------|-------|
| Stress test framework | ✅ Complete, smoke-tested |
| Data pipeline (klines, funding) | ✅ Verified to 2020-01 |
| Data pipeline (Vision metrics) | ✅ Verified to 2020-09 |
| EM core + auxiliary weights | 🔧 In progress |
| Walk-forward runner | 🔧 In progress |
| K-selection sweep (BIC/AIC/ICL) | ⏳ Pending |
| Real-data stress-test validation | ⏳ Pending |

---

## License

MIT — see [LICENSE](LICENSE).
