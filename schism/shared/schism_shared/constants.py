from typing import Final

CADENCE: Final[str] = "1h"
COMMON_FLOOR: Final[str] = "2020-09-01"  # OI/LSR Vision Metrics floor — hard floor for all features
APPROX_BARS: Final[int] = 50_000         # ~5.7 years at 1h cadence

# PROVISIONAL: K=4 is the working assumption; pending BIC/AIC/ICL sweep over {2,3,4,5,6}.
K: Final[int] = 4

STREAM_NAMES: Final[dict[str, str]] = {
    "raw_bar":      "raw_bar",
    "feature_vec":  "feature_vec",
    "regime_state": "regime_state",
    "refit_event":  "refit_event",
    "gate_result":  "gate_result",
}
