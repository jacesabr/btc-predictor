"""
Linear Regression Channel Strategy

Replaces the previous sklearn/XGBoost ML strategies with a deterministic
Linear Regression Channel indicator — no training required, data-ready
from the first Binance candle batch.

Signal:     UP when regression slope > 0 (rising trend), DOWN otherwise.
Confidence: 0.50 + R²×0.25 + slope-magnitude boost (max 0.85).
Value:      R² — measures how linear/clean the trend is (0–1).
"""

import numpy as np
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class LinearRegressionChannel:
    """
    Fits a least-squares regression line to the last PERIOD closes and
    extrapolates one bar forward.  Requires no training or labelling —
    works immediately with any price data.

    Uses Binance OHLCV closes when available, falls back to tick prices.
    The strategy key is kept as 'ml_logistic' so the frontend still
    renders it as 'LR' without any frontend change.
    """

    name = "ml_logistic"   # keep original key → frontend shows "LR"
    PERIOD = 30

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])

        # Prefer exchange closes (cleaner, volume-weighted)
        if ohlcv and len(ohlcv) >= self.PERIOD + 5:
            src = np.array([float(c[4]) for c in ohlcv[-(self.PERIOD + 5):]], dtype=float)
        elif len(prices) >= self.PERIOD + 5:
            src = np.array(prices[-(self.PERIOD + 5):], dtype=float)
        else:
            return {
                "signal": "UP", "confidence": 0.45,
                "reasoning": "LR: insufficient data",
                "value": "0.000",
                "htf_signal": "N/A", "crossover": False,
                "crossunder": False, "mtf_agree": None,
            }

        recent = src[-self.PERIOD:]
        x    = np.arange(self.PERIOD, dtype=float)
        x_mu = x.mean()
        y_mu = recent.mean()

        slope     = np.sum((x - x_mu) * (recent - y_mu)) / np.sum((x - x_mu) ** 2)
        intercept = y_mu - slope * x_mu

        # R² — measures trend linearity (0 = random, 1 = perfect trend)
        y_hat  = slope * x + intercept
        ss_res = np.sum((recent - y_hat) ** 2)
        ss_tot = np.sum((recent - y_mu) ** 2)
        r2     = max(0.0, 1.0 - ss_res / ss_tot) if ss_tot > 1e-10 else 0.0

        slope_pct = slope / recent[-1] * 100      # % per 1-min bar
        signal    = "UP" if slope > 0 else "DOWN"
        conf      = max(0.40, min(0.85,
                        0.50 + r2 * 0.25 + min(abs(slope_pct) * 15, 0.10)))

        return {
            "signal": signal,
            "confidence": round(conf, 4),
            "reasoning": f"LR({self.PERIOD}) slope={slope_pct:+.4f}%/bar R\u00b2={r2:.3f}",
            "value": f"{r2:.3f}",
            "htf_signal": "N/A", "crossover": False,
            "crossunder": False, "mtf_agree": None,
        }
