"""Configuration for BTC Oracle Predictor."""
import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    # Data source
    poll_interval_seconds: float = 12.0

    # Prediction windows
    window_duration_seconds: int = 300  # 5 minutes

    # Strategy weights (dynamically adjusted based on rolling accuracy)
    # Removed: bollinger, momentum, price_action, mfi, obv, htf_ema, volume_momentum, ml_gradient
    initial_weights: dict = field(default_factory=lambda: {
        # Oscillators
        "rsi":         1.0,
        "macd":        1.0,
        "stochastic":  1.0,
        # Trend & structure
        "ema_cross":   1.1,
        "supertrend":  1.1,
        "adx":         1.0,
        # DeepSeek specialist patterns (overrides math fallbacks at bar open)
        "alligator":   1.1,
        "acc_dist":    1.0,
        "dow_theory":  1.2,
        "fib_pullback":1.0,
        "harmonic":    1.0,
        # Volume / VWAP
        "vwap":        1.1,
        # Market / crowd
        "polymarket":  1.3,
        # Linear regression channel
        "ml_logistic": 1.2,
    })

    # EV thresholds
    min_ev_to_enter: float = 0.05
    strong_ev_threshold: float = 0.15
    max_kelly_fraction: float = 0.25

    # Backtest
    rolling_window_size: int = 12
    min_predictions_for_weight_update: int = 10

    # API
    api_host: str = "0.0.0.0"
    api_port: int = int(os.environ.get("PORT", 8000))

    # DeepSeek AI integration
    deepseek_api_key: str = os.environ.get("DEEPSEEK_API_KEY", "")
    deepseek_model: str = "deepseek-chat"
    deepseek_vision_model: str = "deepseek-vl2"
    deepseek_use_vision: bool = False
    deepseek_enabled: bool = True

    # Dashboard signal API keys (for microstructure data fed into DeepSeek)
    coinalyze_key: str = os.environ.get("COINALYZE_KEY", "")

    # Feature engineering
    feature_windows: List[int] = field(default_factory=lambda: [5, 10, 15, 30, 60, 120])
    rsi_period: int = 14
    bollinger_period: int = 20
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    ema_fast: int = 8
    ema_slow: int = 21
    stochastic_period: int = 14
