"""
Feature Engineering Pipeline

Transforms raw tick data into features for ML models and strategies.
Each feature is computed from the price history leading up to a prediction window.
"""

import numpy as np
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class FeatureSet:
    """Complete feature set for a single prediction window."""
    timestamp: float
    features: Dict[str, float]
    
    def to_array(self, feature_names: Optional[List[str]] = None) -> np.ndarray:
        names = feature_names or sorted(self.features.keys())
        return np.array([self.features.get(n, 0.0) for n in names])


class FeatureEngine:
    """Computes features from price history for prediction models."""

    @staticmethod
    def compute_all(prices: List[float], spreads: Optional[List[float]] = None, ohlcv: Optional[List] = None) -> Dict[str, float]:
        """Compute full feature set from price array."""
        if len(prices) < 30:
            return {}
        
        features = {}
        p = np.array(prices)
        
        # --- Returns at multiple horizons ---
        for lookback in [1, 2, 5, 10, 15, 30]:
            if len(p) > lookback:
                ret = (p[-1] / p[-1 - lookback] - 1) * 100
                features[f"return_{lookback}"] = ret
        
        # --- RSI ---
        features["rsi_14"] = FeatureEngine._rsi(p, 14)
        features["rsi_7"] = FeatureEngine._rsi(p, 7)
        
        # --- MACD ---
        # Compute the full EMA series to derive a proper 9-bar signal line
        k12 = 2 / (12 + 1)
        k26 = 2 / (26 + 1)
        ema12_series = np.empty(len(p))
        ema26_series = np.empty(len(p))
        ema12_series[0] = p[0]
        ema26_series[0] = p[0]
        for i in range(1, len(p)):
            ema12_series[i] = p[i] * k12 + ema12_series[i - 1] * (1 - k12)
            ema26_series[i] = p[i] * k26 + ema26_series[i - 1] * (1 - k26)
        macd_series = ema12_series - ema26_series
        k9 = 2 / (9 + 1)
        sig_series = np.empty(len(macd_series))
        sig_series[0] = macd_series[0]
        for i in range(1, len(macd_series)):
            sig_series[i] = macd_series[i] * k9 + sig_series[i - 1] * (1 - k9)
        macd_line = float(macd_series[-1])
        signal_line = float(sig_series[-1])
        features["macd"] = macd_line
        features["macd_signal"] = signal_line
        features["macd_histogram"] = macd_line - signal_line
        
        # --- Bollinger Bands ---
        sma20 = np.mean(p[-20:])
        std20 = np.std(p[-20:])
        if std20 > 0:
            features["bollinger_pct_b"] = (p[-1] - (sma20 - 2 * std20)) / (4 * std20)
            features["bollinger_width"] = (4 * std20) / sma20
        else:
            features["bollinger_pct_b"] = 0.5
            features["bollinger_width"] = 0
        
        # --- Stochastic ---
        for period in [14, 7]:
            window = p[-period:]
            low, high = np.min(window), np.max(window)
            features[f"stoch_k_{period}"] = ((p[-1] - low) / (high - low) * 100) if high != low else 50
        
        # --- EMAs and crossovers ---
        for period in [5, 8, 13, 21]:
            features[f"ema_{period}"] = FeatureEngine._ema(p, period)
            features[f"price_vs_ema_{period}"] = (p[-1] / features[f"ema_{period}"] - 1) * 100
        
        features["ema_cross_8_21"] = features["ema_8"] - features["ema_21"]
        
        # --- Volatility ---
        for lookback in [5, 10, 20]:
            if len(p) > lookback:
                returns = np.diff(p[-lookback:]) / p[-lookback:-1]
                features[f"volatility_{lookback}"] = np.std(returns) * 100
        
        # --- Price position in range ---
        for lookback in [10, 30, 60]:
            if len(p) > lookback:
                window = p[-lookback:]
                low, high = np.min(window), np.max(window)
                features[f"price_position_{lookback}"] = ((p[-1] - low) / (high - low)) if high != low else 0.5
        
        # --- Momentum acceleration ---
        if len(p) > 10:
            mom5 = p[-1] - p[-6]
            mom5_prev = p[-2] - p[-7]
            features["momentum_acceleration"] = mom5 - mom5_prev
        
        # --- Bid-ask spread features ---
        if spreads and len(spreads) > 10:
            s = np.array(spreads)
            features["spread_current"] = s[-1]
            features["spread_mean_10"] = np.mean(s[-10:])
            features["spread_expanding"] = 1.0 if s[-1] > np.mean(s[-10:]) else 0.0
        
        # --- Volume-based features (from Binance OHLCV klines) ---
        # kline format: [open_time, open, high, low, close, volume, ...]
        if ohlcv and len(ohlcv) >= 15:
            features["mfi_14"]        = FeatureEngine._mfi(ohlcv, 14)
            features["mfi_7"]         = FeatureEngine._mfi(ohlcv, 7)
            vwap = FeatureEngine._vwap(ohlcv[-20:])
            if vwap > 0:
                features["vwap_ref"]       = vwap
                features["price_vs_vwap"]  = (p[-1] / vwap - 1) * 100
            obv_slope = FeatureEngine._obv(ohlcv[-10:]) - FeatureEngine._obv(ohlcv[-20:-10])
            features["obv_slope"]     = obv_slope
            # Volume surge: recent vs average
            vols = [float(k[5]) for k in ohlcv[-20:]]
            if len(vols) > 5 and np.mean(vols[:-1]) > 0:
                features["volume_surge"] = vols[-1] / np.mean(vols[:-1])
            # VWAP deviation bands
            if vwap > 0:
                closes = [float(k[4]) for k in ohlcv[-20:]]
                tp_vals = [(float(k[2])+float(k[3])+float(k[4]))/3 for k in ohlcv[-20:]]
                std_tp = float(np.std(tp_vals))
                features["vwap_band_pos"] = (p[-1] - vwap) / (2 * std_tp) if std_tp > 0 else 0

        # --- Higher-order features ---
        if len(p) > 20:
            # Trend strength: how linear is recent price action
            x = np.arange(20)
            slope, intercept = np.polyfit(x, p[-20:], 1)
            predicted = slope * x + intercept
            ss_res = np.sum((p[-20:] - predicted) ** 2)
            ss_tot = np.sum((p[-20:] - np.mean(p[-20:])) ** 2)
            r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
            features["trend_r_squared"] = r_squared
            features["trend_slope"] = slope
        
        return features

    @staticmethod
    def _rsi(prices: np.ndarray, period: int) -> float:
        """Wilder's RSI — uses full price history with exponential smoothing (matches TradingView)."""
        if len(prices) < period + 1:
            return 50.0
        deltas = np.diff(prices.astype(float))
        gains  = np.maximum(deltas, 0.0)
        losses = np.maximum(-deltas, 0.0)
        avg_g = float(gains[:period].mean())
        avg_l = float(losses[:period].mean())
        for i in range(period, len(deltas)):
            avg_g = (avg_g * (period - 1) + gains[i]) / period
            avg_l = (avg_l * (period - 1) + losses[i]) / period
        if avg_l == 0:
            return 100.0 if avg_g > 0 else 50.0
        return 100.0 - (100.0 / (1.0 + avg_g / avg_l))

    @staticmethod
    def _ema(prices: np.ndarray, period: int) -> float:
        k = 2 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = price * k + ema * (1 - k)
        return ema

    @staticmethod
    def _ema_from_values(values: np.ndarray, period: int) -> float:
        k = 2 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = v * k + ema * (1 - k)
        return ema

    @staticmethod
    def _mfi(ohlcv: List, period: int = 14) -> float:
        """Money Flow Index using Binance kline data. Wilder-smoothed to prevent spurious 100.0."""
        if len(ohlcv) < period + 1:
            return 50.0
        tp  = [(float(k[2]) + float(k[3]) + float(k[4])) / 3 for k in ohlcv]
        vol = [float(k[5]) for k in ohlcv]
        rmf = [tp[i] * vol[i] for i in range(len(tp))]
        pos = sum(rmf[i] for i in range(1, period + 1) if tp[i] > tp[i - 1])
        neg = sum(rmf[i] for i in range(1, period + 1) if tp[i] < tp[i - 1])
        for i in range(period + 1, len(tp)):
            new_pos = rmf[i] if tp[i] > tp[i - 1] else 0.0
            new_neg = rmf[i] if tp[i] < tp[i - 1] else 0.0
            pos = (pos * (period - 1) + new_pos) / period
            neg = (neg * (period - 1) + new_neg) / period
        if neg == 0:
            return 100.0 if pos > 0 else 50.0
        return 100.0 - (100.0 / (1.0 + pos / neg))

    @staticmethod
    def _vwap(ohlcv: List) -> float:
        """Volume Weighted Average Price."""
        cum_tv = sum((float(k[2]) + float(k[3]) + float(k[4])) / 3 * float(k[5]) for k in ohlcv)
        cum_v  = sum(float(k[5]) for k in ohlcv)
        return cum_tv / cum_v if cum_v > 0 else 0.0

    @staticmethod
    def _obv(ohlcv: List) -> float:
        """On Balance Volume (cumulative)."""
        obv = 0.0
        for i in range(1, len(ohlcv)):
            c, cp = float(ohlcv[i][4]), float(ohlcv[i - 1][4])
            v = float(ohlcv[i][5])
            obv += v if c > cp else (-v if c < cp else 0.0)
        return obv

    @staticmethod
    def get_feature_names() -> List[str]:
        """Return sorted list of all feature names for consistent ordering."""
        # Generate a dummy feature set to get all names
        dummy_prices = list(np.cumsum(np.random.randn(200) * 0.1) + 70000)
        features = FeatureEngine.compute_all(dummy_prices)
        return sorted(features.keys())
