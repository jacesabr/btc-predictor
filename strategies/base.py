"""
Trading Strategies — optimised for 1-minute BTC scalping

Settings rationale
──────────────────
RSI(4):         Fast RSI for scalping; OB/OS at 80/20 instead of 70/30
MACD(3,10,16):  Raschke "3-10 oscillator" — standard intraday scalping setup
BB(10):         Shorter look-back captures micro-volatility better than 20
Stoch(5,3,3):   Fast stochastic — standard scalp/day-trade setup
EMA cross:      LTF(4,9) for 1-min entries; HTF(8,21) on aggregated 5-min
Momentum ROC:   4-bar rate-of-change for ultra-short speed reading
MFI(7):         Reduced from 14 — quicker volume-weighted OB/OS detection
OBV(4 vs 8):    Short slope windows for 1-min accumulation/distribution
VWAP:           Rolling 20-bar VWAP — unchanged but now has MTF layer
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import numpy as np


# =============================================================================
# Module-level helpers
# =============================================================================

def _ema_series(prices: np.ndarray, period: int) -> np.ndarray:
    """Return the full EMA array (same length as prices)."""
    k = 2.0 / (period + 1)
    out = np.empty(len(prices))
    out[0] = prices[0]
    for i in range(1, len(prices)):
        out[i] = prices[i] * k + out[i - 1] * (1 - k)
    return out


def _rsi_val(prices: np.ndarray, period: int) -> float:
    """Wilder's RSI — uses full price history with exponential smoothing (matches TradingView)."""
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices.astype(float))
    gains  = np.maximum(deltas, 0.0)
    losses = np.maximum(-deltas, 0.0)
    # Seed with simple average of first period
    avg_g = float(gains[:period].mean())
    avg_l = float(losses[:period].mean())
    # Wilder smoothing over all remaining bars
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0 if avg_g > 0 else 50.0
    return 100.0 - (100.0 / (1.0 + avg_g / avg_l))


def _aggregate_ohlcv(ohlcv: list, n: int = 5) -> list:
    """
    Aggregate consecutive groups of *n* 1-min bars into higher-timeframe bars.
    OHLCV format: [ts, open, high, low, close, vol]
    Returns only complete groups (trailing partial group is discarded).
    """
    if len(ohlcv) < n:
        return []
    num_bars = len(ohlcv) // n
    result = []
    for i in range(num_bars):
        chunk = ohlcv[i * n:(i + 1) * n]
        ts    = chunk[0][0]
        open_ = float(chunk[0][1])
        high  = max(float(c[2]) for c in chunk)
        low   = min(float(c[3]) for c in chunk)
        close = float(chunk[-1][4])
        vol   = sum(float(c[5]) for c in chunk)
        result.append([ts, open_, high, low, close, vol])
    return result


def _closes(ohlcv: list) -> list:
    return [float(c[4]) for c in ohlcv]


# =============================================================================
# Base class
# =============================================================================

class BaseStrategy(ABC):
    name: str = "base"

    @abstractmethod
    def predict(self, prices: List[float], **kwargs) -> Dict:
        pass

    def _clamp_confidence(self, conf: float) -> float:
        return max(0.40, min(0.85, conf))

    def _no_data(self, reason: str = "Insufficient data") -> Dict:
        return {
            "signal": "UP", "confidence": 0.45, "reasoning": reason, "value": "N/A",
            "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
        }


# =============================================================================
# Strategy implementations
# =============================================================================

class RSIStrategy(BaseStrategy):
    """RSI(4) — fast RSI for 1-min scalping. OB/OS at 80/20."""
    name = "rsi"
    PERIOD = 4
    OB, OS = 80, 20

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        # Use 1-min OHLCV close prices to match Binance/TradingView RSI(4).
        # Tick prices update every second so RSI(4) on ticks would be ~4-second
        # RSI, not 4-bar RSI. Fall back to tick prices only if klines unavailable.
        if ohlcv and len(ohlcv) >= self.PERIOD + 2:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)

        if len(p) < self.PERIOD + 2:
            return self._no_data()

        rsi      = _rsi_val(p,      self.PERIOD)
        prev_rsi = _rsi_val(p[:-1], self.PERIOD)

        crossover  = prev_rsi <= self.OS < rsi
        crossunder = prev_rsi >= self.OB > rsi

        # LTF signal
        if rsi < self.OS:
            signal = "UP";   conf = self._clamp_confidence(0.60 + (self.OS - rsi) / 100)
            reasoning = f"RSI oversold at {rsi:.1f}"
        elif rsi > self.OB:
            signal = "DOWN"; conf = self._clamp_confidence(0.60 + (rsi - self.OB) / 100)
            reasoning = f"RSI overbought at {rsi:.1f}"
        elif rsi < 45:
            signal = "UP";   conf = self._clamp_confidence(0.50 + (45 - rsi) / 200)
            reasoning = f"RSI leaning OS at {rsi:.1f}"
        elif rsi > 55:
            signal = "DOWN"; conf = self._clamp_confidence(0.50 + (rsi - 55) / 200)
            reasoning = f"RSI leaning OB at {rsi:.1f}"
        else:
            signal = "UP";   conf = 0.45
            reasoning = f"RSI neutral at {rsi:.1f}"

        if crossover:
            reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder:
            reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)

        # HTF
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * (self.PERIOD + 2):
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= self.PERIOD + 2:
                htf_rsi = _rsi_val(np.array(_closes(htf)), self.PERIOD)
                htf_signal = "UP" if htf_rsi < 50 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{rsi:.1f}",
            "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree,
        }


class MACDStrategy(BaseStrategy):
    """MACD(3,10,16) — Raschke's 3-10 oscillator; crossover on histogram sign change."""
    name = "macd"
    FAST, SLOW, SIG = 3, 10, 16

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        need = self.SLOW + self.SIG + 2
        # Use 1m OHLCV closes to match Binance/TradingView
        if ohlcv and len(ohlcv) >= need:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)

        if len(p) < need:
            return self._no_data()

        ema_f    = _ema_series(p, self.FAST)
        ema_s    = _ema_series(p, self.SLOW)
        macd_line = ema_f - ema_s
        sig_line  = _ema_series(macd_line, self.SIG)

        hist      = macd_line[-1]  - sig_line[-1]
        prev_hist = macd_line[-2]  - sig_line[-2]

        crossover  = prev_hist < 0 <= hist
        crossunder = prev_hist > 0 >= hist

        if hist >= 0:
            signal = "UP";   conf = self._clamp_confidence(0.52 + min(abs(hist) * 10, 0.30))
            reasoning = f"Bullish MACD hist +{hist:.4f}"
        else:
            signal = "DOWN"; conf = self._clamp_confidence(0.52 + min(abs(hist) * 10, 0.30))
            reasoning = f"Bearish MACD hist {hist:.4f}"

        if crossover:
            reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.07)
        elif crossunder:
            reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.07)

        # HTF
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * need:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= need:
                hp      = np.array(_closes(htf), dtype=float)
                h_ema_f = _ema_series(hp, self.FAST)
                h_ema_s = _ema_series(hp, self.SLOW)
                h_macd  = h_ema_f - h_ema_s
                h_sig   = _ema_series(h_macd, self.SIG)
                h_hist  = h_macd[-1] - h_sig[-1]
                htf_signal = "UP" if h_hist > 0 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{hist:.4f}",
            "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree,
        }


class BollingerStrategy(BaseStrategy):
    """Bollinger Bands(10, 2.0) — tighter window for 1-min micro-volatility."""
    name = "bollinger"
    PERIOD = 10
    WIDTH  = 2.0

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        # Use 1m OHLCV closes to match Binance/TradingView
        if ohlcv and len(ohlcv) >= self.PERIOD + 1:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)

        if len(p) < self.PERIOD + 1:
            return self._no_data()

        def pct_b(arr: np.ndarray) -> float:
            w   = arr[-self.PERIOD:]
            sma = np.mean(w)
            std = np.std(w)
            if std == 0:
                return 0.5
            upper, lower = sma + self.WIDTH * std, sma - self.WIDTH * std
            return float((arr[-1] - lower) / (upper - lower))

        pb      = pct_b(p)
        prev_pb = pct_b(p[:-1]) if len(p) > self.PERIOD else pb

        # Price exits lower band → crossover; exits upper band back inside → crossunder
        crossover  = prev_pb < 0   and pb >= 0
        crossunder = prev_pb > 1   and pb <= 1

        if pb < 0.10:
            signal = "UP";   conf = self._clamp_confidence(0.62)
            reasoning = f"Price at lower band (%B: {pb:.2f})"
        elif pb > 0.90:
            signal = "DOWN"; conf = self._clamp_confidence(0.62)
            reasoning = f"Price at upper band (%B: {pb:.2f})"
        elif pb < 0.50:
            signal = "UP";   conf = self._clamp_confidence(0.48 + (0.50 - pb) * 0.20)
            reasoning = f"Below mid-band (%B: {pb:.2f})"
        else:
            signal = "DOWN"; conf = self._clamp_confidence(0.48 + (pb - 0.50) * 0.20)
            reasoning = f"Above mid-band (%B: {pb:.2f})"

        if crossover:
            reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder:
            reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)

        # HTF
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * (self.PERIOD + 1):
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= self.PERIOD + 1:
                htf_pb = pct_b(np.array(_closes(htf), dtype=float))
                htf_signal = "UP" if htf_pb < 0.50 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{pb:.3f}",
            "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree,
        }


class MomentumStrategy(BaseStrategy):
    """Rate-of-Change(4) — 4-bar speed reading for 1-min scalping."""
    name = "momentum"
    PERIOD = 4

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        # Use 1m OHLCV closes to match Binance/TradingView
        if ohlcv and len(ohlcv) >= self.PERIOD + 2:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)

        if len(p) < self.PERIOD + 2:
            return self._no_data()

        def roc(arr):
            return float((arr[-1] / arr[-1 - self.PERIOD] - 1) * 100) if len(arr) > self.PERIOD else 0.0

        mom      = roc(p)
        prev_mom = roc(p[:-1])

        crossover  = prev_mom <= 0 < mom
        crossunder = prev_mom >= 0 > mom

        if abs(mom) < 0.005:
            signal = "UP"; conf = 0.45
            reasoning = f"Flat ROC({self.PERIOD}): {mom:+.4f}%"
        else:
            signal = "UP" if mom > 0 else "DOWN"
            conf   = self._clamp_confidence(0.50 + min(abs(mom) * 5, 0.35))
            reasoning = f"ROC({self.PERIOD}): {mom:+.4f}%"

        if crossover:
            reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder:
            reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)

        # HTF
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * (self.PERIOD + 2):
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) > self.PERIOD:
                htf_p   = _closes(htf)
                htf_mom = roc(np.array(htf_p))
                htf_signal = "UP" if htf_mom > 0 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{mom:.4f}",
            "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree,
        }


class StochasticStrategy(BaseStrategy):
    """Fast Stochastic(5,3,3) — %K/%D using actual OHLCV High/Low/Close."""
    name = "stochastic"
    K_PERIOD = 5
    D_PERIOD = 3
    OB, OS   = 80, 20

    @staticmethod
    def _k_series_ohlcv(bars, k_period: int) -> np.ndarray:
        """Standard %K: (Close - LowestLow) / (HighestHigh - LowestLow) * 100."""
        result = []
        for i in range(k_period - 1, len(bars)):
            w  = bars[i - k_period + 1 : i + 1]
            lo = min(float(b[3]) for b in w)   # actual Low
            hi = max(float(b[2]) for b in w)   # actual High
            c  = float(bars[i][4])              # Close
            result.append(float((c - lo) / (hi - lo) * 100) if hi != lo else 50.0)
        return np.array(result)

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        need  = self.K_PERIOD + self.D_PERIOD + 1

        if ohlcv and len(ohlcv) >= need:
            ks = self._k_series_ohlcv(ohlcv, self.K_PERIOD)
        else:
            # Fallback: close-only approximation
            p = np.array(prices, dtype=float)
            if len(p) < need:
                return self._no_data()
            ks_list = []
            for i in range(self.K_PERIOD - 1, len(p)):
                w = p[i - self.K_PERIOD + 1:i + 1]
                lo, hi = np.min(w), np.max(w)
                ks_list.append(float((p[i] - lo) / (hi - lo) * 100) if hi != lo else 50.0)
            ks = np.array(ks_list)

        if len(ks) < self.D_PERIOD + 1:
            return self._no_data()

        k      = ks[-1]
        d      = float(np.mean(ks[-self.D_PERIOD:]))
        prev_k = ks[-2]
        prev_d = float(np.mean(ks[-self.D_PERIOD - 1:-1]))

        crossover  = prev_k <= prev_d and k > d
        crossunder = prev_k >= prev_d and k < d

        diff = k - d
        if k < self.OS:
            signal = "UP";   conf = self._clamp_confidence(0.62)
            reasoning = f"Stoch oversold K={k:.1f} D={d:.1f}"
        elif k > self.OB:
            signal = "DOWN"; conf = self._clamp_confidence(0.62)
            reasoning = f"Stoch overbought K={k:.1f} D={d:.1f}"
        elif diff > 0:
            signal = "UP";   conf = self._clamp_confidence(0.50 + min(abs(diff) / 100, 0.20))
            reasoning = f"K above D: K={k:.1f} D={d:.1f}"
        else:
            signal = "DOWN"; conf = self._clamp_confidence(0.50 + min(abs(diff) / 100, 0.20))
            reasoning = f"K below D: K={k:.1f} D={d:.1f}"

        if crossover:
            reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.07)
        elif crossunder:
            reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.07)

        # HTF — use real H/L/C on aggregated bars
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * need:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= need:
                htf_ks = self._k_series_ohlcv(htf, self.K_PERIOD)
                if len(htf_ks) >= self.D_PERIOD:
                    htf_k = htf_ks[-1]
                    htf_signal = "UP" if htf_k < 50 else "DOWN"
                    mtf_agree  = signal == htf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{k:.1f}",
            "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree,
        }


class EMACrossStrategy(BaseStrategy):
    """
    Dual EMA cross — both on 1-minute closes.
    Fast  : EMA5 / EMA13  — captures short-term momentum (standard 1m scalping setup)
    Slow  : EMA21 / EMA55 — captures mid-term trend on the same 1m TF; avoids HTF aggregation artefacts
    Both are presented as separate cards in the UI. If they agree the signal is stronger.
    """
    name = "ema_cross"
    FAST_SHORT, FAST_LONG = 5, 13   # fast crossover pair
    SLOW_SHORT, SLOW_LONG = 21, 55  # slow trend-filter pair (same 1m TF, longer window)

    def predict(self, prices: List[float], **kwargs) -> Dict:
        p = np.array(prices, dtype=float)

        need = self.SLOW_LONG + 2
        if len(p) < need:
            return self._no_data()

        # ── Fast pair: EMA5 / EMA13 ───────────────────────────────────────────
        f_fast = _ema_series(p, self.FAST_SHORT)
        f_slow = _ema_series(p, self.FAST_LONG)
        f_diff      = float(f_fast[-1] - f_slow[-1])
        f_prev_diff = float(f_fast[-2] - f_slow[-2])
        crossover  = f_prev_diff <= 0 < f_diff
        crossunder = f_prev_diff >= 0 > f_diff

        fast_signal = "UP" if f_diff > 0 else "DOWN"
        fast_conf   = self._clamp_confidence(0.52 + min(abs(f_diff) / 100, 0.30))
        reasoning   = (f"EMA{self.FAST_SHORT} {'above' if f_diff > 0 else 'below'} "
                       f"EMA{self.FAST_LONG} by {f_diff:.2f}")
        if crossover:
            reasoning += " [CROSS↑]"; fast_conf = self._clamp_confidence(fast_conf + 0.08)
        elif crossunder:
            reasoning += " [CROSS↓]"; fast_conf = self._clamp_confidence(fast_conf + 0.08)

        # ── Slow pair: EMA21 / EMA55 — same 1m TF ────────────────────────────
        s_fast = _ema_series(p, self.SLOW_SHORT)
        s_slow = _ema_series(p, self.SLOW_LONG)
        s_diff      = float(s_fast[-1] - s_slow[-1])
        s_prev_diff = float(s_fast[-2] - s_slow[-2])
        slow_crossover  = s_prev_diff <= 0 < s_diff
        slow_crossunder = s_prev_diff >= 0 > s_diff

        slow_signal = "UP" if s_diff > 0 else "DOWN"
        slow_conf   = self._clamp_confidence(0.54 + min(abs(s_diff) / 100, 0.28))
        if slow_crossover:
            slow_conf = self._clamp_confidence(slow_conf + 0.09)
        elif slow_crossunder:
            slow_conf = self._clamp_confidence(slow_conf + 0.09)

        # Agreement badge
        agree = fast_signal == slow_signal
        if agree:
            fast_conf = self._clamp_confidence(fast_conf + 0.04)

        # Primary output = fast signal (first card); slow exposed as htf_signal
        return {
            "signal": fast_signal, "confidence": fast_conf,
            "reasoning": reasoning, "value": f"{f_diff:.2f}",
            "htf_signal": slow_signal,
            "crossover": crossover, "crossunder": crossunder,
            "mtf_agree": agree,
            # Extra fields for split UI cards
            "fast_ema_f_val": float(f_fast[-1]), "fast_ema_s_val": float(f_slow[-1]),
            "slow_ema_f_val": float(s_fast[-1]), "slow_ema_s_val": float(s_slow[-1]),
            "slow_diff": s_diff, "slow_crossover": slow_crossover, "slow_crossunder": slow_crossunder,
            "slow_confidence": slow_conf,
        }


class PriceActionStrategy(BaseStrategy):
    """Price Action — V-bottom / inverted-V reversal over 4-bar windows."""
    name = "price_action"
    WINDOW = 4

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        p = np.array(prices, dtype=float)

        if len(p) < self.WINDOW * 2:
            return self._no_data()

        recent_trend = float(p[-1] - p[-self.WINDOW])
        prev_trend   = float(p[-self.WINDOW] - p[-self.WINDOW * 2])

        crossover  = recent_trend > 0 > prev_trend   # V-bottom
        crossunder = recent_trend < 0 < prev_trend   # Inverted-V

        if crossover:
            signal = "UP";   conf = self._clamp_confidence(0.62)
            reasoning = f"V-bottom reversal [{recent_trend:.2f}]"
        elif crossunder:
            signal = "DOWN"; conf = self._clamp_confidence(0.62)
            reasoning = f"Inv-V reversal [{recent_trend:.2f}]"
        else:
            signal = "UP" if recent_trend > 0 else "DOWN"
            conf   = self._clamp_confidence(0.50)
            reasoning = f"{'Bullish' if recent_trend > 0 else 'Bearish'} continuation [{recent_trend:.2f}]"

        # HTF
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * self.WINDOW * 2:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= self.WINDOW * 2:
                hc = _closes(htf)
                htf_trend = hc[-1] - hc[-self.WINDOW]
                htf_signal = "UP" if htf_trend > 0 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{recent_trend:.2f}",
            "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree,
        }


class MFIStrategy(BaseStrategy):
    """MFI(4) — volume-weighted RSI with OB/OS at 80/20; period 4 for fast scalping."""
    name = "mfi"
    PERIOD = 4
    OB, OS = 80, 20

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])

        if not ohlcv or len(ohlcv) < self.PERIOD + 2:
            return self._no_data("No OHLCV data")

        mfi      = self._calc_mfi(ohlcv,      self.PERIOD)
        prev_mfi = self._calc_mfi(ohlcv[:-1], self.PERIOD) if len(ohlcv) > self.PERIOD + 2 else mfi

        crossover  = prev_mfi <= self.OS < mfi
        crossunder = prev_mfi >= self.OB > mfi

        if mfi < self.OS:
            signal = "UP";   conf = self._clamp_confidence(0.62 + (self.OS - mfi) / 100)
            reasoning = f"MFI oversold at {mfi:.1f}"
        elif mfi > self.OB:
            signal = "DOWN"; conf = self._clamp_confidence(0.62 + (mfi - self.OB) / 100)
            reasoning = f"MFI overbought at {mfi:.1f}"
        elif mfi < 45:
            signal = "UP";   conf = self._clamp_confidence(0.50 + (45 - mfi) / 200)
            reasoning = f"MFI leaning OS at {mfi:.1f}"
        elif mfi > 55:
            signal = "DOWN"; conf = self._clamp_confidence(0.50 + (mfi - 55) / 200)
            reasoning = f"MFI leaning OB at {mfi:.1f}"
        else:
            signal = "UP";   conf = 0.45
            reasoning = f"MFI neutral at {mfi:.1f}"

        if crossover:
            reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder:
            reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)

        # HTF
        htf_signal, mtf_agree = "N/A", None
        if len(ohlcv) >= 5 * (self.PERIOD + 2):
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= self.PERIOD + 1:
                htf_mfi = self._calc_mfi(htf, self.PERIOD)
                htf_signal = "UP" if htf_mfi < 50 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{mfi:.1f}",
            "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree,
        }

    @staticmethod
    def _calc_mfi(ohlcv, period: int) -> float:
        """
        Standard MFI using a simple rolling window — matches TradingView/Binance.

        Formula:
            TP  = (High + Low + Close) / 3
            RMF = TP * Volume
            Classify each bar: TP > prev_TP → positive MF; TP < prev_TP → negative MF
            PMF = sum of positive RMF over last `period` classified bars
            NMF = sum of negative RMF over last `period` classified bars
            MFI = 100 * PMF / (PMF + NMF)

        We need `period + 1` bars (to get `period` TP differences).
        """
        if len(ohlcv) < period + 1:
            return 50.0
        # Use only the most recent period+1 bars — pure rolling window, no smoothing
        bars = ohlcv[-(period + 1):]
        tp  = [(float(k[2]) + float(k[3]) + float(k[4])) / 3 for k in bars]
        vol = [float(k[5]) for k in bars]
        rmf = [tp[i] * vol[i] for i in range(len(tp))]
        pos = sum(rmf[i] for i in range(1, period + 1) if tp[i] > tp[i - 1])
        neg = sum(rmf[i] for i in range(1, period + 1) if tp[i] < tp[i - 1])
        total = pos + neg
        if total == 0:
            return 50.0
        return 100.0 * pos / total


class VWAPStrategy(BaseStrategy):
    """
    Anchored VWAP with 1/2/3σ standard-deviation bands.

    Anchor: the highest-volume bar in the last 50 1m bars — the price level where
    the most participation occurred, i.e. where fair value was most contested.

    VWAP + bands are computed from the anchor forward.  Position relative to those
    bands determines directional bias and confidence:

      Price > VWAP      → long bias  (buyers dominating since the anchor)
      Price < VWAP      → short bias (sellers dominating since the anchor)

      Confidence tiers:
        Within ±1σ  → 0.50–0.56  (neutral zone, slight lean)
        ±1σ band    → 0.57       (trend has momentum)
        ±2σ band    → 0.68       (extended move, high conviction)
        ±3σ band    → 0.78       (extreme extension)

      Band crossings (price flipping through VWAP) add +0.05.
    """
    name     = "vwap"
    LOOKBACK = 50    # bars to search for volume anchor
    MIN_BARS = 5     # minimum bars after anchor for a valid VWAP

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < self.MIN_BARS:
            return self._no_data("No OHLCV data")

        bars = ohlcv[-self.LOOKBACK:]

        # ── Find anchor: bar with highest volume in lookback ──────────────────
        vols      = [float(k[5]) for k in bars]
        anchor_i  = max(range(len(vols)), key=lambda i: vols[i])
        anchor_bars = bars[anchor_i:]

        # If the anchor is the very last bar use the full lookback instead
        if len(anchor_bars) < self.MIN_BARS:
            anchor_bars = bars

        # ── Anchored VWAP ─────────────────────────────────────────────────────
        tps  = np.array([(float(k[2]) + float(k[3]) + float(k[4])) / 3.0
                         for k in anchor_bars], dtype=float)
        vols_arr = np.array([float(k[5]) for k in anchor_bars], dtype=float)

        total_vol = vols_arr.sum()
        if total_vol == 0:
            return self._no_data("Zero volume")

        vwap = float(np.dot(tps, vols_arr) / total_vol)

        # ── Volume-weighted standard deviation ────────────────────────────────
        vw_var = float(np.dot(vols_arr, (tps - vwap) ** 2) / total_vol)
        sigma  = float(np.sqrt(max(vw_var, 0.0)))

        cur  = prices[-1]
        prev = prices[-2] if len(prices) > 1 else cur

        # Z-score relative to VWAP (in σ units; 0 if σ is negligible)
        z = (cur - vwap) / sigma if sigma > 1e-8 else (cur - vwap) / (vwap * 0.001 + 1e-8)

        # ── Signal and confidence ─────────────────────────────────────────────
        above = cur > vwap
        az    = abs(z)

        if az >= 3.0:
            conf       = 0.78
            band_label = "3σ ext"
        elif az >= 2.0:
            conf       = 0.68
            band_label = "2σ band"
        elif az >= 1.0:
            conf       = 0.57
            band_label = "1σ band"
        else:
            conf       = 0.50 + az * 0.06   # 0.50 at centre → 0.56 at 1σ boundary
            band_label = "VWAP zone"

        signal = "UP" if above else "DOWN"

        # ── VWAP crossings ────────────────────────────────────────────────────
        crossover  = prev <= vwap < cur
        crossunder = prev >= vwap > cur
        if crossover or crossunder:
            conf += 0.05

        anchor_bars_ago = len(bars) - anchor_i
        reasoning = (
            f"Anchored VWAP ${vwap:.2f} | σ=${sigma:.2f} | "
            f"{'above' if above else 'below'} {band_label} (z={z:+.2f}) | "
            f"anchor {anchor_bars_ago}b ago (peak vol)"
        )
        if crossover:
            reasoning += " [CROSS↑]"
        elif crossunder:
            reasoning += " [CROSS↓]"

        return {
            "signal":     signal,
            "confidence": self._clamp_confidence(conf),
            "reasoning":  reasoning,
            "value":      f"${vwap:.2f}",
            "htf_signal": "N/A",
            "crossover":  crossover,
            "crossunder": crossunder,
            "mtf_agree":  None,
        }


class OBVStrategy(BaseStrategy):
    """OBV — 4-bar vs 8-bar slope; crossover on slope sign change."""
    name = "obv"
    SHORT_WIN = 4
    LONG_WIN  = 8

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])

        if not ohlcv or len(ohlcv) < self.LONG_WIN * 2 + 2:
            return self._no_data("No OHLCV data")

        obv_r = self._calc_obv(ohlcv[-self.SHORT_WIN:])
        obv_p = self._calc_obv(ohlcv[-self.LONG_WIN:-self.SHORT_WIN])
        slope = obv_r - obv_p

        obv_r_prev = self._calc_obv(ohlcv[-self.SHORT_WIN - 1:-1])
        obv_p_prev = self._calc_obv(ohlcv[-self.LONG_WIN - 1:-self.SHORT_WIN - 1])
        prev_slope = obv_r_prev - obv_p_prev

        crossover  = prev_slope <= 0 < slope
        crossunder = prev_slope >= 0 > slope

        if slope > 0:
            signal = "UP";   conf = self._clamp_confidence(0.52 + min(abs(slope) / 500, 0.28))
            reasoning = f"OBV accumulation +{slope:.0f}"
        else:
            signal = "DOWN"; conf = self._clamp_confidence(0.52 + min(abs(slope) / 500, 0.28))
            reasoning = f"OBV distribution {slope:.0f}"

        if crossover:
            reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder:
            reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)

        # No MTF: OBV is cumulative from an arbitrary start; 5m-aggregated slope = same data, just coarser
        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{obv_r:.1f}",
            "htf_signal": "N/A", "crossover": crossover, "crossunder": crossunder, "mtf_agree": None,
        }

    @staticmethod
    def _calc_obv(ohlcv) -> float:
        obv = 0.0
        for i in range(1, len(ohlcv)):
            c, cp = float(ohlcv[i][4]), float(ohlcv[i - 1][4])
            v = float(ohlcv[i][5])
            obv += v if c > cp else (-v if c < cp else 0.0)
        return obv


class PolymarketStrategy(BaseStrategy):
    """Polymarket crowd wisdom — implied probability as a signal."""
    name = "polymarket"

    def predict(self, prices: List[float], **kwargs) -> Dict:
        prob = kwargs.get("polymarket_prob")
        if prob is None:
            return self._no_data("No market data")
        prob = float(prob)
        if prob > 0.58:
            return {"signal": "UP",   "confidence": self._clamp_confidence(0.50 + (prob - 0.50)),
                    "reasoning": f"Crowd {prob*100:.1f}% UP (strong)", "value": f"{prob*100:.1f}%",
                    "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}
        elif prob < 0.42:
            return {"signal": "DOWN", "confidence": self._clamp_confidence(0.50 + (0.50 - prob)),
                    "reasoning": f"Crowd {(1-prob)*100:.1f}% DOWN (strong)", "value": f"{prob*100:.1f}%",
                    "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}
        elif prob >= 0.50:
            return {"signal": "UP",   "confidence": self._clamp_confidence(0.47 + (prob - 0.50)),
                    "reasoning": f"Crowd leans UP at {prob*100:.1f}%", "value": f"{prob*100:.1f}%",
                    "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}
        else:
            return {"signal": "DOWN", "confidence": self._clamp_confidence(0.47 + (0.50 - prob)),
                    "reasoning": f"Crowd leans DOWN at {prob*100:.1f}%", "value": f"{prob*100:.1f}%",
                    "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}


class SupertrendStrategy(BaseStrategy):
    """
    Supertrend — ATR(10, ×3) dynamic support/resistance band.
    Price above band = bullish; price below = bearish.
    Band flip generates the strongest signal (crossover/crossunder).
    Requires Binance OHLCV.
    """
    name   = "supertrend"
    PERIOD = 10
    MULT   = 3.0

    @staticmethod
    def _rma(arr: np.ndarray, period: int) -> np.ndarray:
        """Wilder's Running Moving Average."""
        result = np.zeros(len(arr))
        result[period - 1] = arr[:period].mean()
        for i in range(period, len(arr)):
            result[i] = (result[i - 1] * (period - 1) + arr[i]) / period
        return result

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < self.PERIOD + 5:
            return self._no_data("Need OHLCV (Binance)")

        highs  = np.array([float(c[2]) for c in ohlcv], dtype=float)
        lows   = np.array([float(c[3]) for c in ohlcv], dtype=float)
        closes = np.array([float(c[4]) for c in ohlcv], dtype=float)
        n      = len(closes)

        tr    = np.zeros(n)
        tr[0] = highs[0] - lows[0]
        for i in range(1, n):
            tr[i] = max(highs[i] - lows[i],
                        abs(highs[i] - closes[i - 1]),
                        abs(lows[i]  - closes[i - 1]))

        atr = self._rma(tr, self.PERIOD)
        hl2 = (highs + lows) / 2.0
        bu  = hl2 + self.MULT * atr
        bl  = hl2 - self.MULT * atr

        fu        = bu.copy()
        fl        = bl.copy()
        direction = np.ones(n, dtype=int)

        for i in range(1, n):
            fu[i] = bu[i] if (bu[i] < fu[i - 1] or closes[i - 1] > fu[i - 1]) else fu[i - 1]
            fl[i] = bl[i] if (bl[i] > fl[i - 1] or closes[i - 1] < fl[i - 1]) else fl[i - 1]
            if   direction[i - 1] == -1 and closes[i] > fu[i - 1]:
                direction[i] =  1
            elif direction[i - 1] ==  1 and closes[i] < fl[i - 1]:
                direction[i] = -1
            else:
                direction[i] = direction[i - 1]

        cur_dir  = int(direction[-1])
        prev_dir = int(direction[-2])
        crossover  = prev_dir == -1 and cur_dir ==  1
        crossunder = prev_dir ==  1 and cur_dir == -1

        signal   = "UP" if cur_dir == 1 else "DOWN"
        cur_atr  = float(atr[-1])
        band     = float(fl[-1]) if cur_dir == 1 else float(fu[-1])
        dist_atr = abs(closes[-1] - band) / cur_atr if cur_atr > 0 else 0.0

        conf = self._clamp_confidence(0.55 + min(dist_atr * 0.06, 0.25))
        if crossover or crossunder:
            conf = self._clamp_confidence(conf + 0.08)
            reasoning = f"ST flipped {'UP' if crossover else 'DOWN'} | ATR={cur_atr:.1f}"
        else:
            reasoning = f"ST {'bull' if cur_dir == 1 else 'bear'} | dist={dist_atr:.2f}\u00d7ATR"

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning,
            "value": f"{cur_atr:.1f}",
            "htf_signal": "N/A", "crossover": crossover,
            "crossunder": crossunder, "mtf_agree": None,
        }


class ADXStrategy(BaseStrategy):
    """
    ADX/DMI — Average Directional Index with +DI/−DI lines.
    ADX measures trend STRENGTH; +DI vs −DI gives direction.
    Unique insight: confirms whether a move has momentum behind it.
    Requires Binance OHLCV.
    """
    name   = "adx"
    PERIOD = 14

    @staticmethod
    def _rma(arr: np.ndarray, period: int) -> np.ndarray:
        result = np.zeros(len(arr))
        result[period - 1] = arr[:period].mean()
        for i in range(period, len(arr)):
            result[i] = (result[i - 1] * (period - 1) + arr[i]) / period
        return result

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        need  = self.PERIOD * 3
        if not ohlcv or len(ohlcv) < need:
            return self._no_data(f"Need {need}+ bars")

        highs  = np.array([float(c[2]) for c in ohlcv], dtype=float)
        lows   = np.array([float(c[3]) for c in ohlcv], dtype=float)
        closes = np.array([float(c[4]) for c in ohlcv], dtype=float)
        n      = len(closes)

        tr  = np.zeros(n)
        dmp = np.zeros(n)
        dmm = np.zeros(n)
        for i in range(1, n):
            up    = highs[i] - highs[i - 1]
            down  = lows[i - 1] - lows[i]
            tr[i]  = max(highs[i] - lows[i],
                         abs(highs[i] - closes[i - 1]),
                         abs(lows[i]  - closes[i - 1]))
            dmp[i] = up   if (up > down   and up   > 0) else 0.0
            dmm[i] = down if (down > up   and down > 0) else 0.0

        tr_r  = self._rma(tr[1:],  self.PERIOD)
        dmp_r = self._rma(dmp[1:], self.PERIOD)
        dmm_r = self._rma(dmm[1:], self.PERIOD)

        with np.errstate(divide="ignore", invalid="ignore"):
            di_plus  = 100.0 * np.where(tr_r > 0, dmp_r / tr_r, 0.0)
            di_minus = 100.0 * np.where(tr_r > 0, dmm_r / tr_r, 0.0)
            di_sum   = di_plus + di_minus
            dx       = 100.0 * np.where(di_sum > 0,
                                         np.abs(di_plus - di_minus) / di_sum, 0.0)

        adx_arr = self._rma(dx, self.PERIOD)

        adx   = float(adx_arr[-1])
        dip   = float(di_plus[-1])
        dim   = float(di_minus[-1])
        p_dip = float(di_plus[-2])
        p_dim = float(di_minus[-2])

        crossover  = p_dip <= p_dim and dip > dim
        crossunder = p_dip >= p_dim and dip < dim

        signal = "UP" if dip > dim else "DOWN"

        if   adx >= 30: conf = self._clamp_confidence(0.72)
        elif adx >= 20: conf = self._clamp_confidence(0.60)
        elif adx >= 12: conf = self._clamp_confidence(0.52)
        else:           conf = self._clamp_confidence(0.44)

        if crossover or crossunder:
            conf = self._clamp_confidence(conf + 0.08)

        trend_str = "strong" if adx >= 25 else ("moderate" if adx >= 20 else "weak")
        reasoning = f"ADX={adx:.1f} ({trend_str}) +DI={dip:.1f} \u2212DI={dim:.1f}"
        if crossover:    reasoning += " [+DI\u2191]"
        elif crossunder: reasoning += " [\u2212DI\u2191]"

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning,
            "value": f"{adx:.1f}",
            "htf_signal": "N/A", "crossover": crossover,
            "crossunder": crossunder, "mtf_agree": None,
        }


class HTFEMAStrategy(BaseStrategy):
    """
    Higher-Timeframe EMA cross — EMA(8, 21) on 15-min bars aggregated
    from 1-min Binance candles.  500 1-min bars → ~33 15-min bars.
    mtf_agree shows whether 1-min EMA aligns with 15-min trend.
    """
    name    = "htf_ema"
    AGG     = 15
    FAST    = 8
    SLOW    = 21

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv   = kwargs.get("ohlcv", [])
        need_1m = self.AGG * (self.SLOW + 3)

        if not ohlcv or len(ohlcv) < need_1m:
            return self._no_data(f"Need {need_1m}+ 1-min bars for 15-min EMA")

        htf = _aggregate_ohlcv(ohlcv, self.AGG)
        if len(htf) < self.SLOW + 3:
            return self._no_data(f"Only {len(htf)} 15-min bars, need {self.SLOW + 3}")

        hp        = np.array(_closes(htf), dtype=float)
        ema_f     = _ema_series(hp, self.FAST)
        ema_s     = _ema_series(hp, self.SLOW)
        diff      = float(ema_f[-1] - ema_s[-1])
        prev_diff = float(ema_f[-2] - ema_s[-2])

        crossover  = prev_diff <= 0 < diff
        crossunder = prev_diff >= 0 > diff

        signal = "UP" if diff > 0 else "DOWN"
        rel    = abs(diff) / float(hp[-1])
        conf   = self._clamp_confidence(0.54 + min(rel * 300, 0.27))

        if crossover:
            conf = self._clamp_confidence(conf + 0.10)
            reasoning = f"15m EMA{self.FAST} crossed ABOVE EMA{self.SLOW} [{diff:+.2f}]"
        elif crossunder:
            conf = self._clamp_confidence(conf + 0.10)
            reasoning = f"15m EMA{self.FAST} crossed BELOW EMA{self.SLOW} [{diff:+.2f}]"
        else:
            side = "above" if diff > 0 else "below"
            reasoning = f"15m EMA{self.FAST} {side} EMA{self.SLOW} by {abs(diff):.2f}"

        # LTF confluence: same EMA periods on 1-min
        ltf_signal, mtf_agree = "N/A", None
        p = np.array(prices, dtype=float)
        if len(p) >= self.SLOW + 2:
            lf         = _ema_series(p, self.FAST)
            ls         = _ema_series(p, self.SLOW)
            ltf_signal = "UP" if lf[-1] > ls[-1] else "DOWN"
            mtf_agree  = signal == ltf_signal

        return {
            "signal": signal, "confidence": conf, "reasoning": reasoning,
            "value": f"{diff:+.2f}",
            "htf_signal": ltf_signal,
            "crossover": crossover, "crossunder": crossunder,
            "mtf_agree": mtf_agree,
        }


# =============================================================================
# Strategy Registry
# =============================================================================

class WilliamsAlligatorStrategy(BaseStrategy):
    """Williams Alligator — Jaw(13)/Teeth(8)/Lips(5) Wilder's SMMA. Sleeping = ranging."""
    name = "alligator"

    @staticmethod
    def _smma(arr: np.ndarray, period: int) -> float:
        if len(arr) < period:
            return float(arr[-1])
        k = 1.0 / period
        val = float(arr[0])
        for v in arr[1:]:
            val = float(v) * k + val * (1.0 - k)
        return val

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        p = np.array(prices, dtype=float)
        if len(p) < 15:
            return self._no_data()
        jaw   = self._smma(p, 13); teeth = self._smma(p, 8); lips = self._smma(p, 5)
        bull  = lips > teeth > jaw; bear = lips < teeth < jaw
        spread_pct = abs(lips - jaw) / jaw * 100 if jaw > 0 else 0.0
        prev_p     = p[:-1]
        prev_lips  = self._smma(prev_p, 5)  if len(prev_p) >= 5  else lips
        prev_jaw   = self._smma(prev_p, 13) if len(prev_p) >= 13 else jaw
        crossover  = prev_lips <= prev_jaw and lips > jaw
        crossunder = prev_lips >= prev_jaw and lips < jaw
        if bull:
            signal = "UP";   conf = self._clamp_confidence(0.56 + min(spread_pct * 4, 0.24))
            reasoning = f"Alligator bullish (spread {spread_pct:.3f}%)"
        elif bear:
            signal = "DOWN"; conf = self._clamp_confidence(0.56 + min(spread_pct * 4, 0.24))
            reasoning = f"Alligator bearish (spread {spread_pct:.3f}%)"
        else:
            signal = "UP" if lips > jaw else "DOWN"; conf = 0.45
            reasoning = "Alligator sleeping (ranging)"
        if crossover:   reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.06)
        elif crossunder: reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.06)
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 30:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= 13:
                hp = np.array(_closes(htf), dtype=float)
                htf_j = self._smma(hp, 13); htf_t = self._smma(hp, 8); htf_l = self._smma(hp, 5)
                htf_signal = "UP" if htf_l > htf_t > htf_j else ("DOWN" if htf_l < htf_t < htf_j else ("UP" if htf_l > htf_j else "DOWN"))
                mtf_agree = signal == htf_signal
        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{spread_pct:.3f}%",
                "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree}


class AccDistStrategy(BaseStrategy):
    """Accumulation/Distribution Line — 4-bar CLV×Volume slope."""
    name = "acc_dist"
    PERIOD = 4

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        need  = self.PERIOD * 2 + 2
        if not ohlcv or len(ohlcv) < need:
            return self._no_data("No OHLCV data")
        def _ad(bars):
            ad, vals = 0.0, []
            for k in bars:
                h, l, c, v = float(k[2]), float(k[3]), float(k[4]), float(k[5])
                ad += ((c - l) - (h - c)) / (h - l) * v if h != l else 0.0
                vals.append(ad)
            return vals
        ad = _ad(ohlcv)
        slope      = ad[-1] - ad[-(self.PERIOD + 1)]
        prev_slope = ad[-(self.PERIOD + 1)] - ad[-(self.PERIOD * 2 + 1)]
        crossover  = prev_slope <= 0 < slope; crossunder = prev_slope >= 0 > slope
        ref = max(abs(ad[-1]), 1.0)
        if slope > 0:
            signal = "UP";   conf = self._clamp_confidence(0.52 + min(abs(slope) / ref * 0.5, 0.28))
            reasoning = f"A/D accumulation (+{slope:.0f})"
        else:
            signal = "DOWN"; conf = self._clamp_confidence(0.52 + min(abs(slope) / ref * 0.5, 0.28))
            reasoning = f"A/D distribution ({slope:.0f})"
        if crossover:   reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder: reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)
        # No MTF: A/D is cumulative; 5m-aggregated version uses same raw volume, no extra insight
        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{ad[-1]:.0f}",
                "htf_signal": "N/A", "crossover": crossover, "crossunder": crossunder, "mtf_agree": None}


class DowTheoryStrategy(BaseStrategy):
    """Dow Theory — trend confirmed via HH+HL (uptrend) or LH+LL (downtrend)."""
    name = "dow_theory"
    SWING_N = 4

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        p     = np.array(prices, dtype=float)
        if len(p) < self.SWING_N * 2 + 2:
            return self._no_data()
        def find_swings(arr):
            n = self.SWING_N; sh: List = []; sl: List = []
            for i in range(n, len(arr) - n):
                w = arr[i - n: i + n + 1]
                if arr[i] >= w.max(): sh.append((i, float(arr[i])))
                if arr[i] <= w.min(): sl.append((i, float(arr[i])))
            return sh, sl
        sh, sl = find_swings(p)
        if len(sh) < 2 or len(sl) < 2:
            return self._no_data("Insufficient swings")
        h1, h2 = sh[-2][1], sh[-1][1]; l1, l2 = sl[-2][1], sl[-1][1]
        hh = h2 > h1; hl = l2 > l1; lh = h2 < h1; ll = l2 < l1
        if hh and hl:       signal, conf, structure = "UP",   self._clamp_confidence(0.65), "UPTREND"
        elif lh and ll:     signal, conf, structure = "DOWN", self._clamp_confidence(0.65), "DOWNTREND"
        elif hh:            signal, conf, structure = "UP",   self._clamp_confidence(0.54), "HH+LL"
        elif ll:            signal, conf, structure = "DOWN", self._clamp_confidence(0.54), "LH+LL"
        else:               signal, conf, structure = ("UP" if p[-1] > float(np.mean(p[-10:])) else "DOWN"), 0.45, "RANGING"
        reasoning = f"Dow: {structure} H:{h1:.0f}→{h2:.0f} L:{l1:.0f}→{l2:.0f}"
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 30:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= self.SWING_N * 2 + 2:
                htf_sh, htf_sl = find_swings(np.array(_closes(htf), dtype=float))
                if len(htf_sh) >= 2 and len(htf_sl) >= 2:
                    htf_hh = htf_sh[-1][1] > htf_sh[-2][1]; htf_hl = htf_sl[-1][1] > htf_sl[-2][1]
                    htf_signal = "UP" if (htf_hh and htf_hl) else ("DOWN" if (not htf_hh and not htf_hl) else signal)
                    mtf_agree = signal == htf_signal
        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": structure[:8],
                "htf_signal": htf_signal, "crossover": hh and hl, "crossunder": lh and ll, "mtf_agree": mtf_agree}


class FibPullbackStrategy(BaseStrategy):
    """Fibonacci Pullback — 38.2/50/61.8% retracements as continuation signals."""
    name = "fib_pullback"
    FIB_LEVELS = [0.236, 0.382, 0.500, 0.618, 0.786]
    TOL = 0.0030; LOOKBACK = 30

    def predict(self, prices: List[float], **kwargs) -> Dict:
        p = np.array(prices, dtype=float)
        if len(p) < self.LOOKBACK:
            return self._no_data()
        window  = p[-self.LOOKBACK:]; sw_high = float(window.max()); sw_low = float(window.min())
        rng     = sw_high - sw_low;   current = float(p[-1])
        uptrend = int(window.argmax()) > int(window.argmin())
        if rng < current * 0.0005:
            return {"signal": "UP", "confidence": 0.45, "reasoning": "Range too small for Fib",
                    "value": "—", "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}
        pb_pct = (sw_high - current) / rng if uptrend else (current - sw_low) / rng
        at_fib = next((lvl for lvl in self.FIB_LEVELS if abs(pb_pct - lvl) < self.TOL), None)
        label  = f"{pb_pct * 100:.1f}%"
        if at_fib and at_fib >= 0.382:
            signal = "UP" if uptrend else "DOWN"
            conf   = self._clamp_confidence(0.62 + at_fib * 0.10)
            reasoning = f"{'Bounce' if uptrend else 'Reject'} at {at_fib*100:.1f}% Fib ({label})"
        elif pb_pct > 0.786:
            signal = "DOWN" if uptrend else "UP"; conf = self._clamp_confidence(0.58)
            reasoning = f"{'Failed bounce' if uptrend else 'Failed breakdown'} ({label})"
        else:
            signal = "UP" if uptrend else "DOWN"; conf = 0.47
            reasoning = f"{'Uptrend' if uptrend else 'Downtrend'} pullback {label}"
        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": label,
                "htf_signal": "N/A", "crossover": bool(at_fib and uptrend),
                "crossunder": bool(at_fib and not uptrend), "mtf_agree": None}


class HarmonicPatternStrategy(BaseStrategy):
    """Harmonic Patterns — Gartley/Bat/Crab/Butterfly via XABCD Fibonacci ratios."""
    name = "harmonic"
    _PATTERNS = {
        "GARTLEY":   {"ab_xa": (0.55, 0.68), "bc_ab": (0.35, 0.90)},
        "BAT":       {"ab_xa": (0.35, 0.52), "bc_ab": (0.35, 0.90)},
        "CRAB":      {"ab_xa": (0.35, 0.65), "bc_ab": (0.35, 0.90)},
        "BUTTERFLY": {"ab_xa": (0.68, 0.82), "bc_ab": (0.35, 0.90)},
    }

    def predict(self, prices: List[float], **kwargs) -> Dict:
        p = np.array(prices, dtype=float)
        if len(p) < 30:
            return self._no_data()
        window = p[-min(50, len(p)):]; n_sw = 3; pivots: List = []
        for i in range(n_sw, len(window) - n_sw):
            w = window[i - n_sw: i + n_sw + 1]
            if window[i] >= w.max():  pivots.append(("H", float(window[i])))
            elif window[i] <= w.min(): pivots.append(("L", float(window[i])))
        filtered: List = []
        for pt in pivots:
            if not filtered or filtered[-1][0] != pt[0]:
                filtered.append(pt)
        if len(filtered) < 4:
            return {"signal": "UP", "confidence": 0.44, "reasoning": "Not enough pivots for harmonic",
                    "value": "—", "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}
        _, X_p = filtered[-4]; _, A_p = filtered[-3]; _, B_p = filtered[-2]; _, C_p = filtered[-1]
        XA = abs(A_p - X_p); AB = abs(B_p - A_p); BC = abs(C_p - B_p)
        if XA < 1e-6 or AB < 1e-6:
            return {"signal": "UP", "confidence": 0.44, "reasoning": "Zero-range swing",
                    "value": "—", "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}
        ab_xa = AB / XA; bc_ab = BC / AB
        detected = next((name for name, r in self._PATTERNS.items()
                         if r["ab_xa"][0] <= ab_xa <= r["ab_xa"][1] and r["bc_ab"][0] <= bc_ab <= r["bc_ab"][1]), None)
        bullish = A_p < X_p
        if detected:
            signal = "UP" if bullish else "DOWN"; conf = self._clamp_confidence(0.62)
            reasoning = f"{detected} {'bull' if bullish else 'bear'} (AB/XA={ab_xa:.3f} BC/AB={bc_ab:.3f})"
        else:
            signal = "UP" if p[-1] > float(np.mean(p[-10:])) else "DOWN"; conf = 0.44
            reasoning = f"No harmonic (AB/XA={ab_xa:.3f} BC/AB={bc_ab:.3f})"
        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": detected or "—",
                "htf_signal": "N/A", "crossover": bool(detected and bullish),
                "crossunder": bool(detected and not bullish), "mtf_agree": None}


# =============================================================================
# Strategy Registry
# =============================================================================

ALL_STRATEGIES = [
    # Core oscillators
    RSIStrategy(),
    MACDStrategy(),
    StochasticStrategy(),
    # Trend & structure
    EMACrossStrategy(),
    SupertrendStrategy(),
    ADXStrategy(),
    # Pattern-based (DeepSeek specialists override these at bar open)
    WilliamsAlligatorStrategy(),
    AccDistStrategy(),
    DowTheoryStrategy(),
    FibPullbackStrategy(),
    HarmonicPatternStrategy(),
    # Volume / VWAP
    VWAPStrategy(),
    # Market / crowd
    PolymarketStrategy(),
    # Removed (duplicates):
    #   BollingerStrategy   — redundant with RSI/Stoch for OB/OS
    #   MomentumStrategy    — ROC subsumed by RSI + MACD
    #   PriceActionStrategy — V-bottom subsumed by Dow Theory
    #   MFIStrategy         — volume-weighted RSI; OBV/A/D handle volume
    #   (VWAPStrategy now active as Anchored VWAP)
    #   OBVStrategy         — redundant with Acc/Dist specialist
    #   HTFEMAStrategy      — duplicate of EMACross slow pair (EMA21/55)
]


def get_all_predictions(prices: List[float], **kwargs) -> Dict[str, Dict]:
    """Run all strategies and return their predictions."""
    results = {}
    for strategy in ALL_STRATEGIES:
        try:
            results[strategy.name] = strategy.predict(prices, **kwargs)
        except Exception as e:
            results[strategy.name] = {
                "signal": "UP", "confidence": 0.45,
                "reasoning": f"Error: {e}", "value": "ERR",
                "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
            }
    return results
