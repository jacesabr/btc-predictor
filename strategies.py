"""
Trading Strategies, Ensemble, and EV Calculator
================================================
Active rule-based strategies for BTC 5-minute prediction:
  RSI, MACD, Stochastic, EMA Cross, VWAP,
  Supertrend, ADX, Alligator, Acc/Dist, Dow Theory, Fib Pullback, Harmonic

Plus LinearRegressionChannel (key: ml_logistic), EnsemblePredictor, and EV tools.

Dead strategies removed: Bollinger, Momentum, PriceAction, MFI, OBV, HTF EMA.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional
import numpy as np


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _ema_series(prices: np.ndarray, period: int) -> np.ndarray:
    k = 2.0 / (period + 1)
    out = np.empty(len(prices))
    out[0] = prices[0]
    for i in range(1, len(prices)):
        out[i] = prices[i] * k + out[i-1] * (1 - k)
    return out


def _rsi_val(prices: np.ndarray, period: int) -> float:
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices.astype(float))
    gains  = np.maximum(deltas, 0.0)
    losses = np.maximum(-deltas, 0.0)
    avg_g  = float(gains[:period].mean())
    avg_l  = float(losses[:period].mean())
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0 if avg_g > 0 else 50.0
    return 100.0 - (100.0 / (1.0 + avg_g / avg_l))


def _aggregate_ohlcv(ohlcv: list, n: int = 5) -> list:
    if len(ohlcv) < n:
        return []
    num_bars = len(ohlcv) // n
    result = []
    for i in range(num_bars):
        chunk = ohlcv[i*n:(i+1)*n]
        result.append([
            chunk[0][0],
            float(chunk[0][1]),
            max(float(c[2]) for c in chunk),
            min(float(c[3]) for c in chunk),
            float(chunk[-1][4]),
            sum(float(c[5]) for c in chunk),
        ])
    return result


def _closes(ohlcv: list) -> list:
    return [float(c[4]) for c in ohlcv]


# ══════════════════════════════════════════════════════════════════════════════
# Base Strategy
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# Active Strategies
# ══════════════════════════════════════════════════════════════════════════════

class RSIStrategy(BaseStrategy):
    """RSI(4) — fast RSI for 1-min scalping. OB/OS at 80/20."""
    name = "rsi"
    PERIOD = 4
    OB, OS = 80, 20

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
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

        if crossover:   reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder: reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)

        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * (self.PERIOD + 2):
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= self.PERIOD + 2:
                htf_rsi = _rsi_val(np.array(_closes(htf)), self.PERIOD)
                htf_signal = "UP" if htf_rsi < 50 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{rsi:.1f}",
                "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree}


class MACDStrategy(BaseStrategy):
    """MACD(3,10,16) — Raschke 3-10 oscillator."""
    name = "macd"
    FAST, SLOW, SIG = 3, 10, 16

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        need  = self.SLOW + self.SIG + 2
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
        hist      = macd_line[-1] - sig_line[-1]
        prev_hist = macd_line[-2] - sig_line[-2]
        crossover  = prev_hist < 0 <= hist
        crossunder = prev_hist > 0 >= hist

        if hist >= 0:
            signal = "UP";   conf = self._clamp_confidence(0.52 + min(abs(hist) * 10, 0.30))
            reasoning = f"Bullish MACD hist +{hist:.4f}"
        else:
            signal = "DOWN"; conf = self._clamp_confidence(0.52 + min(abs(hist) * 10, 0.30))
            reasoning = f"Bearish MACD hist {hist:.4f}"

        if crossover:   reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.07)
        elif crossunder: reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.07)

        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * need:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= need:
                hp = np.array(_closes(htf), dtype=float)
                h_macd = _ema_series(hp, self.FAST) - _ema_series(hp, self.SLOW)
                h_hist = (h_macd - _ema_series(h_macd, self.SIG))[-1]
                htf_signal = "UP" if h_hist > 0 else "DOWN"
                mtf_agree  = signal == htf_signal

        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{hist:.4f}",
                "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree}


class StochasticStrategy(BaseStrategy):
    """Fast Stochastic(5,3,3) — %K/%D using actual OHLCV High/Low/Close."""
    name = "stochastic"
    K_PERIOD = 5
    D_PERIOD = 3
    OB, OS   = 80, 20

    @staticmethod
    def _k_series_ohlcv(bars, k_period: int) -> np.ndarray:
        result = []
        for i in range(k_period - 1, len(bars)):
            w  = bars[i - k_period + 1 : i + 1]
            lo = min(float(b[3]) for b in w)
            hi = max(float(b[2]) for b in w)
            c  = float(bars[i][4])
            result.append(float((c - lo) / (hi - lo) * 100) if hi != lo else 50.0)
        return np.array(result)

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        need  = self.K_PERIOD + self.D_PERIOD + 1

        if ohlcv and len(ohlcv) >= need:
            ks = self._k_series_ohlcv(ohlcv, self.K_PERIOD)
        else:
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

        k      = ks[-1];  d  = float(np.mean(ks[-self.D_PERIOD:]))
        prev_k = ks[-2]; prev_d = float(np.mean(ks[-self.D_PERIOD - 1:-1]))
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

        if crossover:   reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.07)
        elif crossunder: reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.07)

        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 5 * need:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= need:
                htf_ks = self._k_series_ohlcv(htf, self.K_PERIOD)
                if len(htf_ks) >= self.D_PERIOD:
                    htf_signal = "UP" if htf_ks[-1] < 50 else "DOWN"
                    mtf_agree  = signal == htf_signal

        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{k:.1f}",
                "htf_signal": htf_signal, "crossover": crossover, "crossunder": crossunder, "mtf_agree": mtf_agree}


class EMACrossStrategy(BaseStrategy):
    """Dual EMA cross — EMA5/13 (fast) and EMA21/55 (slow) on 1m closes."""
    name = "ema_cross"
    FAST_SHORT, FAST_LONG = 5, 13
    SLOW_SHORT, SLOW_LONG = 21, 55

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if ohlcv and len(ohlcv) >= self.SLOW_LONG + 2:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)
        if len(p) < self.SLOW_LONG + 2:
            return self._no_data()

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
        if crossover:   reasoning += " [CROSS↑]"; fast_conf = self._clamp_confidence(fast_conf + 0.08)
        elif crossunder: reasoning += " [CROSS↓]"; fast_conf = self._clamp_confidence(fast_conf + 0.08)

        s_fast = _ema_series(p, self.SLOW_SHORT)
        s_slow = _ema_series(p, self.SLOW_LONG)
        s_diff      = float(s_fast[-1] - s_slow[-1])
        s_prev_diff = float(s_fast[-2] - s_slow[-2])
        slow_crossover  = s_prev_diff <= 0 < s_diff
        slow_crossunder = s_prev_diff >= 0 > s_diff

        slow_signal = "UP" if s_diff > 0 else "DOWN"
        slow_conf   = self._clamp_confidence(0.54 + min(abs(s_diff) / 100, 0.28))
        if slow_crossover:   slow_conf = self._clamp_confidence(slow_conf + 0.09)
        elif slow_crossunder: slow_conf = self._clamp_confidence(slow_conf + 0.09)

        agree = fast_signal == slow_signal
        if agree:
            fast_conf = self._clamp_confidence(fast_conf + 0.04)

        return {
            "signal": fast_signal, "confidence": fast_conf,
            "reasoning": reasoning, "value": f"{f_diff:.2f}",
            "htf_signal": slow_signal,
            "crossover": crossover, "crossunder": crossunder, "mtf_agree": agree,
            "fast_ema_f_val": float(f_fast[-1]), "fast_ema_s_val": float(f_slow[-1]),
            "slow_ema_f_val": float(s_fast[-1]), "slow_ema_s_val": float(s_slow[-1]),
            "slow_diff": s_diff, "slow_crossover": slow_crossover, "slow_crossunder": slow_crossunder,
            "slow_confidence": slow_conf,
        }


class VWAPStrategy(BaseStrategy):
    """Anchored VWAP with 1/2/3σ bands. Anchor = highest-volume bar in last 50 candles."""
    name     = "vwap"
    LOOKBACK = 50
    MIN_BARS = 5

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < self.MIN_BARS:
            return self._no_data("No OHLCV data")

        bars     = ohlcv[-self.LOOKBACK:]
        vols     = [float(k[5]) for k in bars]
        anchor_i = max(range(len(vols)), key=lambda i: vols[i])
        anchor_bars = bars[anchor_i:]
        if len(anchor_bars) < self.MIN_BARS:
            anchor_bars = bars

        tps  = np.array([(float(k[2])+float(k[3])+float(k[4]))/3 for k in anchor_bars], dtype=float)
        va   = np.array([float(k[5]) for k in anchor_bars], dtype=float)
        total_vol = va.sum()
        if total_vol == 0:
            return self._no_data("Zero volume")

        vwap  = float(np.dot(tps, va) / total_vol)
        vw_var = float(np.dot(va, (tps - vwap)**2) / total_vol)
        sigma = float(np.sqrt(max(vw_var, 0.0)))
        cur   = prices[-1]; prev = prices[-2] if len(prices) > 1 else cur
        z     = (cur - vwap) / sigma if sigma > 1e-8 else (cur - vwap) / (vwap * 0.001 + 1e-8)
        above = cur > vwap; az = abs(z)

        if az >= 3.0:   conf = 0.78; band_label = "3σ ext"
        elif az >= 2.0: conf = 0.68; band_label = "2σ band"
        elif az >= 1.0: conf = 0.57; band_label = "1σ band"
        else:           conf = 0.50 + az * 0.06; band_label = "VWAP zone"

        signal     = "UP" if above else "DOWN"
        crossover  = prev <= vwap < cur
        crossunder = prev >= vwap > cur
        if crossover or crossunder:
            conf += 0.05

        anchor_bars_ago = len(bars) - anchor_i
        reasoning = (f"Anchored VWAP ${vwap:.2f} | σ=${sigma:.2f} | "
                     f"{'above' if above else 'below'} {band_label} (z={z:+.2f}) | "
                     f"anchor {anchor_bars_ago}b ago (peak vol)")
        if crossover:   reasoning += " [CROSS↑]"
        elif crossunder: reasoning += " [CROSS↓]"

        return {"signal": signal, "confidence": self._clamp_confidence(conf),
                "reasoning": reasoning, "value": f"${vwap:.2f}",
                "htf_signal": "N/A", "crossover": crossover, "crossunder": crossunder, "mtf_agree": None}


class SupertrendStrategy(BaseStrategy):
    """Supertrend — ATR(10, ×3) dynamic band. Price above = bull; flip = strong signal."""
    name   = "supertrend"
    PERIOD = 10
    MULT   = 3.0

    @staticmethod
    def _rma(arr: np.ndarray, period: int) -> np.ndarray:
        result = np.zeros(len(arr))
        result[period - 1] = arr[:period].mean()
        for i in range(period, len(arr)):
            result[i] = (result[i-1] * (period - 1) + arr[i]) / period
        return result

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < self.PERIOD + 5:
            return self._no_data("Need OHLCV (Binance)")

        highs  = np.array([float(c[2]) for c in ohlcv], dtype=float)
        lows   = np.array([float(c[3]) for c in ohlcv], dtype=float)
        closes = np.array([float(c[4]) for c in ohlcv], dtype=float)
        n      = len(closes)

        tr = np.zeros(n); tr[0] = highs[0] - lows[0]
        for i in range(1, n):
            tr[i] = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))

        atr = self._rma(tr, self.PERIOD)
        hl2 = (highs + lows) / 2.0
        bu  = hl2 + self.MULT * atr
        bl  = hl2 - self.MULT * atr
        fu  = bu.copy(); fl = bl.copy()
        direction = np.ones(n, dtype=int)

        for i in range(1, n):
            fu[i] = bu[i] if (bu[i] < fu[i-1] or closes[i-1] > fu[i-1]) else fu[i-1]
            fl[i] = bl[i] if (bl[i] > fl[i-1] or closes[i-1] < fl[i-1]) else fl[i-1]
            if   direction[i-1] == -1 and closes[i] > fu[i-1]: direction[i] =  1
            elif direction[i-1] ==  1 and closes[i] < fl[i-1]: direction[i] = -1
            else: direction[i] = direction[i-1]

        cur_dir    = int(direction[-1]); prev_dir = int(direction[-2])
        crossover  = prev_dir == -1 and cur_dir ==  1
        crossunder = prev_dir ==  1 and cur_dir == -1
        signal     = "UP" if cur_dir == 1 else "DOWN"
        cur_atr    = float(atr[-1])
        band       = float(fl[-1]) if cur_dir == 1 else float(fu[-1])
        dist_atr   = abs(closes[-1] - band) / cur_atr if cur_atr > 0 else 0.0
        conf       = self._clamp_confidence(0.55 + min(dist_atr * 0.06, 0.25))

        if crossover or crossunder:
            conf = self._clamp_confidence(conf + 0.08)
            reasoning = f"ST flipped {'UP' if crossover else 'DOWN'} | ATR={cur_atr:.1f}"
        else:
            reasoning = f"ST {'bull' if cur_dir == 1 else 'bear'} | dist={dist_atr:.2f}×ATR"

        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{cur_atr:.1f}",
                "htf_signal": "N/A", "crossover": crossover, "crossunder": crossunder, "mtf_agree": None}


class ADXStrategy(BaseStrategy):
    """ADX/DMI(14) — trend strength + direction."""
    name   = "adx"
    PERIOD = 14

    @staticmethod
    def _rma(arr: np.ndarray, period: int) -> np.ndarray:
        result = np.zeros(len(arr))
        result[period - 1] = arr[:period].mean()
        for i in range(period, len(arr)):
            result[i] = (result[i-1] * (period - 1) + arr[i]) / period
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
        tr = np.zeros(n); dmp = np.zeros(n); dmm = np.zeros(n)
        for i in range(1, n):
            up   = highs[i] - highs[i-1]; down = lows[i-1] - lows[i]
            tr[i]  = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
            dmp[i] = up   if (up > down   and up   > 0) else 0.0
            dmm[i] = down if (down > up   and down > 0) else 0.0

        tr_r  = self._rma(tr[1:],  self.PERIOD)
        dmp_r = self._rma(dmp[1:], self.PERIOD)
        dmm_r = self._rma(dmm[1:], self.PERIOD)
        with np.errstate(divide="ignore", invalid="ignore"):
            di_plus  = 100.0 * np.where(tr_r > 0, dmp_r / tr_r, 0.0)
            di_minus = 100.0 * np.where(tr_r > 0, dmm_r / tr_r, 0.0)
            di_sum   = di_plus + di_minus
            dx       = 100.0 * np.where(di_sum > 0, np.abs(di_plus - di_minus) / di_sum, 0.0)
        adx_arr = self._rma(dx, self.PERIOD)

        adx   = float(adx_arr[-1]); dip = float(di_plus[-1]);  dim = float(di_minus[-1])
        p_dip = float(di_plus[-2]); p_dim = float(di_minus[-2])
        crossover  = p_dip <= p_dim and dip > dim
        crossunder = p_dip >= p_dim and dip < dim
        signal     = "UP" if dip > dim else "DOWN"

        if   adx >= 30: conf = self._clamp_confidence(0.72)
        elif adx >= 20: conf = self._clamp_confidence(0.60)
        elif adx >= 12: conf = self._clamp_confidence(0.52)
        else:           conf = self._clamp_confidence(0.44)
        if crossover or crossunder:
            conf = self._clamp_confidence(conf + 0.08)

        trend_str = "strong" if adx >= 25 else ("moderate" if adx >= 20 else "weak")
        reasoning = f"ADX={adx:.1f} ({trend_str}) +DI={dip:.1f} −DI={dim:.1f}"
        if crossover:    reasoning += " [+DI↑]"
        elif crossunder: reasoning += " [−DI↑]"

        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{adx:.1f}",
                "htf_signal": "N/A", "crossover": crossover, "crossunder": crossunder, "mtf_agree": None}


class WilliamsAlligatorStrategy(BaseStrategy):
    """Williams Alligator — Jaw(13)/Teeth(8)/Lips(5) SMMA. Sleeping = ranging."""
    name = "alligator"

    @staticmethod
    def _smma(arr: np.ndarray, period: int) -> float:
        if len(arr) < period:
            return float(arr[-1])
        k = 1.0 / period; val = float(arr[0])
        for v in arr[1:]:
            val = float(v) * k + val * (1.0 - k)
        return val

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if ohlcv and len(ohlcv) >= 15:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)
        if len(p) < 15:
            return self._no_data()
        jaw = self._smma(p, 13); teeth = self._smma(p, 8); lips = self._smma(p, 5)
        bull = lips > teeth > jaw; bear = lips < teeth < jaw
        spread_pct = abs(lips - jaw) / jaw * 100 if jaw > 0 else 0.0
        prev_p = p[:-1]
        prev_lips = self._smma(prev_p, 5)  if len(prev_p) >= 5  else lips
        prev_jaw  = self._smma(prev_p, 13) if len(prev_p) >= 13 else jaw
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
        if crossover:    reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.06)
        elif crossunder: reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.06)

        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 30:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= 13:
                hp = np.array(_closes(htf), dtype=float)
                htf_j = self._smma(hp, 13); htf_t = self._smma(hp, 8); htf_l = self._smma(hp, 5)
                htf_signal = "UP" if htf_l > htf_t > htf_j else ("DOWN" if htf_l < htf_t < htf_j else ("UP" if htf_l > htf_j else "DOWN"))
                mtf_agree  = signal == htf_signal

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
            ad = 0.0; vals = []
            for k in bars:
                h, l, c, v = float(k[2]), float(k[3]), float(k[4]), float(k[5])
                ad += ((c - l) - (h - c)) / (h - l) * v if h != l else 0.0
                vals.append(ad)
            return vals

        ad         = _ad(ohlcv)
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
        if crossover:    reasoning += " [CROSS↑]"; conf = self._clamp_confidence(conf + 0.05)
        elif crossunder: reasoning += " [CROSS↓]"; conf = self._clamp_confidence(conf + 0.05)

        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": f"{ad[-1]:.0f}",
                "htf_signal": "N/A", "crossover": crossover, "crossunder": crossunder, "mtf_agree": None}


class DowTheoryStrategy(BaseStrategy):
    """Dow Theory — trend confirmed via HH+HL (uptrend) or LH+LL (downtrend)."""
    name = "dow_theory"
    SWING_N = 4

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if ohlcv and len(ohlcv) >= self.SWING_N * 2 + 2:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)
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

        if hh and hl:   signal, conf, structure = "UP",   self._clamp_confidence(0.65), "UPTREND"
        elif lh and ll: signal, conf, structure = "DOWN", self._clamp_confidence(0.65), "DOWNTREND"
        elif hh:        signal, conf, structure = "UP",   self._clamp_confidence(0.54), "HH+LL"
        elif ll:        signal, conf, structure = "DOWN", self._clamp_confidence(0.54), "LH+LL"
        else:           signal, conf, structure = ("UP" if p[-1] > float(np.mean(p[-10:])) else "DOWN"), 0.45, "RANGING"

        reasoning = f"Dow: {structure} H:{h1:.0f}→{h2:.0f} L:{l1:.0f}→{l2:.0f}"
        htf_signal, mtf_agree = "N/A", None
        if ohlcv and len(ohlcv) >= 30:
            htf = _aggregate_ohlcv(ohlcv, 5)
            if len(htf) >= self.SWING_N * 2 + 2:
                htf_sh, htf_sl = find_swings(np.array(_closes(htf), dtype=float))
                if len(htf_sh) >= 2 and len(htf_sl) >= 2:
                    htf_hh = htf_sh[-1][1] > htf_sh[-2][1]; htf_hl = htf_sl[-1][1] > htf_sl[-2][1]
                    htf_signal = "UP" if (htf_hh and htf_hl) else ("DOWN" if (not htf_hh and not htf_hl) else signal)
                    mtf_agree  = signal == htf_signal

        return {"signal": signal, "confidence": conf, "reasoning": reasoning, "value": structure[:8],
                "htf_signal": htf_signal, "crossover": hh and hl, "crossunder": lh and ll, "mtf_agree": mtf_agree}


class FibPullbackStrategy(BaseStrategy):
    """Fibonacci Pullback — 38.2/50/61.8% retracements as continuation signals."""
    name = "fib_pullback"
    FIB_LEVELS = [0.236, 0.382, 0.500, 0.618, 0.786]
    TOL = 0.0030; LOOKBACK = 30

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if ohlcv and len(ohlcv) >= self.LOOKBACK:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)
        if len(p) < self.LOOKBACK:
            return self._no_data()
        window  = p[-self.LOOKBACK:]; sw_high = float(window.max()); sw_low = float(window.min())
        rng     = sw_high - sw_low; current = float(p[-1])
        uptrend = int(window.argmax()) > int(window.argmin())
        if rng < current * 0.0005:
            return {"signal": "UP", "confidence": 0.45, "reasoning": "Range too small for Fib",
                    "value": "—", "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}
        pb_pct  = (sw_high - current) / rng if uptrend else (current - sw_low) / rng
        at_fib  = next((lvl for lvl in self.FIB_LEVELS if abs(pb_pct - lvl) < self.TOL), None)
        label   = f"{pb_pct * 100:.1f}%"

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
        ohlcv = kwargs.get("ohlcv", [])
        if ohlcv and len(ohlcv) >= 30:
            p = np.array([float(k[4]) for k in ohlcv], dtype=float)
        else:
            p = np.array(prices, dtype=float)
        if len(p) < 30:
            return self._no_data()
        window = p[-min(50, len(p)):]; n_sw = 3; pivots: List = []
        for i in range(n_sw, len(window) - n_sw):
            w = window[i - n_sw: i + n_sw + 1]
            if window[i] >= w.max():   pivots.append(("H", float(window[i])))
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


class LinearRegressionChannel(BaseStrategy):
    """Linear Regression Channel — least-squares slope on last 30 closes. Key: ml_logistic."""
    name   = "ml_logistic"
    PERIOD = 30

    def predict(self, prices: List[float], **kwargs) -> Dict:
        ohlcv = kwargs.get("ohlcv", [])
        if ohlcv and len(ohlcv) >= self.PERIOD + 5:
            src = np.array([float(c[4]) for c in ohlcv[-(self.PERIOD + 5):]], dtype=float)
        elif len(prices) >= self.PERIOD + 5:
            src = np.array(prices[-(self.PERIOD + 5):], dtype=float)
        else:
            return {"signal": "UP", "confidence": 0.45, "reasoning": "LR: insufficient data",
                    "value": "0.000", "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}

        recent = src[-self.PERIOD:]
        x    = np.arange(self.PERIOD, dtype=float); x_mu = x.mean(); y_mu = recent.mean()
        slope     = np.sum((x - x_mu) * (recent - y_mu)) / np.sum((x - x_mu) ** 2)
        intercept = y_mu - slope * x_mu
        y_hat     = slope * x + intercept
        ss_res    = np.sum((recent - y_hat) ** 2)
        ss_tot    = np.sum((recent - y_mu) ** 2)
        r2        = max(0.0, 1.0 - ss_res / ss_tot) if ss_tot > 1e-10 else 0.0
        slope_pct = slope / recent[-1] * 100
        signal    = "UP" if slope > 0 else "DOWN"
        conf      = max(0.40, min(0.85, 0.50 + r2 * 0.25 + min(abs(slope_pct) * 15, 0.10)))

        return {"signal": signal, "confidence": round(conf, 4),
                "reasoning": f"LR({self.PERIOD}) slope={slope_pct:+.4f}%/bar R²={r2:.3f}",
                "value": f"{r2:.3f}", "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}


# ── Registry ──────────────────────────────────────────────────────────────────

ALL_STRATEGIES = [
    RSIStrategy(), MACDStrategy(), StochasticStrategy(),
    EMACrossStrategy(), SupertrendStrategy(), ADXStrategy(),
    WilliamsAlligatorStrategy(), AccDistStrategy(), DowTheoryStrategy(),
    FibPullbackStrategy(), HarmonicPatternStrategy(),
    VWAPStrategy(),
]


def get_all_predictions(prices: List[float], **kwargs) -> Dict[str, Dict]:
    results = {}
    for strategy in ALL_STRATEGIES:
        try:
            results[strategy.name] = strategy.predict(prices, **kwargs)
        except Exception as e:
            results[strategy.name] = {
                "signal": "UP", "confidence": 0.45, "reasoning": f"Error: {e}", "value": "ERR",
                "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
            }
    return results


# ══════════════════════════════════════════════════════════════════════════════
# Ensemble Predictor
# ══════════════════════════════════════════════════════════════════════════════

_DISABLED_THRESH  = 0.40
_WEAK_THRESH      = 0.50
_STRONG_THRESH    = 0.60
_EXCELLENT_THRESH = 0.65


def accuracy_to_label(accuracy: float, total: int, min_samples: int = 10) -> str:
    if total < min_samples: return "LEARNING"
    if accuracy < _DISABLED_THRESH:  return "DISABLED"
    if accuracy < _WEAK_THRESH:      return "WEAK"
    if accuracy < _STRONG_THRESH:    return "MARGINAL"
    if accuracy < _EXCELLENT_THRESH: return "RELIABLE"
    return "EXCELLENT"


def accuracy_to_target_weight(accuracy: float, total: int, min_samples: int = 10) -> float:
    if total < min_samples:
        return 1.0
    if accuracy < _DISABLED_THRESH:
        return 0.05
    if accuracy < _WEAK_THRESH:
        t = (accuracy - _DISABLED_THRESH) / (_WEAK_THRESH - _DISABLED_THRESH)
        return round(0.10 + t * 0.40, 3)
    if accuracy < _STRONG_THRESH:
        t = (accuracy - _WEAK_THRESH) / (_STRONG_THRESH - _WEAK_THRESH)
        return round(0.50 + t * 0.70, 3)
    if accuracy < _EXCELLENT_THRESH:
        t = (accuracy - _STRONG_THRESH) / (_EXCELLENT_THRESH - _STRONG_THRESH)
        return round(1.20 + t * 0.80, 3)
    t = min(1.0, (accuracy - _EXCELLENT_THRESH) / 0.15)
    return round(min(3.0, 2.00 + t * 1.00), 3)


class EnsemblePredictor:
    """Weighted ensemble that dynamically adjusts strategy weights based on accuracy."""

    def __init__(self, initial_weights: Optional[Dict[str, float]] = None):
        self.weights = initial_weights or {}
        self.default_weight = 1.0

    def predict(self, strategy_predictions: Dict[str, Dict]) -> Dict:
        if not strategy_predictions:
            return {"signal": "UP", "confidence": 0.5, "up_probability": 0.5,
                    "bullish_count": 0, "bearish_count": 0}

        up_score = down_score = 0.0
        for name, pred in strategy_predictions.items():
            w    = self.weights.get(name, self.default_weight)
            conf = pred.get("confidence", 0.5)
            if pred["signal"] == "UP":
                up_score += conf * w
            else:
                down_score += conf * w

        total      = up_score + down_score
        up_prob    = up_score / total if total > 0 else 0.5
        confidence = max(up_prob, 1 - up_prob)
        signal     = "NEUTRAL" if confidence < 0.65 else ("UP" if up_prob > 0.5 else "DOWN")
        return {
            "signal":             signal,
            "confidence":         confidence,
            "up_probability":     up_prob,
            "bullish_count":      sum(1 for p in strategy_predictions.values() if p["signal"] == "UP"),
            "bearish_count":      sum(1 for p in strategy_predictions.values() if p["signal"] == "DOWN"),
            "weighted_up_score":  up_score,
            "weighted_down_score": down_score,
        }

    def update_weights(
        self,
        strategy_accuracies: Dict[str, float],
        min_samples: int = 10,
        learning_rate: float = 0.15,
        strategy_counts: Optional[Dict[str, int]] = None,
    ):
        for name, accuracy in strategy_accuracies.items():
            count  = (strategy_counts or {}).get(name, min_samples)
            target = accuracy_to_target_weight(accuracy, count, min_samples)
            current = self.weights.get(name, self.default_weight)
            self.weights[name] = round(current * (1 - learning_rate) + target * learning_rate, 3)

    def update_weights_from_full_stats(
        self,
        accuracy_stats: Dict[str, Dict],
        min_samples: int = 10,
        learning_rate: float = 0.15,
    ):
        accuracies = {n: s["accuracy"] for n, s in accuracy_stats.items()}
        counts     = {n: s["total"]    for n, s in accuracy_stats.items()}
        self.update_weights(accuracies, min_samples, learning_rate, counts)

    def get_weights(self) -> Dict[str, float]:
        return dict(self.weights)


# ══════════════════════════════════════════════════════════════════════════════
# EV Calculator
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class EVResult:
    expected_value: float
    edge: float
    implied_probability: float
    model_probability: float
    kelly_fraction: float
    signal: str
    reasoning: str


def calculate_ev(
    model_probability: float,
    market_odds: float,
    max_kelly: float = 0.25,
    min_ev_threshold: float = 0.05,
    strong_ev_threshold: float = 0.15,
) -> EVResult:
    implied_prob = 1.0 / (1.0 + market_odds) if market_odds > 0 else 1.0
    edge = model_probability - implied_prob
    ev   = (model_probability * market_odds) - (1 - model_probability)

    if market_odds > 0 and edge > 0:
        kelly = min(edge / market_odds, max_kelly)
    else:
        kelly = 0.0

    if ev >= strong_ev_threshold:
        signal    = "STRONG_ENTER"
        reasoning = f"+EV of {ev:.3f} exceeds strong threshold. Edge: {edge*100:.1f}%. Kelly suggests {kelly*100:.1f}%."
    elif ev >= min_ev_threshold:
        signal    = "MARGINAL"
        reasoning = f"+EV of {ev:.3f} is positive but marginal. Edge: {edge*100:.1f}%. Proceed with caution."
    elif ev > 0:
        signal    = "WEAK"
        reasoning = f"Slightly +EV ({ev:.3f}) but below threshold. Consider passing."
    else:
        signal    = "PASS"
        reasoning = f"Negative EV ({ev:.3f}). Market odds don't justify entry. Edge: {edge*100:.1f}%."

    return EVResult(
        expected_value=ev, edge=edge,
        implied_probability=implied_prob, model_probability=model_probability,
        kelly_fraction=kelly, signal=signal, reasoning=reasoning,
    )


