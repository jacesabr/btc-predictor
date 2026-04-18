"""
Data Feed
=========
Three data sources bundled together:

  BinanceCollector — polls BTCUSDT spot price every poll_interval seconds via REST.
  FeatureEngine    — computes RSI, MACD, Bollinger, EMAs, VWAP, OBV, volatility, etc.
  PolymarketFeed   — polls the BTC Up/Down 5-minute Polymarket market for crowd odds.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

import aiohttp
import numpy as np

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# BinanceCollector
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Tick:
    timestamp: float
    mid_price: float
    bid_price: float
    ask_price: float
    spread: float
    source: str = "binance_rest"

    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp)


class BinanceCollector:
    """Fetches BTC/USD from Binance REST, falls back to CoinAPI if blocked."""

    BINANCE_URL  = "https://api.binance.com/api/v3/ticker/price"
    COINAPI_URL  = "https://rest.coinapi.io/v1/exchangerate/BTC/USD"

    def __init__(self, poll_interval: float = 2.0, max_ticks: int = 10000, coinapi_key: str = ""):
        self.poll_interval = poll_interval
        self.max_ticks = max_ticks
        self.ticks: List[Tick] = []
        self.callbacks: List[Callable] = []
        self._running = False
        self._last_real_price: Optional[float] = None
        self._coinapi_key = coinapi_key
        logger.info("Collector using Binance REST price feed (CoinAPI fallback: %s)", "yes" if coinapi_key else "no")

    def on_tick(self, callback: Callable[[Tick], None]):
        self.callbacks.append(callback)

    async def start(self):
        self._running = True
        logger.info("Collector started (interval: %.1fs)", self.poll_interval)
        while self._running:
            try:
                tick = await self._fetch_binance_price()
                if tick:
                    self._store_tick(tick)
                    for cb in self.callbacks:
                        try:
                            cb(tick)
                        except Exception as exc:
                            logger.error("Tick callback error: %s", exc)
                else:
                    logger.error("Binance price fetch returned no data")
            except Exception as exc:
                logger.error("Fetch error: %s", exc)
            await asyncio.sleep(self.poll_interval)

    async def stop(self):
        self._running = False

    async def _fetch_binance_price(self) -> Optional[Tick]:
        try:
            connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    self.BINANCE_URL,
                    params={"symbol": "BTCUSDT"},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                    price = float(data["price"])
                    self._last_real_price = price
                    spread = price * 0.00005
                    return Tick(
                        timestamp=time.time(),
                        mid_price=price,
                        bid_price=price - spread / 2,
                        ask_price=price + spread / 2,
                        spread=spread,
                        source="binance_rest",
                    )
        except Exception as exc:
            logger.warning("Binance price fetch failed: %s — trying CoinAPI fallback", exc)

        if not self._coinapi_key:
            return None
        try:
            connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    self.COINAPI_URL,
                    headers={"X-CoinAPI-Key": self._coinapi_key},
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    data = await resp.json()
                    price = float(data["rate"])
                    self._last_real_price = price
                    spread = price * 0.00005
                    return Tick(
                        timestamp=time.time(),
                        mid_price=price,
                        bid_price=price - spread / 2,
                        ask_price=price + spread / 2,
                        spread=spread,
                        source="coinapi_rest",
                    )
        except Exception as exc:
            logger.warning("CoinAPI price fetch failed: %s", exc)
            return None

    def seed_from_klines(self, klines: list, n: int = 200):
        """Pre-seed tick history from Binance klines so ensemble warms up instantly."""
        if not klines or len(self.ticks) >= 30:
            return
        self.ticks.clear()
        rows = klines[-n:]
        for row in rows:
            price = float(row[4])
            ts = int(row[0]) / 1000
            spread = price * 0.00005
            self._store_tick(Tick(
                timestamp=ts,
                mid_price=price,
                bid_price=price - spread / 2,
                ask_price=price + spread / 2,
                spread=spread,
                source="binance_kline_seed",
            ))
        self._last_real_price = float(rows[-1][4])
        logger.info("Seeded %d ticks from Binance klines (last price: %.2f)", len(rows), self._last_real_price)

    def _store_tick(self, tick: Tick):
        self.ticks.append(tick)
        if len(self.ticks) > self.max_ticks:
            self.ticks = self.ticks[-self.max_ticks:]

    def get_prices(self, n: Optional[int] = None) -> List[float]:
        ticks = self.ticks[-n:] if n else self.ticks
        return [t.mid_price for t in ticks]

    def get_spreads(self, n: Optional[int] = None) -> List[float]:
        ticks = self.ticks[-n:] if n else self.ticks
        return [t.spread for t in ticks]

    def get_ticks_since(self, timestamp: float) -> List[Tick]:
        return [t for t in self.ticks if t.timestamp >= timestamp]

    @property
    def current_price(self) -> Optional[float]:
        return self.ticks[-1].mid_price if self.ticks else None

    @property
    def tick_count(self) -> int:
        return len(self.ticks)

    @property
    def data_source(self) -> str:
        return "binance_rest" if self._last_real_price else "unavailable"


# ══════════════════════════════════════════════════════════════════════════════
# FeatureEngine
# ══════════════════════════════════════════════════════════════════════════════

class FeatureEngine:
    """Computes technical features from price history for prediction models."""

    @staticmethod
    def compute_all(prices: List[float], spreads: Optional[List[float]] = None, ohlcv: Optional[List] = None) -> Dict[str, float]:
        if len(prices) < 30:
            return {}

        features = {}
        p = np.array(prices)

        for lookback in [1, 2, 5, 10, 15, 30]:
            if len(p) > lookback:
                features[f"return_{lookback}"] = (p[-1] / p[-1 - lookback] - 1) * 100

        features["rsi_14"] = FeatureEngine._rsi(p, 14)
        features["rsi_7"]  = FeatureEngine._rsi(p, 7)

        k12 = 2 / (12 + 1); k26 = 2 / (26 + 1)
        ema12 = np.empty(len(p)); ema26 = np.empty(len(p))
        ema12[0] = ema26[0] = p[0]
        for i in range(1, len(p)):
            ema12[i] = p[i] * k12 + ema12[i-1] * (1 - k12)
            ema26[i] = p[i] * k26 + ema26[i-1] * (1 - k26)
        macd_s = ema12 - ema26
        k9 = 2 / (9 + 1)
        sig_s = np.empty(len(macd_s)); sig_s[0] = macd_s[0]
        for i in range(1, len(macd_s)):
            sig_s[i] = macd_s[i] * k9 + sig_s[i-1] * (1 - k9)
        features["macd"]            = float(macd_s[-1])
        features["macd_signal"]     = float(sig_s[-1])
        features["macd_histogram"]  = float(macd_s[-1] - sig_s[-1])

        sma20 = np.mean(p[-20:]); std20 = np.std(p[-20:])
        if std20 > 0:
            features["bollinger_pct_b"] = (p[-1] - (sma20 - 2*std20)) / (4*std20)
            features["bollinger_width"] = (4*std20) / sma20
        else:
            features["bollinger_pct_b"] = 0.5
            features["bollinger_width"] = 0

        for period in [14, 7]:
            window = p[-period:]
            lo, hi = np.min(window), np.max(window)
            features[f"stoch_k_{period}"] = ((p[-1] - lo) / (hi - lo) * 100) if hi != lo else 50

        for period in [5, 8, 13, 21]:
            features[f"ema_{period}"] = FeatureEngine._ema(p, period)
            features[f"price_vs_ema_{period}"] = (p[-1] / features[f"ema_{period}"] - 1) * 100
        features["ema_cross_8_21"] = features["ema_8"] - features["ema_21"]

        for lookback in [5, 10, 20]:
            if len(p) > lookback:
                returns = np.diff(p[-lookback:]) / p[-lookback:-1]
                features[f"volatility_{lookback}"] = np.std(returns) * 100

        for lookback in [10, 30, 60]:
            if len(p) > lookback:
                window = p[-lookback:]
                lo, hi = np.min(window), np.max(window)
                features[f"price_position_{lookback}"] = ((p[-1] - lo) / (hi - lo)) if hi != lo else 0.5

        if len(p) > 10:
            mom5 = p[-1] - p[-6]; mom5_prev = p[-2] - p[-7]
            features["momentum_acceleration"] = mom5 - mom5_prev

        if spreads and len(spreads) > 10:
            s = np.array(spreads)
            features["spread_current"]   = s[-1]
            features["spread_mean_10"]   = np.mean(s[-10:])
            features["spread_expanding"] = 1.0 if s[-1] > np.mean(s[-10:]) else 0.0

        if ohlcv and len(ohlcv) >= 15:
            features["mfi_14"]  = FeatureEngine._mfi(ohlcv, 14)
            features["mfi_7"]   = FeatureEngine._mfi(ohlcv, 7)
            vwap = FeatureEngine._vwap(ohlcv[-20:])
            if vwap > 0:
                features["vwap_ref"]      = vwap
                features["price_vs_vwap"] = (p[-1] / vwap - 1) * 100
            obv_slope = FeatureEngine._obv(ohlcv[-10:]) - FeatureEngine._obv(ohlcv[-20:-10])
            features["obv_slope"] = obv_slope
            vols = [float(k[5]) for k in ohlcv[-20:]]
            if len(vols) > 5 and np.mean(vols[:-1]) > 0:
                features["volume_surge"] = vols[-1] / np.mean(vols[:-1])
            if vwap > 0:
                tp_vals = [(float(k[2])+float(k[3])+float(k[4]))/3 for k in ohlcv[-20:]]
                std_tp = float(np.std(tp_vals))
                features["vwap_band_pos"] = (p[-1] - vwap) / (2 * std_tp) if std_tp > 0 else 0

        if len(p) > 20:
            x = np.arange(20)
            slope, intercept = np.polyfit(x, p[-20:], 1)
            predicted = slope * x + intercept
            ss_res = np.sum((p[-20:] - predicted) ** 2)
            ss_tot = np.sum((p[-20:] - np.mean(p[-20:])) ** 2)
            features["trend_r_squared"] = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
            features["trend_slope"]     = slope

        return features

    @staticmethod
    def _rsi(prices: np.ndarray, period: int) -> float:
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

    @staticmethod
    def _ema(prices: np.ndarray, period: int) -> float:
        k = 2 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = price * k + ema * (1 - k)
        return ema

    @staticmethod
    def _mfi(ohlcv: List, period: int = 14) -> float:
        if len(ohlcv) < period + 1:
            return 50.0
        tp  = [(float(k[2]) + float(k[3]) + float(k[4])) / 3 for k in ohlcv]
        vol = [float(k[5]) for k in ohlcv]
        rmf = [tp[i] * vol[i] for i in range(len(tp))]
        pos = sum(rmf[i] for i in range(1, period + 1) if tp[i] > tp[i-1])
        neg = sum(rmf[i] for i in range(1, period + 1) if tp[i] < tp[i-1])
        for i in range(period + 1, len(tp)):
            new_pos = rmf[i] if tp[i] > tp[i-1] else 0.0
            new_neg = rmf[i] if tp[i] < tp[i-1] else 0.0
            pos = (pos * (period - 1) + new_pos) / period
            neg = (neg * (period - 1) + new_neg) / period
        if neg == 0:
            return 100.0 if pos > 0 else 50.0
        return 100.0 - (100.0 / (1.0 + pos / neg))

    @staticmethod
    def _vwap(ohlcv: List) -> float:
        cum_tv = sum((float(k[2])+float(k[3])+float(k[4]))/3 * float(k[5]) for k in ohlcv)
        cum_v  = sum(float(k[5]) for k in ohlcv)
        return cum_tv / cum_v if cum_v > 0 else 0.0

    @staticmethod
    def _obv(ohlcv: List) -> float:
        obv = 0.0
        for i in range(1, len(ohlcv)):
            c, cp = float(ohlcv[i][4]), float(ohlcv[i-1][4])
            v = float(ohlcv[i][5])
            obv += v if c > cp else (-v if c < cp else 0.0)
        return obv


# ══════════════════════════════════════════════════════════════════════════════
# PolymarketFeed
# ══════════════════════════════════════════════════════════════════════════════

GAMMA_API = "https://gamma-api.polymarket.com"


class PolymarketFeed:
    """
    Polls the Polymarket BTC Up/Down 5-minute prediction market.
    Slug: btc-updown-5m-{window_start_unix}
    """

    def __init__(self, poll_interval: float = 5.0):
        self.poll_interval  = poll_interval
        self.yes_price:     float = 0.5
        self.implied_prob:  float = 0.5
        self.market_odds:   float = 1.0
        self.market_question: Optional[str] = None
        self.volume_24hr:   float = 0.0
        self.open_interest: float = 0.0
        self.liquidity:     float = 0.0
        self.active_slug:   Optional[str] = None
        self._last_update:  float = 0.0
        self._running = False

    @property
    def is_live(self) -> bool:
        return (time.time() - self._last_update) < 30

    def to_dict(self) -> dict:
        return {
            "yes_price":       round(self.yes_price, 4),
            "implied_prob":    round(self.implied_prob, 4),
            "market_odds":     round(self.market_odds, 4),
            "market_question": self.market_question,
            "volume_24hr":     round(self.volume_24hr, 2),
            "open_interest":   round(self.open_interest, 2),
            "liquidity":       round(self.liquidity, 2),
            "is_live":         self.is_live,
            "last_update":     self._last_update,
            "active_slug":     self.active_slug,
            "embed_url":       (
                f"https://embed.polymarket.com/market?market={self.active_slug}"
                "&theme=light&chart=true&buttons=true&fit=true"
                if self.active_slug else None
            ),
        }

    async def run(self):
        self._running = True
        while self._running:
            try:
                await self._poll()
            except Exception as exc:
                logger.error("Polymarket feed error: %s", exc)
            await asyncio.sleep(self.poll_interval)

    async def stop(self):
        self._running = False

    async def _poll(self):
        now          = int(time.time())
        window_start = (now // 300) * 300
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(connector=connector) as session:
            for offset in [0, 300, -300]:
                slug = f"btc-updown-5m-{window_start + offset}"
                up_price = await self._fetch_slug(session, slug)
                if up_price is not None:
                    self.active_slug = slug
                    was_stale = not self.is_live
                    self._update_price(up_price)
                    if was_stale:
                        logger.info(
                            "Polymarket: LIVE | %s | UP=%.1f%% odds=1:%.3f OI=$%.0f",
                            self.market_question, up_price * 100, self.market_odds, self.open_interest,
                        )
                    return
        logger.warning("Polymarket: no active BTC 5-min market found for window %d", window_start)

    async def _fetch_slug(self, session: aiohttp.ClientSession, slug: str) -> Optional[float]:
        url = f"{GAMMA_API}/events"
        try:
            async with session.get(url, params={"slug": slug}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                events = await resp.json()
        except Exception as exc:
            logger.debug("Gamma API error for slug %s: %s", slug, exc)
            return None

        if not events:
            return None
        event = events[0]
        if event.get("closed") or not event.get("active"):
            return None
        markets = event.get("markets", [])
        if not markets:
            return None
        market = markets[0]
        self.market_question = event.get("title") or market.get("question")
        self.volume_24hr   = float(event.get("volume24hr") or 0)
        self.open_interest = float(market.get("openInterest") or event.get("openInterest") or 0)
        self.liquidity     = float(event.get("liquidity") or 0)

        outcomes_raw = market.get("outcomes", "[]")
        prices_raw   = market.get("outcomePrices", "[]")
        try:
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            prices   = json.loads(prices_raw)   if isinstance(prices_raw, str)   else prices_raw
        except Exception:
            return None

        if not prices:
            return None
        for i, outcome in enumerate(outcomes):
            if str(outcome).upper() in ("UP", "YES", "HIGHER", "RISE"):
                if i < len(prices):
                    return float(prices[i])
        return float(prices[0])

    def _update_price(self, up_price: float):
        up_price       = max(0.01, min(0.99, up_price))
        self.yes_price = up_price
        self.implied_prob = up_price
        self.market_odds  = (1.0 - up_price) / up_price
        self._last_update = time.time()
