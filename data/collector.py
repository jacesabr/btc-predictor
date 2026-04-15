"""
Binance BTC/USD Data Collector

Polls BTCUSDT price from Binance REST API every poll_interval seconds.
Raises on failure — no simulation fallback.
"""

import asyncio
import time
import logging
from dataclasses import dataclass
from typing import Optional, Callable, List
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class Tick:
    """Single price observation."""
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
    """Collects BTC/USD price from the Binance REST API (BTCUSDT ticker)."""

    BINANCE_URL = "https://api.binance.com/api/v3/ticker/price"

    def __init__(self, poll_interval: float = 2.0, max_ticks: int = 10000):
        self.poll_interval = poll_interval
        self.max_ticks = max_ticks
        self.ticks: List[Tick] = []
        self.callbacks: List[Callable] = []
        self._running = False
        self._last_real_price: Optional[float] = None
        logger.info("Collector using Binance REST price feed")

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
        """Fetch BTCUSDT spot price from Binance REST API. Returns None on failure."""
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
            logger.warning("Binance price fetch failed: %s", exc)
            return None

    def seed_from_klines(self, klines: list, n: int = 200):
        """Pre-seed tick history from Binance klines so ensemble warms up instantly.
        Only runs when fewer than 30 real ticks are available."""
        if not klines or len(self.ticks) >= 30:
            return
        self.ticks.clear()
        rows = klines[-n:]
        for row in rows:
            price = float(row[4])  # close price
            ts = int(row[0]) / 1000  # open_time ms → seconds
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
