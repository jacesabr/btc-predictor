"""
Polymarket Live Odds Fetcher — BTC Up/Down 5-minute markets

Market slug pattern: btc-updown-5m-{unix_timestamp}
where unix_timestamp is the 5-minute-aligned start of the window.

Prices come directly from the Gamma API outcomePrices field —
no CLOB authentication required.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"


class PolymarketFeed:
    """
    Polls the Polymarket BTC Up/Down 5-minute prediction market.

    The market slug is btc-updown-5m-{window_start_unix} where the timestamp
    is aligned to 5-minute (300-second) boundaries from the Unix epoch.

    Usage:
        feed = PolymarketFeed()
        asyncio.create_task(feed.run())
        ...
        prob  = feed.implied_prob    # e.g. 0.51 = market thinks 51% chance UP
        odds  = feed.market_odds     # payout ratio: (1 - up_price) / up_price
        fresh = feed.is_live         # True if last update < 30s ago
    """

    def __init__(self, poll_interval: float = 5.0):
        self.poll_interval = poll_interval

        # Live state
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

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

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
            "embed_url":       f"https://embed.polymarket.com/market?market={self.active_slug}&theme=light&chart=true&buttons=true&fit=true" if self.active_slug else None,
        }

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Polling via Gamma API event slug
    # ------------------------------------------------------------------

    async def _poll(self):
        """
        Compute the current 5-minute window slug and fetch prices.
        Falls back to the next window if the current one isn't live yet.
        """
        now = int(time.time())
        window_start = (now // 300) * 300

        # ThreadedResolver uses system DNS (works through VPN, unlike aiodns)
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(connector=connector) as session:
            # Try current window, then next window (opens a few seconds early)
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
                    else:
                        logger.debug("Polymarket: up=%.3f odds=1:%.3f", up_price, self.market_odds)
                    return

        logger.warning("Polymarket: no active BTC 5-min market found for window %d", window_start)

    async def _fetch_slug(
        self, session: aiohttp.ClientSession, slug: str
    ) -> Optional[float]:
        """
        Fetch the Polymarket event by slug and return the UP outcome price,
        or None if the market doesn't exist / has no prices.
        """
        url = f"{GAMMA_API}/events"
        params = {"slug": slug}
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return None
                events = await resp.json()
        except Exception as exc:
            logger.debug("Gamma API error for slug %s: %s", slug, exc)
            return None

        if not events:
            return None

        event = events[0]

        # Skip closed or inactive markets
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

        # outcomePrices is a JSON-encoded string like '["0.505", "0.495"]'
        outcomes_raw = market.get("outcomes", "[]")
        prices_raw   = market.get("outcomePrices", "[]")

        try:
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            prices   = json.loads(prices_raw)   if isinstance(prices_raw, str)   else prices_raw
        except Exception:
            return None

        if not prices:
            return None

        # Match UP outcome to its price
        for i, outcome in enumerate(outcomes):
            if str(outcome).upper() in ("UP", "YES", "HIGHER", "RISE"):
                if i < len(prices):
                    return float(prices[i])

        # Fallback: first outcome is always "Up" in these markets
        return float(prices[0])

    # ------------------------------------------------------------------
    # Price conversion
    # ------------------------------------------------------------------

    def _update_price(self, up_price: float):
        up_price = max(0.01, min(0.99, up_price))
        self.yes_price    = up_price
        self.implied_prob = up_price
        # market_odds: profit per $1 staked on UP if it wins
        self.market_odds  = (1.0 - up_price) / up_price
        self._last_update = time.time()
