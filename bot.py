"""
Polymarket Trading Bot
======================
Monitors DeepSeek predictions and places live Polymarket orders.

Entry: when DeepSeek signals UP or DOWN (NEUTRAL skipped).
Sizing: 10% of current USDC wallet balance per trade.

Requires:
  POLYMARKET_PRIVATE_KEY in .env
  pip install py-clob-client

Order records saved to: results/live_orders.ndjson

Usage (standalone):
  python bot.py

Usage (embedded):
  from bot import PolymarketBot
  bot = PolymarketBot()
  asyncio.create_task(bot.run())
"""

import argparse
import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional

import aiohttp
import httpx

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

logger = logging.getLogger(__name__)

_DATA_DIR  = Path(__file__).parent / "results"
_LIVE_FILE = _DATA_DIR / "live_orders.ndjson"

_CLOB_HOST  = "https://clob.polymarket.com"
_GAMMA_HOST = "https://gamma-api.polymarket.com"

_POLYGON_RPC    = "https://polygon-rpc.com"
_USDC_CONTRACT  = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
_BALANCE_OF_SIG = "70a08231"

FIXED_TRADE_USDC = 3.0
LIMIT_PRICE      = 0.49   # only fill if we get <50% price (better than even odds)

# Set to True once funded and ready to trade
LIVE_TRADING_ENABLED = False


def _append_ndjson(path: Path, record: Dict):
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


class PolymarketBot:

    def __init__(self, trade_size: float = FIXED_TRADE_USDC):
        self.trade_size      = trade_size
        self._open_trades: Dict[float, Dict] = {}
        self._last_bar: Optional[float] = None
        self._private_key: Optional[str] = os.getenv("POLYMARKET_PRIVATE_KEY")
        self._address: Optional[str] = None

        if not self._private_key:
            raise ValueError("POLYMARKET_PRIVATE_KEY not found in .env")

        try:
            from eth_account import Account
            self._address = Account.from_key(self._private_key).address
            logger.info("Wallet: %s", self._address)
        except Exception as exc:
            raise RuntimeError(f"Could not derive wallet address: {exc}")

    # ── Public interface ──────────────────────────────────────

    async def run(self):
        logger.info("PolymarketBot started (size=$%.2f per trade)", self.trade_size)
        try:
            from engine import current_state
        except ImportError:
            logger.error("engine.py not found — must run in same process as engine.py")
            return
        while True:
            try:
                await self._tick(current_state)
            except Exception as exc:
                logger.error("Tick error: %s", exc)
            await asyncio.sleep(2)

    async def run_from_websocket(self, ws_url: str = "ws://localhost:8000/ws"):
        logger.info("PolymarketBot connecting to %s", ws_url)
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        logger.info("Connected")
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._tick(json.loads(msg.data))
            except Exception as exc:
                logger.warning("WebSocket disconnected: %s — reconnecting in 5s", exc)
                await asyncio.sleep(5)

    # ── Balance ───────────────────────────────────────────────

    async def get_usdc_balance(self) -> float:
        try:
            padded  = self._address[2:].lower().zfill(64)
            payload = {
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": _USDC_CONTRACT, "data": f"0x{_BALANCE_OF_SIG}{padded}"}, "latest"],
                "id": 1,
            }
            async with httpx.AsyncClient(timeout=8) as client:
                resp   = await client.post(_POLYGON_RPC, json=payload)
                result = resp.json().get("result", "0x0")
                return int(result, 16) / 1e6
        except Exception as exc:
            logger.warning("Balance fetch failed: %s", exc)
            return 0.0

    # ── Tick ──────────────────────────────────────────────────

    async def _tick(self, state: Dict):
        window_start = state.get("window_start_time")
        if window_start is None or window_start == self._last_bar:
            return

        if state.get("pending_deepseek_ready"):
            pred   = state.get("pending_deepseek_prediction") or {}
            signal = pred.get("signal")
            if signal in ("UP", "DOWN"):
                slug = pred.get("polymarket_url", "").split("/")[-1]
                await self._enter_trade(
                    window_start       = window_start,
                    signal             = signal,
                    confidence         = pred.get("confidence", 0),
                    reasoning          = pred.get("reasoning", ""),
                    window_start_price = state.get("window_start_price"),
                    slug               = slug,
                )
                self._last_bar = window_start

    # ── Order ─────────────────────────────────────────────────

    async def _enter_trade(
        self,
        window_start: float,
        signal: str,
        confidence: int,
        reasoning: str,
        window_start_price: Optional[float],
        slug: str,
    ):
        if window_start in self._open_trades:
            return

        balance = await self.get_usdc_balance()
        if balance < MIN_TRADE_USDC:
            logger.warning("SKIP — balance $%.2f below minimum $%.2f", balance, MIN_TRADE_USDC)
            return

        trade = {
            "window_start":     window_start,
            "signal":           signal,
            "confidence":       confidence,
            "reasoning":        reasoning[:200],
            "entry_price":      window_start_price,
            "trade_size_usdc":  self.trade_size,
            "balance_at_entry": balance,
            "slug":             slug,
            "entered_at":       time.time(),
        }

        success = await self._place_order(trade, slug, signal)
        if success:
            self._open_trades[window_start] = trade

    async def _place_order(self, trade: Dict, slug: str, signal: str) -> bool:
        if not LIVE_TRADING_ENABLED:
            logger.info("TRADING DISABLED | would place: %s $%.2f on %s",
                        signal, trade["trade_size_usdc"], slug)
            _append_ndjson(_LIVE_FILE, {**trade, "status": "disabled"})
            return False

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import OrderArgs, OrderType
        except ImportError:
            logger.error("py-clob-client not installed — run: pip install py-clob-client")
            return False

        try:
            token_id = await self._get_token_id(slug, signal)
            if not token_id:
                logger.warning("No token_id for slug=%s signal=%s", slug, signal)
                _append_ndjson(_LIVE_FILE, {**trade, "status": "error", "error": "token_id not found"})
                return False

            client = ClobClient(_CLOB_HOST, key=self._private_key, chain_id=137)
            creds  = client.create_or_derive_api_creds()
            if creds is None:
                raise RuntimeError("Failed to obtain CLOB API credentials")
            client.set_api_creds(creds)
            shares     = round(trade["trade_size_usdc"] / LIMIT_PRICE, 2)
            order_args = OrderArgs(
                token_id   = token_id,
                price      = LIMIT_PRICE,
                size       = shares,          # shares = USDC / price
                side       = "BUY",
                expiration = int(trade["window_start"] + 300),  # GTD: expires at bar close
            )
            signed   = client.create_order(order_args)
            resp     = client.post_order(signed, OrderType.GTD)
            order_id = resp.get("orderID") or resp.get("id", "unknown")

            logger.info("ORDER PLACED | %s | $%.2f | limit=%.2f | shares=%.4f | order_id=%s",
                        signal, trade["trade_size_usdc"], LIMIT_PRICE, shares, order_id)
            _append_ndjson(_LIVE_FILE, {**trade, "status": "placed", "order_id": order_id, "token_id": token_id})
            return True

        except Exception as exc:
            logger.error("Order FAILED: %s", exc)
            _append_ndjson(_LIVE_FILE, {**trade, "status": "error", "error": str(exc)})
            return False

    async def _get_token_id(self, slug: str, signal: str) -> Optional[str]:
        """
        Gamma API returns clobTokenIds as ["up_token_id", "down_token_id"]
        matching outcomes ["Up", "Down"] at the same index.
        """
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp   = await client.get(f"{_GAMMA_HOST}/markets?slug={slug}")
                data   = resp.json()
                market = data[0] if isinstance(data, list) and data else {}
                outcomes  = json.loads(market.get("outcomes") or "[]")
                token_ids = json.loads(market.get("clobTokenIds") or "[]")
                if not outcomes or not token_ids:
                    return None
                target = "Up" if signal == "UP" else "Down"
                idx    = outcomes.index(target) if target in outcomes else -1
                if 0 <= idx < len(token_ids):
                    return token_ids[idx]
        except Exception as exc:
            logger.warning("get_token_id failed: %s", exc)
        return None


# ── Standalone entry point ────────────────────────────────────

async def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ws",   default="ws://localhost:8000/ws")
    parser.add_argument("--size", type=float, default=FIXED_TRADE_USDC,
                        help="Fixed USDC per trade (min $5)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bot = PolymarketBot(trade_size=args.size)
    bal = await bot.get_usdc_balance()
    print(f"Wallet balance: ${bal:.2f} USDC")
    print(f"Trade size: ${args.size:.2f} per trade (fixed)")
    print(f"Connecting to {args.ws}\nCtrl+C to stop\n")
    await bot.run_from_websocket(args.ws)


if __name__ == "__main__":
    asyncio.run(_main())
