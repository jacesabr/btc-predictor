"""
Market Microstructure Signals
==============================
Fetches live market-microstructure signals in parallel from public APIs.
Used at every 5-minute bar open to give the AI full market context.

Sources: Kraken, OKX, Bybit, Deribit, Fear&Greed, Mempool,
         CoinGecko, Coinalyze (optional).

Public exports:
  fetch_dashboard_signals(coinalyze_key) -> Dict
  extract_signal_directions(ds)          -> Dict[str, "UP"|"DOWN"|"NEUTRAL"]
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

_TIMEOUT = aiohttp.ClientTimeout(total=9)


# ─────────────────────────────────────────────────────────────
# Source attribution per dashboard signal.
#   - `api`:     the raw API endpoint that produced each number
#   - `chart`:   a human-viewable chart/page a trader can cross-check
#   - `scope`:   short descriptor of what the number covers (venue/product/time)
# This is read by the prompt builder in ai.py to cite sources inline, and
# returned in fetch_dashboard_signals() so the Vercel frontend can render a
# source link next to each pill.
# ─────────────────────────────────────────────────────────────
SIGNAL_SOURCES: Dict[str, Dict[str, str]] = {
    "order_book": {
        "api":    "Binance /api/v3/depth + Binance futures /fapi/v1/depth + Bybit /v5/market/orderbook + Coinbase /products/BTC-USD/book + Kraken /0/public/Depth",
        "chart":  "https://www.tradingview.com/symbols/BTCUSD/",
        "scope":  "Aggregate BTC resting liquidity within 0.25% / 0.5% / 1.0% of mid across up to 5 venues",
    },
    "long_short": {
        "api":    "https://fapi.binance.com/futures/data/globalLongShortAccountRatio + topLongShortAccountRatio",
        "chart":  "https://www.coinglass.com/LongShortRatio",
        "scope":  "Binance USD-M futures, account-count based — 'top' = top 20% by margin. Binance-only, retail-only.",
    },
    "taker_flow": {
        "api":    "https://fapi.binance.com/futures/data/takerlongshortRatio",
        "chart":  "https://www.coinglass.com/BitcoinTakerBuySellVolume",
        "scope":  "Binance USD-M perp BTCUSDT, 5-minute aggressor volume (BTC).",
    },
    "oi_funding": {
        "api":    "https://fapi.binance.com/fapi/v1/openInterest + premiumIndex",
        "chart":  "https://www.coinglass.com/BitcoinOpenInterest",
        "scope":  "Binance USD-M perp only (~25% of global BTC futures OI).",
    },
    "oi_velocity": {
        "api":    "https://fapi.binance.com/futures/data/openInterestHist",
        "chart":  "https://www.coinglass.com/BitcoinOpenInterest",
        "scope":  "Binance OI change rate over 30m (6 × 5-min bars).",
    },
    "coinalyze": {
        "api":    "https://api.coinalyze.net/v1/funding-rate",
        "chart":  "https://coinalyze.net/bitcoin/funding-rate/",
        "scope":  "Cross-exchange aggregate funding rate (Coinalyze-derived).",
    },
    "coinalyze_aggregate": {
        "api":    "https://api.coinalyze.net/v1/open-interest-history + liquidation-history",
        "chart":  "https://coinalyze.net/bitcoin/open-interest/",
        "scope":  "Aggregated OI + liquidations across 7 major BTC perpetual venues (Coinalyze).",
    },
    "liquidations": {
        "api":    "https://www.okx.com/api/v5/public/liquidation-orders (mgnMode=cross, BTC-USDT-SWAP)",
        "chart":  "https://www.coinglass.com/LiquidationData",
        "scope":  "OKX cross-margin perp, last 5 min. USD = sz × ctVal (0.01) × bkPx.",
    },
    "bybit_liquidations": {
        "api":    "https://www.okx.com/api/v5/public/liquidation-orders (mgnMode=isolated, BTC-USDT-SWAP)",
        "chart":  "https://www.coinglass.com/LiquidationData",
        "scope":  "OKX isolated-margin perp, last 15 min.",
    },
    "coinglass_liquidations": {
        "api":    "https://open-api.coinglass.com/api/futures/liquidation/aggregated-history",
        "chart":  "https://www.coinglass.com/LiquidationData",
        "scope":  "CoinGlass cross-exchange aggregated liquidations, 5-min bars.",
    },
    "fear_greed": {
        "api":    "https://api.alternative.me/fng/",
        "chart":  "https://alternative.me/crypto/fear-and-greed-index/",
        "scope":  "DAILY macro sentiment (volatility 25% + volume 25% + social 15% + dominance 10% + google 10%).",
    },
    "mempool": {
        "api":    "https://mempool.space/api/v1/fees/recommended + /api/mempool",
        "chart":  "https://mempool.space/",
        "scope":  "Bitcoin network fee urgency + pending tx count.",
    },
    "coingecko": {
        "api":    "https://api.coingecko.com/api/v3/simple/price",
        "chart":  "https://www.coingecko.com/en/coins/bitcoin",
        "scope":  "24h change + market cap + volume (daily/hourly macro).",
    },
    "kraken_premium": {
        "api":    "https://api.kraken.com/0/public/Ticker vs https://www.okx.com/api/v5/market/ticker",
        "chart":  "https://www.tradingview.com/symbols/BTCUSD/?exchange=KRAKEN",
        "scope":  "Kraken spot vs OKX spot mid-price spread %.",
    },
    "spot_whale_flow": {
        "api":    "https://api.binance.com/api/v3/aggTrades + https://api.exchange.coinbase.com/products/BTC-USD/trades + https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist",
        "chart":  "https://www.coinglass.com/WhaleAlert",
        "scope":  "3-venue spot aggTrades ≥0.5 BTC, fixed 5-min window.",
    },
    "bybit_funding": {
        "api":    "https://api.bybit.com/v5/market/tickers",
        "chart":  "https://www.coinglass.com/FundingRate",
        "scope":  "Bybit USD-M perp funding rate.",
    },
    "okx_funding": {
        "api":    "https://www.okx.com/api/v5/public/funding-rate",
        "chart":  "https://www.coinglass.com/FundingRate",
        "scope":  "OKX USD-M perp funding rate.",
    },
    "btc_dominance": {
        "api":    "https://api.coingecko.com/api/v3/global",
        "chart":  "https://www.coingecko.com/en/global-charts",
        "scope":  "BTC market cap % of total crypto (hourly macro).",
    },
    "top_position_ratio": {
        "api":    "https://fapi.binance.com/futures/data/topLongShortPositionRatio",
        "chart":  "https://www.binance.com/en/futures/funding-history/perpetual/trading-data",
        "scope":  "Binance-only. Top 20% of accounts by margin, position-notional-weighted.",
    },
    "funding_trend": {
        "api":    "https://fapi.binance.com/fapi/v1/fundingRate",
        "chart":  "https://www.coinglass.com/FundingRate",
        "scope":  "Binance funding history, 6-period moving average + trend.",
    },
    "deribit_dvol": {
        "api":    "https://www.deribit.com/api/v2/public/get_index_price?index_name=btcdvol_usdc",
        "chart":  "https://www.deribit.com/statistics/BTC/volatility-index/",
        "scope":  "Deribit DVOL: 30-day forward annualized implied volatility %.",
    },
    "deribit_options": {
        "api":    "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option",
        "chart":  "https://www.deribit.com/options/BTC/",
        "scope":  "Deribit BTC options: P/C OI ratio + max pain across all expiries.",
    },
    "deribit_skew_term": {
        "api":    "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option",
        "chart":  "https://www.deribit.com/options/BTC/",
        "scope":  "Deribit BTC: 25Δ risk reversal (30d) + ATM IV term structure (7d/30d/90d) + P/C VOLUME (today).",
    },
    "spot_perp_basis": {
        "api":    "https://api.binance.com/api/v3/ticker/bookTicker + https://fapi.binance.com/fapi/v1/premiumIndex",
        "chart":  "https://www.coinglass.com/Basis",
        "scope":  "Binance perp mark minus Binance spot mid, as % of spot.",
    },
    "cvd": {
        "api":    "https://fapi.binance.com/fapi/v1/klines + https://api.binance.com/api/v3/klines (5m, 12 bars, takerBuyBaseAssetVolume)",
        "chart":  "https://www.tradingview.com/script/i6V0bC8v-Volume-Delta-CVD/",
        "scope":  "Cumulative (taker-buy − taker-sell) over last 12 5m bars, split by spot vs perp.",
    },
    "btc_onchain": {
        "api":    "https://api.bitcoin-data.com/v1/sopr + mvrv-zscore",
        "chart":  "https://bitcoin-data.com/",
        "scope":  "DAILY on-chain macro: SOPR (profit/loss spending) + MVRV Z-Score (over/undervaluation).",
    },
}


def _attach_source(payload: Optional[Dict], key: str) -> Optional[Dict]:
    """Attach SIGNAL_SOURCES[key] to a fetcher's output dict (no-op if None)."""
    if not isinstance(payload, dict):
        return payload
    src = SIGNAL_SOURCES.get(key)
    if src:
        payload["source"] = src
    return payload


async def _get(url: str, headers: Optional[Dict] = None) -> Any:
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(connector=connector, timeout=_TIMEOUT) as session:
        async with session.get(url, headers=headers or {}) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} from {url}")
            return await resp.json(content_type=None)


def _band_depth(bids: list, asks: list, mid: float, pct: float) -> tuple:
    """Return (bid_btc, ask_btc) resting within ±pct of mid."""
    lo = mid * (1 - pct / 100.0)
    hi = mid * (1 + pct / 100.0)
    bid_btc = sum(float(b[1]) for b in bids if float(b[0]) >= lo)
    ask_btc = sum(float(a[1]) for a in asks if float(a[0]) <= hi)
    return bid_btc, ask_btc


async def _fetch_venue_book(venue: str) -> Optional[Dict]:
    """Pull a single venue's full book, return bids/asks/mid or None on failure."""
    try:
        if venue == "binance_spot":
            d = await _get("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5000")
            bids = [(float(b[0]), float(b[1])) for b in d.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in d.get("asks", [])]
        elif venue == "binance_perp":
            d = await _get("https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000")
            bids = [(float(b[0]), float(b[1])) for b in d.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in d.get("asks", [])]
        elif venue == "bybit_spot":
            d = await _get("https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200")
            result = d.get("result") or {}
            bids = [(float(b[0]), float(b[1])) for b in result.get("b", [])]
            asks = [(float(a[0]), float(a[1])) for a in result.get("a", [])]
        elif venue == "coinbase":
            d = await _get("https://api.exchange.coinbase.com/products/BTC-USD/book?level=2")
            bids = [(float(b[0]), float(b[1])) for b in d.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in d.get("asks", [])]
        elif venue == "kraken":
            d = await _get("https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=500")
            book = (d.get("result") or {}).get("XXBTZUSD", {})
            bids = [(float(b[0]), float(b[1])) for b in book.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in book.get("asks", [])]
        else:
            return None
        if not bids or not asks:
            return None
        mid = (bids[0][0] + asks[0][0]) / 2.0
        return {"venue": venue, "bids": bids, "asks": asks, "mid": mid}
    except Exception as e:
        logger.debug("Depth fetch failed for %s: %s", venue, e)
        return None


async def _fetch_order_book() -> Dict:
    """
    Aggregate bid/ask depth within %-bands of mid across 5 venues.

    Replaces the prior "top-20-levels on Kraken" approach, which produced
    sub-10-BTC "bid walls" over ~$10 price spans — not a bid wall by any
    trading definition. The band-based calc is venue-agnostic and matches
    how traders think about resting liquidity.
    """
    venues = ["binance_spot", "binance_perp", "bybit_spot", "coinbase", "kraken"]
    books  = await asyncio.gather(*(_fetch_venue_book(v) for v in venues))
    books  = [b for b in books if b is not None]

    if not books:
        return {
            "signal":         "UNAVAILABLE",
            "data_available": False,
            "interpretation": "Order book depth unavailable — all venue fetches failed.",
        }

    # Cross-venue mid = weighted average of top-of-book mids
    agg_mid = sum(b["mid"] for b in books) / len(books)

    # Per-venue band depth at 0.25%, 0.5%, 1.0%
    per_venue: Dict[str, Dict] = {}
    tot_bid_025 = tot_ask_025 = 0.0
    tot_bid_05  = tot_ask_05  = 0.0
    tot_bid_1   = tot_ask_1   = 0.0
    for b in books:
        mid = b["mid"]
        bb025, aa025 = _band_depth(b["bids"], b["asks"], mid, 0.25)
        bb05,  aa05  = _band_depth(b["bids"], b["asks"], mid, 0.5)
        bb1,   aa1   = _band_depth(b["bids"], b["asks"], mid, 1.0)
        per_venue[b["venue"]] = {
            "mid":          round(mid, 2),
            "bid_025_btc":  round(bb025, 2), "ask_025_btc": round(aa025, 2),
            "bid_05_btc":   round(bb05,  2), "ask_05_btc":  round(aa05,  2),
            "bid_1_btc":    round(bb1,   2), "ask_1_btc":   round(aa1,   2),
        }
        tot_bid_025 += bb025; tot_ask_025 += aa025
        tot_bid_05  += bb05;  tot_ask_05  += aa05
        tot_bid_1   += bb1;   tot_ask_1   += aa1

    imb_05 = ((tot_bid_05 - tot_ask_05) / (tot_bid_05 + tot_ask_05)) * 100 if (tot_bid_05 + tot_ask_05) > 0 else 0.0
    imb_1  = ((tot_bid_1  - tot_ask_1)  / (tot_bid_1  + tot_ask_1))  * 100 if (tot_bid_1  + tot_ask_1)  > 0 else 0.0

    # Signal based on 0.5% band (the conventional "immediate defense" zone).
    # Threshold ±8% imbalance — tighter than the old ±5% because a 5-venue
    # aggregate is MUCH less noisy than a 20-level single-venue snapshot.
    sig = "BULLISH" if imb_05 > 8 else "BEARISH" if imb_05 < -8 else "NEUTRAL"
    venue_str = f"across {len(books)} venues ({', '.join(b['venue'] for b in books)})"
    interp = (
        f"Aggregate bid-heavy book {venue_str}: {tot_bid_05:.1f} BTC bids vs {tot_ask_05:.1f} BTC asks "
        f"within 0.5% of ${agg_mid:,.0f} ({imb_05:+.1f}%). "
        "Real resting demand — passive buyers defending this zone."
        if imb_05 > 8 else
        f"Aggregate ask-heavy book {venue_str}: {tot_bid_05:.1f} BTC bids vs {tot_ask_05:.1f} BTC asks "
        f"within 0.5% of ${agg_mid:,.0f} ({imb_05:+.1f}%). "
        "Real resting supply capping immediate rallies."
        if imb_05 < -8 else
        f"Balanced aggregate book ({tot_bid_05:.1f} bid / {tot_ask_05:.1f} ask BTC within 0.5% "
        f"of ${agg_mid:,.0f}, {imb_05:+.1f}%). No clear resting-liquidity edge — defer to taker flow."
    )

    return {
        # ── Primary aggregated fields (the numbers traders actually want) ──
        "mid_usd":               round(agg_mid, 2),
        "bid_depth_025pct_btc":  round(tot_bid_025, 2),
        "ask_depth_025pct_btc":  round(tot_ask_025, 2),
        "bid_depth_05pct_btc":   round(tot_bid_05,  2),
        "ask_depth_05pct_btc":   round(tot_ask_05,  2),
        "bid_depth_1pct_btc":    round(tot_bid_1,   2),
        "ask_depth_1pct_btc":    round(tot_ask_1,   2),
        "imbalance_05pct_pct":   round(imb_05, 2),
        "imbalance_1pct_pct":    round(imb_1,  2),
        "venues_included":       [b["venue"] for b in books],
        "per_venue":             per_venue,
        # ── Back-compat aliases: repoint to aggregated 0.5%-band numbers so
        #     downstream code (prompt builder, frontend) works unchanged ──
        "bid_vol_btc":           round(tot_bid_05, 2),
        "ask_vol_btc":           round(tot_ask_05, 2),
        "imbalance_pct":         round(imb_05, 2),
        "signal":                sig,
        "data_available":        True,
        "interpretation":        interp,
    }


async def _fetch_long_short() -> Dict:
    try:
        gl, tp = await asyncio.gather(
            _get("https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
                 "?symbol=BTCUSDT&period=5m&limit=1"),
            _get("https://fapi.binance.com/futures/data/topLongShortAccountRatio"
                 "?symbol=BTCUSDT&period=5m&limit=1"),
        )
        g   = (gl[0]  if gl  else {})
        tp0 = (tp[0]  if tp  else {})
        lsr  = float(g.get("longShortRatio", 1.0))
        lp   = float(g.get("longAccount",    0.5)) * 100
        sp   = 100.0 - lp
        tlp  = float(tp0.get("longAccount", 0.5)) * 100
        tsp  = 100.0 - tlp
    except Exception as _e:
        logger.debug("Binance long/short failed, using Bybit fallback: %s", _e)
        data = await _get(
            "https://api.bybit.com/v5/market/account-ratio"
            "?category=linear&symbol=BTCUSDT&period=5min&limit=1"
        )
        row  = ((data.get("result") or {}).get("list") or [{}])[0]
        lp   = float(row.get("buyRatio",  0.5)) * 100
        sp   = 100.0 - lp
        lsr  = lp / sp if sp > 0 else 1.0
        tlp  = lp
        tsp  = sp
    div  = lp - tlp
    # Binance's "global" vs "top" long/short ratios are BOTH account-based and
    # BOTH reflect retail users on Binance — not smart money, institutions, or
    # whales. "Top" = top 20% of accounts by margin balance on this symbol, i.e.
    # the largest retail accounts, still Binance-only, still unweighted by size.
    r_sig = (
        "BEARISH_CONTRARIAN" if lsr > 1.35 else
        "BULLISH_CONTRARIAN" if lsr < 0.75 else
        "NEUTRAL"
    )
    s_sig = "BULLISH" if tlp > 60 else "BEARISH" if tlp < 40 else "NEUTRAL"
    if abs(div) > 10:
        interp = (
            f"Binance top-account traders {tlp:.0f}% long vs all-account {lp:.0f}% long "
            f"({abs(div):.1f}% divergence). Top-20%-by-margin accounts diverge from the broad "
            f"account base — note this is Binance-only retail stratification, not institutional flow."
        )
    else:
        interp = (
            f"Binance account tiers aligned ({abs(div):.1f}% diff). "
            f"Top accounts {tlp:.0f}% long / all accounts {lp:.0f}% long — "
            f"no stratification signal within Binance retail."
        )
    return {
        "retail_lsr":                    round(lsr, 4),
        "retail_long_pct":               round(lp,  1),
        "retail_short_pct":              round(sp,  1),
        "top_accounts_long_pct":         round(tlp, 1),
        "top_accounts_short_pct":        round(tsp, 1),
        "retail_signal_contrarian":      r_sig,
        "top_accounts_signal":           s_sig,
        "top_vs_all_div_pct":            round(div, 1),
        # Backwards-compat keys (prompt builder / frontend still read these).
        # TODO Phase 7: drop aliases after frontend label audit.
        "smart_money_long_pct":          round(tlp, 1),
        "smart_money_short_pct":         round(tsp, 1),
        "smart_money_signal":            s_sig,
        "smart_vs_retail_div_pct":       round(div, 1),
        "interpretation":                interp,
    }


async def _fetch_taker_flow() -> Dict:
    try:
        data = await _get(
            "https://fapi.binance.com/futures/data/takerlongshortRatio"
            "?symbol=BTCUSDT&period=5m&limit=3"
        )
        latest = data[-1] if data else {}
        bsr = float(latest.get("buySellRatio", 1.0))
        bv  = float(latest.get("buyVol",  0.0))
        sv  = float(latest.get("sellVol", 0.0))
        _data_for_trend = data
    except Exception as _e:
        logger.debug("Binance taker flow failed, using OKX fallback: %s", _e)
        try:
            raw = await _get(
                "https://www.okx.com/api/v5/rubik/stat/taker-volume"
                "?ccy=BTC&instType=SWAP&period=5m&limit=3"
            )
            rows = raw.get("data") or []
            if not rows:
                raise ValueError("Empty OKX taker volume response")
            bv, sv = float(rows[0][1]), float(rows[0][2])
            bsr = bv / sv if sv > 0 else 1.0
            _data_for_trend = [
                {"buySellRatio": float(r[1]) / float(r[2]) if float(r[2]) > 0 else 1.0}
                for r in reversed(rows)
            ]
        except Exception as _e2:
            # Binance + OKX failed — try Kraken's public Trades endpoint as a last
            # resort. Kraken spot is reachable from Render (where Binance/OKX are
            # geo-blocked). We aggregate taker buy/sell volume from the last 5 min
            # of trades (side flag "b"=buy taker, "s"=sell taker).
            logger.debug("Binance+OKX taker flow failed, trying Kraken: %s", _e2)
            try:
                import time as _time
                since_ns = int((_time.time() - 360) * 1e9)   # last ~6 min, safety window
                raw = await _get(f"https://api.kraken.com/0/public/Trades?pair=XBTUSD&since={since_ns}")
                result = raw.get("result") or {}
                trades = result.get("XXBTZUSD") or result.get("XBTUSD") or []
                if not trades:
                    raise ValueError("empty Kraken trades response")
                cutoff = _time.time() - 300   # strict 5-min window for aggregation
                bv = sum(float(t[1]) for t in trades if float(t[2]) >= cutoff and t[3] == "b")
                sv = sum(float(t[1]) for t in trades if float(t[2]) >= cutoff and t[3] == "s")
                if bv + sv < 0.01:
                    raise ValueError("Kraken trades volume too small to be meaningful")
                bsr = bv / sv if sv > 0 else 1.0
                # No 3-bar trend from single Trades call — flag as SINGLE_BAR
                _data_for_trend = []
                logger.info("Taker flow from Kraken fallback: BSR=%.3f buy=%.1f sell=%.1f BTC", bsr, bv, sv)
            except Exception as _e3:
                logger.warning("Taker flow fetch failed on Binance, OKX, and Kraken: %s", _e3)
                return {
                    "signal":            "UNAVAILABLE",
                    "data_available":    False,
                    "interpretation":    "Taker flow data unavailable — Binance, OKX, and Kraken fetches all failed.",
                }
    sig = "BULLISH" if bsr > 1.12 else "BEARISH" if bsr < 0.90 else "NEUTRAL"
    if len(_data_for_trend) >= 3:
        ratios  = [float(d.get("buySellRatio", 1)) for d in _data_for_trend]
        min_chg = 0.02
        rising  = (ratios[-1] > ratios[-2] * (1 + min_chg) and
                   ratios[-2] > ratios[-3] * (1 + min_chg))
        falling = (ratios[-1] < ratios[-2] * (1 - min_chg) and
                   ratios[-2] < ratios[-3] * (1 - min_chg))
        trend = "ACCELERATING_BULLISH" if rising else "ACCELERATING_BEARISH" if falling else "MIXED"
    else:
        trend = "INSUFFICIENT_DATA"
    interp = (
        f"Aggressive buyers crossing the ask (BSR={bsr:.3f}). "
        "Real directional conviction — participants paying premium to execute long."
        if bsr > 1.12 else
        f"Aggressive sellers hitting the bid (BSR={bsr:.3f}). "
        "Bearish momentum confirmed by actual execution pressure."
        if bsr < 0.90 else
        f"Balanced aggressor flow (BSR={bsr:.3f}). Neither side willing to pay spread. "
        "Chop or reversal environment — rely on order-book and L/S signals."
    )
    return {
        "buy_sell_ratio":    round(bsr, 4),
        "taker_buy_vol_btc": round(bv,  1),
        "taker_sell_vol_btc":round(sv,  1),
        "signal":            sig,
        "data_available":    True,
        "trend_3bars":       trend,
        "interpretation":    interp,
    }


async def _fetch_oi_funding() -> Dict:
    try:
        oi, pi = await asyncio.gather(
            _get("https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"),
            _get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"),
        )
        oiv  = float(oi.get("openInterest", 0))
        fr   = float(pi.get("lastFundingRate", 0))
        mp   = float(pi.get("markPrice",  0))
        ip   = float(pi.get("indexPrice", 0))
        prem = ((mp - ip) / ip) * 100 if ip else 0.0
        ntf  = pi.get("nextFundingTime", 0)
    except Exception as _e:
        logger.debug("Binance OI/funding failed, using OKX fallback: %s", _e)
        oi_r, fr_r, mk_r, ix_r = await asyncio.gather(
            _get("https://www.okx.com/api/v5/public/open-interest"
                 "?instType=SWAP&instId=BTC-USDT-SWAP"),
            _get("https://www.okx.com/api/v5/public/funding-rate"
                 "?instId=BTC-USDT-SWAP"),
            _get("https://www.okx.com/api/v5/public/mark-price"
                 "?instType=SWAP&instId=BTC-USDT-SWAP"),
            _get("https://www.okx.com/api/v5/market/index-tickers"
                 "?instId=BTC-USDT"),
        )
        oi_row = (oi_r.get("data") or [{}])[0]
        fr_row = (fr_r.get("data") or [{}])[0]
        mk_row = (mk_r.get("data") or [{}])[0]
        ix_row = (ix_r.get("data") or [{}])[0]
        oiv  = float(oi_row.get("oiCcy") or oi_row.get("oi") or 0)
        fr   = float(fr_row.get("fundingRate", 0))
        mp   = float(mk_row.get("markPx", 0))
        ip   = float(ix_row.get("idxPx", 0))
        prem = ((mp - ip) / ip) * 100 if ip else 0.0
        ntf  = int(fr_row.get("nextFundingTime", 0))
    fr_sig  = "BEARISH" if fr > 0.0006 else "BULLISH" if fr < 0 else "NEUTRAL"
    p_sig   = "BEARISH" if prem > 0.03 else "BULLISH" if prem < -0.03 else "NEUTRAL"
    return {
        "open_interest_btc":         round(oiv, 1),
        "funding_rate_8h_pct":       round(fr * 100, 5),
        "mark_price":                round(mp, 2),
        "index_price":               round(ip, 2),
        "mark_premium_vs_index_pct": round(prem, 4),
        "next_funding_time_ms":      ntf,
        "funding_signal":            fr_sig,
        "premium_signal":            p_sig,
    }


async def _fetch_liquidations() -> Dict:
    fetch_failed = False
    try:
        data = await _get(
            "https://www.okx.com/api/v5/public/liquidation-orders"
            "?instType=SWAP&mgnMode=cross&instId=BTC-USDT-SWAP&state=filled&limit=100"
        )
    except Exception as _e:
        logger.warning("OKX liquidations fetch failed: %s", _e)
        data = {}
        fetch_failed = True
    rows = []
    for event in (data.get("data") or []):
        for detail in (event.get("details") or []):
            rows.append(detail)
    if not rows:
        # If the fetch itself failed, this "no rows" is data-gap, not observed
        # calm — never tell the model "stable market, no cascades" when we
        # couldn't check. Prompt builder should omit the section entirely.
        if fetch_failed:
            return {
                "signal":         "UNAVAILABLE",
                "data_available": False,
                "interpretation": "Liquidation data unavailable — OKX fetch failed.",
            }
        return {
            "total": 0, "long_liq_count": 0, "short_liq_count": 0,
            "long_liq_usd": 0, "short_liq_usd": 0,
            "velocity_per_min": 0.0,
            "signal": "NEUTRAL",
            "data_available": True,
            "interpretation": "No recent liquidations — stable market, no cascades detected.",
        }
    now_ms = time.time() * 1000
    cutoff = now_ms - 300_000
    recent = [r for r in rows if float(r.get("ts", 0)) >= cutoff]
    window = recent if recent else rows
    # OKX: posSide="long" + side="sell" → forced long liquidation (bearish)
    #       posSide="short" + side="buy" → forced short liquidation (bullish squeeze)
    # BTC-USDT-SWAP ctVal=0.01 BTC per contract (sz is in CONTRACTS, not BTC).
    # USD notional = sz * ctVal * bkPx.
    OKX_CT_VAL_BTC = 0.01
    longs  = [r for r in window if r.get("posSide", "").lower() == "long"]
    shorts = [r for r in window if r.get("posSide", "").lower() == "short"]
    lvol   = sum(float(r.get("sz", 0)) * OKX_CT_VAL_BTC * float(r.get("bkPx", 0)) for r in longs)
    svol   = sum(float(r.get("sz", 0)) * OKX_CT_VAL_BTC * float(r.get("bkPx", 0)) for r in shorts)
    if recent and len(recent) >= 2:
        times    = sorted(float(r.get("ts", 0)) for r in recent if r.get("ts"))
        span_min = max((times[-1] - times[0]) / 60_000, 0.1)
        velocity = round(len(recent) / span_min, 1)
    else:
        velocity = round(len(window) / 5.0, 1)
    sig    = "BEARISH" if lvol > svol * 1.5 else "BULLISH" if svol > lvol * 1.5 else "NEUTRAL"
    prices = [float(r.get("bkPx", 0)) for r in window if r.get("bkPx")]
    p_range = (f"${min(prices):,.0f}-${max(prices):,.0f}" if prices else "N/A")
    cascade_note = (
        f" CASCADE ACTIVE ({velocity:.0f}/min — extremely high velocity)."
        if velocity >= 20 else
        f" Elevated cascade velocity ({velocity:.0f}/min)."
        if velocity >= 5 else ""
    )
    interp = (
        f"Long cascade — {len(longs)} forced sells in last 5min, ${lvol:,.0f} liquidated.{cascade_note} "
        "Longs being unwound — mechanical selling pressure until liq pool is exhausted."
        if lvol > svol * 1.5 else
        f"Short squeeze — {len(shorts)} shorts force-covered, ${svol:,.0f} buying.{cascade_note} "
        "Shorts liquidated = forced buyers — upside spike until squeeze exhausts."
        if svol > lvol * 1.5 else
        f"Mixed liquidations ({len(longs)} long / {len(shorts)} short, {velocity:.0f}/min). "
        "No directional cascade — market clearing both sides, no strong directional bias."
    )
    return {
        "total":             len(window),
        "long_liq_count":    len(longs),
        "short_liq_count":   len(shorts),
        "long_liq_usd":      round(lvol, 0),
        "short_liq_usd":     round(svol, 0),
        "velocity_per_min":  velocity,
        "price_range":       p_range,
        "signal":            sig,
        "data_available":    True,
        "interpretation":    interp,
    }


async def _fetch_fear_greed() -> Dict:
    data  = await _get("https://api.alternative.me/fng/?limit=2")
    items = data.get("data", [])
    cur   = items[0] if items else {}
    prev  = items[1] if len(items) > 1 else {}
    v     = int(cur.get("value", 50))
    label = cur.get("value_classification", "Neutral")
    pv    = int(prev.get("value", v))
    delta = v - pv
    sig   = "BULLISH_CONTRARIAN" if v < 30 else "BEARISH_CONTRARIAN" if v > 75 else "NEUTRAL"
    # Fear & Greed is a DAILY index built from 30/90-day volatility, volume,
    # social, dominance, Google-trends inputs — it barely moves bar-to-bar and
    # is macro context, not a 5-min signal. Do not phrase it as actionable.
    interp = (
        f"Extreme Fear ({v}, daily macro). Historically precedes multi-day bounces; "
        "weak directional edge at 5-min horizon. Use as regime context."
        if v < 30 else
        f"Extreme Greed ({v}, daily macro). Elevated reversal risk over days — "
        "not a 5-min trigger. Use as regime context."
        if v > 75 else
        f"Neutral sentiment ({v} — {label}, daily macro). "
        "Weight bar-level flow and depth signals more heavily."
    )
    return {
        "value":          v,
        "label":          label,
        "previous_day":   pv,
        "daily_delta":    delta,
        "signal":         sig,
        "interpretation": interp,
    }


async def _fetch_mempool() -> Dict:
    fees, mp = await asyncio.gather(
        _get("https://mempool.space/api/v1/fees/recommended"),
        _get("https://mempool.space/api/mempool"),
    )
    ff    = fees.get("fastestFee",  0)
    hf    = fees.get("halfHourFee", 0)
    of_   = fees.get("hourFee",     0)
    count = mp.get("count", 0)
    vsize = mp.get("vsize",  0)
    sig   = "BEARISH" if ff > 50 else "BULLISH" if ff < 10 else "NEUTRAL"
    interp = (
        f"High fee urgency ({ff} sat/vB) — network congested or panic exits. "
        "Users paying premium to clear on-chain. Bearish stress signal."
        if ff > 50 else
        f"Very low fees ({ff} sat/vB) — calm network, no on-chain panic. "
        "Slightly positive background context."
        if ff < 10 else
        f"Normal mempool ({ff} sat/vB, {count:,} pending txs). "
        "No on-chain stress. Background context only."
    )
    return {
        "fastest_fee_sat_vb":   ff,
        "half_hour_fee_sat_vb": hf,
        "hour_fee_sat_vb":      of_,
        "pending_tx_count":     count,
        "mempool_size_mb":      round(vsize / 1e6, 2),
        "signal":               sig,
        "interpretation":       interp,
    }


async def _fetch_deribit_dvol() -> Dict:
    data = await _get(
        "https://www.deribit.com/api/v2/public/get_index_price?index_name=btcdvol_usdc"
    )
    result = data.get("result") or {}
    dvol = float(result.get("index_price", 60))
    # DVOL = annualised implied volatility %. <40 calm, 40-80 normal, >80 fear
    sig = "BEARISH" if dvol > 80 else "BULLISH" if dvol < 40 else "NEUTRAL"
    interp = (
        f"DVOL {dvol:.1f}% — extreme volatility regime. Options pricing in large moves. "
        "High IV = elevated risk, typically bearish for near-term price stability."
        if dvol > 80 else
        f"DVOL {dvol:.1f}% — calm volatility regime. Low option premiums signal complacency. "
        "Low IV historically precedes breakouts."
        if dvol < 40 else
        f"DVOL {dvol:.1f}% — normal volatility regime. No extreme option pricing in either direction."
    )
    return {
        "dvol_pct":       round(dvol, 2),
        "signal":         sig,
        "interpretation": interp,
    }


async def _fetch_kraken_premium() -> Dict:
    try:
        kraken_data, okx_data = await asyncio.gather(
            _get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD"),
            _get("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT"),
        )
        k_price = float(kraken_data["result"]["XXBTZUSD"]["c"][0])
        okx_rows = okx_data.get("data") or []
        o_price  = float(okx_rows[0]["last"]) if okx_rows else 0.0
        ref_label = "OKX"
    except Exception as _e:
        logger.debug("Kraken/OKX premium failed, using Binance fallback: %s", _e)
        binance_data, okx_data = await asyncio.gather(
            _get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"),
            _get("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT"),
        )
        k_price = float(binance_data["price"])
        okx_rows = okx_data.get("data") or []
        o_price  = float(okx_rows[0]["last"]) if okx_rows else k_price
        ref_label = "OKX"
    spread_pct = (k_price - o_price) / o_price * 100 if o_price else 0.0
    sig = "BULLISH" if spread_pct > 0.05 else "BEARISH" if spread_pct < -0.05 else "NEUTRAL"
    interp = (
        f"Kraken premium +{spread_pct:.3f}% over {ref_label}. "
        "EU/US regulated buyers paying above global average — institutional accumulation."
        if spread_pct > 0.05 else
        f"Kraken discount {spread_pct:.3f}% vs {ref_label}. "
        "Regulated market selling pressure; bearish for EU/US demand outlook."
        if spread_pct < -0.05 else
        f"Kraken/{ref_label} near-parity ({spread_pct:+.3f}%). "
        "No cross-exchange arbitrage pressure — neutral signal."
    )
    return {
        "kraken_price": round(k_price, 2),
        "okx_price":    round(o_price, 2),
        "spread_pct":   round(spread_pct, 4),
        "signal":       sig,
        "interpretation": interp,
    }


async def _fetch_spot_perp_basis() -> Dict:
    """
    Instantaneous spot-perp basis — (perp mark - spot mid) / spot, in %.

    Directly measures leverage demand. Funding rate is the fee that *closes*
    basis over 8h; basis is the current mispricing the leverage demand creates.
    Positive basis = leveraged longs paying up for exposure (bullish momentum
    but fragile); negative = leveraged shorts / spot premium (bearish or
    discount opportunity).
    """
    try:
        spot_r, perp_r = await asyncio.gather(
            _get("https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT"),
            _get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"),
        )
        spot_mid = (float(spot_r["bidPrice"]) + float(spot_r["askPrice"])) / 2.0
        perp_mark = float(perp_r["markPrice"])
    except Exception as e:
        logger.warning("Spot-perp basis fetch failed: %s", e)
        return {
            "signal":         "UNAVAILABLE",
            "data_available": False,
            "interpretation": "Spot-perp basis unavailable — Binance fetch failed.",
        }
    basis_usd = perp_mark - spot_mid
    basis_pct = (basis_usd / spot_mid) * 100 if spot_mid else 0.0
    # Thresholds: ±0.03% = typical noise; ±0.08% = meaningful leverage demand
    sig = "BULLISH" if basis_pct > 0.08 else "BEARISH" if basis_pct < -0.08 else "NEUTRAL"
    interp = (
        f"Perp trades {basis_usd:+.2f} over spot ({basis_pct:+.3f}%) — leveraged longs paying up. "
        "Leverage-driven momentum; watch for funding to cool it off."
        if basis_pct > 0.08 else
        f"Perp trades {basis_usd:+.2f} vs spot ({basis_pct:+.3f}%) — spot premium / perp discount. "
        "Either spot accumulation ahead of perp, or leverage unwind."
        if basis_pct < -0.08 else
        f"Perp ≈ spot ({basis_pct:+.3f}%, {basis_usd:+.2f} USD). "
        "No leverage-demand edge; funding and taker flow matter more."
    )
    return {
        "spot_mid":       round(spot_mid, 2),
        "perp_mark":      round(perp_mark, 2),
        "basis_usd":      round(basis_usd, 2),
        "basis_pct":      round(basis_pct, 4),
        "signal":         sig,
        "data_available": True,
        "interpretation": interp,
    }


async def _fetch_cvd() -> Dict:
    """
    Cumulative Volume Delta over the last 12 5-min bars, for both spot and
    perp. CVD = Σ(taker_buy_vol - taker_sell_vol). Divergences between price
    and CVD are classic reversal tells; divergences between spot and perp
    CVD isolate institutional spot buys vs leveraged perp pressure.

    Uses the `takerBuyBaseAssetVolume` field from Binance klines (unambiguously
    in BTC — cross-verified against takerlongshortRatio buyVol).
    """
    try:
        perp_k, spot_k = await asyncio.gather(
            _get("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=12"),
            _get("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=12"),
        )
    except Exception as e:
        logger.warning("CVD fetch failed: %s", e)
        return {
            "signal":         "UNAVAILABLE",
            "data_available": False,
            "interpretation": "CVD unavailable — Binance klines fetch failed.",
        }

    def _cvd_sum(klines: list) -> Dict:
        # Kline fields: [openTime, open, high, low, close, volume, closeTime,
        #                quoteVol, trades, takerBuyBaseVol, takerBuyQuoteVol, _]
        per_bar = []
        cumulative = 0.0
        for k in klines:
            vol       = float(k[5])
            taker_buy = float(k[9])
            taker_sell = vol - taker_buy
            delta     = taker_buy - taker_sell
            cumulative += delta
            per_bar.append(delta)
        return {
            "cvd_total_btc":   cumulative,
            "last_bar_btc":    per_bar[-1] if per_bar else 0.0,
            "last_3bars_btc":  sum(per_bar[-3:]) if per_bar else 0.0,
            "last_12bars_btc": cumulative,
        }

    perp_cvd = _cvd_sum(perp_k)
    spot_cvd = _cvd_sum(spot_k)
    divergence_btc = spot_cvd["cvd_total_btc"] - perp_cvd["cvd_total_btc"]

    # Price move over window for context
    try:
        p_open  = float(perp_k[0][1]) if perp_k else 0.0
        p_close = float(perp_k[-1][4]) if perp_k else 0.0
        move_pct = ((p_close - p_open) / p_open) * 100 if p_open else 0.0
    except Exception:
        move_pct = 0.0

    # Signal: bullish if aggregate CVD strongly positive, bearish if strongly negative.
    # Thresholds calibrated against typical 1h BTC flow (~500-2000 BTC of delta).
    total = perp_cvd["cvd_total_btc"] + spot_cvd["cvd_total_btc"]
    sig = "BULLISH" if total > 400 else "BEARISH" if total < -400 else "NEUTRAL"

    # Divergence flags — the classic "institutional spot buy while perp sells" setup
    div_note = ""
    if abs(divergence_btc) > 150:
        if divergence_btc > 0 and perp_cvd["cvd_total_btc"] < 0:
            div_note = (
                f" DIVERGENCE: spot CVD +{spot_cvd['cvd_total_btc']:.0f} BTC while "
                f"perp CVD {perp_cvd['cvd_total_btc']:+.0f} BTC — spot accumulation "
                "under leveraged-short pressure (classic bullish divergence setup)."
            )
        elif divergence_btc < 0 and perp_cvd["cvd_total_btc"] > 0:
            div_note = (
                f" DIVERGENCE: spot CVD {spot_cvd['cvd_total_btc']:+.0f} BTC while "
                f"perp CVD +{perp_cvd['cvd_total_btc']:.0f} BTC — spot distribution "
                "under leveraged-long pressure (classic bearish divergence setup)."
            )

    interp = (
        f"Aggregate 1h CVD +{total:.0f} BTC (spot +{spot_cvd['cvd_total_btc']:.0f}, "
        f"perp +{perp_cvd['cvd_total_btc']:.0f}) with price {move_pct:+.2f}%. "
        "Persistent buying pressure — directional conviction confirmed by actual execution."
        if total > 400 else
        f"Aggregate 1h CVD {total:.0f} BTC (spot {spot_cvd['cvd_total_btc']:+.0f}, "
        f"perp {perp_cvd['cvd_total_btc']:+.0f}) with price {move_pct:+.2f}%. "
        "Persistent selling pressure — sellers stepping through the bid."
        if total < -400 else
        f"Balanced 1h CVD ({total:+.0f} BTC) with price {move_pct:+.2f}%. "
        "No persistent directional flow."
    ) + div_note

    return {
        "perp_cvd_1h_btc":        round(perp_cvd["cvd_total_btc"],  1),
        "spot_cvd_1h_btc":        round(spot_cvd["cvd_total_btc"],  1),
        "perp_cvd_last_bar_btc":  round(perp_cvd["last_bar_btc"],   1),
        "spot_cvd_last_bar_btc":  round(spot_cvd["last_bar_btc"],   1),
        "perp_cvd_last_3bars_btc":round(perp_cvd["last_3bars_btc"], 1),
        "spot_cvd_last_3bars_btc":round(spot_cvd["last_3bars_btc"], 1),
        "aggregate_cvd_1h_btc":   round(total, 1),
        "spot_perp_divergence_btc": round(divergence_btc, 1),
        "price_move_1h_pct":      round(move_pct, 3),
        "signal":                 sig,
        "data_available":         True,
        "interpretation":         interp,
    }


async def _fetch_oi_velocity() -> Dict:
    try:
        data = await _get(
            "https://fapi.binance.com/futures/data/openInterestHist"
            "?symbol=BTCUSDT&period=5m&limit=6"
        )
        if not data or len(data) < 2:
            raise ValueError("Insufficient OI history")
        oi_vals = [float(d.get("sumOpenInterest", 0)) for d in data]
    except Exception as _e:
        logger.debug("Binance OI velocity failed, using Bybit fallback: %s", _e)
        raw = await _get(
            "https://api.bybit.com/v5/market/open-interest"
            "?category=linear&symbol=BTCUSDT&intervalTime=5min&limit=6"
        )
        rows = (raw.get("result") or {}).get("list") or []
        if len(rows) < 2:
            raise ValueError("Insufficient Bybit OI history")
        oi_vals = [float(r.get("openInterest", 0)) for r in reversed(rows)]
    oi_chg_pct = (oi_vals[-1] - oi_vals[0]) / oi_vals[0] * 100 if oi_vals[0] else 0.0
    oi_bar_chg = (oi_vals[-1] - oi_vals[-2]) / oi_vals[-2] * 100 if oi_vals[-2] else 0.0
    sig = (
        "BULLISH" if oi_chg_pct > 0.3 else
        "BEARISH" if oi_chg_pct < -0.3 else
        "NEUTRAL"
    )
    interp = (
        f"OI rising +{oi_chg_pct:.2f}% over 30min (last bar: +{oi_bar_chg:.2f}%). "
        "New positions being opened — conviction-driven entry, likely long-dominated."
        if oi_chg_pct > 0.3 else
        f"OI falling {oi_chg_pct:.2f}% over 30min (last bar: {oi_bar_chg:.2f}%). "
        "Position liquidation / de-levering — short-term bearish pressure."
        if oi_chg_pct < -0.3 else
        f"OI stable ({oi_chg_pct:+.2f}% over 30min). "
        "No significant position accumulation or exit. Follow price action."
    )
    return {
        "oi_current_btc":     round(oi_vals[-1], 1),
        "oi_change_30m_pct":  round(oi_chg_pct, 4),
        "oi_change_1bar_pct": round(oi_bar_chg, 4),
        "signal":             sig,
        "interpretation":     interp,
    }


async def _fetch_spot_whale_flow() -> Dict:
    """
    Spot whale flow across Binance spot + Coinbase + Bitfinex over a fixed
    5-min window. Threshold 0.5 BTC per trade — the prior 2 BTC threshold
    produced empty samples on Kraken (confirmed by live test).

    Uses `aggTrades` on Binance for proper 5-min coverage and per-venue trade
    feeds on Coinbase/Bitfinex. Each venue is independently fallback-tolerant.
    """
    threshold_btc = 0.5
    now_ms   = int(time.time() * 1000)
    start_ms = now_ms - 5 * 60 * 1000

    async def _binance_spot() -> tuple:
        try:
            data = await _get(
                "https://api.binance.com/api/v3/aggTrades"
                f"?symbol=BTCUSDT&startTime={start_ms}&endTime={now_ms}&limit=1000"
            )
        except Exception as e:
            logger.debug("Binance aggTrades failed: %s", e)
            return 0.0, 0.0, 0
        bv = sv = 0.0; n = 0
        for t in (data or []):
            qty = float(t.get("q", 0))
            if qty < threshold_btc: continue
            # isBuyerMaker True -> buyer was maker -> the aggressor was a seller.
            if t.get("m", False):
                sv += qty
            else:
                bv += qty
            n += 1
        return bv, sv, n

    async def _coinbase() -> tuple:
        try:
            data = await _get("https://api.exchange.coinbase.com/products/BTC-USD/trades?limit=1000")
        except Exception as e:
            logger.debug("Coinbase trades failed: %s", e)
            return 0.0, 0.0, 0
        bv = sv = 0.0; n = 0
        from datetime import datetime, timezone
        cutoff_s = start_ms / 1000
        for t in (data or []):
            try:
                qty = float(t.get("size", 0))
                ts = datetime.fromisoformat(t["time"].replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                continue
            if ts < cutoff_s: continue
            if qty < threshold_btc: continue
            # Coinbase "side" is the taker side
            if t.get("side") == "buy":
                bv += qty
            else:
                sv += qty
            n += 1
        return bv, sv, n

    async def _bitfinex() -> tuple:
        try:
            data = await _get(
                f"https://api-pub.bitfinex.com/v2/trades/tBTCUSD/hist?limit=1000&start={start_ms}&end={now_ms}"
            )
        except Exception as e:
            logger.debug("Bitfinex trades failed: %s", e)
            return 0.0, 0.0, 0
        bv = sv = 0.0; n = 0
        # Format: [ID, MTS, AMOUNT, PRICE]  positive amount = buy, negative = sell
        for t in (data or []):
            try:
                amount = float(t[2])
            except Exception:
                continue
            qty = abs(amount)
            if qty < threshold_btc: continue
            if amount > 0:
                bv += qty
            else:
                sv += qty
            n += 1
        return bv, sv, n

    (b_bv, b_sv, b_n), (c_bv, c_sv, c_n), (x_bv, x_sv, x_n) = await asyncio.gather(
        _binance_spot(), _coinbase(), _bitfinex()
    )
    buy_vol  = b_bv + c_bv + x_bv
    sell_vol = b_sv + c_sv + x_sv
    n_trades = b_n + c_n + x_n
    total    = buy_vol + sell_vol
    venues_ok = [name for name, n in
                 (("Binance", b_n), ("Coinbase", c_n), ("Bitfinex", x_n)) if n > 0]

    if total == 0:
        return {
            "signal":         "NEUTRAL",
            "data_available": True,
            "whale_buy_btc":  0, "whale_sell_btc": 0, "whale_buy_pct": 50,
            "large_trade_btc": 0, "whale_trade_count": 0,
            "venues_with_data": venues_ok,
            "interpretation":
                f"No spot trades ≥{threshold_btc} BTC in last 5m across "
                f"{','.join(venues_ok) or 'any venue'}. Thin spot-whale activity.",
        }
    buy_pct = buy_vol / total * 100
    sig = "BULLISH" if buy_pct > 60 else "BEARISH" if buy_pct < 40 else "NEUTRAL"
    interp = (
        f"Spot whale buyers dominate across {','.join(venues_ok)}: {buy_pct:.1f}% of "
        f"{n_trades} large trades ({buy_vol:.1f} BTC buys / {sell_vol:.1f} BTC sells) in last 5m. "
        "Genuine multi-venue spot accumulation — no leverage involved."
        if buy_pct > 60 else
        f"Spot whale sellers dominate across {','.join(venues_ok)}: only {buy_pct:.1f}% buys "
        f"({buy_vol:.1f} BTC buys / {sell_vol:.1f} BTC sells across {n_trades} trades in 5m). "
        "Multi-venue spot distribution — real holders exiting."
        if buy_pct < 40 else
        f"Spot whales balanced across {','.join(venues_ok)}: {buy_pct:.1f}% buys, "
        f"{total:.1f} BTC in {n_trades} large trades last 5m. No clear direction."
    )
    return {
        "whale_buy_btc":     round(buy_vol, 2),
        "whale_sell_btc":    round(sell_vol, 2),
        "whale_buy_pct":     round(buy_pct, 1),
        "large_trade_btc":   round(total, 2),
        "whale_trade_count": n_trades,
        "venues_with_data":  venues_ok,
        "signal":            sig,
        "data_available":    True,
        "interpretation":    interp,
    }


# NOTE: the dashboard key remains "bybit_liquidations" for schema continuity
# (storage, prompt builder, frontend mapping). The function and data it returns
# are OKX isolated-margin BTC-USDT-SWAP — the original naming was wrong.
async def _fetch_okx_isolated_liquidations() -> Dict:
    try:
        data = await _get(
            "https://www.okx.com/api/v5/public/liquidation-orders"
            "?instType=SWAP&mgnMode=isolated&instId=BTC-USDT-SWAP&state=filled&limit=100"
        )
    except Exception as _e:
        logger.debug("OKX isolated liquidations failed: %s", _e)
        data = {}
    rows = []
    for event in (data.get("data") or []):
        for detail in (event.get("details") or []):
            rows.append(detail)
    if not rows:
        return {
            "total": 0, "long_liq_usd": 0, "short_liq_usd": 0,
            "signal": "NEUTRAL",
            "interpretation": "No isolated-margin liquidations in recent window.",
        }
    now_ms = time.time() * 1000
    cutoff = now_ms - 900_000  # 15-min window for isolated margin
    recent = [r for r in rows if float(r.get("ts", now_ms)) >= cutoff] or rows
    # Same BTC-USDT-SWAP contract-value correction as _fetch_liquidations.
    OKX_CT_VAL_BTC = 0.01
    longs  = [r for r in recent if r.get("posSide", "").lower() == "long"]
    shorts = [r for r in recent if r.get("posSide", "").lower() == "short"]
    l_usd  = sum(float(r.get("sz", 0)) * OKX_CT_VAL_BTC * float(r.get("bkPx", 0)) for r in longs)
    s_usd  = sum(float(r.get("sz", 0)) * OKX_CT_VAL_BTC * float(r.get("bkPx", 0)) for r in shorts)
    sig    = "BEARISH" if l_usd > s_usd * 1.5 else "BULLISH" if s_usd > l_usd * 1.5 else "NEUTRAL"
    interp = (
        f"OKX isolated-margin long cascade: ${l_usd:,.0f} longs liquidated vs ${s_usd:,.0f} shorts. "
        "Independent confirmation vs cross-margin book — longs being unwound on OKX."
        if l_usd > s_usd * 1.5 else
        f"OKX isolated-margin short squeeze: ${s_usd:,.0f} shorts force-covered vs ${l_usd:,.0f} longs. "
        "Independent confirmation of forced short covering on OKX."
        if s_usd > l_usd * 1.5 else
        f"OKX isolated-margin mixed liqs: ${l_usd:,.0f} long / ${s_usd:,.0f} short. "
        "No directional cascade in isolated book."
    )
    return {
        "total":          len(recent),
        "long_liq_usd":   round(l_usd, 0),
        "short_liq_usd":  round(s_usd, 0),
        "signal":         sig,
        "interpretation": interp,
    }


async def _fetch_okx_funding() -> Dict:
    data = await _get(
        "https://www.okx.com/api/v5/public/funding-rate?instId=BTC-USDT-SWAP"
    )
    rows = data.get("data") or []
    if not rows:
        raise ValueError("Empty OKX funding response")
    fr  = float(rows[0].get("fundingRate", 0))
    sig = "BEARISH" if fr > 0.0005 else "BULLISH" if fr < 0 else "NEUTRAL"
    interp = (
        f"OKX funding POSITIVE ({fr*100:.4f}%) — longs paying shorts. "
        "OKX-specific leverage buildup; longs at squeeze risk."
        if fr > 0.0005 else
        f"OKX funding NEGATIVE ({fr*100:.4f}%) — shorts paying longs. "
        "OKX structural long bias; shorts incentivised to close."
        if fr < 0 else
        f"OKX funding near-zero ({fr*100:.4f}%). No leverage imbalance on OKX."
    )
    return {
        "funding_rate_pct": round(fr * 100, 5),
        "signal":           sig,
        "interpretation":   interp,
    }


# Aggregated BTC perp symbols across major venues. Codes from Coinalyze
# /future-markets discovery. Order matters only for logging clarity; the
# endpoint returns one row per symbol regardless.
_COINALYZE_BTC_PERPS = [
    "BTCUSDT_PERP.A",  # Binance USDT-M
    "BTCUSDT.6",       # Bybit USDT
    "BTCUSDT_PERP.3",  # OKX USDT
    "BTC-PERPETUAL.2", # Deribit inverse (USD)
    "BTCUSDT_PERP.4",  # Bitget USDT
    "BTCUSDT_PERP.F",  # Bitmart USDT (light)
    "BTCUSDT_PERP.0",  # additional venue for breadth
]


async def _coinalyze_get(api_key: str, path: str, **params) -> Any:
    qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
    sep = "&" if qs else ""
    return await _get(f"https://api.coinalyze.net/v1/{path}?{qs}{sep}api_key={api_key}")


async def _fetch_coinalyze_aggregate(api_key: str) -> Dict:
    """
    Aggregated BTC-perp OI, liquidations, and taker flow across ~7 venues.

    This is the "real" aggregate that replaces Binance-only OI and single-venue
    liquidation numbers. Using Coinalyze avoids having to hit each exchange's
    endpoint individually (the user already has a free-tier key; rate limit is
    40 req/min and we use ~3 per bar).
    """
    symbols = ",".join(_COINALYZE_BTC_PERPS)
    now = int(time.time())
    # 35 min window so we can compute a 30m velocity like the per-venue OI code
    frm = now - 35 * 60

    async def _oi() -> Optional[Dict]:
        try:
            return await _coinalyze_get(
                api_key, "open-interest-history",
                symbols=symbols, interval="5min", **{"from": frm, "to": now},
                convert_to_usd="true",
            )
        except Exception as e:
            logger.warning("Coinalyze OI-history failed: %s", e)
            return None

    async def _liq() -> Optional[Dict]:
        try:
            return await _coinalyze_get(
                api_key, "liquidation-history",
                symbols=symbols, interval="5min", **{"from": frm, "to": now},
                convert_to_usd="true",
            )
        except Exception as e:
            logger.warning("Coinalyze liq-history failed: %s", e)
            return None

    oi_rows, liq_rows = await asyncio.gather(_oi(), _liq())
    result: Dict[str, Any] = {"data_available": True}

    # ── Aggregate OI (USD) — current + 30m change ─────────────────────────
    # CRITICAL: venues report at different cadences and some symbols may be
    # missing bars. Summing per-timestamp naively produces nonsense deltas
    # (e.g. if venue A only has t0 and venue B only has t1, A+B at t0 vs A+B
    # at t1 is not a real change). Only use timestamps where ALL present
    # venues have a value.
    if oi_rows:
        # rows = [{symbol, history: [{t, o, h, l, c}]}, ...]
        per_venue_series: Dict[str, Dict[int, float]] = {}
        for r in oi_rows:
            sym = r.get("symbol", "?")
            per_venue_series[sym] = {
                int(pt["t"]): float(pt.get("c", 0) or 0)
                for pt in (r.get("history") or [])
                if pt.get("c") is not None
            }
        # Keep only venues that actually reported data
        per_venue_series = {s: d for s, d in per_venue_series.items() if d}
        if per_venue_series:
            common_ts = set.intersection(*(set(d.keys()) for d in per_venue_series.values()))
            if len(common_ts) >= 2:
                sorted_ts = sorted(common_ts)
                oi_now  = sum(d[sorted_ts[-1]] for d in per_venue_series.values())
                oi_prev = sum(d[sorted_ts[0]]  for d in per_venue_series.values())
                chg_pct = (oi_now - oi_prev) / oi_prev * 100 if oi_prev else 0.0
                span_min = (sorted_ts[-1] - sorted_ts[0]) / 60.0
                result["agg_oi_usd"]            = round(oi_now, 0)
                result["agg_oi_change_pct"]     = round(chg_pct, 4)
                result["agg_oi_change_span_min"] = round(span_min, 1)
                result["agg_oi_venues_count"]   = len(per_venue_series)
            elif len(common_ts) == 1:
                # Only one aligned snapshot — report current, no delta
                ts = next(iter(common_ts))
                result["agg_oi_usd"] = round(sum(d[ts] for d in per_venue_series.values()), 0)
                result["agg_oi_venues_count"] = len(per_venue_series)
            else:
                result["agg_oi_usd"] = None
        else:
            result["agg_oi_usd"] = None
    else:
        result["agg_oi_usd"] = None

    # ── Aggregate liquidations (USD) — last 5m bar ────────────────────────
    if liq_rows:
        # rows = [{symbol, history: [{t, l, s}]}]  where l=long-liq USD, s=short-liq USD
        liq_long_latest = 0.0
        liq_short_latest = 0.0
        liq_long_15m = 0.0
        liq_short_15m = 0.0
        max_t = 0
        for r in liq_rows:
            for pt in (r.get("history") or []):
                max_t = max(max_t, int(pt["t"]))
        cutoff_15 = max_t - 15 * 60
        for r in liq_rows:
            for pt in (r.get("history") or []):
                t = int(pt["t"])
                l = float(pt.get("l", 0) or 0)
                s = float(pt.get("s", 0) or 0)
                if t == max_t:
                    liq_long_latest  += l
                    liq_short_latest += s
                if t >= cutoff_15:
                    liq_long_15m  += l
                    liq_short_15m += s
        result["agg_long_liq_usd_5m"]  = round(liq_long_latest,  0)
        result["agg_short_liq_usd_5m"] = round(liq_short_latest, 0)
        result["agg_long_liq_usd_15m"] = round(liq_long_15m,     0)
        result["agg_short_liq_usd_15m"]= round(liq_short_15m,    0)
    else:
        result["agg_long_liq_usd_5m"] = None

    # ── Narrative signal based on liquidations (more actionable than OI) ──
    ll = result.get("agg_long_liq_usd_5m")
    ss = result.get("agg_short_liq_usd_5m")
    if ll is not None and ss is not None:
        if ll > ss * 1.5 and ll > 500_000:
            sig = "BEARISH"
            interp = (
                f"Aggregate long cascade across {len(liq_rows)} venues: "
                f"${ll:,.0f} longs liquidated vs ${ss:,.0f} shorts in last 5m "
                f"(15m: ${result['agg_long_liq_usd_15m']:,.0f}L / ${result['agg_short_liq_usd_15m']:,.0f}S). "
                "Real cross-venue forced selling."
            )
        elif ss > ll * 1.5 and ss > 500_000:
            sig = "BULLISH"
            interp = (
                f"Aggregate short squeeze across {len(liq_rows)} venues: "
                f"${ss:,.0f} shorts force-covered vs ${ll:,.0f} longs in last 5m "
                f"(15m: ${result['agg_long_liq_usd_15m']:,.0f}L / ${result['agg_short_liq_usd_15m']:,.0f}S). "
                "Real cross-venue forced buying."
            )
        else:
            sig = "NEUTRAL"
            interp = (
                f"Aggregate liquidations balanced across {len(liq_rows) if liq_rows else 0} venues: "
                f"${ll:,.0f} long / ${ss:,.0f} short (5m). No dominant cascade."
            )
    else:
        sig = "UNAVAILABLE"
        interp = "Aggregate liquidation data unavailable."

    result["signal"] = sig
    result["interpretation"] = interp
    return result


async def _fetch_btc_dominance() -> Dict:
    data  = await _get("https://api.coingecko.com/api/v3/global")
    gdata = data.get("data") or data
    dom   = float((gdata.get("market_cap_percentage") or {}).get("btc", 50))
    chg   = float(gdata.get("market_cap_change_percentage_24h_usd", 0))
    sig   = "BULLISH" if dom > 54 else "BEARISH" if dom < 44 else "NEUTRAL"
    interp = (
        f"BTC dominance {dom:.1f}% — above 54% signals strong BTC preference, "
        "alt capital rotating into BTC. Historically bullish for BTC price."
        if dom > 54 else
        f"BTC dominance {dom:.1f}% — below 44%, capital flowing to alts. "
        "Risk appetite high in alts; BTC may underperform short-term."
        if dom < 44 else
        f"BTC dominance {dom:.1f}% — mid-range, neutral rotation signal. "
        f"Total crypto market 24h: {chg:+.2f}%."
    )
    return {
        "btc_dominance_pct":     round(dom, 2),
        "market_change_24h_pct": round(chg, 3),
        "signal":                sig,
        "interpretation":        interp,
    }


async def _fetch_top_position_ratio() -> Dict:
    try:
        data = await _get(
            "https://fapi.binance.com/futures/data/topLongShortPositionRatio"
            "?symbol=BTCUSDT&period=5m&limit=1"
        )
        row  = data[0] if data else {}
        lsr  = float(row.get("longShortRatio", 1.0))
        lp   = float(row.get("longAccount",   0.5)) * 100
        sp   = 100.0 - lp
        _approx = False
    except Exception as _e:
        logger.debug("Binance top position ratio failed, using Bybit fallback: %s", _e)
        raw  = await _get(
            "https://api.bybit.com/v5/market/account-ratio"
            "?category=linear&symbol=BTCUSDT&period=5min&limit=1"
        )
        row  = ((raw.get("result") or {}).get("list") or [{}])[0]
        lp   = float(row.get("buyRatio", 0.5)) * 100
        sp   = 100.0 - lp
        lsr  = lp / sp if sp > 0 else 1.0
        _approx = True
    sig  = "BULLISH" if lsr > 1.3 else "BEARISH" if lsr < 0.77 else "NEUTRAL"
    _src = " (Bybit approx)" if _approx else ""
    # "Top positions" on Binance = positions held by the top 20% of accounts by
    # margin balance on this symbol. This is notional-weighted across those
    # accounts but still Binance-retail — not institutional flow.
    interp = (
        f"Binance top-position accounts {lp:.0f}% long by notional (ratio {lsr:.3f}){_src}. "
        "Top-20%-by-margin retail accounts heavily net-long — directional bias among larger retail."
        if lsr > 1.3 else
        f"Binance top-position accounts only {lp:.0f}% long by notional (ratio {lsr:.3f}){_src}. "
        "Top-20%-by-margin retail accounts net-short — bearish notional bias among larger retail."
        if lsr < 0.77 else
        f"Binance top-position accounts {lp:.0f}% long by notional (ratio {lsr:.3f}){_src}. "
        "No extreme positioning among top-20%-by-margin retail accounts."
    )
    return {
        "long_short_ratio":   round(lsr, 4),
        "long_position_pct":  round(lp, 1),
        "short_position_pct": round(sp, 1),
        "signal":             sig,
        "interpretation":     interp,
    }


async def _fetch_funding_trend() -> Dict:
    try:
        data = await _get(
            "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=6"
        )
        if not data or len(data) < 2:
            raise ValueError("Insufficient funding rate history")
        rates = [float(d.get("fundingRate", 0)) for d in data]
    except Exception as _e:
        logger.debug("Binance funding trend failed, using Bybit fallback: %s", _e)
        raw = await _get(
            "https://api.bybit.com/v5/market/funding/history"
            "?category=linear&symbol=BTCUSDT&limit=6"
        )
        rows = (raw.get("result") or {}).get("list") or []
        if len(rows) < 2:
            raise ValueError("Insufficient Bybit funding history")
        rates = [float(r.get("fundingRate", 0)) for r in reversed(rows)]
    latest = rates[-1]
    avg    = sum(rates) / len(rates)
    trend  = rates[-1] - rates[0]
    sig = (
        "BEARISH" if latest > 0.0005 and trend > 0 else
        "BULLISH" if latest < 0 or (trend < -0.0002 and latest < 0.0003) else
        "NEUTRAL"
    )
    interp = (
        f"Funding rate RISING trend ({rates[0]*100:.4f}% → {latest*100:.4f}%). "
        "Accelerating leverage buildup — cascading liquidations likely if price reverses."
        if latest > 0.0005 and trend > 0 else
        f"Funding rate FALLING/NEGATIVE (current {latest*100:.4f}%, trend {trend*100:+.4f}%). "
        "De-levering or shorts paying longs — supportive of upward price pressure."
        if latest < 0 or (trend < -0.0002 and latest < 0.0003) else
        f"Funding rate stable near-zero (current {latest*100:.4f}%, trend {trend*100:+.4f}%). "
        "Balanced leverage — no directional funding bias."
    )
    return {
        "funding_latest_pct": round(latest * 100, 5),
        "funding_avg_6p_pct": round(avg * 100, 5),
        "funding_trend":      round(trend * 100, 5),
        "signal":             sig,
        "interpretation":     interp,
    }


async def _fetch_coinalyze(api_key: str) -> Dict:
    data  = await _get(
        f"https://api.coinalyze.net/v1/funding-rate"
        f"?symbols=BTCUSDT_PERP.A&api_key={api_key}"
    )
    items = data if isinstance(data, list) else data.get("data", [])
    if not items:
        raise ValueError("Empty Coinalyze response")
    item = items[0]
    frv  = float(item.get("fr") or item.get("value") or item.get("funding_rate") or 0)
    sig  = "BEARISH" if frv > 0.0005 else "BULLISH" if frv < 0 else "NEUTRAL"
    interp = (
        f"Cross-exchange funding POSITIVE ({frv*100:.4f}%) — aggregate longs paying shorts "
        "across all venues. Leveraged longs at risk of liquidation cascade."
        if frv > 0.0005 else
        f"Cross-exchange funding NEGATIVE ({frv*100:.4f}%) — shorts paying longs. "
        "Structural upward price bias as market incentivises shorts to close."
        if frv < 0 else
        f"Cross-exchange funding near-zero ({frv*100:.4f}%) — no systemic leverage bias."
    )
    return {
        "funding_rate_8h_pct": round(frv * 100, 5),
        "signal":              sig,
        "interpretation":      interp,
    }


async def _fetch_coingecko() -> Dict:
    data = await _get(
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin&vs_currencies=usd"
        "&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true"
    )
    b    = data.get("bitcoin", {})
    mcap = float(b.get("usd_market_cap",  0))
    vol  = float(b.get("usd_24h_vol",     0))
    ch   = float(b.get("usd_24h_change",  0))
    vm   = vol / mcap * 100 if mcap else 0
    return {
        "market_cap_usd":        round(mcap, 0),
        "volume_24h_usd":        round(vol,  0),
        "change_24h_pct":        round(ch,   3),
        "vol_to_mcap_ratio_pct": round(vm,   3),
        "interpretation": (
            f"24h change {ch:+.2f}%, vol/mcap={vm:.2f}%. "
            f"{'Elevated' if vm > 5 else 'Normal'} activity. "
            f"Macro momentum {'BULLISH' if ch > 0 else 'BEARISH'}."
        ),
    }


async def _fetch_deribit_options() -> Dict:
    summaries, idx_r = await asyncio.gather(
        _get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
             "?currency=BTC&kind=option"),
        _get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd"),
    )
    rows = summaries.get("result") or []
    if not rows:
        raise ValueError("Empty Deribit options response")
    spot = float((idx_r.get("result") or {}).get("index_price", 0))

    call_oi = put_oi = 0.0
    strikes: Dict[float, Dict] = {}
    for s in rows:
        name = s.get("instrument_name", "")
        oi   = float(s.get("open_interest", 0))
        parts = name.split("-")
        if len(parts) < 4:
            continue
        opt_type = parts[-1]
        try:
            strike = float(parts[-2])
        except ValueError:
            continue
        if opt_type == "C":
            call_oi += oi
            strikes.setdefault(strike, {"c": 0.0, "p": 0.0})["c"] += oi
        elif opt_type == "P":
            put_oi += oi
            strikes.setdefault(strike, {"c": 0.0, "p": 0.0})["p"] += oi

    total_oi = call_oi + put_oi
    pcr = put_oi / call_oi if call_oi > 0 else 1.0

    max_pain = spot
    if strikes and spot > 0:
        min_pain = float("inf")
        for test_s in sorted(strikes):
            pain = sum(
                max(0.0, test_s - k) * v["c"] + max(0.0, k - test_s) * v["p"]
                for k, v in strikes.items()
            )
            if pain < min_pain:
                min_pain = pain
                max_pain = test_s

    dist = ((max_pain - spot) / spot * 100) if spot > 0 else 0.0
    sig  = (
        "BEARISH_CONTRARIAN" if pcr > 1.3 else
        "BULLISH_CONTRARIAN" if pcr < 0.6 else
        "NEUTRAL"
    )
    interp = (
        f"Put/Call OI ratio {pcr:.3f} — heavy put buying. "
        f"Contrarian BULLISH: institutional hedging = spot still supported. "
        f"Max pain ${max_pain:,.0f} ({dist:+.1f}% from spot)."
        if pcr > 1.3 else
        f"Put/Call OI ratio {pcr:.3f} — call-heavy (complacency). "
        f"Contrarian BEARISH: retail chasing calls = potential reversal risk. "
        f"Max pain ${max_pain:,.0f} ({dist:+.1f}% from spot)."
        if pcr < 0.6 else
        f"Put/Call OI ratio {pcr:.3f} — balanced options positioning. "
        f"Max pain ${max_pain:,.0f} ({dist:+.1f}% from spot). "
        "Price gravitates toward max pain near expiry."
    )
    return {
        "put_oi_btc":       round(put_oi,   1),
        "call_oi_btc":      round(call_oi,  1),
        "total_oi_btc":     round(total_oi, 1),
        "put_call_ratio":   round(pcr,  4),
        "max_pain_usd":     round(max_pain, 0),
        "spot_price":       round(spot, 0),
        "dist_to_pain_pct": round(dist, 2),
        "signal":           sig,
        "interpretation":   interp,
    }


async def _fetch_deribit_skew_term() -> Dict:
    """
    Deribit options enrichment: 25-delta risk reversal (skew), IV term
    structure, and P/C VOLUME ratio (different from OI ratio).

    25-delta risk reversal = IV(25Δ call) − IV(25Δ put), per expiry.
      Positive → calls bid (bullish skew).
      Negative → puts bid (bearish skew, tail hedging).

    Term structure = ATM IV at roughly 7d, 30d, 90d.
      Inverted (short > long) signals short-dated fear — classic pre-selloff.

    P/C volume = today's flow; P/C OI (separate fetcher) = accumulated positioning.
    """
    try:
        summaries_r, idx_r = await asyncio.gather(
            _get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
                 "?currency=BTC&kind=option"),
            _get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd"),
        )
    except Exception as e:
        logger.warning("Deribit skew/term fetch failed: %s", e)
        return {
            "signal":         "UNAVAILABLE",
            "data_available": False,
            "interpretation": "Deribit skew/term unavailable — fetch failed.",
        }
    rows = summaries_r.get("result") or []
    spot = float((idx_r.get("result") or {}).get("index_price", 0))
    if not rows or spot <= 0:
        return {
            "signal":         "UNAVAILABLE",
            "data_available": False,
            "interpretation": "Deribit skew/term unavailable — empty response.",
        }

    # Parse name like "BTC-28MAR26-80000-C" → expiry, strike, type
    from datetime import datetime, timezone
    now_ts = time.time()
    # Bucket by expiry
    per_expiry: Dict[str, Dict] = {}
    call_volume_btc = put_volume_btc = 0.0
    for s in rows:
        name = s.get("instrument_name", "")
        parts = name.split("-")
        if len(parts) < 4:
            continue
        expiry_str, strike_str, opt_type = parts[1], parts[2], parts[3]
        try:
            strike = float(strike_str)
            expiry_dt = datetime.strptime(expiry_str, "%d%b%y").replace(tzinfo=timezone.utc)
            days_to_exp = (expiry_dt.timestamp() - now_ts) / 86400
        except Exception:
            continue
        if days_to_exp < 0 or days_to_exp > 365:
            continue
        mark_iv = float(s.get("mark_iv") or 0)
        delta   = s.get("greeks", {}).get("delta") if isinstance(s.get("greeks"), dict) else s.get("delta")
        try:
            delta = float(delta) if delta is not None else None
        except (TypeError, ValueError):
            delta = None
        vol_btc = float(s.get("volume") or 0)

        # Accumulate P/C volume across all expiries
        if opt_type == "C":
            call_volume_btc += vol_btc
        elif opt_type == "P":
            put_volume_btc += vol_btc

        bucket = per_expiry.setdefault(expiry_str, {
            "days": days_to_exp, "calls": [], "puts": [],
        })
        entry = {"strike": strike, "iv": mark_iv, "delta": delta, "volume": vol_btc}
        if opt_type == "C":
            bucket["calls"].append(entry)
        elif opt_type == "P":
            bucket["puts"].append(entry)

    # ── IV term structure: pick expiries closest to 7d, 30d, 90d; use ATM IV ──
    def _atm_iv(bucket: Dict) -> Optional[float]:
        """Nearest-to-spot strike's IV, averaged across call+put sides."""
        all_opts = bucket["calls"] + bucket["puts"]
        if not all_opts:
            return None
        nearest = min(all_opts, key=lambda o: abs(o["strike"] - spot))
        # Use IVs from both sides at that same strike if available
        target_strike = nearest["strike"]
        ivs = [o["iv"] for o in all_opts if o["strike"] == target_strike and o["iv"] > 0]
        if not ivs:
            return None
        return sum(ivs) / len(ivs)

    def _nearest_expiry(days_target: float) -> Optional[Dict]:
        if not per_expiry:
            return None
        candidates = [(abs(b["days"] - days_target), b) for b in per_expiry.values()]
        return min(candidates, key=lambda x: x[0])[1] if candidates else None

    iv_7d   = _nearest_expiry(7)
    iv_30d  = _nearest_expiry(30)
    iv_90d  = _nearest_expiry(90)
    iv_7d_val  = _atm_iv(iv_7d)  if iv_7d  else None
    iv_30d_val = _atm_iv(iv_30d) if iv_30d else None
    iv_90d_val = _atm_iv(iv_90d) if iv_90d else None
    term_inverted = (
        iv_7d_val is not None and iv_30d_val is not None and iv_7d_val > iv_30d_val + 3
    )
    term_contango = (
        iv_7d_val is not None and iv_90d_val is not None and iv_90d_val > iv_7d_val + 3
    )

    # ── 25-delta risk reversal on the 30d expiry (conventional) ──────────
    rr_25d_pct: Optional[float] = None
    if iv_30d:
        # Find option with delta nearest ±0.25 on each side
        calls_with_delta = [c for c in iv_30d["calls"] if c["delta"] is not None and c["iv"] > 0]
        puts_with_delta  = [p for p in iv_30d["puts"]  if p["delta"] is not None and p["iv"] > 0]
        if calls_with_delta and puts_with_delta:
            c25 = min(calls_with_delta, key=lambda o: abs(o["delta"] - 0.25))
            p25 = min(puts_with_delta,  key=lambda o: abs(o["delta"] + 0.25))
            rr_25d_pct = c25["iv"] - p25["iv"]

    skew_sig = "NEUTRAL"
    if rr_25d_pct is not None:
        if rr_25d_pct > 1.5:
            skew_sig = "BULLISH"
        elif rr_25d_pct < -1.5:
            skew_sig = "BEARISH"

    total_volume_btc = call_volume_btc + put_volume_btc
    pcv = put_volume_btc / call_volume_btc if call_volume_btc > 0 else 1.0
    pcv_sig = (
        "BEARISH_CONTRARIAN" if pcv > 1.2 else
        "BULLISH_CONTRARIAN" if pcv < 0.65 else
        "NEUTRAL"
    )

    # Build interpretation combining the three angles
    parts = []
    if rr_25d_pct is not None:
        if rr_25d_pct > 1.5:
            parts.append(
                f"30d 25Δ risk reversal +{rr_25d_pct:.1f}% — calls bid over puts, "
                "bullish skew (traders paying for upside)."
            )
        elif rr_25d_pct < -1.5:
            parts.append(
                f"30d 25Δ risk reversal {rr_25d_pct:.1f}% — puts bid over calls, "
                "bearish skew (tail-hedging dominant)."
            )
        else:
            parts.append(f"30d 25Δ risk reversal {rr_25d_pct:+.1f}% — symmetric skew, no directional bias.")
    if iv_7d_val and iv_30d_val:
        if term_inverted:
            parts.append(
                f"Term structure inverted: 7d IV {iv_7d_val:.1f}% > 30d IV {iv_30d_val:.1f}% — "
                "short-dated stress, classic pre-move signal."
            )
        elif term_contango and iv_90d_val:
            parts.append(
                f"Term in contango: 7d {iv_7d_val:.1f}% < 30d {iv_30d_val:.1f}% < 90d {iv_90d_val:.1f}% — "
                "normal regime, no short-dated fear premium."
            )
        else:
            parts.append(
                f"Term structure flat: 7d {iv_7d_val:.1f}% / 30d {iv_30d_val:.1f}%"
                + (f" / 90d {iv_90d_val:.1f}%" if iv_90d_val else "")
                + " — no regime signal from IV curve."
            )
    if call_volume_btc > 0:
        parts.append(
            f"P/C volume ratio {pcv:.2f} "
            f"({put_volume_btc:.1f} put / {call_volume_btc:.1f} call BTC traded today)."
        )

    # Primary signal: skew (directional), fall back to term-inversion
    if skew_sig != "NEUTRAL":
        sig = skew_sig
    elif term_inverted:
        sig = "BEARISH"
    else:
        sig = "NEUTRAL"

    return {
        "rr_25d_30d_pct":        round(rr_25d_pct, 2) if rr_25d_pct is not None else None,
        "iv_7d_atm_pct":         round(iv_7d_val, 2)  if iv_7d_val  is not None else None,
        "iv_30d_atm_pct":        round(iv_30d_val, 2) if iv_30d_val is not None else None,
        "iv_90d_atm_pct":        round(iv_90d_val, 2) if iv_90d_val is not None else None,
        "term_inverted":         term_inverted,
        "term_contango":         term_contango,
        "put_volume_btc":        round(put_volume_btc, 1),
        "call_volume_btc":       round(call_volume_btc, 1),
        "put_call_volume_ratio": round(pcv, 4),
        "pc_volume_signal":      pcv_sig,
        "skew_signal":           skew_sig,
        "signal":                sig,
        "data_available":        True,
        "interpretation":        " ".join(parts) if parts else "Deribit options enrichment unavailable.",
    }


_btc_onchain_cache: Dict = {}
_btc_onchain_cache_ts: float = 0.0
_BTC_ONCHAIN_TTL = 3600.0  # 1 hour — bitcoin-data.com rate-limits aggressively

async def _fetch_btc_onchain() -> Dict:
    global _btc_onchain_cache, _btc_onchain_cache_ts
    if _btc_onchain_cache and (time.time() - _btc_onchain_cache_ts) < _BTC_ONCHAIN_TTL:
        return _btc_onchain_cache

    sopr_r, mvrv_r = await asyncio.gather(
        _get("https://api.bitcoin-data.com/v1/sopr"),
        _get("https://api.bitcoin-data.com/v1/mvrv-zscore"),
    )
    sopr_row = sopr_r[-1] if sopr_r else {}
    mvrv_row = mvrv_r[-1] if mvrv_r else {}
    sopr  = float(sopr_row.get("sopr",       1.0))
    mvrv  = float(mvrv_row.get("mvrvZscore", 1.0))
    sopr_date = sopr_row.get("d", "N/A")
    mvrv_date = mvrv_row.get("d", "N/A")

    sopr_sig = "BULLISH" if sopr > 1.02 else "BEARISH_CONTRARIAN" if sopr < 0.98 else "NEUTRAL"
    mvrv_sig = "BEARISH_CONTRARIAN" if mvrv > 3.5 else "BULLISH" if mvrv < 0.5 else "NEUTRAL"

    sopr_interp = (
        f"SOPR {sopr:.4f} > 1.0 — holders spending at PROFIT. "
        "Selling pressure possible but trend intact. Watch for SOPR collapse as reversal warning."
        if sopr > 1.02 else
        f"SOPR {sopr:.4f} < 1.0 — coins moving at LOSS (capitulation). "
        "Historically strong accumulation zone — contrarian BULLISH."
        if sopr < 0.98 else
        f"SOPR {sopr:.4f} near 1.0 — breakeven spending. No strong on-chain directional bias."
    )
    mvrv_interp = (
        f"MVRV Z-Score {mvrv:.3f} > 3.5 — historically overvalued. "
        "Long-term holders sitting on large unrealized gains — elevated distribution risk."
        if mvrv > 3.5 else
        f"MVRV Z-Score {mvrv:.3f} < 0.5 — historically undervalued. "
        "Market below realized value — strong long-term accumulation zone."
        if mvrv < 0.5 else
        f"MVRV Z-Score {mvrv:.3f} — fair value range. No extreme macro over/undervaluation."
    )
    _btc_onchain_cache = {
        "sopr":               round(sopr, 5),
        "sopr_date":          sopr_date,
        "sopr_signal":        sopr_sig,
        "sopr_interpretation":sopr_interp,
        "mvrv_zscore":        round(mvrv, 4),
        "mvrv_date":          mvrv_date,
        "mvrv_signal":        mvrv_sig,
        "mvrv_interpretation":mvrv_interp,
        "signal":             sopr_sig,
        "interpretation":     sopr_interp,
    }
    _btc_onchain_cache_ts = time.time()
    return _btc_onchain_cache


async def _fetch_coinglass_liquidations(api_key: str) -> Dict:
    data = await _get(
        "https://open-api.coinglass.com/api/futures/liquidation/aggregated-history"
        "?symbol=BTC&interval=5m&limit=3",
        headers={"CG-API-KEY": api_key},
    )
    rows = data.get("data") or []
    if isinstance(rows, dict):
        rows = rows.get("list") or []
    if not rows:
        raise ValueError("Empty CoinGlass liquidation response")
    latest = rows[-1]
    long_usd  = float(latest.get("longLiqUsd",  latest.get("long",  0)) or 0)
    short_usd = float(latest.get("shortLiqUsd", latest.get("short", 0)) or 0)
    t3_long   = sum(float(r.get("longLiqUsd",  r.get("long",  0)) or 0) for r in rows)
    t3_short  = sum(float(r.get("shortLiqUsd", r.get("short", 0)) or 0) for r in rows)

    sig = "BEARISH" if long_usd > short_usd * 1.5 else "BULLISH" if short_usd > long_usd * 1.5 else "NEUTRAL"
    interp = (
        f"Cross-exchange long cascade: ${long_usd:,.0f} longs liquidated vs ${short_usd:,.0f} shorts (5min). "
        "Forced long unwinding creates mechanical sell pressure."
        if long_usd > short_usd * 1.5 else
        f"Cross-exchange short squeeze: ${short_usd:,.0f} shorts force-covered vs ${long_usd:,.0f} longs (5min). "
        "Forced buying from liquidated shorts — upside spike risk."
        if short_usd > long_usd * 1.5 else
        f"Mixed cross-exchange liquidations: ${long_usd:,.0f} long / ${short_usd:,.0f} short (5min). "
        "No dominant cascade — market clearing both sides."
    )
    return {
        "long_liq_usd":         round(long_usd,  0),
        "short_liq_usd":        round(short_usd, 0),
        "total_3bar_long_usd":  round(t3_long,   0),
        "total_3bar_short_usd": round(t3_short,  0),
        "signal":               sig,
        "interpretation":       interp,
    }


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

def extract_signal_directions(ds: Dict[str, Any]) -> Dict[str, str]:
    """Map each dashboard indicator's signal to 'UP', 'DOWN', or 'NEUTRAL'."""
    result: Dict[str, str] = {}

    def _map(raw: str) -> str:
        s = (raw or "").upper()
        if s in ("BULLISH", "BULLISH_CONTRARIAN"):
            return "UP"
        if s in ("BEARISH", "BEARISH_CONTRARIAN"):
            return "DOWN"
        return "NEUTRAL"

    mappings = [
        ("order_book",           lambda d: _map(d.get("signal", ""))),
        ("long_short",           lambda d: _map(d.get("retail_signal_contrarian", ""))),
        ("taker_flow",           lambda d: _map(d.get("signal", ""))),
        ("oi_funding",           lambda d: _map(d.get("funding_signal", ""))),
        ("liquidations",         lambda d: _map(d.get("signal", ""))),
        ("fear_greed",           lambda d: _map(d.get("signal", ""))),
        ("mempool",              lambda d: _map(d.get("signal", ""))),
        ("coinalyze",            lambda d: _map(d.get("signal", ""))),
        ("coingecko",            lambda d: "UP" if float(d.get("change_24h_pct", 0) or 0) > 0 else
                                           "DOWN" if float(d.get("change_24h_pct", 0) or 0) < 0 else "NEUTRAL"),
        ("deribit_dvol",         lambda d: _map(d.get("signal", ""))),
        ("kraken_premium",       lambda d: _map(d.get("signal", ""))),
        ("oi_velocity",          lambda d: _map(d.get("signal", ""))),
        ("spot_whale_flow",      lambda d: _map(d.get("signal", ""))),
        ("bybit_liquidations",   lambda d: _map(d.get("signal", ""))),
        ("okx_funding",          lambda d: _map(d.get("signal", ""))),
        ("btc_dominance",           lambda d: _map(d.get("signal", ""))),
        ("top_position_ratio",      lambda d: _map(d.get("signal", ""))),
        ("funding_trend",           lambda d: _map(d.get("signal", ""))),
        ("deribit_options",         lambda d: _map(d.get("signal", ""))),
        ("deribit_skew_term",       lambda d: _map(d.get("signal", ""))),
        ("spot_perp_basis",         lambda d: _map(d.get("signal", ""))),
        ("cvd",                     lambda d: _map(d.get("signal", ""))),
        ("coinalyze_aggregate",     lambda d: _map(d.get("signal", ""))),
        ("btc_onchain",             lambda d: _map(d.get("sopr_signal", ""))),
        ("coinglass_liquidations",  lambda d: _map(d.get("signal", ""))),
    ]

    for key, fn in mappings:
        v = ds.get(key)
        if v:
            result[key] = fn(v)

    return result


async def fetch_dashboard_signals(
    coinalyze_key:  str = "",
    coinglass_key:  str = "",
) -> Dict[str, Any]:
    """Fetch all dashboard signals in parallel. Each key is None if its fetch fails."""
    tasks = {
        "order_book":         _fetch_order_book(),
        "long_short":         _fetch_long_short(),
        "taker_flow":         _fetch_taker_flow(),
        "oi_funding":         _fetch_oi_funding(),
        "liquidations":       _fetch_liquidations(),
        "fear_greed":         _fetch_fear_greed(),
        "mempool":            _fetch_mempool(),
        "coingecko":          _fetch_coingecko(),
        "kraken_premium":     _fetch_kraken_premium(),
        "oi_velocity":        _fetch_oi_velocity(),
        "spot_whale_flow":    _fetch_spot_whale_flow(),
        "bybit_liquidations": _fetch_okx_isolated_liquidations(),
        "okx_funding":        _fetch_okx_funding(),
        "btc_dominance":      _fetch_btc_dominance(),
        "top_position_ratio": _fetch_top_position_ratio(),
        "funding_trend":      _fetch_funding_trend(),
        "deribit_dvol":       _fetch_deribit_dvol(),
        "deribit_options":    _fetch_deribit_options(),
        "deribit_skew_term":  _fetch_deribit_skew_term(),
        "spot_perp_basis":    _fetch_spot_perp_basis(),
        "cvd":                _fetch_cvd(),
        "btc_onchain":        _fetch_btc_onchain(),
    }
    if coinalyze_key:
        tasks["coinalyze"] = _fetch_coinalyze(coinalyze_key)
        tasks["coinalyze_aggregate"] = _fetch_coinalyze_aggregate(coinalyze_key)
    if coinglass_key:
        tasks["coinglass_liquidations"] = _fetch_coinglass_liquidations(coinglass_key)

    keys  = list(tasks.keys())
    coros = list(tasks.values())
    raw   = await asyncio.gather(*coros, return_exceptions=True)

    result: Dict[str, Any] = {}
    for key, val in zip(keys, raw):
        if isinstance(val, Exception):
            logger.warning("Dashboard signal '%s' failed: %s", key, val)
            result[key] = None
        else:
            result[key] = _attach_source(val, key)

    result["fetched_at"] = time.time()
    n_ok = sum(1 for v in result.values() if v is not None and not isinstance(v, float))
    logger.info("Dashboard signals fetched: %d/%d ok", n_ok, len(keys))
    return result
