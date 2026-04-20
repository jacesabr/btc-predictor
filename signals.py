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


async def _get(url: str, headers: Optional[Dict] = None) -> Any:
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(connector=connector, timeout=_TIMEOUT) as session:
        async with session.get(url, headers=headers or {}) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} from {url}")
            return await resp.json(content_type=None)


async def _fetch_order_book() -> Dict:
    try:
        data = await _get("https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=20")
        book = (data.get("result") or {}).get("XXBTZUSD", {})
        bv = sum(float(b[1]) for b in book.get("bids", []))
        av = sum(float(a[1]) for a in book.get("asks", []))
    except Exception as _e:
        logger.debug("Kraken order book failed, using Binance fallback: %s", _e)
        data = await _get("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20")
        bv = sum(float(b[1]) for b in data.get("bids", []))
        av = sum(float(a[1]) for a in data.get("asks", []))
    imb = ((bv - av) / (bv + av)) * 100 if (bv + av) > 0 else 0.0
    sig = "BULLISH" if imb > 5 else "BEARISH" if imb < -5 else "NEUTRAL"
    interp = (
        f"Strong bid wall — buyers defending at market. {imb:+.1f}% bid-heavy. "
        "Immediate upward price support from passive bids."
        if imb > 5 else
        f"Ask-heavy book — sellers in control. {imb:+.1f}% ask dominant. "
        "Sell-side wall capping immediate rallies."
        if imb < -5 else
        f"Balanced order book ({imb:+.1f}%). Neither side defending aggressively; "
        "defer to taker flow for directional edge."
    )
    return {
        "bid_vol_btc":    round(bv, 2),
        "ask_vol_btc":    round(av, 2),
        "imbalance_pct":  round(imb, 2),
        "signal":         sig,
        "interpretation": interp,
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
    r_sig = (
        "BEARISH_CONTRARIAN" if lsr > 1.35 else
        "BULLISH_CONTRARIAN" if lsr < 0.75 else
        "NEUTRAL"
    )
    s_sig = "BULLISH" if tlp > 60 else "BEARISH" if tlp < 40 else "NEUTRAL"
    if abs(div) > 10:
        interp = (
            f"Smart money {tlp:.0f}% long vs retail {lp:.0f}% — "
            f"{'FOLLOW SMART MONEY LONG' if tlp > lp else 'FOLLOW SMART MONEY SHORT'} "
            f"({abs(div):.1f}% divergence). Retail is likely on the wrong side."
        )
    else:
        interp = (
            f"Smart money and retail aligned ({abs(div):.1f}% diff). "
            f"Both {tlp:.0f}%/{lp:.0f}% long — high-conviction bias, harder to fade."
        )
    return {
        "retail_lsr":                  round(lsr, 4),
        "retail_long_pct":             round(lp,  1),
        "retail_short_pct":            round(sp,  1),
        "smart_money_long_pct":        round(tlp, 1),
        "smart_money_short_pct":       round(tsp, 1),
        "retail_signal_contrarian":    r_sig,
        "smart_money_signal":          s_sig,
        "smart_vs_retail_div_pct":     round(div, 1),
        "interpretation":              interp,
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
    try:
        data = await _get(
            "https://www.okx.com/api/v5/public/liquidation-orders"
            "?instType=SWAP&mgnMode=cross&instId=BTC-USDT-SWAP&state=filled&limit=100"
        )
    except Exception as _e:
        logger.debug("OKX liquidations failed: %s", _e)
        data = {}
    rows = []
    for event in (data.get("data") or []):
        for detail in (event.get("details") or []):
            rows.append(detail)
    if not rows:
        return {
            "total": 0, "long_liq_count": 0, "short_liq_count": 0,
            "long_liq_usd": 0, "short_liq_usd": 0,
            "velocity_per_min": 0.0,
            "signal": "NEUTRAL",
            "interpretation": "No recent liquidations — stable market, no cascades detected.",
        }
    now_ms = time.time() * 1000
    cutoff = now_ms - 300_000
    recent = [r for r in rows if float(r.get("ts", 0)) >= cutoff]
    window = recent if recent else rows
    # OKX: posSide="long" + side="sell" → forced long liquidation (bearish)
    #       posSide="short" + side="buy" → forced short liquidation (bullish squeeze)
    longs  = [r for r in window if r.get("posSide", "").lower() == "long"]
    shorts = [r for r in window if r.get("posSide", "").lower() == "short"]
    lvol   = sum(float(r.get("sz", 0)) * float(r.get("bkPx", 0)) for r in longs)
    svol   = sum(float(r.get("sz", 0)) * float(r.get("bkPx", 0)) for r in shorts)
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
    interp = (
        f"Extreme Fear ({v}). Historically precedes sharp bounces. "
        "Retail capitulation = smart money accumulation zone. Contrarian LONG signal."
        if v < 30 else
        f"Extreme Greed ({v}). Elevated reversal risk — smart money fades retail euphoria. "
        "Contrarian SHORT lean; watch for distribution signals."
        if v > 75 else
        f"Neutral sentiment ({v} — {label}). No extreme contrarian edge. "
        "Weight technical and flow signals more heavily."
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
    threshold_btc = 2.0
    buy_vol = sell_vol = 0.0
    source = "Kraken"
    try:
        data = await _get("https://api.kraken.com/0/public/Trades?pair=XBTUSD")
        trades = (data.get("result") or {}).get("XXBTZUSD", [])
        if not trades:
            raise ValueError("Empty Kraken trades response")
        for t in trades:
            # Format: [price, volume, time, buy/sell("b"/"s"), market/limit, misc, trade_id]
            vol = float(t[1])
            if vol < threshold_btc:
                continue
            if t[3] == "b":
                buy_vol += vol
            else:
                sell_vol += vol
    except Exception as _e:
        logger.debug("Kraken spot whale failed, using Binance fallback: %s", _e)
        source = "Binance"
        data = await _get("https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=500")
        for t in data:
            vol = float(t["qty"])
            if vol < threshold_btc:
                continue
            if not t["isBuyerMaker"]:
                buy_vol += vol
            else:
                sell_vol += vol
    total   = buy_vol + sell_vol
    buy_pct = buy_vol / total * 100 if total > 0 else 50.0
    sig = "BULLISH" if buy_pct > 60 else "BEARISH" if buy_pct < 40 else "NEUTRAL"
    interp = (
        f"{source} spot whale buyers dominate: {buy_pct:.1f}% ({buy_vol:.1f} BTC) of large "
        f"spot trades are buys. Genuine spot accumulation — no leverage involved."
        if buy_pct > 60 else
        f"{source} spot whale sellers dominate: only {buy_pct:.1f}% buys ({sell_vol:.1f} BTC "
        "sold). Spot distribution — real holders exiting."
        if buy_pct < 40 else
        f"{source} spot whales balanced ({buy_pct:.1f}% buys, {total:.1f} BTC in large trades). "
        "No clear direction from block orders."
    )
    return {
        "whale_buy_btc":   round(buy_vol, 2),
        "whale_sell_btc":  round(sell_vol, 2),
        "whale_buy_pct":   round(buy_pct, 1),
        "large_trade_btc": round(total, 2),
        "signal":          sig,
        "interpretation":  interp,
    }


async def _fetch_bybit_liquidations() -> Dict:
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
    longs  = [r for r in recent if r.get("posSide", "").lower() == "long"]
    shorts = [r for r in recent if r.get("posSide", "").lower() == "short"]
    l_usd  = sum(float(r.get("sz", 0)) * float(r.get("bkPx", 0)) for r in longs)
    s_usd  = sum(float(r.get("sz", 0)) * float(r.get("bkPx", 0)) for r in shorts)
    sig    = "BEARISH" if l_usd > s_usd * 1.5 else "BULLISH" if s_usd > l_usd * 1.5 else "NEUTRAL"
    interp = (
        f"Isolated-margin long cascade: ${l_usd:,.0f} longs liquidated vs ${s_usd:,.0f} shorts. "
        "Cross-margin confirmation of downward cascade."
        if l_usd > s_usd * 1.5 else
        f"Isolated-margin short squeeze: ${s_usd:,.0f} shorts force-covered vs ${l_usd:,.0f} longs. "
        "Cross-margin squeeze confirmation — forced buying pressure."
        if s_usd > l_usd * 1.5 else
        f"Isolated-margin mixed liqs: ${l_usd:,.0f} long / ${s_usd:,.0f} short. "
        "No directional cascade in isolated margin book."
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
    interp = (
        f"Top traders {lp:.0f}% long by position notional (ratio {lsr:.3f}){_src}. "
        "Smart-money heavily positioned long — high-conviction directional bias."
        if lsr > 1.3 else
        f"Top traders only {lp:.0f}% long by position notional (ratio {lsr:.3f}){_src}. "
        "Smart-money short-positioned — bearish notional bias."
        if lsr < 0.77 else
        f"Top traders {lp:.0f}% long by notional (ratio {lsr:.3f}){_src}. "
        "No extreme positioning by large accounts."
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
        "bybit_liquidations": _fetch_bybit_liquidations(),
        "okx_funding":        _fetch_okx_funding(),
        "btc_dominance":      _fetch_btc_dominance(),
        "top_position_ratio": _fetch_top_position_ratio(),
        "funding_trend":      _fetch_funding_trend(),
        "deribit_dvol":       _fetch_deribit_dvol(),
        "deribit_options":    _fetch_deribit_options(),
        "btc_onchain":        _fetch_btc_onchain(),
    }
    if coinalyze_key:
        tasks["coinalyze"] = _fetch_coinalyze(coinalyze_key)
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
            result[key] = val

    result["fetched_at"] = time.time()
    n_ok = sum(1 for v in result.values() if v is not None and not isinstance(v, float))
    logger.info("Dashboard signals fetched: %d/%d ok", n_ok, len(keys))
    return result
