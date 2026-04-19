"""
AI Pipeline
===========
All three DeepSeek calls that fire each 5-minute bar, plus the full prompt
builder and response parser.

Call order per bar:
  1. run_specialists()        — OHLCV → 5 specialist signals + creative_edge
  2. run_historical_analyst() — all resolved bars + current context → free-form analysis
  3. DeepSeekPredictor.predict() — everything → final UP/DOWN/NEUTRAL + reasoning

Prompt files loaded from:
  specialists/unified_analyst/PROMPT.md
  specialists/historical_analyst/PROMPT.md

Debug output written to specialists/*/last_*.txt for inspection.

Public exports:
  SPECIALIST_KEYS            — set of strategy keys from the specialist call
  run_specialists(api_key, klines)  -> (strategy_dict, creative_edge)
  run_historical_analyst(...)       -> str | None
  DeepSeekPredictor                 — main prediction class
  build_prompt(...)                 -> str
  parse_response(text)              -> (signal, confidence, reasoning, ...)
"""

import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np

logger = logging.getLogger(__name__)

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-reasoner"

COHERE_EMBED_URL  = "https://api.cohere.com/v2/embed"
COHERE_RERANK_URL = "https://api.cohere.com/v2/rerank"
COHERE_EMBED_MODEL  = "embed-english-v3.0"
COHERE_RERANK_MODEL = "rerank-english-v3.0"
COHERE_PRE_FILTER_K = 50   # cosine candidates before reranking
COHERE_FINAL_K      = 20   # final bars sent to LLM after reranking

_ROOT = Path(__file__).parent   # btc-predictor/

SPECIALIST_KEYS = {"alligator", "acc_dist", "dow_theory", "fib_pullback", "harmonic"}


class CohereUnavailableError(RuntimeError):
    """Raised when Cohere API is unreachable or returns an error. No fallback — app pauses."""

_DAYS     = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_SESSIONS = [(0, 8, "ASIA"), (8, 13, "LONDON"), (13, 16, "OVERLAP"), (16, 21, "NY"), (21, 24, "LATE")]


# ═══════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════

def _save(path: Path, content: str):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    except Exception as exc:
        logger.warning("Could not save %s: %s", path.name, exc)


def _append(path: Path, content: str):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(content + "\n")
    except Exception as exc:
        logger.warning("Could not append %s: %s", path.name, exc)


async def _api_call(
    api_key: str,
    prompt: str,
    max_tokens: int = 1000,
    timeout_s: float = 90.0,
) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model":       DEEPSEEK_MODEL,
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  max_tokens,
        "temperature": 0.1,
    }
    timeout   = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(DEEPSEEK_API_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {body[:300]}")
            data = await resp.json(content_type=None)
            return data["choices"][0]["message"]["content"]


# ═══════════════════════════════════════════════════════════════
# Cohere embed + rerank
# ═══════════════════════════════════════════════════════════════

async def embed_text(cohere_key: str, text: str, input_type: str = "search_document") -> np.ndarray:
    """
    Embed text via Cohere embed-english-v3.0 (1024 dims).
    Raises CohereUnavailableError on any failure — no fallback.
    input_type: "search_document" when indexing a bar, "search_query" when querying.
    """
    if not cohere_key:
        raise CohereUnavailableError("COHERE_API_KEY not configured")
    headers = {
        "Authorization": f"Bearer {cohere_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "model": COHERE_EMBED_MODEL,
        "texts": [text],
        "input_type": input_type,
        "embedding_types": ["float"],
    }
    timeout   = aiohttp.ClientTimeout(total=30.0)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(COHERE_EMBED_URL, headers=headers, json=payload) as resp:
                body = await resp.text()
                if resp.status != 200:
                    raise CohereUnavailableError(f"Cohere embed HTTP {resp.status}: {body[:300]}")
                data = await resp.json(content_type=None)
                vec  = np.array(data["embeddings"]["float"][0], dtype=np.float32)
                norm = np.linalg.norm(vec)
                if norm < 1e-8:
                    raise CohereUnavailableError("Cohere returned zero-norm embedding")
                return vec / norm
    except CohereUnavailableError:
        raise
    except Exception as exc:
        raise CohereUnavailableError(f"Cohere embed request failed: {exc}") from exc


async def rerank_bars(
    cohere_key: str,
    query_text: str,
    candidate_texts: List[str],
    top_n: int = COHERE_FINAL_K,
) -> List[int]:
    """
    Re-rank candidate bar texts against the current bar query via Cohere Rerank v3.
    Returns list of original indices in ranked order (best first).
    Raises CohereUnavailableError on any failure.
    """
    if not cohere_key:
        raise CohereUnavailableError("COHERE_API_KEY not configured")
    if not candidate_texts:
        return []
    headers = {
        "Authorization": f"Bearer {cohere_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model":     COHERE_RERANK_MODEL,
        "query":     query_text[:2048],
        "documents": [t[:1024] for t in candidate_texts],
        "top_n":     min(top_n, len(candidate_texts)),
    }
    timeout   = aiohttp.ClientTimeout(total=30.0)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(COHERE_RERANK_URL, headers=headers, json=payload) as resp:
                body = await resp.text()
                if resp.status != 200:
                    raise CohereUnavailableError(f"Cohere rerank HTTP {resp.status}: {body[:300]}")
                data = await resp.json(content_type=None)
                return [r["index"] for r in data["results"]]
    except CohereUnavailableError:
        raise
    except Exception as exc:
        raise CohereUnavailableError(f"Cohere rerank request failed: {exc}") from exc


# ═══════════════════════════════════════════════════════════════
# SECTION 1 — Prompt builder & response parser
# ═══════════════════════════════════════════════════════════════

N_CANDLES = 100


def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    k = 2 / (period + 1)
    out = np.empty(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _smma(arr: np.ndarray, period: int) -> np.ndarray:
    k = 1.0 / period
    out = np.empty(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _smma_val(arr: np.ndarray, period: int) -> float:
    if len(arr) < period:
        return float(arr[-1])
    k = 1.0 / period
    val = float(arr[0])
    for v in arr[1:]:
        val = float(v) * k + val * (1.0 - k)
    return val


def _rsi(closes: np.ndarray, period: int = 4) -> np.ndarray:
    out = np.full(len(closes), 50.0)
    if len(closes) <= period:
        return out
    deltas = np.diff(closes.astype(float))
    gains  = np.maximum(deltas, 0.0)
    losses = np.maximum(-deltas, 0.0)
    avg_g = float(gains[:period].mean())
    avg_l = float(losses[:period].mean())
    def _rv(g, l):
        if l == 0: return 100.0 if g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + g / l)
    out[period] = _rv(avg_g, avg_l)
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        out[i + 1] = _rv(avg_g, avg_l)
    return out


def _mfi_series(highs, lows, closes, vols, period=4):
    n = len(closes)
    out = np.full(n, 50.0)
    if n <= period:
        return out
    tp  = (highs + lows + closes) / 3.0
    rmf = tp * vols
    dtp = np.diff(tp)
    pos = float(np.sum(rmf[1:period+1][dtp[:period] > 0]))
    neg = float(np.sum(rmf[1:period+1][dtp[:period] < 0]))
    def _mfi_val(p, ng):
        if ng == 0: return 100.0 if p > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + p / ng)
    out[period] = _mfi_val(pos, neg)
    for i in range(period, n - 1):
        new_pos = float(rmf[i+1]) if dtp[i] > 0 else 0.0
        new_neg = float(rmf[i+1]) if dtp[i] < 0 else 0.0
        pos = (pos * (period - 1) + new_pos) / period
        neg = (neg * (period - 1) + new_neg) / period
        out[i+1] = _mfi_val(pos, neg)
    return out


def _linreg(y: np.ndarray) -> Tuple[float, float, float]:
    n = len(y)
    if n < 2:
        return 0.0, float(y[0]) if n else 0.0, 0.0
    x     = np.arange(n, dtype=float)
    m     = (n * x.dot(y) - x.sum() * y.sum()) / (n * x.dot(x) - x.sum() ** 2)
    b     = (y.sum() - m * x.sum()) / n
    ss_res = np.sum((y - (m * x + b)) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2     = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
    return m, b, max(0.0, r2)


def _find_swings(highs, lows, n=3):
    sh, sl = [], []
    for i in range(n, len(highs) - n):
        if highs[i] >= highs[i-n:i+n+1].max():
            sh.append((i, float(highs[i])))
        if lows[i] <= lows[i-n:i+n+1].min():
            sl.append((i, float(lows[i])))
    return sh, sl


def _cluster(levels, tol_pct=0.15):
    if not levels:
        return []
    lvls = sorted(levels)
    groups = [[lvls[0]]]
    for v in lvls[1:]:
        if (v - groups[-1][-1]) / groups[-1][-1] * 100 < tol_pct:
            groups[-1].append(v)
        else:
            groups.append([v])
    return [float(np.mean(g)) for g in groups]


def _market_structure(sh, sl):
    if len(sh) < 2 or len(sl) < 2:
        return "INSUFFICIENT DATA"
    h1, h2 = sh[-2][1], sh[-1][1]
    l1, l2 = sl[-2][1], sl[-1][1]
    hh, hl = h2 > h1, l2 > l1
    lh, ll = h2 < h1, l2 < l1
    if hh and hl: return "UPTREND  (HH + HL)"
    if lh and ll: return "DOWNTREND  (LH + LL)"
    if hh and ll: return "EXPANDING RANGE  (HH + LL)"
    if lh and hl: return "CONTRACTING RANGE  (LH + HL)"
    return "MIXED / RANGING"


def _trend_label(slope_per_bar, r2, price):
    slope_pct = slope_per_bar / price * 100
    strength  = "strong" if r2 > 0.70 else "moderate" if r2 > 0.35 else "weak"
    direction = "up" if slope_pct > 0.001 else "down" if slope_pct < -0.001 else "flat"
    return f"{direction.upper()}  {strength}  (slope {slope_pct:+.4f}%/bar  R²={r2:.2f})"


def _analyse_structure(klines, tick_prices):
    use_klines = bool(klines and len(klines) >= 20)
    if use_klines:
        rows   = klines[-N_CANDLES:]
        closes = np.array([float(r[4]) for r in rows])
        highs  = np.array([float(r[2]) for r in rows])
        lows   = np.array([float(r[3]) for r in rows])
        vols   = np.array([float(r[5]) for r in rows])
    else:
        closes = np.array(tick_prices[-N_CANDLES:])
        highs  = closes * 1.0001
        lows   = closes * 0.9999
        vols   = np.ones(len(closes))

    n   = len(closes)
    mid = float(closes[-1])
    sh_list, sl_list = _find_swings(highs, lows, n=3)
    res_raw = [p for _, p in sh_list if p > mid]
    sup_raw = [p for _, p in sl_list if p < mid]
    res = sorted(_cluster(res_raw))[:4]
    sup = sorted(_cluster(sup_raw))[-4:]
    structure = _market_structure(sh_list, sl_list)
    macro_n   = min(80, n); m_mac, _, r2_mac = _linreg(closes[-macro_n:])
    micro_n   = min(20, n); m_mic, _, r2_mic = _linreg(closes[-micro_n:])
    range_high = float(highs.max()); range_low = float(lows.min())
    range_pos  = (mid - range_low) / (range_high - range_low) if range_high > range_low else 0.5
    vol_recent = vols[-5:].mean()  if len(vols) >= 5  else vols.mean()
    vol_base   = vols[-25:-5].mean() if len(vols) >= 25 else vols.mean()
    vol_trend  = ("INCREASING" if vol_recent > vol_base * 1.1 else
                  "DECREASING" if vol_recent < vol_base * 0.9 else "STEADY")
    return dict(
        mid=mid, structure=structure,
        macro_label=_trend_label(m_mac, r2_mac, mid),
        micro_label=_trend_label(m_mic, r2_mic, mid),
        macro_slope_pct=m_mac / mid * 100, micro_slope_pct=m_mic / mid * 100,
        macro_r2=r2_mac, micro_r2=r2_mic,
        res=res, sup=sup,
        range_high=range_high, range_low=range_low, range_pos=range_pos,
        vol_trend=vol_trend, n_bars=n,
    )


def _tag(val, hi, lo, hi_lbl, lo_lbl):
    if val >= hi: return f"⚠ {hi_lbl}"
    if val <= lo: return f"⚠ {lo_lbl}"
    return "–"


def _fmt_usd(n):
    if n >= 1e9: return f"${n/1e9:.2f}B"
    if n >= 1e6: return f"${n/1e6:.1f}M"
    if n >= 1e3: return f"${n/1e3:.1f}K"
    return f"${n:.0f}"


def _build_dashboard_block(ds, window_start_price, dashboard_accuracy=None):
    if not ds:
        return "  (dashboard signals unavailable this bar)"

    def _acc_tag(name):
        if not dashboard_accuracy: return ""
        stats = dashboard_accuracy.get(name)
        if not stats or stats.get("total", 0) < 5: return "  [acc: learning]"
        acc = stats["accuracy"] * 100
        cor = stats["correct"]; tot = stats["total"]
        label = ("EXCELLENT" if acc >= 65 else "RELIABLE" if acc >= 55 else
                 "MARGINAL"  if acc >= 48 else "WEAK")
        return f"  [acc {acc:.0f}% {cor}/{tot} — {label}]"

    def _s(key, sub, default="N/A"):
        v = ds.get(key)
        return str(v.get(sub, default)) if v else default

    def _f(key, sub, default=0.0):
        v = ds.get(key)
        if v is None: return default
        try: return float(v.get(sub, default))
        except: return default

    oif = ds.get("oi_funding") or {}
    lines = []

    ob = ds.get("order_book")
    if ob:
        imb = ob.get("imbalance_pct", 0); bv = ob.get("bid_vol_btc", 0); av = ob.get("ask_vol_btc", 0)
        lines += [
            "  [ORDER BOOK DEPTH — Binance spot, top-20 levels]",
            f"  Bid volume  : {bv:.1f} BTC    Ask volume : {av:.1f} BTC",
            f"  Imbalance   : {imb:+.2f}%   Signal: {ob.get('signal','NEUTRAL')}{_acc_tag('order_book')}",
            f"  → {ob.get('interpretation','')}", "",
        ]
    else:
        lines += ["  [ORDER BOOK] unavailable", ""]

    ls = ds.get("long_short")
    if ls:
        lines += [
            "  [LONG / SHORT RATIO — Binance Futures 5m]",
            f"  Retail     : L/S = {ls.get('retail_lsr',1):.3f}   Long {ls.get('retail_long_pct',50):.1f}%  /  Short {ls.get('retail_short_pct',50):.1f}%   "
            f"Contrarian signal: {ls.get('retail_signal_contrarian','NEUTRAL')}{_acc_tag('long_short')}",
            f"  Smart money: Long {ls.get('smart_money_long_pct',50):.1f}%  /  Short {ls.get('smart_money_short_pct',50):.1f}%   "
            f"Signal: {ls.get('smart_money_signal','NEUTRAL')}   Divergence vs retail: {ls.get('smart_vs_retail_div_pct',0):+.1f}%",
            f"  → {ls.get('interpretation','')}", "",
        ]
    else:
        lines += ["  [LONG/SHORT RATIO] unavailable", ""]

    tk = ds.get("taker_flow")
    if tk:
        lines += [
            "  [TAKER AGGRESSOR FLOW — Binance Futures 5m, last 3 bars]",
            f"  Buy/Sell ratio : {tk.get('buy_sell_ratio',1):.4f}   Taker buys: {tk.get('taker_buy_vol_btc',0):.1f} BTC  "
            f"Taker sells: {tk.get('taker_sell_vol_btc',0):.1f} BTC",
            f"  Signal: {tk.get('signal','NEUTRAL')}{_acc_tag('taker_flow')}    3-bar trend: {tk.get('trend_3bars','MIXED')}",
            f"  → {tk.get('interpretation','')}", "",
        ]
    else:
        lines += ["  [TAKER FLOW] unavailable", ""]

    lq = ds.get("liquidations")
    if lq:
        lines += [
            "  [LIQUIDATIONS — Binance Futures, last 5 min]",
            f"  Long liquidated : {lq.get('long_liq_count',0)} orders  ({_fmt_usd(lq.get('long_liq_usd',0))})    "
            f"Short liquidated: {lq.get('short_liq_count',0)} orders  ({_fmt_usd(lq.get('short_liq_usd',0))})",
            f"  Velocity: {lq.get('velocity_per_min',0):.1f}/min   Price range: {lq.get('price_range','N/A')}   Signal: {lq.get('signal','NEUTRAL')}{_acc_tag('liquidations')}",
            f"  → {lq.get('interpretation','')}", "",
        ]
    else:
        lines += ["  [LIQUIDATIONS] unavailable", ""]

    if oif:
        lines += [
            "  [OPEN INTEREST + FUNDING — Binance Futures perpetual]",
            f"  OI: {oif.get('open_interest_btc',0):,.0f} BTC    Funding (8h): {oif.get('funding_rate_8h_pct',0):+.5f}%  [{oif.get('funding_signal','NEUTRAL')}{_acc_tag('oi_funding')}]",
            f"  Mark: ${oif.get('mark_price',0):,.2f}   Index: ${oif.get('index_price',0):,.2f}   Mark premium: {oif.get('mark_premium_vs_index_pct',0):+.4f}%  [{oif.get('premium_signal','NEUTRAL')}]",
            "",
        ]
    else:
        lines += ["  [OI + FUNDING] unavailable", ""]

    cz = ds.get("coinalyze")
    if cz:
        bn_fr = oif.get("funding_rate_8h_pct") if oif else None
        delta = f"  Δ vs Binance: {cz.get('funding_rate_8h_pct',0) - bn_fr:+.5f}%" if bn_fr is not None else ""
        lines += [
            "  [COINALYZE — Cross-exchange aggregate funding (BTCUSDT perp)]",
            f"  Aggregate funding (8h): {cz.get('funding_rate_8h_pct',0):+.5f}%   Signal: {cz.get('signal','NEUTRAL')}{_acc_tag('coinalyze')}{delta}",
            f"  → {cz.get('interpretation','')}", "",
        ]

    dv = ds.get("deribit_dvol")
    if dv:
        lines += [
            "  [DERIBIT DVOL — BTC 30-day implied volatility index]",
            f"  DVOL: {dv.get('dvol_pct', 0):.1f}%   Signal: {dv.get('signal', 'NEUTRAL')}{_acc_tag('deribit_dvol')}",
            f"  → {dv.get('interpretation', '')}", "",
        ]

    fg = ds.get("fear_greed")
    if fg:
        lines += [
            "  [FEAR & GREED INDEX — alternative.me, daily]",
            f"  Score : {fg.get('value',50)}  ({fg.get('label','Neutral')})   Yesterday: {fg.get('previous_day',50)}   Daily Δ: {fg.get('daily_delta',0):+d}",
            f"  Signal: {fg.get('signal','NEUTRAL')}{_acc_tag('fear_greed')}",
            f"  → {fg.get('interpretation','')}", "",
        ]
    else:
        lines += ["  [FEAR & GREED] unavailable", ""]

    cg = ds.get("coingecko")
    if cg:
        lines += [
            "  [COINGECKO MARKET OVERVIEW]",
            f"  Market cap : {_fmt_usd(cg.get('market_cap_usd',0))}   24h vol : {_fmt_usd(cg.get('volume_24h_usd',0))}   "
            f"Vol/MCap: {cg.get('vol_to_mcap_ratio_pct',0):.2f}%",
            f"  24h change : {cg.get('change_24h_pct',0):+.3f}%{_acc_tag('coingecko')}",
            f"  → {cg.get('interpretation','')}", "",
        ]

    mp = ds.get("mempool")
    if mp:
        lines += [
            "  [MEMPOOL — mempool.space on-chain fee pressure]",
            f"  Fastest: {mp.get('fastest_fee_sat_vb',0)} sat/vB   30min: {mp.get('half_hour_fee_sat_vb',0)} sat/vB   1hr: {mp.get('hour_fee_sat_vb',0)} sat/vB",
            f"  Pending: {mp.get('pending_tx_count',0):,} txs   Size: {mp.get('mempool_size_mb',0):.2f} MB   Signal: {mp.get('signal','NEUTRAL')}{_acc_tag('mempool')}",
            f"  → {mp.get('interpretation','')}", "",
        ]

    for key, label in [
        ("kraken_premium",     "[KRAKEN PREMIUM — Kraken vs OKX institutional spread]"),
        ("oi_velocity",        "[OI VELOCITY — Binance Futures OI change rate over 30 min]"),
        ("spot_whale_flow",    "[SPOT WHALE FLOW — Binance spot aggTrades ≥5 BTC]"),
        ("bybit_liquidations", "[BYBIT LIQUIDATIONS — cross-exchange cascade validation]"),
        ("okx_funding",        "[OKX FUNDING RATE — independent cross-exchange funding]"),
        ("btc_dominance",      "[BTC DOMINANCE — CoinGecko global market share]"),
        ("top_position_ratio", "[TOP TRADER POSITION RATIO — Binance Futures notional-weighted]"),
        ("funding_trend",      "[FUNDING RATE TREND — Binance 6-period history]"),
    ]:
        v = ds.get(key)
        if v:
            sig = v.get("signal", "NEUTRAL")
            interp = v.get("interpretation", "")
            lines += [f"  {label}", f"  Signal: {sig}{_acc_tag(key)}", f"  → {interp}", ""]
        else:
            lines += [f"  {label.replace('[','[').replace(']',']')} unavailable", ""]

    opt = ds.get("deribit_options")
    if opt:
        lines += [
            "  [DERIBIT OPTIONS — BTC put/call OI ratio + max pain]",
            f"  Put OI: {opt.get('put_oi_btc',0):,.0f} BTC   Call OI: {opt.get('call_oi_btc',0):,.0f} BTC   "
            f"P/C ratio: {opt.get('put_call_ratio',1):.3f}",
            f"  Max pain: ${opt.get('max_pain_usd',0):,.0f}  ({opt.get('dist_to_pain_pct',0):+.1f}% from spot)   "
            f"Signal: {opt.get('signal','NEUTRAL')}{_acc_tag('deribit_options')}",
            f"  → {opt.get('interpretation','')}", "",
        ]
    else:
        lines += ["  [DERIBIT OPTIONS] unavailable", ""]

    oc = ds.get("btc_onchain")
    if oc:
        lines += [
            f"  [BTC ON-CHAIN — BGMetrics daily, as of {oc.get('sopr_date','N/A')}]",
            f"  SOPR: {oc.get('sopr',1):.5f}  Signal: {oc.get('sopr_signal','NEUTRAL')}{_acc_tag('btc_onchain')}",
            f"  → {oc.get('sopr_interpretation','')}",
            f"  MVRV Z-Score: {oc.get('mvrv_zscore',0):.4f}  Signal: {oc.get('mvrv_signal','NEUTRAL')}",
            f"  → {oc.get('mvrv_interpretation','')}", "",
        ]
    else:
        lines += ["  [BTC ON-CHAIN] unavailable", ""]

    cgl = ds.get("coinglass_liquidations")
    if cgl:
        lines += [
            "  [COINGLASS LIQUIDATIONS — cross-exchange aggregate (5min)]",
            f"  Long liq: {_fmt_usd(cgl.get('long_liq_usd',0))}   Short liq: {_fmt_usd(cgl.get('short_liq_usd',0))}   "
            f"Signal: {cgl.get('signal','NEUTRAL')}{_acc_tag('coinglass_liquidations')}",
            f"  → {cgl.get('interpretation','')}", "",
        ]

    return "\n".join(lines).rstrip()


def _build_dashboard_accuracy_block(dashboard_accuracy):
    if not dashboard_accuracy:
        return "  (no microstructure history yet)"
    _NAMES = {
        "order_book": "Order Book", "long_short": "Long/Short", "taker_flow": "Taker Flow",
        "oi_funding": "OI + Funding", "liquidations": "Liquidations", "fear_greed": "Fear & Greed",
        "mempool": "Mempool", "coinalyze": "Coinalyze", "coingecko": "CoinGecko",
        "deribit_dvol": "Deribit DVOL",
        "kraken_premium": "Kraken Premium", "oi_velocity": "OI Velocity",
        "spot_whale_flow": "Spot Whale Flow", "bybit_liquidations": "Bybit Liquidations",
        "okx_funding": "OKX Funding", "btc_dominance": "BTC Dominance",
        "top_position_ratio": "Top Position Ratio", "funding_trend": "Funding Rate Trend",
        "deribit_options": "Deribit Options P/C", "btc_onchain": "BTC On-Chain SOPR",
        "coinglass_liquidations": "CoinGlass Liquidations",
    }
    lines = []
    for key in _NAMES:
        stats = dashboard_accuracy.get(key)
        if not stats: continue
        total = stats.get("total", 0); correct = stats.get("correct", 0)
        acc   = stats.get("accuracy", 0.5)
        label = ("EXCELLENT" if acc >= 0.65 else "RELIABLE" if acc >= 0.55 else
                 "MARGINAL"  if acc >= 0.48 else "WEAK" if total >= 5 else "LEARNING")
        note  = "  ← HIGH TRUST" if label == "EXCELLENT" else "  ← LOW TRUST" if label == "WEAK" else ""
        lines.append(f"  {_NAMES[key]:<34} {acc*100:5.1f}%  ({correct}/{total})  [{label}]{note}")
    return "\n".join(lines) if lines else "  (no resolved microstructure data yet)"


def _build_indicator_track_record(indicator_accuracy, weights):
    from strategies import accuracy_to_label
    if not indicator_accuracy:
        return "  (no historical data yet — all indicators treated equally)"
    lines = []
    def sort_key(item):
        acc = item[1].get("accuracy", 0.5); tot = item[1].get("total", 0)
        label = accuracy_to_label(acc, tot)
        order = {"DISABLED": 0, "WEAK": 1, "LEARNING": 2, "MARGINAL": 3, "RELIABLE": 4, "EXCELLENT": 5}
        return order.get(label, 2), acc
    for name, stats in sorted(indicator_accuracy.items(), key=sort_key):
        correct = stats.get("correct", 0); total = stats.get("total", 0)
        accuracy = stats.get("accuracy", 0.5); w = (weights or {}).get(name, 1.0)
        label = accuracy_to_label(accuracy, total)
        note = ""
        if label == "DISABLED":   note = "  ← IGNORE — worse than coin flip"
        elif label == "WEAK":     note = "  ← LOW TRUST — below 50%"
        elif label == "EXCELLENT": note = "  ← HIGH TRUST — consistently outperforms"
        elif label == "RELIABLE":  note = "  ← TRUST — above-average track record"
        lines.append(
            f"  {name:<22} {accuracy*100:5.1f}%  ({correct}/{total})  weight={w:.2f}  [{label}]{note}"
        )
    return "\n".join(lines) if lines else "  (no resolved predictions yet)"


def build_prompt(
    prices, klines, features, strategy_preds, recent_accuracy, window_num,
    deepseek_accuracy, window_start_price, window_start_time,
    polymarket_slug=None, ensemble_result=None, dashboard_signals=None,
    indicator_accuracy=None, ensemble_weights=None, historical_analysis=None,
    creative_edge=None, dashboard_accuracy=None, neutral_analysis=None,
) -> str:
    f  = features
    fv = lambda k, d=0.0: f.get(k, d)

    def _strat_val(key, default):
        s = strategy_preds.get(key, {})
        try: return float(s.get("value", ""))
        except: return default

    now  = prices[-1]
    p1m  = prices[-30]  if len(prices) >= 30  else prices[0]
    p5m  = prices[-150] if len(prices) >= 150 else prices[0]
    p15m = prices[-450] if len(prices) >= 450 else prices[0]
    def pct(a, b): return ((a / b) - 1) * 100 if b else 0

    sa = _analyse_structure(klines, prices)
    res, sup = sa["res"], sa["sup"]
    level_lines = []
    for lvl in reversed(res):
        level_lines.append(f"  RESISTANCE  ${lvl:>10,.2f}   (+{(lvl-now)/now*100:.2f}%)")
    level_lines.append(f"  ── current  ${now:>10,.2f} ──")
    for lvl in reversed(sup):
        level_lines.append(f"  SUPPORT     ${lvl:>10,.2f}   (-{(now-lvl)/now*100:.2f}%)")
    levels_block = "\n".join(level_lines)

    mac_up = sa["macro_slope_pct"] > 0; mic_up = sa["micro_slope_pct"] > 0
    if mac_up and mic_up:      alignment = "macro UP + micro UP → momentum aligned bullish"
    elif mac_up and not mic_up: alignment = "macro UP + micro DOWN → pullback in uptrend"
    elif not mac_up and mic_up: alignment = "macro DOWN + micro UP → bounce in downtrend"
    else:                       alignment = "macro DOWN + micro DOWN → momentum aligned bearish"

    rp = sa["range_pos"]
    rp_str = "upper third" if rp > 0.66 else "lower third" if rp < 0.33 else "mid range"

    dashboard_block = _build_dashboard_block(dashboard_signals, window_start_price, dashboard_accuracy)

    rsi4   = _strat_val("rsi", fv("rsi_7", 50))
    stoch  = _strat_val("stochastic", fv("stoch_k_14", 50))
    mfi4   = _strat_val("mfi", fv("mfi_7", 50))
    macd   = fv("macd"); macd_h = fv("macd_histogram")
    bb_pct = fv("bollinger_pct_b", 0.5); bb_w = fv("bollinger_width")
    vol10  = fv("volatility_10")
    ret1   = fv("return_1"); ret5 = fv("return_5"); ret10 = fv("return_10")
    macd_dir = "bullish" if macd_h > 0 else "bearish"
    macd_exp = ("expanding" if abs(macd_h) > abs(fv("macd_histogram_prev", macd_h)) else "contracting")

    ema_s         = strategy_preds.get("ema_cross", {})
    ema_fast_diff = float(ema_s.get("value", 0) or 0)
    ema_slow_diff = float(ema_s.get("slow_diff", 0) or 0)
    ema_fast_sig  = ema_s.get("signal", "?")
    ema_slow_sig  = ema_s.get("htf_signal", "?")
    ema_agree     = (ema_fast_sig == ema_slow_sig
                     if ema_fast_sig not in ("?","N/A") and ema_slow_sig not in ("?","N/A") else None)

    alligator_block = "(no OHLCV)"
    if klines and len(klines) >= 15:
        kc = np.array([float(k[4]) for k in klines], dtype=float)
        jaw_v = _smma_val(kc, 13); tth_v = _smma_val(kc, 8); lip_v = _smma_val(kc, 5)
        allig_bull = lip_v > tth_v > jaw_v; allig_bear = lip_v < tth_v < jaw_v
        allig_state = ("BULLISH (Lips>Teeth>Jaw)" if allig_bull else
                       "BEARISH (Lips<Teeth<Jaw)" if allig_bear else "SLEEPING (ranging)")
        alligator_block = (f"  Jaw(13)={jaw_v:,.2f}  Teeth(8)={tth_v:,.2f}  Lips(5)={lip_v:,.2f}\n"
                           f"  State: {allig_state}")

    fib_block = "(no range data)"
    if klines and len(klines) >= 30:
        khigh = np.array([float(k[2]) for k in klines[-30:]])
        klow  = np.array([float(k[3]) for k in klines[-30:]])
        sw_hi = float(khigh.max()); sw_lo = float(klow.min()); rng30 = sw_hi - sw_lo
        if rng30 > 0:
            hi_idx = int(khigh.argmax()); lo_idx = int(klow.argmin())
            uptrend30 = hi_idx > lo_idx
            pb_pct = (sw_hi - now) / rng30 if uptrend30 else (now - sw_lo) / rng30
            fib_block = (
                f"  Swing H=${sw_hi:,.2f}  L=${sw_lo:,.2f}  range=${rng30:,.2f}\n"
                f"  Current pullback: {pb_pct*100:.1f}% ({'from high in uptrend' if uptrend30 else 'from low in downtrend'})\n"
                f"  Fib 38.2% = ${sw_hi - 0.382*rng30:,.2f}   50% = ${sw_hi - 0.500*rng30:,.2f}   61.8% = ${sw_hi - 0.618*rng30:,.2f}"
            )

    acc_dist_block = "(no OHLCV)"
    if klines and len(klines) >= 10:
        ad = 0.0; ad_vals = []
        for k in klines[-20:]:
            h, l, c, v = float(k[2]), float(k[3]), float(k[4]), float(k[5])
            ad += ((c - l) - (h - c)) / (h - l) * v if h != l else 0.0
            ad_vals.append(ad)
        ad_slope = ad_vals[-1] - ad_vals[-5] if len(ad_vals) >= 5 else 0.0
        acc_dist_block = f"  A/D={ad_vals[-1]:.0f}  slope(5)={ad_slope:+.0f}  {'ACCUMULATION ↑' if ad_slope > 0 else 'DISTRIBUTION ↓'}"

    csv_block = "(no kline data)"
    if klines and len(klines) >= 5:
        csv_rows = ["Time(UTC),Open,High,Low,Close,Volume,QuoteVol,Trades,BuyVol%"]
        for k in klines[-50:]:
            try:
                ts_str  = time.strftime("%m-%d %H:%M", time.gmtime(int(k[0]) / 1000))
                vol     = float(k[5])
                quote_v = float(k[7]) if len(k) > 7 else 0.0
                trades  = int(k[8])   if len(k) > 8 else 0
                buy_vol = float(k[9]) if len(k) > 9 else 0.0
                buy_pct = round(buy_vol / vol * 100, 1) if vol > 0 else 0.0
                csv_rows.append(
                    f"{ts_str},{float(k[1]):.2f},{float(k[2]):.2f},"
                    f"{float(k[3]):.2f},{float(k[4]):.2f},{vol:.1f},{quote_v:.0f},{trades},{buy_pct}"
                )
            except Exception:
                pass
        csv_block = "\n".join(csv_rows)

    bullish = sum(1 for p in strategy_preds.values() if p.get("signal") == "UP")
    bearish = sum(1 for p in strategy_preds.values() if p.get("signal") == "DOWN")
    strat_lines = [
        f"  {name:<18} {'↑' if p.get('signal')=='UP' else '↓'} {p.get('confidence',0)*100:4.0f}%  {(p.get('reasoning') or '')[:55]}"
        for name, p in strategy_preds.items()
    ]

    if ensemble_result:
        ensemble_block = (
            f"  Signal      : {ensemble_result.get('signal','?')}\n"
            f"  Confidence  : {ensemble_result.get('confidence',0)*100:.1f}%\n"
            f"  UP prob     : {ensemble_result.get('up_probability',0.5)*100:.1f}%\n"
            f"  Votes       : {ensemble_result.get('bullish_count',bullish)}↑ bullish  /  {ensemble_result.get('bearish_count',bearish)}↓ bearish\n"
            f"  Weighted UP : {ensemble_result.get('weighted_up_score',0):.3f}   Weighted DN : {ensemble_result.get('weighted_down_score',0):.3f}"
        )
    else:
        ensemble_block = f"  {bullish}↑ bullish  /  {bearish}↓ bearish  (no weighted result)"

    recent_accuracy = recent_accuracy or 0.0
    ds_total   = (deepseek_accuracy or {}).get("total", 0)
    ds_correct = (deepseek_accuracy or {}).get("correct", 0)
    ds_str     = (f"{ds_correct}/{ds_total}  ({(deepseek_accuracy or {}).get('accuracy', 0)*100:.1f}%)"
                  if ds_total > 0 else "no prior predictions")

    indicator_track_record  = _build_indicator_track_record(indicator_accuracy, ensemble_weights)
    dashboard_accuracy_block = _build_dashboard_accuracy_block(dashboard_accuracy)
    creative_block   = (creative_edge.strip() if creative_edge and creative_edge.strip()
                        else "  (no creative edge observation this window)")
    historical_block = (historical_analysis.strip() if historical_analysis and historical_analysis.strip()
                        else "  (historical analyst did not fire this window — no resolved bars yet)")

    # NEUTRAL abstention performance block
    na = neutral_analysis or {}
    na_total = na.get("total", 0)
    if na_total > 0:
        na_up   = na.get("market_went_up", 0)
        na_down = na.get("market_went_down", 0)
        pct_up  = na.get("pct_up", 0.0)
        pct_down = na.get("pct_down", 0.0)
        dominant = "UP" if na_up > na_down else ("DOWN" if na_down > na_up else "EVEN")
        neutral_block = (
            f"  Total NEUTRAL calls (abstentions): {na_total}\n"
            f"  After those abstentions the market went:\n"
            f"    UP   {na_up:>3} times  ({pct_up:.0f}%)\n"
            f"    DOWN {na_down:>3} times  ({pct_down:.0f}%)\n"
            f"  Dominant post-neutral direction: {dominant}\n"
            f"  Implication: in {max(pct_up, pct_down):.0f}% of your past abstentions,\n"
            f"  committing to {dominant} would have been the winning call.\n"
            f"  Consider this when deciding whether to abstain again now."
        )
    else:
        neutral_block = "  No NEUTRAL abstentions on record yet."

    ts_start = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(window_start_time))
    ts_end   = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time + 300))
    pm_url   = (f"https://polymarket.com/event/{polymarket_slug}"
                if polymarket_slug else "N/A (market slug not available)")
    n_bars_v = sa['n_bars']

    return f"""\
You are a professional price-action trader and narrative analyst for BTCUSDT, 1-minute candles.
All data below is REAL, computed from live Binance OHLCV + live market microstructure feeds.

══════════════════════════════════════════════
  WINDOW #{window_num}
  START : {ts_start}
  END   : {ts_end}  (5-minute window closes at this time)
  Entry price  : ${window_start_price:,.2f}
  QUESTION     : ABOVE or BELOW ${window_start_price:,.2f} at {ts_end}?

  Polymarket market  : {pm_url}
══════════════════════════════════════════════

──────────────────────────────────────────────
  PRICE STRUCTURE  (last {n_bars_v} bars)
──────────────────────────────────────────────
  Market structure (Dow Theory) : {sa['structure']}
  100-bar range    : ${sa['range_low']:,.2f} – ${sa['range_high']:,.2f}
  Position in range: {rp_str}  ({sa['range_pos']:.0%} from bottom)

  Macro trend (80 bars) : {sa['macro_label']}
  Micro trend (20 bars) : {sa['micro_label']}
  MTF alignment : {alignment}

──────────────────────────────────────────────
  KEY LEVELS  (swing-point clusters)
──────────────────────────────────────────────
{levels_block}

──────────────────────────────────────────────
  FIBONACCI PULLBACK  (last 30 bars)
──────────────────────────────────────────────
{fib_block}

──────────────────────────────────────────────
  WILLIAMS ALLIGATOR
──────────────────────────────────────────────
{alligator_block}

──────────────────────────────────────────────
  ACCUMULATION / DISTRIBUTION
──────────────────────────────────────────────
{acc_dist_block}

──────────────────────────────────────────────
  PRICE ACTION  (tick returns)
──────────────────────────────────────────────
  1m ago   ${p1m:,.2f}  ({pct(now,p1m):+.3f}%)
  5m ago   ${p5m:,.2f}  ({pct(now,p5m):+.3f}%)
  15m ago  ${p15m:,.2f}  ({pct(now,p15m):+.3f}%)
  ret[1t]  {ret1:+.4f}%   ret[5t] {ret5:+.4f}%   ret[10t] {ret10:+.4f}%
  Volume trend (recent vs prior) : {sa['vol_trend']}

──────────────────────────────────────────────
  OSCILLATORS & OVERLAYS  (period=4 for scalping)
──────────────────────────────────────────────
  RSI(4)     {rsi4:5.1f}   {_tag(rsi4, 80, 20, 'OVERBOUGHT', 'OVERSOLD')}
  MFI(4)     {mfi4:5.1f}   {_tag(mfi4, 80, 20, 'OVERBOUGHT', 'OVERSOLD')}
  Stoch %K   {stoch:5.1f}   {_tag(stoch, 80, 20, 'OVERBOUGHT', 'OVERSOLD')}
  MACD       {macd:+.5f}   hist {macd_h:+.5f}  [{macd_dir}, {macd_exp}]
  BB %B      {bb_pct:.4f}   width {bb_w:.5f}
  EMA5/13 (fast)  diff={ema_fast_diff:+.2f}  [{ema_fast_sig}]
  EMA21/55 (slow) diff={ema_slow_diff:+.2f}  [{ema_slow_sig}]{'  ✓ AGREE' if ema_agree else ('  ⚠ SPLIT' if ema_agree is not None else '')}
  Volatility {vol10:.4f}%

──────────────────────────────────────────────
  MARKET MICROSTRUCTURE  (live signals — most predictive for next 5 min)
──────────────────────────────────────────────
{dashboard_block}

──────────────────────────────────────────────
  ENSEMBLE WEIGHTED VOTE  (pre-computed, use as one input)
──────────────────────────────────────────────
{ensemble_block}

──────────────────────────────────────────────
  INDIVIDUAL STRATEGY SIGNALS  ({bullish}↑  /  {bearish}↓)
──────────────────────────────────────────────
{chr(10).join(strat_lines)}

──────────────────────────────────────────────
  SPECIALIST CREATIVE EDGE
──────────────────────────────────────────────
{creative_block}

──────────────────────────────────────────────
  MICROSTRUCTURE INDICATOR ACCURACY
──────────────────────────────────────────────
{dashboard_accuracy_block}

──────────────────────────────────────────────
  INDICATOR TRACK RECORD  (last ~100 resolved predictions)
──────────────────────────────────────────────
{indicator_track_record}

──────────────────────────────────────────────
  HISTORICAL SIMILARITY ANALYST
──────────────────────────────────────────────
{historical_block}

──────────────────────────────────────────────
  LAST 50 BARS  (1-min, real Binance data)
  Columns: Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%
  BuyVol% = taker-buy base volume / total volume × 100
  Rows are oldest → newest. The last row is the current bar.
──────────────────────────────────────────────
{csv_block}

──────────────────────────────────────────────
  YOUR NEUTRAL (ABSTENTION) PERFORMANCE
──────────────────────────────────────────────
{neutral_block}

──────────────────────────────────────────────
  TRACK RECORD
──────────────────────────────────────────────
  Ensemble (last 12)  {recent_accuracy*100:.1f}%
  Your prior          {ds_str}

══════════════════════════════════════════════
RESPOND EXACTLY IN THIS FORMAT:
══════════════════════════════════════════════
POSITION: ABOVE | BELOW | NEUTRAL
CONFIDENCE: XX%
DATA_RECEIVED: [state which signals were available]
DATA_REQUESTS: [NONE — or list additional data needed]
NARRATIVE: [2-4 sentences telling the STORY of the chart. Name specific prices and TIMES from Time(UTC) column.]
FREE_OBSERVATION: [1-2 sentences on anything unusual or most significant convergence of signals.]
REASONS:
1. [MICROSTRUCTURE: Order book, taker flow, liquidations, spot whale flow]
2. [FUNDING + POSITIONING: Funding rates, OI velocity, L/S ratio, top position ratio]
3. [TECHNICAL + CROSS-EXCHANGE: RSI/Stoch/MACD, Alligator, Fib, CoinAPI momentum, ensemble]
4. [SYNTHESIS: Dominant bias. Single most decisive factor. Biggest risk. Final conviction.]"""


def parse_response(text: str) -> Tuple[str, int, str, str, str, str, str]:
    """Parse DeepSeek response → (signal, confidence, reasoning, data_received, data_requests, narrative, free_observation)."""
    signal, confidence = "UNKNOWN", 50
    numbered: List[str] = []
    data_received = data_requests = narrative = free_observation = ""
    in_reasons = False

    for line in text.strip().splitlines():
        s = line.strip()
        u = s.upper()
        if u.startswith("POSITION:"):
            val = u.replace("POSITION:", "").strip()
            if "ABOVE" in val:     signal = "UP"
            elif "BELOW" in val:   signal = "DOWN"
            elif "NEUTRAL" in val: signal = "NEUTRAL"
        elif u.startswith("CONFIDENCE:"):
            try: confidence = int(float(u.replace("CONFIDENCE:", "").replace("%", "").strip()))
            except: pass
        elif s.upper().startswith("DATA_RECEIVED:"):
            data_received = s[len("DATA_RECEIVED:"):].strip(); in_reasons = False
        elif s.upper().startswith("DATA_REQUESTS:"):
            data_requests = s[len("DATA_REQUESTS:"):].strip(); in_reasons = False
        elif s.upper().startswith("NARRATIVE:"):
            narrative = s[len("NARRATIVE:"):].strip(); in_reasons = False
        elif s.upper().startswith("FREE_OBSERVATION:"):
            free_observation = s[len("FREE_OBSERVATION:"):].strip(); in_reasons = False
        elif u.startswith("REASONS:") or u.startswith("REASON:"):
            in_reasons = True
        elif in_reasons and re.match(r"^\d+\.", s):
            numbered.append(re.sub(r"^\d+\.\s*", "", s))
        elif in_reasons and s and not numbered:
            numbered.append(s)

    if signal == "UNKNOWN":
        tu = text.upper()
        if "POSITION: ABOVE" in tu:     signal = "UP"
        elif "POSITION: BELOW" in tu:   signal = "DOWN"
        elif "POSITION: NEUTRAL" in tu: signal = "NEUTRAL"

    # Hard filter: sub-65% confidence calls have no reliable edge — treat as abstention
    if signal in ("UP", "DOWN") and confidence < 65:
        logger.info("parse_response: overriding %s@%d%% → NEUTRAL (below 65%% threshold)", signal, confidence)
        signal = "NEUTRAL"

    reasoning = "\n".join(numbered).strip()[:1400]
    if not reasoning:
        m = re.search(r"REASONS?:\s*(.*)", text, re.IGNORECASE | re.DOTALL)
        if m: reasoning = m.group(1).strip()[:1400]

    return signal, confidence, reasoning, data_received, data_requests, narrative, free_observation


# ═══════════════════════════════════════════════════════════════
# SECTION 1b — Post-mortem analyst
# Fires after bar resolves. Sends full prediction record + actual
# outcome + fresh market data so DeepSeek can explain itself.
# ═══════════════════════════════════════════════════════════════

_PM_DIR = _ROOT / "specialists" / "postmortem"


async def run_postmortem(
    api_key: str,
    ds_record: Dict,
    actual_direction: str,
    end_price: float,
    updated_klines: List,
    updated_features: Dict,
    updated_dashboard: Dict,
) -> str:
    """Send the resolved prediction back to DeepSeek for self-analysis.
    Returns the raw postmortem text, or "" on failure."""
    if not api_key:
        return ""

    signal     = ds_record.get("signal", "UNKNOWN")
    confidence = ds_record.get("confidence", 0)
    start_price = ds_record.get("start_price", 0)
    window_start = ds_record.get("window_start", 0)
    reasoning  = ds_record.get("reasoning", "")
    narrative  = ds_record.get("narrative", "")
    free_obs   = ds_record.get("free_observation", "")
    data_recv  = ds_record.get("data_received", "")
    data_req   = ds_record.get("data_requests", "")

    correct = (signal == actual_direction) if signal in ("UP", "DOWN") else None
    verdict = "CORRECT" if correct is True else ("WRONG" if correct is False else "ABSTENTION (NEUTRAL)")
    price_delta = end_price - start_price if end_price and start_price else 0
    pct_move    = price_delta / start_price * 100 if start_price else 0

    bar_ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(window_start))

    # Fresh kline CSV (last 30 bars after resolution)
    ohlcv_lines = ["Time(UTC),Open,High,Low,Close,Volume"]
    for k in (updated_klines or [])[-30:]:
        try:
            t = time.strftime("%H:%M", time.gmtime(float(k[0]) / 1000))
            ohlcv_lines.append(f"{t},{float(k[1]):.2f},{float(k[2]):.2f},{float(k[3]):.2f},{float(k[4]):.2f},{float(k[5]):.4f}")
        except Exception:
            pass
    ohlcv_block = "\n".join(ohlcv_lines)

    # Compact features block
    feat_lines = []
    fv = lambda k, d=0: updated_features.get(k, d)
    for key in ("rsi_7", "macd_hist", "stoch_k_14", "ema_diff_5_13", "atr_14", "volume_ratio", "trend_slope", "trend_r2"):
        v = updated_features.get(key)
        if v is not None:
            feat_lines.append(f"  {key}: {v:.4f}")
    features_block = "\n".join(feat_lines) or "  (not available)"

    # Dashboard signals summary
    dash_lines = []
    if updated_dashboard and isinstance(updated_dashboard, dict):
        for k, v in updated_dashboard.items():
            if k == "fetched_at" or not isinstance(v, dict):
                continue
            sig = v.get("signal", "")
            interp = v.get("interpretation", "")[:80]
            if sig:
                dash_lines.append(f"  {k}: {sig}  — {interp}")
    dash_block = "\n".join(dash_lines) or "  (not available)"

    prompt = f"""You are reviewing your own 5-minute BTC/USDT prediction after it resolved.
Be ruthlessly honest. Identify exactly what went wrong or right and how to improve.

══ ORIGINAL PREDICTION (bar {bar_ts}) ══════════════════════════════
Signal     : {signal}  ({confidence}%)
Start price: ${start_price:,.2f}
Reasoning  : {reasoning[:800]}
Narrative  : {narrative[:400]}
Free obs   : {free_obs[:300]}
Data recv  : {data_recv}
Data req   : {data_req}

══ ACTUAL OUTCOME ══════════════════════════════════════════════════
Actual direction : {actual_direction}
End price        : ${end_price:,.2f}
Move             : {price_delta:+.2f} ({pct_move:+.4f}%)
VERDICT          : {verdict}

══ UPDATED MARKET DATA (post-close, last 30 bars) ══════════════════
{ohlcv_block}

══ UPDATED FEATURES ════════════════════════════════════════════════
{features_block}

══ UPDATED MICROSTRUCTURE SIGNALS ══════════════════════════════════
{dash_block}

══ YOUR TASK ════════════════════════════════════════════════════════
Analyze this prediction. Respond EXACTLY in this format:

VERDICT: {verdict}
ROOT_CAUSE: [1-2 sentences — the single most important reason the prediction was {verdict.split()[0].lower()}]
MISLEADING_SIGNALS: [which signals/indicators pointed the wrong way, or which ones you over-weighted]
RELIABLE_SIGNALS: [which signals were actually correct / worth trusting more]
DATA_GAPS: [specific data you wish you had — or NONE if data was sufficient]
UPDATED_READING: [given the actual move, what was really happening in the market at that moment]
LESSON: [one concrete rule to apply next time in a similar setup]"""

    try:
        t0 = time.time()
        raw = await _api_call(api_key, prompt, max_tokens=1200, timeout_s=50.0)
        elapsed = int((time.time() - t0) * 1000)
        logger.info("Postmortem completed for bar %s (%s) in %dms", bar_ts, verdict, elapsed)
        _save(_PM_DIR / f"last_{int(window_start)}.txt", f"=== PROMPT ===\n{prompt}\n\n=== RESPONSE ===\n{raw}")
        return raw.strip()
    except Exception as exc:
        logger.warning("Postmortem failed for bar %s: %s", bar_ts, exc)
        return ""


# ═══════════════════════════════════════════════════════════════
# SECTION 2 — Unified specialist (OHLCV → 5 specialist signals)
# ═══════════════════════════════════════════════════════════════

_SPEC_DIR      = _ROOT / "specialists" / "unified_analyst"
_SPEC_PROMPT   = _SPEC_DIR / "PROMPT.md"
_SPEC_SENT     = _SPEC_DIR / "last_sent.txt"
_SPEC_PROMPT_OUT = _SPEC_DIR / "last_prompt.txt"
_SPEC_RESPONSE = _SPEC_DIR / "last_response.txt"
_SPEC_SUGGEST  = _SPEC_DIR / "suggestions.txt"


def _ohlcv_csv(klines, n=60):
    rows = ["Time(UTC),Open,High,Low,Close,Volume,QuoteVol,Trades,BuyVol%"]
    for k in klines[-n:]:
        try:
            ts_s    = time.strftime("%m-%d %H:%M", time.gmtime(int(k[0]) / 1000))
            vol     = float(k[5])
            quote_v = float(k[7]) if len(k) > 7 else 0.0
            trades  = int(k[8])   if len(k) > 8 else 0
            buy_vol = float(k[9]) if len(k) > 9 else 0.0
            buy_pct = round(buy_vol / vol * 100, 1) if vol > 0 else 0.0
            rows.append(
                f"{ts_s},{float(k[1]):.2f},{float(k[2]):.2f},"
                f"{float(k[3]):.2f},{float(k[4]):.2f},{vol:.1f},"
                f"{quote_v:.0f},{trades},{buy_pct}"
            )
        except Exception:
            pass
    return "\n".join(rows)


def _parse_specialist_response(text: str) -> Tuple[Dict[str, Dict], Optional[str], Optional[str]]:
    lines = text.strip().splitlines()
    raw: Dict[str, str] = {}
    for line in lines:
        if ":" in line:
            key, _, val = line.partition(":")
            raw[key.strip().upper()] = val.strip()

    def _sig(key):
        v = raw.get(key, "ABOVE").upper()
        return "UP" if "ABOVE" in v else "DOWN"

    def _conf(key):
        try: return max(0.45, min(0.95, float(raw.get(key, "55").replace("%", "").strip()) / 100))
        except: return 0.55

    strategies = {
        "dow_theory":  {"signal": _sig("DOW_POSITION"),  "confidence": _conf("DOW_CONFIDENCE"),
                        "reasoning": raw.get("DOW_REASON", ""), "value": raw.get("DOW_STRUCTURE", "")[:20],
                        "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None},
        "fib_pullback": {"signal": _sig("FIB_POSITION"), "confidence": _conf("FIB_CONFIDENCE"),
                         "reasoning": raw.get("FIB_REASON", ""), "value": raw.get("FIB_LEVEL", "")[:20],
                         "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None},
        "alligator":   {"signal": _sig("ALG_POSITION"),  "confidence": _conf("ALG_CONFIDENCE"),
                        "reasoning": raw.get("ALG_REASON", ""), "value": raw.get("ALG_STATE", "")[:20],
                        "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None},
        "acc_dist":    {"signal": _sig("ACD_POSITION"),  "confidence": _conf("ACD_CONFIDENCE"),
                        "reasoning": raw.get("ACD_REASON", ""), "value": raw.get("ACD_VALUE", "")[:20],
                        "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None},
        "harmonic":    {"signal": _sig("HAR_POSITION"),  "confidence": _conf("HAR_CONFIDENCE"),
                        "reasoning": raw.get("HAR_REASON", ""), "value": raw.get("HAR_PATTERN", "")[:20],
                        "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None},
    }
    creative_edge = raw.get("CREATIVE_EDGE", "").strip()
    if creative_edge.upper() == "NONE" or not creative_edge:
        creative_edge = None
    suggestion = raw.get("SUGGESTION", "").strip()
    if suggestion.upper() == "NONE" or not suggestion:
        suggestion = None
    return strategies, creative_edge, suggestion


async def run_specialists(
    api_key: str,
    klines:  List,
) -> Tuple[Dict[str, Optional[Dict]], Optional[str]]:
    """Fire ONE unified specialist call covering DOW/FIB/ALG/ACD/HAR + creative edge."""
    if not klines or len(klines) < 20:
        logger.warning("Specialists: not enough klines (%d) — skipping", len(klines) if klines else 0)
        return {}, None

    try:
        template = _SPEC_PROMPT.read_text(encoding="utf-8")
    except Exception as exc:
        logger.error("Unified analyst: could not load PROMPT.md: %s", exc)
        return {}, None

    if not template:
        return {}, None

    t0     = time.time()
    csv    = _ohlcv_csv(klines, 60)
    prompt = template.format(csv=csv)
    ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    _save(_SPEC_SENT,      f"# Sent at {ts_str}\n\n{csv}")
    _save(_SPEC_PROMPT_OUT, f"# Sent at {ts_str}\n\n{prompt}")

    try:
        raw = await _api_call(api_key, prompt, max_tokens=1000, timeout_s=25.0)
        _append(_SPEC_RESPONSE, f"\n{'='*60}\n# {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n{'='*60}\n\n{raw}")
        strategies, creative_edge, suggestion = _parse_specialist_response(raw)
        if suggestion:
            _append(_SPEC_SUGGEST, f"[{ts_str}] {suggestion}")
        elapsed = time.time() - t0
        logger.info("Unified specialist %.1fs | %s | creative_edge: %s",
                    elapsed, " ".join(f"{k[:3]}={v['signal']}" for k, v in strategies.items()),
                    "YES" if creative_edge else "none")
        return strategies, creative_edge
    except Exception as exc:
        _append(_SPEC_RESPONSE, f"\n{'='*60}\n# ERROR {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n{'='*60}\n\n{exc}")
        logger.warning("Unified specialist failed: %s", exc)
        return {}, None


# ═══════════════════════════════════════════════════════════════
# SECTION 3 — Historical similarity analyst
# ═══════════════════════════════════════════════════════════════

_HIST_DIR      = _ROOT / "specialists" / "historical_analyst"
_HIST_PROMPT   = _HIST_DIR / "PROMPT.md"
_HIST_SENT     = _HIST_DIR / "last_sent.txt"
_HIST_PROMPT_OUT = _HIST_DIR / "last_prompt.txt"
_HIST_RESPONSE = _HIST_DIR / "last_response.txt"
_HIST_SUGGEST  = _HIST_DIR / "suggestions.txt"


def _session(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    for start, end, label in _SESSIONS:
        if start <= dt.hour < end:
            return label
    return "LATE"


def _fmt_indicators(ind: Dict) -> str:
    keys = [("rsi_14","RSI"),("mfi_14","MFI"),("macd_histogram","MACD"),
            ("stoch_k_14","STOCH"),("bollinger_pct_b","BB"),
            ("volume_surge","VSURGE"),("price_vs_vwap","VWAP%"),
            ("obv_slope","OBV"),("trend_r_squared","R2")]
    parts = []
    for k, label in keys:
        v = ind.get(k)
        if v is not None:
            try: parts.append(f"{label}={float(v):.1f}")
            except: pass
    return " ".join(parts) if parts else "no_data"


def _fmt_strategy_votes(votes: Dict) -> str:
    parts = []
    for name, vote in sorted(votes.items()):
        if isinstance(vote, dict):
            sig = (vote.get("signal") or "").upper()
            if sig in ("UP", "DOWN"):
                conf  = int((vote.get("confidence") or 0.5) * 100)
                arrow = "↑" if sig == "UP" else "↓"
                short = name.replace("strat:", "").replace("dash:", "d:").replace("spec:", "s:")
                parts.append(f"{short[:8]}={arrow}{conf}")
    return " ".join(parts) if parts else "none"


def _fmt_specialists(sp: Dict) -> str:
    keys = [("dow_theory","DOW"),("fib_pullback","FIB"),
            ("alligator","ALG"),("acc_dist","ACD"),("harmonic","HAR")]
    parts = []
    for k, label in keys:
        v = sp.get(k)
        if v:
            sig  = "U" if v.get("signal") == "UP" else "D"
            conf = int((v.get("confidence") or 0.5) * 100)
            parts.append(f"{label}={sig}{conf}")
    return " ".join(parts) if parts else "none"


def _fmt_dashboard_directions(dash: Dict) -> str:
    _ABBREV = {
        "order_book":"ob","long_short":"ls","taker_flow":"tf","oi_funding":"oif",
        "liquidations":"liq","fear_greed":"fg","mempool":"mem","coinalyze":"cz",
        "coingecko":"cg","deribit_dvol":"dvol",
        "kraken_premium":"krak","oi_velocity":"oiv","spot_whale_flow":"swf",
        "bybit_liquidations":"bybit","okx_funding":"okx","btc_dominance":"btcd",
        "top_position_ratio":"tpr","funding_trend":"ft",
    }
    parts = []
    for key, abbrev in _ABBREV.items():
        v = (dash.get(key) or "").upper()
        if v == "UP":   parts.append(f"{abbrev}=UP")
        elif v == "DOWN": parts.append(f"{abbrev}=DN")
    return " ".join(parts) if parts else "all_neutral"


# ── STEP 1: Symbolic tokeniser ────────────────────────────────────────────────
# Converts raw numeric indicator values into discrete human-readable labels.
# The LLM understands "RSI_OVERSOLD" far better than it understands "28.3"
# because language models are trained on text, not on numerical magnitudes.
# Each bucket boundary is a classical technical-analysis threshold.

def _symbolize_indicators(ind: Dict) -> Dict[str, str]:
    """Map numeric indicator values to symbolic tokens (e.g. RSI=28 → RSI_OVERSOLD)."""

    def rsi(v):
        if v is None:  return "RSI_UNK"
        if v < 30:     return "RSI_OVERSOLD"
        if v < 45:     return "RSI_LOW"
        if v < 55:     return "RSI_MID"
        if v < 70:     return "RSI_HIGH"
        return              "RSI_OVERBOUGHT"

    def macd(v):
        if v is None:  return "MACD_UNK"
        if v < -5:     return "MACD_STRONG_BEAR"
        if v < -1:     return "MACD_BEAR"
        if v < 1:      return "MACD_NEUTRAL"
        if v < 5:      return "MACD_BULL"
        return              "MACD_STRONG_BULL"

    def stoch(v):
        if v is None:  return "STOCH_UNK"
        if v < 20:     return "STOCH_OVERSOLD"
        if v < 40:     return "STOCH_LOW"
        if v < 60:     return "STOCH_MID"
        if v < 80:     return "STOCH_HIGH"
        return              "STOCH_OVERBOUGHT"

    def bb(v):
        if v is None:  return "BB_UNK"
        if v < 0.2:    return "BB_BELOW_BAND"
        if v < 0.4:    return "BB_LOWER"
        if v < 0.6:    return "BB_MID"
        if v < 0.8:    return "BB_UPPER"
        return              "BB_ABOVE_BAND"

    def mfi(v):
        if v is None:  return "MFI_UNK"
        if v < 20:     return "MFI_OVERSOLD"
        if v < 45:     return "MFI_LOW"
        if v < 55:     return "MFI_MID"
        if v < 80:     return "MFI_HIGH"
        return              "MFI_OVERBOUGHT"

    return {
        "rsi":  rsi(ind.get("rsi_14")),
        "macd": macd(ind.get("macd_histogram")),
        "stoch": stoch(ind.get("stoch_k_14")),
        "bb":   bb(ind.get("bollinger_pct_b")),
        "mfi":  mfi(ind.get("mfi_14")),
    }


# ── STEP 2: Feature vector extractor ──────────────────────────────────────────
# Converts a bar record into a compact normalized numpy vector.
# This is what we use for the MATH (cosine similarity) — not the LLM.
# Each feature is scaled to [-1, +1] so no single indicator dominates.
# Dashboard signals (order book, taker flow, etc.) are encoded as -1/0/+1.

_DASH_KEYS_VEC = [
    "order_book", "long_short", "taker_flow", "fear_greed",
    "mempool", "oi_funding", "okx_funding", "btc_dominance",
]

def _bar_feature_vector(record: Dict) -> Optional[np.ndarray]:
    """Return a unit-normalized feature vector for a bar record, or None if data missing."""
    ind  = record.get("indicators", {}) or {}
    dash = record.get("dashboard_signals_raw", {}) or {}

    def norm(v, lo, hi):
        """Scale v from [lo, hi] → [-1, +1]. Missing → 0 (neutral)."""
        if v is None: return 0.0
        return max(-1.0, min(1.0, (float(v) - lo) / (hi - lo) * 2.0 - 1.0))

    def dsig(key):
        """Encode UP→+1, DOWN→-1, anything else→0."""
        v = (dash.get(key) or "").upper()
        return 1.0 if v == "UP" else -1.0 if v == "DOWN" else 0.0

    features = np.array([
        # ── Technical indicators ───────────────────────────────
        norm(ind.get("rsi_14"),          0,   100),
        norm(ind.get("macd_histogram"), -20,   20),
        norm(ind.get("stoch_k_14"),      0,   100),
        norm(ind.get("bollinger_pct_b"), 0,     1),
        norm(ind.get("mfi_14"),          0,   100),
        norm(ind.get("price_vs_vwap"),  -1,     1),
        norm(ind.get("volume_surge"),    0,     5),
        norm(ind.get("obv_slope"),      -1,     1),
        # ── Microstructure / dashboard signals ─────────────────
        dsig("order_book"),
        dsig("long_short"),
        dsig("taker_flow"),
        dsig("fear_greed"),
        dsig("mempool"),
        dsig("oi_funding"),
        dsig("okx_funding"),
        dsig("btc_dominance"),
    ], dtype=np.float32)

    norm_val = np.linalg.norm(features)
    if norm_val < 1e-8:
        return None          # all-zero vector — bar has no usable data
    return features / norm_val   # unit vector → dot product == cosine similarity


def _bar_embed_text(record: Dict) -> str:
    """
    Full natural-language essay about a resolved bar — every fact, in order of importance.

    This is intentionally the richest possible text. Cohere embed-english-v3.0 encodes
    it into a 1024-dim semantic vector capturing the complete fingerprint of the bar:
    market regime, all indicator states, strategy alignment, microstructure, DeepSeek's
    reasoning, whether it was right or wrong, and the post-mortem lesson.

    At 10,000+ bars this makes similarity search extraordinarily powerful — finding bars
    where the same market regime produced the same reasoning which led to the same outcome,
    something impossible with hand-crafted vectors.

    Text is NOT manually truncated (no [:N] slicing). The outcome + predictions + key
    indicators come first so the most critical semantics land within Cohere's token window.
    The reranker sees the full text with no limit.
    """
    ts    = record.get("window_start", 0)
    dt    = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
    day   = _DAYS[dt.weekday()] if dt else "?"
    t_s   = dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "?"
    ses   = record.get("session") or (_session(ts) if ts else "?")
    bar_n = record.get("window_count") or "?"
    actual = record.get("actual_direction", "?")
    sp    = float(record.get("start_price") or 0)
    ep    = float(record.get("end_price") or 0)
    chg   = ((ep - sp) / sp * 100) if sp and ep else 0.0
    lat   = record.get("latency_ms") or record.get("deepseek_latency_ms") or ""

    # ── Predictions & outcome ────────────────────────────────────
    e_sig  = record.get("ensemble_signal") or "?"
    e_conf = int(float(record.get("ensemble_conf") or 0) * 100)
    e_bull = record.get("ensemble_bullish") or ""
    e_bear = record.get("ensemble_bearish") or ""
    d_sig  = record.get("deepseek_signal") or record.get("signal") or "?"
    d_conf = record.get("deepseek_conf") or record.get("confidence") or 0
    h_rec  = record.get("historical_analyst") or {}
    h_sig  = h_rec.get("signal", "") if isinstance(h_rec, dict) else ""
    h_conf = int(float(h_rec.get("confidence", 0)) * 100) if isinstance(h_rec, dict) else 0

    ds_correct = record.get("deepseek_correct") if "deepseek_correct" in record else record.get("correct")
    if d_sig == "NEUTRAL":
        outcome = "NO TRADE — DeepSeek abstained (NEUTRAL)"
    elif ds_correct is True:
        outcome = f"CORRECT — predicted {d_sig}, price actually went {actual}"
    elif ds_correct is False:
        outcome = f"WRONG — predicted {d_sig}, price actually went {actual}"
    else:
        outcome = "PENDING (not yet resolved)"

    # ── Indicator helpers ────────────────────────────────────────
    ind = record.get("indicators", {}) or {}

    def fv(k):
        v = ind.get(k)
        return float(v) if v is not None else None

    def rsi_label(v):
        if v is None: return ""
        if v >= 80: return "EXTREMELY OVERBOUGHT"
        if v >= 70: return "OVERBOUGHT"
        if v >= 55: return "HIGH — leaning bullish"
        if v >= 45: return "NEUTRAL"
        if v >= 30: return "LOW — leaning bearish"
        if v >= 20: return "OVERSOLD"
        return "EXTREMELY OVERSOLD"

    def bb_label(v):
        if v is None: return ""
        if v >= 1.0: return "ABOVE UPPER BAND — extreme extension"
        if v >= 0.8: return "AT UPPER BAND — overbought stretch"
        if v >= 0.6: return "UPPER HALF"
        if v >= 0.4: return "MID BAND — balanced"
        if v >= 0.2: return "LOWER HALF"
        if v >= 0.0: return "AT LOWER BAND — oversold stretch"
        return "BELOW LOWER BAND — extreme extension"

    def macd_label(v):
        if v is None: return ""
        if v >  5: return "STRONG BULL momentum"
        if v >  1: return "BULL momentum building"
        if v > -1: return "NEUTRAL — no momentum edge"
        if v > -5: return "BEAR momentum building"
        return "STRONG BEAR momentum"

    def vol_label(v):
        if v is None: return ""
        if v >= 3.0: return "EXTREME VOLUME SPIKE"
        if v >= 2.0: return "HIGH VOLUME"
        if v >= 1.3: return "ELEVATED VOLUME"
        if v >= 0.7: return "NORMAL VOLUME"
        return "LOW VOLUME — weak conviction"

    def obv_label(v):
        if v is None: return ""
        return "ACCUMULATION — buying pressure" if v > 0 else "DISTRIBUTION — selling pressure"

    rsi_v  = fv("rsi_4");    rsi14 = fv("rsi_14") or rsi_v
    macdh  = fv("macd_histogram"); macds = fv("macd_signal")
    stoch  = fv("stoch_k_5") or fv("stoch_k_14")
    bbpct  = fv("bollinger_pct_b"); bbw = fv("bollinger_width")
    mfi14  = fv("mfi_14");   mfi7 = fv("mfi_7")
    vwap_p = fv("price_vs_vwap"); vwap_b = fv("vwap_band_pos")
    obv    = fv("obv_slope"); vsurge = fv("volume_surge")
    r2     = fv("trend_r_squared"); tslope = fv("trend_slope")
    ecross = fv("ema_cross_8_21")
    pve5   = fv("price_vs_ema_5");  pve8  = fv("price_vs_ema_8")
    pve13  = fv("price_vs_ema_13"); pve21 = fv("price_vs_ema_21")
    vol5   = fv("volatility_5");  vol10 = fv("volatility_10"); vol20 = fv("volatility_20")
    pp10   = fv("price_position_10"); pp30 = fv("price_position_30"); pp60 = fv("price_position_60")
    ret1   = fv("return_1");  ret2  = fv("return_2");  ret5  = fv("return_5")
    ret10  = fv("return_10"); ret15 = fv("return_15"); ret30 = fv("return_30")
    momacc = fv("momentum_acceleration")

    # ── Strategy votes ───────────────────────────────────────────
    votes = record.get("strategy_votes", {}) or {}
    _STRAT = [
        ("rsi","RSI"), ("macd","MACD"), ("stochastic","Stochastic"),
        ("ema_cross","EMA Fast/Slow"), ("supertrend","Supertrend"), ("adx","ADX"),
        ("alligator","Alligator"), ("acc_dist","Accumulation/Distribution"),
        ("dow_theory","Dow Theory"), ("fib_pullback","Fibonacci"),
        ("harmonic","Harmonic Pattern"), ("vwap","VWAP"),
        ("polymarket","Crowd (Polymarket)"), ("ml_logistic","Linear Regression"),
    ]

    # ── Dashboard signals ────────────────────────────────────────
    dash = record.get("dashboard_signals_raw", {}) or {}
    _DASH = [
        ("order_book","Order book bid/ask imbalance"),
        ("long_short","Long/short ratio"),
        ("taker_flow","Taker buy/sell flow"),
        ("fear_greed","Fear & greed index"),
        ("mempool","Bitcoin mempool congestion"),
        ("oi_funding","Open interest funding rate"),
        ("okx_funding","OKX funding rate"),
        ("btc_dominance","BTC market dominance"),
        ("liquidations","Liquidation events"),
        ("coinalyze","Coinalyze OI signal"),
        ("coingecko","CoinGecko market sentiment"),
        ("deribit_dvol","Deribit implied volatility"),
        ("kraken_premium","Kraken BTC premium"),
        ("oi_velocity","Open interest velocity"),
        ("spot_whale_flow","Spot whale order flow"),
        ("bybit_liquidations","Bybit liquidation cascade"),
        ("top_position_ratio","Top trader position ratio"),
        ("funding_trend","Funding rate trend"),
    ]

    # ── Specialist signals ───────────────────────────────────────
    spec = record.get("specialist_signals", {}) or {}

    # ── DeepSeek text ────────────────────────────────────────────
    reasoning  = (record.get("deepseek_reasoning") or record.get("reasoning") or "").strip()
    narrative  = (record.get("deepseek_narrative") or record.get("narrative") or "").strip()
    free_obs   = (record.get("deepseek_free_obs") or record.get("free_observation") or "").strip()
    data_req   = (record.get("data_requests") or "").strip()
    ce         = (record.get("creative_edge") or "").strip()
    postmortem = (record.get("postmortem") or "").strip()

    # ── Accuracy snapshot ────────────────────────────────────────
    acc = record.get("accuracy_snapshot", {}) or {}

    # ── Assemble essay (most critical facts first) ───────────────
    # Use session/day/hour only — specific date biases cosine toward time proximity
    L = []
    hour_utc = dt.hour if dt else "?"
    L.append(f"BTC 5-MINUTE BAR #{bar_n} — {day} {ses} SESSION  Hour UTC: {hour_utc:02d}")
    L.append("=" * 60)
    L.append("")
    L.append("OUTCOME & PREDICTIONS")
    L.append(f"  {outcome}")
    L.append(f"  Price: ${sp:,.2f} → ${ep:,.2f}  ({chg:+.4f}%)  Actual direction: {actual}")
    L.append(f"  DeepSeek prediction: {d_sig} at {d_conf}% confidence" + (f"  (latency: {lat}ms)" if lat else ""))
    L.append(f"  Ensemble prediction: {e_sig} at {e_conf}%" + (f"  ({e_bull} bullish votes / {e_bear} bearish votes)" if e_bull or e_bear else ""))
    if h_sig:
        L.append(f"  Historical analyst signal: {h_sig} at {h_conf}%")
    if acc:
        ens_acc = acc.get("ensemble_accuracy"); ens_tot = acc.get("ensemble_total", 0)
        ds_acc  = acc.get("deepseek_accuracy"); ds_tot  = acc.get("deepseek_total", 0)
        ag_acc  = acc.get("agree_accuracy");    ag_tot  = acc.get("agree_total", 0)
        best    = acc.get("best_indicator");    best_a  = acc.get("best_indicator_accuracy")
        if ens_acc is not None: L.append(f"  System accuracy at bar time — Ensemble: {ens_acc}% ({ens_tot} bars)  DeepSeek: {ds_acc}% ({ds_tot} bars)  When agree: {ag_acc}% ({ag_tot} bars)")
        if best: L.append(f"  Best signal at bar time: {best} at {best_a}%")
    L.append("")
    L.append("TECHNICAL INDICATORS")
    if rsi14  is not None: L.append(f"  RSI: {rsi14:.1f} — {rsi_label(rsi14)}")
    if macdh  is not None: L.append(f"  MACD histogram: {macdh:+.3f} — {macd_label(macdh)}" + (f"  signal line: {macds:.3f}" if macds is not None else ""))
    if stoch  is not None: L.append(f"  Stochastic K: {stoch:.1f} — {rsi_label(stoch)}")
    if bbpct  is not None: L.append(f"  Bollinger %B: {bbpct:.3f} — {bb_label(bbpct)}" + (f"  width: {bbw:.4f}" if bbw is not None else ""))
    if mfi14  is not None: L.append(f"  Money Flow Index (14): {mfi14:.1f} — {rsi_label(mfi14)}" + (f"  MFI(7): {mfi7:.1f}" if mfi7 is not None else ""))
    if vwap_p is not None: L.append(f"  Price vs VWAP: {vwap_p:+.3f}%  {'above VWAP — bullish bias' if vwap_p > 0 else 'below VWAP — bearish bias'}" + (f"  band position: {vwap_b:+.2f}σ" if vwap_b is not None else ""))
    if obv    is not None: L.append(f"  OBV slope: {obv:+.1f} — {obv_label(obv)}")
    if vsurge is not None: L.append(f"  Volume surge: {vsurge:.2f}x average — {vol_label(vsurge)}")
    if r2     is not None: L.append(f"  Trend R²: {r2:.3f}" + (f"  slope: {tslope:+.5f}" if tslope is not None else ""))
    if ecross is not None: L.append(f"  EMA(8/21) cross: {ecross:+.3f} — {'bullish: fast above slow' if ecross > 0 else 'bearish: fast below slow'}")
    if pve5   is not None: L.append(f"  Price vs EMA5: {pve5:+.3f}%  vs EMA8: {pve8:+.3f}%" + (f"  vs EMA13: {pve13:+.3f}%  vs EMA21: {pve21:+.3f}%" if pve13 is not None else ""))
    vols = [(v, n) for v, n in [(vol5,"5-bar"),(vol10,"10-bar"),(vol20,"20-bar")] if v is not None]
    if vols: L.append(f"  Volatility: {' | '.join(f'{n}: {v:.4f}%' for v, n in vols)}")
    pps = [(v, n) for v, n in [(pp10,"10-bar"),(pp30,"30-bar"),(pp60,"60-bar")] if v is not None]
    if pps: L.append(f"  Price position in range: {' | '.join(f'{n}: {v:.2f}' for v, n in pps)}  (0=bottom 1=top)")
    rets = [(v, n) for v, n in [(ret1,"1m"),(ret2,"2m"),(ret5,"5m"),(ret10,"10m"),(ret15,"15m"),(ret30,"30m")] if v is not None]
    if rets: L.append(f"  Recent returns: {' | '.join(f'{n}: {v:+.3f}%' for v, n in rets)}")
    if momacc is not None: L.append(f"  Momentum acceleration: {momacc:+.5f} — {'accelerating upward' if momacc > 0 else 'decelerating / reversing'}")
    L.append("")
    L.append("STRATEGY VOTES (all models)")
    for key in sorted(votes.keys()):
        v = votes[key]
        if isinstance(v, dict) and v.get("signal"):
            conf = int(float(v.get("confidence") or 0.5) * 100)
            rsn  = (v.get("reasoning") or "").strip()
            line = f"  {key}: {v['signal']} at {conf}%"
            if rsn: line += f" — {rsn[:120]}"
            L.append(line)
    L.append("")
    L.append("MARKET MICROSTRUCTURE (live derivatives & sentiment data)")
    for key in sorted(dash.keys()):
        v = (dash.get(key) or "").upper()
        if v in ("UP", "DOWN", "NEUTRAL"):
            L.append(f"  {key}: {v}")
    L.append("")
    L.append("SPECIALIST SIGNALS (DeepSeek pattern recognition)")
    for key in sorted(spec.keys()):
        v = spec[key]
        if isinstance(v, dict) and v.get("signal"):
            conf = int(float(v.get("confidence") or 0.5) * 100)
            rsn  = (v.get("reasoning") or "").strip()
            L.append(f"  {key}: {v['signal']} at {conf}%" + (f" — {rsn}" if rsn else ""))
    if ce:
        L.append("")
        L.append("CREATIVE EDGE (cross-pattern synthesis)")
        L.append(f"  {ce}")
    if reasoning:
        L.append("")
        L.append("DEEPSEEK FULL REASONING")
        for bullet in reasoning.split("\n"):
            b = bullet.strip()
            if b: L.append(f"  {b}")
    if narrative:
        L.append("")
        L.append("PRICE NARRATIVE")
        L.append(f"  {narrative}")
    if free_obs:
        L.append("")
        L.append("FREE OBSERVATION")
        L.append(f"  {free_obs}")
    if data_req and data_req.upper() not in ("", "NONE"):
        L.append("")
        L.append("DATA GAPS (additional data AI requested)")
        L.append(f"  {data_req}")
    if postmortem:
        L.append("")
        L.append("POST-MORTEM — DeepSeek self-analysis after outcome was revealed")
        for line in postmortem.split("\n"):
            l = line.strip()
            if l: L.append(f"  {l}")

    hist_analysis = (record.get("historical_analysis") or "").strip()
    if hist_analysis:
        L.append("")
        L.append("HISTORICAL ANALYST OUTPUT (similar past bars at time of prediction)")
        for line in hist_analysis.split("\n"):
            l = line.strip()
            if l: L.append(f"  {l}")

    full_prompt = (record.get("full_prompt") or "").strip()
    if full_prompt:
        L.append("")
        L.append("FULL PROMPT SENT TO DEEPSEEK (complete market context at bar open)")
        L.append(full_prompt)

    return "\n".join(L)


# _cohere_prefilter removed — replaced by pgvector cosine search in PostgreSQL


_KEY_DASH_COMPACT = [
    ("order_book","ob"),("long_short","ls"),("taker_flow","tf"),
    ("fear_greed","fg"),("mempool","mem"),
]

def _build_history_table(records: List[Dict], compact: bool = False) -> str:
    if not records:
        return "  (no resolved history yet)"
    lines = []
    for i, r in enumerate(records, 1):
        ts     = r.get("window_start", 0)
        dt     = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
        day    = _DAYS[dt.weekday()] if dt else "?"
        time_s = dt.strftime("%Y-%m-%d %H:%M") if dt else "?"
        ses    = r.get("session") or (_session(ts) if ts else "?")
        actual = r.get("actual_direction", "?")
        sp     = r.get("start_price", 0); ep = r.get("end_price", 0)
        price_s = f"${sp:,.0f}→${ep:,.0f}" if sp and ep else "?"
        e_sig  = r.get("ensemble_signal", "?"); e_conf = int((r.get("ensemble_conf") or 0) * 100)
        e_ok   = "✓" if r.get("ensemble_correct") else "✗"
        d_sig  = r.get("deepseek_signal", ""); d_conf = r.get("deepseek_conf", 0)
        d_ok   = "✓" if r.get("deepseek_correct") else "✗" if r.get("deepseek_correct") is False else "?"
        ind_s  = _fmt_indicators(r.get("indicators", {}))
        vote_s = _fmt_strategy_votes(r.get("strategy_votes", {}))
        spec_s = _fmt_specialists(r.get("specialist_signals", {}))
        dash_s = _fmt_dashboard_directions(r.get("dashboard_signals_raw", {}))
        ce     = (r.get("creative_edge") or "").strip()

        if compact:
            # ── STEP 5: Compact one-line format with symbolic tokens ────────────
            # Symbolic tokens (RSI_OVERSOLD) are used instead of raw numbers (28.3)
            # because LLMs pattern-match on language tokens, not numeric magnitudes.
            # The similarity SEARCH already used the raw numbers (Step 2/3).
            # Here we only need the LLM to REASON about the pattern — labels are better.
            time_c = dt.strftime("%a%H:%M") if dt else "?"
            ses_c  = ses[:3].upper() if ses else "?"
            out_c  = actual[0] if actual and actual != "?" else "?"
            ens_c  = f"ENS={e_sig[0] if e_sig else '?'}{e_conf}{e_ok}"
            ds_c   = f"DS={d_sig[0] if d_sig else '?'}{d_conf}{d_ok}" if d_sig else "DS=?"
            # Symbolic indicator tokens (via Step 1 tokeniser)
            sym    = _symbolize_indicators(r.get("indicators", {}))
            ind_c  = f"{sym['rsi']} {sym['macd']} {sym['stoch']} {sym['bb']}"
            # SPEC only when signals present
            spec_c = f" SPEC:{spec_s}" if spec_s != "none" else ""
            # Key dashboard signals only
            dash   = r.get("dashboard_signals_raw", {})
            dp     = []
            for key, abbrev in _KEY_DASH_COMPACT:
                v = (dash.get(key) or "").upper()
                if v == "UP":   dp.append(f"{abbrev}=UP")
                elif v == "DOWN": dp.append(f"{abbrev}=DN")
            dash_c = " ".join(dp) if dp else "neutral"
            ce_c   = ce[:150].replace("\n", " ") if ce else "—"
            lines.append(f"#{i:03d} {time_c}{ses_c} {out_c}|{ens_c} {ds_c}|{ind_c}{spec_c}|{dash_c}|CE:{ce_c}")
        else:
            ce_s = ce[:70].replace("\n", " ") if ce else "—"
            lines.append(
                f"  #{i:03d}|{actual}|{price_s}|{day} {time_s} {ses}\n"
                f"    ENS={e_sig[0] if e_sig else '?'}{e_conf}{e_ok}  DS={d_sig[0] if d_sig else '?'}{d_conf}{d_ok}\n"
                f"    {ind_s}\n    STRAT: {vote_s}\n    SPEC: {spec_s}  |  DASH: {dash_s}\n    CE: {ce_s}"
            )
    return "\n".join(lines)


def _build_current_bar(
    current_indicators, current_strategy_votes, window_start_time,
    specialist_signals=None, creative_edge="", ensemble_signal="",
    ensemble_conf=0.0, dashboard_directions=None,
) -> str:
    dt     = datetime.fromtimestamp(window_start_time, tz=timezone.utc) if window_start_time else None
    day    = _DAYS[dt.weekday()] if dt else "?"
    time_s = dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "?"
    ses    = _session(window_start_time) if window_start_time else "?"
    ind_s  = _fmt_indicators(current_indicators)
    vote_s = _fmt_strategy_votes(current_strategy_votes)
    spec_s = _fmt_specialists(specialist_signals or {})
    dash_s = _fmt_dashboard_directions(dashboard_directions or {})
    ce_s   = (creative_edge or "—").strip()
    return (
        f"  {day} {time_s}  {ses}\n"
        f"  Indicators   : {ind_s}\n  Strategies   : {vote_s}\n"
        f"  Specialists  : {spec_s}\n  Microstructure: {dash_s}\n"
        f"  Ensemble     : {ensemble_signal or '?'} {int(ensemble_conf*100)}%\n"
        f"  Creative edge: {ce_s}"
    )


def _parse_historical_signal(raw: str) -> Dict:
    """Extract POSITION/CONFIDENCE/LEAN from the historical analyst header."""
    signal, confidence, lean = "NEUTRAL", 50, ""
    for line in raw.splitlines():
        s = line.strip(); u = s.upper()
        if u.startswith("POSITION:"):
            val = u.replace("POSITION:", "").strip()
            if "UP" in val:      signal = "UP"
            elif "DOWN" in val:  signal = "DOWN"
            else:                signal = "NEUTRAL"
        elif u.startswith("CONFIDENCE:"):
            try: confidence = int(float(u.replace("CONFIDENCE:", "").replace("%", "").strip()))
            except: pass
        elif u.startswith("LEAN:"):
            lean = s[len("LEAN:"):].strip()
        elif lean and signal != "NEUTRAL":
            break
    return {
        "signal":     signal,
        "confidence": max(0.45, min(0.95, confidence / 100)),
        "reasoning":  lean,
        "value":      f"{confidence}%",
        "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
    }


async def run_historical_analyst(
    api_key: str,
    history_records: List[Dict],
    current_indicators: Dict,
    current_strategy_votes: Dict,
    window_start_time: float = 0.0,
    specialist_signals: Optional[Dict] = None,
    creative_edge: str = "",
    ensemble_signal: str = "",
    ensemble_conf: float = 0.0,
    dashboard_directions: Optional[Dict] = None,
    cohere_api_key: str = "",
    pgvector_search_fn=None,
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Fire historical similarity analyst using Cohere embed + pgvector + Cohere rerank.

    Pipeline:
      1. Embed current bar opening text via Cohere embed-english-v3.0 (search_query)
      2. pgvector cosine search in PostgreSQL → top-50 most similar stored bars
      3. Cohere rerank → final top-20 most contextually relevant bars
      4. Fire DeepSeek historical analyst on those 20 bars

    Raises CohereUnavailableError if Cohere is down — no fallback, caller handles pause.
    """
    try:
        template = _HIST_PROMPT.read_text(encoding="utf-8")
    except Exception as exc:
        logger.error("Historical analyst: could not load PROMPT.md: %s", exc)
        return None, None

    if not template:
        return None, None

    t0 = time.time()

    current_bar = _build_current_bar(
        current_indicators, current_strategy_votes, window_start_time,
        specialist_signals, creative_edge, ensemble_signal, ensemble_conf, dashboard_directions,
    )

    # ── Step 1: Embed current bar opening conditions via Cohere ──
    current_vec = await embed_text(cohere_api_key, current_bar, input_type="search_query")
    logger.info("Cohere embed: current bar encoded (%d dims)", len(current_vec))

    # ── Step 2: pgvector cosine search → top-50 most similar bars ──
    total_searched = len(history_records)
    if pgvector_search_fn is not None:
        pre_bars = await asyncio.to_thread(pgvector_search_fn, current_vec, COHERE_PRE_FILTER_K)
        if not pre_bars:
            logger.info("pgvector: no embedded bars yet, falling back to most recent %d", COHERE_PRE_FILTER_K)
            pre_bars = history_records[-COHERE_PRE_FILTER_K:]
    else:
        pre_bars = history_records[-COHERE_PRE_FILTER_K:]
    pre_texts = [_bar_embed_text(b) for b in pre_bars]
    logger.info("pgvector search: %d candidates from %d total bars", len(pre_bars), total_searched)

    # ── Step 3: Cohere rerank → final top-20 (raises CohereUnavailableError if down) ──
    if len(pre_bars) > COHERE_FINAL_K:
        ranked_indices = await rerank_bars(
            cohere_api_key, current_bar, pre_texts, top_n=COHERE_FINAL_K,
        )
        similar_bars = [pre_bars[i] for i in ranked_indices]
        logger.info("Cohere rerank: %d → %d bars selected", len(pre_bars), len(similar_bars))
    else:
        similar_bars = pre_bars
        logger.info("Cohere rerank skipped: only %d candidates (≤%d)", len(pre_bars), COHERE_FINAL_K)

    history_table_full    = _build_history_table(history_records, compact=False)
    history_table_compact = _build_history_table(similar_bars,    compact=True)

    n      = len(similar_bars)
    prompt = template.format(n=n, history_table=history_table_compact, current_bar=current_bar)

    ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    _save(_HIST_SENT, (
        f"# {ts_str}  |  Total: {total_searched} bars  |  pgvector top-{len(pre_bars)}  |  After Cohere rerank: {n}\n\n"
        f"=== FULL HISTORY (saved, not sent to LLM) ===\n{history_table_full}\n\n"
        f"=== TOP-{n} AFTER COHERE EMBED+RERANK (sent to LLM) ===\n{history_table_compact}\n\n"
        f"=== CURRENT BAR ===\n{current_bar}"
    ))
    _save(_HIST_PROMPT_OUT, f"# {ts_str}\n\n{prompt}")

    try:
        raw     = await _api_call(api_key, prompt, max_tokens=800, timeout_s=60.0)
        elapsed = time.time() - t0
        _save(_HIST_RESPONSE, f"# {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}  elapsed={elapsed:.1f}s\n\n{raw}")
        for line in raw.splitlines():
            if line.strip().upper().startswith("SUGGESTION:"):
                suggestion = line.partition(":")[2].strip()
                if suggestion and suggestion.upper() != "NONE":
                    _append(_HIST_SUGGEST, f"[{ts_str}] {suggestion}")
                break
        signal_dict = _parse_historical_signal(raw)
        logger.info("Historical analyst %.1fs | %s %.0f%% | top-%d from %d bars via Cohere",
                    elapsed, signal_dict["signal"], signal_dict["confidence"] * 100, n, total_searched)
        return signal_dict, raw.strip()
    except Exception as exc:
        _save(_HIST_RESPONSE, f"# ERROR {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n{exc}")
        logger.warning("Historical analyst DeepSeek call failed: %s", exc)
        return None, None


# ═══════════════════════════════════════════════════════════════
# SECTION 4 — Main DeepSeek predictor
# ═══════════════════════════════════════════════════════════════

_PRED_DIR      = _ROOT / "specialists" / "main_predictor"
_PRED_PROMPT   = _PRED_DIR / "last_prompt.txt"
_PRED_RESPONSE = _PRED_DIR / "last_response.txt"
_PRED_SUGGEST  = _PRED_DIR / "suggestions.txt"


class DeepSeekPredictor:
    """Generates the main DeepSeek prediction at bar open."""

    def __init__(self, api_key: str, model: str = DEEPSEEK_MODEL, initial_bar_count: int = 0):
        self.api_key      = api_key
        self.model        = model
        self.window_count = initial_bar_count

    async def predict(
        self,
        prices, klines, features, strategy_preds, recent_accuracy,
        deepseek_accuracy, window_start_time, window_start_price,
        polymarket_slug=None, ensemble_result=None, dashboard_signals=None,
        indicator_accuracy=None, ensemble_weights=None,
        historical_analysis=None, creative_edge=None, dashboard_accuracy=None,
        neutral_analysis=None,
    ) -> Dict:
        self.window_count += 1
        t0 = time.time()

        polymarket_url   = (f"https://polymarket.com/event/{polymarket_slug}" if polymarket_slug else "")
        window_start_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(window_start_time))
        window_end_str   = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(window_start_time + 300))

        prompt = build_prompt(
            prices=prices, klines=klines, features=features,
            strategy_preds=strategy_preds, recent_accuracy=recent_accuracy,
            window_num=self.window_count, deepseek_accuracy=deepseek_accuracy,
            window_start_price=window_start_price, window_start_time=window_start_time,
            polymarket_slug=polymarket_slug, ensemble_result=ensemble_result,
            dashboard_signals=dashboard_signals, indicator_accuracy=indicator_accuracy,
            ensemble_weights=ensemble_weights, historical_analysis=historical_analysis,
            creative_edge=creative_edge, dashboard_accuracy=dashboard_accuracy,
            neutral_analysis=neutral_analysis,
        )

        ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        _save(_PRED_PROMPT, f"# {ts_str}  (window #{self.window_count})\n\n{prompt}")

        raw_response: Optional[str] = None
        error_msg    = ""
        try:
            raw_response = await _api_call(self.api_key, prompt, max_tokens=2500, timeout_s=50.0)
        except Exception as exc:
            error_msg = str(exc)
            logger.error("DeepSeek call failed: %s", exc)
            _append(_PRED_RESPONSE, f"\n{'='*60}\n# ERROR {ts_str}\n{'='*60}\n\n{exc}")

        if raw_response is None:
            return {
                "signal": "ERROR", "confidence": 0, "reasoning": error_msg,
                "data_received": "", "data_requests": "", "narrative": "", "free_observation": "",
                "raw_response": "", "full_prompt": prompt, "polymarket_url": polymarket_url,
                "window_start": window_start_str, "window_end": window_end_str,
                "latency_ms": int((time.time() - t0) * 1000), "completed_at": time.time(),
                "window_count": self.window_count,
            }

        _append(_PRED_RESPONSE, f"\n{'='*60}\n# {ts_str}  (window #{self.window_count})\n{'='*60}\n\n{raw_response}")
        signal, confidence, reasoning, data_received, data_requests, narrative, free_observation = parse_response(raw_response)
        latency_ms = int((time.time() - t0) * 1000)

        for line in raw_response.splitlines():
            if line.strip().upper().startswith("SUGGESTION:"):
                suggestion = line.partition(":")[2].strip()
                if suggestion and suggestion.upper() != "NONE":
                    _append(_PRED_SUGGEST, f"[{ts_str}] {suggestion}")
                break

        logger.info("DeepSeek #%d → %s  conf=%d%%  latency=%dms",
                    self.window_count, signal, confidence, latency_ms)

        return {
            "signal": signal, "confidence": confidence, "reasoning": reasoning,
            "data_received": data_received, "data_requests": data_requests,
            "narrative": narrative, "free_observation": free_observation,
            "raw_response": raw_response, "full_prompt": prompt,
            "polymarket_url": polymarket_url, "window_start": window_start_str,
            "window_end": window_end_str, "latency_ms": latency_ms,
            "completed_at": time.time(), "window_count": self.window_count,
        }
