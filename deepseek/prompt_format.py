"""
DeepSeek Query Format
=====================
Prompt : price structure, key levels, micro/macro trend + all indicator values.
         Last 100 × 1-min OHLCV bars sent as text to deepseek-chat.
Response expected:
    POSITION: ABOVE | BELOW
    CONFIDENCE: XX%
    REASON: 2-3 sentences
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

N_CANDLES  = 100   # bars sent to DeepSeek


# ─────────────────────────────────────────────────────────────
# Math helpers
# ─────────────────────────────────────────────────────────────

def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    k = 2 / (period + 1)
    out = np.empty(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _smma(arr: np.ndarray, period: int) -> np.ndarray:
    """Wilder's SMMA — used by Williams Alligator (full series)."""
    k = 1.0 / period
    out = np.empty(len(arr))
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out


def _smma_val(arr: np.ndarray, period: int) -> float:
    """Wilder's SMMA — single scalar value at the end of arr."""
    if len(arr) < period:
        return float(arr[-1])
    k = 1.0 / period
    val = float(arr[0])
    for v in arr[1:]:
        val = float(v) * k + val * (1.0 - k)
    return val


def _rsi(closes: np.ndarray, period: int = 4) -> np.ndarray:
    """Wilder's RSI — uses full history with exponential smoothing (matches TradingView)."""
    out = np.full(len(closes), 50.0)
    if len(closes) <= period:
        return out
    deltas = np.diff(closes.astype(float))
    gains  = np.maximum(deltas, 0.0)
    losses = np.maximum(-deltas, 0.0)
    avg_g = float(gains[:period].mean())
    avg_l = float(losses[:period].mean())
    def _rv(g, l):
        if l == 0:
            return 100.0 if g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + g / l)
    out[period] = _rv(avg_g, avg_l)
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        out[i + 1] = _rv(avg_g, avg_l)
    return out


def _mfi_series(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                vols: np.ndarray, period: int = 4) -> np.ndarray:
    """Wilder-smoothed MFI series — prevents spurious 100.0 from short windows."""
    n = len(closes)
    out = np.full(n, 50.0)
    if n <= period:
        return out
    tp  = (highs + lows + closes) / 3.0
    rmf = tp * vols
    dtp = np.diff(tp)
    # Seed
    pos = float(np.sum(rmf[1:period + 1][dtp[:period] > 0]))
    neg = float(np.sum(rmf[1:period + 1][dtp[:period] < 0]))
    def _mfi_val(p, ng):
        if ng == 0:
            return 100.0 if p > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + p / ng)
    out[period] = _mfi_val(pos, neg)
    for i in range(period, n - 1):
        new_pos = float(rmf[i + 1]) if dtp[i] > 0 else 0.0
        new_neg = float(rmf[i + 1]) if dtp[i] < 0 else 0.0
        pos = (pos * (period - 1) + new_pos) / period
        neg = (neg * (period - 1) + new_neg) / period
        out[i + 1] = _mfi_val(pos, neg)
    return out


def _linreg(y: np.ndarray) -> Tuple[float, float, float]:
    """Return (slope, intercept, r²) for array y."""
    n = len(y)
    if n < 2:
        return 0.0, float(y[0]) if n else 0.0, 0.0
    x = np.arange(n, dtype=float)
    m = (n * x.dot(y) - x.sum() * y.sum()) / (n * x.dot(x) - x.sum() ** 2)
    b = (y.sum() - m * x.sum()) / n
    ss_res = np.sum((y - (m * x + b)) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
    return m, b, max(0.0, r2)


# ─────────────────────────────────────────────────────────────
# Price-structure analysis
# ─────────────────────────────────────────────────────────────

def _find_swings(highs: np.ndarray, lows: np.ndarray, n: int = 3) -> Tuple[List, List]:
    """
    Return lists of (bar_index, price) for swing highs and swing lows.
    A swing high: high[i] >= all highs in [i-n .. i+n].
    A swing low : low[i]  <= all lows  in [i-n .. i+n].
    """
    sh, sl = [], []
    for i in range(n, len(highs) - n):
        window_h = highs[i - n: i + n + 1]
        window_l = lows[i - n: i + n + 1]
        if highs[i] >= window_h.max():
            sh.append((i, float(highs[i])))
        if lows[i] <= window_l.min():
            sl.append((i, float(lows[i])))
    return sh, sl


def _cluster(levels: List[float], tol_pct: float = 0.15) -> List[float]:
    """Merge price levels within tol_pct % of each other."""
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


def _market_structure(sh: List, sl: List) -> str:
    """Classify market structure from last 2 swing highs and 2 swing lows."""
    if len(sh) < 2 or len(sl) < 2:
        return "INSUFFICIENT DATA"
    h1, h2 = sh[-2][1], sh[-1][1]
    l1, l2 = sl[-2][1], sl[-1][1]
    hh, hl = h2 > h1, l2 > l1
    lh, ll = h2 < h1, l2 < l1
    if hh and hl:
        return "UPTREND  (HH + HL)"
    if lh and ll:
        return "DOWNTREND  (LH + LL)"
    if hh and ll:
        return "EXPANDING RANGE  (HH + LL)"
    if lh and hl:
        return "CONTRACTING RANGE  (LH + HL)"
    return "MIXED / RANGING"


def _trend_label(slope_per_bar: float, r2: float, price: float) -> str:
    slope_pct = slope_per_bar / price * 100
    strength = "strong" if r2 > 0.70 else "moderate" if r2 > 0.35 else "weak"
    direction = "up" if slope_pct > 0.001 else "down" if slope_pct < -0.001 else "flat"
    return f"{direction.upper()}  {strength}  (slope {slope_pct:+.4f}%/bar  R²={r2:.2f})"


# ─────────────────────────────────────────────────────────────
# Structure analysis (passed to prompt)
# ─────────────────────────────────────────────────────────────

def _analyse_structure(klines: List, tick_prices: List[float]):
    """
    Compute price-structure metrics from last N_CANDLES bars.
    Returns a dict of pre-computed values for the prompt.
    """
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

    # Swing levels
    sh_list, sl_list = _find_swings(highs, lows, n=3)
    res_raw = [p for _, p in sh_list if p > mid]
    sup_raw = [p for _, p in sl_list if p < mid]
    res = sorted(_cluster(res_raw))[:4]
    sup = sorted(_cluster(sup_raw))[-4:]

    # Market structure
    structure = _market_structure(sh_list, sl_list)

    # Macro trend (last 80 bars)
    macro_n = min(80, n)
    m_mac, _, r2_mac = _linreg(closes[-macro_n:])
    # Micro trend (last 20 bars)
    micro_n = min(20, n)
    m_mic, _, r2_mic = _linreg(closes[-micro_n:])

    # 100-bar range
    range_high = float(highs.max())
    range_low  = float(lows.min())
    range_pos  = (mid - range_low) / (range_high - range_low) if range_high > range_low else 0.5

    # Volume trend: average last 5 bars vs prior 20
    vol_recent = vols[-5:].mean()  if len(vols) >= 5  else vols.mean()
    vol_base   = vols[-25:-5].mean() if len(vols) >= 25 else vols.mean()
    vol_trend  = "INCREASING" if vol_recent > vol_base * 1.1 else \
                 "DECREASING" if vol_recent < vol_base * 0.9 else "STEADY"

    return dict(
        mid=mid,
        structure=structure,
        macro_label=_trend_label(m_mac, r2_mac, mid),
        micro_label=_trend_label(m_mic, r2_mic, mid),
        macro_slope_pct=m_mac / mid * 100,
        micro_slope_pct=m_mic / mid * 100,
        macro_r2=r2_mac,
        micro_r2=r2_mic,
        res=res,
        sup=sup,
        range_high=range_high,
        range_low=range_low,
        range_pos=range_pos,
        vol_trend=vol_trend,
        n_bars=n,
    )


# ─────────────────────────────────────────────────────────────
# Prompt builder
# ─────────────────────────────────────────────────────────────

def _tag(val: float, hi: float, lo: float, hi_lbl: str, lo_lbl: str) -> str:
    if val >= hi: return f"⚠ {hi_lbl}"
    if val <= lo: return f"⚠ {lo_lbl}"
    return "–"


def _fmt_usd(n: float) -> str:
    if n >= 1e9: return f"${n/1e9:.2f}B"
    if n >= 1e6: return f"${n/1e6:.1f}M"
    if n >= 1e3: return f"${n/1e3:.1f}K"
    return f"${n:.0f}"


def _build_dashboard_block(
    ds: Optional[Dict],
    window_start_price: float,
    dashboard_accuracy: Optional[Dict] = None,
) -> str:
    """
    Render the full market-microstructure block from dashboard_signals dict.
    Returns a multi-section string ready for insertion into the prompt.
    Each subsection clearly states:
      • the raw numbers
      • the derived signal + historical accuracy score
      • a 1-sentence interpretation for this specific bar
    Returns a placeholder when ds is None.
    """
    if not ds:
        return "  (dashboard signals unavailable this bar)"

    def _acc_tag(name: str) -> str:
        """Return a short accuracy label like '[acc 62% 15/24]' or '' if no data."""
        if not dashboard_accuracy:
            return ""
        stats = dashboard_accuracy.get(name)
        if not stats or stats.get("total", 0) < 5:
            return "  [acc: learning]"
        acc = stats["accuracy"] * 100
        cor = stats["correct"]
        tot = stats["total"]
        label = ("EXCELLENT" if acc >= 65 else
                 "RELIABLE"  if acc >= 55 else
                 "MARGINAL"  if acc >= 48 else
                 "WEAK")
        return f"  [acc {acc:.0f}% {cor}/{tot} — {label}]"

    def _s(key: str, sub: str, default="N/A"):
        v = ds.get(key)
        if v is None:
            return default
        return str(v.get(sub, default))

    def _f(key: str, sub: str, default=0.0) -> float:
        v = ds.get(key)
        if v is None:
            return default
        try:
            return float(v.get(sub, default))
        except (TypeError, ValueError):
            return default

    lines = []

    # ── 1. ORDER BOOK ─────────────────────────────────────────────────────────
    ob = ds.get("order_book")
    if ob:
        imb  = ob.get("imbalance_pct", 0)
        bv   = ob.get("bid_vol_btc", 0)
        av   = ob.get("ask_vol_btc", 0)
        sig  = ob.get("signal", "NEUTRAL")
        interp = ob.get("interpretation", "")
        lines += [
            "  [ORDER BOOK DEPTH — Binance spot, top-20 levels]",
            f"  Bid volume  : {bv:.1f} BTC    Ask volume : {av:.1f} BTC",
            f"  Imbalance   : {imb:+.2f}%   Signal: {sig}{_acc_tag('order_book')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [ORDER BOOK] unavailable", ""]

    # ── 2. LONG / SHORT RATIO ─────────────────────────────────────────────────
    ls = ds.get("long_short")
    if ls:
        lsr   = ls.get("retail_lsr", 1)
        rlp   = ls.get("retail_long_pct", 50)
        rsp   = ls.get("retail_short_pct", 50)
        tlp   = ls.get("smart_money_long_pct", 50)
        tsp   = ls.get("smart_money_short_pct", 50)
        div   = ls.get("smart_vs_retail_div_pct", 0)
        r_sig = ls.get("retail_signal_contrarian", "NEUTRAL")
        s_sig = ls.get("smart_money_signal", "NEUTRAL")
        interp = ls.get("interpretation", "")
        lines += [
            "  [LONG / SHORT RATIO — Binance Futures 5m]",
            f"  Retail     : L/S = {lsr:.3f}   Long {rlp:.1f}%  /  Short {rsp:.1f}%   "
            f"Contrarian signal: {r_sig}{_acc_tag('long_short')}",
            f"  Smart money: Long {tlp:.1f}%  /  Short {tsp:.1f}%   "
            f"Signal: {s_sig}   Divergence vs retail: {div:+.1f}%",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [LONG/SHORT RATIO] unavailable", ""]

    # ── 3. TAKER FLOW ─────────────────────────────────────────────────────────
    tk = ds.get("taker_flow")
    if tk:
        bsr   = tk.get("buy_sell_ratio", 1)
        tbv   = tk.get("taker_buy_vol_btc", 0)
        tsv   = tk.get("taker_sell_vol_btc", 0)
        sig   = tk.get("signal", "NEUTRAL")
        trend = tk.get("trend_3bars", "MIXED")
        interp = tk.get("interpretation", "")
        lines += [
            "  [TAKER AGGRESSOR FLOW — Binance Futures 5m, last 3 bars]",
            f"  Buy/Sell ratio : {bsr:.4f}   Taker buys: {tbv:.1f} BTC  "
            f"Taker sells: {tsv:.1f} BTC",
            f"  Signal: {sig}{_acc_tag('taker_flow')}    3-bar trend: {trend}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [TAKER FLOW] unavailable", ""]

    # ── 4. LIQUIDATIONS ───────────────────────────────────────────────────────
    lq = ds.get("liquidations")
    if lq:
        tot   = lq.get("total", 0)
        ll_c  = lq.get("long_liq_count",  0)
        sl_c  = lq.get("short_liq_count", 0)
        ll_u  = lq.get("long_liq_usd",    0)
        sl_u  = lq.get("short_liq_usd",   0)
        vel   = lq.get("velocity_per_min", 0.0)
        p_rng = lq.get("price_range",     "N/A")
        sig   = lq.get("signal", "NEUTRAL")
        interp = lq.get("interpretation", "")
        lines += [
            "  [LIQUIDATIONS — Binance Futures, last 5 min (up to 100 orders)]",
            f"  Long liquidated : {ll_c} orders  ({_fmt_usd(ll_u)})    "
            f"Short liquidated: {sl_c} orders  ({_fmt_usd(sl_u)})",
            f"  Velocity: {vel:.1f}/min   Price range: {p_rng}   Signal: {sig}{_acc_tag('liquidations')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [LIQUIDATIONS] unavailable", ""]

    # ── 5. OI + FUNDING (Binance perp) ────────────────────────────────────────
    oif = ds.get("oi_funding")
    if oif:
        oi_btc = oif.get("open_interest_btc", 0)
        fr_pct = oif.get("funding_rate_8h_pct", 0)
        mp     = oif.get("mark_price", 0)
        ip     = oif.get("index_price", 0)
        prem   = oif.get("mark_premium_vs_index_pct", 0)
        fr_sig = oif.get("funding_signal", "NEUTRAL")
        p_sig  = oif.get("premium_signal", "NEUTRAL")
        lines += [
            "  [OPEN INTEREST + FUNDING — Binance Futures perpetual]",
            f"  OI: {oi_btc:,.0f} BTC    Funding (8h): {fr_pct:+.5f}%  [{fr_sig}{_acc_tag('oi_funding')}]",
            f"  Mark: ${mp:,.2f}   Index: ${ip:,.2f}   Mark premium: {prem:+.4f}%  [{p_sig}]",
            "",
        ]
    else:
        lines += ["  [OI + FUNDING] unavailable", ""]

    # ── 6. COINALYZE CROSS-EXCHANGE FUNDING ───────────────────────────────────
    cz = ds.get("coinalyze")
    if cz:
        cz_fr  = cz.get("funding_rate_8h_pct", 0)
        cz_sig = cz.get("signal", "NEUTRAL")
        interp = cz.get("interpretation", "")
        # Cross-validate against Binance funding if available
        bn_fr  = oif.get("funding_rate_8h_pct", None) if oif else None
        delta  = f"  Δ vs Binance: {cz_fr - bn_fr:+.5f}%" if bn_fr is not None else ""
        lines += [
            "  [COINALYZE — Cross-exchange aggregate funding (BTCUSDT perp)]",
            f"  Aggregate funding (8h): {cz_fr:+.5f}%   Signal: {cz_sig}{_acc_tag('coinalyze')}{delta}",
            f"  → {interp}",
            "",
        ]

    # ── 7. COINAPI CROSS-EXCHANGE PRICE ───────────────────────────────────────
    ca = ds.get("coinapi")
    if ca:
        agg_rate = ca.get("aggregate_rate_usd", 0)
        if agg_rate and window_start_price:
            div_pct = ((window_start_price - agg_rate) / agg_rate) * 100
            d_sig   = ("BEARISH_ARBI" if div_pct > 0.05 else
                       "BULLISH_ARBI" if div_pct < -0.05 else "NEUTRAL")
            arbi_interp = (
                f"Binance trading at PREMIUM vs aggregate ({div_pct:+.4f}%). "
                "Arbitrageurs will sell Binance → downward pressure expected."
                if div_pct > 0.05 else
                f"Binance trading at DISCOUNT vs aggregate ({div_pct:+.4f}%). "
                "Arbitrageurs will buy Binance → upward pressure expected."
                if div_pct < -0.05 else
                f"No significant cross-exchange divergence ({div_pct:+.4f}%). "
                "Arbitrage pressure neutral."
            )
        else:
            div_pct = 0.0; d_sig = "NEUTRAL"; arbi_interp = "N/A"
        lines += [
            "  [COINAPI — Weighted aggregate rate (350+ exchanges)]",
            f"  Aggregate rate : ${agg_rate:,.2f}   Binance bar-open: ${window_start_price:,.2f}",
            f"  Divergence     : {div_pct:+.4f}%   Signal: {d_sig}",
            f"  → {arbi_interp}",
            "",
        ]

    # ── 8. FEAR & GREED ───────────────────────────────────────────────────────
    fg = ds.get("fear_greed")
    if fg:
        v      = fg.get("value", 50)
        lbl    = fg.get("label", "Neutral")
        pv     = fg.get("previous_day", v)
        delta  = fg.get("daily_delta", 0)
        sig    = fg.get("signal", "NEUTRAL")
        interp = fg.get("interpretation", "")
        lines += [
            "  [FEAR & GREED INDEX — alternative.me, daily]",
            f"  Score : {v}  ({lbl})   Yesterday: {pv}   Daily Δ: {delta:+d}",
            f"  Signal: {sig}{_acc_tag('fear_greed')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [FEAR & GREED] unavailable", ""]

    # ── 9. COINGECKO MARKET OVERVIEW ──────────────────────────────────────────
    cg = ds.get("coingecko")
    if cg:
        mcap = cg.get("market_cap_usd",        0)
        vol  = cg.get("volume_24h_usd",         0)
        ch   = cg.get("change_24h_pct",         0)
        vm   = cg.get("vol_to_mcap_ratio_pct",  0)
        interp = cg.get("interpretation", "")
        lines += [
            "  [COINGECKO MARKET OVERVIEW]",
            f"  Market cap : {_fmt_usd(mcap)}   24h vol : {_fmt_usd(vol)}   "
            f"Vol/MCap: {vm:.2f}%",
            f"  24h change : {ch:+.3f}%{_acc_tag('coingecko')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [COINGECKO] unavailable", ""]

    # ── 10. MEMPOOL ───────────────────────────────────────────────────────────
    mp = ds.get("mempool")
    if mp:
        ff     = mp.get("fastest_fee_sat_vb",    0)
        hf     = mp.get("half_hour_fee_sat_vb",  0)
        of_    = mp.get("hour_fee_sat_vb",        0)
        count  = mp.get("pending_tx_count",       0)
        mb     = mp.get("mempool_size_mb",        0)
        sig    = mp.get("signal", "NEUTRAL")
        interp = mp.get("interpretation", "")
        lines += [
            "  [MEMPOOL — mempool.space on-chain fee pressure]",
            f"  Fastest: {ff} sat/vB   30min: {hf} sat/vB   1hr: {of_} sat/vB",
            f"  Pending: {count:,} txs   Size: {mb:.2f} MB   Signal: {sig}{_acc_tag('mempool')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [MEMPOOL] unavailable", ""]

    # ── 11. COINAPI 5-MIN MOMENTUM ────────────────────────────────────────────
    cm = ds.get("coinapi_momentum")
    if cm:
        rc    = cm.get("rate_close", 0)
        roc1  = cm.get("roc_1bar_pct", 0)
        accel = cm.get("roc_accel", 0)
        sig   = cm.get("signal", "NEUTRAL")
        interp = cm.get("interpretation", "")
        lines += [
            "  [COINAPI MOMENTUM — 5-min rate-of-change across 350+ exchanges]",
            f"  Aggregate rate : ${rc:,.2f}   5m ROC: {roc1:+.4f}%   "
            f"Acceleration: {accel:+.4f}%   Signal: {sig}{_acc_tag('coinapi_momentum')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [COINAPI MOMENTUM] unavailable", ""]

    # ── 12. COINAPI LARGE TRADES ──────────────────────────────────────────────
    cl = ds.get("coinapi_large_trades")
    if cl:
        lt_cnt  = cl.get("large_trade_count", 0)
        lb      = cl.get("large_buy_btc", 0)
        ls_     = cl.get("large_sell_btc", 0)
        bp      = cl.get("large_buy_pct", 50)
        sig     = cl.get("signal", "NEUTRAL")
        interp  = cl.get("interpretation", "")
        lines += [
            "  [COINAPI LARGE TRADES — Binance spot trades ≥2 BTC]",
            f"  Large trades: {lt_cnt}   Buy: {lb:.2f} BTC   Sell: {ls_:.2f} BTC   "
            f"Buy%: {bp:.1f}%   Signal: {sig}{_acc_tag('coinapi_large_trades')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [COINAPI LARGE TRADES] unavailable", ""]

    # ── 13. KRAKEN PREMIUM ────────────────────────────────────────────────────
    kp = ds.get("kraken_premium")
    if kp:
        kpr    = kp.get("kraken_price", 0)
        bpr    = kp.get("binance_price", 0)
        spread = kp.get("spread_pct", 0)
        sig    = kp.get("signal", "NEUTRAL")
        interp = kp.get("interpretation", "")
        lines += [
            "  [KRAKEN PREMIUM — Kraken vs Binance institutional spread]",
            f"  Kraken: ${kpr:,.2f}   Binance: ${bpr:,.2f}   Spread: {spread:+.4f}%   "
            f"Signal: {sig}{_acc_tag('kraken_premium')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [KRAKEN PREMIUM] unavailable", ""]

    # ── 14. OI VELOCITY ───────────────────────────────────────────────────────
    oiv = ds.get("oi_velocity")
    if oiv:
        oi_cur  = oiv.get("oi_current_btc", 0)
        chg30   = oiv.get("oi_change_30m_pct", 0)
        chg1    = oiv.get("oi_change_1bar_pct", 0)
        sig     = oiv.get("signal", "NEUTRAL")
        interp  = oiv.get("interpretation", "")
        lines += [
            "  [OI VELOCITY — Binance Futures OI change rate over 30 min (6×5m bars)]",
            f"  Current OI: {oi_cur:,.0f} BTC   30m Δ: {chg30:+.4f}%   "
            f"Last bar Δ: {chg1:+.4f}%   Signal: {sig}{_acc_tag('oi_velocity')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [OI VELOCITY] unavailable", ""]

    # ── 15. SPOT WHALE FLOW ───────────────────────────────────────────────────
    swf = ds.get("spot_whale_flow")
    if swf:
        wb     = swf.get("whale_buy_btc", 0)
        ws     = swf.get("whale_sell_btc", 0)
        wp     = swf.get("whale_buy_pct", 50)
        total_ = swf.get("large_trade_btc", 0)
        sig    = swf.get("signal", "NEUTRAL")
        interp = swf.get("interpretation", "")
        lines += [
            "  [SPOT WHALE FLOW — Binance spot aggTrades ≥5 BTC per fill]",
            f"  Whale buys: {wb:.2f} BTC   Whale sells: {ws:.2f} BTC   "
            f"Total large: {total_:.2f} BTC   Buy%: {wp:.1f}%   "
            f"Signal: {sig}{_acc_tag('spot_whale_flow')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [SPOT WHALE FLOW] unavailable", ""]

    # ── 16. BYBIT LIQUIDATIONS ────────────────────────────────────────────────
    bl = ds.get("bybit_liquidations")
    if bl:
        bl_tot  = bl.get("total", 0)
        bl_lusd = bl.get("long_liq_usd", 0)
        bl_susd = bl.get("short_liq_usd", 0)
        sig     = bl.get("signal", "NEUTRAL")
        interp  = bl.get("interpretation", "")
        lines += [
            "  [BYBIT LIQUIDATIONS — cross-exchange cascade validation]",
            f"  Total: {bl_tot}   Long liq: {_fmt_usd(bl_lusd)}   "
            f"Short liq: {_fmt_usd(bl_susd)}   Signal: {sig}{_acc_tag('bybit_liquidations')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [BYBIT LIQUIDATIONS] unavailable", ""]

    # ── 17. OKX FUNDING RATE ──────────────────────────────────────────────────
    okx = ds.get("okx_funding")
    if okx:
        okx_fr  = okx.get("funding_rate_pct", 0)
        sig     = okx.get("signal", "NEUTRAL")
        interp  = okx.get("interpretation", "")
        # Cross-validate vs Binance funding
        bn_fr2  = oif.get("funding_rate_8h_pct", None) if ds.get("oi_funding") else None
        delta2  = f"   Δ vs Binance: {okx_fr - bn_fr2:+.5f}%" if bn_fr2 is not None else ""
        lines += [
            "  [OKX FUNDING RATE — independent cross-exchange funding confirmation]",
            f"  OKX funding (8h): {okx_fr:+.5f}%{delta2}   "
            f"Signal: {sig}{_acc_tag('okx_funding')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [OKX FUNDING] unavailable", ""]

    # ── 18. BTC DOMINANCE ─────────────────────────────────────────────────────
    btcd = ds.get("btc_dominance")
    if btcd:
        dom    = btcd.get("btc_dominance_pct", 50)
        mchg   = btcd.get("market_change_24h_pct", 0)
        sig    = btcd.get("signal", "NEUTRAL")
        interp = btcd.get("interpretation", "")
        lines += [
            "  [BTC DOMINANCE — CoinGecko global market share]",
            f"  BTC dominance: {dom:.2f}%   Market 24h Δ: {mchg:+.3f}%   "
            f"Signal: {sig}{_acc_tag('btc_dominance')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [BTC DOMINANCE] unavailable", ""]

    # ── 19. TOP TRADER POSITION RATIO (NOTIONAL) ──────────────────────────────
    tpr = ds.get("top_position_ratio")
    if tpr:
        lsr2   = tpr.get("long_short_ratio", 1)
        lp2    = tpr.get("long_position_pct", 50)
        sp2    = tpr.get("short_position_pct", 50)
        sig    = tpr.get("signal", "NEUTRAL")
        interp = tpr.get("interpretation", "")
        lines += [
            "  [TOP TRADER POSITION RATIO — Binance Futures notional-weighted]",
            f"  Ratio (L/S): {lsr2:.4f}   Long: {lp2:.1f}%   Short: {sp2:.1f}%   "
            f"Signal: {sig}{_acc_tag('top_position_ratio')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [TOP POSITION RATIO] unavailable", ""]

    # ── 20. FUNDING RATE TREND ────────────────────────────────────────────────
    ft = ds.get("funding_trend")
    if ft:
        fl     = ft.get("funding_latest_pct", 0)
        fa     = ft.get("funding_avg_6p_pct", 0)
        trend_ = ft.get("funding_trend", 0)
        sig    = ft.get("signal", "NEUTRAL")
        interp = ft.get("interpretation", "")
        lines += [
            "  [FUNDING RATE TREND — Binance 6-period history (30 min)]",
            f"  Latest: {fl:+.5f}%   6-period avg: {fa:+.5f}%   "
            f"Trend Δ: {trend_:+.5f}%   Signal: {sig}{_acc_tag('funding_trend')}",
            f"  → {interp}",
            "",
        ]
    else:
        lines += ["  [FUNDING TREND] unavailable", ""]

    return "\n".join(lines).rstrip()


def _build_dashboard_accuracy_block(dashboard_accuracy: Optional[Dict]) -> str:
    """
    Render a summary table of historical accuracy for each dashboard microstructure indicator.
    dashboard_accuracy: {name: {correct, total, accuracy}}
    """
    if not dashboard_accuracy:
        return "  (no microstructure history yet — accuracy scores appear after 5+ resolved bars)"

    _NAMES = {
        # Original 9
        "order_book":           "Order Book (imbalance)",
        "long_short":           "Long/Short (contrarian)",
        "taker_flow":           "Taker Flow (aggressor)",
        "oi_funding":           "OI + Funding (funding sig)",
        "liquidations":         "Liquidations (Binance)",
        "fear_greed":           "Fear & Greed (contrarian)",
        "mempool":              "Mempool (fee pressure)",
        "coinalyze":            "Coinalyze (x-ex funding)",
        "coingecko":            "CoinGecko (24h change)",
        # 10 new indicators
        "coinapi_momentum":     "CoinAPI Momentum (5m ROC)",
        "coinapi_large_trades": "CoinAPI Large Trades (≥2 BTC)",
        "kraken_premium":       "Kraken Premium (inst spread)",
        "oi_velocity":          "OI Velocity (30m rate)",
        "spot_whale_flow":      "Spot Whale Flow (≥5 BTC)",
        "bybit_liquidations":   "Bybit Liquidations (x-ex)",
        "okx_funding":          "OKX Funding Rate",
        "btc_dominance":        "BTC Dominance (rotation)",
        "top_position_ratio":   "Top Position Ratio (notional)",
        "funding_trend":        "Funding Rate Trend (6-period)",
    }

    lines = []
    for key in _NAMES:
        stats = dashboard_accuracy.get(key)
        if not stats:
            continue
        total   = stats.get("total", 0)
        correct = stats.get("correct", 0)
        acc     = stats.get("accuracy", 0.5)
        label   = ("EXCELLENT" if acc >= 0.65 else
                   "RELIABLE"  if acc >= 0.55 else
                   "MARGINAL"  if acc >= 0.48 else
                   "WEAK"      if total >= 5  else "LEARNING")
        note = ""
        if label == "EXCELLENT":
            note = "  ← HIGH TRUST"
        elif label == "WEAK":
            note = "  ← LOW TRUST — near coin-flip or worse"
        lines.append(
            f"  {_NAMES[key]:<34} {acc*100:5.1f}%  ({correct}/{total})  [{label}]{note}"
        )

    return "\n".join(lines) if lines else "  (no resolved microstructure data yet)"


def _build_indicator_track_record(
    indicator_accuracy: Optional[Dict],
    weights: Optional[Dict],
) -> str:
    """
    Render a table showing each indicator's historical accuracy, weight, and reliability.
    indicator_accuracy: {name: {correct, total, accuracy}}
    weights: {name: float}  — current ensemble weights
    """
    from strategies.ensemble import accuracy_to_label

    if not indicator_accuracy:
        return "  (no historical data yet — all indicators treated equally)"

    lines = []
    # Sort: disabled first (so DeepSeek sees warnings up front), then by accuracy desc
    def sort_key(item):
        acc = item[1].get("accuracy", 0.5)
        tot = item[1].get("total", 0)
        label = accuracy_to_label(acc, tot)
        order = {"DISABLED": 0, "WEAK": 1, "LEARNING": 2, "MARGINAL": 3,
                 "RELIABLE": 4, "EXCELLENT": 5}
        return order.get(label, 2), acc

    for name, stats in sorted(indicator_accuracy.items(), key=sort_key):
        correct  = stats.get("correct", 0)
        total    = stats.get("total", 0)
        accuracy = stats.get("accuracy", 0.5)
        w        = (weights or {}).get(name, 1.0)
        label    = accuracy_to_label(accuracy, total)

        note = ""
        if label == "DISABLED":
            note = "  ← IGNORE — worse than coin flip, near-zero weight"
        elif label == "WEAK":
            note = "  ← LOW TRUST — below 50%, treat with skepticism"
        elif label == "EXCELLENT":
            note = "  ← HIGH TRUST — consistently outperforms"
        elif label == "RELIABLE":
            note = "  ← TRUST — above-average track record"

        lines.append(
            f"  {name:<22} {accuracy*100:5.1f}%  ({correct}/{total})  "
            f"weight={w:.2f}  [{label}]{note}"
        )

    return "\n".join(lines) if lines else "  (no resolved predictions yet)"


def build_prompt(
    prices:               List[float],
    klines:               List,
    features:             Dict[str, float],
    strategy_preds:       Dict,
    recent_accuracy:      float,
    window_num:           int,
    deepseek_accuracy:    Dict,
    window_start_price:   float,
    window_start_time:    float,
    polymarket_slug:      Optional[str]  = None,
    ensemble_result:      Optional[Dict] = None,
    dashboard_signals:    Optional[Dict] = None,
    indicator_accuracy:   Optional[Dict] = None,
    ensemble_weights:     Optional[Dict] = None,
    pattern_analysis:     Optional[str]  = None,
    creative_edge:        Optional[str]  = None,
    bar_insight:          Optional[str]  = None,
    dashboard_accuracy:   Optional[Dict] = None,
) -> str:
    f  = features
    fv = lambda k, d=0.0: f.get(k, d)

    # Prefer kline-based values already computed by strategies over tick-based features
    def _strat_val(key: str, default: float) -> float:
        s = strategy_preds.get(key, {})
        try:
            v = float(s.get("value", ""))
            return v
        except (TypeError, ValueError):
            return default

    now  = prices[-1]
    p1m  = prices[-30]  if len(prices) >= 30  else prices[0]
    p5m  = prices[-150] if len(prices) >= 150 else prices[0]
    p15m = prices[-450] if len(prices) >= 450 else prices[0]
    def pct(a, b): return ((a / b) - 1) * 100 if b else 0

    # Structure analysis
    sa = _analyse_structure(klines, prices)

    # Levels block
    res, sup = sa["res"], sa["sup"]
    level_lines = []
    for lvl in reversed(res):
        dist = (lvl - now) / now * 100
        level_lines.append(f"  RESISTANCE  ${lvl:>10,.2f}   (+{dist:.2f}%)")
    level_lines.append(f"  ── current  ${now:>10,.2f} ──")
    for lvl in reversed(sup):
        dist = (now - lvl) / now * 100
        level_lines.append(f"  SUPPORT     ${lvl:>10,.2f}   (-{dist:.2f}%)")
    levels_block = "\n".join(level_lines)

    # Macro/micro alignment note
    mac_up = sa["macro_slope_pct"] > 0
    mic_up = sa["micro_slope_pct"] > 0
    if mac_up and mic_up:
        alignment = "macro UP + micro UP → momentum aligned bullish"
    elif mac_up and not mic_up:
        alignment = "macro UP + micro DOWN → pullback in uptrend"
    elif not mac_up and mic_up:
        alignment = "macro DOWN + micro UP → bounce in downtrend"
    else:
        alignment = "macro DOWN + micro DOWN → momentum aligned bearish"

    # Range position
    rp = sa["range_pos"]
    rp_str = ("upper third" if rp > 0.66 else
              "lower third" if rp < 0.33 else "mid range")

    # Dashboard signals block (market microstructure)
    dashboard_block = _build_dashboard_block(dashboard_signals, window_start_price, dashboard_accuracy)

    # Indicators — use strategy (kline-based) values where available, fall back to features
    rsi4    = _strat_val("rsi", fv("rsi_7", 50))
    stoch   = _strat_val("stochastic", fv("stoch_k_14", 50))
    mfi4    = _strat_val("mfi", fv("mfi_7", 50))
    macd    = fv("macd"); macd_h = fv("macd_histogram")
    bb_pct  = fv("bollinger_pct_b", 0.5); bb_w = fv("bollinger_width")
    vol10   = fv("volatility_10")
    ret1    = fv("return_1"); ret5 = fv("return_5"); ret10 = fv("return_10")

    macd_dir = "bullish" if macd_h > 0 else "bearish"
    macd_exp = ("expanding" if abs(macd_h) > abs(fv("macd_histogram_prev", macd_h))
                else "contracting")
    # EMA cross — use live strategy values (EMA5/13 fast, EMA21/55 slow, both 1m)
    ema_s = strategy_preds.get("ema_cross", {})
    ema_fast_diff  = float(ema_s.get("value", 0) or 0)
    ema_slow_diff  = float(ema_s.get("slow_diff", 0) or 0)
    ema_fast_sig   = ema_s.get("signal", "?")
    ema_slow_sig   = ema_s.get("htf_signal", "?")
    ema_agree      = ema_fast_sig == ema_slow_sig if ema_fast_sig not in ("?","N/A") and ema_slow_sig not in ("?","N/A") else None

    # Williams Alligator values from klines
    alligator_block = "(no OHLCV)"
    if klines and len(klines) >= 15:
        kc     = np.array([float(k[4]) for k in klines], dtype=float)
        jaw_v  = float(_smma_val(kc, 13))
        tth_v  = float(_smma_val(kc, 8))
        lip_v  = float(_smma_val(kc, 5))
        allig_bull = lip_v > tth_v > jaw_v
        allig_bear = lip_v < tth_v < jaw_v
        allig_state = "BULLISH (Lips>Teeth>Jaw)" if allig_bull else ("BEARISH (Lips<Teeth<Jaw)" if allig_bear else "SLEEPING (ranging)")
        alligator_block = (f"  Jaw(13)={jaw_v:,.2f}  Teeth(8)={tth_v:,.2f}  Lips(5)={lip_v:,.2f}\n"
                           f"  State: {allig_state}")

    # Fibonacci levels from recent swing
    fib_block = "(no range data)"
    use_klines = bool(klines and len(klines) >= 30)
    if use_klines:
        kc30   = np.array([float(k[4]) for k in klines[-30:]])
        khigh  = np.array([float(k[2]) for k in klines[-30:]])
        klow   = np.array([float(k[3]) for k in klines[-30:]])
        sw_hi  = float(khigh.max()); sw_lo = float(klow.min())
        rng30  = sw_hi - sw_lo
        if rng30 > 0:
            hi_idx = int(khigh.argmax()); lo_idx = int(klow.argmin())
            uptrend30 = hi_idx > lo_idx
            pb_pct = (sw_hi - now) / rng30 if uptrend30 else (now - sw_lo) / rng30
            fib_block = (
                f"  Swing H=${sw_hi:,.2f}  L=${sw_lo:,.2f}  range=${rng30:,.2f}\n"
                f"  Current pullback: {pb_pct*100:.1f}% ({'from high in uptrend' if uptrend30 else 'from low in downtrend'})\n"
                f"  Fib 38.2% = ${sw_hi - 0.382*rng30:,.2f}   "
                f"50% = ${sw_hi - 0.500*rng30:,.2f}   "
                f"61.8% = ${sw_hi - 0.618*rng30:,.2f}"
            )

    # A/D line slope
    acc_dist_block = "(no OHLCV)"
    if klines and len(klines) >= 10:
        ad = 0.0; ad_vals = []
        for k in klines[-20:]:
            h, l, c, v = float(k[2]), float(k[3]), float(k[4]), float(k[5])
            ad += ((c - l) - (h - c)) / (h - l) * v if h != l else 0.0
            ad_vals.append(ad)
        ad_slope = ad_vals[-1] - ad_vals[-5] if len(ad_vals) >= 5 else 0.0
        acc_dist_block = f"  A/D={ad_vals[-1]:.0f}  slope(5)={ad_slope:+.0f}  {'ACCUMULATION ↑' if ad_slope > 0 else 'DISTRIBUTION ↓'}"

    # CSV of last 50 bars — enriched (same columns as specialists get)
    # Columns: Time(UTC),Open,High,Low,Close,Volume(BTC),QuoteVol(USDT),Trades,BuyVol%
    # Time(UTC) is MM-DD HH:MM — use this to reference bars (NOT a sequential bar number)
    # k[5]=base_vol  k[7]=quote_vol  k[8]=trades  k[9]=taker_buy_base_vol
    csv_block = "(no kline data)"
    if klines and len(klines) >= 5:
        csv_rows = ["Time(UTC),Open,High,Low,Close,Volume,QuoteVol,Trades,BuyVol%"]
        sample = klines[-50:]
        for k in sample:
            try:
                ts_str  = time.strftime("%m-%d %H:%M", time.gmtime(int(k[0]) / 1000))
                vol     = float(k[5])
                quote_v = float(k[7]) if len(k) > 7 else 0.0
                trades  = int(k[8])   if len(k) > 8 else 0
                buy_vol = float(k[9]) if len(k) > 9 else 0.0
                buy_pct = round(buy_vol / vol * 100, 1) if vol > 0 else 0.0
                csv_rows.append(
                    f"{ts_str},{float(k[1]):.2f},{float(k[2]):.2f},"
                    f"{float(k[3]):.2f},{float(k[4]):.2f},{vol:.1f},"
                    f"{quote_v:.0f},{trades},{buy_pct}"
                )
            except Exception:
                pass
        csv_block = "\n".join(csv_rows)

    # Strategy block
    bullish = sum(1 for p in strategy_preds.values() if p.get("signal") == "UP")
    bearish = sum(1 for p in strategy_preds.values() if p.get("signal") == "DOWN")
    strat_lines = []
    for name, pred in strategy_preds.items():
        arrow = "↑" if pred.get("signal") == "UP" else "↓"
        conf  = pred.get("confidence", 0) * 100
        rsn   = (pred.get("reasoning") or "")[:55]
        strat_lines.append(f"  {name:<18} {arrow} {conf:4.0f}%  {rsn}")

    # Ensemble vote block (pre-computed weighted result)
    if ensemble_result:
        ens_signal   = ensemble_result.get("signal", "?")
        ens_conf     = ensemble_result.get("confidence", 0) * 100
        ens_up_prob  = ensemble_result.get("up_probability", 0.5) * 100
        ens_bull     = ensemble_result.get("bullish_count", bullish)
        ens_bear     = ensemble_result.get("bearish_count", bearish)
        ens_w_up     = ensemble_result.get("weighted_up_score", 0)
        ens_w_dn     = ensemble_result.get("weighted_down_score", 0)
        ensemble_block = (
            f"  Signal      : {ens_signal}\n"
            f"  Confidence  : {ens_conf:.1f}%\n"
            f"  UP prob     : {ens_up_prob:.1f}%\n"
            f"  Votes       : {ens_bull}↑ bullish  /  {ens_bear}↓ bearish\n"
            f"  Weighted UP : {ens_w_up:.3f}   Weighted DN : {ens_w_dn:.3f}"
        )
    else:
        ensemble_block = f"  {bullish}↑ bullish  /  {bearish}↓ bearish  (no weighted result)"

    recent_accuracy = recent_accuracy or 0.0   # guard: None when MongoDB is down
    ds_total   = (deepseek_accuracy or {}).get("total", 0)
    ds_correct = (deepseek_accuracy or {}).get("correct", 0)
    ds_str     = (f"{ds_correct}/{ds_total}  ({(deepseek_accuracy or {}).get('accuracy', 0)*100:.1f}%)"
                  if ds_total > 0 else "no prior predictions")

    # Indicator track record block
    indicator_track_record = _build_indicator_track_record(indicator_accuracy, ensemble_weights)

    # Dashboard microstructure accuracy block
    dashboard_accuracy_block = _build_dashboard_accuracy_block(dashboard_accuracy)

    # Creative edge block
    creative_block = (
        creative_edge.strip()
        if creative_edge and creative_edge.strip()
        else "  (no creative edge observation this window)"
    )

    # Pattern analyst block
    pattern_block = (
        pattern_analysis.strip()
        if pattern_analysis and pattern_analysis.strip()
        else "  (pattern analyst did not fire this window)"
    )

    # Bar insight analyst block
    bar_insight_block = (
        bar_insight.strip()
        if bar_insight and bar_insight.strip()
        else "  (bar insight analyst did not fire this window — needs 5+ resolved bars)"
    )

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
  PRICE STRUCTURE  (last {sa['n_bars']} bars)
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
  (Unified technical specialist — non-standard observation from OHLCV scan)
──────────────────────────────────────────────
{creative_block}

──────────────────────────────────────────────
  MICROSTRUCTURE INDICATOR ACCURACY  (historical correctness of each live signal)
  Each accuracy score = how often this indicator's UP/DOWN call matched actual bar direction.
  NEUTRAL calls are not scored. Scores shown inline next to each signal above.
  Weight HIGH TRUST signals more heavily; treat WEAK signals with skepticism.
──────────────────────────────────────────────
{dashboard_accuracy_block}

──────────────────────────────────────────────
  INDICATOR TRACK RECORD  (last ~100 resolved predictions)
  Ensemble weights are auto-adjusted by accuracy. DISABLED = <40%, near-zero weight.
  When DISABLED or WEAK indicators disagree with RELIABLE/EXCELLENT ones, favour the latter.
──────────────────────────────────────────────
{indicator_track_record}

──────────────────────────────────────────────
  PATTERN ANALYST  (historical indicator pattern matching)
  Finds similar past indicator setups and time/session patterns.
──────────────────────────────────────────────
{pattern_block}

──────────────────────────────────────────────
  BAR INSIGHT ANALYST  (deep similarity — full bar history with all specialist outputs)
  Analyses agreement/disagreement between ensemble, DeepSeek, and all specialists.
  Finds second-order patterns: when does agreement predict accuracy?
──────────────────────────────────────────────
{bar_insight_block}

──────────────────────────────────────────────
  LAST 50 BARS  (1-min, real Binance data)
  Columns: Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%
  Time(UTC) format: MM-DD HH:MM  — ALWAYS reference bars by their Time(UTC) (e.g. "04:15"), NEVER by a sequential bar number.
  BuyVol% = taker-buy base volume / total volume × 100  (>60 = buyers aggressive, <40 = sellers aggressive)
  Rows are oldest → newest. The last row is the current (most recent) bar.
──────────────────────────────────────────────
{csv_block}

──────────────────────────────────────────────
  TRACK RECORD
──────────────────────────────────────────────
  Ensemble (last 12)  {recent_accuracy*100:.1f}%
  Your prior          {ds_str}

══════════════════════════════════════════════
  HOW TO ANALYZE — NARRATIVE PRICE FRAMEWORK
══════════════════════════════════════════════

PRICE IS A BATTLE RECORD. Every candle is a skirmish between buyers and sellers.
Your job: read who won past battles, reconstruct the story, and predict the next move.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  READING THE LAST 50 BARS — BUILD THE STORY
  NOTE: Reference ALL bars by their Time(UTC) (e.g. "04:15"), never by sequential number.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SWING HIGH = Buyer victory. Buyers overwhelmed sellers and pushed price to that peak.
  → High volume at the high: buyers had real conviction. Low volume: fragile, suspect.
  → "Buyers defeated sellers at $X at HH:MM. If we return to $X — will buyers win again, or
     have the defending forces been depleted?"

SWING LOW = Seller victory. Sellers forced buyers to abandon positions at that trough.
  → High volume at the low: genuine distribution. Low volume pullback: sellers are weak.
  → "Sellers forced capitulation at $Y at HH:MM. If we return to $Y — will sellers press harder
     or were they already exhausted?"

Higher Highs + Higher Lows = BUYER DOMINANCE NARRATIVE:
  "Buyers are winning battle after battle. Each rally exceeds the last — fresh buyers
   keep arriving. Each pullback holds HIGHER than before — existing buyers refuse to sell
   cheap. The selling pressure is weakening at each test of support."

Lower Highs + Lower Lows = SELLER DOMINANCE NARRATIVE:
  "Sellers dominate. Each bounce gets rejected sooner — buyer momentum exhausts earlier.
   Each new low proves sellers are pressing harder. Buyers are losing the will to fight."

LOOK LEFT — Wick Zones (price memory at its most visible):
  Upper wick on a candle = sellers ambushed buyers at that level.
    → "At HH:MM, price wicked to $X — sellers crushed the rally hard and fast.
       Price is now retesting $X. Sellers likely remember this level. Watch for rejection."
  Lower wick on a candle = buyers absorbed sellers aggressively and snapped back.
    → "At HH:MM, price wicked down to $Y — buyers stepped in with force.
       Price returning to $Y = a return to that buyer-power zone. Watch for bounce."
  PROMINENT WICK RULE: If current price is AT or NEAR a prominent wick high or low
  from the last 50 bars, name it explicitly in your NARRATIVE with its exact Time(UTC)
  (e.g. "04:15"), exact price, and what BuyVol% was at that bar (conviction check).
  DO NOT use sequential bar numbers — use the Time(UTC) column value only.

VOLUME CONFIRMS THE STORY:
  High-volume up bar: Buyers showed up with conviction. The move is real.
  High-volume down bar: Sellers showed up with conviction. The selling is real.
  Low-volume new HIGH: "False breakout suspect — not enough buyers at the highs."
  Low-volume PULLBACK: "Healthy — sellers not pressing hard. Trend likely continues."
  BuyVol% > 60%: Aggressors are buyers. They're paying up to buy NOW.
  BuyVol% < 40%: Aggressors are sellers. They're hitting bids to sell NOW.
  BuyVol% ~50% on a large move: Passive — the move may not sustain.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  DATA STREAM ANALYSIS GUIDE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ORDER BOOK — The Visible Battlefield:
  Bid wall = buyers standing their ground at a price. A visible line in the sand.
  Ask wall = sellers defending a ceiling.
  ABSORPTION signal (most important): taker sells are heavy BUT price is NOT falling
    → bid wall is quietly defeating sellers. Extremely bullish. "Battle won in silence."
  ABSORPTION bearish: taker buys are heavy BUT price is NOT rising
    → ask wall is absorbing buyers. "Sellers are holding the fortress."
  Imbalance > +5%: Buyer firepower exceeds seller firepower. "Larger defending army."
  Imbalance < -5%: Seller firepower dominates. "Cap on rallies, expect resistance."
  Shifting imbalance (was −8%, now +3%): Buyers repositioning. Early reversal signal.

TAKER FLOW — Who Is Being Aggressive:
  Market BUY = Someone so eager they paid the ask. That is CONVICTION.
  Market SELL = Someone so urgent they hit the bid. That is PANIC or CONVICTION.
  BSR > 1.0: Buyers are the aggressors. "Buyers are chasing. They cannot wait."
  BSR < 1.0: Sellers are the aggressors. "Sellers are dumping. They cannot wait."
  3-bar trend accelerating toward 1.0 from below = buyer return, potential reversal.
  3-bar trend decelerating from >1.2 = buyer exhaustion. Top may be forming.
  Rising price + falling BSR = price rising but buyers losing urgency. Caution signal.
  Falling price + rising BSR = price falling but buyers stepping in. Watch for bottom.

LIQUIDATIONS — Forced Soldiers:
  Long liqs: over-leveraged bulls margin called → forced selling (waterfall risk).
    Cluster of long liqs = cascade. Each forced sell triggers the next stop-loss.
    After large long liq sweep: "All weak longs are gone. Who is left to sell?" = bottom.
  Short liqs: over-leveraged bears forced to buy → short squeeze (irrational spike risk).
    After short liqs exhaust: artificial buying ends, sharp reversal often follows.
  Scale matters: $50K = noise. $500K+ = meaningful. $2M+ = structural price impact.

LONG/SHORT RATIO — The Crowd's Bet:
  Retail is a CONTRARIAN indicator. Extremes reveal where maximum pain will come.
    Retail > 60% long = crowd tilted. Market inflicts pain = DOWN.
    Retail > 60% short = crowd tilted short. Pain comes UP = squeeze fuel.
  Smart money (top 20%) is a FOLLOW signal. They survived markets for a reason.
    Smart money long + retail short = strong bullish divergence. Follow smart money.
    Smart money short + retail long = strong bearish. Professionals fading the crowd.
  Divergence: ±5% = mild. ±10% = meaningful. ±15%+ = highest-conviction contrarian signal.

FUNDING RATE — The Cost of the Battle:
  Positive funding: longs pay shorts every 8h. "Bulls taxed to stay leveraged."
    High positive (>0.05%): leveraged longs are a rubber band — any dip triggers cascades.
    Mild positive (0.01%): normal healthy long market. No extreme squeeze risk.
  Negative funding: shorts paying longs. Short squeeze fuel building.
  Binance vs Coinalyze divergence: Binance extreme but Coinalyze mild = Binance-specific
    overleveraging. Cross-exchange contagion risk is lower. Squeeze contained to Binance.

CROSS-EXCHANGE DIVERGENCE (CoinAPI aggregate + Kraken premium):
  CoinAPI aggregate PREMIUM: Binance trading above 350-exchange weighted rate.
    Arbi bots will sell Binance mechanically → 1-3 min selling pressure.
  CoinAPI aggregate DISCOUNT: Binance below fair value → arbi buyers incoming.
  Kraken PREMIUM (+0.05%+): US/EU institutional buyers paying above Binance.
    Historically precedes sustained directional moves driven by regulated-market demand.
  Kraken DISCOUNT: regulated sellers dominating; can front-run retail reversal.
  Under ±0.05%: background noise, not actionable for 5-min close.

COINAPI LARGE TRADES (≥2 BTC fills via CoinAPI):
  Whale buy dominance (>60%): large spot orders hitting the ask — accumulation.
    Real money moving, not leverage. Spot buying = directional conviction.
  Whale sell dominance (<40%): large orders hitting the bid — distribution.
  Watch for divergence vs order-book: whale buys + ask-heavy book = invisible wall.

OI VELOCITY (Binance OI change over 30 min):
  Rapid OI increase (+0.3%+ over 30m): new positions opening aggressively.
    Combined with rising price = longs piling in. Combined with falling price = short attack.
  OI collapse (−0.3%+): de-levering — positions being closed, often precedes volatility.
  Flat OI + price moving: price driven by spot, not leverage. More sustainable.

SPOT WHALE FLOW (Binance aggTrades ≥5 BTC):
  Distinct from taker flow: captures single large fills, not aggregate aggressor ratio.
  Whale buy > 60% of large fills: real money entering long on spot — no leverage risk.
  Whale sell > 60%: holders distributing. Spot selling is structurally more bearish
    than futures selling (no forced cover, no funding reset mechanism).

BYBIT LIQUIDATIONS (cross-exchange cascade validation):
  Confirms or denies Binance liquidation signals. If BOTH exchanges show same cascade
    direction: signal strength doubles. Binance says long cascade but Bybit NEUTRAL =
    Binance-specific, contained risk. Both showing = systemically important cascade.
  Cross-exchange short squeeze (both BULLISH): most reliable squeeze signal available.

MULTI-EXCHANGE FUNDING (Binance vs Coinalyze vs OKX):
  Three independent funding readings. Agreement = systemic leverage imbalance.
  Divergence (e.g. Binance positive but OKX negative): exchange-specific positioning,
    not market-wide. Lower cascade contagion risk. Less predictive.
  Rising funding trend (6-period): leverage BUILDING, not static. Cascade risk
    increases with each bar. A rising trend hitting 0.05%+ = extreme squeeze risk.
  Falling funding trend: de-levering in progress. Price often stabilises or bounces.

TOP TRADER POSITION RATIO (notional-weighted):
  Unlike account ratio (counts traders), position ratio weights by dollar size.
  A ratio of 1.5 means top traders hold 60% of notional in longs.
  Follow signal (not contrarian): top traders' notional bets predict short-term direction
    more reliably than retail account counts. Ratio >1.3 = substantial long bias.
  Divergence vs retail L/S: if top position ratio is bullish BUT retail is contrarian
    bullish (too many retail longs) — smart money positioned same as crowd = less edge.

BTC DOMINANCE (macro rotation):
  Rising dominance (>54%): capital leaving alts, flowing into BTC. Bitcoin-specific
    buying pressure. Bullish for 5m BTC. Common in risk-off environments.
  Falling dominance (<44%): alt season — capital rotating out of BTC. Mild bearish
    backdrop for BTC, though 5m impact is indirect (background context).
  Watch for dominance + market cap divergence: dominance up + total mcap down =
    alts crashing harder than BTC (flight to safety). Bullish BTC context.

FEAR & GREED / COINGECKO / MEMPOOL — Macro Context (background for 5-min):
  F&G < 25: Full panic. Strong hands accumulating. Bullish backdrop.
  F&G > 75: Euphoria. Latecomers buying tops. Bearish backdrop.
  Mempool high fees: urgent on-chain movement. Could signal exchange deposits (selling).
  CoinGecko large 24h negative: overhead resistance from underwater bag-holders.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  SYNTHESIS PRIORITIES (5-minute close)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
IMMEDIATE (highest weight): Order book absorption + taker flow direction + active
  liquidation cascade direction (Binance AND Bybit cross-validation). Spot whale flow
  confirming direction. These four together are the most predictive signal cluster
  for the next 5 minutes.

MOMENTUM (medium weight): L/S smart-vs-retail divergence, top position ratio
  (notional-weighted smart-money bet), funding rate trend direction + OI velocity,
  cross-exchange arbi pressure (CoinAPI aggregate + Kraken premium),
  RSI/Stoch/MACD/Alligator, ensemble vote alignment.

STRUCTURE (context, lower weight for 5-min): Dow HH/HL vs LH/LL, Fib levels,
  key S/R from wick analysis, Harmonic patterns, A/D divergence, MTF agreement.

MACRO (lowest weight — note extremes only): F&G, BTC dominance rotation,
  CoinGecko context, Mempool stress.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  FREE RESEARCH — YOUR OWN PATTERN RECOGNITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
After structured analysis, scan the full data one more time with fresh eyes.
Note ANYTHING unusual, unexpected, or interesting you observe that doesn't fit the
standard framework. This could be:
  • A funding/price anomaly (e.g. funding positive but price falling — who absorbs?)
  • Volume signature that contradicts the price move (high sell vol but price held)
  • Order book structure suggesting a large hidden player (sudden wall appearances)
  • A confluence of signals you rarely see combined (extreme squeeze + F&G panic)
  • A wick sequence pattern (three consecutive lower wicks = buyers defending a shelf)
  • Anything giving you unusually high OR low confidence in this call
Cite specific numbers. This is your intellectual freedom zone — trust your pattern recognition.

══════════════════════════════════════════════
RESPOND EXACTLY IN THIS FORMAT (no extra text before or after):
══════════════════════════════════════════════
POSITION: ABOVE
CONFIDENCE: XX%
DATA_RECEIVED: [REQUIRED — state exactly which signals were available. Example: "Received {n_bars_v} bars OHLCV, all indicator values, microstructure signals (order book, taker flow, L/S ratio, Binance liquidations, OI+funding, funding trend, OI velocity, Coinalyze, CoinAPI aggregate, CoinAPI momentum, CoinAPI large trades, Kraken premium, spot whale flow, Bybit liquidations, OKX funding, BTC dominance, top position ratio, F&G, mempool, CoinGecko), ensemble vote."]
DATA_REQUESTS: [NONE — or list specific additional data that would improve accuracy.]
NARRATIVE: [2-4 sentences telling the STORY of the chart. Name specific prices and TIMES from the Time(UTC) column — NEVER use sequential bar numbers. Who won recent battles? Where did buyers/sellers show force? Any prominent wick zones price is now testing? What does the volume conviction say? Example: "Buyers pushed from $83,200 to swing high $84,150 at 04:12 on rising volume with BuyVol 66%%, showing genuine conviction. Price pulled back to $83,400 at 04:23 where a long lower wick formed — buyers absorbed aggressively and snapped back, marking that zone as buyer territory. We are now retesting the $84,150 swing high where sellers have twice produced upper wicks with declining BuyVol%%, suggesting seller defense is weakening but not yet defeated."]
FREE_OBSERVATION: [1-2 sentences on anything unusual or particularly interesting you noticed across any data stream. Cite exact numbers. If nothing is anomalous, state the single most significant convergence of signals you see.]
REASONS:
1. [MICROSTRUCTURE: Order book imbalance (exact %%), taker flow BSR (exact value + 3-bar trend), Binance liquidation status (direction + USD, cascade active?), Bybit liquidation cross-validation (confirms or contradicts Binance?), spot whale flow buy% (exact) — what story does this tell about the next 5 minutes?]
2. [FUNDING + POSITIONING: Binance funding rate + OKX funding cross-validation (exact values, agree or diverge?), funding trend direction (6-period — rising/falling), OI velocity (30m change %), top position ratio (notional %, exact), L/S smart-vs-retail divergence (exact %%), CoinAPI aggregate + Kraken premium arbi direction — cite all values.]
3. [TECHNICAL + CROSS-EXCHANGE: RSI/Stoch/MACD state, Alligator state, nearest Fib level, key S/R from wick analysis, CoinAPI momentum ROC (exact %), CoinAPI large trade buy% (exact), BTC dominance %, ensemble vote — do all confirm or conflict with microstructure? MTF agreement?]
4. [SYNTHESIS: Dominant bias in one sentence. The SINGLE most decisive factor. Biggest risk to this call. Final conviction level and why.]"""


# ─────────────────────────────────────────────────────────────
# Response parser
# ─────────────────────────────────────────────────────────────

def parse_response(text: str) -> Tuple[str, int, str, str, str, str, str]:
    """
    Parse DeepSeek response.
    Returns (signal, confidence_pct, reasoning, data_received, data_requests,
             narrative, free_observation).
    signal mapped to "UP" | "DOWN" | "UNKNOWN" for storage compatibility.
    Numbered reasons are preserved with newlines so the UI can render them as a list.
    """
    import re
    signal, confidence = "UNKNOWN", 50
    numbered: List[str] = []
    data_received    = ""
    data_requests    = ""
    narrative        = ""
    free_observation = ""
    in_reasons       = False

    for line in text.strip().splitlines():
        s = line.strip()
        u = s.upper()

        if u.startswith("POSITION:"):
            val = u.replace("POSITION:", "").strip()
            if "ABOVE" in val:   signal = "UP"
            elif "BELOW" in val: signal = "DOWN"

        elif u.startswith("CONFIDENCE:"):
            try:
                confidence = int(float(
                    u.replace("CONFIDENCE:", "").replace("%", "").strip()
                ))
            except ValueError:
                pass

        elif s.upper().startswith("DATA_RECEIVED:"):
            data_received = s[len("DATA_RECEIVED:"):].strip()
            in_reasons = False

        elif s.upper().startswith("DATA_REQUESTS:"):
            data_requests = s[len("DATA_REQUESTS:"):].strip()
            in_reasons = False

        elif s.upper().startswith("NARRATIVE:"):
            narrative = s[len("NARRATIVE:"):].strip()
            in_reasons = False

        elif s.upper().startswith("FREE_OBSERVATION:"):
            free_observation = s[len("FREE_OBSERVATION:"):].strip()
            in_reasons = False

        elif u.startswith("REASONS:") or u.startswith("REASON:"):
            in_reasons = True

        elif in_reasons and re.match(r"^\d+\.", s):
            # Numbered reason line — strip the leading "N." and keep clean text
            body = re.sub(r"^\d+\.\s*", "", s)
            numbered.append(body)

        elif in_reasons and s and not numbered:
            # Plain text right after REASONS: header (no numbering)
            numbered.append(s)

    # Fallback signal detection
    if signal == "UNKNOWN":
        tu = text.upper()
        if "POSITION: ABOVE" in tu:  signal = "UP"
        elif "POSITION: BELOW" in tu: signal = "DOWN"

    # Join numbered reasons with newlines for frontend list rendering
    reasoning = "\n".join(numbered).strip()[:1400]
    if not reasoning:
        # Last resort: grab everything after REASON(S): as plain text
        m = re.search(r"REASONS?:\s*(.*)", text, re.IGNORECASE | re.DOTALL)
        if m:
            reasoning = m.group(1).strip()[:1400]

    return signal, confidence, reasoning, data_received, data_requests, narrative, free_observation
