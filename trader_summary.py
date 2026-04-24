"""
Trader-friendly summarizer layer over the main page output.

Compresses DeepSeek reasoning + historical pattern lean + Binance expert notes
into a tight briefing a trader can digest in ~30 seconds, emitted as structured
JSON so the frontend can render with per-bullet green/red tone coloring.

Design constraints (from user):
  * Non-persistent — never written to Postgres. Purely a live ease-of-use layer.
  * One Venice call per bar, not per WebSocket tick: cached by window_start_time.
  * Never blocks the UI. Any failure (HTTP, JSON parse, validation) returns None
    and the frontend falls back to rendering the raw blocks below.
"""
import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional

import aiohttp


logger = logging.getLogger(__name__)

VENICE_URL = "https://api.venice.ai/api/v1/chat/completions"

# In-memory only. Keys are window_start_time (unix-seconds float). Eviction keeps
# the last ~1 hour of bars so reconnecting clients can still see recent summaries.
_cache: Dict[float, dict] = {}
_locks: Dict[float, asyncio.Lock] = {}
_CACHE_MAX = 12


SYSTEM_PROMPT = """You compress a BTC 5-minute prediction analysis into a trader briefing a reader can digest in 30 seconds.

OUTPUT STRICT JSON ONLY, exactly this shape:
{
  "edge": "One punchy sentence — what is the dominant setup RIGHT NOW? Max 25 words. Lead with the strongest directional read. Do NOT cram every signal. If data is missing, say so here. If signal is NEUTRAL with no edge, say that plainly.",
  "watch": [{
    "tone": "bullish|bearish|neutral",
    "text": "ONE complete sentence (not a phrase fragment). State what to watch and WHY it matters. Reference the specific signal by name. Example: 'If taker buy volume surges above 50 BTC, the zero-flow regime breaks and bulls regain control.' NOT 'taker flow'. Max 22 words.",
    "conditions": [{"metric": "<name>", "op": ">"|">="|"<"|"<="|"==", "value": <number>, "unit": "<unit>"}],
    "if_met": "short phrase (<=12 words) stating the DIRECT consequence the INPUT text supports. Omit if text already says it."
  }],
  "actions": [{
    "tone": "bullish|bearish|neutral",
    "text": "ONE complete IF-THEN sentence. 'If price breaks X with Y confirmation, enter long with stop Z.' Never a bare phrase. Max 25 words.",
    "conditions": [same shape as watch],
    "if_met": "the trader's concrete action when conditions fire (<=12 words)."
  }]
}

HARD RULES:
- Restate only facts in the INPUT. Never invent levels, numbers, bars, or directional calls.
- If the source is NEUTRAL or has no setup, say so plainly in edge — do not manufacture one.
- Include numbers only when they carry trading meaning (price levels, bar IDs, ranges, time to close). Drop confidence percentages, latencies, and data-source labels.
- TONE = the trade direction that benefits when ALL conditions fire:
    * "bullish"  — firing favors a LONG trade (price expected to move UP)
    * "bearish"  — firing favors a SHORT trade (price expected to move DOWN)
    * "neutral"  — firing suggests HOLD / stand aside (no directional edge)
  Judge tone by reading the if_met consequence or the text's outcome clause, NOT the
  trigger signal. Examples:
    * "If bid imbalance collapses below X, downside acceleration likely" → BEARISH
      (the fire condition is bid-support weakening; outcome is price drop).
    * "If taker buy volume spikes above X, breakout confirmed" → BULLISH.
    * "If taker volume stays below X, regime continues, no edge" → NEUTRAL.
  A bullish-sounding trigger can have a bearish outcome (e.g. "if buyers lose control
  at X, reversal down"). Always read the consequence to decide tone.
- **BULLET TEXT MUST BE A COMPLETE SENTENCE.** Never emit a bullet whose text is just the signal name ("taker flow", "spot whale buy"). If you have something to say about a signal, write a full sentence. If you don't, OMIT the bullet entirely. Empty conditions does NOT mean empty text — the text carries the trader-usable info either way.
- Each bullet = ONE sentence. Total briefing <=120 words across edge + all bullets. Scannable in 30s.
- watch: max 3 bullets. actions: max 3 bullets. Less is more.
- No hedging, no meta-commentary, no "the model says".

SCOPE-MATCHING (CRITICAL):
- Every metric in the INPUT is scope-tagged (e.g. "Binance-perp 5m", "aggregate 5-venue
  0.5% band", "daily macro"). Do NOT compare metrics across incompatible scopes in the
  same bullet. Examples of INVALID comparisons to avoid:
    * Aggregate book depth (hundreds of BTC, multi-venue) vs single-venue 5m taker flow
    * Single-exchange OI vs cross-exchange liquidations
    * Daily F&G/SOPR/MVRV as if they were bar-level triggers
- When referencing a number from INPUT, preserve its scope in your text. A "bid wall"
  must cite the depth band it was measured in ("within 0.5% of mid, across N venues").
- Bar-level conditions in `watch`/`actions` MUST use metrics that actually change
  bar-to-bar: price, price_change_pct, taker_buy_volume, taker_sell_volume, taker_ratio,
  bid_imbalance, ask_imbalance, funding_rate, open_interest, rsi, long_short_ratio,
  basis_pct, cvd_1h, rr_25d_30d. Metrics flagged as "MACRO CONTEXT" or "daily" in the
  INPUT may only appear in `edge` as background, never in `conditions`.

CONDITIONS — machine-checkable thresholds that back the bullet:
- PRESERVE ALL DEEPSEEK INTELLIGENCE. If the bullet text cites a specific threshold, emit
  a condition for it even if the metric is niche or not in the common whitelist. The UI
  will say "source unavailable" for metrics it can't verify live — that's better than
  dropping the claim entirely (which loses the edge DeepSeek worked to derive).
- "metric": use a clear snake_case name matching the signal the text references. Prefer
  these common names when they fit: price, price_change_pct, taker_buy_volume,
  taker_sell_volume, taker_volume, taker_ratio, bid_imbalance, ask_imbalance,
  funding_rate, open_interest, rsi, long_short_ratio, basis_pct, perp_cvd_1h,
  spot_cvd_1h, aggregate_cvd_1h, bid_depth_05pct, ask_depth_05pct, rr_25d_30d,
  iv_30d_atm, spot_whale_buy_btc, spot_whale_sell_btc, aggregate_funding_rate,
  aggregate_liquidations_usd, oi_velocity_pct. If the input uses a different signal,
  use a sensible snake_case name (e.g. "coinbase_whale_buy_btc") — UI will mark it
  "source unavailable" and the trader can still see the threshold.
- "op" must be one of: ">", ">=", "<", "<=", "==".
- "value" must be a plain number from the INPUT (do not fabricate). For "between X
  and Y", emit TWO conditions: op ">=" X and op "<=" Y.
- "unit": "USD" for price, "BTC" for volume, "%" for rates/ratios, "" otherwise.
- If the bullet is pure narrative with no cited number, omit conditions.
- CROSS-SIGNAL PROTECTION: do not emit a condition on taker_* when the signal the
  text describes is actually whale_flow (different data source). Pick the metric
  that matches what the text is actually about.

DATA AVAILABILITY HANDLING:
- Preserve every signal the INPUT provides. If a signal is flagged "unavailable" in
  the INPUT, still surface it in the bullet text so the trader sees the gap — e.g.
  "Taker flow unavailable from backend — monitor independently; bullish move invalid
  until confirmed." The UI will render the condition pill as "source unavailable" if
  no live feed can verify it.

NO JARGON WITHOUT EVIDENCE:
- Do NOT use technical-analysis terminology (Wyckoff, Elliott wave, harmonic patterns, distribution phase, accumulation phase, liquidity grab, stop hunt, market structure break, order block, fair value gap, etc.) unless the INPUT gives a concrete price level, bar index, or measured condition that backs it. A percentage alone is NOT evidence. A name alone is NOT evidence.
- If the INPUT contains such a term but only hand-waves it, DROP the term and describe the underlying observation in plain words (e.g., "price compressed for 3 bars" instead of "accumulation phase").
- Prefer plain English: "buyers stepped in at $95,150" over "demand zone held".
"""

# Whitelist of metrics the frontend can look up a live value for. If Venice
# emits a different name, validate will drop the condition rather than let the
# UI show a meaningless pill.
_VALID_METRICS = {
    "price", "price_change_pct",
    "taker_buy_volume", "taker_sell_volume", "taker_volume",
    "taker_ratio", "bid_imbalance", "ask_imbalance",
    "funding_rate", "open_interest", "rsi", "long_short_ratio",
    # Phase 2.5 / 6.5 additions: bar-level metrics only
    "basis_pct", "perp_cvd_1h", "spot_cvd_1h", "aggregate_cvd_1h",
    "bid_depth_05pct", "ask_depth_05pct",
    "rr_25d_30d", "iv_30d_atm",
}
_VALID_OPS = {">", ">=", "<", "<=", "=="}


def _truncate(s: Any, n: int) -> str:
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + " …"


def _build_user_prompt(pred: dict, historical: str, binance_expert: dict) -> str:
    """Assemble the INPUT block from the main-page fields."""
    parts = ["INPUT:"]
    signal = (pred.get("signal") or "?").upper()
    parts.append(f"signal: {signal}")
    parts.append(f"confidence: {pred.get('confidence', '?')}")

    # Data availability flags — surface to Venice so it can warn the trader when
    # critical signals are missing rather than silently filling with assumptions.
    data_received = pred.get("data_received") or ""
    data_requests = pred.get("data_requests") or ""
    if data_received:
        parts.append(f"data_received: {_truncate(data_received, 400)}")
    if data_requests and data_requests.upper() != "NONE":
        parts.append(f"data_requests (gaps flagged by DeepSeek): {_truncate(data_requests, 400)}")

    # For NEUTRAL bars the abstention rationale is often long and nuanced —
    # truncating it kills the case for not-trading and forces Venice to invent
    # an "edge" story where there isn't one. Skip truncation for NEUTRAL.
    is_neutral = signal == "NEUTRAL"

    reasoning = pred.get("reasoning") or ""
    if reasoning:
        r = reasoning if is_neutral else _truncate(reasoning, 3000)
        parts.append(f"reasoning:\n{r}")

    narrative = pred.get("narrative") or ""
    if narrative:
        n = narrative if is_neutral else _truncate(narrative, 800)
        parts.append(f"narrative: {n}")

    free_obs = pred.get("free_observation") or ""
    if free_obs:
        f = free_obs if is_neutral else _truncate(free_obs, 600)
        parts.append(f"free_observation: {f}")

    if historical:
        h = historical if is_neutral else _truncate(historical, 2000)
        parts.append(f"historical_pattern:\n{h}")

    if binance_expert:
        notes = (
            binance_expert.get("narrative")
            or binance_expert.get("analysis")
            or binance_expert.get("reasoning")
            or ""
        )
        sig = binance_expert.get("signal")
        if notes or sig:
            n = notes if is_neutral else _truncate(notes, 1500)
            parts.append(f"binance_expert: signal={sig or '?'} notes={n}")

    return "\n\n".join(parts)


async def _call_venice(
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    timeout_s: float = 25.0,
) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model":       model,
        "messages":    [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        "max_tokens":      900,
        "temperature":     0.2,
        "response_format": {"type": "json_object"},
    }
    timeout   = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(VENICE_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Venice HTTP {resp.status}: {body[:400]}")
            data = json.loads(body)
            return data["choices"][0]["message"]["content"]


import re as _re
_METRIC_NAME_OK = _re.compile(r"^[a-z][a-z0-9_]{1,60}$")

def _norm_conditions(raw: Any) -> list:
    """Validate + normalize a bullet's conditions array. Drops anything malformed,
    but accepts ANY snake_case metric name (not only the common whitelist) so
    DeepSeek's intelligence is preserved even when a signal has no frontend live
    feed — the UI will just render "source unavailable" for unknown metrics."""
    if not isinstance(raw, list):
        return []
    out = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        metric = c.get("metric")
        op     = c.get("op")
        val    = c.get("value")
        unit   = c.get("unit", "")
        if not isinstance(metric, str) or not _METRIC_NAME_OK.match(metric):
            continue
        if op not in _VALID_OPS:
            continue
        try:
            val = float(val)
        except (TypeError, ValueError):
            continue
        out.append({
            "metric": metric, "op": op, "value": val,
            "unit":   str(unit or "")[:16],
        })
        if len(out) >= 4:
            break
    return out


def _validate(obj: Any) -> Optional[dict]:
    """Return a normalized summary dict or None if shape is wrong."""
    if not isinstance(obj, dict):
        return None
    edge = obj.get("edge")
    if not isinstance(edge, str) or not edge.strip():
        return None

    def _norm_bullets(key: str, cap: int) -> list:
        arr = obj.get(key) or []
        if not isinstance(arr, list):
            return []
        out = []
        for b in arr:
            if not isinstance(b, dict):
                continue
            text = b.get("text")
            tone = b.get("tone", "neutral")
            if not isinstance(text, str) or not text.strip():
                continue
            if tone not in ("bullish", "bearish", "neutral"):
                tone = "neutral"
            if_met_raw = b.get("if_met")
            if_met = str(if_met_raw).strip()[:200] if if_met_raw else ""
            out.append({
                "tone":       tone,
                "text":       text.strip(),
                "conditions": _norm_conditions(b.get("conditions")),
                "if_met":     if_met or None,
            })
            if len(out) >= cap:
                break
        return out

    return {
        "edge":    edge.strip(),
        "watch":   _norm_bullets("watch",   4),
        "actions": _norm_bullets("actions", 3),
    }


def _evict_old() -> None:
    if len(_cache) <= _CACHE_MAX:
        return
    for k in sorted(_cache.keys())[: len(_cache) - _CACHE_MAX]:
        _cache.pop(k, None)
        _locks.pop(k, None)


async def get_or_build(
    window_start_time: float,
    pred: dict,
    historical: str,
    binance_expert: dict,
    api_key: str,
    model: str,
) -> Optional[dict]:
    """
    Return the cached summary for this bar, or build it once. Returns None on
    any failure so the caller (engine/server) can fall back to the raw blocks.
    """
    if not api_key or not window_start_time or not pred or pred.get("signal") in (None, "ERROR", "UNAVAILABLE"):
        return None

    cached = _cache.get(window_start_time)
    if cached:
        return cached

    lock = _locks.setdefault(window_start_time, asyncio.Lock())
    async with lock:
        cached = _cache.get(window_start_time)
        if cached:
            return cached

        started = time.time()
        try:
            user_prompt = _build_user_prompt(pred, historical, binance_expert)
            raw = await _call_venice(api_key, model, SYSTEM_PROMPT, user_prompt)
        except Exception as exc:
            logger.warning("trader_summary Venice call FAILED for bar %s: %s", window_start_time, exc)
            return None

        try:
            obj = json.loads(raw)
        except Exception as exc:
            logger.warning("trader_summary JSON parse FAILED for bar %s: %s (raw=%r)", window_start_time, exc, raw[:300])
            return None

        cleaned = _validate(obj)
        if not cleaned:
            logger.warning("trader_summary invalid shape for bar %s: %r", window_start_time, obj)
            return None

        cleaned["generated_at"]   = time.time()
        cleaned["generation_ms"]  = int((time.time() - started) * 1000)
        cleaned["model"]          = model
        cleaned["window_start"]   = window_start_time

        _cache[window_start_time] = cleaned
        _evict_old()
        logger.info(
            "trader_summary bar %s built in %dms (watch=%d actions=%d)",
            window_start_time, cleaned["generation_ms"], len(cleaned["watch"]), len(cleaned["actions"]),
        )
        return cleaned


def get_cached(window_start_time: Optional[float]) -> Optional[dict]:
    if window_start_time is None:
        return None
    return _cache.get(window_start_time)


def drop(window_start_time: Optional[float]) -> None:
    if window_start_time is None:
        return
    _cache.pop(window_start_time, None)
    _locks.pop(window_start_time, None)
