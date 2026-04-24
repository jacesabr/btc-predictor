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
  "edge": "1-2 plain-English sentences describing what the setup IS. NEVER just echo the signal direction. Speak to a human trader making a decision in the next few minutes.",
  "watch": [{"tone": "bullish|bearish|neutral", "text": "a condition, level, or bar-level event that would confirm or invalidate the setup"}],
  "actions": [{"tone": "bullish|bearish|neutral", "text": "concrete IF-THEN guidance — 'if price breaks X do Y', 'stand aside unless Z'. NEVER a bare 'buy' or 'sell'."}]
}

HARD RULES:
- Restate only facts in the INPUT. Never invent levels, numbers, bars, or directional calls.
- If the source is NEUTRAL or has no setup, say so plainly in edge — do not manufacture one.
- Include numbers only when they carry trading meaning (price levels, bar IDs, ranges, time to close). Drop confidence percentages, latencies, and data-source labels.
- Tag each bullet by which SIDE of the trade it implies (bullish, bearish, or neutral).
- Each bullet <= 2 sentences. No hedging, no meta-commentary, no "the model says".
- watch: max 4 bullets. actions: max 3 bullets.
"""


def _truncate(s: Any, n: int) -> str:
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + " …"


def _build_user_prompt(pred: dict, historical: str, binance_expert: dict) -> str:
    """Assemble the INPUT block from the main-page fields."""
    parts = ["INPUT:"]
    parts.append(f"signal: {pred.get('signal', '?')}")
    parts.append(f"confidence: {pred.get('confidence', '?')}")

    reasoning = pred.get("reasoning") or ""
    if reasoning:
        parts.append(f"reasoning:\n{_truncate(reasoning, 3000)}")

    narrative = pred.get("narrative") or ""
    if narrative:
        parts.append(f"narrative: {_truncate(narrative, 800)}")

    free_obs = pred.get("free_observation") or ""
    if free_obs:
        parts.append(f"free_observation: {_truncate(free_obs, 600)}")

    if historical:
        parts.append(f"historical_pattern:\n{_truncate(historical, 2000)}")

    if binance_expert:
        # bar_binance_expert is a specialist dict; pull whatever text field is there.
        notes = (
            binance_expert.get("narrative")
            or binance_expert.get("analysis")
            or binance_expert.get("reasoning")
            or ""
        )
        sig = binance_expert.get("signal")
        if notes or sig:
            parts.append(f"binance_expert: signal={sig or '?'} notes={_truncate(notes, 1500)}")

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
            out.append({"tone": tone, "text": text.strip()})
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
