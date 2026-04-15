"""
Unified Technical Specialist
==============================
ONE DeepSeek call that analyses the OHLCV data from five angles simultaneously,
then hunts for any creative edge beyond the standard frameworks.

Prompt is loaded from:
  specialists/unified_analyst/PROMPT.md  ← edit this to change what the specialist does

After each call, the following are written (overwrite):
  specialists/unified_analyst/last_sent.txt      — the exact OHLCV CSV sent
  specialists/unified_analyst/last_prompt.txt    — the full prompt (prompt + data)
  specialists/unified_analyst/last_response.txt  — raw DeepSeek response
  specialists/unified_analyst/suggestions.txt    — appended suggestion from the model

Returns:
    strategy_dict  — {name: {signal, confidence, reasoning, value}} for all 5 strategies
                     Keys: alligator | acc_dist | dow_theory | fib_pullback | harmonic
    creative_edge  — free-form string injected into the main DeepSeek prompt
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-chat"
CALL_TIMEOUT_S   = 25.0
MAX_TOKENS       = 1000

SPECIALIST_KEYS = {"alligator", "acc_dist", "dow_theory", "fib_pullback", "harmonic"}

logger = logging.getLogger(__name__)

# Paths relative to project root (btc-predictor/)
_ROOT      = Path(__file__).parent.parent
_SPEC_DIR  = _ROOT / "specialists" / "unified_analyst"
_PROMPT_FILE    = _SPEC_DIR / "PROMPT.md"
_SENT_FILE      = _SPEC_DIR / "last_sent.txt"
_PROMPT_OUT     = _SPEC_DIR / "last_prompt.txt"
_RESPONSE_FILE  = _SPEC_DIR / "last_response.txt"
_SUGGEST_FILE   = _SPEC_DIR / "suggestions.txt"


# ─────────────────────────────────────────────────────────────
# Prompt loader
# ─────────────────────────────────────────────────────────────

def _load_prompt_template() -> str:
    """Load prompt from PROMPT.md; fall back to empty string if missing."""
    try:
        return _PROMPT_FILE.read_text(encoding="utf-8")
    except Exception as exc:
        logger.error("Unified analyst: could not load PROMPT.md: %s", exc)
        return ""


# ─────────────────────────────────────────────────────────────
# I/O savers
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# OHLCV formatter
# ─────────────────────────────────────────────────────────────

def _ohlcv_csv(klines: List, n: int = 60) -> str:
    rows = ["Time(UTC),Open,High,Low,Close,Volume,QuoteVol,Trades,BuyVol%"]
    for k in klines[-n:]:
        try:
            ts      = time.strftime("%m-%d %H:%M", time.gmtime(int(k[0]) / 1000))
            vol     = float(k[5])
            quote_v = float(k[7]) if len(k) > 7 else 0.0
            trades  = int(k[8])   if len(k) > 8 else 0
            buy_vol = float(k[9]) if len(k) > 9 else 0.0
            buy_pct = round(buy_vol / vol * 100, 1) if vol > 0 else 0.0
            rows.append(
                f"{ts},{float(k[1]):.2f},{float(k[2]):.2f},"
                f"{float(k[3]):.2f},{float(k[4]):.2f},{vol:.1f},"
                f"{quote_v:.0f},{trades},{buy_pct}"
            )
        except Exception:
            pass
    return "\n".join(rows)


# ─────────────────────────────────────────────────────────────
# API call
# ─────────────────────────────────────────────────────────────

async def _call(api_key: str, prompt: str) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model":       DEEPSEEK_MODEL,
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  MAX_TOKENS,
        "temperature": 0.1,
    }
    timeout   = aiohttp.ClientTimeout(total=CALL_TIMEOUT_S)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(DEEPSEEK_API_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {body[:200]}")
            data = await resp.json(content_type=None)
            return data["choices"][0]["message"]["content"]


# ─────────────────────────────────────────────────────────────
# Response parser
# ─────────────────────────────────────────────────────────────

def _parse_response(text: str) -> Tuple[Dict[str, Dict], Optional[str], Optional[str]]:
    """
    Parse the unified specialist response.
    Returns ({strategy_name: {signal, confidence, reasoning, value}}, creative_edge, suggestion).
    """
    lines = text.strip().splitlines()
    raw: Dict[str, str] = {}
    for line in lines:
        if ":" in line:
            key, _, val = line.partition(":")
            raw[key.strip().upper()] = val.strip()

    def _sig(key: str) -> str:
        v = raw.get(key, "ABOVE").upper()
        return "UP" if "ABOVE" in v else "DOWN"

    def _conf(key: str) -> float:
        try:
            return max(0.45, min(0.95, float(raw.get(key, "55").replace("%", "").strip()) / 100))
        except ValueError:
            return 0.55

    strategies = {
        "dow_theory": {
            "signal":     _sig("DOW_POSITION"),
            "confidence": _conf("DOW_CONFIDENCE"),
            "reasoning":  raw.get("DOW_REASON", ""),
            "value":      raw.get("DOW_STRUCTURE", "")[:20],
            "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
        },
        "fib_pullback": {
            "signal":     _sig("FIB_POSITION"),
            "confidence": _conf("FIB_CONFIDENCE"),
            "reasoning":  raw.get("FIB_REASON", ""),
            "value":      raw.get("FIB_LEVEL", "")[:20],
            "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
        },
        "alligator": {
            "signal":     _sig("ALG_POSITION"),
            "confidence": _conf("ALG_CONFIDENCE"),
            "reasoning":  raw.get("ALG_REASON", ""),
            "value":      raw.get("ALG_STATE", "")[:20],
            "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
        },
        "acc_dist": {
            "signal":     _sig("ACD_POSITION"),
            "confidence": _conf("ACD_CONFIDENCE"),
            "reasoning":  raw.get("ACD_REASON", ""),
            "value":      raw.get("ACD_VALUE", "")[:20],
            "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
        },
        "harmonic": {
            "signal":     _sig("HAR_POSITION"),
            "confidence": _conf("HAR_CONFIDENCE"),
            "reasoning":  raw.get("HAR_REASON", ""),
            "value":      raw.get("HAR_PATTERN", "")[:20],
            "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None,
        },
    }

    creative_edge = raw.get("CREATIVE_EDGE", "").strip()
    if creative_edge.upper() == "NONE" or not creative_edge:
        creative_edge = None

    suggestion = raw.get("SUGGESTION", "").strip()
    if suggestion.upper() == "NONE" or not suggestion:
        suggestion = None

    return strategies, creative_edge, suggestion


# ─────────────────────────────────────────────────────────────
# Public runner
# ─────────────────────────────────────────────────────────────

async def run_specialists(
    api_key: str,
    klines:  List,
) -> Tuple[Dict[str, Optional[Dict]], Optional[str]]:
    """
    Fire ONE unified specialist call covering all 5 technical frameworks.

    Prompt loaded from specialists/unified_analyst/PROMPT.md (editable).
    Input/output saved for review after every call.

    Returns:
        (strategy_dict, creative_edge)
    """
    if not klines or len(klines) < 20:
        logger.warning("Specialists: not enough klines (%d) — skipping", len(klines) if klines else 0)
        return {}, None

    template = _load_prompt_template()
    if not template:
        logger.error("Unified analyst: empty prompt template — skipping call")
        return {}, None

    t0     = time.time()
    csv    = _ohlcv_csv(klines, 60)
    prompt = template.format(csv=csv)

    # Save what we're sending
    ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    _save(_SENT_FILE,   f"# Sent at {ts_str}\n\n{csv}")
    _save(_PROMPT_OUT,  f"# Sent at {ts_str}\n\n{prompt}")

    try:
        raw = await _call(api_key, prompt)

        # Append response (preserve history)
        _append(_RESPONSE_FILE, f"\n{'='*60}\n# Received at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n{'='*60}\n\n{raw}")

        strategies, creative_edge, suggestion = _parse_response(raw)

        # Append suggestion to log file
        if suggestion:
            _append(_SUGGEST_FILE,
                    f"[{ts_str}] {suggestion}")
            logger.info("Unified analyst suggestion: %s", suggestion)

        elapsed = time.time() - t0
        signals = {k: v["signal"] for k, v in strategies.items()}
        logger.info(
            "Unified specialist complete in %.1fs | signals: %s | creative_edge: %s",
            elapsed,
            " ".join(f"{k[:3]}={v}" for k, v in signals.items()),
            "YES" if creative_edge else "none",
        )
        return strategies, creative_edge

    except Exception as exc:
        _append(_RESPONSE_FILE, f"\n{'='*60}\n# ERROR at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n{'='*60}\n\n{exc}")
        logger.warning("Unified specialist failed: %s", exc)
        return {}, None
