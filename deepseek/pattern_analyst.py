"""
Pattern Analyst Specialist
===========================
Fires a dedicated DeepSeek call at each 5-minute bar open.

Receives:
  - Last 25 resolved prediction windows (indicator values + strategy votes + WIN/LOSS)
  - The current window's live indicator values and strategy signals

Asks DeepSeek to:
  1. Find the 3-5 historical windows most similar to the current setup
  2. Report what happened in those cases
  3. Identify which indicator/signal combinations reliably predicted wins vs losses
  4. Give a directional lean based purely on pattern matching

Prompt is loaded from:
  specialists/pattern_analyst/PROMPT.md  ← edit this to change analyst behaviour

After each call, the following are written (overwrite):
  specialists/pattern_analyst/last_sent.txt      — history table + current state sent
  specialists/pattern_analyst/last_prompt.txt    — full assembled prompt
  specialists/pattern_analyst/last_response.txt  — raw DeepSeek response
  specialists/pattern_analyst/suggestions.txt    — appended suggestion from the model

The text output is injected into the main DeepSeek prompt as an additional
specialist insight section.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-chat"
CALL_TIMEOUT_S   = 25.0
MAX_TOKENS       = 700

logger = logging.getLogger(__name__)

# Paths relative to project root
_ROOT          = Path(__file__).parent.parent
_SPEC_DIR      = _ROOT / "specialists" / "pattern_analyst"
_PROMPT_FILE   = _SPEC_DIR / "PROMPT.md"
_SENT_FILE     = _SPEC_DIR / "last_sent.txt"
_PROMPT_OUT    = _SPEC_DIR / "last_prompt.txt"
_RESPONSE_FILE = _SPEC_DIR / "last_response.txt"
_SUGGEST_FILE  = _SPEC_DIR / "suggestions.txt"

# Indicators to extract from the snapshot
_IND_KEYS = [
    ("rsi_14",          "RSI"),
    ("mfi_14",          "MFI"),
    ("macd_histogram",  "MACD_H"),
    ("stoch_k_14",      "STOCH"),
    ("bollinger_pct_b", "BB_B"),
    ("volume_surge",    "VSURGE"),
    ("price_vs_vwap",   "VWAP%"),
    ("obv_slope",       "OBV"),
    ("trend_r_squared", "TREND_R2"),
]

# Strategy signals to show in the history table
_VOTE_KEYS = [
    "dow_theory", "alligator", "fib_pullback", "acc_dist", "harmonic",
    "rsi", "macd", "polymarket",
]


# ─────────────────────────────────────────────────────────────
# Prompt loader
# ─────────────────────────────────────────────────────────────

def _load_prompt_template() -> str:
    try:
        return _PROMPT_FILE.read_text(encoding="utf-8")
    except Exception as exc:
        logger.error("Pattern analyst: could not load PROMPT.md: %s", exc)
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
# Formatters
# ─────────────────────────────────────────────────────────────

def _fmt_indicators(ind: Dict) -> str:
    parts = []
    for key, label in _IND_KEYS:
        v = ind.get(key)
        if v is not None:
            try:
                parts.append(f"{label}={float(v):.1f}")
            except (TypeError, ValueError):
                pass
    return " ".join(parts) if parts else "no_data"


def _fmt_votes(votes: Dict) -> str:
    parts = []
    for key in _VOTE_KEYS:
        v = votes.get(key)
        if v:
            sig  = "↑" if v.get("signal") == "UP" else "↓"
            conf = int((v.get("confidence") or 0.5) * 100)
            parts.append(f"{key[:3].upper()}={sig}{conf}%")
    return " ".join(parts) if parts else "no_data"


_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_SESSIONS = [
    (0,  8,  "ASIA   "),
    (8,  13, "LONDON "),
    (13, 16, "OVERLAP"),
    (16, 21, "NY     "),
    (21, 24, "LATE   "),
]

def _session(hour_utc: int) -> str:
    for start, end, label in _SESSIONS:
        if start <= hour_utc < end:
            return label
    return "LATE   "


def _fmt_timestamp(ts: float) -> str:
    if not ts:
        return "?"
    dt  = datetime.fromtimestamp(ts, tz=timezone.utc)
    day = _DAYS[dt.weekday()]
    ses = _session(dt.hour)
    return f"{day} {dt.strftime('%Y-%m-%d %H:%M:%S')} UTC  {ses}"


def _build_history_table(records: List[Dict]) -> str:
    if not records:
        return "  (no resolved history yet)"
    lines = []
    for i, rec in enumerate(records, 1):
        direction = rec["actual_direction"]
        ts_str    = _fmt_timestamp(rec.get("window_start"))
        ind_str   = _fmt_indicators(rec.get("indicators", {}))
        vote_str  = _fmt_votes(rec.get("strategy_votes", {}))
        price     = rec.get("start_price")
        price_str = f"${price:,.0f}" if price else "?"
        lines.append(
            f"  #{i:02d} {direction:<4} {price_str:<10} {ts_str} | {ind_str} | {vote_str}"
        )
    return "\n".join(lines)


def _build_current_state(
    current_indicators: Dict,
    current_strategy_votes: Dict,
    current_ts: float = 0.0,
) -> str:
    ts_str   = _fmt_timestamp(current_ts or time.time())
    ind_str  = _fmt_indicators(current_indicators)
    vote_str = _fmt_votes(current_strategy_votes)
    return f"  {ts_str}\n  {ind_str} | {vote_str}"


# ─────────────────────────────────────────────────────────────
# API call
# ─────────────────────────────────────────────────────────────

async def _call_api(api_key: str, prompt: str) -> str:
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
# Public runner
# ─────────────────────────────────────────────────────────────

def _fmt_dashboard_accuracy(dashboard_accuracy: Optional[Dict]) -> str:
    """Format dashboard microstructure accuracy scores for inclusion in the pattern prompt."""
    if not dashboard_accuracy:
        return "  (no microstructure accuracy data yet)"
    _LABELS = {
        "order_book":   "OrderBook",
        "long_short":   "L/S-Contrarian",
        "taker_flow":   "TakerFlow",
        "oi_funding":   "OI+Funding",
        "liquidations": "Liquidations",
        "fear_greed":   "Fear&Greed",
        "mempool":      "Mempool",
        "coinalyze":    "Coinalyze",
        "coingecko":    "CoinGecko",
    }
    parts = []
    for key, label in _LABELS.items():
        stats = dashboard_accuracy.get(key)
        if not stats or stats.get("total", 0) < 5:
            continue
        acc = stats["accuracy"] * 100
        cor = stats["correct"]
        tot = stats["total"]
        parts.append(f"  {label:<18} {acc:.0f}%  ({cor}/{tot})")
    return "\n".join(parts) if parts else "  (fewer than 5 resolved bars per indicator)"


async def run_pattern_analyst(
    api_key: str,
    history_records: List[Dict],
    current_indicators: Dict,
    current_strategy_votes: Dict,
    window_start_time: float = 0.0,
    dashboard_accuracy: Optional[Dict] = None,
) -> Optional[str]:
    """
    Fire the pattern analyst call.

    Prompt loaded from specialists/pattern_analyst/PROMPT.md (editable).
    Input/output saved for review after every call.

    Returns the analysis text (injected into the main DeepSeek prompt), or None on failure.
    """
    if not history_records:
        logger.info("Pattern analyst: no history yet — skipping (need at least 1 resolved window)")
        return None

    template = _load_prompt_template()
    if not template:
        logger.error("Pattern analyst: empty prompt template — skipping call")
        return None

    t0            = time.time()
    history_table = _build_history_table(history_records)
    current_state = _build_current_state(current_indicators, current_strategy_votes, window_start_time)
    dash_acc_str  = _fmt_dashboard_accuracy(dashboard_accuracy)

    # Build microstructure accuracy section to append to the prompt
    micro_section = (
        f"\n\n=== MICROSTRUCTURE INDICATOR ACCURACY (historical UP/DOWN hit rate) ===\n"
        f"{dash_acc_str}\n"
        f"Use this when weighting microstructure signals found in the history rows above.\n"
    )

    prompt = template.format(
        n=len(history_records),
        history_table=history_table,
        current_state=current_state,
    ) + micro_section

    ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

    # Save what we're sending
    sent_content = (
        f"# Sent at {ts_str}\n"
        f"# History rows: {len(history_records)}\n\n"
        f"=== HISTORY TABLE ===\n{history_table}\n\n"
        f"=== CURRENT STATE ===\n{current_state}\n\n"
        f"=== MICROSTRUCTURE ACCURACY ===\n{dash_acc_str}"
    )
    _save(_SENT_FILE,  sent_content)
    _save(_PROMPT_OUT, f"# Sent at {ts_str}\n\n{prompt}")

    try:
        raw = await _call_api(api_key, prompt)
        elapsed = time.time() - t0

        # Save response
        _save(_RESPONSE_FILE,
              f"# Received at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n{raw}")

        # Extract and log suggestion line if present
        for line in raw.splitlines():
            if line.strip().upper().startswith("SUGGESTION:"):
                suggestion = line.partition(":")[2].strip()
                if suggestion and suggestion.upper() != "NONE":
                    _append(_SUGGEST_FILE, f"[{ts_str}] {suggestion}")
                    logger.info("Pattern analyst suggestion: %s", suggestion)
                break

        logger.info("Pattern analyst complete in %.1fs (%d chars)", elapsed, len(raw))
        return raw.strip()

    except Exception as exc:
        _save(_RESPONSE_FILE,
              f"# ERROR at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n{exc}")
        logger.warning("Pattern analyst failed: %s", exc)
        return None
