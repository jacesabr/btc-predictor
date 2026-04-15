"""
Bar Insight Analyst Specialist
================================
Fires at each 5-minute bar open with access to the COMPLETE resolved history.

Unlike the pattern_analyst (which only sees indicators + strategy votes),
this specialist receives every field from every resolved bar:
  - ensemble signal + confidence + correct
  - DeepSeek signal + confidence + reasoning + correct
  - all 5 unified specialist signals (DOW, FIB, ALG, ACD, HAR)
  - creative edge observation
  - pattern analyst directional lean
  - all technical indicators
  - session / day-of-week context

It looks for second-order patterns: when do the specialists agree vs disagree,
when does agreement predict accuracy, what creative edges actually worked, etc.

Prompt is loaded from:
  specialists/bar_insight_analyst/PROMPT.md  ← edit to change analysis focus

After each call, writes (overwrite):
  specialists/bar_insight_analyst/last_sent.txt
  specialists/bar_insight_analyst/last_prompt.txt
  specialists/bar_insight_analyst/last_response.txt
  specialists/bar_insight_analyst/suggestions.txt   (appended)
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-chat"
CALL_TIMEOUT_S   = 25.0
MAX_TOKENS       = 800

logger = logging.getLogger(__name__)

_ROOT          = Path(__file__).parent.parent
_SPEC_DIR      = _ROOT / "specialists" / "bar_insight_analyst"
_PROMPT_FILE   = _SPEC_DIR / "PROMPT.md"
_SENT_FILE     = _SPEC_DIR / "last_sent.txt"
_PROMPT_OUT    = _SPEC_DIR / "last_prompt.txt"
_RESPONSE_FILE = _SPEC_DIR / "last_response.txt"
_SUGGEST_FILE  = _SPEC_DIR / "suggestions.txt"

_DAYS     = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_SESSIONS = [(0,8,"ASIA"),(8,13,"LONDON"),(13,16,"OVERLAP"),(16,21,"NY"),(21,24,"LATE")]


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _load_prompt_template() -> str:
    try:
        return _PROMPT_FILE.read_text(encoding="utf-8")
    except Exception as exc:
        logger.error("Bar insight analyst: could not load PROMPT.md: %s", exc)
        return ""


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


def _session(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    for start, end, label in _SESSIONS:
        if start <= dt.hour < end:
            return label
    return "LATE"


def _fmt_indicators(ind: Dict) -> str:
    keys = [("rsi_14","RSI"),("mfi_14","MFI"),("macd_histogram","MACD"),
            ("stoch_k_14","STOCH"),("bollinger_pct_b","BB")]
    parts = []
    for k, label in keys:
        v = ind.get(k)
        if v is not None:
            try:
                parts.append(f"{label}={float(v):.1f}")
            except Exception:
                pass
    return " ".join(parts) if parts else "?"


def _fmt_specialists(sp: Dict) -> str:
    """Compact specialist signal string: DOW=U67 FIB=D55 etc."""
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


def _fmt_pattern_lean(text: str) -> str:
    """Extract the directional lean from pattern analyst text (last ~60 chars of first UP/DOWN mention)."""
    if not text:
        return "?"
    upper = text.upper()
    for token in ("UP", "DOWN", "NO EDGE", "NEUTRAL"):
        if token in upper:
            return token
    return "?"


def _build_history_table(records: List[Dict]) -> str:
    if not records:
        return "  (no resolved history yet)"
    lines = []
    for i, r in enumerate(records, 1):
        ts      = r.get("window_start", 0)
        dt      = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
        time_s  = dt.strftime("%a %H:%M") if dt else "?"
        ses     = r.get("session", _session(ts) if ts else "?")
        actual  = r.get("actual_direction", "?")
        sp      = r.get("start_price", 0)
        ep      = r.get("end_price", 0)
        price_s = f"${sp:,.0f}→${ep:,.0f}" if sp and ep else "?"
        # Ensemble
        e_sig   = r.get("ensemble_signal", "?")
        e_conf  = int((r.get("ensemble_conf") or 0) * 100)
        e_ok    = "✓" if r.get("ensemble_correct") else "✗"
        ens_s   = f"{e_sig[0]}{e_conf}{e_ok}"
        # DeepSeek
        d_sig   = r.get("deepseek_signal", "")
        d_conf  = r.get("deepseek_conf", 0)
        d_ok    = ("✓" if r.get("deepseek_correct") else
                   "✗" if r.get("deepseek_correct") is False else "?")
        ds_s    = f"{d_sig[0] if d_sig else '?'}{d_conf}{d_ok}"
        # Indicators
        ind_s   = _fmt_indicators(r.get("indicators", {}))
        # Specialists
        sp_s    = _fmt_specialists(r.get("specialist_signals", {}))
        # Creative edge (truncated)
        ce      = (r.get("creative_edge") or "").strip()
        ce_s    = ce[:40].replace("\n", " ") if ce else "none"
        # Pattern lean
        pa_lean = _fmt_pattern_lean(r.get("pattern_analysis", ""))

        lines.append(
            f"  #{i:03d}|{actual}|{price_s}|{ses} {time_s}"
            f"|ENS={ens_s} DS={ds_s}"
            f"|{ind_s}"
            f"|{sp_s}"
            f"|CE:{ce_s}"
            f"|PA:{pa_lean}"
        )
    return "\n".join(lines)


def _build_current_bar(
    current_indicators:    Dict,
    current_strategy_votes: Dict,
    window_start_time:     float,
    ensemble_signal:       str  = "",
    ensemble_conf:         float = 0.0,
    specialist_signals:    Dict = None,
    creative_edge:         str  = "",
    pattern_analysis:      str  = "",
) -> str:
    dt    = datetime.fromtimestamp(window_start_time, tz=timezone.utc) if window_start_time else None
    time_s = dt.strftime("%a %Y-%m-%d %H:%M UTC") if dt else "?"
    ses   = _session(window_start_time) if window_start_time else "?"
    ind_s = _fmt_indicators(current_indicators)
    sp_s  = _fmt_specialists(specialist_signals or {})
    ce_s  = (creative_edge or "none").strip()[:80]
    pa_s  = _fmt_pattern_lean(pattern_analysis)
    e_sig = ensemble_signal or "?"
    e_conf = int(ensemble_conf * 100)

    return (
        f"  Time: {time_s}  Session: {ses}\n"
        f"  Indicators: {ind_s}\n"
        f"  Specialists: {sp_s}\n"
        f"  Ensemble: {e_sig} {e_conf}%\n"
        f"  Creative edge: {ce_s}\n"
        f"  Pattern analyst lean: {pa_s}"
    )


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

async def run_bar_insight_analyst(
    api_key:               str,
    history_records:       List[Dict],
    current_indicators:    Dict,
    current_strategy_votes: Dict,
    window_start_time:     float = 0.0,
    ensemble_signal:       str   = "",
    ensemble_conf:         float = 0.0,
    specialist_signals:    Dict  = None,
    creative_edge:         str   = "",
    pattern_analysis:      str   = "",
) -> "tuple[Optional[str], str]":
    """
    Fire the bar insight analyst with the complete bar history.

    Requires at least 5 resolved bars to produce meaningful analysis.
    Returns (analysis_text, call_signal) where call_signal is "UP", "DOWN", or "".
    Returns (None, "") if skipped or on error.
    """
    if not history_records or len(history_records) < 5:
        logger.info("Bar insight analyst: need 5+ resolved bars (have %d) — skipping",
                    len(history_records) if history_records else 0)
        return None, ""

    template = _load_prompt_template()
    if not template:
        logger.error("Bar insight analyst: empty PROMPT.md — skipping")
        return None, ""

    t0            = time.time()
    history_table = _build_history_table(history_records)
    current_bar   = _build_current_bar(
        current_indicators, current_strategy_votes, window_start_time,
        ensemble_signal, ensemble_conf, specialist_signals, creative_edge, pattern_analysis,
    )

    prompt = template.format(
        n=len(history_records),
        history_table=history_table,
        current_bar=current_bar,
    )

    ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

    sent_content = (
        f"# Sent at {ts_str}\n"
        f"# History rows: {len(history_records)}\n\n"
        f"=== HISTORY TABLE ===\n{history_table}\n\n"
        f"=== CURRENT BAR ===\n{current_bar}"
    )
    _save(_SENT_FILE,  sent_content)
    _save(_PROMPT_OUT, f"# Sent at {ts_str}\n\n{prompt}")

    try:
        raw     = await _call_api(api_key, prompt)
        elapsed = time.time() - t0

        _save(_RESPONSE_FILE,
              f"# Received at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n{raw}")

        # Extract CALL and SUGGESTION from structured output lines
        call_signal = ""
        for line in raw.splitlines():
            upper = line.strip().upper()
            if upper.startswith("CALL:") and not call_signal:
                val = line.partition(":")[2].strip().upper()
                if "UP" in val:
                    call_signal = "UP"
                elif "DOWN" in val:
                    call_signal = "DOWN"
                # NONE / empty → leave as ""
            if upper.startswith("SUGGESTION:"):
                suggestion = line.partition(":")[2].strip()
                if suggestion and suggestion.upper() != "NONE":
                    _append(_SUGGEST_FILE, f"[{ts_str}] {suggestion}")
                    logger.info("Bar insight analyst suggestion: %s", suggestion)

        logger.info("Bar insight analyst complete in %.1fs (%d chars) call=%s",
                    elapsed, len(raw), call_signal or "NONE")
        return raw.strip(), call_signal

    except Exception as exc:
        _save(_RESPONSE_FILE,
              f"# ERROR at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n\n{exc}")
        logger.warning("Bar insight analyst failed: %s", exc)
        return None, ""
