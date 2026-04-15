"""
DeepSeek AI Predictor
=====================
Sends all indicator data to DeepSeek as text at the start of every 5-minute window
and records whether the prediction was correct.

Prompt format is defined in prompt_format.py.

After each call, the following are written (overwrite):
  specialists/main_predictor/last_prompt.txt    — the full prompt sent
  specialists/main_predictor/last_response.txt  — raw DeepSeek response
  specialists/main_predictor/suggestions.txt    — appended SUGGESTION lines from model
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp

from deepseek.prompt_format import build_prompt, parse_response

logger = logging.getLogger(__name__)

DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_MODEL   = "deepseek-chat"

# Paths for saving last prompt / response
_ROOT          = Path(__file__).parent.parent
_SPEC_DIR      = _ROOT / "specialists" / "main_predictor"
_PROMPT_OUT    = _SPEC_DIR / "last_prompt.txt"
_RESPONSE_FILE = _SPEC_DIR / "last_response.txt"
_SUGGEST_FILE  = _SPEC_DIR / "suggestions.txt"


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
# API call
# ─────────────────────────────────────────────────────────────

async def _call_api(api_key: str, prompt: str, model: str) -> str:
    """POST to DeepSeek API. Raises on non-200."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    payload = {
        "model":       model,
        "messages":    [{"role": "user", "content": prompt}],
        "max_tokens":  2200,
        "temperature": 0.1,
    }

    timeout   = aiohttp.ClientTimeout(total=45)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(DEEPSEEK_API_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {body[:400]}")
            data = await resp.json(content_type=None)
            return data["choices"][0]["message"]["content"]


# ─────────────────────────────────────────────────────────────
# Predictor class
# ─────────────────────────────────────────────────────────────

class DeepSeekPredictor:
    """
    Generates DeepSeek predictions at the start of every 5-minute window.

    Usage:
        result = await predictor.predict(prices, klines, features, strategy_preds, ...)
    """

    def __init__(self, api_key: str, model: str = DEEPSEEK_MODEL):
        self.api_key    = api_key
        self.model      = model
        self.window_count = 0

    async def predict(
        self,
        prices:               List[float],
        klines:               List,
        features:             Dict[str, float],
        strategy_preds:       Dict,
        recent_accuracy:      float,
        deepseek_accuracy:    Dict,
        window_start_time:    float,
        window_start_price:   float,
        polymarket_slug:      Optional[str]  = None,
        ensemble_result:      Optional[Dict] = None,
        dashboard_signals:    Optional[Dict] = None,
        indicator_accuracy:   Optional[Dict] = None,
        ensemble_weights:     Optional[Dict] = None,
        pattern_analysis:     Optional[str]  = None,
        creative_edge:        Optional[str]  = None,
        bar_insight:          Optional[str]  = None,
        dashboard_accuracy:   Optional[Dict] = None,
    ) -> Dict:
        """
        Run a full DeepSeek prediction cycle.

        Returns dict:
            signal         : "UP" | "DOWN" | "UNKNOWN" | "ERROR"
            confidence     : int (0-100)
            reasoning      : str  concise analysis from the model
            data_received  : str  DeepSeek confirmation of what data it analyzed
            data_requests  : str  additional data DeepSeek asked for (or "NONE")
            raw_response   : str  full API text
            full_prompt    : str  full prompt sent
            polymarket_url : str  direct Polymarket market link
            window_start   : str  human-readable window start timestamp
            window_end     : str  human-readable window end timestamp
            latency_ms     : int
            window_count   : int
        """
        self.window_count += 1
        t0 = time.time()

        # ── Build metadata strings for result ────────────────────────────────
        polymarket_url = (f"https://polymarket.com/event/{polymarket_slug}"
                         if polymarket_slug else "")
        window_start_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(window_start_time))
        window_end_str   = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(window_start_time + 300))

        # ── Build prompt ──────────────────────────────────────────────────────
        prompt = build_prompt(
            prices=prices,
            klines=klines,
            features=features,
            strategy_preds=strategy_preds,
            recent_accuracy=recent_accuracy,
            window_num=self.window_count,
            deepseek_accuracy=deepseek_accuracy,
            window_start_price=window_start_price,
            window_start_time=window_start_time,
            polymarket_slug=polymarket_slug,
            ensemble_result=ensemble_result,
            dashboard_signals=dashboard_signals,
            indicator_accuracy=indicator_accuracy,
            ensemble_weights=ensemble_weights,
            pattern_analysis=pattern_analysis,
            creative_edge=creative_edge,
            bar_insight=bar_insight,
            dashboard_accuracy=dashboard_accuracy,
        )
        full_prompt = prompt

        ts_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        _save(_PROMPT_OUT, f"# Sent at {ts_str}  (window #{self.window_count})\n\n{prompt}")

        # ── API call ──────────────────────────────────────────────────────────
        raw_response: Optional[str] = None
        error_msg   = ""

        try:
            raw_response = await _call_api(self.api_key, prompt, self.model)
        except Exception as exc:
            error_msg = str(exc)
            logger.error("DeepSeek call failed: %s", exc)
            _append(_RESPONSE_FILE,
                    f"\n{'='*60}\n# ERROR at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}\n{'='*60}\n\n{exc}")

        if raw_response is None:
            return {
                "signal":           "ERROR",
                "confidence":       0,
                "reasoning":        error_msg,
                "data_received":    "",
                "data_requests":    "",
                "narrative":        "",
                "free_observation": "",
                "raw_response":     "",
                "full_prompt":      full_prompt,
                "polymarket_url":   polymarket_url,
                "window_start":     window_start_str,
                "window_end":       window_end_str,
                "latency_ms":       int((time.time() - t0) * 1000),
                "completed_at":     time.time(),
                "window_count":     self.window_count,
            }

        # ── Parse response ────────────────────────────────────────────────────
        _append(_RESPONSE_FILE,
                f"\n{'='*60}\n# Received at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}  (window #{self.window_count})\n{'='*60}\n\n{raw_response}")

        signal, confidence, reasoning, data_received, data_requests, narrative, free_observation = parse_response(raw_response)
        latency_ms = int((time.time() - t0) * 1000)

        # Extract and log SUGGESTION if present
        for line in raw_response.splitlines():
            if line.strip().upper().startswith("SUGGESTION:"):
                suggestion = line.partition(":")[2].strip()
                if suggestion and suggestion.upper() != "NONE":
                    _append(_SUGGEST_FILE, f"[{ts_str}] {suggestion}")
                    logger.info("Main predictor suggestion: %s", suggestion)
                break

        logger.info(
            "DeepSeek #%d → %s  conf=%d%%  latency=%dms  data_req=%s",
            self.window_count, signal, confidence, latency_ms,
            data_requests or "none",
        )
        if data_requests and data_requests.upper() not in ("NONE", ""):
            logger.info("DeepSeek data request: %s", data_requests)
        if narrative:
            logger.info("DeepSeek narrative: %s", narrative[:120])

        return {
            "signal":           signal,
            "confidence":       confidence,
            "reasoning":        reasoning,
            "data_received":    data_received,
            "data_requests":    data_requests,
            "narrative":        narrative,
            "free_observation": free_observation,
            "raw_response":     raw_response,
            "full_prompt":      full_prompt,
            "polymarket_url":   polymarket_url,
            "window_start":     window_start_str,
            "window_end":       window_end_str,
            "latency_ms":       latency_ms,
            "completed_at":     time.time(),
            "window_count":     self.window_count,
        }
