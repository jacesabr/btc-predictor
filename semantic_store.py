"""
Semantic Pattern Store
======================
Appends the COMPLETE resolved bar record to results/pattern_history.ndjson.
Every specialist signal, indicator value, strategy vote, and final outcome is stored
for the historical similarity analyst — and for future semantic/vector search over
100,000+ bars.

The file grows indefinitely; never trimmed. Designed to be embedded later.

Schema per record:
  window_start, window_end, actual_direction, start_price, end_price
  session, day_of_week, hour_utc
  ensemble_signal, ensemble_conf, ensemble_correct
  deepseek_signal, deepseek_conf, deepseek_correct, deepseek_reasoning, deepseek_narrative
  specialist_signals: {dow_theory, fib_pullback, alligator, acc_dist, harmonic}
  historical_analysis
  strategy_votes: {name: {signal, confidence, reasoning, value}}
  indicators: {rsi_14, macd_histogram, bollinger_pct_b, ...}
  dashboard_signals_raw: {indicator_name: "UP" | "DOWN" | "NEUTRAL"}
  accuracy_snapshot: all main-page accuracy stats captured at bar close
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DATA_DIR   = Path(__file__).parent / "results"
_HIST_FILE  = _DATA_DIR / "pattern_history.ndjson"
_RESET_FILE = Path(__file__).parent / "score_reset.json"
_lock       = threading.Lock()


def _score_reset_at() -> float:
    """Return the Unix timestamp after which bars count toward scores. 0 = count all."""
    try:
        data = json.loads(_RESET_FILE.read_text(encoding="utf-8"))
        return float(data.get("reset_at", 0))
    except Exception:
        return 0.0

_DAYS     = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_SESSIONS = [
    (0,  8,  "ASIA"),
    (8,  13, "LONDON"),
    (13, 16, "OVERLAP"),
    (16, 21, "NY"),
    (21, 24, "LATE"),
]


def _session_label(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    for start, end, label in _SESSIONS:
        if start <= dt.hour < end:
            return label
    return "LATE"


# ── Write ─────────────────────────────────────────────────────────────────────

def append_resolved_window(
    window_start:           float,
    actual_direction:       str,
    start_price:            float,
    strategy_votes:         Dict,
    indicators:             Dict,
    window_end:             float                = 0.0,
    end_price:              float                = 0.0,
    ensemble_signal:        str                  = "",
    ensemble_conf:          float                = 0.0,
    ensemble_correct:       Optional[bool]       = None,
    deepseek_signal:        str                  = "",
    deepseek_conf:          int                  = 0,
    deepseek_correct:       Optional[bool]       = None,
    deepseek_reasoning:     str                  = "",
    deepseek_narrative:     str                  = "",
    deepseek_free_obs:      str                  = "",
    specialist_signals:     Optional[Dict]       = None,
    historical_analysis:    str                  = "",
    dashboard_signals_raw:  Optional[Dict]       = None,
    accuracy_snapshot:      Optional[Dict]       = None,
    full_prompt:            str                  = "",
    trade_action:           str                  = "",
    window_count:           int                  = 0,
    binance_expert_analysis = None,
):
    """Append one fully resolved bar to the history file. Thread-safe."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    record = {
        "window_start":       window_start,
        "window_end":         window_end or window_start + 300,
        "window_count":       window_count,
        "actual_direction":   actual_direction,
        "start_price":        start_price,
        "end_price":          end_price,
        "session":            _session_label(window_start),
        "day_of_week":        _DAYS[dt.weekday()],
        "hour_utc":           dt.hour,
        "ensemble_signal":    ensemble_signal,
        "ensemble_conf":      round(ensemble_conf, 4),
        "ensemble_correct":   ensemble_correct,
        "deepseek_signal":    deepseek_signal,
        "deepseek_conf":      deepseek_conf,
        "deepseek_correct":   deepseek_correct,
        "deepseek_reasoning": deepseek_reasoning,
        "deepseek_narrative": deepseek_narrative,
        "deepseek_free_obs":  deepseek_free_obs,
        "specialist_signals":  specialist_signals or {},
        "historical_analysis": historical_analysis,
        "binance_expert_analysis": binance_expert_analysis or {},
        "strategy_votes":     strategy_votes,
        "indicators":         indicators,
        "dashboard_signals_raw": dashboard_signals_raw or {},
        "accuracy_snapshot": accuracy_snapshot or {},
        "full_prompt":        full_prompt,
        "trade_action":       trade_action,
    }
    try:
        with _lock:
            with open(_HIST_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
        logger.debug("semantic_store: appended bar %s (%s)", dt.strftime("%H:%M"), actual_direction)
    except Exception as exc:
        logger.warning("semantic_store: failed to append record: %s", exc)


# ── Read ──────────────────────────────────────────────────────────────────────

def load_all() -> List[Dict]:
    """Load full history in chronological order (oldest first)."""
    if not _HIST_FILE.exists():
        return []
    records = []
    try:
        with _lock:
            with open(_HIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
    except Exception as exc:
        logger.warning("semantic_store: failed to read file: %s", exc)
    return records


def load_recent(n: int) -> List[Dict]:
    return load_all()[-n:]


def record_count() -> int:
    if not _HIST_FILE.exists():
        return 0
    try:
        with _lock:
            with open(_HIST_FILE, "r", encoding="utf-8") as f:
                return sum(1 for line in f if line.strip())
    except Exception:
        return 0


# ── Accuracy analytics ────────────────────────────────────────────────────────

def compute_all_indicator_accuracy(n: Optional[int] = None) -> Dict[str, Dict]:
    """
    Compute per-indicator win/loss scores across all prediction sources:
    strategy_votes, specialist_signals, dashboard_signals_raw, deepseek, ensemble.

    Returns {name: {wins, losses, total, directional, accuracy}}.
    Also includes "best_indicator" key with the highest-accuracy name (≥20 calls).
    """
    cutoff  = _score_reset_at()
    records = load_all()
    records = [r for r in records if r.get("window_start", 0) >= cutoff]
    if n is not None:
        records = records[-n:]

    counts: Dict[str, Dict[str, int]] = {}

    def _tally(name: str, predicted: str, actual: str):
        if name not in counts:
            counts[name] = {"wins": 0, "losses": 0, "total": 0}
        counts[name]["total"] += 1
        if predicted not in ("UP", "DOWN"):
            return
        if predicted == actual:
            counts[name]["wins"] += 1
        else:
            counts[name]["losses"] += 1

    def _sig_of(val) -> str:
        if isinstance(val, dict):
            val = val.get("signal")
        if isinstance(val, str):
            return val.upper()
        return ""

    for rec in records:
        try:
            actual = rec.get("actual_direction", "")
            if actual not in ("UP", "DOWN"):
                continue

            for strat_name, vote in (rec.get("strategy_votes") or {}).items():
                name_str = str(strat_name)
                if name_str.startswith("dash:") or name_str.startswith("spec:"):
                    continue
                _tally(f"strat:{name_str}", _sig_of(vote), actual)

            for spec_name, spec in (rec.get("specialist_signals") or {}).items():
                _tally(f"spec:{spec_name}", _sig_of(spec), actual)

            for ind_name, ind_sig in (rec.get("dashboard_signals_raw") or {}).items():
                _tally(f"dash:{ind_name}", _sig_of(ind_sig), actual)

            _tally("deepseek", _sig_of(rec.get("deepseek_signal")), actual)
            _tally("ensemble", _sig_of(rec.get("ensemble_signal")), actual)
        except Exception as rec_exc:
            logger.warning(
                "compute_all_indicator_accuracy: skipped bar ws=%s — %s: %s",
                rec.get("window_start"), type(rec_exc).__name__, rec_exc,
            )
            continue

    result: Dict[str, Dict] = {}
    for name, c in counts.items():
        total       = c["total"]
        wins        = c["wins"]
        losses      = c["losses"]
        directional = wins + losses
        result[name] = {
            "wins":        wins,
            "losses":      losses,
            "total":       total,
            "directional": directional,
            "accuracy":    round(wins / directional, 4) if directional > 0 else 0.5,
        }

    qualified = {k: v for k, v in result.items() if v["directional"] > 0}
    best = max(qualified, key=lambda k: qualified[k]["accuracy"]) if qualified else None
    result["best_indicator"] = best  # type: ignore[assignment]

    return result


def compute_dashboard_accuracy(n: Optional[int] = None) -> Dict[str, Dict]:
    """Per-indicator accuracy for dashboard microstructure signals."""
    cutoff  = _score_reset_at()
    records = load_all()
    records = [r for r in records if r.get("window_start", 0) >= cutoff]
    if n is not None:
        records = records[-n:]

    counts: Dict[str, Dict[str, int]] = {}

    for rec in records:
        actual    = rec.get("actual_direction", "")
        raw_sigs  = rec.get("dashboard_signals_raw", {})
        if not actual or not raw_sigs:
            continue
        for indicator, predicted in raw_sigs.items():
            if predicted not in ("UP", "DOWN"):
                continue
            if indicator not in counts:
                counts[indicator] = {"correct": 0, "total": 0}
            counts[indicator]["total"] += 1
            if predicted == actual:
                counts[indicator]["correct"] += 1

    result: Dict[str, Dict] = {}
    for name, c in counts.items():
        total   = c["total"]
        correct = c["correct"]
        result[name] = {
            "correct":  correct,
            "total":    total,
            "accuracy": round(correct / total, 4) if total > 0 else 0.5,
        }
    return result


def clean_incomplete_windows(window_starts) -> int:
    """
    Remove pattern_history records whose window_start is in window_starts.
    Returns the number of records removed.
    """
    ws_set = set(window_starts)
    if not ws_set:
        return 0
    with _lock:
        if not _HIST_FILE.exists():
            return 0
        records = []
        removed = 0
        with open(_HIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    if r.get("window_start") in ws_set:
                        removed += 1
                    else:
                        records.append(r)
                except Exception:
                    pass
        if removed:
            with open(_HIST_FILE, "w", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r, default=str) + "\n")
    return removed


# ── File-based vector stubs (no pgvector locally) ────────────────────────────

def store_embedding(window_start: float, vector) -> None:
    """No-op for file-based backend — pgvector only available on Railway."""
    pass


def search_similar(query_vec, k: int = 50) -> list:
    """Fallback: return most recent k bars by timestamp (no vector search locally)."""
    return load_all()[-k:]


# ── Backend routing ───────────────────────────────────────────────────────────
# When DATABASE_URL is present (Railway), transparently swap every public
# function to its PostgreSQL equivalent. Local dev is completely unaffected.

if os.environ.get("DATABASE_URL"):
    try:
        from semantic_store_pg import (
            append_resolved_window,
            load_all,
            compute_dashboard_accuracy,
            compute_all_indicator_accuracy,
            store_embedding,
            search_similar,
            fetch_postmortems,
        )
        logger.info("semantic_store: using PostgreSQL backend")
    except ImportError as _e:
        logger.warning("semantic_store: psycopg2 not installed, falling back to file (%s)", _e)
else:
    def fetch_postmortems(window_starts):   # type: ignore
        """File-backed shim — local dev has no postmortems table, return empty."""
        return {}

