"""
Pattern History Store
=====================
Appends the COMPLETE resolved bar record to a persistent NDJSON file.
Every specialist, every indicator value, every signal, and the final outcome
is stored so the bar_insight_analyst and pattern_analyst have the full picture.

File: results/pattern_history.ndjson
One JSON record per line. Grows indefinitely; never trimmed.

Complete record schema:
{
  # Identity
  "window_start":       float,   # unix timestamp of bar open
  "window_end":         float,   # unix timestamp of bar close
  "actual_direction":   str,     # "UP" or "DOWN" (known at bar close)
  "start_price":        float,
  "end_price":          float,

  # Math ensemble
  "ensemble_signal":    str,     # "UP" | "DOWN"
  "ensemble_conf":      float,   # 0.0-1.0
  "ensemble_correct":   bool,

  # DeepSeek main prediction
  "deepseek_signal":    str,     # "UP" | "DOWN" | "ERROR"
  "deepseek_conf":      int,     # 0-100
  "deepseek_correct":   bool | None,
  "deepseek_reasoning": str,
  "deepseek_narrative": str,
  "deepseek_free_obs":  str,     # free observation from model

  # Specialist outputs (unified analyst)
  "specialist_signals": dict,    # {dow_theory, fib_pullback, alligator, acc_dist, harmonic}
                                 # each: {signal, confidence, reasoning, value}
  "creative_edge":      str,     # unified analyst creative observation

  # Pattern analyst output
  "pattern_analysis":   str,     # full text from pattern analyst

  # Bar insight analyst output
  "bar_insight_text":   str,     # full text from bar insight analyst
  "bar_insight_signal": str,     # structured CALL: "UP" | "DOWN" | ""

  # Strategy votes (all rule-based signals)
  "strategy_votes":     dict,    # {name: {signal, confidence, reasoning, value}}

  # Technical indicators (feature engine)
  "indicators":         dict,    # rsi_14, macd_histogram, bollinger_pct_b, etc.

  # Dashboard microstructure signal directions at bar open
  # {indicator_name: "UP" | "DOWN" | "NEUTRAL"}  — used for accuracy tracking
  "dashboard_signals_raw": dict,

  # Session context
  "session":            str,     # ASIA | LONDON | OVERLAP | NY | LATE
  "day_of_week":        str,     # Mon | Tue | Wed | Thu | Fri | Sat | Sun
}
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DATA_DIR  = Path(__file__).parent.parent / "results"
_HIST_FILE = _DATA_DIR / "pattern_history.ndjson"
_lock      = threading.Lock()

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


def _ensure_dir():
    _DATA_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────
# Write
# ─────────────────────────────────────────────────────────────

def append_resolved_window(
    window_start:           float,
    actual_direction:       str,
    start_price:            float,
    strategy_votes:         Dict,
    indicators:             Dict,
    # Extended fields — all optional for backwards compat
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
    creative_edge:          str                  = "",
    pattern_analysis:       str                  = "",
    bar_insight_text:       str                  = "",
    bar_insight_signal:     str                  = "",
    dashboard_signals_raw:  Optional[Dict]       = None,
):
    """
    Append one fully resolved bar to the history file.
    Call at bar close after actual_direction and all DeepSeek outputs are known.
    Thread-safe.
    """
    _ensure_dir()
    dt  = datetime.fromtimestamp(window_start, tz=timezone.utc)
    record = {
        # Identity
        "window_start":       window_start,
        "window_end":         window_end or window_start + 300,
        "actual_direction":   actual_direction,
        "start_price":        start_price,
        "end_price":          end_price,
        # Session context
        "session":            _session_label(window_start),
        "day_of_week":        _DAYS[dt.weekday()],
        "hour_utc":           dt.hour,
        # Math ensemble
        "ensemble_signal":    ensemble_signal,
        "ensemble_conf":      round(ensemble_conf, 4),
        "ensemble_correct":   ensemble_correct,
        # DeepSeek main prediction
        "deepseek_signal":    deepseek_signal,
        "deepseek_conf":      deepseek_conf,
        "deepseek_correct":   deepseek_correct,
        "deepseek_reasoning": deepseek_reasoning,
        "deepseek_narrative": deepseek_narrative,
        "deepseek_free_obs":  deepseek_free_obs,
        # Specialist outputs
        "specialist_signals": specialist_signals or {},
        "creative_edge":      creative_edge,
        "pattern_analysis":   pattern_analysis,
        "bar_insight_text":   bar_insight_text,
        "bar_insight_signal": bar_insight_signal,
        # Full indicator + vote snapshots
        "strategy_votes":     strategy_votes,
        "indicators":         indicators,
        # Dashboard microstructure signal directions at bar open (for accuracy tracking)
        "dashboard_signals_raw": dashboard_signals_raw or {},
    }
    try:
        with _lock:
            with open(_HIST_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
        logger.debug("pattern_history: appended bar %s (%s)",
                     dt.strftime("%H:%M"), actual_direction)
    except Exception as exc:
        logger.warning("pattern_history: failed to append record: %s", exc)


# ─────────────────────────────────────────────────────────────
# Read
# ─────────────────────────────────────────────────────────────

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
        logger.warning("pattern_history: failed to read file: %s", exc)
    return records


def load_recent(n: int) -> List[Dict]:
    """Return the most recent n resolved bars."""
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


def compute_bar_insight_accuracy(n: Optional[int] = None) -> Dict:
    """
    Compute historical accuracy for the bar insight analyst's structured CALL signal.

    Returns: {"correct": int, "total": int, "accuracy": float}
    Only bars where bar_insight_signal is "UP" or "DOWN" are scored.
    """
    records = load_all()
    if n is not None:
        records = records[-n:]

    correct = 0
    total   = 0
    for rec in records:
        actual  = rec.get("actual_direction", "")
        signal  = rec.get("bar_insight_signal", "")
        if actual not in ("UP", "DOWN") or signal not in ("UP", "DOWN"):
            continue
        total += 1
        if signal == actual:
            correct += 1

    return {
        "correct":  correct,
        "total":    total,
        "accuracy": round(correct / total, 4) if total > 0 else 0.5,
    }


def compute_all_indicator_accuracy(n: Optional[int] = None) -> Dict[str, Dict]:
    """
    Compute per-indicator win/loss scores across ALL prediction sources:
      - strategy_votes   : every rule-based / ML strategy (RSI, MACD, etc.)
      - specialist_signals: DeepSeek specialist analysts
      - dashboard_signals_raw: microstructure signals (order book, taker flow, etc.)
      - deepseek          : main DeepSeek AI prediction
      - ensemble          : math ensemble prediction

    Each entry has the same shape as DeepSeek accuracy tracking:
        {
          "wins":     int,
          "losses":   int,
          "total":    int,
          "accuracy": float,   # wins / total, 0.5 when total == 0
        }

    Only UP/DOWN predictions are scored; NEUTRAL / UNKNOWN / ERROR are skipped.

    Also returns a top-level "best_indicator" key with the name of the single
    indicator that has the highest accuracy (min 3 resolved calls to qualify).
    """
    records = load_all()
    if n is not None:
        records = records[-n:]

    counts: Dict[str, Dict[str, int]] = {}

    def _tally(name: str, predicted: str, actual: str):
        if predicted not in ("UP", "DOWN"):
            return
        if name not in counts:
            counts[name] = {"wins": 0, "losses": 0, "total": 0}
        counts[name]["total"] += 1
        if predicted == actual:
            counts[name]["wins"] += 1
        else:
            counts[name]["losses"] += 1

    for rec in records:
        actual = rec.get("actual_direction", "")
        if actual not in ("UP", "DOWN"):
            continue

        # ── Strategy votes (rule-based + ML signals) ──────────────────────────
        for strat_name, vote in (rec.get("strategy_votes") or {}).items():
            sig = ""
            if isinstance(vote, dict):
                sig = (vote.get("signal") or "").upper()
            elif isinstance(vote, str):
                sig = vote.upper()
            _tally(f"strat:{strat_name}", sig, actual)

        # ── Specialist signals ──────────────────────────────────────────────
        for spec_name, spec in (rec.get("specialist_signals") or {}).items():
            sig = ""
            if isinstance(spec, dict):
                sig = (spec.get("signal") or "").upper()
            _tally(f"spec:{spec_name}", sig, actual)

        # ── Dashboard microstructure signals ────────────────────────────────
        for ind_name, ind_sig in (rec.get("dashboard_signals_raw") or {}).items():
            _tally(f"dash:{ind_name}", (ind_sig or "").upper(), actual)

        # ── DeepSeek main prediction ────────────────────────────────────────
        ds_sig = (rec.get("deepseek_signal") or "").upper()
        _tally("deepseek", ds_sig, actual)

        # ── Ensemble math prediction ────────────────────────────────────────
        ens_sig = (rec.get("ensemble_signal") or "").upper()
        _tally("ensemble", ens_sig, actual)

    # Build result with accuracy field
    result: Dict[str, Dict] = {}
    for name, c in counts.items():
        total = c["total"]
        wins  = c["wins"]
        result[name] = {
            "wins":     wins,
            "losses":   c["losses"],
            "total":    total,
            "accuracy": round(wins / total, 4) if total > 0 else 0.5,
        }

    # Best indicator: highest accuracy with ≥ 3 resolved calls
    qualified = {k: v for k, v in result.items() if v["total"] >= 3}
    best = max(qualified, key=lambda k: qualified[k]["accuracy"]) if qualified else None
    result["best_indicator"] = best  # type: ignore[assignment]

    return result


def compute_dashboard_accuracy(n: Optional[int] = None) -> Dict[str, Dict]:
    """
    Compute per-indicator accuracy scores for the dashboard microstructure signals.

    Iterates over the last `n` resolved bars (all bars if n is None), checks the
    dashboard_signals_raw direction against actual_direction, and returns:

        {
          "order_book":  {"correct": 12, "total": 20, "accuracy": 0.60},
          "long_short":  {"correct":  8, "total": 18, "accuracy": 0.44},
          ...
        }

    Only UP/DOWN predictions are scored (NEUTRAL is skipped — no directional call made).
    """
    records = load_all()
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
                continue   # NEUTRAL — no call made
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
