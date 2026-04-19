"""
File-based Storage — NDJSON persistence for ticks, predictions, and DeepSeek predictions.

Data is stored in results/ at the project root:
  - ticks.ndjson
  - predictions.ndjson
  - deepseek_predictions.ndjson
"""

import json
import os
import threading
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_DATA_DIR        = Path(__file__).parent / "results"
_RESET_FILE      = Path(__file__).parent / "score_reset.json"
_MAX_TICKS       = 5000


def _score_reset_at() -> float:
    """Return the Unix timestamp after which bars count toward scores. 0 = count all."""
    try:
        data = json.loads(_RESET_FILE.read_text(encoding="utf-8"))
        return float(data.get("reset_at", 0))
    except Exception:
        return 0.0


def _read_ndjson(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def _append_ndjson(path: Path, record: Dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")


def _rewrite_ndjson(path: Path, records: List[Dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")


class Storage:
    def __init__(self, uri: str = "", db_name: str = "btc_predictor"):
        self._lock = threading.Lock()
        self._ticks_path = _DATA_DIR / "ticks.ndjson"
        self._preds_path = _DATA_DIR / "predictions.ndjson"
        self._ds_path    = _DATA_DIR / "deepseek_predictions.ndjson"
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("File storage initialised at %s", _DATA_DIR)

    # ── Ticks ─────────────────────────────────────────────────────────────────

    def store_tick(self, timestamp: float, mid: float, bid: float, ask: float, spread: float):
        with self._lock:
            _append_ndjson(self._ticks_path, {
                "timestamp": timestamp,
                "mid_price": mid,
                "bid_price": bid,
                "ask_price": ask,
                "spread": spread,
            })
            records = _read_ndjson(self._ticks_path)
            if len(records) > _MAX_TICKS:
                _rewrite_ndjson(self._ticks_path, records[-_MAX_TICKS:])

    def get_prices(self, n: Optional[int] = None, since: Optional[float] = None) -> List[float]:
        with self._lock:
            records = _read_ndjson(self._ticks_path)
        records.sort(key=lambda r: r["timestamp"])
        if since:
            records = [r for r in records if r["timestamp"] >= since]
        if n:
            records = records[-n:]
        return [r["mid_price"] for r in records]

    # ── Ensemble predictions ──────────────────────────────────────────────────

    def store_prediction(
        self,
        window_start: float,
        window_end: float,
        start_price: float,
        signal: str,
        confidence: float,
        strategy_votes: Dict,
        market_odds: Optional[float] = None,
        ev: Optional[float] = None,
    ):
        with self._lock:
            records = _read_ndjson(self._preds_path)
            if any(r["window_start"] == window_start for r in records):
                return
            _append_ndjson(self._preds_path, {
                "window_start":   window_start,
                "window_end":     window_end,
                "start_price":    start_price,
                "signal":         signal,
                "confidence":     confidence,
                "strategy_votes": json.dumps(strategy_votes),
                "market_odds":    market_odds,
                "ev":             ev,
                "created_at":     time.time(),
            })

    def resolve_prediction(self, window_start: float, end_price: float):
        with self._lock:
            records = _read_ndjson(self._preds_path)
            updated = False
            for r in records:
                if r["window_start"] == window_start:
                    actual = "UP" if end_price >= r["start_price"] else "DOWN"
                    r["end_price"] = end_price
                    r["actual_direction"] = actual
                    # NEUTRAL is an abstention — not scored as correct or wrong
                    r["correct"] = None if r.get("signal") == "NEUTRAL" else (1 if actual == r["signal"] else 0)
                    updated = True
                    break
            if updated:
                _rewrite_ndjson(self._preds_path, records)

    def get_rolling_accuracy(self, n: int = 12) -> Tuple[int, int, float]:
        cutoff = _score_reset_at()
        with self._lock:
            records = _read_ndjson(self._preds_path)
        resolved = [r for r in records if r.get("correct") is not None and r.get("signal") != "NEUTRAL" and r["window_start"] >= cutoff]
        resolved.sort(key=lambda r: r["window_start"], reverse=True)
        resolved = resolved[:n]
        if not resolved:
            return 0, 0, 0.0
        total = len(resolved)
        correct = sum(1 for r in resolved if r["correct"])
        return total, correct, correct / total

    def get_total_accuracy(self) -> Tuple[int, int, float, int]:
        cutoff = _score_reset_at()
        with self._lock:
            records = _read_ndjson(self._preds_path)
        scoped   = [r for r in records if r["window_start"] >= cutoff]
        resolved = [r for r in scoped if r.get("correct") is not None and r.get("signal") != "NEUTRAL"]
        neutral  = sum(1 for r in scoped if r.get("signal") == "NEUTRAL")
        if not resolved:
            return 0, 0, 0.0, neutral
        total   = len(resolved)
        correct = sum(1 for r in resolved if r["correct"])
        return total, correct, correct / total, neutral

    def get_strategy_rolling_accuracy(self, n: int = 20) -> Dict[str, float]:
        cutoff = _score_reset_at()
        with self._lock:
            records = _read_ndjson(self._preds_path)
        resolved = [r for r in records if r.get("actual_direction") and r["window_start"] >= cutoff]
        resolved.sort(key=lambda r: r["window_start"], reverse=True)
        resolved = resolved[:n]
        if not resolved:
            return {}
        accuracy: Dict[str, Dict] = {}
        for doc in resolved:
            votes_raw = doc.get("strategy_votes", "{}")
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else votes_raw
            except Exception:
                votes = {}
            actual = doc["actual_direction"]
            for name, vote in votes.items():
                sig = vote.get("signal") if isinstance(vote, dict) else str(vote)
                if sig not in ("UP", "DOWN"):
                    continue
                if name not in accuracy:
                    accuracy[name] = {"correct": 0, "total": 0}
                accuracy[name]["total"] += 1
                if sig == actual:
                    accuracy[name]["correct"] += 1
        return {
            name: s["correct"] / s["total"] if s["total"] > 0 else 0.5
            for name, s in accuracy.items()
        }

    def get_strategy_accuracy_full(self, n: int = 100) -> Dict[str, Dict]:
        cutoff = _score_reset_at()
        with self._lock:
            records = _read_ndjson(self._preds_path)
        resolved = [r for r in records if r.get("actual_direction") and r["window_start"] >= cutoff]
        resolved.sort(key=lambda r: r["window_start"], reverse=True)
        resolved = resolved[:n]
        if not resolved:
            return {}
        stats: Dict[str, Dict] = {}
        for doc in resolved:
            votes_raw = doc.get("strategy_votes", "{}")
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else votes_raw
            except Exception:
                votes = {}
            actual = doc["actual_direction"]
            for name, vote in votes.items():
                sig = vote.get("signal") if isinstance(vote, dict) else str(vote)
                if sig not in ("UP", "DOWN"):
                    continue
                if name not in stats:
                    stats[name] = {"correct": 0, "total": 0}
                stats[name]["total"] += 1
                if sig == actual:
                    stats[name]["correct"] += 1
        return {
            name: {
                "correct":  s["correct"],
                "total":    s["total"],
                "accuracy": s["correct"] / s["total"] if s["total"] > 0 else 0.5,
            }
            for name, s in stats.items()
        }

    def get_recent_predictions(self, n: int = 50) -> List[Dict]:
        with self._lock:
            records = _read_ndjson(self._preds_path)
        # Include neutrals and losses — filter only on actual_direction being resolved
        resolved = [r for r in records if r.get("actual_direction")]
        resolved.sort(key=lambda r: r["window_start"], reverse=True)
        fields = {"window_start", "window_end", "start_price", "end_price",
                  "signal", "confidence", "actual_direction", "correct", "market_odds", "ev"}
        return [{k: v for k, v in r.items() if k in fields} for r in resolved[:n]]

    def get_neutral_analysis(self) -> Dict:
        """Return stats on NEUTRAL DeepSeek predictions to help tune the neutral threshold."""
        cutoff = _score_reset_at()
        with self._lock:
            ds_records = _read_ndjson(self._ds_path)
        neutrals = [
            r for r in ds_records
            if r.get("signal") == "NEUTRAL" and r.get("actual_direction") and r["window_start"] >= cutoff
        ]
        total = len(neutrals)
        if not total:
            return {
                "total": 0, "market_went_up": 0, "market_went_down": 0,
                "pct_up": 0.0, "pct_down": 0.0,
                "would_have_won_if_traded_up": 0, "would_have_won_if_traded_down": 0,
                "records": [],
            }
        up   = sum(1 for r in neutrals if r["actual_direction"] == "UP")
        down = total - up
        records_out = sorted(
            [{"window_start": r["window_start"], "actual_direction": r["actual_direction"],
              "confidence": r.get("confidence", 0), "reasoning": r.get("reasoning", "")[:200]}
             for r in neutrals],
            key=lambda x: x["window_start"], reverse=True,
        )
        return {
            "total":              total,
            "market_went_up":     up,
            "market_went_down":   down,
            "pct_up":             round(up / total * 100, 1),
            "pct_down":           round(down / total * 100, 1),
            "would_have_won_if_traded_up":   up,
            "would_have_won_if_traded_down": down,
            "records": records_out,
        }

    # ── DeepSeek predictions ──────────────────────────────────────────────────

    def store_deepseek_prediction(
        self,
        window_start: float,
        window_end: float,
        start_price: float,
        signal: str,
        confidence: int,
        reasoning: str,
        raw_response: str,
        full_prompt: str,
        polymarket_url: str,
        strategy_snapshot: str,
        latency_ms: int,
        window_count: int,
        data_received: str = "",
        data_requests: str = "",
        indicators_snapshot: str = "",
        narrative: str = "",
        free_observation: str = "",
        chart_path: str = "",
        dashboard_signals_snapshot: str = "",
    ):
        with self._lock:
            records = _read_ndjson(self._ds_path)
            records = [r for r in records if r["window_start"] != window_start]
            records.append({
                "window_start":        window_start,
                "window_end":          window_end,
                "start_price":         start_price,
                "signal":              signal,
                "confidence":          confidence,
                "reasoning":           reasoning,
                "raw_response":        raw_response,
                "full_prompt":         full_prompt,
                "polymarket_url":      polymarket_url,
                "strategy_snapshot":   strategy_snapshot,
                "latency_ms":          latency_ms,
                "window_count":        window_count,
                "data_received":       data_received,
                "data_requests":       data_requests,
                "indicators_snapshot": indicators_snapshot,
                "narrative":           narrative,
                "free_observation":    free_observation,
                "chart_path":          chart_path,
                "dashboard_signals_snapshot": dashboard_signals_snapshot,
                "created_at":          time.time(),
            })
            _rewrite_ndjson(self._ds_path, records)

    def resolve_deepseek_prediction(self, window_start: float, end_price: float):
        with self._lock:
            records = _read_ndjson(self._ds_path)
            updated = False
            for r in records:
                if r["window_start"] == window_start:
                    actual = "UP" if end_price >= r["start_price"] else "DOWN"
                    r["end_price"] = end_price
                    r["actual_direction"] = actual
                    # NEUTRAL is an abstention — not scored as correct or wrong
                    r["correct"] = None if r.get("signal") == "NEUTRAL" else (1 if actual == r["signal"] else 0)
                    updated = True
                    break
            if updated:
                _rewrite_ndjson(self._ds_path, records)

    def store_postmortem(self, window_start: float, postmortem: str):
        with self._lock:
            records = _read_ndjson(self._ds_path)
            for r in records:
                if r["window_start"] == window_start:
                    r["postmortem"] = postmortem
                    break
            _rewrite_ndjson(self._ds_path, records)

    def get_agree_accuracy(self) -> Dict:
        cutoff = _score_reset_at()
        with self._lock:
            ds_records  = _read_ndjson(self._ds_path)
            ens_records = _read_ndjson(self._preds_path)
        ds_map = {r["window_start"]: r for r in ds_records
                  if r.get("actual_direction") and r["window_start"] >= cutoff}
        if not ds_map:
            return {"total_agree": 0, "correct_agree": 0, "accuracy_agree": 0.0}
        total = correct = 0
        for r in ens_records:
            ws = r["window_start"]
            if ws in ds_map and r.get("actual_direction") and ws >= cutoff:
                # Only count agreement on directional predictions — skip NEUTRAL on either side
                if r.get("signal") == "NEUTRAL" or ds_map[ws].get("signal") == "NEUTRAL":
                    continue
                if ds_map[ws]["signal"] == r["signal"]:
                    total += 1
                    if r["actual_direction"] == r["signal"]:
                        correct += 1
        return {
            "total_agree": total,
            "correct_agree": correct,
            "accuracy_agree": correct / total if total > 0 else 0.0,
        }

    def get_deepseek_accuracy(self) -> Dict:
        cutoff = _score_reset_at()
        with self._lock:
            records = _read_ndjson(self._ds_path)
        scoped   = [r for r in records if r["window_start"] >= cutoff]
        neutrals = sum(1 for r in scoped if r.get("signal") == "NEUTRAL")
        # Only score directional predictions — NEUTRAL is an abstention
        resolved = [r for r in scoped if r.get("correct") is not None and r.get("signal") != "NEUTRAL"]
        if not resolved:
            return {"total": 0, "correct": 0, "accuracy": 0.0, "neutrals": neutrals, "directional": 0}
        total   = len(resolved)
        correct = sum(1 for r in resolved if r["correct"])
        return {"total": total, "correct": correct, "accuracy": correct / total if total > 0 else 0.0,
                "neutrals": neutrals, "directional": total}

    def get_recent_deepseek_predictions(self, n: int = 50) -> List[Dict]:
        with self._lock:
            records = _read_ndjson(self._ds_path)
        records.sort(key=lambda r: r["window_start"], reverse=True)
        return records[:n]

    def get_audit_records(self, n: int = 500) -> List[Dict]:
        with self._lock:
            records = _read_ndjson(self._ds_path)
        records.sort(key=lambda r: r["window_start"], reverse=True)
        records = records[:n]
        for rec in records:
            for field in ("indicators_snapshot", "strategy_snapshot"):
                raw = rec.get(field)
                if raw and isinstance(raw, str):
                    try:
                        rec[field] = json.loads(raw)
                    except Exception:
                        pass
        return records

    def store_accuracy_snapshot(self, window_start: float, snapshot: Dict):
        with self._lock:
            records = _read_ndjson(self._ds_path)
            updated = False
            for r in records:
                if r["window_start"] == window_start:
                    r["accuracy_snapshot"] = snapshot
                    updated = True
                    break
            if updated:
                _rewrite_ndjson(self._ds_path, records)

    def get_prediction_history_with_indicators(self, n: int = 25) -> List[Dict]:
        with self._lock:
            pred_records = _read_ndjson(self._preds_path)
            ds_records   = _read_ndjson(self._ds_path)

        resolved = [r for r in pred_records if r.get("actual_direction")]
        resolved.sort(key=lambda r: r["window_start"], reverse=True)
        resolved = resolved[:n]
        if not resolved:
            return []

        ds_map = {r["window_start"]: r for r in ds_records}

        result = []
        for doc in reversed(resolved):
            ws = doc["window_start"]
            votes_raw = doc.get("strategy_votes", "{}")
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else (votes_raw or {})
            except Exception:
                votes = {}
            ind_raw = ds_map.get(ws, {}).get("indicators_snapshot", "{}")
            try:
                indicators = json.loads(ind_raw) if isinstance(ind_raw, str) else (ind_raw or {})
            except Exception:
                indicators = {}
            result.append({
                "window_start":     ws,
                "actual_direction": doc["actual_direction"],
                "start_price":      doc.get("start_price"),
                "strategy_votes":   votes,
                "indicators":       indicators,
            })
        return result

    def close(self):
        pass


# ── Backend factory ───────────────────────────────────────────────────────────
# Import this instead of Storage() directly:
#   from storage import get_storage
#   storage = get_storage()
# Local (no DATABASE_URL) → file-based Storage above.
# Railway (DATABASE_URL set) → PostgreSQL StoragePG.

def get_storage(**kwargs):
    if os.environ.get("DATABASE_URL"):
        from storage_pg import StoragePG
        return StoragePG(**kwargs)
    return Storage(**kwargs)
