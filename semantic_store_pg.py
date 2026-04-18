"""
PostgreSQL backend for semantic_store — mirrors the exact interface of semantic_store.py.
Used automatically when DATABASE_URL is set. Falls back to file-based when absent.

Table: pattern_history — one row per resolved bar, full JSON blob.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_lock = threading.Lock()

_DAYS     = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_SESSIONS = [(0,8,"ASIA"),(8,13,"LONDON"),(13,16,"OVERLAP"),(16,21,"NY"),(21,24,"LATE")]


def _session_label(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    for start, end, label in _SESSIONS:
        if start <= dt.hour < end:
            return label
    return "LATE"


def _conn():
    from storage_pg import _get_pool
    return _get_pool().getconn()


def _put(conn):
    from storage_pg import _get_pool
    _get_pool().putconn(conn)


# ── Schema ────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS pattern_history (
    window_start DOUBLE PRECISION PRIMARY KEY,
    data         TEXT NOT NULL,
    created_at   DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pattern_history_ws ON pattern_history (window_start);
"""


def _ensure_table():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
    finally:
        _put(conn)


_table_ready = False


def _init():
    global _table_ready
    if not _table_ready:
        _ensure_table()
        _table_ready = True


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
    creative_edge:          str                  = "",
    historical_analysis:    str                  = "",
    dashboard_signals_raw:  Optional[Dict]       = None,
    accuracy_snapshot:      Optional[Dict]       = None,
):
    _init()
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    record = {
        "window_start":       window_start,
        "window_end":         window_end or window_start + 300,
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
        "specialist_signals": specialist_signals or {},
        "creative_edge":      creative_edge,
        "historical_analysis": historical_analysis,
        "strategy_votes":     strategy_votes,
        "indicators":         indicators,
        "dashboard_signals_raw": dashboard_signals_raw or {},
        "accuracy_snapshot":  accuracy_snapshot or {},
    }
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pattern_history (window_start, data, created_at) "
                "VALUES (%s, %s, %s) ON CONFLICT (window_start) DO UPDATE SET data=EXCLUDED.data",
                (window_start, json.dumps(record, default=str), time.time()),
            )
        conn.commit()
    finally:
        _put(conn)


# ── Read ──────────────────────────────────────────────────────────────────────

def load_all(limit: int = 10000) -> List[Dict]:
    """Return up to `limit` most recent bars, oldest → newest."""
    _init()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data FROM pattern_history ORDER BY window_start DESC LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
        records = []
        for (data_str,) in rows:
            try:
                records.append(json.loads(data_str))
            except Exception:
                pass
        records.sort(key=lambda r: r.get("window_start", 0))
        return records
    finally:
        _put(conn)


# ── Accuracy helpers (same interface as semantic_store.py) ────────────────────

def compute_dashboard_accuracy(n: int = 200) -> Dict:
    records = load_all(n)
    resolved = [r for r in records if r.get("actual_direction")]
    if not resolved:
        return {}
    counts: Dict[str, Dict] = {}
    for r in resolved:
        actual = r["actual_direction"]
        dash = r.get("dashboard_signals_raw") or {}
        for key, val in dash.items():
            if val not in ("UP", "DOWN"):
                continue
            if key not in counts:
                counts[key] = {"correct": 0, "total": 0}
            counts[key]["total"] += 1
            if val == actual:
                counts[key]["correct"] += 1
    return {
        k: {"accuracy": v["correct"] / v["total"], "correct": v["correct"], "total": v["total"]}
        for k, v in counts.items() if v["total"] > 0
    }


def compute_all_indicator_accuracy(n: int = 100) -> Dict:
    records = load_all(n)
    resolved = [r for r in records if r.get("actual_direction")]
    if not resolved:
        return {}
    sums: Dict[str, Dict] = {}
    for r in resolved:
        actual = r["actual_direction"]
        ind = r.get("indicators") or {}
        votes = r.get("strategy_votes") or {}
        for name, vote in votes.items():
            sig = vote.get("signal") if isinstance(vote, dict) else None
            if sig not in ("UP", "DOWN"):
                continue
            if name not in sums:
                sums[name] = {"correct": 0, "total": 0}
            sums[name]["total"] += 1
            if sig == actual:
                sums[name]["correct"] += 1
    return {
        k: {"accuracy": v["correct"] / v["total"], "correct": v["correct"], "total": v["total"]}
        for k, v in sums.items() if v["total"] > 0
    }
