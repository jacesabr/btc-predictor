"""
Semantic store — pattern_history (Postgres + pgvector).

One row per resolved 5-minute bar, stored as a JSON blob in the `data` column,
plus a REAL[] embedding column populated after Cohere encodes the bar's essay.

Production has always run against Postgres; the old file-backed backend and the
`DATABASE_URL`-conditional import shim have been removed.
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np
import psycopg2
import psycopg2.extras

try:
    from pgvector.psycopg2 import register_vector as _register_vector
    _PGVECTOR_AVAILABLE = True
except ImportError:
    _PGVECTOR_AVAILABLE = False

logger = logging.getLogger(__name__)

_lock = threading.Lock()

_DAYS     = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_SESSIONS = [(0, 8, "ASIA"), (8, 13, "LONDON"), (13, 16, "OVERLAP"), (16, 21, "NY"), (21, 24, "LATE")]


def _session_label(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    for start, end, label in _SESSIONS:
        if start <= dt.hour < end:
            return label
    return "LATE"


# ── Connection ────────────────────────────────────────────────────────────────

def _conn():
    from storage_pg import _get_pool
    conn = _get_pool().getconn()
    if _PGVECTOR_AVAILABLE:
        try:
            _register_vector(conn)
        except Exception:
            pass
    return conn


def _put(conn):
    from storage_pg import _get_pool
    _get_pool().putconn(conn)


# ── Schema ────────────────────────────────────────────────────────────────────

_DDL_BASE = """
CREATE TABLE IF NOT EXISTS pattern_history (
    window_start DOUBLE PRECISION PRIMARY KEY,
    data         TEXT NOT NULL,
    created_at   DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pattern_history_ws ON pattern_history (window_start);
ALTER TABLE pattern_history ADD COLUMN IF NOT EXISTS embedding REAL[];
"""


def _ensure_table():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL_BASE)
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
    historical_analysis:    str                  = "",
    dashboard_signals_raw:  Optional[Dict]       = None,
    accuracy_snapshot:      Optional[Dict]       = None,
    full_prompt:            str                  = "",
    trade_action:           str                  = "",
    window_count:           int                  = 0,
    binance_expert_analysis = None,
):
    _init()
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
        "specialist_signals": specialist_signals or {},
        "historical_analysis": historical_analysis,
        "binance_expert_analysis": binance_expert_analysis or {},
        "strategy_votes":     strategy_votes,
        "indicators":         indicators,
        "dashboard_signals_raw": dashboard_signals_raw or {},
        "accuracy_snapshot":  accuracy_snapshot or {},
        "full_prompt":         full_prompt,
        "trade_action":        trade_action,
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


def fetch_postmortems(window_starts: List[float]) -> Dict[float, str]:
    """Return {window_start: postmortem_text} for the given bars.

    The historical-analyst prompt injects postmortems alongside each bar's
    predictions so the LLM can see how each similar setup RESOLVED — not just
    the indicator state. Stored in deepseek_predictions; pattern_history only
    has the pre-resolve snapshot.
    """
    if not window_starts:
        return {}
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT window_start, postmortem FROM deepseek_predictions "
                "WHERE window_start = ANY(%s) AND postmortem IS NOT NULL AND LENGTH(postmortem) > 50",
                (list(window_starts),),
            )
            return {float(ws): pm for ws, pm in cur.fetchall()}
    except Exception as exc:
        logger.warning("fetch_postmortems failed: %s", exc)
        return {}
    finally:
        _put(conn)


# ── Vector search ─────────────────────────────────────────────────────────────

def store_embedding(window_start: float, vector: np.ndarray):
    """Store a Cohere 1024-dim embedding for a resolved bar as a REAL[] array."""
    _init()
    if vector is None:
        logger.warning("store_embedding: skipping None vector for window_start=%.0f", window_start)
        return
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pattern_history SET embedding = %s WHERE window_start = %s",
                (vector.astype(np.float32).tolist(), float(window_start)),
            )
            if cur.rowcount == 0:
                logger.warning(
                    "store_embedding: no pattern_history row for window_start=%.0f — bar not embedded",
                    window_start,
                )
            else:
                logger.info("store_embedding: saved %d-dim vector for bar %.0f", len(vector), window_start)
        conn.commit()
    finally:
        _put(conn)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def search_similar(query_vec: np.ndarray, k: int = 50) -> List[Dict]:
    """Return up to k most similar bars using cosine similarity on stored REAL[]
    embeddings. Falls back to the most recent k bars if no embeddings exist yet.
    """
    _init()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data, embedding FROM pattern_history WHERE embedding IS NOT NULL ORDER BY window_start DESC LIMIT 2000",
            )
            rows = cur.fetchall()

        if not rows:
            logger.info("search_similar: no embeddings stored yet, falling back to recent bars")
            return load_all()[-k:]

        q = query_vec.astype(np.float32)
        scored = []
        for data_str, emb_list in rows:
            try:
                bar = json.loads(data_str)
                emb = np.array(emb_list, dtype=np.float32)
                sim = _cosine_similarity(q, emb)
                bar["_similarity"] = round(sim, 4)
                scored.append((sim, bar))
            except Exception:
                pass

        scored.sort(key=lambda x: x[0], reverse=True)
        results = [bar for _, bar in scored[:k]]
        if results:
            logger.info("cosine search: %d/%d bars scored, top sim=%.3f",
                        len(scored), len(rows), results[0]["_similarity"])
        return results if results else load_all()[-k:]
    finally:
        _put(conn)


# ── Accuracy computations ─────────────────────────────────────────────────────

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


def compute_all_indicator_accuracy(n: Optional[int] = None) -> Dict:
    from storage_pg import get_reset_at
    cutoff = get_reset_at()
    limit = n if n is not None and n > 0 else 10000
    records = [r for r in load_all(limit) if r.get("window_start", 0) >= cutoff]
    resolved = [r for r in records if r.get("actual_direction") in ("UP", "DOWN")]
    if not resolved:
        return {"best_indicator": None}

    counts: Dict[str, Dict] = {}

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
        """Normalise any vote-ish value to 'UP'/'DOWN'/'NEUTRAL'/''."""
        if isinstance(val, dict):
            val = val.get("signal")
        if isinstance(val, str):
            return val.upper()
        return ""

    for rec in resolved:
        try:
            actual = rec["actual_direction"]

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

    result: Dict = {}
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
    result["best_indicator"] = max(qualified, key=lambda k: qualified[k]["accuracy"]) if qualified else None
    return result
