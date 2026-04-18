"""
PostgreSQL backend for Storage — mirrors the exact interface of storage.py.
Used automatically when DATABASE_URL environment variable is set (Railway).
Falls back to file-based storage.py when DATABASE_URL is absent (local dev).

Tables created on first run:
  ticks                 — rolling price ticks (capped at 5000)
  predictions           — ensemble predictions + outcomes
  deepseek_predictions  — DeepSeek predictions + outcomes
"""

import json
import logging
import os
import threading
import time
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from psycopg2 import pool

logger = logging.getLogger(__name__)

_MAX_TICKS = 5000

# ── Connection pool (thread-safe, 1–5 connections) ────────────────────────────

_pool: Optional[pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                url = os.environ["DATABASE_URL"]
                _pool = pool.ThreadedConnectionPool(1, 5, dsn=url)
                logger.info("PostgreSQL pool created")
    return _pool


def _conn():
    """Borrow a connection from the pool."""
    return _get_pool().getconn()


def _put(conn):
    """Return a connection to the pool."""
    _get_pool().putconn(conn)


# ── Schema bootstrap ──────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS ticks (
    id          BIGSERIAL PRIMARY KEY,
    timestamp   DOUBLE PRECISION NOT NULL,
    mid_price   DOUBLE PRECISION NOT NULL,
    bid_price   DOUBLE PRECISION,
    ask_price   DOUBLE PRECISION,
    spread      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS predictions (
    window_start     DOUBLE PRECISION PRIMARY KEY,
    window_end       DOUBLE PRECISION,
    start_price      DOUBLE PRECISION,
    signal           TEXT,
    confidence       DOUBLE PRECISION,
    strategy_votes   TEXT,
    market_odds      DOUBLE PRECISION,
    ev               DOUBLE PRECISION,
    end_price        DOUBLE PRECISION,
    actual_direction TEXT,
    correct          INTEGER,
    created_at       DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS deepseek_predictions (
    window_start      DOUBLE PRECISION PRIMARY KEY,
    window_end        DOUBLE PRECISION,
    start_price       DOUBLE PRECISION,
    end_price         DOUBLE PRECISION,
    signal            TEXT,
    confidence        DOUBLE PRECISION,
    reasoning         TEXT,
    narrative         TEXT,
    free_observation  TEXT,
    data_received     TEXT,
    data_requests     TEXT,
    latency_ms        INTEGER,
    window_count      INTEGER,
    actual_direction  TEXT,
    correct           BOOLEAN,
    created_at        DOUBLE PRECISION
);
"""


def init_schema():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
        logger.info("PostgreSQL schema ready")
    finally:
        _put(conn)


# ── Storage class (same public interface as storage.py) ───────────────────────

class StoragePG:
    def __init__(self, uri: str = "", db_name: str = "btc_predictor"):
        self._lock = threading.Lock()
        init_schema()
        logger.info("PostgreSQL storage initialised")

    # ── Ticks ─────────────────────────────────────────────────────────────────

    def store_tick(self, timestamp: float, mid: float, bid: float, ask: float, spread: float):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ticks (timestamp, mid_price, bid_price, ask_price, spread) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (timestamp, mid, bid, ask, spread),
                )
                # Keep only the most recent MAX_TICKS rows
                cur.execute(
                    "DELETE FROM ticks WHERE id NOT IN ("
                    "  SELECT id FROM ticks ORDER BY timestamp DESC LIMIT %s"
                    ")",
                    (_MAX_TICKS,),
                )
            conn.commit()
        finally:
            _put(conn)

    def get_prices(self, n: Optional[int] = None, since: Optional[float] = None) -> List[float]:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                if since:
                    cur.execute(
                        "SELECT mid_price FROM ticks WHERE timestamp >= %s ORDER BY timestamp",
                        (since,),
                    )
                else:
                    limit = n or _MAX_TICKS
                    cur.execute(
                        "SELECT mid_price FROM ticks ORDER BY timestamp DESC LIMIT %s",
                        (limit,),
                    )
                    rows = [r[0] for r in cur.fetchall()]
                    return list(reversed(rows))
                return [r[0] for r in cur.fetchall()]
        finally:
            _put(conn)

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
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO predictions "
                    "(window_start, window_end, start_price, signal, confidence, "
                    " strategy_votes, market_odds, ev, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (window_start) DO NOTHING",
                    (window_start, window_end, start_price, signal, confidence,
                     json.dumps(strategy_votes), market_odds, ev, time.time()),
                )
            conn.commit()
        finally:
            _put(conn)

    def resolve_prediction(self, window_start: float, end_price: float):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT signal, start_price FROM predictions WHERE window_start = %s",
                    (window_start,),
                )
                row = cur.fetchone()
                if not row:
                    return
                signal, start_price = row
                actual = "UP" if end_price >= start_price else "DOWN"
                correct = 1 if actual == signal else 0
                cur.execute(
                    "UPDATE predictions SET end_price=%s, actual_direction=%s, correct=%s "
                    "WHERE window_start=%s",
                    (end_price, actual, correct, window_start),
                )
            conn.commit()
        finally:
            _put(conn)

    def get_rolling_accuracy(self, n: int = 12) -> Tuple[int, int, float]:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT correct FROM predictions WHERE correct IS NOT NULL "
                    "ORDER BY window_start DESC LIMIT %s",
                    (n,),
                )
                rows = cur.fetchall()
            if not rows:
                return 0, 0, 0.0
            total = len(rows)
            correct = sum(r[0] for r in rows)
            return total, correct, correct / total
        finally:
            _put(conn)

    def get_total_accuracy(self) -> Tuple[int, int, float]:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*), SUM(correct) FROM predictions WHERE correct IS NOT NULL"
                )
                row = cur.fetchone()
            total = row[0] or 0
            correct = int(row[1] or 0)
            return total, correct, correct / total if total > 0 else 0.0
        finally:
            _put(conn)

    def get_strategy_rolling_accuracy(self, n: int = 20) -> Dict[str, float]:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT strategy_votes, actual_direction FROM predictions "
                    "WHERE actual_direction IS NOT NULL ORDER BY window_start DESC LIMIT %s",
                    (n,),
                )
                rows = cur.fetchall()
        finally:
            _put(conn)
        accuracy: Dict[str, Dict] = {}
        for votes_raw, actual in rows:
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else votes_raw
            except Exception:
                votes = {}
            for name, vote in votes.items():
                if name not in accuracy:
                    accuracy[name] = {"correct": 0, "total": 0}
                accuracy[name]["total"] += 1
                if vote.get("signal") == actual:
                    accuracy[name]["correct"] += 1
        return {
            name: s["correct"] / s["total"] if s["total"] > 0 else 0.5
            for name, s in accuracy.items()
        }

    def get_strategy_accuracy_full(self, n: int = 100) -> Dict[str, Dict]:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT strategy_votes, actual_direction FROM predictions "
                    "WHERE actual_direction IS NOT NULL ORDER BY window_start DESC LIMIT %s",
                    (n,),
                )
                rows = cur.fetchall()
        finally:
            _put(conn)
        stats: Dict[str, Dict] = {}
        for votes_raw, actual in rows:
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else votes_raw
            except Exception:
                votes = {}
            for name, vote in votes.items():
                sig = vote.get("signal", "") if isinstance(vote, dict) else ""
                if not sig or sig not in ("UP", "DOWN", "NEUTRAL"):
                    continue
                if name not in stats:
                    stats[name] = {"correct": 0, "total": 0, "directional": 0}
                if sig in ("UP", "DOWN"):
                    stats[name]["total"] += 1
                    stats[name]["directional"] += 1
                    if sig == actual:
                        stats[name]["correct"] += 1
        return {
            name: {
                "accuracy": s["correct"] / s["directional"] if s["directional"] > 0 else 0.5,
                "correct": s["correct"],
                "total": s["total"],
                "directional": s["directional"],
            }
            for name, s in stats.items()
        }

    def get_rolling_window_accuracy(self, n: int = 12) -> Dict:
        return {}

    def get_agree_accuracy(self) -> Dict:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT p.actual_direction, p.signal, d.signal "
                    "FROM predictions p JOIN deepseek_predictions d USING (window_start) "
                    "WHERE p.actual_direction IS NOT NULL AND d.signal NOT IN ('ERROR','NEUTRAL')"
                )
                rows = cur.fetchall()
        finally:
            _put(conn)
        total = correct = 0
        for actual, ens_sig, ds_sig in rows:
            if ens_sig == ds_sig:
                total += 1
                if ens_sig == actual:
                    correct += 1
        return {
            "total_agree": total,
            "correct_agree": correct,
            "accuracy_agree": correct / total if total > 0 else 0.0,
        }

    # ── DeepSeek predictions ──────────────────────────────────────────────────

    def store_deepseek_prediction(self, record: Dict):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO deepseek_predictions "
                    "(window_start, window_end, start_price, signal, confidence, reasoning, "
                    " narrative, free_observation, data_received, data_requests, "
                    " latency_ms, window_count, created_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (window_start) DO NOTHING",
                    (
                        record.get("window_start"), record.get("window_end"),
                        record.get("start_price"), record.get("signal"),
                        record.get("confidence"), record.get("reasoning"),
                        record.get("narrative"), record.get("free_observation"),
                        record.get("data_received"), record.get("data_requests"),
                        record.get("latency_ms"), record.get("window_count"),
                        time.time(),
                    ),
                )
            conn.commit()
        finally:
            _put(conn)

    def resolve_deepseek_prediction(self, window_start: float, end_price: float, actual: str):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT signal, start_price FROM deepseek_predictions WHERE window_start=%s",
                    (window_start,),
                )
                row = cur.fetchone()
                if not row:
                    return
                signal, start_price = row
                correct = actual == signal
                cur.execute(
                    "UPDATE deepseek_predictions "
                    "SET end_price=%s, actual_direction=%s, correct=%s "
                    "WHERE window_start=%s",
                    (end_price, actual, correct, window_start),
                )
            conn.commit()
        finally:
            _put(conn)

    def get_deepseek_accuracy(self) -> Dict:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT signal, correct FROM deepseek_predictions "
                    "WHERE correct IS NOT NULL"
                )
                rows = cur.fetchall()
        finally:
            _put(conn)
        if not rows:
            return {"total": 0, "correct": 0, "accuracy": 0.0, "neutrals": 0, "directional": 0}
        total = len(rows)
        correct = sum(1 for _, c in rows if c)
        neutrals = sum(1 for s, _ in rows if s == "NEUTRAL")
        directional = total - neutrals
        return {
            "total": total, "correct": correct,
            "accuracy": correct / total if total > 0 else 0.0,
            "neutrals": neutrals, "directional": directional,
        }

    def get_recent_deepseek_predictions(self, n: int = 50) -> List[Dict]:
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM deepseek_predictions ORDER BY window_start DESC LIMIT %s",
                    (n,),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            _put(conn)

    def get_audit_records(self, n: int = 500) -> List[Dict]:
        return self.get_recent_deepseek_predictions(n)
