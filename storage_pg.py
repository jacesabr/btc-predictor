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
CREATE TABLE IF NOT EXISTS score_reset (
    id        INTEGER PRIMARY KEY DEFAULT 1,
    reset_at  DOUBLE PRECISION NOT NULL DEFAULT 0,
    reset_note TEXT
);
INSERT INTO score_reset (id, reset_at) VALUES (1, 0) ON CONFLICT DO NOTHING;

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
    window_start              DOUBLE PRECISION PRIMARY KEY,
    window_end                DOUBLE PRECISION,
    start_price               DOUBLE PRECISION,
    end_price                 DOUBLE PRECISION,
    signal                    TEXT,
    confidence                DOUBLE PRECISION,
    reasoning                 TEXT,
    narrative                 TEXT,
    free_observation          TEXT,
    data_received             TEXT,
    data_requests             TEXT,
    latency_ms                INTEGER,
    window_count              INTEGER,
    actual_direction          TEXT,
    correct                   BOOLEAN,
    created_at                DOUBLE PRECISION,
    chart_path                TEXT,
    raw_response              TEXT,
    full_prompt               TEXT,
    strategy_snapshot         TEXT,
    indicators_snapshot       TEXT,
    dashboard_signals_snapshot TEXT,
    postmortem                TEXT,
    polymarket_url            TEXT
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
    migrate_deepseek_columns()


def migrate_deepseek_columns():
    """Add columns introduced after initial deploy — safe to run repeatedly (IF NOT EXISTS)."""
    new_cols = [
        ("chart_path",                 "TEXT"),
        ("raw_response",               "TEXT"),
        ("full_prompt",                "TEXT"),
        ("strategy_snapshot",          "TEXT"),
        ("indicators_snapshot",        "TEXT"),
        ("dashboard_signals_snapshot", "TEXT"),
        ("postmortem",                 "TEXT"),
        ("polymarket_url",             "TEXT"),
    ]
    conn = _conn()
    try:
        with conn.cursor() as cur:
            for col, typ in new_cols:
                cur.execute(
                    f"ALTER TABLE deepseek_predictions ADD COLUMN IF NOT EXISTS {col} {typ}"
                )
        conn.commit()
        logger.info("deepseek_predictions column migration complete")
    finally:
        _put(conn)


def migrate_neutral_correct():
    """One-time fix: NEUTRAL predictions were wrongly scored correct=0 (loss). Set to NULL."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE predictions SET correct = NULL WHERE signal = 'NEUTRAL' AND correct IS NOT NULL"
            )
            p_count = cur.rowcount
            cur.execute(
                "UPDATE deepseek_predictions SET correct = NULL WHERE signal = 'NEUTRAL' AND correct IS NOT NULL"
            )
            ds_count = cur.rowcount
        conn.commit()
        if p_count or ds_count:
            logger.info("Neutral migration: patched %d predictions, %d deepseek_predictions", p_count, ds_count)
    finally:
        _put(conn)


def get_reset_at() -> float:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT reset_at FROM score_reset WHERE id=1")
            row = cur.fetchone()
            return float(row[0]) if row else 0.0
    finally:
        _put(conn)


def set_reset_at(ts: float, note: str = "") -> None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE score_reset SET reset_at=%s, reset_note=%s WHERE id=1",
                (ts, note),
            )
        conn.commit()
    finally:
        _put(conn)


# ── Storage class (same public interface as storage.py) ───────────────────────

class StoragePG:
    def __init__(self, uri: str = "", db_name: str = "btc_predictor"):
        self._lock = threading.Lock()
        init_schema()
        migrate_neutral_correct()
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
                    (float(window_start), float(window_end), float(start_price), signal, float(confidence),
                     json.dumps(strategy_votes),
                     float(market_odds) if market_odds is not None else None,
                     float(ev) if ev is not None else None,
                     time.time()),
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
                # NEUTRAL is an abstention — not scored as correct or wrong
                correct = None if signal == "NEUTRAL" else (1 if actual == signal else 0)
                cur.execute(
                    "UPDATE predictions SET end_price=%s, actual_direction=%s, correct=%s "
                    "WHERE window_start=%s",
                    (end_price, actual, correct, window_start),
                )
            conn.commit()
        finally:
            _put(conn)

    def get_rolling_accuracy(self, n: int = 12) -> Tuple[int, int, float]:
        cutoff = get_reset_at()
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT correct FROM predictions "
                    "WHERE correct IS NOT NULL AND signal != 'NEUTRAL' AND window_start >= %s "
                    "ORDER BY window_start DESC LIMIT %s",
                    (cutoff, n),
                )
                rows = cur.fetchall()
            if not rows:
                return 0, 0, 0.0
            total = len(rows)
            correct = sum(r[0] for r in rows)
            return total, correct, correct / total
        finally:
            _put(conn)

    def get_total_accuracy(self) -> Tuple[int, int, float, int]:
        cutoff = get_reset_at()
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*), SUM(correct) FROM predictions "
                    "WHERE correct IS NOT NULL AND signal != 'NEUTRAL' AND window_start >= %s",
                    (cutoff,),
                )
                row = cur.fetchone()
                cur.execute(
                    "SELECT COUNT(*) FROM predictions "
                    "WHERE signal = 'NEUTRAL' AND window_start >= %s",
                    (cutoff,),
                )
                neutral_row = cur.fetchone()
            total   = row[0] or 0
            correct = int(row[1] or 0)
            neutral = neutral_row[0] or 0
            return total, correct, correct / total if total > 0 else 0.0, neutral
        finally:
            _put(conn)

    def get_strategy_rolling_accuracy(self, n: int = 20) -> Dict[str, float]:
        cutoff = get_reset_at()
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT strategy_votes, actual_direction FROM predictions "
                    "WHERE actual_direction IS NOT NULL AND window_start >= %s "
                    "ORDER BY window_start DESC LIMIT %s",
                    (cutoff, n),
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
        cutoff = get_reset_at()
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT strategy_votes, actual_direction FROM predictions "
                    "WHERE actual_direction IS NOT NULL AND window_start >= %s "
                    "ORDER BY window_start DESC LIMIT %s",
                    (cutoff, n),
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
                    "WHERE p.actual_direction IS NOT NULL "
                    "AND p.signal NOT IN ('ERROR','NEUTRAL') "
                    "AND d.signal NOT IN ('ERROR','NEUTRAL')"
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

    def store_deepseek_prediction(self, record: Dict = None, **kwargs):
        if record is None:
            record = kwargs
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO deepseek_predictions "
                    "(window_start, window_end, start_price, signal, confidence, reasoning, "
                    " narrative, free_observation, data_received, data_requests, "
                    " latency_ms, window_count, created_at, "
                    " chart_path, raw_response, full_prompt, strategy_snapshot, "
                    " indicators_snapshot, dashboard_signals_snapshot, polymarket_url) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (window_start) DO NOTHING",
                    (
                        float(record.get("window_start") or 0),
                        float(record.get("window_end") or 0),
                        float(record.get("start_price") or 0),
                        record.get("signal"),
                        float(record.get("confidence") or 0),
                        record.get("reasoning"),
                        record.get("narrative"),
                        record.get("free_observation"),
                        record.get("data_received"),
                        record.get("data_requests"),
                        record.get("latency_ms"),
                        record.get("window_count"),
                        time.time(),
                        record.get("chart_path", ""),
                        record.get("raw_response", ""),
                        record.get("full_prompt", ""),
                        record.get("strategy_snapshot", ""),
                        record.get("indicators_snapshot", ""),
                        record.get("dashboard_signals_snapshot", ""),
                        record.get("polymarket_url", ""),
                    ),
                )
            conn.commit()
        finally:
            _put(conn)

    def store_postmortem(self, window_start: float, postmortem: str):
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE deepseek_predictions SET postmortem=%s WHERE window_start=%s",
                    (postmortem, float(window_start)),
                )
            conn.commit()
        finally:
            _put(conn)

    def resolve_deepseek_prediction(self, window_start: float, end_price: float, actual: str = None):
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
                if actual is None:
                    actual = "UP" if end_price >= start_price else "DOWN"
                correct = None if signal == "NEUTRAL" else (actual == signal)
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
        cutoff = get_reset_at()
        conn = _conn()
        try:
            with conn.cursor() as cur:
                # Directional predictions only (correct is set for UP/DOWN, NULL for NEUTRAL)
                cur.execute(
                    "SELECT correct FROM deepseek_predictions "
                    "WHERE correct IS NOT NULL AND signal != 'NEUTRAL' AND window_start >= %s",
                    (cutoff,),
                )
                directional_rows = cur.fetchall()
                # Neutral count is separate — correct=NULL so excluded above
                cur.execute(
                    "SELECT COUNT(*) FROM deepseek_predictions "
                    "WHERE signal = 'NEUTRAL' AND window_start >= %s",
                    (cutoff,),
                )
                neutrals = cur.fetchone()[0] or 0
        finally:
            _put(conn)
        total = len(directional_rows)
        correct = sum(1 for (c,) in directional_rows if c)
        return {
            "total": total, "correct": correct,
            "accuracy": correct / total if total > 0 else 0.0,
            "neutrals": neutrals, "directional": total,
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

    def get_neutral_analysis(self) -> Dict:
        """Return stats on NEUTRAL DeepSeek predictions to help tune the neutral threshold."""
        cutoff = get_reset_at()
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT window_start, actual_direction, confidence, reasoning "
                    "FROM deepseek_predictions "
                    "WHERE signal = 'NEUTRAL' AND actual_direction IS NOT NULL AND window_start >= %s "
                    "ORDER BY window_start DESC",
                    (cutoff,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        finally:
            _put(conn)
        total = len(rows)
        if not total:
            return {
                "total": 0, "market_went_up": 0, "market_went_down": 0,
                "pct_up": 0.0, "pct_down": 0.0,
                "would_have_won_if_traded_up": 0, "would_have_won_if_traded_down": 0,
                "records": [],
            }
        up   = sum(1 for r in rows if r["actual_direction"] == "UP")
        down = total - up
        records_out = [
            {"window_start": r["window_start"], "actual_direction": r["actual_direction"],
             "confidence": r.get("confidence", 0),
             "reasoning": (r.get("reasoning") or "")[:200]}
            for r in rows
        ]
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

    def get_recent_predictions(self, n: int = 50) -> List[Dict]:
        cutoff = get_reset_at()
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT window_start, window_end, start_price, end_price, signal, "
                    "confidence, actual_direction, correct, market_odds, ev, strategy_votes "
                    "FROM predictions WHERE window_start >= %s "
                    "ORDER BY window_start DESC LIMIT %s",
                    (cutoff, n),
                )
                rows = []
                for r in cur.fetchall():
                    d = dict(r)
                    if isinstance(d.get("strategy_votes"), str):
                        try:
                            d["strategy_votes"] = json.loads(d["strategy_votes"])
                        except Exception:
                            d["strategy_votes"] = {}
                    rows.append(d)
                return rows
        finally:
            _put(conn)

    def get_prediction_history_with_indicators(self, n: int = 50) -> List[Dict]:
        return self.get_recent_predictions(n)

    def store_accuracy_snapshot(self, window_start: float, snapshot: Dict) -> None:
        pass
