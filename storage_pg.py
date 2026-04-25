"""
PostgreSQL storage layer. DATABASE_URL is required.

Tables created on first run:
  ticks                  — rolling price ticks (capped at 5000)
  predictions            — ensemble predictions + outcomes
  deepseek_predictions   — DeepSeek predictions + outcomes
  score_reset            — single-row timestamp filtering accuracy counters
  events                 — error/flag/suggestion log (hydrated into /errors on boot)

The long-dead file-based `storage.py` backend has been removed; production
has always run on Postgres.
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
                _pool = pool.ThreadedConnectionPool(2, 20, dsn=url)
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

-- Persistent flag / error / suggestion log. Survives deploy so the ERRORS tab
-- and /api/suggestions keep their history across Render restarts.
CREATE TABLE IF NOT EXISTS events (
    id            BIGSERIAL PRIMARY KEY,
    logged_at     DOUBLE PRECISION NOT NULL,
    source        TEXT,              -- e.g. historical_analyst / binance_expert / main_predictor
    kind          TEXT,              -- ERROR | UNAVAILABLE | DATA_GAP | FREE_OBS | SUGGESTION
    message       TEXT,
    bar_time      TEXT,
    bar_num       TEXT,
    window_start  DOUBLE PRECISION,
    raw_excerpt   TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_logged_at ON events (logged_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_kind_logged_at ON events (kind, logged_at DESC);

-- Embedding pipeline audit log. Persisted in Postgres (not the ephemeral
-- container FS) so the UI can read audit history after any Render redeploy.
CREATE TABLE IF NOT EXISTS embedding_audits (
    id             BIGSERIAL PRIMARY KEY,
    logged_at      DOUBLE PRECISION NOT NULL,
    timestamp_str  TEXT NOT NULL,
    elapsed_s      DOUBLE PRECISION,
    audit_signal   TEXT,
    summary        TEXT,
    issues         JSONB,
    suggestions    JSONB,
    full_analysis  TEXT,
    stats          JSONB,
    raw            TEXT
);
CREATE INDEX IF NOT EXISTS idx_embedding_audits_logged_at ON embedding_audits (logged_at DESC);
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
        ("embedding",                  "TEXT"),
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
                sig = vote.get("signal") if isinstance(vote, dict) else vote
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
                # Guard against zero/null prices: if either end is missing the
                # bar can't be classified. The next bar's close will backfill
                # this row via backfill_stuck_correct if possible.
                if not start_price or not end_price or start_price <= 0 or end_price <= 0:
                    return
                if actual is None:
                    # Flat bars (start == end within tick precision) are
                    # neither UP nor DOWN — previously we treated them as UP
                    # because of `>=`, which silently inflated UP accuracy.
                    # Leave actual/correct NULL for true flats.
                    if abs(end_price - start_price) < 1e-6:
                        cur.execute(
                            "UPDATE deepseek_predictions "
                            "SET end_price=%s, actual_direction=NULL, correct=NULL "
                            "WHERE window_start=%s",
                            (end_price, window_start),
                        )
                        conn.commit()
                        return
                    actual = "UP" if end_price > start_price else "DOWN"
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

    def backfill_stuck_correct(self, limit: int = 100) -> Dict:
        """Audit + recompute end_price / actual_direction / correct on the
        last N bars, regardless of whether they already have values set.
        Per user: "no need to look for null bars, just audit and fix if we
        won or lost according to our prediction the last 55 bars".

        For each bar: look up the NEXT consecutive bar's start_price and
        use it as the end_price approximation (5-min tick boundary —
        negligible delta vs intra-bar move). Rows already matching the
        recomputed values are left alone so re-runs are cheap no-ops.

        Returns {scanned, updated, unchanged, skipped_no_next_bar, changes}
        where `changes` is a list of up to 50 before/after diffs for
        visibility into what the audit corrected.
        """
        conn = _conn()
        updated = 0
        unchanged = 0
        no_next = 0
        changes: List[Dict] = []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH recent AS (
                        SELECT window_start, window_end, start_price, signal,
                               end_price, actual_direction, correct
                          FROM deepseek_predictions
                         WHERE signal IN ('UP','DOWN','NEUTRAL')
                         ORDER BY window_start DESC
                         LIMIT %s
                    )
                    SELECT r.window_start, r.start_price, r.signal,
                           r.end_price, r.actual_direction, r.correct,
                           n.start_price AS next_start_price
                      FROM recent r
                      LEFT JOIN deepseek_predictions n
                        ON n.window_start = r.window_end
                    """,
                    (int(limit),),
                )
                rows = cur.fetchall()
                scanned = len(rows)
                for (ws, sp, sig, ep_old, ad_old, cr_old, next_sp) in rows:
                    start_price = float(sp or 0)
                    if next_sp is None or start_price <= 0:
                        no_next += 1
                        continue
                    end_price = float(next_sp)
                    if end_price <= 0:
                        no_next += 1
                        continue
                    # Flat bars (start == end) aren't directional — leave
                    # actual/correct NULL so they don't inflate UP stats.
                    if abs(end_price - start_price) < 1e-6:
                        actual = None
                        correct = None
                    else:
                        actual = "UP" if end_price > start_price else "DOWN"
                        correct = None if sig == "NEUTRAL" else (actual == sig)
                    same_end     = (ep_old is not None) and (abs(float(ep_old) - end_price) < 0.01)
                    same_actual  = (ad_old == actual)
                    same_correct = (cr_old == correct) if correct is not None else (cr_old is None)
                    if same_end and same_actual and same_correct:
                        unchanged += 1
                        continue
                    cur.execute(
                        "UPDATE deepseek_predictions "
                        "SET end_price=%s, actual_direction=%s, correct=%s "
                        "WHERE window_start=%s",
                        (end_price, actual, correct, ws),
                    )
                    updated += 1
                    if len(changes) < 50:
                        changes.append({
                            "window_start": ws,
                            "signal": sig,
                            "before": {
                                "end_price":        float(ep_old) if ep_old is not None else None,
                                "actual_direction": ad_old,
                                "correct":          cr_old,
                            },
                            "after": {
                                "end_price":        end_price,
                                "actual_direction": actual,
                                "correct":          correct,
                            },
                        })
            conn.commit()
        finally:
            _put(conn)
        return {
            "scanned":             scanned,
            "updated":             updated,
            "unchanged":           unchanged,
            "skipped_no_next_bar": no_next,
            "changes":             changes,
        }

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

    def reset_all_tables(self) -> Dict[str, int]:
        """Delete all rows from ticks, predictions, deepseek_predictions. Returns row counts deleted."""
        conn = _conn()
        counts = {}
        try:
            with conn.cursor() as cur:
                for table in ("ticks", "predictions", "deepseek_predictions"):
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cur.fetchone()[0]
                    cur.execute(f"DELETE FROM {table}")
            conn.commit()
        finally:
            _put(conn)
        return counts

    def store_event(self, *, source: str, kind: str, message: str,
                    bar_time: str = "", bar_num: str = "",
                    window_start: float = 0.0, raw_excerpt: str = "",
                    logged_at: float = 0.0) -> None:
        """Persist an error / flag / suggestion so the ERRORS tab survives deploys."""
        import time as _time
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO events (logged_at, source, kind, message, bar_time, bar_num, window_start, raw_excerpt) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (logged_at or _time.time(), source or "", kind or "", (message or "")[:4000],
                     bar_time or "", str(bar_num or ""),
                     float(window_start or 0.0), (raw_excerpt or "")[:4000]),
                )
            conn.commit()
        except Exception as exc:
            logger.warning("store_event failed: %s", exc)
        finally:
            _put(conn)

    def load_recent_events(self, limit: int = 500, kind: Optional[str] = None) -> List[Dict]:
        """Return most-recent events (newest first), optionally filtered by kind."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if kind:
                    cur.execute(
                        "SELECT logged_at, source, kind, message, bar_time, bar_num, window_start, raw_excerpt "
                        "FROM events WHERE kind = %s ORDER BY logged_at DESC LIMIT %s",
                        (kind, limit),
                    )
                else:
                    cur.execute(
                        "SELECT logged_at, source, kind, message, bar_time, bar_num, window_start, raw_excerpt "
                        "FROM events ORDER BY logged_at DESC LIMIT %s",
                        (limit,),
                    )
                rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("load_recent_events failed: %s", exc)
            return []
        finally:
            _put(conn)

    def store_embedding_audit(self, audit: Dict) -> bool:
        """Persist one embedding audit result. Returns True on success."""
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO embedding_audits "
                    "(logged_at, timestamp_str, elapsed_s, audit_signal, summary, "
                    " issues, suggestions, full_analysis, stats, raw) "
                    "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s)",
                    (
                        float(audit.get("timestamp") or time.time()),
                        str(audit.get("timestamp_str") or ""),
                        float(audit.get("elapsed_s") or 0.0),
                        str(audit.get("audit_signal") or ""),
                        str(audit.get("summary") or ""),
                        json.dumps(audit.get("issues") or [], default=str, ensure_ascii=False),
                        json.dumps(audit.get("suggestions") or [], default=str, ensure_ascii=False),
                        str(audit.get("full_analysis") or ""),
                        json.dumps(audit.get("stats") or {}, default=str, ensure_ascii=False),
                        str(audit.get("raw") or ""),
                    ),
                )
            conn.commit()
            return True
        except Exception as exc:
            logger.warning("store_embedding_audit failed: %s", exc)
            return False
        finally:
            _put(conn)

    def load_embedding_audits(self, limit: int = 20) -> List[Dict]:
        """Return recent embedding audits (newest first)."""
        conn = _conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT logged_at, timestamp_str, elapsed_s, audit_signal, summary, "
                    "       issues, suggestions, full_analysis, stats, raw "
                    "FROM embedding_audits ORDER BY logged_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
            result = []
            for r in rows:
                d = dict(r)
                # psycopg2 returns JSONB as dict/list already, but be defensive
                for k in ("issues", "suggestions", "stats"):
                    v = d.get(k)
                    if isinstance(v, str):
                        try:
                            d[k] = json.loads(v)
                        except Exception:
                            pass
                d["timestamp"] = d.pop("logged_at", 0.0)
                result.append(d)
            return result
        except Exception as exc:
            logger.warning("load_embedding_audits failed: %s", exc)
            return []
        finally:
            _put(conn)


def get_storage(**kwargs):
    return StoragePG(**kwargs)
