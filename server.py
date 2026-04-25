"""
FastAPI Server — thin HTTP + WebSocket layer. All prediction logic lives in
engine.py. See the route table by grepping `@app.` — it's short.
"""

import asyncio
import json
import logging
import os
import pathlib
import time
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import (
    config, collector, storage, ensemble, lr_strategy, feature_engine,
    deepseek, binance_klines, current_state, ws_clients,
    STATIC_DIR, CHARTS_DIR,
    _json_safe, _safe_storage, _dashboard_signals_to_preds,
    _pred_for_ws, _run_full_prediction, _run_deepseek,
    generate_bar_chart, run_collector, run_binance_feed,
    run_indicator_refresh, run_prediction_loop, run_embedding_audit_loop, SPECIALIST_KEYS,
    _error_log, load_embedding_audit_log, _trigger_embedding_bootstrap,
)
from signals import fetch_dashboard_signals, extract_signal_directions
from semantic_store import compute_all_indicator_accuracy, compute_dashboard_accuracy, load_all as load_pattern_history

logger = logging.getLogger(__name__)

app = FastAPI(title="Simple Analysis", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Admin session middleware ─────────────────────────────────
# Signed, HttpOnly cookie. The real password is set via ADMIN_PASSWORD on
# Render; there is no code-level fallback — if the env var is missing, login
# is impossible. Cookie is HttpOnly (XSS can't read), SameSite=lax (CSRF
# limited), signed with SESSION_SECRET so the client can't forge it.
from starlette.middleware.sessions import SessionMiddleware
_SESSION_SECRET = os.environ.get("SESSION_SECRET") or "dev-only-change-me-please-via-render-env"
app.add_middleware(SessionMiddleware, secret_key=_SESSION_SECRET,
                   session_cookie="btc_oracle_session", same_site="lax",
                   https_only=True, max_age=60*60*24)  # 24h sessions

_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD") or ""
_MAX_FAILED_ATTEMPTS = 3

# Bump this string (any new unique value) and redeploy to clear the lockout
# after 3 failed attempts. There is NO time-based auto-recovery — a code change
# is the only way back in. Keep the history so we can see past unlocks.
_LOGIN_UNLOCK_TOKEN = "initial-2026-04-24"

from fastapi import HTTPException, Request, Depends
def require_admin(request: Request):
    """Dependency: 401 unless the session cookie carries an admin flag."""
    if not request.session.get("admin"):
        raise HTTPException(status_code=401, detail="admin authentication required")
    return True


def _count_failed_logins_since_unlock() -> int:
    """Counts LOGIN_FAIL events since the most recent LOGIN_UNLOCK matching the
    current code-level token. If no matching unlock exists, writes one (this is
    a fresh unlock after a token bump) and returns 0."""
    events = storage.load_recent_events(limit=1000)
    unlock_at = None
    for ev in events:
        if ev.get("kind") == "LOGIN_UNLOCK" and ev.get("message") == _LOGIN_UNLOCK_TOKEN:
            unlock_at = float(ev.get("logged_at") or 0.0)
            break
    if unlock_at is None:
        storage.store_event(source="admin", kind="LOGIN_UNLOCK",
                            message=_LOGIN_UNLOCK_TOKEN,
                            raw_excerpt="admin login counter reset via code-change token bump")
        return 0
    return sum(1 for ev in events
               if ev.get("kind") == "LOGIN_FAIL"
               and float(ev.get("logged_at") or 0.0) > unlock_at)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/charts", StaticFiles(directory=str(CHARTS_DIR)), name="charts")


# ── Startup ───────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    asyncio.create_task(run_collector())
    asyncio.create_task(run_prediction_loop())
    asyncio.create_task(run_binance_feed())
    asyncio.create_task(run_indicator_refresh())
    asyncio.create_task(run_embedding_audit_loop())
    # Count unembedded bars and surface a DATA_GAP event if any exist.
    # Does NOT mass-embed — operator triggers POST /admin/embed-missing manually.
    _trigger_embedding_bootstrap()


# ── Pydantic models ───────────────────────────────────────────

class BacktestResponse(BaseModel):
    total_predictions: int
    correct_predictions: int
    accuracy: float
    all_time_total: int
    all_time_correct: int
    all_time_accuracy: float
    all_time_neutral: int
    strategy_accuracies: Dict[str, float]

# ── Admin auth endpoints ─────────────────────────────────────

class LoginBody(BaseModel):
    password: str

@app.get("/admin/status")
async def admin_status(request: Request):
    """Cheap check — UI uses this to decide whether to show the admin panel
    or the login form. Safe to call unauthenticated."""
    return {"authenticated": bool(request.session.get("admin"))}

@app.post("/admin/login")
async def admin_login(body: LoginBody, request: Request):
    """Accepts password; on match sets a signed HttpOnly session cookie.
    Hard 3-strike lockout: after 3 failed attempts (across all IPs, persisted
    to the events table) further logins return 403 until the operator bumps
    _LOGIN_UNLOCK_TOKEN in code and redeploys. No time-based recovery."""
    ip = (request.client.host if request.client else "unknown") or "unknown"
    fails = _count_failed_logins_since_unlock()
    if fails >= _MAX_FAILED_ATTEMPTS:
        raise HTTPException(status_code=403,
            detail="admin locked — 3 failed attempts reached. Unlock requires a code change.")

    # Reject empty expected-password (env var not set) so an unconfigured
    # instance can never be logged into — even with an empty body.
    if not _ADMIN_PASSWORD or body.password != _ADMIN_PASSWORD:
        await asyncio.sleep(0.5)   # slow brute-force, obscure timing side-channel
        storage.store_event(source="admin", kind="LOGIN_FAIL",
                            message=f"attempt {fails + 1}/{_MAX_FAILED_ATTEMPTS}",
                            raw_excerpt=f"ip={ip}")
        remaining = _MAX_FAILED_ATTEMPTS - (fails + 1)
        if remaining <= 0:
            raise HTTPException(status_code=403,
                detail="admin locked — 3 failed attempts reached. Unlock requires a code change.")
        raise HTTPException(status_code=401,
            detail=f"incorrect password ({remaining} attempt(s) remaining before permanent lock)")

    request.session["admin"] = True
    return {"ok": True}

@app.post("/admin/logout")
async def admin_logout(request: Request):
    request.session.clear()
    return {"ok": True}


# ── REST endpoints ────────────────────────────────────────────

@app.get("/")
async def serve_dashboard():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/price")
async def get_price():
    return {
        "price":               collector.current_price,
        "tick_count":          collector.tick_count,
        "data_source":         collector.data_source,
        "window_start_price":  current_state["window_start_price"],
    }


@app.get("/deepseek-status")
async def deepseek_status():
    return {
        "pending_deepseek_ready":      current_state.get("pending_deepseek_ready", False),
        "pending_deepseek_prediction": _pred_for_ws(current_state.get("pending_deepseek_prediction")),
        "deepseek_prediction":         _pred_for_ws(current_state.get("deepseek_prediction")),
        "deepseek_enabled":            deepseek is not None,
        "window_start_time":           current_state.get("window_start_time"),
        "specialist_completed_at":     current_state.get("specialist_completed_at"),
        "bar_historical_analysis":     current_state.get("bar_historical_analysis", ""),
        "bar_historical_context":      current_state.get("bar_historical_context", ""),
        "bar_binance_expert":          current_state.get("bar_binance_expert", {}),
        "service_unavailable":         current_state.get("service_unavailable", False),
        "service_unavailable_reason":  current_state.get("service_unavailable_reason", ""),
    }

@app.get("/backtest", response_model=BacktestResponse, dependencies=[Depends(require_admin)])
async def get_backtest():
    total, correct, accuracy = _safe_storage(storage.get_rolling_accuracy, config.rolling_window_size, default=(0, 0, 0.0))
    at_total, at_correct, at_accuracy, at_neutral = _safe_storage(storage.get_total_accuracy, default=(0, 0, 0.0, 0))
    strategy_acc = _safe_storage(storage.get_strategy_rolling_accuracy, default={})
    return BacktestResponse(
        total_predictions=total, correct_predictions=correct, accuracy=accuracy,
        all_time_total=at_total, all_time_correct=at_correct, all_time_accuracy=at_accuracy,
        all_time_neutral=at_neutral,
        strategy_accuracies=strategy_acc,
    )


@app.get("/predictions/recent", dependencies=[Depends(require_admin)])
async def get_recent_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_predictions, n, default=[])


@app.get("/api/proxy/coinalyze")
async def proxy_coinalyze():
    import aiohttp
    try:
        url = f"https://api.coinalyze.net/v1/funding-rate?symbols=BTCUSDT_PERP.A&api_key={config.coinalyze_key}"
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                return await resp.json(content_type=None)
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/proxy/okx-liquidations")
async def proxy_okx_liquidations():
    import aiohttp
    try:
        url = "https://www.okx.com/api/v5/public/liquidation-orders?instType=SWAP&mgnMode=cross&instId=BTC-USDT-SWAP&state=filled&limit=100"
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                return await resp.json(content_type=None)
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/weights")
async def get_weights():
    from strategies import accuracy_to_label
    weights   = ensemble.get_weights()
    acc_stats = _safe_storage(storage.get_strategy_accuracy_full, 100, default={})
    result    = {}
    for name, w in weights.items():
        stats    = acc_stats.get(name, {})
        accuracy = stats.get("accuracy", None)
        total    = stats.get("total", 0)
        result[name] = {
            "weight":   w,
            "accuracy": round(accuracy * 100, 1) if accuracy is not None else None,
            "correct":  stats.get("correct", 0),
            "total":    total,
            "label":    accuracy_to_label(accuracy or 0.5, total) if total > 0 else "LEARNING",
        }
    for name, stats in acc_stats.items():
        if name not in result:
            accuracy = stats.get("accuracy", 0.5); total = stats.get("total", 0)
            result[name] = {
                "weight": 1.0, "accuracy": round(accuracy * 100, 1),
                "correct": stats.get("correct", 0), "total": total,
                "label": accuracy_to_label(accuracy, total),
            }
    return result


@app.get("/deepseek/accuracy")
async def get_deepseek_accuracy():
    acc = _safe_storage(storage.get_deepseek_accuracy, default={"total": 0, "correct": 0, "accuracy": 0.0})
    return {**acc, "current_prediction": current_state.get("deepseek_prediction"), "enabled": config.deepseek_enabled}


@app.get("/accuracy/agree")
async def get_agree_accuracy():
    return _safe_storage(storage.get_agree_accuracy, default={})


@app.get("/backend", dependencies=[Depends(require_admin)])
async def get_backend_snapshot():
    return {
        "snapshot": current_state.get("backend_snapshot") or {},
        "deepseek": current_state.get("deepseek_prediction") or {},
    }


@app.get("/deepseek/predictions", dependencies=[Depends(require_admin)])
async def get_deepseek_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])


_HEAVY_FIELDS = {"full_prompt", "raw_response"}

@app.get("/deepseek/predictions/{window_start}", dependencies=[Depends(require_admin)])
async def get_deepseek_prediction_detail(window_start: float):
    """Single record with all fields including full_prompt and raw_response."""
    docs = _safe_storage(storage.get_recent_deepseek_predictions, 9999, default=[])
    for doc in docs:
        if doc.get("window_start") == window_start:
            return doc
    return {}


@app.post("/admin/backfill-correct", dependencies=[Depends(require_admin)])
async def backfill_correct(limit: int = 100):
    """One-shot backfill for bars stuck with correct=NULL (e.g. from the
    pm_odds_open crash before commit 5dc942f). For each stuck bar, uses the
    NEXT consecutive bar's start_price as the end_price approximation
    (5-min tick boundary — inter-bar delta is negligible vs intra-bar move).
    Idempotent: only updates bars where correct IS NULL AND end_price IS
    NULL. Returns counts."""
    limit = max(1, min(int(limit or 100), 500))
    return _safe_storage(storage.backfill_stuck_correct, limit, default={"error": "storage_unavailable"})


@app.get("/historical-analysis/{window_start}", dependencies=[Depends(require_admin)])
async def get_historical_analysis(window_start: float):
    """
    Comprehensive audit of prediction pipeline for a single window.
    Shows: embeddings query → re-ranking → final prompt → deepseek response.
    """
    docs = _safe_storage(storage.get_recent_deepseek_predictions, 9999, default=[])
    doc = None
    for d in docs:
        if d.get("window_start") == window_start:
            doc = d
            break

    if not doc:
        return {"status": "not_found", "window_start": window_start}

    # Parse JSON fields
    try:
        strategy_snap = json.loads(doc.get("strategy_snapshot") or "{}")
        indicators_snap = json.loads(doc.get("indicators_snapshot") or "{}")
        dashboard_snap = json.loads(doc.get("dashboard_signals_snapshot") or "{}")
    except:
        strategy_snap = {}
        indicators_snap = {}
        dashboard_snap = {}

    # Extract data flow from request/response
    data_received = doc.get("data_received", "").split("\n") if doc.get("data_received") else []
    data_requests = doc.get("data_requests", "").split("\n") if doc.get("data_requests") else []

    return {
        "status": "ok",
        "window_start": doc.get("window_start"),
        "window_end": doc.get("window_end"),
        "start_price": doc.get("start_price"),
        "end_price": doc.get("end_price"),
        "actual_direction": doc.get("actual_direction"),
        "correct": doc.get("correct"),
        "prediction": {
            "signal": doc.get("signal"),
            "confidence": doc.get("confidence"),
            "reasoning": doc.get("reasoning", ""),
            "narrative": doc.get("narrative", ""),
            "free_observation": doc.get("free_observation", ""),
        },
        "input_data": {
            "strategies": strategy_snap,
            "indicators": indicators_snap,
            "dashboard_signals": dashboard_snap,
        },
        "pipeline": {
            "data_requests": [r.strip() for r in data_requests if r.strip()],
            "data_received": [r.strip() for r in data_received if r.strip()],
            "latency_ms": doc.get("latency_ms", 0),
            "window_count": doc.get("window_count", 0),
        },
        "prompting": {
            "full_prompt": doc.get("full_prompt", ""),
            "raw_response": doc.get("raw_response", ""),
        },
        "metadata": {
            "chart_path": doc.get("chart_path", ""),
            "postmortem": doc.get("postmortem", ""),
        },
    }


@app.get("/deepseek/source-history", dependencies=[Depends(require_admin)])
async def get_deepseek_source_history(n: int = 20):
    docs    = _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])
    results = []
    for doc in docs:
        for field in ("dashboard_signals_snapshot", "strategy_snapshot", "indicators_snapshot"):
            raw = doc.get(field)
            if raw and isinstance(raw, str):
                try: doc[field] = json.loads(raw)
                except: pass
        results.append({
            "window_start":               doc.get("window_start"),
            "window_end":                 doc.get("window_end"),
            "start_price":                doc.get("start_price"),
            "signal":                     doc.get("signal"),
            "confidence":                 doc.get("confidence"),
            "reasoning":                  doc.get("reasoning", ""),
            "narrative":                  doc.get("narrative", ""),
            "data_received":              doc.get("data_received", ""),
            "data_requests":              doc.get("data_requests", ""),
            "free_observation":           doc.get("free_observation", ""),
            "full_prompt":                doc.get("full_prompt", ""),
            "raw_response":               doc.get("raw_response", ""),
            "chart_path":                 doc.get("chart_path", ""),
            "postmortem":                 doc.get("postmortem", ""),
            "latency_ms":                 doc.get("latency_ms", 0),
            "window_count":               doc.get("window_count", 0),
            "actual_direction":           doc.get("actual_direction"),
            "correct":                    doc.get("correct"),
            "end_price":                  doc.get("end_price"),
            "dashboard_signals_snapshot": doc.get("dashboard_signals_snapshot", {}),
            "strategy_snapshot":          doc.get("strategy_snapshot", {}),
        })
    return results


@app.get("/api/timings", dependencies=[Depends(require_admin)])
async def get_timings():
    """Return per-stage pipeline timings for the current bar + recent history.

    Lets the user observe which stage (cohere embed, pgvector, rerank, deepseek
    calls) is burning time and whether a bar overran the 5-min close.
    """
    from engine import current_state
    bt = current_state.get("bar_timings") or {}
    hist = current_state.get("timings_history") or []
    return {
        "current":  bt,
        "history":  list(reversed(hist[-30:])),  # newest first, cap to 30
        "history_total": len(hist),
    }


@app.get("/api/embedding-audit", dependencies=[Depends(require_admin)])
async def get_embedding_audit():
    """Return embedding audit log (last N audits) + current stats."""
    log = _safe_storage(load_embedding_audit_log, n=20, default=[])
    return {
        "audit_log": log,
        "last_audit": log[0] if log else None,
        "last_audit_time": log[0].get("timestamp_str") if log else None,
    }


@app.get("/api/inspect/last-deepseek", dependencies=[Depends(require_admin)])
async def inspect_last_deepseek():
    """Return raw text of the most recent files sent to / received from DeepSeek.

    Exposes what DeepSeek actually sees so the pipeline can be inspected
    without waiting for the 4-hour embedding audit.
    """
    specialists_root = pathlib.Path(__file__).parent / "specialists"
    targets = {
        "historical_analyst.last_sent":     "historical_analyst/last_sent.txt",
        "historical_analyst.last_prompt":   "historical_analyst/last_prompt.txt",
        "historical_analyst.last_response": "historical_analyst/last_response.txt",
        "main_predictor.last_prompt":       "main_predictor/last_prompt.txt",
        "main_predictor.last_response":     "main_predictor/last_response.txt",
        "unified_analyst.last_sent":        "unified_analyst/last_sent.txt",
        "unified_analyst.last_prompt":      "unified_analyst/last_prompt.txt",
        "unified_analyst.last_response":    "unified_analyst/last_response.txt",
        "binance_expert.last_response":     "binance_expert/last_response.txt",
        "embedding_audit.last_raw":         "embedding_audit/last_raw.txt",
    }
    files = {}
    for key, rel in targets.items():
        p = specialists_root / rel
        try:
            st = p.stat()
            files[key] = {
                "path": rel,
                "size_bytes": st.st_size,
                "mtime": st.st_mtime,
                "mtime_str": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(st.st_mtime)),
                "content": p.read_text(encoding="utf-8", errors="replace"),
                "exists": True,
            }
        except FileNotFoundError:
            files[key] = {"path": rel, "exists": False, "content": "", "size_bytes": 0, "mtime": None, "mtime_str": None}
        except Exception as exc:
            files[key] = {"path": rel, "exists": False, "content": f"(read error: {exc})", "size_bytes": 0, "mtime": None, "mtime_str": None}
    return {"server_time": time.time(), "files": files}


@app.get("/accuracy/all")
async def get_all_accuracy(n: int = 100):
    # Public — powers the SOURCES tab's "Prediction Accuracy — All Sources"
    # panel which anyone can view alongside the live microstructure signals.
    # The data is aggregate signal-performance stats, no user PII.
    try:
      from strategies import accuracy_to_label
      limit     = n if n > 0 else None
      all_stats = compute_all_indicator_accuracy(limit)
      wts       = ensemble.get_weights()
    except Exception as exc:
      import logging; logging.getLogger("server").error("/accuracy/all failed: %s", exc, exc_info=True)
      return {"ai": [], "strategies": [], "specialists": [], "microstructure": [], "error": str(exc)}
    all_stats.pop("best_indicator", None)

    def _row(key, name, stats, weight=None):
        wins        = stats.get("wins", stats.get("correct", 0))
        total       = stats.get("total", 0)
        directional = stats.get("directional", total)
        acc         = stats.get("accuracy", 0.5)
        label       = accuracy_to_label(acc, directional) if directional >= 3 else "LEARNING"
        return {"key": key, "name": name, "accuracy": round(acc * 100, 1),
                "correct": wins, "total": total, "label": label,
                "weight": round(weight, 3) if weight is not None else None}

    STRAT_NAMES = {
        "rsi":"RSI","macd":"MACD","stochastic":"Stochastic","ema_cross":"EMA Fast",
        "supertrend":"Supertrend","adx":"ADX","alligator":"Alligator","acc_dist":"Acc/Dist",
        "dow_theory":"Dow Theory","fib_pullback":"Fibonacci","harmonic":"Harmonic",
        "vwap":"AVWAP","ml_logistic":"Lin Reg",
    }
    SPEC_NAMES = {
        "spec:dow_theory":"DOW Theory","spec:fib_pullback":"FIB Pullback",
        "spec:alligator":"ALG Alligator","spec:acc_dist":"ACD Acc/Dist","spec:harmonic":"HAR Harmonic",
    }
    DASH_NAMES = {
        "dash:order_book":"Order Book","dash:long_short":"Long/Short",
        "dash:taker_flow":"Taker Flow","dash:oi_funding":"OI + Funding",
        "dash:liquidations":"Liquidations","dash:fear_greed":"Fear & Greed",
        "dash:mempool":"Mempool","dash:coinalyze":"Coinalyze",
        "dash:deribit_dvol":"Deribit DVOL","dash:coingecko":"CoinGecko",
    }

    ai           = [_row(k, n, all_stats[k], wts.get(k)) for k, n in [("deepseek","DeepSeek AI"),("ensemble","Math Ensemble")] if k in all_stats]
    strategies   = [_row(f"strat:{k}", n, all_stats[f"strat:{k}"], wts.get(k)) for k, n in STRAT_NAMES.items() if f"strat:{k}" in all_stats]
    specialists  = [_row(k, n, all_stats[k]) for k, n in SPEC_NAMES.items() if k in all_stats]
    microstructure = [_row(k, n, all_stats[k]) for k, n in DASH_NAMES.items() if k in all_stats]

    for lst in (ai, strategies, specialists, microstructure):
        lst.sort(key=lambda r: (r["total"] >= 3, r["accuracy"]), reverse=True)

    return {"ai": ai, "strategies": strategies, "specialists": specialists, "microstructure": microstructure}


@app.get("/best-indicator", dependencies=[Depends(require_admin)])
async def get_best_indicator(n: int = 0):
    limit = n if n > 0 else None
    try:
        data = compute_all_indicator_accuracy(limit)
    except Exception as exc:
        return {"error": str(exc)}
    best   = data.pop("best_indicator", None)
    ranked = sorted(
        [{"name": k, **v} for k, v in data.items()],
        key=lambda x: (x["accuracy"], x["total"]), reverse=True,
    )
    try:
        pattern_record_count = len(load_pattern_history())
    except Exception:
        pattern_record_count = -1
    return {"best_indicator": best, "ranked": ranked, "total_indicators": len(ranked), "pattern_record_count": pattern_record_count}


@app.get("/errors", dependencies=[Depends(require_admin)])
async def get_errors():
    from datetime import datetime, timezone
    errors = []
    for e in reversed(_error_log):
        dt = datetime.fromtimestamp(e["logged_at"], tz=timezone.utc)
        errors.append({**e, "logged_at_str": dt.strftime("%Y-%m-%d %H:%M:%S UTC")})
    return {"errors": errors, "count": len(errors)}


@app.get("/api/suggestions", dependencies=[Depends(require_admin)])
async def get_suggestions(limit: int = 30):
    """System-improvement suggestions harvested from postmortems + specialist files.

    Pulls three sources:
      1. Postmortems on recent bars (LESSON_NAME / LESSON_RULE / LESSON_EFFECT blocks).
         These are the system's own self-derived rules for avoiding repeat mistakes.
      2. historical_analyst/suggestions.txt — appended by run_historical_analyst
      3. unified_analyst/suggestions.txt   — appended by run_specialists

    The ERRORS tab renders this so the user can see *what the system thinks it
    should do differently*, not just that something went wrong.
    """
    import re
    from datetime import datetime, timezone
    lessons   = []
    hist_sugg = []
    uni_sugg  = []

    # 1) Postmortem lessons
    try:
        from storage_pg import _conn, _put
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT window_start, signal, correct, postmortem FROM deepseek_predictions "
                    "WHERE postmortem IS NOT NULL AND LENGTH(postmortem) > 200 "
                    "ORDER BY window_start DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        finally:
            _put(conn)

        seen_names = set()
        for ws, sig, correct, pm in rows:
            name = rule = effect = preconds = root_cause = error_class = ""
            for line in pm.splitlines():
                s = line.strip()
                if s.startswith("LESSON_NAME:"):    name       = s.split(":",1)[1].strip()
                elif s.startswith("LESSON_RULE:"):  rule       = s.split(":",1)[1].strip()
                elif s.startswith("LESSON_EFFECT:"):effect     = s.split(":",1)[1].strip()
                elif s.startswith("LESSON_PRECONDITIONS:"): preconds = s.split(":",1)[1].strip()
                elif s.startswith("ROOT_CAUSE:"):  root_cause  = s.split(":",1)[1].strip()
                elif s.startswith("ERROR_CLASS:"): error_class = s.split(":",1)[1].strip()
            if not name or name.upper() in ("NONE", "N/A", ""):
                continue
            if name in seen_names:
                continue
            seen_names.add(name)
            lessons.append({
                "window_start":  float(ws),
                "window_start_str": datetime.fromtimestamp(float(ws), tz=timezone.utc)
                                        .strftime("%Y-%m-%d %H:%M UTC"),
                "signal":        sig,
                "correct":       correct,
                "name":          name,
                "rule":          rule,
                "effect":        effect,
                "preconditions": preconds,
                "root_cause":    root_cause,
                "error_class":   error_class,
            })
    except Exception as exc:
        logger.warning("postmortem lesson fetch failed: %s", exc)

    # 2+3) Specialist SUGGESTION events. Previously read from ephemeral
    # specialists/*/suggestions.txt files; those get wiped on each Render
    # deploy. Now backed by the `events` Postgres table (store_event writes
    # every SUGGESTION flag) so history survives.
    hist_sugg: list = []
    uni_sugg:  list = []
    try:
        rows = storage.load_recent_events(limit=200, kind="SUGGESTION")
        for r in rows:
            src = (r.get("source") or "").lower()
            msg = r.get("message") or ""
            bar = r.get("bar_time") or ""
            line = f"[{bar}] {msg}" if bar else msg
            if "historical_analyst" in src and len(hist_sugg) < 20:
                hist_sugg.append(line)
            elif "unified_analyst" in src and len(uni_sugg) < 20:
                uni_sugg.append(line)
    except Exception as exc:
        logger.warning("SUGGESTION event fetch failed: %s", exc)

    return {
        "lessons":                       lessons,
        "historical_analyst_suggestions": hist_sugg,
        "unified_analyst_suggestions":    uni_sugg,
        "counts": {
            "lessons":             len(lessons),
            "historical_analyst":  len(hist_sugg),
            "unified_analyst":     len(uni_sugg),
        },
    }


@app.post("/force-predict", dependencies=[Depends(require_admin)])
async def force_predict():
    prices = collector.get_prices(400)
    if len(prices) < 30:
        return {"status": "error", "detail": "Not enough price data (need 30, have %d)" % len(prices)}

    saved = {k: current_state.get(k) for k in
             ("window_start_time","window_start_price","prediction","ensemble_prediction",
              "deepseek_prediction","strategies","specialist_completed_at")}
    try:
        pred, strategy_preds, _, _, window_start_time, window_start_price = \
            await _run_full_prediction(prices, is_force=True)
        current_state["window_start_time"]  = saved["window_start_time"]
        current_state["window_start_price"] = saved["window_start_price"] or window_start_price
        logger.info("Force predict (dry run) | %s (%.1f%%)", pred["signal"], pred["confidence"] * 100)
        return {"status": "ok", "signal": pred["signal"], "confidence": pred["confidence"],
                "dry_run": True, "note": "Preview only — not saved, timer unchanged"}
    except Exception as exc:
        for k, v in saved.items():
            current_state[k] = v
        logger.error("Force predict failed: %s", exc)
        return {"status": "error", "detail": str(exc)}


@app.post("/admin/reset", dependencies=[Depends(require_admin)])
async def reset_database():
    try:
        counts = storage.reset_all_tables()
        return {"status": "ok", "deleted": counts}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.post("/admin/embed-missing", dependencies=[Depends(require_admin)])
async def embed_missing(limit: int = 20):
    """Manually Cohere-embed unembedded RESOLVED bars, up to `limit` at a time.

    The old mass-bootstrap was removed because a coverage-check bug was
    silently re-embedding every bar on every deploy. This replaces it with
    a deliberate, rate-limited, capped operation the operator triggers when
    the ERRORS tab shows unembedded bars.

    Returns counts of: requested, attempted, succeeded, failed, and the
    window_starts that were processed.
    """
    from ai import embed_text, _bar_embed_text
    from semantic_store import embedded_window_starts, load_all, store_embedding
    if not config.cohere_api_key:
        return {"status": "error", "detail": "COHERE_API_KEY not configured"}
    if limit <= 0 or limit > 100:
        return {"status": "error", "detail": "limit must be 1..100"}
    try:
        embedded = embedded_window_starts()
        all_bars = load_all()
        resolved = [r for r in all_bars if r.get("actual_direction")]
        missing  = [r for r in resolved if r.get("window_start") not in embedded][:limit]
        if not missing:
            return {"status": "ok", "requested": limit, "attempted": 0,
                    "succeeded": 0, "failed": 0, "message": "no missing embeddings"}
        succeeded, failed, done_ws = 0, 0, []
        for bar in missing:
            ws = bar.get("window_start")
            try:
                text = _bar_embed_text(bar)
                vec  = await embed_text(config.cohere_api_key, text, input_type="search_document")
                store_embedding(ws, vec)
                succeeded += 1
                done_ws.append(ws)
                await asyncio.sleep(0.1)   # ~10/sec rate limit
            except Exception as exc:
                failed += 1
                logger.warning("embed_missing: bar %.0f failed: %s", ws or 0, exc)
        return {
            "status": "ok", "requested": limit, "attempted": len(missing),
            "succeeded": succeeded, "failed": failed,
            "window_starts": done_ws,
            "remaining_missing": max(0, len(resolved) - len(embedded) - succeeded),
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


# Removed 3 one-shot migration endpoints that had already been run and were no
# longer referenced by anything:
#   /admin/fix-neutral-correct       — one-time NEUTRAL.correct=NULL migration
#   /admin/backfill-pattern-history  — replayed deepseek_predictions into pattern_history
#   /admin/embed-pattern-history     — Cohere-embedded all un-embedded bars
# Restore from git history if you ever need to rerun them.


# ── WebSocket ─────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    logger.info("WS client connected (%d total)", len(ws_clients))
    try:
        while True:
            if current_state["price"] is not None:
                try:
                    # Extract dashboard_signals from the backend_snapshot. This is
                    # the live metric source the frontend's metric() lookup reads
                    # for rendering condition-pill "now" values (bid/ask depth,
                    # whale flows, funding, OI, liquidations, basis, CVD, skew).
                    # Previously only exposed via /backend (admin-only) so non-admin
                    # users got backendSnap=null and every non-price/non-strategy
                    # pill rendered "source unavailable" — bullets never fired.
                    _bs = current_state.get("backend_snapshot") or {}
                    _dash = _bs.get("dashboard_signals") if isinstance(_bs, dict) else None
                    payload = _json_safe({
                        "type":                        "tick",
                        "price":                       current_state["price"],
                        "window_start_price":          current_state["window_start_price"],
                        "window_start_time":           current_state["window_start_time"],
                        "prediction":                  current_state["prediction"],
                        "ensemble_prediction":         current_state.get("ensemble_prediction"),
                        "strategies":                  current_state["strategies"],
                        "deepseek_prediction":         _pred_for_ws(current_state.get("deepseek_prediction")),
                        "pending_deepseek_prediction": _pred_for_ws(current_state.get("pending_deepseek_prediction")),
                        "pending_deepseek_ready":      current_state.get("pending_deepseek_ready", False),
                        "agree_accuracy":              current_state.get("agree_accuracy"),
                        "specialist_completed_at":     current_state.get("specialist_completed_at"),
                        "bar_historical_analysis":     current_state.get("bar_historical_analysis", ""),
                        "bar_historical_context":      current_state.get("bar_historical_context", ""),
                        "bar_binance_expert":          current_state.get("bar_binance_expert", {}),
                        "service_unavailable":         current_state.get("service_unavailable", False),
                        "service_unavailable_reason":  current_state.get("service_unavailable_reason", ""),
                        "dashboard_signals":           _dash,
                    })
                    await websocket.send_json(payload)
                except Exception as ws_exc:
                    logger.warning("WS send failed: %r", ws_exc)
                    break
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)
        logger.info("WS client disconnected (%d remaining)", len(ws_clients))


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.api_host, port=config.api_port)
