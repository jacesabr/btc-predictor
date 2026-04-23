"""
FastAPI Server
==============
Thin HTTP + WebSocket layer. All prediction logic lives in engine.py.

Endpoints:
  GET  /                     — serve dashboard HTML
  GET  /price                — current price + window info
  GET  /predict              — instant ensemble prediction (on-demand)
  GET  /deepseek-status      — lightweight DeepSeek state poll
  GET  /backtest             — rolling + all-time accuracy
  GET  /predictions/recent   — recent resolved predictions
  GET  /candles              — tick-aggregated OHLC
  GET  /candles/binance      — cached 1m Binance klines
  GET  /polymarket           — Polymarket market state
  GET  /weights              — ensemble weights + accuracy
  GET  /deepseek/accuracy    — DeepSeek accuracy stats
  GET  /deepseek/predictions — recent DeepSeek predictions
  GET  /deepseek/source-history — full data-source audit per prediction
  GET  /audit                — full prediction audit table
  GET  /audit/export         — download audit as JSON
  GET  /accuracy/all         — structured accuracy leaderboard
  GET  /accuracy/agree       — accuracy when ensemble + DeepSeek agree
  GET  /best-indicator       — win/loss leaderboard for every tracked signal
  GET  /backend              — last prediction pipeline snapshot
  POST /ev                   — expected value calculator
  POST /weights/update       — trigger ensemble weight refresh
  POST /force-predict        — dry-run prediction (no DB write)
  POST /admin/reset          — clear all local data files
  WS   /ws                   — live price + strategy state stream
"""

import asyncio
import io
import json
import logging
import os
import pathlib
import time
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import (
    config, collector, storage, ensemble, lr_strategy, feature_engine,
    polymarket_feed, deepseek, binance_klines, current_state, ws_clients,
    STATIC_DIR, CHARTS_DIR,
    _json_safe, _safe_storage, _dashboard_signals_to_preds,
    _pred_for_ws, _run_full_prediction, _run_deepseek,
    generate_bar_chart, run_collector, run_binance_feed,
    run_indicator_refresh, run_prediction_loop, run_embedding_audit_loop, SPECIALIST_KEYS,
    _error_log, load_embedding_audit_log, _trigger_embedding_bootstrap,
)
from signals import fetch_dashboard_signals, extract_signal_directions
from strategies import get_all_predictions, calculate_ev, required_accuracy_for_odds
from semantic_store import compute_all_indicator_accuracy, compute_dashboard_accuracy, load_all as load_pattern_history
from ai import SPECIALIST_KEYS as _SPEC_KEYS

logger = logging.getLogger(__name__)

app = FastAPI(title="BTC Oracle Predictor", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/charts", StaticFiles(directory=str(CHARTS_DIR)), name="charts")


# ── Startup ───────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    asyncio.create_task(run_collector())
    asyncio.create_task(run_prediction_loop())
    asyncio.create_task(polymarket_feed.run())
    asyncio.create_task(run_binance_feed())
    asyncio.create_task(run_indicator_refresh())
    asyncio.create_task(run_embedding_audit_loop())


# ── Pydantic models ───────────────────────────────────────────

class PredictionResponse(BaseModel):
    signal: str
    confidence: float
    up_probability: float
    bullish_count: int
    bearish_count: int
    strategies: Dict
    ev: Optional[Dict] = None

class BacktestResponse(BaseModel):
    total_predictions: int
    correct_predictions: int
    accuracy: float
    all_time_total: int
    all_time_correct: int
    all_time_accuracy: float
    all_time_neutral: int
    strategy_accuracies: Dict[str, float]

class EVRequest(BaseModel):
    market_odds: float
    model_probability: Optional[float] = None


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
        "bar_binance_expert":          current_state.get("bar_binance_expert", {}),   # <-- ADD THIS LINE
        "service_unavailable":         current_state.get("service_unavailable", False),
        "service_unavailable_reason":  current_state.get("service_unavailable_reason", ""),
    }

@app.get("/predict", response_model=PredictionResponse)
async def get_prediction():
    prices = collector.get_prices(400)
    if len(prices) < 30:
        return PredictionResponse(
            signal="WAIT", confidence=0, up_probability=0.5,
            bullish_count=0, bearish_count=0, strategies={},
        )
    poly_prob      = polymarket_feed.implied_prob if polymarket_feed.is_live else None
    strategy_preds = get_all_predictions(prices, ohlcv=list(binance_klines), polymarket_prob=poly_prob)
    strategy_preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=list(binance_klines))
    result = ensemble.predict(strategy_preds)
    return PredictionResponse(
        signal=result["signal"], confidence=float(result["confidence"]),
        up_probability=float(result["up_probability"]),
        bullish_count=int(result["bullish_count"]), bearish_count=int(result["bearish_count"]),
        strategies=_json_safe(strategy_preds),
    )


@app.post("/ev")
async def calculate_expected_value(req: EVRequest):
    model_prob = req.model_probability
    if model_prob is None and current_state["prediction"]:
        model_prob = current_state["prediction"].get("confidence", 0.5)
    if model_prob is None:
        model_prob = 0.5
    result = calculate_ev(model_prob, req.market_odds)
    return {
        "ev":                   result.expected_value,
        "edge":                 result.edge,
        "implied_probability":  result.implied_probability,
        "model_probability":    result.model_probability,
        "kelly_fraction":       result.kelly_fraction,
        "signal":               result.signal,
        "reasoning":            result.reasoning,
        "min_accuracy_needed":  required_accuracy_for_odds(req.market_odds),
    }


@app.get("/backtest", response_model=BacktestResponse)
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


@app.get("/predictions/recent")
async def get_recent_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_predictions, n, default=[])


@app.get("/candles")
async def get_candles(resolution: int = 60, limit: int = 200):
    try:
        all_ticks = storage.get_ticks_raw(limit=limit * resolution * 2)
    except Exception as exc:
        logger.warning("Candles: could not read ticks: %s", exc)
        return []
    if not all_ticks:
        return []
    candles = {}
    for doc in all_ticks:
        bucket = int(doc["timestamp"] // resolution) * resolution
        p = doc["mid_price"]
        if bucket not in candles:
            candles[bucket] = {"time": bucket, "open": p, "high": p, "low": p, "close": p}
        else:
            c = candles[bucket]
            c["high"] = max(c["high"], p); c["low"] = min(c["low"], p); c["close"] = p
    return sorted(candles.values(), key=lambda c: c["time"])[-limit:]


@app.get("/candles/binance")
async def get_binance_candles():
    if not binance_klines:
        return []
    return [
        {"time": int(k[0] / 1000), "open": float(k[1]),
         "high": float(k[2]), "low": float(k[3]), "close": float(k[4])}
        for k in binance_klines
    ]


@app.get("/polymarket")
async def get_polymarket():
    return polymarket_feed.to_dict()



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


@app.post("/weights/update")
async def update_weights():
    strategy_acc = _safe_storage(storage.get_strategy_rolling_accuracy, default={})
    if strategy_acc:
        ensemble.update_weights(strategy_acc)
    return {"status": "updated", "weights": ensemble.get_weights()}


@app.get("/deepseek/accuracy")
async def get_deepseek_accuracy():
    acc = _safe_storage(storage.get_deepseek_accuracy, default={"total": 0, "correct": 0, "accuracy": 0.0})
    return {**acc, "current_prediction": current_state.get("deepseek_prediction"), "enabled": config.deepseek_enabled}


@app.get("/accuracy/agree")
async def get_agree_accuracy():
    return _safe_storage(storage.get_agree_accuracy, default={})


@app.get("/backend")
async def get_backend_snapshot():
    return {
        "snapshot": current_state.get("backend_snapshot") or {},
        "deepseek": current_state.get("deepseek_prediction") or {},
    }


@app.get("/deepseek/predictions")
async def get_deepseek_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])


_HEAVY_FIELDS = {"full_prompt", "raw_response"}

@app.get("/deepseek/predictions/list")
async def get_deepseek_predictions_list(n: int = 300):
    """Lean list for history UI — strips full_prompt and raw_response to keep payload small.
    Those fields are fetched on demand via /deepseek/predictions/{window_start}."""
    docs = _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])
    return [{k: v for k, v in doc.items() if k not in _HEAVY_FIELDS} for doc in docs]


@app.get("/deepseek/predictions/{window_start}")
async def get_deepseek_prediction_detail(window_start: float):
    """Single record with all fields including full_prompt and raw_response."""
    docs = _safe_storage(storage.get_recent_deepseek_predictions, 9999, default=[])
    for doc in docs:
        if doc.get("window_start") == window_start:
            return doc
    return {}


@app.get("/history/all")
async def get_all_history():
    """All DeepSeek prediction summaries (lean — no prompt/raw) for history overview."""
    return _safe_storage(storage.get_all_deepseek_summaries, default=[])


@app.get("/historical-analysis/{window_start}")
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


@app.post("/admin/clean-incomplete")
async def admin_clean_incomplete():
    """Remove unresolved bars (no actual_direction) from all storage tables."""
    result = _safe_storage(storage.clean_incomplete_records, default={})
    if result and result.get("removed_window_starts"):
        from semantic_store import clean_incomplete_windows
        removed_ph = clean_incomplete_windows(set(result["removed_window_starts"]))
        result["removed_pattern_history"] = removed_ph
    return result or {}


@app.get("/deepseek/source-history")
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


@app.get("/neutral-analysis")
async def get_neutral_analysis():
    """Stats on NEUTRAL predictions — use to tune the neutral confidence threshold."""
    return _safe_storage(storage.get_neutral_analysis, default={
        "total": 0, "market_went_up": 0, "market_went_down": 0,
        "pct_up": 0.0, "pct_down": 0.0,
        "would_have_won_if_traded_up": 0, "would_have_won_if_traded_down": 0,
        "records": [],
    })


@app.get("/audit")
async def get_audit(n: int = 500):
    records  = _safe_storage(storage.get_audit_records, n, default=[])
    neutrals = [r for r in records if r.get("signal") == "NEUTRAL"]
    resolved = [r for r in records if r.get("correct") is not None]
    wins     = sum(1 for r in resolved if r["correct"])
    return {
        "summary": {
            "total_predictions":    len(records),
            "resolved_predictions": len(resolved),
            "neutrals":             len(neutrals),
            "wins":                 wins,
            "losses":               len(resolved) - wins,
            "win_rate": round(wins / len(resolved), 4) if resolved else None,
        },
        "records": records,
    }


@app.get("/audit/export")
async def export_audit(n: int = 500):
    records  = _safe_storage(storage.get_audit_records, n, default=[])
    neutrals = [r for r in records if r.get("signal") == "NEUTRAL"]
    resolved = [r for r in records if r.get("correct") is not None]
    wins     = sum(1 for r in resolved if r["correct"])
    payload  = {
        "exported_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "summary": {
            "total_predictions": len(records), "resolved_predictions": len(resolved),
            "neutrals": len(neutrals),
            "wins": wins, "losses": len(resolved) - wins,
            "win_rate": round(wins / len(resolved), 4) if resolved else None,
        },
        "records": records,
    }
    body     = json.dumps(payload, indent=2, default=str)
    filename = f"audit_{int(time.time())}.json"
    return StreamingResponse(
        io.BytesIO(body.encode()), media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/timings")
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


@app.get("/api/embedding-audit")
async def get_embedding_audit():
    """Return embedding audit log (last N audits) + current stats."""
    log = _safe_storage(load_embedding_audit_log, n=20, default=[])
    return {
        "audit_log": log,
        "last_audit": log[0] if log else None,
        "last_audit_time": log[0].get("timestamp_str") if log else None,
    }


@app.post("/api/embedding-audit/run")
async def trigger_embedding_audit():
    """Trigger an embedding audit immediately (non-blocking)."""
    from engine import run_embedding_audit
    history = _safe_storage(load_pattern_history, default=[])
    asyncio.create_task(run_embedding_audit(config.deepseek_api_key, history))
    return {"status": "audit triggered", "will_appear_in_log": True}


@app.get("/api/inspect/last-deepseek")
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
        "vwap":"AVWAP","polymarket":"Crowd","ml_logistic":"Lin Reg",
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


@app.get("/best-indicator")
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


@app.get("/errors")
async def get_errors():
    from datetime import datetime, timezone
    errors = []
    for e in reversed(_error_log):
        dt = datetime.fromtimestamp(e["logged_at"], tz=timezone.utc)
        errors.append({**e, "logged_at_str": dt.strftime("%Y-%m-%d %H:%M:%S UTC")})
    return {"errors": errors, "count": len(errors)}


@app.get("/api/suggestions")
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


@app.post("/force-predict")
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


@app.post("/reset-scores")
async def reset_scores():
    import time as _time
    now = _time.time()
    note = f"Score reset {__import__('datetime').datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} via dashboard"
    if os.environ.get("DATABASE_URL"):
        from storage_pg import set_reset_at
        set_reset_at(now, note)
    else:
        import json as _json
        reset_path = pathlib.Path(__file__).parent / "score_reset.json"
        reset_path.write_text(_json.dumps({"reset_at": now, "reset_note": note}))
    return {"status": "ok", "reset_at": now, "note": note}


@app.post("/admin/fix-neutral-correct")
async def fix_neutral_correct():
    """One-time migration: set correct=NULL for all NEUTRAL deepseek predictions."""
    try:
        from storage_pg import _conn, _put
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE deepseek_predictions SET correct = NULL "
                    "WHERE signal = 'NEUTRAL' AND correct IS NOT NULL"
                )
                updated = cur.rowcount
            conn.commit()
        finally:
            _put(conn)
        return {"status": "ok", "rows_updated": updated}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.post("/admin/reset")
async def reset_database():
    try:
        counts = storage.reset_all_tables()
        return {"status": "ok", "deleted": counts}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.post("/admin/backfill-pattern-history")
async def backfill_pattern_history():
    """
    Read all resolved bars from deepseek_predictions and populate pattern_history.
    Safe to call multiple times — uses INSERT ... ON CONFLICT DO NOTHING.
    After populating, trigger background embedding via Cohere for bars with actual_direction.
    """
    from semantic_store import append_resolved_window as _append_ph
    from storage_pg import _conn as _pg_conn, _put as _pg_put
    import json as _json

    inserted = 0
    skipped  = 0
    errors   = 0

    conn = _pg_conn()
    try:
        with conn.cursor(cursor_factory=__import__("psycopg2.extras", fromlist=["RealDictCursor"]).RealDictCursor) as cur:
            cur.execute("""
                SELECT window_start, window_end, start_price, end_price,
                       signal, confidence, reasoning, narrative, free_observation,
                       window_count, actual_direction, correct,
                       strategy_snapshot, indicators_snapshot, dashboard_signals_snapshot,
                       full_prompt
                FROM deepseek_predictions
                WHERE actual_direction IS NOT NULL
                ORDER BY window_start ASC
            """)
            rows = cur.fetchall()
    finally:
        _pg_put(conn)

    from signals import extract_signal_directions as _extract_dirs2
    for row in rows:
        try:
            sv     = _json.loads(row["strategy_snapshot"] or "{}")
            iv     = _json.loads(row["indicators_snapshot"] or "{}")
            dv_raw = _json.loads(row["dashboard_signals_snapshot"] or "{}")
            dv     = _extract_dirs2(dv_raw) if dv_raw else {}
            _append_ph(
                window_start       = float(row["window_start"]),
                actual_direction   = row["actual_direction"] or "",
                start_price        = float(row["start_price"] or 0),
                strategy_votes     = sv,
                indicators         = iv,
                window_end         = float(row["window_end"] or 0),
                end_price          = float(row["end_price"] or 0),
                deepseek_signal    = row["signal"] or "",
                deepseek_conf      = int(float(row["confidence"] or 0) * 100) if (row["confidence"] or 0) <= 1 else int(row["confidence"] or 0),
                deepseek_correct   = bool(row["correct"]) if row["correct"] is not None else None,
                deepseek_reasoning = row["reasoning"] or "",
                deepseek_narrative = row["narrative"] or "",
                deepseek_free_obs  = row["free_observation"] or "",
                dashboard_signals_raw = dv,
                full_prompt        = row["full_prompt"] or "",
                window_count       = int(row["window_count"] or 0),
            )
            inserted += 1
        except Exception as exc:
            logging.getLogger("server").warning("backfill row error ws=%.0f: %s", row.get("window_start", 0), exc)
            errors += 1

    return {"status": "ok", "inserted": inserted, "skipped": skipped, "errors": errors,
            "note": "Call /admin/embed-pattern-history to Cohere-embed the backfilled bars"}


@app.post("/admin/embed-pattern-history")
async def embed_pattern_history(background_tasks: BackgroundTasks):
    """
    Background task: Cohere-embed all pattern_history bars that have no embedding yet.
    Stores REAL[] vectors so cosine similarity search works without pgvector.
    """
    from semantic_store import load_all as _load_ph
    from semantic_store_pg import store_embedding as _store_emb
    from ai import embed_text as _embed, _bar_embed_text

    if not config.cohere_api_key:
        return {"status": "error", "detail": "COHERE_API_KEY not set"}

    async def _run():
        from storage_pg import _conn as _pg_conn, _put as _pg_put
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT window_start FROM pattern_history WHERE embedding IS NULL ORDER BY window_start ASC")
                to_embed = [r[0] for r in cur.fetchall()]
        finally:
            _pg_put(conn)

        records = {r["window_start"]: r for r in _load_ph(10000)}
        done = 0
        for ws in to_embed:
            rec = records.get(ws)
            if not rec:
                continue
            try:
                text = _bar_embed_text(rec)
                vec  = await _embed(config.cohere_api_key, text, input_type="search_document")
                _store_emb(ws, vec)
                done += 1
                if done % 10 == 0:
                    logging.getLogger("server").info("embed-pattern-history: %d/%d done", done, len(to_embed))
            except Exception as exc:
                logging.getLogger("server").warning("embed bar %.0f failed: %s", ws, exc)

        logging.getLogger("server").info("embed-pattern-history complete: %d bars embedded", done)

    background_tasks.add_task(_run)
    return {"status": "started", "message": "Embedding runs in background — check server logs"}


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
                        "polymarket":                  polymarket_feed.to_dict(),
                        "specialist_completed_at":     current_state.get("specialist_completed_at"),
                        "bar_historical_analysis":     current_state.get("bar_historical_analysis", ""),
                        "bar_historical_context":      current_state.get("bar_historical_context", ""),
                        "bar_binance_expert":          current_state.get("bar_binance_expert", {}),
                        "service_unavailable":         current_state.get("service_unavailable", False),
                        "service_unavailable_reason":  current_state.get("service_unavailable_reason", ""),
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
