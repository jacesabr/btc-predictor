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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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
    run_indicator_refresh, run_prediction_loop, SPECIALIST_KEYS,
)
from signals import fetch_dashboard_signals, extract_signal_directions
from strategies import get_all_predictions, calculate_ev, required_accuracy_for_odds
from semantic_store import compute_all_indicator_accuracy, compute_dashboard_accuracy
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
    from storage import _read_ndjson, _DATA_DIR
    try:
        all_ticks = _read_ndjson(_DATA_DIR / "ticks.ndjson")
    except Exception as exc:
        logger.warning("Candles: could not read ticks: %s", exc)
        return []
    if not all_ticks:
        return []
    all_ticks.sort(key=lambda t: t["timestamp"])
    all_ticks = all_ticks[-(limit * resolution * 2):]
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


@app.get("/accuracy/all")
async def get_all_accuracy(n: int = 100):
    from strategies import accuracy_to_label
    limit     = n if n > 0 else None
    all_stats = compute_all_indicator_accuracy(limit)
    wts       = ensemble.get_weights()
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
    return {"best_indicator": best, "ranked": ranked, "total_indicators": len(ranked)}


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
    from storage import _DATA_DIR, _rewrite_ndjson, _read_ndjson
    counts = {}
    try:
        for name in ("ticks", "predictions", "deepseek_predictions"):
            path = _DATA_DIR / f"{name}.ndjson"
            if path.exists():
                counts[name] = len(_read_ndjson(path))
                _rewrite_ndjson(path, [])
            else:
                counts[name] = 0
        return {"status": "ok", "deleted": counts}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


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
                    await websocket.send_json({
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
                    })
                except Exception as ws_exc:
                    logger.warning("WS send failed: %s", ws_exc)
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
