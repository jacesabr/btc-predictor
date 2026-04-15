"""
FastAPI Backend Server

Serves the dashboard (/) and prediction API on the same port.
WebSocket /ws streams live price + strategy state every second.
"""

import asyncio
import json
import time
import logging
import pathlib
from typing import Dict, List, Optional

import aiohttp

import numpy as np
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — must be before pyplot import
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import Config
from data.collector import BinanceCollector
from data.dashboard_signals import fetch_dashboard_signals, extract_signal_directions
from data.features import FeatureEngine
from data.storage_file import Storage
from deepseek.predictor import DeepSeekPredictor
from deepseek.specialists import run_specialists, SPECIALIST_KEYS
from deepseek.pattern_analyst import run_pattern_analyst
from deepseek.bar_insight_analyst import run_bar_insight_analyst
from data.pattern_history import (
    append_resolved_window, load_all as load_pattern_history,
    compute_dashboard_accuracy, compute_all_indicator_accuracy,
    compute_bar_insight_accuracy,
)
from strategies.base import get_all_predictions
from strategies.ensemble import EnsemblePredictor
from strategies.ml_models import LinearRegressionChannel
from utils.ev_calculator import calculate_ev, required_accuracy_for_odds
from utils.polymarket import PolymarketFeed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _json_safe(obj):
    """Recursively convert numpy/non-serializable types to Python natives."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

STATIC_DIR  = pathlib.Path(__file__).parent.parent / "static"
CHARTS_DIR  = pathlib.Path(__file__).parent.parent / "charts"
CHARTS_DIR.mkdir(exist_ok=True)

# Keys that are set once at bar open and must not be overwritten by the refresh loop
_PRESERVED_PRED_PREFIXES = ("dash:",)


def _dashboard_signals_to_preds(dashboard_signals: Optional[Dict]) -> Dict:
    """
    Convert live dashboard signal directions to strategy_pred votes for the ensemble.
    Only UP/DOWN signals cast a vote; NEUTRAL signals are skipped.
    Returned keys use the 'dash:' prefix so they're distinguishable from rule-based strategies.
    """
    if not dashboard_signals:
        return {}
    directions = extract_signal_directions(dashboard_signals)
    return {
        f"dash:{name}": {
            "signal":     direction,
            "confidence": 0.65,
            "reasoning":  f"microstructure:{name}",
        }
        for name, direction in directions.items()
        if direction in ("UP", "DOWN")
    }


def generate_bar_chart(klines: List, window_start: float, signal: str, confidence: int) -> Optional[str]:
    """
    Generate a candlestick chart PNG from the last 30 1m klines ending at window_start.
    Returns the saved file path, or None on failure.
    klines items: [open_time_ms, open, high, low, close, volume, ...]
    """
    try:
        # Filter to candles that closed at or before window_start
        ws_ms = window_start * 1000
        relevant = [k for k in klines if float(k[0]) <= ws_ms]
        if not relevant:
            return None
        bars = relevant[-30:]  # last 30 minutes of 1m candles

        opens  = [float(b[1]) for b in bars]
        highs  = [float(b[2]) for b in bars]
        lows   = [float(b[3]) for b in bars]
        closes = [float(b[4]) for b in bars]
        xs     = list(range(len(bars)))

        fig, ax = plt.subplots(figsize=(9, 3.2), facecolor="#F9F8F6")
        ax.set_facecolor("#F9F8F6")

        for i, (o, h, l, c) in enumerate(zip(opens, highs, lows, closes)):
            color = "#15803D" if c >= o else "#B91C1C"
            ax.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=1)
            body_bot = min(o, c)
            body_h   = max(abs(c - o), 0.1)
            rect = mpatches.FancyBboxPatch(
                (i - 0.35, body_bot), 0.7, body_h,
                boxstyle="square,pad=0",
                facecolor=color, edgecolor=color, linewidth=0, zorder=2,
            )
            ax.add_patch(rect)

        # Mark the prediction bar (last candle = window open)
        sig_color = "#15803D" if signal == "UP" else "#B91C1C"
        sig_arrow = "▲" if signal == "UP" else "▼"
        ax.axvline(x=len(bars)-1, color=sig_color, linewidth=1.2, linestyle="--", alpha=0.7, zorder=3)

        # Labels
        ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(window_start))
        ax.set_title(
            f"BTC/USDT 1m  ·  {ts}  ·  Prediction: {sig_arrow} {signal} {confidence}%",
            fontsize=9, color="#1A1A1A", fontfamily="monospace", pad=5,
        )
        ax.set_xlim(-1, len(bars))
        ax.tick_params(axis="x", labelbottom=False, length=0)
        ax.tick_params(axis="y", labelsize=7, colors="#6B6866")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"${v:,.0f}"))
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_color("#E6E4DF")
        ax.spines["left"].set_color("#E6E4DF")
        ax.grid(axis="y", color="#E6E4DF", linewidth=0.5, linestyle="-")

        fname = f"chart_{int(window_start)}.png"
        fpath = CHARTS_DIR / fname
        fig.tight_layout(pad=0.6)
        fig.savefig(str(fpath), dpi=110, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return str(fpath)
    except Exception as exc:
        logger.warning("Chart generation failed: %s", exc)
        return None


# =============================================================================
# App Setup
# =============================================================================

app = FastAPI(title="BTC Oracle Predictor", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/charts", StaticFiles(directory=str(CHARTS_DIR)), name="charts")

config = Config()
collector = BinanceCollector(
    poll_interval=config.poll_interval_seconds,
)
storage = Storage(uri=config.mongodb_uri, db_name=config.mongodb_db)
logger.info("MongoDB storage connected: %s / %s", config.mongodb_uri.split("@")[-1], config.mongodb_db)
ensemble = EnsemblePredictor(config.initial_weights)
lr_strategy = LinearRegressionChannel()
feature_engine = FeatureEngine()
polymarket_feed = PolymarketFeed(poll_interval=1.0)
deepseek = DeepSeekPredictor(
    api_key=config.deepseek_api_key,
    model=config.deepseek_vision_model if config.deepseek_use_vision else config.deepseek_model,
) if config.deepseek_enabled else None

ws_clients: set = set()
binance_klines: List = []   # cached 1m OHLCV from Binance public API

current_state: Dict = {
    "price": None,
    "window_start_price": None,
    "window_start_time": None,
    "prediction": None,                   # primary prediction (ensemble during bar)
    "ensemble_prediction": None,          # always the math ensemble vote
    "strategies": {},
    "deepseek_prediction": None,          # PREVIOUS bar's result — revealed at bar close
    "pending_deepseek_prediction": None,  # CURRENT bar's result — held until bar closes
    "pending_deepseek_ready": False,      # True once current-bar DeepSeek has returned
    "agree_accuracy": None,
    "specialist_completed_at": None,      # unix ts when specialist batch last finished
    "backend_snapshot": None,             # full data pipeline snapshot for the Backend tab
    # Stored at bar open so _resolve_window can write a complete bar record
    "bar_specialist_signals": {},         # specialist signals from unified analyst
    "bar_creative_edge": "",              # creative edge from unified analyst
    "bar_pattern_analysis": "",          # text from pattern analyst
    "bar_insight_text": "",              # full text from bar insight analyst
    "bar_insight_signal": "",            # structured CALL from bar insight analyst
}


# =============================================================================
# Pydantic Models
# =============================================================================

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
    strategy_accuracies: Dict[str, float]


class EVRequest(BaseModel):
    market_odds: float
    model_probability: Optional[float] = None


# =============================================================================
# REST Endpoints
# =============================================================================

@app.get("/")
async def serve_dashboard():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/price")
async def get_price():
    return {
        "price": collector.current_price,
        "tick_count": collector.tick_count,
        "data_source": collector.data_source,
        "window_start_price": current_state["window_start_price"],
    }


@app.get("/deepseek-status")
async def deepseek_status():
    """Lightweight endpoint — frontend polls this to get DeepSeek state without WS."""
    return {
        "pending_deepseek_ready":      current_state.get("pending_deepseek_ready", False),
        "pending_deepseek_prediction": _pred_for_ws(current_state.get("pending_deepseek_prediction")),
        "deepseek_prediction":         _pred_for_ws(current_state.get("deepseek_prediction")),
        "deepseek_enabled":            deepseek is not None,
        "window_start_time":           current_state.get("window_start_time"),
        "specialist_completed_at":     current_state.get("specialist_completed_at"),
    }


@app.get("/predict", response_model=PredictionResponse)
async def get_prediction():
    prices = collector.get_prices(400)
    if len(prices) < 30:
        return PredictionResponse(
            signal="WAIT", confidence=0, up_probability=0.5,
            bullish_count=0, bearish_count=0, strategies={},
        )

    poly_prob = polymarket_feed.implied_prob if polymarket_feed.is_live else None
    strategy_preds = get_all_predictions(prices, ohlcv=list(binance_klines), polymarket_prob=poly_prob)

    strategy_preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=list(binance_klines))

    result = ensemble.predict(strategy_preds)

    return PredictionResponse(
        signal=result["signal"],
        confidence=result["confidence"],
        up_probability=result["up_probability"],
        bullish_count=result["bullish_count"],
        bearish_count=result["bearish_count"],
        strategies=strategy_preds,
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
        "ev": result.expected_value,
        "edge": result.edge,
        "implied_probability": result.implied_probability,
        "model_probability": result.model_probability,
        "kelly_fraction": result.kelly_fraction,
        "signal": result.signal,
        "reasoning": result.reasoning,
        "min_accuracy_needed": required_accuracy_for_odds(req.market_odds),
    }


@app.get("/backtest", response_model=BacktestResponse)
async def get_backtest():
    total, correct, accuracy = _safe_storage(storage.get_rolling_accuracy, config.rolling_window_size, default=(0, 0, 0.0))
    at_total, at_correct, at_accuracy = _safe_storage(storage.get_total_accuracy, default=(0, 0, 0.0))
    strategy_acc = _safe_storage(storage.get_strategy_rolling_accuracy, default={})
    return BacktestResponse(
        total_predictions=total,
        correct_predictions=correct,
        accuracy=accuracy,
        all_time_total=at_total,
        all_time_correct=at_correct,
        all_time_accuracy=at_accuracy,
        strategy_accuracies=strategy_acc,
    )


@app.get("/predictions/recent")
async def get_recent_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_predictions, n, default=[])


@app.get("/candles")
async def get_candles(resolution: int = 60, limit: int = 200):
    """
    Aggregate raw ticks into OHLC candles.
    resolution: candle size in seconds (default 60 = 1-minute candles)
    limit: max number of candles to return
    """
    try:
        docs = list(storage.db.ticks.find(
            {}, {"timestamp": 1, "mid_price": 1, "_id": 0}
        ).sort("timestamp", -1).limit(limit * resolution * 2))
    except Exception as exc:
        logger.warning("Candles query failed (MongoDB down?): %s", exc)
        return []

    if not docs:
        return []

    docs = list(reversed(docs))
    candles = {}
    for doc in docs:
        bucket = int(doc["timestamp"] // resolution) * resolution
        p = doc["mid_price"]
        if bucket not in candles:
            candles[bucket] = {"time": bucket, "open": p, "high": p, "low": p, "close": p}
        else:
            c = candles[bucket]
            c["high"] = max(c["high"], p)
            c["low"]  = min(c["low"],  p)
            c["close"] = p

    sorted_candles = sorted(candles.values(), key=lambda c: c["time"])
    return sorted_candles[-limit:]


@app.get("/candles/binance")
async def get_binance_candles():
    """Return cached Binance 1m OHLCV as LightweightCharts candlestick data."""
    if not binance_klines:
        return []
    return [
        {
            "time":  int(k[0] / 1000),   # open_time ms → epoch seconds
            "open":  float(k[1]),
            "high":  float(k[2]),
            "low":   float(k[3]),
            "close": float(k[4]),
        }
        for k in binance_klines
    ]


@app.get("/polymarket")
async def get_polymarket():
    return polymarket_feed.to_dict()


# ── Microstructure proxies (CORS-blocked APIs) ──────────────────────────────

@app.get("/api/proxy/coinapi")
async def proxy_coinapi():
    """Proxy CoinAPI BTC/USD rate — browser CORS is blocked without a key."""
    try:
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                "https://rest.coinapi.io/v1/exchangerate/BTC/USD",
                headers={"X-CoinAPI-Key": config.coinapi_key},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                return await resp.json(content_type=None)
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/proxy/coinalyze")
async def proxy_coinalyze():
    """Proxy Coinalyze cross-exchange funding rate — requires API key."""
    try:
        url = (f"https://api.coinalyze.net/v1/funding-rate"
               f"?symbols=BTCUSDT_PERP.A&api_key={config.coinalyze_key}")
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                return await resp.json(content_type=None)
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/weights")
async def get_weights():
    from strategies.ensemble import accuracy_to_label
    weights = ensemble.get_weights()
    acc_stats = _safe_storage(storage.get_strategy_accuracy_full, 100, default={})
    result = {}
    for name, w in weights.items():
        stats = acc_stats.get(name, {})
        accuracy = stats.get("accuracy", None)
        total    = stats.get("total", 0)
        result[name] = {
            "weight":   w,
            "accuracy": round(accuracy * 100, 1) if accuracy is not None else None,
            "correct":  stats.get("correct", 0),
            "total":    total,
            "label":    accuracy_to_label(accuracy or 0.5, total) if total > 0 else "LEARNING",
        }
    # Also include indicators with accuracy data but no weight yet
    for name, stats in acc_stats.items():
        if name not in result:
            accuracy = stats.get("accuracy", 0.5)
            total    = stats.get("total", 0)
            result[name] = {
                "weight":   1.0,
                "accuracy": round(accuracy * 100, 1),
                "correct":  stats.get("correct", 0),
                "total":    total,
                "label":    accuracy_to_label(accuracy, total),
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
    return {
        **acc,
        "current_prediction": current_state.get("deepseek_prediction"),
        "enabled": config.deepseek_enabled,
    }


@app.get("/accuracy/agree")
async def get_agree_accuracy():
    """Accuracy when ensemble and DeepSeek agree on the same signal."""
    return _safe_storage(storage.get_agree_accuracy, default={})


@app.get("/backend")
async def get_backend_snapshot():
    """Full data pipeline snapshot for the last prediction window — used by the Backend tab."""
    return {
        "snapshot": current_state.get("backend_snapshot") or {},
        "deepseek": current_state.get("deepseek_prediction") or {},
    }


@app.get("/deepseek/predictions")
async def get_deepseek_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])


@app.get("/deepseek/source-history")
async def get_deepseek_source_history(n: int = 20):
    """
    Returns recent DeepSeek predictions with their full source data snapshots:
    - dashboard_signals_snapshot: all 10 microstructure sources as fetched at bar open
    - strategy_snapshot: ensemble strategy votes
    - indicators_snapshot: all technical indicator values
    - DeepSeek reasoning / narrative / data_received
    Used by the Source History tab to show what data was retrieved and how DeepSeek used it.
    """
    docs = _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])
    results = []
    for doc in docs:
        # Parse JSON snapshots
        for field in ("dashboard_signals_snapshot", "strategy_snapshot", "indicators_snapshot"):
            raw = doc.get(field)
            if raw and isinstance(raw, str):
                try:
                    doc[field] = json.loads(raw)
                except Exception:
                    pass
        # Only return fields needed by the UI (omit full_prompt/raw_response to keep payload small)
        results.append({
            "window_start":              doc.get("window_start"),
            "window_end":                doc.get("window_end"),
            "start_price":               doc.get("start_price"),
            "signal":                    doc.get("signal"),
            "confidence":                doc.get("confidence"),
            "reasoning":                 doc.get("reasoning", ""),
            "narrative":                 doc.get("narrative", ""),
            "data_received":             doc.get("data_received", ""),
            "data_requests":             doc.get("data_requests", ""),
            "free_observation":          doc.get("free_observation", ""),
            "latency_ms":                doc.get("latency_ms", 0),
            "window_count":              doc.get("window_count", 0),
            "actual_direction":          doc.get("actual_direction"),
            "correct":                   doc.get("correct"),
            "end_price":                 doc.get("end_price"),
            "dashboard_signals_snapshot": doc.get("dashboard_signals_snapshot", {}),
            "strategy_snapshot":         doc.get("strategy_snapshot", {}),
        })
    return results


@app.get("/audit")
async def get_audit(n: int = 500):
    """
    Returns an audit table of all DeepSeek predictions with every piece of
    evidence used to make each call:
      - polymarket_url  : the exact Polymarket market link at prediction time
      - indicators      : every indicator value (RSI, MACD, Bollinger, MFI, VWAP, …)
      - strategy_votes  : ensemble strategy signals
      - deepseek signal, confidence, reasoning
      - resolved outcome (actual_direction, correct)

    Use this endpoint to feed a second LLM for independent win-rate verification.
    """
    records = _safe_storage(storage.get_audit_records, n, default=[])
    total   = len(records)
    resolved = [r for r in records if r.get("correct") is not None]
    wins    = sum(1 for r in resolved if r["correct"])
    return {
        "summary": {
            "total_predictions":    total,
            "resolved_predictions": len(resolved),
            "wins":                 wins,
            "losses":               len(resolved) - wins,
            "win_rate":             round(wins / len(resolved), 4) if resolved else None,
        },
        "records": records,
    }


@app.get("/audit/export")
async def export_audit(n: int = 500):
    """Download the full audit table as a JSON file for offline LLM review."""
    import io
    from fastapi.responses import StreamingResponse

    records = _safe_storage(storage.get_audit_records, n, default=[])
    resolved = [r for r in records if r.get("correct") is not None]
    wins = sum(1 for r in resolved if r["correct"])

    payload = {
        "exported_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "summary": {
            "total_predictions":    len(records),
            "resolved_predictions": len(resolved),
            "wins":                 wins,
            "losses":               len(resolved) - wins,
            "win_rate":             round(wins / len(resolved), 4) if resolved else None,
        },
        "records": records,
    }
    body = json.dumps(payload, indent=2, default=str)
    filename = f"audit_{int(time.time())}.json"
    return StreamingResponse(
        io.BytesIO(body.encode()),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/accuracy/all")
async def get_all_accuracy(n: int = 100):
    """
    Structured accuracy leaderboard for every prediction source.
    Returns four categories: ai, strategies, specialists, microstructure.
    Each entry: {key, name, accuracy, correct, total, label, weight}
    """
    from strategies.ensemble import accuracy_to_label

    limit = n if n > 0 else None
    all_stats  = compute_all_indicator_accuracy(limit)
    bi_acc     = compute_bar_insight_accuracy(limit)
    wts        = ensemble.get_weights()
    all_stats.pop("best_indicator", None)

    def _row(key, name, stats, weight=None):
        wins  = stats.get("wins", stats.get("correct", 0))
        total = stats.get("total", 0)
        acc   = stats.get("accuracy", 0.5)
        label = accuracy_to_label(acc, total) if total >= 3 else "LEARNING"
        return {
            "key":      key,
            "name":     name,
            "accuracy": round(acc * 100, 1),
            "correct":  wins,
            "total":    total,
            "label":    label,
            "weight":   round(weight, 3) if weight is not None else None,
        }

    # AI predictors
    ai = []
    for k, name in [("deepseek","DeepSeek AI"), ("ensemble","Math Ensemble")]:
        if k in all_stats:
            ai.append(_row(k, name, all_stats[k], wts.get(k)))
    if bi_acc["total"] > 0:
        ai.append(_row("bar_insight", "Bar Insight Analyst", bi_acc))

    # Technical strategies (strat: prefix in pattern_history)
    STRAT_NAMES = {
        "rsi":"RSI", "macd":"MACD", "stochastic":"Stochastic",
        "ema_cross":"EMA Fast", "supertrend":"Supertrend", "adx":"ADX",
        "alligator":"Alligator", "acc_dist":"Acc/Dist", "dow_theory":"Dow Theory",
        "fib_pullback":"Fibonacci", "harmonic":"Harmonic", "vwap":"AVWAP",
        "polymarket":"Crowd", "ml_logistic":"Lin Reg",
    }
    strategies = []
    for k, name in STRAT_NAMES.items():
        sk = f"strat:{k}"
        if sk in all_stats:
            strategies.append(_row(sk, name, all_stats[sk], wts.get(k)))

    # Specialist analysts (spec: prefix)
    SPEC_NAMES = {
        "spec:dow_theory":"DOW Theory", "spec:fib_pullback":"FIB Pullback",
        "spec:alligator":"ALG Alligator", "spec:acc_dist":"ACD Acc/Dist",
        "spec:harmonic":"HAR Harmonic",
    }
    specialists = []
    for k, name in SPEC_NAMES.items():
        if k in all_stats:
            specialists.append(_row(k, name, all_stats[k]))

    # Microstructure dashboard signals (dash: prefix)
    DASH_NAMES = {
        "dash:order_book":"Order Book", "dash:long_short":"Long/Short",
        "dash:taker_flow":"Taker Flow", "dash:oi_funding":"OI + Funding",
        "dash:liquidations":"Liquidations", "dash:fear_greed":"Fear & Greed",
        "dash:mempool":"Mempool", "dash:coinalyze":"Coinalyze",
        "dash:coinapi":"CoinAPI", "dash:coingecko":"CoinGecko",
    }
    microstructure = []
    for k, name in DASH_NAMES.items():
        if k in all_stats:
            microstructure.append(_row(k, name, all_stats[k]))

    # Sort each group by accuracy desc
    for lst in (ai, strategies, specialists, microstructure):
        lst.sort(key=lambda r: (r["total"] >= 3, r["accuracy"]), reverse=True)

    return {"ai": ai, "strategies": strategies,
            "specialists": specialists, "microstructure": microstructure}


@app.get("/best-indicator")
async def get_best_indicator(n: int = 0):
    """
    Win/loss leaderboard for every indicator tracked in pattern_history.

    Covers all sources:
      - strat:<name>  — rule-based / ML strategy votes
      - spec:<name>   — DeepSeek specialist analysts
      - dash:<name>   — microstructure dashboard signals
      - deepseek      — main DeepSeek AI prediction
      - ensemble      — math ensemble prediction

    Each entry: { wins, losses, total, accuracy }

    Query params:
      n=0 (default) — use all history
      n=N           — use only the last N resolved bars

    Response also includes:
      best_indicator : name of the indicator with the highest accuracy (≥3 calls)
      ranked         : list of all indicators sorted by accuracy desc
    """
    limit = n if n > 0 else None
    try:
        data = compute_all_indicator_accuracy(limit)
    except Exception as exc:
        logger.warning("best-indicator compute failed: %s", exc)
        return {"error": str(exc)}

    best = data.pop("best_indicator", None)

    # Sort by accuracy desc, then total desc for ties
    ranked = sorted(
        [{"name": k, **v} for k, v in data.items()],
        key=lambda x: (x["accuracy"], x["total"]),
        reverse=True,
    )

    return {
        "best_indicator": best,
        "ranked": ranked,
        "total_indicators": len(ranked),
    }


# =============================================================================
# Admin — reset database
# =============================================================================

@app.post("/force-predict")
async def force_predict():
    """
    Run a full prediction cycle immediately for testing.
    - Does NOT save anything to the database.
    - Does NOT reset the bar timer or window state.
    - Updates the UI live via existing WebSocket state.
    - The next real candle open continues as normal.
    """
    prices = collector.get_prices(400)
    if len(prices) < 30:
        return {"status": "error", "detail": "Not enough price data yet (need 30, have %d)" % len(prices)}

    # Preserve current window so the timer is untouched
    saved_window_start_time  = current_state.get("window_start_time")
    saved_window_start_price = current_state.get("window_start_price")
    saved_prediction         = current_state.get("prediction")
    saved_ensemble           = current_state.get("ensemble_prediction")
    saved_deepseek           = current_state.get("deepseek_prediction")
    saved_strategies         = current_state.get("strategies")
    saved_specialist_at      = current_state.get("specialist_completed_at")

    try:
        # Use current bar's window start (snap to 5m boundary) — keeps timer intact
        now = time.time()
        window_start_time  = saved_window_start_time or (now - (now % 300))
        window_start_price = prices[-1]

        poly_prob = polymarket_feed.implied_prob if polymarket_feed.is_live else None
        klines    = list(binance_klines)
        strategy_preds = get_all_predictions(prices, ohlcv=klines, polymarket_prob=poly_prob)
        features_dict  = feature_engine.compute_all(prices, ohlcv=klines or None)
        try:
            strategy_preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=klines)
        except Exception as lr_exc:
            logger.warning("Force predict — Lin Reg error: %s", lr_exc)

        # Dashboard signals (concurrent)
        dashboard_signals = None
        dashboard_task = asyncio.create_task(
            fetch_dashboard_signals(
                coinapi_key=config.coinapi_key,
                coinalyze_key=config.coinalyze_key,
            )
        )

        # Unified specialist + pattern analyst (dry run)
        fp_pattern_analysis = None
        fp_creative_edge    = None
        fp_dashboard_acc    = compute_dashboard_accuracy(200)
        if deepseek and klines:
            try:
                fp_spec_raw, fp_pattern_analysis = await asyncio.wait_for(
                    asyncio.gather(
                        run_specialists(config.deepseek_api_key, klines),
                        run_pattern_analyst(
                            config.deepseek_api_key,
                            load_pattern_history(),
                            features_dict,
                            dict(strategy_preds),
                            window_start_time=window_start_time,
                            dashboard_accuracy=fp_dashboard_acc,
                        ),
                        return_exceptions=True,
                    ),
                    timeout=30.0,
                )
                if isinstance(fp_spec_raw, Exception):
                    logger.warning("Force predict — specialist raised: %s", fp_spec_raw)
                elif fp_spec_raw:
                    fp_strats, fp_creative_edge = fp_spec_raw
                    for key, result in fp_strats.items():
                        if result is not None:
                            strategy_preds[key] = result
                if isinstance(fp_pattern_analysis, Exception):
                    fp_pattern_analysis = None
            except asyncio.TimeoutError:
                logger.warning("Force predict — specialists/pattern analyst timed out")
            except Exception as exc:
                logger.warning("Force predict — specialist/pattern error: %s", exc)
            current_state["specialist_completed_at"] = time.time()

        try:
            dashboard_signals = await asyncio.wait_for(dashboard_task, timeout=10.0)
        except (asyncio.TimeoutError, Exception) as exc:
            logger.warning("Force predict — dashboard signals error: %s", exc)
            dashboard_task.cancel()

        # ── Inject dashboard microstructure signals as ensemble votes (dash: prefix)
        if dashboard_signals:
            dash_preds = _dashboard_signals_to_preds(dashboard_signals)
            strategy_preds.update(dash_preds)

        pred = ensemble.predict(strategy_preds)
        strategy_preds = _json_safe(strategy_preds)
        pred["source"] = "ensemble"

        # Update UI state — but keep window_start_time/price from the real bar
        current_state["window_start_time"]  = window_start_time
        current_state["window_start_price"] = saved_window_start_price or window_start_price
        current_state["ensemble_prediction"] = pred
        current_state["prediction"]          = pred
        current_state["strategies"]          = strategy_preds

        logger.info("Force predict (dry run) | %s (%.1f%%) | no DB write",
                    pred["signal"], pred["confidence"] * 100)

        # DeepSeek — dry_run=True skips DB write
        if deepseek and features_dict:
            ds_strategy_preds = {
                k: v for k, v in strategy_preds.items()
                if k not in SPECIALIST_KEYS and not k.startswith("dash:")
            }
            rolling_acc = 0.0
            ds_acc = {}
            fp_indicator_acc = {}
            try:
                result = _safe_storage(storage.get_rolling_accuracy,
                                       config.rolling_window_size, default=(0, 0, 0.0))
                _, _, rolling_acc = result or (0, 0, 0.0)
                ds_acc = _safe_storage(storage.get_deepseek_accuracy, default={})
                fp_indicator_acc = _safe_storage(storage.get_strategy_accuracy_full, 100, default={})
                fp_bi_acc = compute_bar_insight_accuracy(100)
                if fp_bi_acc["total"] > 0:
                    fp_indicator_acc["bar_insight"] = fp_bi_acc
            except Exception:
                pass

            asyncio.create_task(
                _run_deepseek(
                    prices=list(prices),
                    klines=list(binance_klines),
                    features=features_dict,
                    strategy_preds=ds_strategy_preds,
                    rolling_acc=rolling_acc,
                    ds_acc=ds_acc,
                    window_start_time=window_start_time,
                    window_end_time=window_start_time + config.window_duration_seconds,
                    window_start_price=saved_window_start_price or window_start_price,
                    ensemble_result=pred,
                    polymarket_slug=polymarket_feed.active_slug,
                    dashboard_signals=dashboard_signals,
                    indicator_accuracy=fp_indicator_acc,
                    ensemble_weights=ensemble.get_weights(),
                    pattern_analysis=fp_pattern_analysis,
                    creative_edge=fp_creative_edge,
                    dashboard_accuracy=fp_dashboard_acc,
                    dry_run=True,
                )
            )

        return {
            "status": "ok",
            "signal": pred["signal"],
            "confidence": pred["confidence"],
            "dry_run": True,
            "note": "Preview only — not saved, timer unchanged",
        }

    except Exception as exc:
        # Restore previous state on failure
        current_state["window_start_time"]  = saved_window_start_time
        current_state["window_start_price"] = saved_window_start_price
        current_state["prediction"]         = saved_prediction
        current_state["ensemble_prediction"] = saved_ensemble
        current_state["deepseek_prediction"] = saved_deepseek
        current_state["strategies"]          = saved_strategies
        current_state["specialist_completed_at"] = saved_specialist_at
        logger.error("Force predict failed: %s", exc)
        return {"status": "error", "detail": str(exc)}


@app.post("/admin/reset")
async def reset_database():
    """Clear all ticks, predictions, and deepseek_predictions from MongoDB."""
    try:
        t = storage.db.ticks.delete_many({})
        p = storage.db.predictions.delete_many({})
        d = storage.db.deepseek_predictions.delete_many({})
        logger.info("Database reset: ticks=%d, predictions=%d, deepseek=%d deleted",
                     t.deleted_count, p.deleted_count, d.deleted_count)
        return {
            "status": "ok",
            "deleted": {
                "ticks": t.deleted_count,
                "predictions": p.deleted_count,
                "deepseek_predictions": d.deleted_count,
            }
        }
    except Exception as exc:
        logger.error("Database reset failed: %s", exc)
        return {"status": "error", "detail": str(exc)}


# =============================================================================
# WebSocket — live feed
# =============================================================================

_WS_STRIP_KEYS = {"full_prompt", "raw_response"}

def _pred_for_ws(pred: Optional[Dict]) -> Optional[Dict]:
    """Return a stripped, json-safe copy of a prediction dict for WS ticks."""
    if pred is None:
        return None
    return _json_safe({k: v for k, v in pred.items() if k not in _WS_STRIP_KEYS})


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
                        "type": "tick",
                        "price": current_state["price"],
                        "window_start_price": current_state["window_start_price"],
                        "window_start_time": current_state["window_start_time"],
                        "prediction": current_state["prediction"],
                        "ensemble_prediction": current_state.get("ensemble_prediction"),
                        "strategies": current_state["strategies"],
                        "deepseek_prediction": _pred_for_ws(current_state.get("deepseek_prediction")),
                        "pending_deepseek_prediction": _pred_for_ws(current_state.get("pending_deepseek_prediction")),
                        "pending_deepseek_ready": current_state.get("pending_deepseek_ready", False),
                        "agree_accuracy": current_state.get("agree_accuracy"),
                        "polymarket": polymarket_feed.to_dict(),
                        "specialist_completed_at": current_state.get("specialist_completed_at"),
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


# =============================================================================
# Background tasks
# =============================================================================

@app.on_event("startup")
async def startup():
    asyncio.create_task(run_collector())
    asyncio.create_task(run_prediction_loop())
    asyncio.create_task(polymarket_feed.run())
    asyncio.create_task(run_binance_feed())
    asyncio.create_task(run_indicator_refresh())


async def _refresh_indicators():
    """Recompute all strategy indicators and push into current_state.
    Specialist keys (DeepSeek AI) are set once at bar open and preserved here —
    the math-based fallbacks must NOT overwrite them mid-window.
    """
    prices = collector.get_prices(400)
    if len(prices) < 30:
        return
    try:
        poly_prob = polymarket_feed.implied_prob if polymarket_feed.is_live else None
        klines    = list(binance_klines)
        preds     = get_all_predictions(prices, ohlcv=klines, polymarket_prob=poly_prob)
        try:
            preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=klines)
        except Exception as lr_exc:
            logger.warning("Lin Reg predict error: %s", lr_exc)
        # Preserve results set once at bar open (specialists + dashboard votes)
        existing = current_state.get("strategies") or {}
        for key in SPECIALIST_KEYS:
            if key in existing:
                preds[key] = existing[key]
        for key, val in existing.items():
            if key.startswith("dash:"):
                preds[key] = val
        current_state["strategies"] = _json_safe(preds)
        logger.debug("Indicators refreshed (%d strategies, specialists preserved)", len(preds))
    except Exception as exc:
        logger.warning("Indicator refresh error: %s", exc)


async def run_binance_feed():
    """Fetch Binance BTCUSDT 1m OHLCV every 60s for volume-based indicators.
    Also seeds the tick collector so the prediction loop always has fresh prices.
    """
    global binance_klines
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": "BTCUSDT", "interval": "1m", "limit": 500}
    while True:
        try:
            connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        binance_klines.clear()
                        binance_klines.extend(data)
                        logger.info("Binance klines updated: %d candles", len(binance_klines))
                        # Always reseed tick collector from klines so prices never go stale
                        collector.seed_from_klines(binance_klines)
                        await _refresh_indicators()
        except Exception as exc:
            logger.warning("Binance feed error: %s", exc)
        await asyncio.sleep(60)


async def run_indicator_refresh():
    """Recompute all strategy indicators every 15s for live display."""
    await asyncio.sleep(20)   # let collector + binance feed warm up
    while True:
        await asyncio.sleep(15)
        await _refresh_indicators()


async def run_collector():
    def on_tick(tick):
        current_state["price"] = tick.mid_price
        _safe_storage(
            storage.store_tick,
            tick.timestamp, tick.mid_price, tick.bid_price, tick.ask_price, tick.spread,
        )

    collector.on_tick(on_tick)
    await collector.start()


async def _run_deepseek(
    prices, klines, features, strategy_preds, rolling_acc, ds_acc,
    window_start_time, window_end_time, window_start_price,
    ensemble_result=None, polymarket_slug=None, dashboard_signals=None,
    indicator_accuracy=None, ensemble_weights=None,
    pattern_analysis=None, creative_edge=None, bar_insight=None,
    dashboard_accuracy=None,
    dry_run=False,
):
    """
    DeepSeek call — fires at bar open, result HELD until bar closes.

    Result is staged in current_state["pending_deepseek_prediction"].
    _resolve_window promotes it to current_state["deepseek_prediction"] at bar close.

    Stale-window guard: if the bar has already rolled over before this completes,
    the result is discarded to prevent corrupting the new bar's state.
    """
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))
    logger.info(">>> DeepSeek request FIRED for bar %s (window_start=%.0f)", bar_ts, window_start_time)
    try:
        result = await deepseek.predict(
            prices=prices,
            klines=klines,
            features=features,
            strategy_preds=strategy_preds,
            recent_accuracy=rolling_acc,
            deepseek_accuracy=ds_acc,
            window_start_time=window_start_time,
            window_start_price=window_start_price,
            polymarket_slug=polymarket_slug,
            ensemble_result=ensemble_result,
            dashboard_signals=dashboard_signals,
            indicator_accuracy=indicator_accuracy,
            ensemble_weights=ensemble_weights,
            pattern_analysis=pattern_analysis,
            creative_edge=creative_edge,
            bar_insight=bar_insight,
            dashboard_accuracy=dashboard_accuracy,
        )

        # ── Stale-window guard ────────────────────────────────────────────────
        # Discard result if the prediction loop has already moved to the next bar.
        current_bar = current_state.get("window_start_time")
        if current_bar is not None and abs(current_bar - window_start_time) > 30:
            logger.warning(
                "DeepSeek stale result DISCARDED — completed for bar %s but current bar is %s",
                bar_ts, time.strftime("%H:%M:%S UTC", time.gmtime(current_bar)),
            )
            return

        logger.info(
            ">>> DeepSeek COMPLETED for bar %s → %s %d%% in %.1fs — held until bar close",
            bar_ts, result.get("signal"), result.get("confidence", 0),
            result.get("latency_ms", 0) / 1000,
        )

        if result.get("data_requests") and result["data_requests"].upper() not in ("NONE", ""):
            logger.info("DeepSeek data request: %s", result["data_requests"])

        if result["signal"] not in ("ERROR", "UNAVAILABLE") and not dry_run:
            chart_path = generate_bar_chart(
                klines, window_start_time, result["signal"], result.get("confidence", 50)
            )
            _safe_storage(
                storage.store_deepseek_prediction,
                window_start=window_start_time,
                window_end=window_end_time,
                start_price=window_start_price,
                signal=result["signal"],
                confidence=result.get("confidence", 50),
                reasoning=result.get("reasoning", ""),
                raw_response=result.get("raw_response", ""),
                full_prompt=result.get("full_prompt", ""),
                polymarket_url=result.get("polymarket_url", ""),
                strategy_snapshot=json.dumps(strategy_preds),
                latency_ms=result.get("latency_ms", 0),
                window_count=result.get("window_count", 0),
                data_received=result.get("data_received", ""),
                data_requests=result.get("data_requests", ""),
                indicators_snapshot=json.dumps(_json_safe(features)),
                narrative=result.get("narrative", ""),
                free_observation=result.get("free_observation", ""),
                chart_path=chart_path or "",
                dashboard_signals_snapshot=json.dumps(_json_safe(dashboard_signals or {})),
            )
        elif dry_run and result["signal"] not in ("ERROR", "UNAVAILABLE"):
            logger.info("Force predict (dry run) — skipping DB write, signal=%s conf=%d%%",
                        result["signal"], result["confidence"])

        # ── Stage result — revealed to UI only when _resolve_window fires ─────
        if result["signal"] not in ("ERROR", "UNAVAILABLE"):
            current_state["pending_deepseek_prediction"] = result
            current_state["pending_deepseek_ready"] = True
            logger.info("DeepSeek result STAGED for bar %s — awaiting bar close to reveal", bar_ts)
        else:
            logger.warning("DeepSeek returned %s for bar %s — not staging", result["signal"], bar_ts)

    except Exception as exc:
        logger.error("_run_deepseek FAILED for bar %s: %s", bar_ts, exc)


def _safe_storage(fn, *args, default=None, **kwargs):
    """Call a storage method; return default on any error (MongoDB down, etc.)."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("Storage call %s failed (MongoDB down?): %s", fn.__name__, exc)
        return default


async def _run_full_prediction(prices, is_force=False):
    """
    Core prediction logic — shared by the 5-minute loop and the /force-predict endpoint.
    Opens a window, runs ensemble + specialists + DeepSeek, updates current_state.
    Returns the ensemble pred dict.
    """
    window_start_price = prices[-1]
    now = time.time()
    # Snap to the current 5-minute bar open so the timer matches Binance/TradingView
    window_start_time = now - (now % 300) if not is_force else now
    current_state["window_start_price"] = window_start_price
    current_state["window_start_time"] = window_start_time
    current_state["specialist_completed_at"] = None
    # Clear pending result from previous bar — new bar, fresh slate
    current_state["pending_deepseek_prediction"] = None
    current_state["pending_deepseek_ready"] = False

    tag = "FORCE" if is_force else "BAR OPEN"
    logger.info(
        "=== %s #%d === %s | price=%.2f ===",
        tag, deepseek.window_count + 1 if deepseek else 0,
        time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time)),
        window_start_price,
    )

    poly_prob = polymarket_feed.implied_prob if polymarket_feed.is_live else None
    klines = list(binance_klines)
    strategy_preds = get_all_predictions(prices, ohlcv=klines, polymarket_prob=poly_prob)
    features_dict = feature_engine.compute_all(prices, ohlcv=klines or None)
    try:
        strategy_preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=klines)
    except Exception as lr_exc:
        logger.warning("Lin Reg predict error: %s", lr_exc)

    # ── Features quality gate — skip DeepSeek if features are empty/invalid ──
    _features_ok = bool(features_dict and len(features_dict) > 5)
    if not _features_ok:
        logger.warning("SKIP DEEPSEEK — feature engine returned insufficient data (%d keys)",
                       len(features_dict) if features_dict else 0)

    # ── Dashboard signals fetch (concurrent with specialists)
    dashboard_signals = None
    dashboard_task = asyncio.create_task(
        fetch_dashboard_signals(
            coinapi_key=config.coinapi_key,
            coinalyze_key=config.coinalyze_key,
        )
    )

    # ── All 3 specialists in parallel: unified analyst + pattern analyst + bar insight analyst
    pattern_analysis  = None
    bar_insight       = None
    creative_edge     = None
    _all_history      = load_pattern_history()   # load once, share across analysts

    # Compute dashboard microstructure accuracy from resolved history
    dashboard_acc = compute_dashboard_accuracy(200)

    if deepseek and klines and _features_ok:
        try:
            spec_raw, pattern_analysis, bar_insight = await asyncio.wait_for(
                asyncio.gather(
                    run_specialists(config.deepseek_api_key, klines),
                    run_pattern_analyst(
                        config.deepseek_api_key,
                        _all_history,
                        features_dict,
                        {k: v for k, v in strategy_preds.items()},
                        window_start_time=window_start_time,
                        dashboard_accuracy=dashboard_acc,
                    ),
                    run_bar_insight_analyst(
                        config.deepseek_api_key,
                        _all_history,
                        features_dict,
                        {k: v for k, v in strategy_preds.items()},
                        window_start_time=window_start_time,
                        # specialist signals not yet known at parallel start — passed after unpack below
                    ),
                    return_exceptions=True,
                ),
                timeout=30.0,
            )

            # Unpack unified specialist → (strategy_dict, creative_edge)
            if isinstance(spec_raw, Exception):
                logger.warning("Unified specialist raised: %s", spec_raw)
            elif spec_raw:
                specialist_results, creative_edge = spec_raw
                for key, result in specialist_results.items():
                    if result is not None:
                        strategy_preds[key] = result
                current_state["bar_specialist_signals"] = _json_safe(specialist_results)
                current_state["bar_creative_edge"]      = creative_edge or ""
                logger.info(
                    "Specialists merged: %s | creative_edge: %s",
                    [k for k, v in specialist_results.items() if v is not None],
                    "yes" if creative_edge else "none",
                )

            # Pattern analyst
            if isinstance(pattern_analysis, Exception):
                logger.warning("Pattern analyst raised: %s", pattern_analysis)
                pattern_analysis = None
            elif pattern_analysis:
                current_state["bar_pattern_analysis"] = pattern_analysis
                logger.info("Pattern analyst: %d chars", len(pattern_analysis))

            # Bar insight analyst — returns (text, call_signal) tuple
            bar_insight_signal = ""
            if isinstance(bar_insight, Exception):
                logger.warning("Bar insight analyst raised: %s", bar_insight)
                bar_insight = None
            elif bar_insight:
                bar_insight, bar_insight_signal = bar_insight
                if bar_insight:
                    current_state["bar_insight_text"]   = bar_insight
                    current_state["bar_insight_signal"] = bar_insight_signal
                    logger.info("Bar insight analyst: %d chars call=%s",
                                len(bar_insight), bar_insight_signal or "NONE")

        except asyncio.TimeoutError:
            logger.warning("Specialists timed out — using math fallbacks")
        except Exception as exc:
            logger.warning("Specialist runner error: %s", exc)
        current_state["specialist_completed_at"] = time.time()

    # ── Dashboard signals
    try:
        dashboard_signals = await asyncio.wait_for(dashboard_task, timeout=10.0)
        n_ok = sum(
            1 for k, v in dashboard_signals.items()
            if v is not None and k != "fetched_at"
        )
        logger.info("Dashboard signals ready: %d/%d sources ok", n_ok,
                     len(dashboard_signals) - 1)
    except asyncio.TimeoutError:
        logger.warning("Dashboard signals timed out — DeepSeek runs without microstructure")
        dashboard_task.cancel()
    except Exception as exc:
        logger.warning("Dashboard signals error: %s", exc)

    # ── Inject dashboard microstructure signals as ensemble votes (dash: prefix)
    if dashboard_signals:
        dash_preds = _dashboard_signals_to_preds(dashboard_signals)
        strategy_preds.update(dash_preds)
        logger.info("Dashboard signals injected into ensemble: %d votes", len(dash_preds))

    pred = ensemble.predict(strategy_preds)
    strategy_preds = _json_safe(strategy_preds)
    pred["source"] = "ensemble"
    current_state["prediction"] = pred
    current_state["ensemble_prediction"] = pred
    current_state["strategies"] = strategy_preds
    current_state["agree_accuracy"] = _safe_storage(storage.get_agree_accuracy, default={})

    pm_odds_open = polymarket_feed.market_odds if polymarket_feed.is_live else None
    pm_ev_open = calculate_ev(pred["confidence"], pm_odds_open).expected_value if pm_odds_open else None

    pm_str = f" | PM:{polymarket_feed.implied_prob*100:.1f}% UP" if polymarket_feed.is_live else ""
    logger.info(
        "Window opened | %s (%.1f%%) | bull:%d bear:%d | price:%.2f%s",
        pred["signal"], pred["confidence"] * 100,
        pred["bullish_count"], pred["bearish_count"], window_start_price, pm_str,
    )

    # ── Accuracy stats + auto weight update
    rolling_acc = 0.0
    ds_acc = {}
    indicator_acc_full = {}
    if deepseek and features_dict:
        result = _safe_storage(storage.get_rolling_accuracy, config.rolling_window_size, default=(0, 0, 0.0))
        _, _, rolling_acc = result
        rolling_acc = rolling_acc or 0.0   # guard: None when MongoDB is down
        ds_acc = _safe_storage(storage.get_deepseek_accuracy, default={})

    # Always fetch indicator accuracy and update ensemble weights (regardless of DeepSeek)
    indicator_acc_full = _safe_storage(storage.get_strategy_accuracy_full, 100, default={})
    # Merge dashboard microstructure accuracy (from pattern_history) into weight update
    # This covers existing history + new bars before strategy_votes accumulates dash: entries
    dash_acc = compute_dashboard_accuracy(100)
    for name, stats in dash_acc.items():
        indicator_acc_full[f"dash:{name}"] = stats
    # Merge bar insight analyst accuracy so it appears in the main DeepSeek prompt
    bi_acc = compute_bar_insight_accuracy(100)
    if bi_acc["total"] > 0:
        indicator_acc_full["bar_insight"] = bi_acc
    if indicator_acc_full:
        ensemble.update_weights_from_full_stats(indicator_acc_full)
        logger.info(
            "Ensemble weights updated from %d indicator histories (%d microstructure, bar_insight=%d)",
            len(indicator_acc_full), len(dash_acc), bi_acc["total"],
        )

    # ── Backend snapshot
    current_state["backend_snapshot"] = _json_safe({
        "window_num": deepseek.window_count + 1 if deepseek else 0,
        "window_start": window_start_time,
        "window_start_price": window_start_price,
        "prices_last20": list(prices[-20:]) if len(prices) >= 20 else list(prices),
        "features": features_dict,
        "strategy_preds": strategy_preds,
        "dashboard_signals": dashboard_signals,
        "ensemble_result": {
            "signal":            pred["signal"],
            "confidence":        pred["confidence"],
            "bullish_count":     pred["bullish_count"],
            "bearish_count":     pred["bearish_count"],
            "up_probability":    pred.get("up_probability", 0.5),
            "weighted_up_score": pred.get("weighted_up_score", 0),
            "weighted_down_score": pred.get("weighted_down_score", 0),
        },
        "polymarket": polymarket_feed.to_dict(),
        "rolling_acc": rolling_acc,
        "ds_acc": ds_acc,
        "captured_at": time.time(),
    })

    # ── DeepSeek prediction (non-blocking) — only fire with good data
    if not deepseek:
        logger.warning("SKIP DEEPSEEK — deepseek predictor is None (disabled in config?)")
    elif not _features_ok:
        logger.warning("SKIP DEEPSEEK — features insufficient (%d keys)", len(features_dict) if features_dict else 0)
    else:
        logger.info("FIRING DEEPSEEK — features=%d klines=%d", len(features_dict), len(list(binance_klines)))
    if deepseek and _features_ok:
        ds_strategy_preds = {
            k: v for k, v in strategy_preds.items()
            if k not in SPECIALIST_KEYS and not k.startswith("dash:")
        }
        asyncio.create_task(
            _run_deepseek(
                prices=list(prices),
                klines=list(binance_klines),
                features=features_dict,
                strategy_preds=ds_strategy_preds,
                rolling_acc=rolling_acc,
                ds_acc=ds_acc,
                window_start_time=window_start_time,
                window_end_time=window_start_time + config.window_duration_seconds,
                window_start_price=window_start_price,
                ensemble_result=pred,
                polymarket_slug=polymarket_feed.active_slug,
                dashboard_signals=dashboard_signals,
                indicator_accuracy=indicator_acc_full,
                ensemble_weights=ensemble.get_weights(),
                pattern_analysis=pattern_analysis,
                creative_edge=creative_edge,
                bar_insight=bar_insight,
                dashboard_accuracy=dashboard_acc,
            )
        )

    return pred, strategy_preds, pm_odds_open, pm_ev_open, window_start_time, window_start_price


async def _resolve_window(
    window_start_time, window_start_price, pred, strategy_preds, pm_odds_open, pm_ev_open
):
    """
    Persist and resolve a closed window.  Runs as a background task so MongoDB
    timeouts (up to 10 s each) don't delay the next window from opening — which
    was causing the bar timer to show 00:00 for up to 40 s between windows.
    """
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))

    # ── Promote pending DeepSeek result → revealed to UI NOW (bar just closed) ──
    pending = current_state.get("pending_deepseek_prediction")
    if pending and pending.get("signal") not in (None, "ERROR", "UNAVAILABLE"):
        current_state["deepseek_prediction"] = pending
        current_state["pending_deepseek_prediction"] = None
        current_state["pending_deepseek_ready"] = False
        logger.info(
            ">>> Bar %s CLOSED — DeepSeek result REVEALED: %s %d%%",
            bar_ts, pending["signal"], pending.get("confidence", 0),
        )
    else:
        logger.warning("Bar %s closed — no pending DeepSeek result to reveal", bar_ts)

    end_prices = collector.get_prices(1)
    if end_prices:
        end_price = end_prices[-1]
        actual    = "UP" if end_price >= window_start_price else "DOWN"
        correct   = actual == pred["signal"]

        _safe_storage(storage.store_prediction,
            window_start=window_start_time,
            window_end=window_start_time + config.window_duration_seconds,
            start_price=window_start_price,
            signal=pred["signal"],
            confidence=pred["confidence"],
            strategy_votes=strategy_preds,
            market_odds=pm_odds_open,
            ev=pm_ev_open,
        )
        _safe_storage(storage.resolve_prediction, window_start_time, end_price)

        # ── Complete bar record — every value from every specialist ─────────────
        snap              = current_state.get("backend_snapshot") or {}
        snap_indicators   = snap.get("features", {})
        snap_ds_raw       = snap.get("dashboard_signals")
        # Extract signal directions from the snapshot captured at bar open
        snap_dash_raw     = {}
        if snap_ds_raw:
            try:
                snap_dash_signals = (
                    json.loads(snap_ds_raw) if isinstance(snap_ds_raw, str) else snap_ds_raw
                )
                snap_dash_raw = extract_signal_directions(snap_dash_signals)
            except Exception:
                pass

        ds_pred_snap    = current_state.get("deepseek_prediction") or {}
        ds_correct      = (actual == ds_pred_snap["signal"]
                           if ds_pred_snap.get("signal") not in (None, "ERROR", "UNAVAILABLE")
                           else None)
        try:
            append_resolved_window(
                window_start          = window_start_time,
                window_end            = window_start_time + config.window_duration_seconds,
                actual_direction      = actual,
                start_price           = window_start_price,
                end_price             = end_price,
                ensemble_signal       = pred["signal"],
                ensemble_conf         = pred["confidence"],
                ensemble_correct      = (actual == pred["signal"]),
                deepseek_signal       = ds_pred_snap.get("signal", ""),
                deepseek_conf         = ds_pred_snap.get("confidence", 0),
                deepseek_correct      = ds_correct,
                deepseek_reasoning    = ds_pred_snap.get("reasoning", ""),
                deepseek_narrative    = ds_pred_snap.get("narrative", ""),
                deepseek_free_obs     = ds_pred_snap.get("free_observation", ""),
                specialist_signals    = current_state.get("bar_specialist_signals", {}),
                creative_edge         = current_state.get("bar_creative_edge", ""),
                pattern_analysis      = current_state.get("bar_pattern_analysis", ""),
                bar_insight_text      = current_state.get("bar_insight_text", ""),
                bar_insight_signal    = current_state.get("bar_insight_signal", ""),
                strategy_votes        = strategy_preds,
                indicators            = snap_indicators,
                dashboard_signals_raw = snap_dash_raw,
            )
        except Exception as ph_exc:
            logger.warning("Pattern history append failed: %s", ph_exc)

        # Reset bar-level specialist state for next window
        current_state["bar_specialist_signals"] = {}
        current_state["bar_creative_edge"]      = ""
        current_state["bar_pattern_analysis"]   = ""
        current_state["bar_insight_text"]       = ""
        current_state["bar_insight_signal"]     = ""

        _safe_storage(storage.resolve_deepseek_prediction, window_start_time, end_price)

        ds_pred = current_state.get("deepseek_prediction")
        if ds_pred and ds_pred.get("signal") not in (None, "ERROR"):
            ds_correct = actual == ds_pred["signal"]
            logger.info(
                "DeepSeek | actual:%s | predicted:%s | %s",
                actual, ds_pred["signal"], "WIN" if ds_correct else "LOSS",
            )

        logger.info(
            "Window closed | actual:%s | predicted:%s | %s | Δ%.2f",
            actual, pred["signal"], "WIN" if correct else "LOSS",
            end_price - window_start_price,
        )

    # Update agree accuracy + ensemble weights (MongoDB — may be slow when down)
    current_state["agree_accuracy"] = _safe_storage(storage.get_agree_accuracy, default={})
    result = _safe_storage(storage.get_rolling_accuracy, default=(0, 0, 0.0))
    total = result[0] if result else 0
    if total >= config.min_predictions_for_weight_update:
        acc = _safe_storage(storage.get_strategy_rolling_accuracy, default={})
        # Merge dashboard rolling accuracy (from pattern_history) with dash: prefix
        dash_acc_resolve = compute_dashboard_accuracy(20)
        for name, stats in dash_acc_resolve.items():
            acc[f"dash:{name}"] = stats["accuracy"]
        if acc:
            ensemble.update_weights(acc)


def _data_quality_check(prices: list, klines: list) -> tuple[bool, str]:
    """
    Returns (ok, reason). If ok is False, the prediction is skipped entirely.
    Accuracy matters more than blindly running — skip rather than guess.
    """
    if len(prices) < 30:
        return False, f"insufficient tick prices ({len(prices)}/30 required)"
    if not klines or len(klines) < 20:
        return False, f"insufficient klines ({len(klines) if klines else 0}/20 required)"
    # Check klines recency — last bar open time should be within 3 minutes
    try:
        last_bar_ts = int(klines[-1][0]) / 1000
        stale_secs  = time.time() - last_bar_ts
        if stale_secs > 180:
            return False, f"klines are stale ({stale_secs:.0f}s old — Binance feed may be down)"
    except Exception:
        pass
    return True, ""


async def run_prediction_loop():
    """5-minute window loop: predict → wait → resolve (background) → repeat immediately.

    FAILSAFE: if data quality is insufficient, the window is SKIPPED entirely rather
    than making a blind guess.  The loop still sleeps to the next bar boundary so the
    UI timer stays accurate.
    """
    await asyncio.sleep(5)  # let collector warm up

    while True:
        try:
            prices = collector.get_prices(400)
            klines = list(binance_klines)

            # ── Data quality gate ─────────────────────────────────────────────
            ok, reason = _data_quality_check(prices, klines)
            if not ok:
                logger.warning("SKIP PREDICTION — %s — waiting 15s for data", reason)
                await asyncio.sleep(15)
                continue

            pred, strategy_preds, pm_odds_open, pm_ev_open, window_start_time, window_start_price = \
                await _run_full_prediction(prices)

            # Sleep until this bar's close (aligned to 5m boundary)
            bar_close  = window_start_time + config.window_duration_seconds
            sleep_secs = max(1, bar_close - time.time())
            logger.info(
                "Prediction loop: bar %s sleeping %.1fs until close",
                time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time)), sleep_secs,
            )
            await asyncio.sleep(sleep_secs)

            # Fire resolve as a non-blocking task — new window starts immediately.
            # await sleep(0) yields to the event loop so _resolve_window can run its
            # synchronous prefix (promoting pending_deepseek_prediction) before
            # _run_full_prediction clears it for the next bar.
            asyncio.create_task(_resolve_window(
                window_start_time, window_start_price,
                pred, strategy_preds, pm_odds_open, pm_ev_open,
            ))
            await asyncio.sleep(0)

        except Exception as exc:
            logger.error("Prediction loop CRASHED — recovering in 10s: %s", exc, exc_info=True)
            await asyncio.sleep(10)


# =============================================================================
# Run directly
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.api_host, port=config.api_port)
