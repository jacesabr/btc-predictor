"""
Prediction Engine
=================
All background tasks and shared state. Imported by server.py for REST endpoints.

Background tasks started at startup:
  run_collector()          — tick feed from Binance REST, stores prices
  run_binance_feed()       — 1-min OHLCV klines, refreshed every 60s (Bybit→OKX→Kraken→Binance)
  run_indicator_refresh()  — strategy signals refreshed every 15s
  run_prediction_loop()    — 5-minute bar loop: predict → wait → resolve
  polymarket_feed.run()    — polls Polymarket Gamma API for BTC Up/Down market

Key state object: current_state dict — read by WebSocket and REST endpoints.
"""

import asyncio
import json
import logging
import pathlib
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

try:
    from dotenv import load_dotenv
    load_dotenv(pathlib.Path(__file__).parent / ".env")
except ImportError:
    pass

from config import Config
from data_feed import BinanceCollector, FeatureEngine, PolymarketFeed
from signals import fetch_dashboard_signals, extract_signal_directions
from storage import Storage, get_storage
from semantic_store import (
    append_resolved_window,
    load_all as load_pattern_history,
    compute_dashboard_accuracy,
    compute_all_indicator_accuracy,
    store_embedding as store_bar_embedding,
    search_similar as pgvector_search,
)
from strategies import (
    get_all_predictions, EnsemblePredictor, LinearRegressionChannel,
    calculate_ev,
)
from ai import (
    DeepSeekPredictor, run_specialists, run_historical_analyst,
    run_binance_expert, SPECIALIST_KEYS, _build_current_bar, run_postmortem,
    embed_text, _bar_embed_text, CohereUnavailableError,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────
_ROOT       = pathlib.Path(__file__).parent
STATIC_DIR  = _ROOT / "static"
CHARTS_DIR  = _ROOT / "charts"
CHARTS_DIR.mkdir(exist_ok=True)

# ── Shared objects ────────────────────────────────────────────
config          = Config()
collector       = BinanceCollector(poll_interval=config.poll_interval_seconds)
storage         = get_storage()
ensemble        = EnsemblePredictor(config.initial_weights)
lr_strategy     = LinearRegressionChannel()
feature_engine  = FeatureEngine()
polymarket_feed = PolymarketFeed(poll_interval=1.0)

# Read max stored bar count so window_count persists across restarts
_ds_bar_init: int = 0
try:
    _ds_init_recs = storage.get_recent_deepseek_predictions(9999)
    if _ds_init_recs:
        _ds_bar_init = max((r.get("window_count") or 0 for r in _ds_init_recs), default=0)
    logger.info("Bar counter initialised at %d", _ds_bar_init)
except Exception as _e:
    logger.warning("Could not read initial bar count from storage: %s", _e)

deepseek        = (
    DeepSeekPredictor(
        api_key=config.deepseek_api_key,
        model=(config.deepseek_vision_model if config.deepseek_use_vision
               else config.deepseek_model),
        initial_bar_count=_ds_bar_init,
    )
    if config.deepseek_enabled else None
)

ws_clients:    set  = set()
binance_klines: List = []

# In-memory error log — ERROR/UNAVAILABLE bars logged here, never embedded
_error_log: list = []


current_state: Dict = {
    "price":                      None,
    "window_start_price":         None,
    "window_start_time":          None,
    "prediction":                 None,
    "ensemble_prediction":        None,
    "strategies":                 {},
    "deepseek_prediction":        None,
    "pending_deepseek_prediction": None,
    "pending_deepseek_ready":     False,
    "agree_accuracy":             None,
    "specialist_completed_at":    None,
    "backend_snapshot":           None,
    "bar_specialist_signals":     {},
    "bar_creative_edge":          "",
    "bar_historical_analysis":    "",
    "bar_historical_context":     "",
    "bar_binance_expert":         {},
    "service_unavailable":        False,
    "service_unavailable_reason": "",
}


# ── Utilities ─────────────────────────────────────────────────

def _json_safe(obj):
    if isinstance(obj, dict):   return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):  return int(obj)
    if isinstance(obj, np.floating): return float(obj)
    if isinstance(obj, np.bool_):    return bool(obj)
    if isinstance(obj, np.ndarray):  return obj.tolist()
    return obj


def _safe_storage(fn, *args, default=None, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("Storage call %s failed: %s", fn.__name__, exc)
        return default


async def _safe_storage_async(fn, *args, default=None, **kwargs):
    try:
        return await asyncio.to_thread(fn, *args, **kwargs)
    except Exception as exc:
        logger.warning("Storage call %s failed: %s", fn.__name__, exc)
        return default


_WS_STRIP_KEYS = {"full_prompt", "raw_response"}

def _pred_for_ws(pred: Optional[Dict]) -> Optional[Dict]:
    if pred is None:
        return None
    return _json_safe({k: v for k, v in pred.items() if k not in _WS_STRIP_KEYS})


def _dashboard_signals_to_preds(dashboard_signals: Optional[Dict]) -> Dict:
    if not dashboard_signals:
        return {}
    directions = extract_signal_directions(dashboard_signals)
    return {
        f"dash:{name}": {"signal": direction, "confidence": 0.65, "reasoning": f"microstructure:{name}"}
        for name, direction in directions.items()
        if direction in ("UP", "DOWN")
    }


# ── Chart generation ─────────────────────────────────────────

def generate_bar_chart(klines: List, window_start: float, signal: str, confidence: int) -> Optional[str]:
    try:
        ws_ms    = window_start * 1000
        relevant = [k for k in klines if float(k[0]) <= ws_ms]
        if not relevant:
            return None
        bars   = relevant[-30:]
        opens  = [float(b[1]) for b in bars]
        highs  = [float(b[2]) for b in bars]
        lows   = [float(b[3]) for b in bars]
        closes = [float(b[4]) for b in bars]

        fig, ax = plt.subplots(figsize=(9, 3.2), facecolor="#F9F8F6")
        ax.set_facecolor("#F9F8F6")
        for i, (o, h, l, c) in enumerate(zip(opens, highs, lows, closes)):
            color = "#15803D" if c >= o else "#B91C1C"
            ax.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=1)
            rect = mpatches.FancyBboxPatch(
                (i - 0.35, min(o, c)), 0.7, max(abs(c - o), 0.1),
                boxstyle="square,pad=0", facecolor=color, edgecolor=color, linewidth=0, zorder=2,
            )
            ax.add_patch(rect)

        sig_color = "#15803D" if signal == "UP" else "#B91C1C"
        sig_arrow = "▲" if signal == "UP" else "▼"
        ax.axvline(x=len(bars)-1, color=sig_color, linewidth=1.2, linestyle="--", alpha=0.7, zorder=3)
        ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(window_start))
        ax.set_title(f"BTC/USDT 1m  ·  {ts}  ·  Prediction: {sig_arrow} {signal} {confidence}%",
                     fontsize=9, color="#1A1A1A", fontfamily="monospace", pad=5)
        ax.set_xlim(-1, len(bars))
        ax.tick_params(axis="x", labelbottom=False, length=0)
        ax.tick_params(axis="y", labelsize=7, colors="#6B6866")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"${v:,.0f}"))
        for spine in ("top", "right"): ax.spines[spine].set_visible(False)
        for spine in ("bottom", "left"): ax.spines[spine].set_color("#E6E4DF")
        ax.grid(axis="y", color="#E6E4DF", linewidth=0.5)

        fname = f"chart_{int(window_start)}.png"
        fpath = CHARTS_DIR / fname
        fig.tight_layout(pad=0.6)
        fig.savefig(str(fpath), dpi=110, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return str(fpath)
    except Exception as exc:
        logger.warning("Chart generation failed: %s", exc)
        return None


# ── Post-mortem background task ───────────────────────────────

async def _run_postmortem_background(
    ds_record: dict,
    actual_direction: str,
    end_price: float,
    klines: list,
    features: dict,
    dashboard_signals,
    embed_rec: Optional[dict] = None,
):
    """Fire after bar resolves: ask DeepSeek to explain itself, store result."""
    if not config.deepseek_api_key:
        return
    try:
        text = await run_postmortem(
            api_key=config.deepseek_api_key,
            ds_record=ds_record,
            actual_direction=actual_direction,
            end_price=end_price,
            updated_klines=klines,
            updated_features=features or {},
            updated_dashboard=dashboard_signals or {},
        )
        ws = ds_record.get("window_start", 0)
        if text:
            _safe_storage(storage.store_postmortem, ws, text)
            logger.info("Postmortem stored for bar %.0f", ws)
        # Always embed after postmortem attempt — full record with postmortem if available
        base = embed_rec if embed_rec is not None else ds_record
        asyncio.create_task(_embed_bar_background(ws, {**base, "postmortem": text or ""}))
    except Exception as exc:
        logger.warning("Postmortem background task failed: %s", exc)
        # Still embed even if postmortem failed — full record minus postmortem text
        ws = ds_record.get("window_start", 0)
        base = embed_rec if embed_rec is not None else ds_record
        asyncio.create_task(_embed_bar_background(ws, {**base, "postmortem": ""}))


# ── Cohere embedding background task ─────────────────────────

async def _embed_bar_background(window_start: float, bar_record: dict):
    """
    Fire after bar resolves: embed full bar text via Cohere and store in
    pattern_history.embedding via pgvector. No local cache — PostgreSQL owns it.
    """
    if not config.cohere_api_key:
        return
    try:
        text = _bar_embed_text(bar_record)
        vec  = await embed_text(config.cohere_api_key, text, input_type="search_document")
        await asyncio.to_thread(store_bar_embedding, window_start, vec)
        logger.info("Cohere embedding stored in pgvector for bar %.0f (%d dims)", window_start, len(vec))
    except CohereUnavailableError as exc:
        logger.warning("Cohere embed background failed (bar %.0f): %s", window_start, exc)
    except Exception as exc:
        logger.warning("Embedding background task failed: %s", exc)


# ── DeepSeek task ─────────────────────────────────────────────

async def _run_deepseek(
    prices, klines, features, strategy_preds, rolling_acc, ds_acc,
    window_start_time, window_end_time, window_start_price,
    ensemble_result=None, polymarket_slug=None, dashboard_signals=None,
    indicator_accuracy=None, ensemble_weights=None,
    historical_analysis=None, creative_edge=None, dashboard_accuracy=None,
    dry_run=False, binance_expert_task=None,
):
    """Fire DeepSeek at bar open; stage result until bar closes."""
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))
    logger.info(">>> DeepSeek FIRED for bar %s", bar_ts)
    try:
        # Await Binance expert (was created ~15-25s ago after dashboard signals arrived).
        # DeepSeek itself takes 30-50s, so waiting here costs nothing — expert finishes first.
        binance_expert_result = None
        if binance_expert_task is not None:
            try:
                binance_expert_result = await asyncio.wait_for(
                    asyncio.shield(binance_expert_task), timeout=25.0
                )
                if binance_expert_result:
                    current_state["bar_binance_expert"] = _json_safe(binance_expert_result)
                    logger.info("Binance expert result received before DeepSeek API call — injected into prompt")
            except asyncio.TimeoutError:
                logger.warning("Binance expert timed out (25s) — DeepSeek fires without it")
            except Exception as bx_exc:
                logger.warning("Binance expert task error: %s — DeepSeek fires without it", bx_exc)

        neutral_analysis = _safe_storage(storage.get_neutral_analysis, default={})
        result = await deepseek.predict(
            prices=prices, klines=klines, features=features,
            strategy_preds=strategy_preds, recent_accuracy=rolling_acc,
            deepseek_accuracy=ds_acc, window_start_time=window_start_time,
            window_start_price=window_start_price, polymarket_slug=polymarket_slug,
            ensemble_result=ensemble_result, dashboard_signals=dashboard_signals,
            indicator_accuracy=indicator_accuracy, ensemble_weights=ensemble_weights,
            historical_analysis=historical_analysis, creative_edge=creative_edge,
            dashboard_accuracy=dashboard_accuracy, neutral_analysis=neutral_analysis,
            binance_expert_analysis=binance_expert_result,
        )

        # Stale-window guard
        current_bar = current_state.get("window_start_time")
        if current_bar is not None and abs(current_bar - window_start_time) > 30:
            logger.warning("DeepSeek stale result DISCARDED for bar %s", bar_ts)
            return

        logger.info(">>> DeepSeek COMPLETED bar %s → %s %d%% in %.1fs — held until close",
                    bar_ts, result.get("signal"), result.get("confidence", 0),
                    result.get("latency_ms", 0) / 1000)

        if result["signal"] not in ("ERROR", "UNAVAILABLE") and not dry_run:
            chart_path = generate_bar_chart(klines, window_start_time,
                                            result["signal"], result.get("confidence", 50))
            _safe_storage(
                storage.store_deepseek_prediction,
                window_start=window_start_time, window_end=window_end_time,
                start_price=window_start_price, signal=result["signal"],
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

        if result["signal"] not in ("ERROR", "UNAVAILABLE"):
            current_state["pending_deepseek_prediction"] = result
            current_state["pending_deepseek_ready"]      = True
            logger.info("DeepSeek STAGED for bar %s — awaiting bar close", bar_ts)
        else:
            logger.warning("DeepSeek returned %s for bar %s — not staging", result["signal"], bar_ts)

    except Exception as exc:
        logger.error("_run_deepseek FAILED for bar %s: %s", bar_ts, exc)


# ── Core prediction logic ─────────────────────────────────────

def _data_quality_check(prices: list, klines: list) -> tuple:
    if len(prices) < 30:
        return False, f"insufficient tick prices ({len(prices)}/30 required)"
    if not klines or len(klines) < 20:
        return False, f"insufficient klines ({len(klines) if klines else 0}/20 required)"
    try:
        last_bar_ts = int(klines[-1][0]) / 1000
        stale_secs  = time.time() - last_bar_ts
        if stale_secs > 180:
            return False, f"klines stale ({stale_secs:.0f}s old)"
    except Exception:
        pass
    return True, ""


async def _run_full_prediction(prices, is_force=False):
    """Open a window, run ensemble + specialists + DeepSeek, update current_state."""
    window_start_price = prices[-1]
    now                = time.time()
    window_start_time  = now - (now % 300) if not is_force else now

    current_state["window_start_price"]          = window_start_price
    current_state["window_start_time"]           = window_start_time
    current_state["specialist_completed_at"]     = None
    current_state["pending_deepseek_prediction"] = None
    current_state["pending_deepseek_ready"]      = False
    current_state["bar_historical_analysis"]     = ""
    current_state["bar_historical_context"]      = ""
    current_state["bar_binance_expert"]          = {}

    tag = "FORCE" if is_force else "BAR OPEN"
    logger.info("=== %s #%d === %s | price=%.2f ===",
                tag, deepseek.window_count + 1 if deepseek else 0,
                time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time)), window_start_price)

    poly_prob     = polymarket_feed.implied_prob if polymarket_feed.is_live else None
    klines        = list(binance_klines)
    strategy_preds = get_all_predictions(prices, ohlcv=klines, polymarket_prob=poly_prob)
    features_dict  = feature_engine.compute_all(prices, ohlcv=klines or None)
    try:
        strategy_preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=klines)
    except Exception as lr_exc:
        logger.warning("Lin Reg predict error: %s", lr_exc)

    _features_ok = bool(features_dict and len(features_dict) > 5)

    # Concurrent: dashboard signals + specialist + historical analyst all fire together
    dashboard_signals = None
    dashboard_task    = asyncio.create_task(
        fetch_dashboard_signals(
            coinalyze_key=config.coinalyze_key,
            coinglass_key=config.coinglass_key,
        )
    )
    dashboard_acc = compute_dashboard_accuracy(200)

    _all_history = load_pattern_history()[-10000:]

    # Seed the similarity search with the previous bar's dashboard signals.
    # The current bar's dashboard hasn't arrived yet (it's fetching concurrently),
    # but microstructure is sticky bar-to-bar so last bar's signals are a good proxy.
    # This means all 16 feature dimensions (8 indicators + 8 dashboard) are populated
    # rather than leaving the dashboard half as zeros.
    _prev_dash_raw: Dict = {}
    try:
        _prev_snap = current_state.get("backend_snapshot") or {}
        _prev_ds   = _prev_snap.get("dashboard_signals")
        if _prev_ds:
            _parsed = json.loads(_prev_ds) if isinstance(_prev_ds, str) else _prev_ds
            _prev_dash_raw = extract_signal_directions(_parsed)
    except Exception:
        pass

    # Historical analyst task — fires immediately with pre-specialist data (indicators +
    # strategy votes are already available; specialist signals added to context after)
    historical_task = None
    if deepseek and _features_ok:
        historical_task = asyncio.create_task(
            run_historical_analyst(
                config.deepseek_api_key, _all_history, features_dict,
                {k: v for k, v in strategy_preds.items()},
                window_start_time=window_start_time,
                specialist_signals=None, creative_edge="",
                ensemble_signal="", ensemble_conf=0.0,
                dashboard_directions=_prev_dash_raw or None,
                cohere_api_key=config.cohere_api_key,
                pgvector_search_fn=pgvector_search,
            )
        )

    # Step 1 — Unified specialist (runs concurrently with historical + dashboard)
    creative_edge      = None
    specialist_results = {}
    if deepseek and klines and _features_ok:
        try:
            spec_raw = await asyncio.wait_for(
                run_specialists(config.deepseek_api_key, klines), timeout=100.0,
            )
            if isinstance(spec_raw, tuple):
                specialist_results, creative_edge = spec_raw
                for key, result in specialist_results.items():
                    if result is not None:
                        strategy_preds[key] = result
                current_state["bar_specialist_signals"] = _json_safe(specialist_results)
                current_state["bar_creative_edge"]      = creative_edge or ""
        except asyncio.TimeoutError:
            logger.warning("Unified specialist timed out")
        except Exception as exc:
            logger.warning("Unified specialist error: %s", exc)
        current_state["specialist_completed_at"] = time.time()

    # Step 2 — Dashboard signals
    try:
        dashboard_signals = await asyncio.wait_for(dashboard_task, timeout=10.0)
        n_ok = sum(1 for k, v in dashboard_signals.items() if v is not None and k != "fetched_at")
        logger.info("Dashboard signals ready: %d sources ok", n_ok)
    except asyncio.TimeoutError:
        logger.warning("Dashboard signals timed out")
        dashboard_task.cancel()
    except Exception as exc:
        logger.warning("Dashboard signals error: %s", exc)

    # Step 3 — Inject dashboard into ensemble + fire Binance expert immediately
    binance_expert_task = None
    if dashboard_signals:
        dash_preds = _dashboard_signals_to_preds(dashboard_signals)
        strategy_preds.update(dash_preds)
        if deepseek:
            binance_expert_task = asyncio.create_task(
                run_binance_expert(config.deepseek_api_key, dashboard_signals)
            )
            logger.info("Binance expert task fired")

    # Step 4 — Ensemble
    pred                              = ensemble.predict(strategy_preds)
    strategy_preds                    = _json_safe(strategy_preds)
    pred["source"]                    = "ensemble"
    current_state["prediction"]       = pred
    current_state["ensemble_prediction"] = pred
    current_state["strategies"]       = strategy_preds
    current_state["agree_accuracy"]   = _safe_storage(storage.get_agree_accuracy, default={})

    # Step 5 — Check if historical analyst already finished (it ran concurrently).
    # DeepSeek fires NOW regardless — historical analysis is an enrichment, not a prerequisite.
    # If the historical task is still running when DeepSeek fires, it gets None (prompt shows
    # the "did not fire" placeholder). The task is then cancelled to avoid wasting API quota.
    historical_analysis = None
    hist_signal         = None
    dash_directions: Dict = {}
    if dashboard_signals:
        try: dash_directions = extract_signal_directions(dashboard_signals)
        except: pass

    if deepseek and _features_ok and historical_task:
        if historical_task.done():
            try:
                hist_result = historical_task.result()
                if isinstance(hist_result, tuple) and len(hist_result) == 2:
                    hist_signal, historical_analysis = hist_result
                # Clear unavailable flag if Cohere came back
                current_state["service_unavailable"]        = False
                current_state["service_unavailable_reason"] = ""
            except CohereUnavailableError as cohere_exc:
                logger.error("Cohere unavailable — pausing predictions: %s", cohere_exc)
                current_state["service_unavailable"]        = True
                current_state["service_unavailable_reason"] = str(cohere_exc)
                return None, {}, None, None, window_start_time, window_start_price
            except Exception as exc:
                logger.warning("Historical analyst task error (ignored): %s", exc)
            if historical_analysis and hist_signal:
                current_state["bar_historical_analysis"] = historical_analysis
                current_state["bar_historical_context"] = _build_current_bar(
                    features_dict, {k: v for k, v in strategy_preds.items()},
                    window_start_time, specialist_results, creative_edge or "",
                    pred["signal"], pred["confidence"], dash_directions,
                )
                strategy_preds["historical_analyst"] = hist_signal
                logger.info("Historical analyst finished before DeepSeek — injected into prompt")
            else:
                logger.info("Historical analyst done but returned no output — DeepSeek fires without it")
        else:
            logger.info("Historical analyst still running — DeepSeek fires without waiting (task cancelled)")
            historical_task.cancel()

    pm_odds_open = polymarket_feed.market_odds if polymarket_feed.is_live else None
    pm_ev_open   = (calculate_ev(pred["confidence"], pm_odds_open).expected_value
                    if pm_odds_open else None)

    # Accuracy stats + weight update
    rolling_acc = 0.0; ds_acc = {}; indicator_acc_full = {}
    if deepseek and features_dict:
        result = _safe_storage(storage.get_rolling_accuracy, config.rolling_window_size, default=(0, 0, 0.0))
        _, _, rolling_acc = result
        rolling_acc = rolling_acc or 0.0
        ds_acc = _safe_storage(storage.get_deepseek_accuracy, default={})

    indicator_acc_full = _safe_storage(storage.get_strategy_accuracy_full, 100, default={})
    dash_acc2 = compute_dashboard_accuracy(100)
    for name, stats in dash_acc2.items():
        indicator_acc_full[f"dash:{name}"] = stats
    if indicator_acc_full:
        ensemble.update_weights_from_full_stats(indicator_acc_full)

    # Backend snapshot
    current_state["backend_snapshot"] = _json_safe({
        "window_num":          deepseek.window_count + 1 if deepseek else 0,
        "window_start":        window_start_time,
        "window_start_price":  window_start_price,
        "prices_last20":       list(prices[-20:]) if len(prices) >= 20 else list(prices),
        "features":            features_dict,
        "strategy_preds":      strategy_preds,
        "dashboard_signals":   dashboard_signals,
        "ensemble_result": {
            "signal":              pred["signal"],
            "confidence":          pred["confidence"],
            "bullish_count":       pred["bullish_count"],
            "bearish_count":       pred["bearish_count"],
            "up_probability":      pred.get("up_probability", 0.5),
            "weighted_up_score":   pred.get("weighted_up_score", 0),
            "weighted_down_score": pred.get("weighted_down_score", 0),
        },
        "polymarket":  polymarket_feed.to_dict(),
        "rolling_acc": rolling_acc,
        "ds_acc":      ds_acc,
        "captured_at": time.time(),
    })

    # Fire DeepSeek (non-blocking) — always fires if data is available
    if deepseek and _features_ok:
        ds_strategy_preds = {
            k: v for k, v in strategy_preds.items()
            if k not in SPECIALIST_KEYS and not k.startswith("dash:")
            and k != "historical_analyst"
        }
        asyncio.create_task(
            _run_deepseek(
                prices=list(prices), klines=list(binance_klines),
                features=features_dict, strategy_preds=ds_strategy_preds,
                rolling_acc=rolling_acc, ds_acc=ds_acc,
                window_start_time=window_start_time,
                window_end_time=window_start_time + config.window_duration_seconds,
                window_start_price=window_start_price,
                ensemble_result=pred, polymarket_slug=polymarket_feed.active_slug,
                dashboard_signals=dashboard_signals,
                indicator_accuracy=indicator_acc_full,
                ensemble_weights=ensemble.get_weights(),
                historical_analysis=historical_analysis,
                creative_edge=creative_edge, dashboard_accuracy=dashboard_acc,
                binance_expert_task=binance_expert_task,
            )
        )

    return pred, strategy_preds, pm_odds_open, pm_ev_open, window_start_time, window_start_price


async def _resolve_window(
    window_start_time, window_start_price, pred, strategy_preds, pm_odds_open, pm_ev_open
):
    """Persist and resolve a closed window. Runs as a background task."""
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))

    # Promote pending DeepSeek result → revealed to UI now
    pending = current_state.get("pending_deepseek_prediction")
    if pending and pending.get("signal") not in (None, "ERROR", "UNAVAILABLE"):
        current_state["deepseek_prediction"]        = pending
        current_state["pending_deepseek_prediction"] = None
        current_state["pending_deepseek_ready"]      = False
        logger.info(">>> Bar %s CLOSED — DeepSeek REVEALED: %s %d%%",
                    bar_ts, pending["signal"], pending.get("confidence", 0))
    else:
        logger.warning("Bar %s closed — no pending DeepSeek result to reveal", bar_ts)

    end_prices = collector.get_prices(1)
    if end_prices:
        end_price = end_prices[-1]
        actual    = "UP" if end_price >= window_start_price else "DOWN"
        correct   = actual == pred["signal"]

        await _safe_storage_async(
            storage.store_prediction,
            window_start=window_start_time,
            window_end=window_start_time + config.window_duration_seconds,
            start_price=window_start_price, signal=pred["signal"],
            confidence=pred["confidence"], strategy_votes=strategy_preds,
            market_odds=pm_odds_open, ev=pm_ev_open,
        )
        await _safe_storage_async(storage.resolve_prediction, window_start_time, end_price)

        snap            = current_state.get("backend_snapshot") or {}
        snap_indicators = snap.get("features", {})
        snap_ds_raw     = snap.get("dashboard_signals")
        snap_dash_raw   = {}
        if snap_ds_raw:
            try:
                snap_dash_signals = (
                    json.loads(snap_ds_raw) if isinstance(snap_ds_raw, str) else snap_ds_raw
                )
                snap_dash_raw = extract_signal_directions(snap_dash_signals)
            except Exception:
                pass

        ds_pred_snap = current_state.get("deepseek_prediction") or {}
        ds_correct   = (actual == ds_pred_snap["signal"]
                        if ds_pred_snap.get("signal") not in (None, "ERROR", "UNAVAILABLE")
                        else None)

        await _safe_storage_async(storage.resolve_deepseek_prediction, window_start_time, end_price)

        # Safeguard: warn if no DeepSeek record was stored for this bar
        if ds_pred_snap.get("signal") in (None, "ERROR", "UNAVAILABLE"):
            bar_num = deepseek.window_count if deepseek else "?"
            logger.warning(
                "SAFEGUARD: bar %s (#%s) closed with no valid DeepSeek record "
                "(signal=%s) — ensemble result still stored, but DS history is incomplete",
                bar_ts, bar_num, ds_pred_snap.get("signal"),
            )
            _error_log.append({
                "window_start":  window_start_time,
                "bar_time":      bar_ts,
                "bar_num":       bar_num,
                "signal":        ds_pred_snap.get("signal") or "NONE",
                "reasoning":     ds_pred_snap.get("reasoning", ""),
                "raw_response":  ds_pred_snap.get("raw_response", "")[:2000],
                "logged_at":     time.time(),
            })

        # Build embed record first — shared by initial embed AND postmortem re-embed
        _ds_correct_embed = (
            None if ds_pred_snap.get("signal") == "NEUTRAL"
            else (actual == ds_pred_snap["signal"]) if ds_pred_snap.get("signal") in ("UP", "DOWN")
            else None
        )
        _n_bull = sum(1 for v in strategy_preds.values()
                      if isinstance(v, dict) and v.get("signal") == "UP")
        _n_bear = sum(1 for v in strategy_preds.values()
                      if isinstance(v, dict) and v.get("signal") == "DOWN")
        _embed_rec = {
            "window_start":         window_start_time,
            "window_count":         ds_pred_snap.get("window_count") or (deepseek.window_count if deepseek else 0),
            "actual_direction":     actual,
            "start_price":          window_start_price,
            "end_price":            end_price,
            "latency_ms":           ds_pred_snap.get("latency_ms", 0),
            "ensemble_signal":      pred.get("signal", ""),
            "ensemble_conf":        pred.get("confidence", 0),
            "ensemble_bullish":     _n_bull,
            "ensemble_bearish":     _n_bear,
            "deepseek_signal":      ds_pred_snap.get("signal", ""),
            "deepseek_conf":        ds_pred_snap.get("confidence", 0),
            "deepseek_correct":     _ds_correct_embed,
            "deepseek_reasoning":   ds_pred_snap.get("reasoning", ""),
            "deepseek_narrative":   ds_pred_snap.get("narrative", ""),
            "deepseek_free_obs":    ds_pred_snap.get("free_observation", ""),
            "historical_analyst":   strategy_preds.get("historical_analyst", {}),
            "indicators":           current_state.get("backend_snapshot", {}).get("features", {}),
            "strategy_votes":       strategy_preds,
            "specialist_signals":   current_state.get("bar_specialist_signals", {}),
            "dashboard_signals_raw": snap_dash_raw,
            "creative_edge":            current_state.get("bar_creative_edge", ""),
            "historical_analysis":      current_state.get("bar_historical_analysis", ""),
            "binance_expert_analysis":  current_state.get("bar_binance_expert", {}),
            "full_prompt":              ds_pred_snap.get("full_prompt", ""),
            "session":                  None,
            "postmortem":               "",   # filled in by postmortem handler before embedding
        }

        # Fire postmortem in background — non-blocking, best effort
        # Embedding happens INSIDE the postmortem handler once the full record is complete.
        # For bars with no postmortem (None/ERROR/UNAVAILABLE), embed now with what we have.
        if ds_pred_snap.get("signal") not in (None, "ERROR", "UNAVAILABLE"):
            _pm_record = {**ds_pred_snap, "window_start": window_start_time}
            _pm_klines = list(binance_klines) if binance_klines else []
            _pm_features = dict(current_state.get("backend_snapshot", {}).get("features", {}))
            _pm_dash = current_state.get("backend_snapshot", {}).get("dashboard_signals")
            if isinstance(_pm_dash, str):
                try: _pm_dash = json.loads(_pm_dash)
                except: _pm_dash = {}
            asyncio.create_task(_run_postmortem_background(
                ds_record=_pm_record,
                actual_direction=actual,
                end_price=end_price,
                klines=_pm_klines,
                features=_pm_features,
                dashboard_signals=_pm_dash,
                embed_rec=_embed_rec,
            ))
        else:
            # ERROR/UNAVAILABLE/None — logged to error tab, never embedded
            pass

        ens_at_total, ens_at_correct, ens_at_acc, *_ = (
            await _safe_storage_async(storage.get_total_accuracy, default=(0, 0, 0.0, 0))
        ) or (0, 0, 0.0, 0)
        ds_acc_snap = (
            await _safe_storage_async(storage.get_deepseek_accuracy, default={"total": 0, "correct": 0, "accuracy": 0.0})
        ) or {}
        agree_snap  = (
            await _safe_storage_async(storage.get_agree_accuracy, default={"total_agree": 0, "correct_agree": 0, "accuracy_agree": 0.0})
        ) or {}

        accuracy_snapshot: dict = {
            "ensemble_accuracy": round(ens_at_acc * 100, 2),
            "ensemble_total":    ens_at_total,
            "ensemble_correct":  ens_at_correct,
            "deepseek_accuracy": round(ds_acc_snap.get("accuracy", 0.0) * 100, 2),
            "deepseek_total":    ds_acc_snap.get("total", 0),
            "deepseek_correct":  ds_acc_snap.get("correct", 0),
            "agree_accuracy":    round(agree_snap.get("accuracy_agree", 0.0) * 100, 2),
            "agree_total":       agree_snap.get("total_agree", 0),
            "agree_correct":     agree_snap.get("correct_agree", 0),
            "best_indicator":          None,
            "best_indicator_accuracy": None,
            "best_indicator_total":    None,
            "best_indicator_wins":     None,
        }

        try:
            _ds_signal   = ds_pred_snap.get("signal", "")
            _trade_action = _ds_signal if _ds_signal in ("UP", "DOWN", "NEUTRAL") else "NEUTRAL"
            append_resolved_window(
                window_start       = window_start_time,
                window_end         = window_start_time + config.window_duration_seconds,
                actual_direction   = actual,
                start_price        = window_start_price,
                end_price          = end_price,
                ensemble_signal    = pred["signal"],
                ensemble_conf      = pred["confidence"],
                ensemble_correct   = (actual == pred["signal"]),
                deepseek_signal    = _ds_signal,
                deepseek_conf      = ds_pred_snap.get("confidence", 0),
                deepseek_correct   = ds_correct,
                deepseek_reasoning = ds_pred_snap.get("reasoning", ""),
                deepseek_narrative = ds_pred_snap.get("narrative", ""),
                deepseek_free_obs  = ds_pred_snap.get("free_observation", ""),
                specialist_signals         = current_state.get("bar_specialist_signals", {}),
                creative_edge              = current_state.get("bar_creative_edge", ""),
                historical_analysis        = current_state.get("bar_historical_analysis", ""),
                binance_expert_analysis    = current_state.get("bar_binance_expert", {}),
                strategy_votes             = strategy_preds,
                indicators                 = snap_indicators,
                dashboard_signals_raw      = snap_dash_raw,
                accuracy_snapshot          = accuracy_snapshot,
                full_prompt                = ds_pred_snap.get("full_prompt", ""),
                trade_action               = _trade_action,
                window_count               = ds_pred_snap.get("window_count") or (deepseek.window_count if deepseek else 0),
            )
        except Exception as ph_exc:
            logger.warning("Pattern history append failed: %s", ph_exc)

        try:
            all_ind   = compute_all_indicator_accuracy()
            best_name = all_ind.pop("best_indicator", None)
            if best_name and best_name in all_ind:
                bi = all_ind[best_name]
                accuracy_snapshot["best_indicator"]          = best_name
                accuracy_snapshot["best_indicator_accuracy"] = round(bi["accuracy"] * 100, 2)
                accuracy_snapshot["best_indicator_total"]    = bi["total"]
                accuracy_snapshot["best_indicator_wins"]     = bi["wins"]
        except Exception as bi_exc:
            logger.warning("Best-indicator snapshot failed: %s", bi_exc)

        await _safe_storage_async(storage.store_accuracy_snapshot, window_start_time, accuracy_snapshot)

        # Reset bar-level state (historical analysis cleared at next bar open, not here)
        current_state["bar_specialist_signals"]  = {}
        current_state["bar_creative_edge"]       = ""
        current_state["bar_binance_expert"]      = {}

        logger.info("Window closed | actual:%s | predicted:%s | %s | Δ%.2f",
                    actual, pred["signal"], "WIN" if correct else "LOSS",
                    end_price - window_start_price)

    current_state["agree_accuracy"] = await _safe_storage_async(storage.get_agree_accuracy, default={})
    result = await _safe_storage_async(storage.get_rolling_accuracy, default=(0, 0, 0.0))
    total  = result[0] if result else 0
    if total >= config.min_predictions_for_weight_update:
        acc = await _safe_storage_async(storage.get_strategy_rolling_accuracy, default={})
        dash_acc_resolve = compute_dashboard_accuracy(20)
        for name, stats in dash_acc_resolve.items():
            acc[f"dash:{name}"] = stats["accuracy"]
        if acc:
            ensemble.update_weights(acc)


# ── Background tasks ─────────────────────────────────────────

async def _refresh_indicators():
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
        existing = current_state.get("strategies") or {}
        for key in SPECIALIST_KEYS:
            if key in existing:
                preds[key] = existing[key]
        for key, val in existing.items():
            if key.startswith("dash:"):
                preds[key] = val
        current_state["strategies"] = _json_safe(preds)
    except Exception as exc:
        logger.warning("Indicator refresh error: %s", exc)


async def run_binance_feed():
    """Fetch 1m OHLCV every 60s — Bybit primary, OKX fallback, Kraken fallback, Binance last resort."""
    global binance_klines
    while True:
        fetched = False

        # 1. Bybit klines [ts_ms, open, high, low, close, volume] — same layout as Binance
        try:
            connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(
                    "https://api.bybit.com/v5/market/kline",
                    params={"category": "spot", "symbol": "BTCUSDT", "interval": "1", "limit": "500"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Bybit returns newest-first; reverse so oldest-first like Binance
                        bars = list(reversed(data["result"]["list"]))
                        # Convert to Binance kline format: [open_time_ms, o, h, l, close, vol]
                        klines = [[int(b[0]), b[1], b[2], b[3], b[4], b[5]] for b in bars]
                        binance_klines.clear()
                        binance_klines.extend(klines)
                        logger.info("Bybit klines updated: %d candles", len(klines))
                        collector.seed_from_klines(binance_klines)
                        await _refresh_indicators()
                        fetched = True
        except Exception as exc:
            logger.warning("Bybit klines error: %s — trying OKX", exc)

        # 2. OKX klines fallback — no API key, India-accessible, returns oldest-first
        # Response: [[ts_ms, open, high, low, close, vol, volCcy, volCcyQuote, confirm], ...]
        if not fetched:
            try:
                connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        "https://www.okx.com/api/v5/market/history-candles",
                        params={"instId": "BTC-USDT", "bar": "1m", "limit": "300"},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            # OKX returns newest-first; reverse so oldest-first
                            bars = list(reversed(data["data"]))
                            # Convert to Binance format: [open_time_ms, o, h, l, close, vol]
                            klines = [[int(b[0]), b[1], b[2], b[3], b[4], b[5]] for b in bars]
                            binance_klines.clear()
                            binance_klines.extend(klines)
                            logger.info("OKX klines updated: %d candles", len(klines))
                            collector.seed_from_klines(binance_klines)
                            await _refresh_indicators()
                            fetched = True
            except Exception as exc:
                logger.warning("OKX klines error: %s — trying Kraken", exc)

        # 3. Kraken OHLC fallback [time, open, high, low, close, vwap, volume, count]
        if not fetched:
            try:
                connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        "https://api.kraken.com/0/public/OHLC",
                        params={"pair": "XBTUSD", "interval": "1"},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            bars = list(data["result"].values())[0]
                            # Convert to Binance format: [open_time_ms, o, h, l, close, vol]
                            klines = [[int(b[0]) * 1000, b[1], b[2], b[3], b[4], b[6]] for b in bars]
                            binance_klines.clear()
                            binance_klines.extend(klines)
                            logger.info("Kraken klines updated: %d candles", len(klines))
                            collector.seed_from_klines(binance_klines)
                            await _refresh_indicators()
                            fetched = True
            except Exception as exc:
                logger.warning("Kraken klines error: %s", exc)

        # 4. Binance last resort
        if not fetched:
            try:
                connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(
                        "https://api.binance.com/api/v3/klines",
                        params={"symbol": "BTCUSDT", "interval": "1m", "limit": "500"},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            binance_klines.clear()
                            binance_klines.extend(data)
                            logger.info("Binance klines updated: %d candles", len(data))
                            collector.seed_from_klines(binance_klines)
                            await _refresh_indicators()
            except Exception as exc:
                logger.warning("Binance klines error: %s", exc)

        await asyncio.sleep(60)


async def run_indicator_refresh():
    await asyncio.sleep(20)
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


async def run_prediction_loop():
    """5-minute bar loop: predict → sleep → resolve (background) → repeat."""
    await asyncio.sleep(5)
    while True:
        try:
            prices = collector.get_prices(400)
            klines = list(binance_klines)
            ok, reason = _data_quality_check(prices, klines)
            if not ok:
                logger.warning("SKIP PREDICTION — %s — waiting 15s", reason)
                await asyncio.sleep(15)
                continue

            pred, strategy_preds, pm_odds_open, pm_ev_open, window_start_time, window_start_price = \
                await _run_full_prediction(prices)

            bar_close  = window_start_time + config.window_duration_seconds
            sleep_secs = max(1, bar_close - time.time())
            logger.info("Prediction loop: bar %s sleeping %.1fs until close",
                        time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time)), sleep_secs)
            await asyncio.sleep(sleep_secs)

            asyncio.create_task(_resolve_window(
                window_start_time, window_start_price,
                pred, strategy_preds, pm_odds_open, pm_ev_open,
            ))
            await asyncio.sleep(0)

        except Exception as exc:
            logger.error("Prediction loop CRASHED — recovering in 10s: %s", exc, exc_info=True)
            await asyncio.sleep(10)
