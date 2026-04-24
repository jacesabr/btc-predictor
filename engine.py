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
import os
import pathlib
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
import numpy as np
# matplotlib is imported lazily inside generate_bar_chart — importing it at the
# module level adds ~1–2s to cold boot. Charts are only generated at bar-close
# so the hot path (tick collector, first /price response) is faster without it.

try:
    from dotenv import load_dotenv
    load_dotenv(pathlib.Path(__file__).parent / ".env")
except ImportError:
    pass

from config import Config
from data_feed import BinanceCollector, FeatureEngine, PolymarketFeed
from signals import fetch_dashboard_signals, extract_signal_directions
from storage_pg import StoragePG as Storage, get_storage
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
    run_embedding_audit, load_embedding_audit_log as _load_embedding_audit_ndjson,
    set_flag_callback, set_audit_persist_callback,
)
import trader_summary

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Venice summarizer — read env vars directly to keep config.py untouched.
# The summarizer is optional/non-persistent, so it's gated purely on presence of the key.
VENICE_API_KEY = os.environ.get("VENICE_API_KEY", "")
VENICE_MODEL   = os.environ.get("VENICE_MODEL", "qwen3-next-80b")

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

# In-memory error log — ERROR/UNAVAILABLE bars + non-fatal DeepSeek flags
# (DATA_GAP / FREE_OBS / SUGGESTION) emitted by ai.py via set_flag_callback.
# Hydrated from Postgres `events` table at startup so it survives deploys.
_error_log: list = []
_ERROR_LOG_MAX = 500   # bound memory; oldest entries evicted past this


def _record_deepseek_flag(source: str, kind: str, message: str, ctx: dict) -> None:
    """Append a non-fatal DeepSeek flag to the in-memory error log AND persist
    it to Postgres. Wired into ai.set_flag_callback at startup. Skips empty/NONE
    messages (the scanner already filters those, but defend anyway).
    """
    msg = (message or "").strip()
    if not msg or msg.upper() == "NONE":
        return
    ws = float(ctx.get("window_start_time") or 0.0)
    bar_ts = (
        time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ws))
        if ws else (ctx.get("bar_ts") or "")
    )
    now_ts = time.time()
    raw_excerpt = (ctx.get("raw_excerpt") or "")[:2000]
    bar_num = ctx.get("window_count") or ""

    # In-memory for the live /errors endpoint
    _error_log.append({
        "window_start": ws,
        "bar_time":     bar_ts,
        "bar_num":      bar_num,
        "signal":       kind,           # "DATA_GAP" | "FREE_OBS" | "SUGGESTION"
        "source":       source,         # which DeepSeek call raised it
        "message":      msg,
        "reasoning":    msg,            # legacy field reused for UI back-compat
        "raw_response": raw_excerpt,
        "logged_at":    now_ts,
    })
    if len(_error_log) > _ERROR_LOG_MAX:
        del _error_log[:len(_error_log) - _ERROR_LOG_MAX]

    # Persistent row in Postgres — survives deploys
    try:
        storage.store_event(
            source=source, kind=kind, message=msg,
            bar_time=bar_ts, bar_num=str(bar_num),
            window_start=ws, raw_excerpt=raw_excerpt,
            logged_at=now_ts,
        )
    except Exception as exc:
        logger.warning("store_event failed (non-fatal): %s", exc)


def _hydrate_error_log_from_db() -> None:
    """Refill _error_log from Postgres on startup so the ERRORS tab isn't empty
    immediately after a Render redeploy."""
    try:
        rows = storage.load_recent_events(limit=_ERROR_LOG_MAX)
    except Exception as exc:
        logger.warning("hydrate_error_log failed: %s", exc)
        return
    # rows are newest-first; _error_log is oldest-first
    for r in reversed(rows):
        _error_log.append({
            "window_start": r.get("window_start") or 0.0,
            "bar_time":     r.get("bar_time") or "",
            "bar_num":      r.get("bar_num") or "",
            "signal":       r.get("kind"),
            "source":       r.get("source"),
            "message":      r.get("message"),
            "reasoning":    r.get("message"),
            "raw_response": r.get("raw_excerpt") or "",
            "logged_at":    r.get("logged_at") or 0.0,
        })
    logger.info("Hydrated _error_log from DB: %d entries", len(_error_log))


_hydrate_error_log_from_db()
set_flag_callback(_record_deepseek_flag)


def _record_embedding_audit(audit: dict) -> None:
    """Persist an embedding audit to Postgres and push it into current_state so
    the /api/embedding-audit poller sees it immediately — regardless of whether
    the NDJSON write (ephemeral FS) succeeded."""
    try:
        current_state["embedding_audit_log"] = [
            audit,
            *current_state.get("embedding_audit_log", [])[:19],
        ]
    except Exception:
        logger.exception("push to current_state.embedding_audit_log failed")
    try:
        ok = storage.store_embedding_audit(audit)
        if not ok:
            logger.warning("embedding audit PG insert returned False")
    except Exception:
        logger.exception("embedding audit PG insert raised")


set_audit_persist_callback(_record_embedding_audit)


def load_embedding_audit_log(n: int = 20) -> list:
    """Read recent audits for the dashboard. Postgres is primary (durable across
    deploys); current_state fills in anything not yet committed; the local
    NDJSON cache is last-resort fallback for backwards compatibility.
    """
    try:
        rows = storage.load_embedding_audits(limit=n)
        if rows:
            return rows
    except Exception:
        logger.exception("load_embedding_audits (PG) failed — falling back")
    live = current_state.get("embedding_audit_log") or []
    if live:
        return live[:n]
    try:
        return _load_embedding_audit_ndjson(n=n)
    except Exception:
        return []

def _trigger_embedding_bootstrap():
    """Startup: count unembedded RESOLVED bars and log a warning event if any.

    Mass-embedding on startup is DISABLED — a silent bug in an earlier version
    repeatedly burned Cohere budget by re-embedding bars that were already in
    pgvector (the JSON blob didn't mirror the column, so coverage always
    appeared 0%). Automatic mass-embed is gone permanently. Per-bar embeds
    still happen on bar resolution (1 call each), and the current-bar query
    embed + rerank still happen per bar at prediction time — those are needed.

    If any resolved bars lack vectors, the operator can trigger a small,
    rate-limited embed via POST /admin/embed-missing?limit=N.
    """
    if not config.cohere_api_key:
        logger.info("Embedding coverage check skipped: Cohere key not configured")
        return
    try:
        from semantic_store import embedded_window_starts, load_all as _load_all
        embedded = embedded_window_starts()
        all_bars = _load_all()
        resolved = [r for r in all_bars if r.get("actual_direction")]
        missing  = [r for r in resolved if r.get("window_start") not in embedded]
        total, have, gap = len(resolved), len(resolved) - len(missing), len(missing)
        coverage = have / total * 100 if total else 100.0
        logger.info("Embedding coverage: %d/%d resolved bars have vectors (%.1f%%)",
                    have, total, coverage)
        if gap > 0:
            msg = (f"{gap} resolved bar(s) missing Cohere embeddings "
                   f"(coverage {coverage:.1f}%). Trigger manually with "
                   f"POST /admin/embed-missing?limit=20 to fix in small batches.")
            logger.warning(msg)
            try:
                storage.store_event(
                    source="embedding",
                    kind="DATA_GAP",
                    message=msg,
                    bar_time="",
                    bar_num="",
                    window_start=0.0,
                    raw_excerpt="",
                )
            except Exception:
                pass
    except Exception as exc:
        logger.warning("Embedding coverage check failed: %s", exc)


def _dashboard_accuracy_from_records(records):
    """Compute dashboard-signal accuracy from already-loaded pattern_history rows.
    Same logic as semantic_store.compute_dashboard_accuracy, but reuses the list
    we already have in memory instead of re-querying Postgres per bar.
    """
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
            counts.setdefault(key, {"correct": 0, "total": 0})
            counts[key]["total"] += 1
            if val == actual:
                counts[key]["correct"] += 1
    return {
        k: {"accuracy": v["correct"] / v["total"], "correct": v["correct"], "total": v["total"]}
        for k, v in counts.items() if v["total"] > 0
    }


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
    "bar_historical_analysis":    "",
    "bar_historical_context":     "",
    "bar_historical_analyst_fired": False,
    "bar_binance_expert":         {},
    "trader_summary":             None,
    "service_unavailable":        False,
    "service_unavailable_reason": "",
    "embedding_audit_log":        [],
    "bar_timings":                {},    # live per-stage timings for the bar in progress
    "timings_history":            [],    # capped list of recent bar timing snapshots
}

_TIMINGS_HISTORY_MAX = 50


def _record_stage(stage: str, elapsed_s: float, ok: bool, error: str = "") -> None:
    """Record a pipeline stage timing into the live bar_timings dict."""
    try:
        current_state.setdefault("bar_timings", {}).setdefault("stages", {})[stage] = {
            "elapsed_s": round(float(elapsed_s), 2),
            "ok":        bool(ok),
            "error":     str(error)[:300] if error else "",
        }
    except Exception:
        pass


async def _build_trader_summary_bg(window_start_time: float) -> None:
    """Generate the Venice-powered trader briefing for this bar and stash it on
    current_state so the next WebSocket tick picks it up. Any failure is
    logged and the state stays None — the frontend renders the raw blocks."""
    try:
        pred = current_state.get("pending_deepseek_prediction") or {}
        if not pred or pred.get("signal") in (None, "ERROR", "UNAVAILABLE"):
            return
        # Only generate for the bar we opened this task for — if the bar has
        # already flipped to the next one, bail.
        if current_state.get("window_start_time") != window_start_time:
            return
        summary = await trader_summary.get_or_build(
            window_start_time=window_start_time,
            pred=pred,
            historical=current_state.get("bar_historical_analysis", "") or "",
            binance_expert=current_state.get("bar_binance_expert", {}) or {},
            api_key=VENICE_API_KEY,
            model=VENICE_MODEL,
        )
        # Another bar may have opened while Venice was working; don't overwrite
        # a fresher bar's state with a stale summary.
        if summary and current_state.get("window_start_time") == window_start_time:
            current_state["trader_summary"] = summary
    except Exception as exc:
        logger.warning("trader_summary bg task FAILED for bar %s: %s", window_start_time, exc)


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
    # Lazy matplotlib import — keeps cold boot ~1-2s faster when the service
    # hasn't yet needed to render a chart.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
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
                     fontsize=9, color="#1A1A1A", pad=5)
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
        plt.close("all")
        return str(fpath)
    except Exception as exc:
        plt.close("all")
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
    pattern_history.embedding via pgvector. PostgreSQL owns it.

    Fail-closed dedup: if this bar already has a vector, or the dedup check
    itself raises, do NOT call Cohere. A prior bug re-embedded everything on
    each deploy — we skip on any uncertainty rather than risk burning budget.
    store_embedding is also idempotent at the SQL layer as a last resort.
    """
    if not config.cohere_api_key:
        return
    try:
        from semantic_store import embedded_window_starts
        already = float(window_start) in embedded_window_starts()
    except Exception as exc:
        logger.warning("Cohere embed ABORTED — dedup check failed for bar %.0f: %s "
                       "(fail-closed; would rather skip one embed than risk mass re-embed)",
                       window_start, exc)
        return
    if already:
        logger.info("Cohere embed skipped — bar %.0f already has a vector", window_start)
        return
    try:
        text = _bar_embed_text(bar_record)
        vec  = await embed_text(config.cohere_api_key, text, input_type="search_document")
        store_bar_embedding(window_start, vec)
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
    historical_analysis=None, dashboard_accuracy=None,
    dry_run=False, binance_expert_analysis=None,
):
    """Fire DeepSeek at bar open; stage result until bar closes.
    binance_expert_analysis is now the materialised dict (ran serially before
    historical analyst earlier in the pipeline), not an in-flight task.
    """
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))
    logger.info(">>> DeepSeek FIRED for bar %s", bar_ts)
    try:
        neutral_analysis = _safe_storage(storage.get_neutral_analysis, default={})
        _main_t0 = time.time()
        result = await deepseek.predict(
            prices=prices, klines=klines, features=features,
            strategy_preds=strategy_preds, recent_accuracy=rolling_acc,
            deepseek_accuracy=ds_acc, window_start_time=window_start_time,
            window_start_price=window_start_price, polymarket_slug=polymarket_slug,
            ensemble_result=ensemble_result, dashboard_signals=dashboard_signals,
            indicator_accuracy=indicator_accuracy, ensemble_weights=ensemble_weights,
            historical_analysis=historical_analysis,
            dashboard_accuracy=dashboard_accuracy, neutral_analysis=neutral_analysis,
            binance_expert_analysis=binance_expert_analysis or current_state.get("bar_binance_expert") or None,
            historical_failure_note=current_state.get("bar_historical_failure_note", ""),
        )
        _record_stage("main_deepseek", time.time() - _main_t0, ok=(result.get("signal") not in ("ERROR", "UNAVAILABLE")),
                      error=(result.get("reasoning", "") if result.get("signal") in ("ERROR", "UNAVAILABLE") else ""))

        # Bar-close overrun check: if the whole pipeline ran past the bar close,
        # we discard the prediction (too late to be actionable) but keep the
        # timing breakdown so the user can see WHICH stage blew the budget.
        bar_close_ts = window_end_time
        overran = time.time() > bar_close_ts
        if overran:
            logger.warning("Pipeline OVERRAN bar close for bar %s by %.1fs — discarding prediction, keeping timings",
                           bar_ts, time.time() - bar_close_ts)
            try:
                current_state.setdefault("bar_timings", {})["overran_bar_close"] = True
                current_state["bar_timings"]["overran_by_s"] = round(time.time() - bar_close_ts, 1)
            except Exception:
                pass
            return

        # Stale-window guard (separate from overrun: a new bar already started)
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
            # Fire-and-forget: build the trader-friendly summary via Venice. Cached by
            # window_start_time, so this runs at most once per bar. Failure is silent —
            # the frontend just shows the raw blocks below when trader_summary is null.
            if VENICE_API_KEY:
                asyncio.create_task(_build_trader_summary_bg(window_start_time))
        else:
            logger.warning("DeepSeek returned %s for bar %s — not staging", result["signal"], bar_ts)

    except Exception as exc:
        logger.error("_run_deepseek FAILED for bar %s: %s", bar_ts, exc)
        try:
            current_state.setdefault("bar_timings", {})["pipeline_error"] = f"{type(exc).__name__}: {exc}"[:300]
        except Exception:
            pass
    finally:
        # Flush the bar's timing breakdown into history so the Timing tab can show it
        try:
            bt = dict(current_state.get("bar_timings") or {})
            if bt and bt.get("stages"):
                bt["completed_at"]   = time.time()
                bt["total_elapsed_s"] = round(time.time() - bt.get("started_at", time.time()), 2)
                bt["bar"]            = bar_ts
                history = current_state.setdefault("timings_history", [])
                history.append(bt)
                # Cap the history so state stays small
                if len(history) > _TIMINGS_HISTORY_MAX:
                    del history[:len(history) - _TIMINGS_HISTORY_MAX]
        except Exception as flush_exc:
            logger.warning("Failed to flush bar_timings: %s", flush_exc)


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
    current_state["bar_historical_analyst_fired"] = False
    current_state["bar_binance_expert"]          = {}
    current_state["trader_summary"]              = None
    current_state["bar_timings"]                 = {
        "window_start_time": window_start_time,
        "started_at":        now,
        "stages":            {},
    }

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
    # One DB load per bar: pull history once, derive the 200-bar dashboard view
    # from the same records instead of a second scan.
    _all_history = load_pattern_history()[-10000:]
    dashboard_acc = _dashboard_accuracy_from_records(_all_history[-200:])

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

    # Step 1 — Unified specialist
    specialist_results = {}
    if deepseek and klines and _features_ok:
        _spec_t0 = time.time()
        try:
            spec_raw = await asyncio.wait_for(
                run_specialists(config.deepseek_api_key, klines), timeout=100.0,
            )
            specialist_results = spec_raw or {}
            if specialist_results:
                for key, result in specialist_results.items():
                    if result is not None:
                        strategy_preds[key] = result
                current_state["bar_specialist_signals"] = _json_safe(specialist_results)
            _record_stage("specialists", time.time() - _spec_t0, ok=True)
        except asyncio.TimeoutError:
            logger.warning("Unified specialist timed out")
            _record_stage("specialists", time.time() - _spec_t0, ok=False, error="timeout 100s")
        except Exception as exc:
            logger.warning("Unified specialist error: %s", exc)
            _record_stage("specialists", time.time() - _spec_t0, ok=False, error=f"{type(exc).__name__}: {exc}")
        current_state["specialist_completed_at"] = time.time()

    # Step 2 — Dashboard signals
    _dash_t0 = time.time()
    try:
        dashboard_signals = await asyncio.wait_for(dashboard_task, timeout=10.0)
        n_ok = sum(1 for k, v in dashboard_signals.items() if v is not None and k != "fetched_at")
        logger.info("Dashboard signals ready: %d sources ok", n_ok)
        _record_stage("dashboard_signals", time.time() - _dash_t0, ok=True)
    except asyncio.TimeoutError:
        logger.warning("Dashboard signals timed out")
        dashboard_task.cancel()
        _record_stage("dashboard_signals", time.time() - _dash_t0, ok=False, error="timeout 10s")
    except Exception as exc:
        logger.warning("Dashboard signals error: %s", exc)
        _record_stage("dashboard_signals", time.time() - _dash_t0, ok=False, error=f"{type(exc).__name__}: {exc}")

    # Step 3 — Inject dashboard into ensemble
    if dashboard_signals:
        dash_preds = _dashboard_signals_to_preds(dashboard_signals)
        strategy_preds.update(dash_preds)

    # Step 4 — Ensemble
    pred                              = _json_safe(ensemble.predict(strategy_preds))
    strategy_preds                    = _json_safe(strategy_preds)
    pred["source"]                    = "ensemble"
    current_state["prediction"]       = pred
    current_state["ensemble_prediction"] = pred
    current_state["strategies"]       = strategy_preds
    current_state["agree_accuracy"]   = _safe_storage(storage.get_agree_accuracy, default={})

    # Step 5a — Binance expert runs BEFORE historical analyst so the historical
    # query can include the expert's conclusion. This adds ~8s to the sequential
    # path but gives retrieval a materially richer signal: past bars are scored
    # against not just raw features but also the Binance expert's read.
    dash_directions: Dict = {}
    if dashboard_signals:
        try: dash_directions = extract_signal_directions(dashboard_signals)
        except: pass

    binance_expert_result = None
    if deepseek and dashboard_signals:
        _bx_t0 = time.time()
        try:
            binance_expert_result = await asyncio.wait_for(
                run_binance_expert(config.deepseek_api_key, dashboard_signals),
                timeout=75.0,
            )
            if binance_expert_result:
                current_state["bar_binance_expert"] = _json_safe(binance_expert_result)
                logger.info("Binance expert done in %.1fs — feeding to historical analyst",
                            time.time() - _bx_t0)
            _record_stage("binance_expert", time.time() - _bx_t0, ok=bool(binance_expert_result))
        except asyncio.TimeoutError:
            logger.warning("Binance expert timed out (75s) — historical analyst fires without it")
            _record_stage("binance_expert", time.time() - _bx_t0, ok=False, error="timeout 75s")
        except Exception as bx_exc:
            logger.warning("Binance expert error: %s — historical analyst fires without it", bx_exc)
            _record_stage("binance_expert", time.time() - _bx_t0, ok=False,
                          error=f"{type(bx_exc).__name__}: {bx_exc}")

    # Step 5b — Historical analyst (now sees Binance expert output too)
    historical_analyst_fired = False
    hist_signal = None
    historical_analysis = None
    hist_failure_note: Optional[str] = None
    if deepseek and _features_ok:
        _hist_t0 = time.time()
        _hist_sub: Dict = {}   # sub-stage timings populated inside run_historical_analyst
        try:
            # No timeout — wait for the analyst to finish. If it runs past the
            # bar close, the main predictor still uses whatever context arrived
            # and the failure stage is logged to the Timing tab.
            hist_result = await run_historical_analyst(
                config.deepseek_api_key, _all_history, features_dict,
                {k: v for k, v in strategy_preds.items()},
                window_start_time=window_start_time,
                specialist_signals=specialist_results or None,
                ensemble_signal=pred["signal"],
                ensemble_conf=float(pred.get("confidence", 0.0)),
                dashboard_directions=dash_directions or None,
                dashboard_signals_raw=dashboard_signals or None,
                binance_expert_analysis=binance_expert_result,
                cohere_api_key=config.cohere_api_key,
                pgvector_search_fn=pgvector_search,
                timings_sink=_hist_sub,
            )
            _hist_elapsed = time.time() - _hist_t0
            if isinstance(hist_result, tuple) and len(hist_result) == 2:
                hist_signal, historical_analysis = hist_result
            current_state["service_unavailable"]        = False
            current_state["service_unavailable_reason"] = ""
            _record_stage("historical_total", _hist_elapsed, ok=True)
        except CohereUnavailableError as cohere_exc:
            _record_stage("historical_total", time.time() - _hist_t0, ok=False,
                          error=f"CohereUnavailable: {cohere_exc}")
            logger.error("Cohere unavailable — pausing predictions: %s", cohere_exc)
            current_state["service_unavailable"]        = True
            current_state["service_unavailable_reason"] = str(cohere_exc)
            return None, {}, None, None, window_start_time, window_start_price
        except Exception as exc:
            _hist_elapsed = time.time() - _hist_t0
            failed_stage = _hist_sub.get("_last_stage", "unknown")
            hist_failure_note = (
                f"historical analyst failed after {_hist_elapsed:.1f}s during stage "
                f"'{failed_stage}' — {type(exc).__name__}: {exc}"
            )
            logger.warning("Historical analyst ERROR after %.1fs at stage=%s: %s",
                           _hist_elapsed, failed_stage, exc)
            _record_stage("historical_total", _hist_elapsed, ok=False,
                          error=f"{type(exc).__name__} at {failed_stage}: {exc}")

        # Flatten sub-stage timings into bar_timings (skip the internal _last_stage key)
        for _k, _v in _hist_sub.items():
            if _k.startswith("_") or not isinstance(_v, dict):
                continue
            _record_stage(
                f"historical_{_k}",
                _v.get("elapsed_s", 0.0),
                ok=_v.get("ok", True),
                error=_v.get("error", ""),
            )

        if historical_analysis and hist_signal:
            historical_analyst_fired = True
            current_state["bar_historical_analysis"] = historical_analysis
            current_state["bar_historical_context"] = _build_current_bar(
                features_dict, {k: v for k, v in strategy_preds.items()},
                window_start_time, specialist_results,
                pred["signal"], pred["confidence"], dash_directions,
            )
            strategy_preds["historical_analyst"] = hist_signal
            logger.info("Historical analyst FIRED in %.1fs — %d-bar context injected into DeepSeek prompt",
                        _hist_elapsed, len(_all_history))
        else:
            logger.warning("Historical analyst NO OUTPUT after %.1fs — DeepSeek fires without similarity context", _hist_elapsed)

    # Expose to dashboard/state whether historical analyst actually fired
    current_state["bar_historical_analyst_fired"] = historical_analyst_fired
    current_state["bar_historical_failure_note"]  = hist_failure_note or ""

    # Binance expert already ran at Step 5a above (result in binance_expert_result);
    # pass it directly to the main predictor instead of an in-flight task.

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
    # Reuse the 200-bar dashboard accuracy already computed earlier in this bar
    # (was a 2nd DB load at window=100 — the 200-bar superset is strictly better).
    for name, stats in dashboard_acc.items():
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
                dashboard_accuracy=dashboard_acc,
                binance_expert_analysis=binance_expert_result,
            )
        )

    return pred, strategy_preds, pm_odds_open, pm_ev_open, window_start_time, window_start_price


async def _resolve_window(
    window_start_time, window_start_price, pred, strategy_preds, pm_odds_open, pm_ev_open
):
    """Persist and resolve a closed window. Runs as a background task."""
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))

    # Safeguard: pred must be a dict with signal/confidence
    if pred is None or not isinstance(pred, dict):
        logger.error("SAFEGUARD: _resolve_window called with pred=%s (type=%s), cannot resolve",
                     pred, type(pred).__name__)
        return

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
        correct   = None if pred["signal"] == "NEUTRAL" else (actual == pred["signal"])

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
                snap_dash_raw = (
                    json.loads(snap_ds_raw) if isinstance(snap_ds_raw, str) else snap_ds_raw
                )
            except Exception:
                pass

        ds_pred_snap = current_state.get("deepseek_prediction") or {}
        ds_correct   = (None if ds_pred_snap.get("signal") in (None, "ERROR", "UNAVAILABLE", "NEUTRAL")
                        else (actual == ds_pred_snap["signal"]))

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
            _pm_record = {**ds_pred_snap, "window_start": window_start_time, "start_price": window_start_price}
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
                ensemble_correct   = (None if pred["signal"] == "NEUTRAL" else (actual == pred["signal"])),
                deepseek_signal    = _ds_signal,
                deepseek_conf      = ds_pred_snap.get("confidence", 0),
                deepseek_correct   = ds_correct,
                deepseek_reasoning = ds_pred_snap.get("reasoning", ""),
                deepseek_narrative = ds_pred_snap.get("narrative", ""),
                deepseek_free_obs  = ds_pred_snap.get("free_observation", ""),
                specialist_signals         = current_state.get("bar_specialist_signals", {}),
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
            logger.warning("Best-indicator snapshot failed: %s", bi_exc, exc_info=True)


        # Reset bar-level state (historical analysis cleared at next bar open, not here)
        current_state["bar_specialist_signals"]  = {}
        current_state["bar_binance_expert"]      = {}

        logger.info("Window closed | actual:%s | predicted:%s | %s | Δ%.2f",
                    actual, pred["signal"], ("WIN" if correct else ("NEUTRAL" if correct is None else "LOSS")),
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
    """Fetch 1m OHLCV every 60s — Bybit primary, then OKX → Kraken → Binance."""
    global binance_klines
    while True:
        fetched = False

        # 1. Bybit klines primary [ts_ms, open, high, low, close, volume] — Binance layout
        if not fetched:
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
                            # Bybit spot v5 response per bar: [startTime, open, high, low, close, volume, turnover]
                            bars = list(reversed(data["result"]["list"]))
                            klines = [
                                [int(b[0]), b[1], b[2], b[3], b[4], b[5],
                                 None,                       # close_time
                                 b[6] if len(b) > 6 else None,   # turnover = quote vol
                                 None,                       # trades count unavailable
                                 None]                       # taker_buy_base_vol unavailable
                                for b in bars
                            ]
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
                            # OKX response per bar: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                            bars = list(reversed(data["data"]))
                            klines = [
                                [int(b[0]), b[1], b[2], b[3], b[4], b[5],
                                 None,                       # close_time
                                 b[7] if len(b) > 7 else None,   # volCcyQuote = quote vol
                                 None,                       # trades count unavailable
                                 None]                       # taker_buy_base_vol unavailable
                                for b in bars
                            ]
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
                            # Kraken OHLC per bar: [time, open, high, low, close, vwap, volume, count]
                            klines = []
                            for b in bars:
                                try:
                                    vwap_v = float(b[5])
                                    vol_v  = float(b[6])
                                    quote_approx = vwap_v * vol_v  # vwap * volume ≈ quote volume
                                except Exception:
                                    quote_approx = None
                                klines.append([
                                    int(b[0]) * 1000, b[1], b[2], b[3], b[4], b[6],
                                    None,                                       # close_time
                                    str(quote_approx) if quote_approx is not None else None,
                                    b[7] if len(b) > 7 else None,               # trade count
                                    None,                                       # taker_buy_base_vol unavailable
                                ])
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
    # Track the last bar window we processed. If a crash + 10s recovery puts us back here
    # during the SAME 5-min window, we skip re-running `_run_full_prediction` — a duplicate
    # call would reset `current_state["bar_historical_analysis"]` and orphan the in-flight
    # `_run_deepseek` task (resulting in `did not fire`/`did not complete` sections in the
    # main prompt even though the specialists actually ran).
    last_processed_window: Optional[int] = None
    while True:
        try:
            prices = collector.get_prices(400)
            klines = list(binance_klines)
            ok, reason = _data_quality_check(prices, klines)
            if not ok:
                logger.warning("SKIP PREDICTION — %s — waiting 15s", reason)
                await asyncio.sleep(15)
                continue

            now_ts = time.time()
            current_window = int(now_ts - (now_ts % 300))
            if current_window == last_processed_window:
                # Same 5-min window as our last attempt — prior run is still in flight (or crashed).
                # Sleep until bar close so we don't wipe bar_historical_analysis / bar_binance_expert.
                bar_close = current_window + config.window_duration_seconds
                wait = max(1, bar_close - time.time())
                logger.warning(
                    "Prediction loop SKIPPING duplicate start of bar %s — sleeping %.1fs until close "
                    "(prior run still in flight; would wipe bar state)",
                    time.strftime("%H:%M:%S UTC", time.gmtime(current_window)), wait,
                )
                await asyncio.sleep(wait)
                continue

            last_processed_window = current_window
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


async def run_embedding_audit_loop():
    """Fire embedding audit every 4 hours using deepseek-reasoner."""
    await asyncio.sleep(60)
    while True:
        try:
            await asyncio.sleep(14400)
            if not config.deepseek_api_key:
                logger.info("Embedding audit skipped: no DeepSeek API key")
                continue

            history = _safe_storage(load_pattern_history, default=[])
            audit_result = await run_embedding_audit(config.deepseek_api_key, history)
            # Persistence + current_state push are handled by the
            # set_audit_persist_callback hook — see _record_embedding_audit.
            if audit_result:
                logger.info("Embedding audit completed: %s", audit_result.get("audit_signal"))
            else:
                logger.warning("Embedding audit failed or was skipped")

        except Exception as exc:
            logger.error("Embedding audit loop CRASHED: %s", exc, exc_info=True)
            await asyncio.sleep(300)
