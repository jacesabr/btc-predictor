# BTC 5-Minute Predictor — Architecture

## Overview

Predicts BTC price direction over 5-minute bars and optionally trades Polymarket BTC Up/Down markets. The system fuses 16+ technical strategies, 5 DeepSeek specialist agents, and a microstructure signal layer into a weighted ensemble, then uses EV + Kelly sizing to decide whether a Polymarket bet is worth placing.

---

## File Map

```
btc-predictor/
├── storage.py          — NDJSON file persistence
├── semantic_store.py   — Pattern history / future vector embeddings
├── data_feed.py        — Live price data (Binance + Polymarket)
├── strategies.py       — All active technical strategies + ensemble
├── signals.py          — External macro/sentiment signals (CoinAPI, Coinalyze)
├── ai.py               — Full DeepSeek AI pipeline (3 sequential calls per bar)
├── engine.py           — Shared state, background tasks, prediction loop
├── server.py           — FastAPI HTTP + WebSocket layer
├── bot.py              — Polymarket paper/live trading bot
├── config.py           — All tuneable parameters (untouched)
└── specialists/        — Prompt files for DeepSeek specialist agents
    ├── unified_analyst/
    ├── historical_analyst/
    └── main_predictor/
```

---

## Files

### `storage.py`
Thin NDJSON append-and-read layer. All data lives under `results/` as line-delimited JSON files:

| File | Contents |
|------|----------|
| `results/ticks.ndjson` | Every 5-minute bar result (resolved direction, confidence, all strategy votes) |
| `results/predictions.ndjson` | Pending predictions written at bar open, resolved at bar close |
| `results/deepseek_predictions.ndjson` | Raw DeepSeek outputs, staged until bar close |
| `results/paper_trades.ndjson` | Paper-mode Polymarket trade log |
| `results/live_orders.ndjson` | Live-mode Polymarket order log |

Key exports: `Storage` (class), `_DATA_DIR`, `_read_ndjson`, `_rewrite_ndjson`.

---

### `semantic_store.py`
Stores resolved bar patterns for future semantic search / vector embedding. Each record captures the full feature snapshot of a bar alongside its outcome. Later sessions can load all records and build embeddings to retrieve "similar past setups."

Key exports: `append_resolved_window`, `load_all`, `query_similar` (placeholder for vector search).

---

### `data_feed.py`
Three live-data collectors merged into one file:

- **`BinanceCollector`** — polls Binance REST API every N seconds for 1m klines; aggregates into 5m bars.
- **`FeatureEngine`** — computes OHLCV features, RSI, ATR, VWAP, etc. from raw klines.
- **`PolymarketFeed`** — polls Polymarket Gamma API every 1s for active BTC 5-minute market slugs, mid-prices, and liquidity.

---

### `strategies.py`
All active technical strategies plus the ensemble layer. Six dead strategies were removed (Bollinger, Momentum, PriceAction, MFI, OBV, HTF-EMA).

**Active strategies (16):**
`VWAP`, `RSI`, `MACD`, `EMA`, `Supertrend`, `IchimokuCloud`, `ADX`, `VolumeProfile`, `MarketMicrostructure`, `OrderFlowImbalance`, `FibonacciRetracement`, `ElliottWave`, `WyckoffPhase`, `AlligatorStrategy`, `AccDistStrategy`, `DowTheoryStrategy`

**`EnsemblePredictor`** — weighted voting across all strategy signals + specialist outputs. Weights are loaded from `config.py` and updated via Bayesian adjustment after each resolved bar.

**`calculate_ev()`** — computes expected value of a Polymarket bet given ensemble confidence and current market price.

**`LinearRegressionChannel`** — standalone regression-channel strategy used for trend confirmation.

**`accuracy_to_label()`** — converts a float accuracy into a human-readable tier label (used by the AI prompt builder).

---

### `signals.py`
Fetches external macro and sentiment signals that don't come from Binance:

- Funding rates, open interest, liquidations (Coinalyze)
- Fear & Greed index, dominance, global market cap (CoinAPI / aggregators)

Returns a structured dict consumed by `engine.py` to enrich the prediction context.

Key exports: `fetch_dashboard_signals(coinapi_key, coinalyze_key)`, `extract_signal_directions(ds)`.

---

### `ai.py`
The full DeepSeek pipeline — three sequential API calls per bar:

```
Bar opens
   │
   ├─ 1. run_specialists()        — 5 specialist agents in parallel
   │      alligator, acc_dist, dow_theory, fib_pullback, harmonic
   │      → each reads its own prompt file from specialists/unified_analyst/
   │
   ├─ 2. run_historical_analyst() — reviews last N resolved bars
   │      → reads prompt from specialists/historical_analyst/
   │
   └─ 3. DeepSeekPredictor.predict()  — main predictor
          → reads prompt from specialists/main_predictor/
          → receives specialist outputs + historical analysis as context
          → returns { direction, confidence, reasoning }
```

Also contains all prompt-building helpers (`build_prompt`, `parse_response`) and the shared `_api_call()` wrapper with retry logic.

Key exports: `DeepSeekPredictor`, `run_specialists`, `run_historical_analyst`, `SPECIALIST_KEYS`.

---

### `engine.py`
The brain. Instantiates all shared objects and runs background async tasks.

**Shared singletons (imported by server.py and bot.py):**
```python
config, collector, storage, ensemble, lr_strategy,
feature_engine, polymarket_feed, deepseek,
current_state, ws_clients, binance_klines
```

**`current_state` dict** — the live snapshot broadcast to WebSocket clients every 2s:
```python
{
  "price": float,
  "window_start_price": float,
  "window_start": ISO timestamp,
  "window_end": ISO timestamp,
  "direction": "UP" | "DOWN" | None,
  "confidence": 0-100,
  "pending_deepseek_prediction": dict | None,   # staged, revealed at bar close
  "pending_deepseek_ready": bool,               # True = DeepSeek has responded
  "strategy_predictions": [...],
  "polymarket": {...},
  ...
}
```

**Background tasks:**
| Task | Role |
|------|------|
| `run_collector` | Keeps `binance_klines` buffer fresh (1m candles) |
| `run_binance_feed` | Drives the 5-minute bar clock (snapped to UTC) |
| `run_prediction_loop` | At each bar open: runs strategies → fires DeepSeek → broadcasts WS |
| `run_indicator_refresh` | Refreshes external signals every 60s |

**Bar lifecycle:**
1. `run_binance_feed` detects new 5m bar boundary.
2. `_run_full_prediction` — gathers all strategy votes, calls `_run_deepseek` in background.
3. `_run_deepseek` — calls `run_specialists → run_historical_analyst → DeepSeekPredictor.predict`. Sets `pending_deepseek_ready = True` when done.
4. At next bar close: `_resolve_window` — compares predicted direction to actual close, updates ensemble weights, appends to semantic store.

---

### `server.py`
Thin FastAPI layer. Imports everything from `engine.py` — no business logic here.

**Endpoints:**
| Route | Description |
|-------|-------------|
| `GET /` | Serves the dashboard UI (`static/index.html`) |
| `WS /ws` | Pushes `current_state` JSON every 2s to all connected clients |
| `GET /api/predictions` | Last N predictions from NDJSON |
| `GET /api/candles` | OHLCV candles for chart rendering |
| `GET /api/paper-trades` | Paper trade history |
| `POST /api/reset` | Clears all result files (admin) |
| `GET /charts/{filename}` | Serves generated bar charts |

Start: `uvicorn server:app --reload`

---

### `bot.py`
Polymarket trading bot. Watches `current_state` for DeepSeek signals and places bets.

**Modes:**
- `paper` (default) — simulates trades against virtual $1000 USDC, records to `results/paper_trades.ndjson`. Safe to run anytime.
- `live` — places real orders via Polymarket CLOB API. Requires `POLYMARKET_PRIVATE_KEY` + `POLYMARKET_ADDRESS` env vars and `pip install py-clob-client`.

**Entry logic:**
```
pending_deepseek_ready == True          ← DeepSeek has returned a prediction
AND no open position for this bar
AND confidence >= min_confidence (55)
AND EV >= min_ev (0.01)
→ enter at bar open price
```

**Position sizing:** Kelly Criterion capped at `max_kelly_fraction` (from config).

**Run embedded** (inside the server process — `engine.py` calls `bot.run()`).

**Run standalone:**
```bash
python bot.py --mode paper --ws ws://localhost:8000/ws
```
Standalone mode connects to the `/ws` WebSocket feed, so the server must be running separately.

---

## Data Flow Diagram

```
Binance REST ──► BinanceCollector ──► binance_klines buffer
                                           │
                               FeatureEngine (features)
                                           │
                          ┌────────────────▼────────────────┐
                          │         run_prediction_loop      │
                          │                                  │
                          │  strategies.get_all_predictions  │
                          │  signals.fetch_dashboard_signals │
                          │  ensemble.predict()              │
                          │                                  │
                          │  ── background ──                │
                          │  run_specialists()               │
                          │  run_historical_analyst()        │
                          │  DeepSeekPredictor.predict()     │
                          │  → pending_deepseek_ready=True   │
                          └─────────────┬───────────────────┘
                                        │
                               current_state dict
                                    /       \
                            WS /ws          bot.py
                         (dashboard)    (trade entry)
                                              │
                                    Polymarket CLOB
                                  (paper or live orders)
                                              │
                              results/paper_trades.ndjson
                              results/live_orders.ndjson
```

---

## Configuration

All tuneable parameters live in `config.py` (unchanged):
- `poll_interval_seconds` — Binance polling rate
- `initial_weights` — per-strategy ensemble weights
- `deepseek_enabled` — toggle DeepSeek pipeline
- `deepseek_api_key`, `coinapi_key`, `coinalyze_key`
- `min_confidence`, `min_ev`, `max_kelly_fraction` — bot filters

---

## Running

```bash
# Install deps
pip install fastapi uvicorn httpx websockets

# For live Polymarket trading only
pip install py-clob-client

# Start prediction server + embedded paper bot
uvicorn server:app --host 0.0.0.0 --port 8000

# OR run bot standalone (server must already be running)
python bot.py --mode paper --ws ws://localhost:8000/ws
```

Dashboard: `http://localhost:8000`
