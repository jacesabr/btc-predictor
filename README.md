# BTC Oracle Predictor

Binary prediction system for **"Bitcoin Up or Down in 5 minutes"** Polymarket markets.

Uses 19 math-based strategies, 5 parallel DeepSeek specialist calls, 20 live microstructure feeds, a historical pattern similarity engine, and a self-optimizing weighted ensemble — all feeding one final DeepSeek reasoning call per 5-minute bar.

---

## ⚠️ Deployment Policy — Railway + PostgreSQL Only

**This app runs exclusively on Railway and uses Railway's managed PostgreSQL database.**

- Do **NOT** run the app on `localhost` / `127.0.0.1` — there is no local dev mode.
- Do **NOT** use any local database (no local Postgres, no MongoDB Atlas, no NDJSON files as primary storage).
- All persistence (`ticks`, `predictions`, `deepseek_predictions`, `pattern_history`, embeddings) goes to the Railway PostgreSQL instance via `storage_pg.py` / `semantic_store_pg.py`.
- The canonical entry points are `Procfile` (`web: python -m uvicorn api.server:app --host 0.0.0.0 --port $PORT`) and `railway.toml`.
- Connection is via `DATABASE_URL` injected by Railway — never commit or hard-code a connection string.
- All external API keys (`DEEPSEEK_API_KEY`, `COINAPI_KEY`, `COINALYZE_KEY`, `COHERE_API_KEY`, etc.) are set as Railway environment variables on the service.

Any workflow that starts a local uvicorn / starts a local DB / writes to local NDJSON is out of spec and should be removed on sight.

---

## Project Structure

```
btc-predictor/
├── api/
│   └── server.py              # FastAPI app, WebSocket, all background loops, prediction orchestration
├── data/
│   ├── collector.py           # Polls Binance BTCUSDT REST every 12s for live price ticks
│   ├── features.py            # 50+ technical indicators from ticks + OHLCV
│   ├── dashboard_signals.py   # 20 live microstructure feeds (fetched in parallel at each bar open)
│   └── pattern_history.py     # Persistent bar history + per-indicator accuracy tracking
├── storage_pg.py              # Railway PostgreSQL storage (ticks / predictions / deepseek_predictions / pattern_history)
├── semantic_store_pg.py       # Railway PostgreSQL embeddings store for semantic pattern retrieval
├── deepseek/
│   ├── predictor.py           # Main DeepSeek reasoning call — full context, async, result revealed at bar close
│   ├── specialists.py         # 5 focused parallel DeepSeek calls (Dow/Fib/Alligator/A-D/Harmonic)
│   ├── prompt_format.py       # Prompt builder + structured response parser
│   └── historical_analyst.py  # Finds 3–5 historically similar bars; injects context into main prompt
├── strategies/
│   ├── base.py                # 19 math-based strategies + math fallbacks for all 5 specialist types
│   ├── ensemble.py            # Weighted voting combiner with dynamic accuracy-based weight updates
│   └── ml_models.py           # Linear regression channel strategy (scikit-learn)
├── utils/
│   ├── polymarket.py          # Polymarket Gamma API — live odds, market slug, implied probability
│   └── ev_calculator.py       # Expected value + Kelly criterion sizing
├── static/
│   ├── index.html             # Dashboard entry point
│   └── app.jsx                # React app (Babel standalone, no build step)
├── specialists/               # Runtime I/O artifacts (last_prompt.txt, last_response.txt per specialist)
├── charts/                    # Candlestick PNGs regenerated each bar
├── results/                   # Production NDJSON data (ticks, predictions, deepseek_predictions, pattern_history)
├── config.py                  # All configuration: API keys, weights, timing, MongoDB URI
├── Procfile                   # Heroku: uvicorn api.server:app
└── requirements.txt
```

---

## How It Works

At each **5-minute candle open** (UTC-aligned):

1. **Math strategies** — 19 technical strategies compute signals from live ticks + 1m OHLCV
2. **Dashboard signals** — 20 microstructure feeds fetched in parallel (order book, funding, liquidations, etc.)
3. **DeepSeek specialists** — 5 focused parallel calls (Dow Theory, Fibonacci, Alligator, Acc/Dist, Harmonics) analyse 60-bar OHLCV; results override math fallbacks in the ensemble
4. **Historical analyst** — scans `pattern_history.ndjson` for the 3–5 most similar past bars and their outcomes
5. **Ensemble vote** — weighted combination of all strategies + specialists + dashboard votes; weights auto-adjust by rolling accuracy
6. **Main DeepSeek call** — receives full context (ensemble, all indicators, all 20 feeds, pattern history); fires async, result held until bar close
7. **Bar close resolution** — both ensemble and DeepSeek predictions resolved against actual price; weights updated; pattern history appended

---

## Prediction Strategies

### Math Strategies (`strategies/base.py`)

| Group | Strategies |
|-------|-----------|
| Oscillators | RSI(4), MACD (3/10/16 Raschke), Stochastic |
| Trend & Structure | EMA Cross (8/21 + 4/9 multi-TF), Supertrend (ATR), ADX |
| Specialist Fallbacks | Dow Theory, Fibonacci Pullback, Williams Alligator, Acc/Dist, Harmonic |
| Volume / Price | VWAP, Volume Flow (OBV slope + surge) |
| Market / Crowd | Polymarket odds (1.3× initial weight) |
| ML | Linear Regression Channel |

### DeepSeek Specialists (`deepseek/specialists.py`)

Five calls fired **in parallel** at bar open (30s timeout), replacing math fallbacks in the ensemble:

- **Dow Theory** — market structure, trend direction, volume confirmation
- **Fibonacci Retracement** — key levels and current price position
- **Williams Alligator** — jaw/teeth/lips trending or tangled
- **Accumulation/Distribution** — volume-weighted flow direction
- **Harmonic Patterns** — Bat, Gartley, Crab, Butterfly, Shark, ABCD PRZ detection

Each returns `{signal, confidence, reasoning}`. If a specialist call fails or times out, the math fallback is used.

### Ensemble (`strategies/ensemble.py`)

Weighted sum of all signals. Weights auto-update each bar:

| Rolling Accuracy | Weight Tier |
|-----------------|------------|
| < 40% | 0.05 — effectively disabled |
| 40–50% | 0.10–0.50 — weak |
| 50–60% | 0.50–1.20 — average to good |
| 60–65% | 1.20–2.00 — boosted |
| > 65% | 2.00–3.00 — excellent |

Learning rate: 0.15 (exponential smoothing — weights move gradually, not instantly).

---

## Data Sources

### Live Price
- **Ticks**: Binance BTCUSDT REST — polled every 12s, last 5,000 kept in memory
- **1m OHLCV**: Binance `/api/v3/klines` — 500 bars, refreshed every 60s

### 20 Market Microstructure Feeds (`data/dashboard_signals.py`)

All fetched **in parallel** at each bar open and injected into the main DeepSeek prompt:

| # | Feed | Source | Signal |
|---|------|--------|--------|
| 1 | Order book imbalance | Binance spot depth-20 | Bid/ask volume balance |
| 2 | Long/short ratio | Binance Futures | Retail crowding + smart money |
| 3 | Taker buy/sell flow | Binance Futures 5m | Aggressor BSR, 3-bar trend |
| 4 | OI + funding | Binance Futures perp | Open interest, 8h funding, mark premium |
| 5 | Liquidations | Binance Futures last-5m | Long/short cascade velocity |
| 6 | Fear & Greed | alternative.me (daily) | Sentiment extremes (contrarian) |
| 7 | Mempool fee pressure | mempool.space | On-chain urgency |
| 8 | Coinalyze funding | Cross-exchange aggregate | Validates Binance funding |
| 9 | CoinAPI aggregated rate | 350+ exchange weighted avg | Cross-exchange arbitrage pressure |
| 10 | CoinGecko market overview | CoinGecko | BTC 24h change, volume, market cap |
| 11 | CoinAPI momentum | Multi-exchange 5m acceleration | Rate-of-change direction |
| 12 | CoinAPI large trades | Whale order flow | > 2 BTC block direction |
| 13 | Kraken premium | Kraken vs Binance spread | Institutional signal |
| 14 | OI velocity | Binance OI change 30m | Open interest rate of change |
| 15 | Spot whale flow | Binance spot aggTrades | > 5 BTC direction |
| 16 | Bybit liquidations | Bybit | Cross-exchange cascade validation |
| 17 | OKX funding rate | OKX | Independent exchange confirmation |
| 18 | BTC dominance | CoinGecko global | Market cap % |
| 19 | Top trader position ratio | Binance top accounts | Notional position direction |
| 20 | Funding trend | Binance historical | Funding rate slope |

Each feed maps to UP/DOWN/NEUTRAL and enters the ensemble as an additional vote (weight 0.65 confidence).

### Polymarket Odds
- Slug: `btc-updown-5m-{unix_timestamp}` (5-min aligned)
- Implied probability + market odds from Gamma API
- Used for EV/Kelly calculation and as a strategy signal (1.3× initial weight)

---

## Main DeepSeek Call (`deepseek/predictor.py`)

Fires **asynchronously** at bar open. Result is held until bar close (stale-window guard discards it if the bar rolls over before completion).

**Inputs:**
- Last 20 price ticks + 100 × 1m OHLCV bars
- All 50+ technical indicators
- All 19 strategy predictions + 5 specialist results
- All 20 dashboard microstructure signals + ensemble vote
- 3–5 historically similar bars + their outcomes (from historical analyst)
- Rolling accuracy metrics per indicator

**Parsed response fields:**

| Field | Description |
|-------|-------------|
| `signal` | `UP` / `DOWN` / `UNKNOWN` / `ERROR` |
| `confidence` | Integer 0–100 |
| `reasoning` | 4 numbered reasons (microstructure / funding / technical / synthesis) |
| `narrative` | 2–4 sentence price-action story |
| `free_observation` | Anomalous or high-conviction pattern |
| `data_received` | DeepSeek's confirmation of what it analysed |
| `data_requests` | Additional data requested, or `NONE` |
| `latency_ms` | API round-trip time |

DeepSeek prediction is tracked **separately** from the ensemble — it is NOT part of the ensemble vote.

---

## Historical Pattern Analyst (`deepseek/historical_analyst.py`)

Scans `results/pattern_history.ndjson` (every prior bar) for the 3–5 most similar past bars by feature similarity (technical indicators, volume profile, microstructure alignment). Each match includes its resolved outcome (UP/DOWN + accuracy). This context is injected into the main DeepSeek prompt to ground predictions in observed history.

Pattern history grows indefinitely and is never trimmed.

---

## Storage

### Railway PostgreSQL (`storage_pg.py`, `semantic_store_pg.py`)

All persistence is a single managed PostgreSQL instance on Railway. Connection via `DATABASE_URL` env var.

| Table | Contents |
|-------|----------|
| `ticks` | Every price tick (timestamp, mid, bid, ask, spread) |
| `predictions` | Ensemble predictions + resolution |
| `deepseek_predictions` | Full audit log: prompt, response, reasoning, indicators, resolution, latency |
| `pattern_history` | Full bar record (specialists + indicators + dashboard + ensemble + DeepSeek + resolution) |
| `embeddings` | Cohere embeddings for semantic pattern retrieval |

No fallbacks, no local NDJSON, no MongoDB. If Postgres is unreachable the service errors out — fix the connection, don't silently degrade.

### Pattern History (`data/pattern_history.py`)

Separate from predictions — stores the full bar record including all specialists, all indicators, all dashboard signals, ensemble prediction, DeepSeek prediction, and resolution. Used for historical similarity matching and per-indicator accuracy leaderboards.

---

## Accuracy Tracking

Three accuracy metrics tracked per window:

- **Ensemble** — all windows
- **DeepSeek** — all windows where DeepSeek fired
- **Agree Only** — windows where ensemble signal == DeepSeek signal (highest quality filter)

Per-indicator rolling accuracy drives ensemble weight updates. Accuracy tiers shown in the dashboard `/accuracy/all` leaderboard, labelled `DISABLED` / `WEAK` / `MARGINAL` / `LEARNING` / `RELIABLE` / `EXCELLENT`.

---

## Dashboard (`static/`)

React app (Babel standalone, no build step) served from the Railway service URL (e.g. `https://<service>.up.railway.app`).

Key endpoints:
- **WebSocket `/ws`** — live price, prediction state, strategies, specialists (1 Hz)
- `GET /weights` — strategy weights + accuracy labels
- `GET /accuracy/all` — full leaderboard (AI, strategies, specialists, microstructure)
- `GET /audit` — every prediction with all evidence (CSV-exportable)
- `GET /deepseek/source-history` — recent DeepSeek calls + full data snapshots

---

## Configuration (`config.py`)

All tunable parameters and API keys:

```python
poll_interval_seconds: 12.0          # Binance tick poll rate
window_duration_seconds: 300         # 5-minute bar
rolling_window_size: 12              # bars for rolling accuracy
min_predictions_for_weight_update: 10

# EV / Kelly
min_ev_to_enter: 0.05
strong_ev_threshold: 0.15
max_kelly_fraction: 0.25

# API keys (loaded from Railway environment)
DATABASE_URL, DEEPSEEK_API_KEY, COHERE_API_KEY, COINAPI_KEY, COINALYZE_KEY
```

Initial strategy weights set in `config.py` under `initial_weights`.

---

## Running (Railway only)

This service is deployed and run **only on Railway**. There is no supported localhost workflow.

```
# railway.toml
[build] builder = "nixpacks"
[deploy] startCommand = "uvicorn server:app --host 0.0.0.0 --port $PORT"

# Procfile
web: python -m uvicorn api.server:app --host 0.0.0.0 --port $PORT
```

Required Railway environment variables (set on the service, never committed):
```
DATABASE_URL=<Railway PostgreSQL URL — auto-injected when the Postgres plugin is attached>
DEEPSEEK_API_KEY=<DeepSeek API key>
COHERE_API_KEY=<Cohere API key>
COINAPI_KEY=<CoinAPI key>         # optional — feeds 9, 11, 12 degrade gracefully
COINALYZE_KEY=<Coinalyze key>     # optional — feed 8 degrades gracefully
```

The public dashboard is served at the Railway-assigned domain (e.g. `https://<service>.up.railway.app`).

**VPN note:** Binance and Chainlink APIs are geo-blocked in India. Because this runs on Railway (not on the user's machine), the outbound region is Railway's egress — the permanent server-side VPN solution, if needed, must be configured on the Railway service, not locally.

---

## Monetization Concept: Copy-Trade Platform

The predictor can serve as the signal source for a SaaS copy-trading platform on top of Polymarket.

### Overview

Users connect their Polymarket credentials, choose Paper Trade (virtual) or Real Trade (live USDC), set position sizing, and follow the bot. The platform executes Polymarket orders on their behalf via `py-clob-client`.

### Polymarket Trading API

```python
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

client = ClobClient("https://clob.polymarket.com", key=PRIVATE_KEY, chain_id=137)
client.set_api_creds(client.create_or_derive_api_creds())

order = client.create_market_order(MarketOrderArgs(
    token_id=TOKEN_ID,   # YES token for UP, NO token for DOWN
    amount=25.0,         # USDC
    side=BUY,
    order_type=OrderType.FOK
))
resp = client.post_order(order, OrderType.FOK)
```

Token IDs are resolved from the market slug via the Gamma API + CLOB API. The `utils/polymarket.py` module already fetches the slug — extend it to resolve token IDs.

**One-time user requirement:** Each user must approve the CTF Exchange + NegRisk Adapter contracts on Polygon before orders will execute.

### Wallet / Custody Options

| Option | Best For | Notes |
|--------|----------|-------|
| **Circle Programmable Wallets** (recommended) | Safety + compliance | Circle handles key security; USDC native; per-user deposit addresses |
| **Privy** | Email-login UX | Official Polymarket example repo exists; user-owned wallets with server-side signing |
| **Turnkey** | Pure backend programmatic | Official Polymarket example; fully API-driven signing |
| **DIY platform wallet** | MVP prototype only | Single private key in env var; all funds at risk if key leaks |

### MVP API Surface

```
POST /api/auth/register|login       JWT auth
GET  /api/bots                      Available bots + live stats
POST /api/follow                    Follow a bot (paper or real, set sizing)
GET  /api/trades                    User trade history
GET  /api/portfolio                 Aggregated P&L
```

**Trade execution:** `TradeExecutor` service subscribes to the predictor signal. On each UP/DOWN, it loops through active followers, aggregates real-trade positions into one platform order, then distributes P&L proportionally.

### Monetization Models

1. **Subscription** — monthly fee for real-trade copy mode; paper trading free
2. **Performance fee** — % of profits from real trades
3. **Freemium** — 1 bot free, premium bots behind paywall
4. **Signal marketplace** — other developers list bots; platform takes a cut

### Key Risks

- Platform private key must NEVER be in code — use env var or secrets manager; consider multi-sig (Gnosis Safe) for production
- FOK orders fail on thin liquidity — add fallback retry as GTC limit order slightly off mid-price
- BTC 5m windows are short — execution must complete within seconds of signal
- Check Polymarket ToS for automated bots acting on behalf of other users
- Holding client USDC may require a Money Transmitter License depending on jurisdiction — get legal advice before public launch

### Useful References

- [py-clob-client](https://github.com/Polymarket/py-clob-client) — official Python client (MIT)
- [Polymarket Agents](https://github.com/Polymarket/agents) — official AI agent framework
- [Polymarket + Privy example](https://github.com/Polymarket/privy-safe-builder-example) — server-side signing
- [polymarket-apis PyPI](https://pypi.org/project/polymarket-apis/) — unified API wrapper
- [PolySimulator](https://polysimulator.com/) — paper trading UX reference
- [CTF Exchange allowance setup](https://gist.github.com/poly-rodr/44313920481de58d5a3f6d1f8226bd5e)
