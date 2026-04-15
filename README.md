# BTC Oracle Predictor

Binary prediction system for "Bitcoin Up or Down in 5 minutes" Polymarket markets.

## Architecture

```
btc-predictor/
├── api/
│   └── server.py              # FastAPI server, WebSocket, prediction loop
├── data/
│   ├── collector.py           # Polls Binance BTCUSDT REST every 12s for live price ticks
│   ├── dashboard_signals.py   # Fetches 10 live microstructure feeds in parallel at each window open
│   ├── storage_mongo.py       # MongoDB Atlas storage (ticks, predictions, deepseek_predictions)
│   ├── storage_file.py        # Local SQLite fallback storage (used when MongoDB is unavailable)
│   ├── features.py            # Feature engineering from raw ticks + OHLCV
│   └── local_data/            # Local data cache directory
├── deepseek/
│   ├── predictor.py           # Main DeepSeek reasoning call (deepseek-chat)
│   ├── specialists.py         # 5 focused parallel DeepSeek calls (Dow/Fib/Alligator/A-D/Harmonic)
│   ├── prompt_format.py       # Prompt builder — 100 bars 1m OHLCV + all indicators as text
│   └── prompts.py             # Specialist prompt templates (DOW_THEORY, FIBONACCI, ALLIGATOR, ACC_DIST, HARMONIC)
├── strategies/
│   ├── base.py                # All 19 math-based strategies + get_all_predictions()
│   ├── ensemble.py            # Weighted ensemble combiner with dynamic weight updates
│   └── ml_models.py           # Linear regression channel strategy
├── utils/
│   ├── ev_calculator.py       # Expected value + Kelly criterion
│   └── polymarket.py          # Polymarket market odds feed
├── static/                    # React dashboard (Babel standalone, no build step)
│   ├── index.html
│   └── app.jsx
├── config.py                  # All configuration (API keys, MongoDB URI, weights)
└── requirements.txt
```

## Data Sources

- **Live price ticks**: Binance BTCUSDT REST API — polled every 12s
- **1m OHLCV**: Binance `/api/v3/klines` — 500 bars fetched every 60s, used by all volume/OHLCV strategies
- **Market odds**: Polymarket API — implied probability and market odds for active 5m BTC market
- **Market microstructure**: 10 live feeds fetched in parallel at each 5-minute window open (see below)

## How It Works

At each 5-minute candle open:

1. **Math strategies** (19 total) compute signals from live ticks + 1m OHLCV
2. **DeepSeek specialists** (5 parallel calls) analyse Dow Theory, Fibonacci, Alligator, Acc/Dist, Harmonics using raw OHLCV text — results replace the math versions in the ensemble
3. **Ensemble vote** weighs all signals (dynamically adjusted by rolling accuracy)
4. **Main DeepSeek** receives ensemble snapshot + all indicators + all 10 microstructure feeds as text, returns UP/DOWN + reasoning — tracked separately, NOT in ensemble
5. At candle close, both ensemble and DeepSeek predictions are resolved against actual price

## Market Microstructure Feeds (`data/dashboard_signals.py`)

All 10 feeds are fetched in parallel at each 5-minute window open and injected into the main DeepSeek prompt:

| # | Feed | Source | What it provides |
|---|------|--------|-----------------|
| 1 | Order book imbalance | Binance spot depth-20 | Bid/ask volume balance, passive buy/sell wall strength |
| 2 | Long / short ratio | Binance Futures (retail + top-20%) | Retail crowding (contrarian) + smart money positioning |
| 3 | Taker buy/sell flow | Binance Futures 5m aggressor | BSR, 3-bar trend — who is paying urgently to buy/sell |
| 4 | Open interest + funding | Binance Futures perpetual | OI in BTC, 8h funding rate, mark vs index premium |
| 5 | Liquidations | Binance Futures last-5min | Long/short liquidation counts, USD value, cascade velocity |
| 6 | Fear & Greed index | alternative.me (daily) | Market sentiment extremes (contrarian at <25 / >75) |
| 7 | Mempool fee pressure | mempool.space | On-chain urgency — high fees may signal exchange deposits/selling |
| 8 | CoinAPI aggregated rate | 350+ exchange weighted avg | Cross-exchange price divergence — arbitrage pressure direction |
| 9 | Coinalyze funding | Cross-exchange aggregate | Validates Binance funding; divergence = Binance-specific overleveraging |
| 10 | CoinGecko market overview | CoinGecko | BTC market cap, 24h volume, 24h change — macro backdrop |

> **Note on feed availability**: In early windows after startup, some feeds may not yet have data
> and appear as "unavailable" in the prompt. This is the root cause of the 3 observed DeepSeek
> `DATA_REQUESTS` in the log (windows #2, #17, #26 all asked for taker flow BSR, liquidation data,
> funding rate, L/S ratio, and/or CoinAPI — all of which ARE collected once the feeds warm up).
> No new data sources are needed; the requests disappear once the system is running.

## Storage (MongoDB Atlas)

| Collection | Contents |
|---|---|
| `ticks` | Every price tick (timestamp, mid, bid, ask, spread) |
| `predictions` | Ensemble predictions + resolution (correct/incorrect) |
| `deepseek_predictions` | Full DeepSeek audit log: prompt, reasoning, indicators snapshot, strategy snapshot, resolution |

A local SQLite fallback (`data/storage_file.py`) is used automatically when MongoDB Atlas is unreachable.

## DeepSeek Response Fields

The main DeepSeek call returns a structured response with the following parsed fields:

| Field | Description |
|---|---|
| `signal` | `UP` / `DOWN` / `UNKNOWN` / `ERROR` |
| `confidence` | Integer 0–100 |
| `reasoning` | 4 numbered reasons (microstructure / funding+positioning / technical / synthesis) |
| `narrative` | 2–4 sentence price-action story — names specific bars, prices, and volume conviction |
| `free_observation` | 1–2 sentences on any anomalous or high-conviction pattern noticed |
| `data_received` | DeepSeek's confirmation of what data it analysed this window |
| `data_requests` | Additional data DeepSeek asked for, or `NONE` — logged to server log and stored |
| `polymarket_url` | Direct link to the active Polymarket market |
| `window_start` / `window_end` | Human-readable window timestamps |
| `latency_ms` | DeepSeek API round-trip time |

All `data_requests` that are not `NONE` are logged at `INFO` level in `server_new.log` as:
```
INFO:api.server:DeepSeek requested additional data: <request text>
```

## Accuracy Tracking

Three accuracy metrics are tracked:
- **Ensemble**: all windows
- **DeepSeek**: all windows where DeepSeek fired
- **Agree Only**: windows where ensemble signal == DeepSeek signal (highest quality filter)

Each indicator's rolling accuracy is tracked individually. The ensemble auto-adjusts weights:
- `DISABLED` (<40% accuracy) — near-zero weight, flagged in DeepSeek prompt as "IGNORE"
- `WEAK` (<50%) — low trust
- `MARGINAL` / `LEARNING` / `RELIABLE` / `EXCELLENT` — graded trust levels

## Running

```bash
cd btc-predictor
pip install -r requirements.txt
uvicorn api.server:app --host 0.0.0.0 --port 8000
```

Dashboard at `http://localhost:8000`

> **VPN note**: Binance and Chainlink APIs are geo-blocked in India. A VPN is required when running locally.
> A permanent server-side VPN solution is the preferred long-term fix.
