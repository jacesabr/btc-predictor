# Polymarket Bitcoin 5-Minute Prediction

A production system that predicts the direction of BTC/USDT at the close of each 5-minute bar, designed to be bet on Polymarket's "BTC up/down" binary markets.

Every 5 minutes the pipeline collects market data, runs it through several specialist analyses, retrieves the most similar resolved bars from a vector database of its own past predictions, and produces a final directional call with a confidence number. The result is stored, later graded against the actual outcome, and the grade is fed back into the retrieval corpus as context for future predictions.

**This is not financial advice.** The win rate on 5-minute BTC direction is bounded by genuine market noise — even a well-designed system rarely exceeds 55-58% on this horizon. Treat any signals from this service as one input among many.

---

## What the system does

Every 5 minutes, at the opening of a new bar:

1. **Collect** real-time data — 1-minute OHLCV klines from Binance, L2 order book, taker-flow ratio, open interest and funding from Binance Futures, spot whale flow from Kraken, cross-exchange funding from Coinalyze, Deribit implied-volatility index (DVOL), Binance-dominance, fear-and-greed index, mempool state.
2. **Compute** 50+ technical features (RSI, MACD, Bollinger, stochastic, EMA crosses, VWAP, OBV, realized volatility at three horizons, Fibonacci retracements, Williams Alligator, accumulation/distribution).
3. **Vote** a 20-strategy ensemble, each strategy weighted by its own measured accuracy over the last 100 resolved bars.
4. **Fire five parallel specialist sub-calls** (Dow Theory, Fibonacci pullback, Williams Alligator, Accumulation/Distribution, Harmonic patterns) in a single batched DeepSeek request.
5. **Run a Binance-microstructure expert** (separate DeepSeek call) that reads only the flow/book/OI/funding data and returns a confluence composite.
6. **Embed the current bar** as a prose essay via Cohere `embed-english-v3.0`, search `pgvector` for the top 50 most similar resolved bars, re-rank to the top 10 via Cohere `rerank-english-v3.0`, and pass those — with each bar's original DeepSeek reasoning, narrative, and post-mortem — to a historical-similarity analyst (another DeepSeek call) that returns a precedent-based position read.
7. **Final predictor** is a last DeepSeek call that sees everything: price structure, indicator track records, ensemble vote, specialist signals, Binance-expert reads, historical analyst's summary, and the last 50 bars as OHLCV CSV. It outputs `UP`, `DOWN`, or `NEUTRAL` with a 55-95% confidence.
8. **Resolve and learn** — when the bar closes, the actual direction is scored. A post-mortem DeepSeek call analyses what the system got right or wrong, extracts a `LESSON_RULE` if applicable, and the full bar (including reasoning, narrative, post-mortem) is re-embedded into the vector store so the next similar setup can be compared against it.

---

## Why this architecture

Five decisions drive the structure:

**Retrieval-augmented, not RL-trained.** The system doesn't train weights end-to-end. It retrieves bars where the market situation *looked similar* and shows the LLM what happened last time, with the original reasoning so the LLM can see where the prior call went right or wrong. This makes the system's state human-auditable — every decision cites its evidence.

**Specialist separation.** One general prompt that tries to analyse every signal at once dilutes attention. Breaking the work into discrete specialists (technical, microstructure, historical precedent) lets each one focus on a narrow slice and produces higher-quality reads that the final call then integrates.

**Post-mortems as persistent memory.** After every bar resolves, a separate DeepSeek call writes a structured post-mortem (verdict, error class, root cause, lesson). That post-mortem is stored and surfaces in future retrievals when similar setups appear. The system accumulates a library of "here's why this pattern fooled us last time."

**Honest confidence.** Confidence numbers have to correspond to expected win rate. A 70% call that's wrong ~30% of the time is *calibrated*, not broken — losses are expected. The historical analyst's rubric explicitly compares observed wrong rate to implied wrong rate before flagging anything.

**NEUTRAL is a cost, not a free win.** Abstaining looks safe but is a passed opportunity. The prompts are calibrated to take 55-65% directional calls when the argument survives steelmanning, not to default to NEUTRAL on any ambiguity.

---

## What's actually novel here

There are thousands of BTC direction-prediction projects. Most are either pure-ML black boxes, rule-based technical systems, or stateless "ask GPT what BTC will do" wrappers. Here's where this one differs — stated plainly, with the bits that are NOT novel called out so you can judge.

### The closed feedback loop

The architecturally unusual thing is a **closed loop between prediction and memory**. Every bar goes through:

```
 predict  →  resolve (grade right/wrong)  →  postmortem (DeepSeek writes why)
     ↑                                                       ↓
     └── retrieve as precedent ← embed as memory ← enrich ──┘
```

Each resolved bar is re-embedded into pgvector *with its own post-mortem attached* as part of the embedding text. The post-mortem is a structured LLM analysis (VERDICT, ERROR_CLASS, ROOT_CAUSE, LESSON_RULE, LESSON_PRECONDITIONS) that explains what went wrong. When a similar setup appears next week, retrieval doesn't just surface "last time this looked like this, it went UP" — it surfaces *"last time this looked like this, we called UP and lost because we over-weighted the bid wall; watch for that."*

Most LLM-trading projects use the LLM as a stateless analyser — each bar is a fresh call, prior mistakes don't inform future ones. This system accumulates its own failure modes, in human-readable form, keyed by situational embedding. That's the piece that took the longest to imagine and to make work.

### Prose-level retrieval, not feature vectors

Both the current bar and the historical precedents are rendered as full essays — technical state, volatility regime, microstructure reads, strategy votes, specialist conclusions, Binance expert verdict — and the essay is what gets embedded. Cohere's embedding model can then rank precedents by *narrative similarity* ("similar situation, similar conflicting signals, similar regime") rather than by raw feature Euclidean distance. Precedents and the current bar speak the same language. Most similarity-search systems embed structured feature vectors; this one embeds prose.

### Calibration discipline baked into prompts

The historical analyst's rubric reasons explicitly about variance: *"A 70% confidence call that loses 30% of the time is calibrated, not broken."* The `ENSEMBLE_WARNING` only fires when observed wrong rate *exceeds* the rate the claimed confidence implies, with a minimum sample size. Most LLM trading prompts either lack calibration language entirely or use rigid fixed thresholds that mistake normal variance for a pattern failure. It seems small; it's one of the largest wins in practice.

### What is NOT unique

To be clear on what could be replicated by anyone in a weekend:

- The data sources — all free and public; no premium feed advantage.
- The LLMs — DeepSeek-chat and Cohere embed/rerank are commodity APIs.
- The prediction target — countless attempts at BTC 5-minute direction already exist.
- The technical stack — FastAPI + Postgres + pgvector is a textbook RAG setup.

### Where the actual moat lives

Not in the code. The code is replicable in 3-5 days by a capable engineer.

The defensible asset is **the accumulated data**: a growing corpus of BTC bars with full DeepSeek reasoning, narrative, post-mortem, microstructure snapshot, strategy vote pattern, and graded outcome — all embedded for semantic retrieval. Bootstrapping that from scratch requires weeks of real-time observation across multiple market regimes. You cannot scrape it and you cannot buy it. It grows automatically as the system runs.

The secondary asset is the **prompt-engineering lineage** — dozens of small tuning decisions encoded into the rubrics (NEUTRAL-is-a-cost, calibrated ENSEMBLE_WARNING, top-10 retrieval, prose current-bar, Binance-expert-before-historical). Each decision is a small bet; the collection is a style that's hard to reproduce by reading the final files alone.

If you are considering using, forking, or valuing this repo: the interesting question is not "does the code work" (it does) but "how much history does it have, and does the prompt style survive regime changes." That takes time to answer honestly. It's why the realistic-expectations section below urges a 1,000-bar minimum sample before drawing any conclusion.

---

## Architecture

```
                ┌──────────────────────────────────────────────────┐
                │  Bar opens (t=0)                                 │
                └────────────┬─────────────────────────────────────┘
                             │
      ┌──────────────────────┼────────────────────────┐
      │                      │                        │
 BinanceCollector        Dashboard signals     Kline feed (1m OHLCV,
 (tick-level price)      (OB, taker flow,      last 50 bars)
                          OI, funding, DVOL,
                          F&G, mempool, ...)
      │                      │                        │
      └──────────────────────┼────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
        Strategy         Specialists   Ensemble vote
        signals          (DOW/FIB/     (weighted by
        (20 indicators)  ALG/ACD/HAR)   measured accuracy)
              │              │              │
              └──────────────┼──────────────┘
                             ▼
                   Binance expert (DeepSeek)
                   reads microstructure, returns
                   confluence composite + veto flags
                             │
                             ▼
                   Historical analyst (DeepSeek)
                   ├─ embed current bar essay (Cohere)
                   ├─ pgvector top-50 cosine search
                   ├─ Cohere rerank → top-10
                   ├─ fetch post-mortems for those 10
                   └─ return POSITION/LEAN/PRECEDENT_TABLE
                             │
                             ▼
                   Main predictor (DeepSeek)
                   sees everything, outputs:
                   UP | DOWN | NEUTRAL  +  55-95%
                             │
                             ▼
                   Staged; revealed at bar close
                             │
         ┌───────────────────┴───────────────────┐
         ▼                                       ▼
  Resolve & grade                        Post-mortem (DeepSeek)
  (correct / wrong)                      writes lesson
         │                                       │
         └────────► re-embed bar with ◄──────────┘
                     post-mortem into
                     pgvector for future
                     retrievals
```

Timing for a typical bar: specialists ~8s, dashboard ~2s, Binance expert ~8s, historical analyst ~18s, main predictor ~14s. Total ~50s on a 300-second budget.

---

## Repository layout

| File | Role |
|---|---|
| `server.py` | FastAPI HTTP + WebSocket layer. Startup tasks, endpoints, web UI mount. |
| `engine.py` | Orchestration: tick collector, bar loop, resolution, state, background tasks. |
| `ai.py` | All DeepSeek specialists (main, historical analyst, Binance expert, unified specialists, post-mortem, embedding audit) + Cohere embed/rerank + prompt builders. |
| `semantic_store.py` | Postgres `pattern_history` + pgvector embedding store + accuracy computations. |
| `storage_pg.py` | Postgres `ticks`, `predictions`, `deepseek_predictions`, `events`, `score_reset` tables + pool + StoragePG class. |
| `strategies.py` | 20 technical strategies (RSI, MACD, Bollinger, EMA, Alligator, ADX, ...) + ensemble + EV calculator. |
| `data_feed.py` | BinanceCollector, FeatureEngine, PolymarketFeed. |
| `signals.py` | Dashboard signal fetchers (order book, taker flow, funding, mempool, DVOL, F&G, ...). |
| `config.py` | Environment-based configuration. |
| `static/app.jsx` | React dashboard (no build step — Babel standalone). |
| `static/index.html` | Entry page. |
| `specialists/*/PROMPT.md` | Prompt templates for each DeepSeek specialist. |
| `bot.py` | Standalone Polymarket trading bot (not run by the web service). |
| `backfill_embeddings.py` | Disaster-recovery utility — re-embed bars if the vector column is wiped. |
| `render.yaml` | Render Blueprint (Postgres + web service). |
| `requirements.txt` | Pinned production dependencies (numpy, psycopg2-binary, pgvector, fastapi, uvicorn, aiohttp, pydantic, websockets, matplotlib, python-dotenv). |
| `requirements-bot.txt` | Extra deps for the standalone Polymarket bot (`py-clob-client`, `eth-account`, `httpx`). |

---

## Tech stack

- **Python 3.11** + FastAPI + uvicorn + asyncio + WebSockets
- **PostgreSQL 17** + pgvector (production on Render)
- **DeepSeek API** — chat model for specialists, main predictor, post-mortems; reasoner for the 4-hourly embedding audit
- **Cohere API** — `embed-english-v3.0` (1024-dim) + `rerank-english-v3.0`
- **Binance / Coinalyze / Deribit / CoinGecko / mempool.space / alternative.me** — all free public endpoints, no private keys required
- **React 18** via Babel standalone (zero build step, served as static files)

---

## Running locally (inspection only — production is on Render)

```bash
git clone https://github.com/jacesabr/btc-predictor
cd btc-predictor
cp .env.example .env          # fill in keys
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8000 --loop asyncio
```

Required environment variables:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Postgres connection (must support pgvector) |
| `DEEPSEEK_API_KEY` | All DeepSeek calls |
| `COHERE_API_KEY` | Embedding + rerank |
| `COINALYZE_KEY` | Cross-exchange aggregated funding |
| `POLYMARKET_PRIVATE_KEY` | Only needed if running `bot.py` for live trading |

---

## Deployment

Production runs on [Render](https://render.com):

- **Web Service** — Python 3.11, `uvicorn server:app --host 0.0.0.0 --port $PORT --loop asyncio`, Starter plan ($7/mo).
- **Managed Postgres** — PG 17 with pgvector, Basic-256mb ($6/mo).
- **Region** — Oregon (closest to DeepSeek / Cohere US endpoints).

Deploy via Blueprint (`render.yaml`) or manually connect the repo and set env vars. Build time is ~45s with pinned wheels; end-to-end push-to-live under 90s.

---

## Realistic expectations

BTC 5-minute direction is an extremely noisy signal. Based on what we know about this horizon:

- **50%** — any system without a real edge
- **52-55%** — modest edge surviving spread and noise; roughly the range most algo shops operate at for retail-accessible data
- **55-58%** — strong for this horizon; would surprise us positively
- **60%+** — statistically extraordinary; the first ~1,000 bars are too few to distinguish genuine edge from a favourable market regime

On Polymarket, break-even win rate after typical bid-ask spread is around 52-53%. Any directional edge below that is negative EV regardless of how high the win rate looks on paper.

We monitor calibration per bar: a 70% confidence call *should* win ~70% of the time. If observed rate drifts well below implied, the pipeline self-flags in the ERRORS tab.

---

## Dashboard tabs

- **LIVE** — current bar: TradingView chart, DeepSeek signal + confidence, live price, bar-close countdown
- **HISTORY** — every resolved bar with the full DeepSeek reasoning, chart at prediction time, post-mortem, and the `LESSON` distilled out of it
- **ENSEMBLE** — every strategy + specialist + dashboard signal's measured accuracy, sorted best to worst
- **TIMING** — per-bar pipeline stage breakdown (specialists, Cohere embed, pgvector search, rerank, DeepSeek call, etc.) so slow stages are obvious
- **EMBED AUDIT** — 7-stage pipeline flow diagram with pass/fail markers, auto-audited every 4h via DeepSeek-reasoner
- **ERRORS** — postmortem-derived improvement lessons at the top, raw error log below

---

## Data flow quick reference

- **Tick ingestion** — Binance REST, every 12s, capped at 5,000 ticks
- **Bar open** — every 5 minutes, synchronised to UTC minute boundaries
- **Prompt size** — ~25 KB typical (main predictor); ~32 KB for historical analyst
- **Token cost** — ~$0.005 per bar DeepSeek, ~$0.01 per bar Cohere, ~$0.10 per bar infra amortised
- **Context retention** — 10,000 most recent resolved bars in pgvector; all pattern history in Postgres permanently

---

## License

Private. Not open source. Do not redistribute without written permission.
