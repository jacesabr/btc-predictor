# Organizing for Backtesting (Future State)

Planning doc — answers two questions:

1. What does each bar's history actually hold today?
2. What changes are needed so we can replay 1000 random historical bars through arbitrary LLMs / embedding models and compare win rates?

> **Note on `BACKTESTING_TEST.md`:** that file describes a Node.js + JSONL system (`server.js`, `backtesting/records/prompts.jsonl`, npm). **None of that exists.** The real stack is FastAPI + Postgres. Treat that doc as aspirational and ignore it. This doc is the actual plan.

---

## 1. What each bar currently stores

Per-bar storage is split across **two Postgres tables** that share `window_start` as the join key.

### `deepseek_predictions` — the prediction record

Defined at [storage_pg.py:91-116](storage_pg.py#L91-L116). One row per 5-minute bar.

| Column | What it is |
|---|---|
| `window_start`, `window_end` | bar timestamps (PK) |
| `start_price`, `end_price` | open / close — what the model was predicting on |
| **`full_prompt`** | **The exact prompt sent to DeepSeek** |
| **`raw_response`** | **DeepSeek's raw response text** |
| `signal` | parsed guess: UP / DOWN / NEUTRAL |
| `confidence` | parsed confidence (0–100) |
| `reasoning`, `narrative`, `free_observation` | parsed sub-fields from the response |
| **`actual_direction`** | **The truth: UP / DOWN / NULL (flat)** |
| **`correct`** | **Did the guess match? bool, or NULL for NEUTRAL/flat** |
| `latency_ms` | API roundtrip time |
| `strategy_snapshot`, `indicators_snapshot`, `dashboard_signals_snapshot` | JSON blobs of all signal state at the moment of prediction |
| `postmortem` | post-resolve self-analysis text |
| `chart_path`, `polymarket_url` | links |

So the answer to the literal question — **yes, each bar holds (full prompt → guess → actual)**. The triple is already there in three columns: `full_prompt`, `signal`, `actual_direction`.

### `pattern_history` — the embedding store

Defined at [semantic_store.py:64-72](semantic_store.py#L64-L72). One row per resolved bar.

| Column | What it is |
|---|---|
| `window_start` | PK |
| `data` | TEXT JSON blob with the full bar fingerprint (indicators, strategy votes, specialist signals, dashboard, deepseek narrative, outcome) — see [semantic_store.py:124-153](semantic_store.py#L124-L153) |
| `embedding` | `REAL[]` 1024-dim Cohere embed-english-v3.0 vector |

The `data` JSON is what gets embedded into the rich-text essay then sent to Cohere — it's a derived view of `deepseek_predictions` + the live indicator state at bar close.

---

## 2. What's missing for "1000 random bars through different LLMs"

The good news: the **prompt-replay path is already viable today**. Pull `full_prompt` + `actual_direction` from `deepseek_predictions`, send the prompt to GPT-4 / Claude / whatever, parse out a UP/DOWN, score against the truth. No data restructuring required to begin.

The bad news: a few structural gaps will bite once you go past a one-off comparison.

### Gaps

**A. No `model` / `prompt_version` column on `deepseek_predictions`.**
Today every row implicitly belongs to "whatever DeepSeek-routed model was running that week" (the OpenRouter migration changed this without leaving a marker). Once you start storing **multiple model verdicts per bar**, you need a key like `(window_start, model_id, prompt_version)` — otherwise everything collides on the existing PK.

**B. Prompts mutate over time.**
The prompt the model saw on 2026-01-15 is not the prompt being constructed today. Fair — the *stored* `full_prompt` is what was sent, so replays of historical prompts are honest. But "what would Claude do on prompt-template-v9 against bar X" is a *different* experiment from "what did DeepSeek do on prompt-template-v6 against bar X." Need to tag prompt template versions.

**C. No look-ahead guarantee at the schema level.**
The user's framing — *"run 1000 random bars on data that came before it"* — is already satisfied by the stored prompt (it was constructed only from then-available data). But once you start re-constructing prompts for a backtest (e.g. testing a new prompt template), there's no schema-level invariant preventing accidentally pulling a tick or indicator value computed *after* `window_start`. Need a clear "as-of `window_start`" snapshot table.

**D. Embeddings are stored in a single column.**
`pattern_history.embedding` is one slot. Testing OpenAI-large vs Cohere-v3 vs Voyage-2 means you can't keep all three side-by-side in the current schema. Need a separate embeddings table keyed by `(window_start, embedding_model)`.

**E. No backtest-run table.**
Each replay run (model, prompt template, sample of bars, results) needs its own table so you can re-query results, build leaderboards, and not stomp on previous runs.

---

## 3. Proposed schema additions

Don't break existing tables. Add new ones; keep `deepseek_predictions` as the live-trading record of truth.

### `model_predictions` (new) — replay results

```sql
CREATE TABLE model_predictions (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    window_start    DOUBLE PRECISION NOT NULL,
    model_id        TEXT NOT NULL,        -- 'claude-opus-4-7', 'gpt-4o', 'deepseek-chat-v3.1', ...
    prompt_version  TEXT NOT NULL,        -- 'v6-2026-01', 'v9-current', git SHA, whatever
    prompt_text     TEXT NOT NULL,        -- exact bytes sent (could differ from deepseek_predictions.full_prompt if testing a new template)
    raw_response    TEXT,
    signal          TEXT,                 -- parsed UP/DOWN/NEUTRAL
    confidence      DOUBLE PRECISION,
    actual_direction TEXT,                -- denormalized from deepseek_predictions for fast scoring
    correct         BOOLEAN,
    latency_ms      INTEGER,
    cost_usd        DOUBLE PRECISION,
    parse_error     TEXT,                 -- non-null when the response couldn't be parsed
    created_at      DOUBLE PRECISION
);
CREATE INDEX idx_model_predictions_run    ON model_predictions (run_id);
CREATE INDEX idx_model_predictions_model  ON model_predictions (model_id, prompt_version);
CREATE UNIQUE INDEX uniq_run_window       ON model_predictions (run_id, window_start);
```

### `backtest_runs` (new) — leaderboard rows

```sql
CREATE TABLE backtest_runs (
    run_id          UUID PRIMARY KEY,
    model_id        TEXT NOT NULL,
    prompt_version  TEXT NOT NULL,
    sample_strategy TEXT,                 -- 'random_1000', 'recent_500', 'stratified_by_session', ...
    sample_seed     INTEGER,              -- so the same 1000 bars can be re-replayed across models
    n_total         INTEGER,
    n_directional   INTEGER,              -- excludes NEUTRAL
    n_correct       INTEGER,
    win_rate        DOUBLE PRECISION,
    avg_latency_ms  DOUBLE PRECISION,
    total_cost_usd  DOUBLE PRECISION,
    started_at      DOUBLE PRECISION,
    completed_at    DOUBLE PRECISION,
    notes           TEXT
);
```

### `bar_embeddings` (new) — multi-model embeddings

```sql
CREATE TABLE bar_embeddings (
    window_start     DOUBLE PRECISION NOT NULL,
    embedding_model  TEXT NOT NULL,       -- 'cohere-embed-v3', 'openai-text-embedding-3-large', 'voyage-2', ...
    dim              INTEGER NOT NULL,
    embedding        REAL[] NOT NULL,
    created_at       DOUBLE PRECISION,
    PRIMARY KEY (window_start, embedding_model)
);
```

Migrating existing Cohere vectors out of `pattern_history.embedding` into this table is a one-liner; keep `pattern_history.embedding` populated for backwards compat for one or two releases, then drop.

### `bar_snapshot` (optional, defensive) — frozen as-of state

If you want to test new *prompt templates* (not just new models on the existing prompt), you need every input the prompt-builder reads to be available in a frozen, as-of-`window_start` form. The cleanest version:

```sql
CREATE TABLE bar_snapshot (
    window_start     DOUBLE PRECISION PRIMARY KEY,
    snapshot         JSONB NOT NULL,      -- everything the prompt builder reads: ticks last N bars, indicators, dashboard signals, specialist outputs, similar-bar IDs (NOT their resolved outcomes)
    created_at       DOUBLE PRECISION
);
```

The `data` blob in `pattern_history` is *almost* this, but it includes resolved outcomes (`actual_direction`, `deepseek_correct`, postmortem) which are look-ahead leaks for backtest purposes. A separate sanitized snapshot avoids accidents.

---

## 4. Recommended replay loop

Once the schema above exists, the replay is straightforward:

```python
# 1. Pick the bar sample (stable, reproducible)
run_id = uuid4()
sample = sample_bars(strategy="random", n=1000, seed=42)  # returns list of window_starts

# 2. For each (model, prompt_version) under test:
for model_id in ["claude-opus-4-7", "gpt-4o", "deepseek-chat-v3.1"]:
    for ws in sample:
        prompt_text = load_prompt(ws, prompt_version)          # from deepseek_predictions.full_prompt OR rebuild from bar_snapshot
        response, latency, cost = call_model(model_id, prompt_text)
        signal, conf = parse_response(response)
        actual = lookup_actual(ws)                             # from deepseek_predictions.actual_direction
        correct = (signal == actual) if signal in ("UP","DOWN") else None
        insert_model_prediction(run_id, ws, model_id, ...)
    finalize_backtest_run(run_id, ...)                          # roll up win_rate into backtest_runs
```

Two important properties this gets you for free:
- **Reproducibility:** `sample_seed` means any future model can be tested on the *exact same* 1000 bars. No more apples-to-oranges leaderboard.
- **No look-ahead:** stored `full_prompt` was constructed only from then-available data, and `actual_direction` is only used for scoring after the fact — never fed into the prompt.

---

## 5. Embedding-model comparison loop

Embedding swaps are slightly different — the question isn't "did the embedding pick the right direction" but "did the embedding retrieve the right neighbors that helped the LLM pick the right direction."

Two-phase test:

1. **Re-embed** all bars with the new model → write to `bar_embeddings` (don't touch the existing Cohere column).
2. **Replay the historical analyst** with neighbors retrieved via the new embedding's vectors. Score the LLM verdicts that result. The embedding model with the higher downstream win rate wins.

This means an embedding-model A/B test is structurally a two-step pipeline: re-embed → replay. Keeping them in distinct tables prevents one experiment from polluting the other.

---

## 6. Suggested implementation order

Don't build all of this up front. Bar count is currently in the low thousands, not 100k. Sequencing:

1. **Add `model_id` + `prompt_version` columns to `deepseek_predictions`** so going forward, every live prediction is tagged. Backfill historical rows with `model_id='deepseek-v?-unknown'` and `prompt_version='legacy'`. Cheap, unblocks everything else.
2. **Build `model_predictions` + `backtest_runs` tables** when you actually want to run the first cross-model test. Until then, YAGNI.
3. **Build `bar_embeddings`** only when the first non-Cohere embedding test is on the table. Keep the current Cohere column working until then.
4. **Build `bar_snapshot`** only if/when prompt-template experiments become a thing. Today's `full_prompt` field is enough for "test a new model on the old prompt."

Step 1 is the only step worth doing pre-emptively; the rest are pull-not-push.

---

## 7. What does NOT need to change

- `full_prompt`, `raw_response`, `signal`, `actual_direction`, `correct` — already there, already correct, already enough for the literal "send historical prompt to a new model" experiment.
- `pattern_history.data` JSON — fine as-is for similarity search; only a problem if you start reusing it as a prompt input (because of resolved-outcome leakage).
- Postmortems — orthogonal to backtesting. Leave alone.

---

## TL;DR

- **Yes**, each bar already holds (prompt, guess, actual). Triple lives in `deepseek_predictions.{full_prompt, signal, actual_direction}`.
- **Replaying 1000 random bars on a different LLM** works *today* with that data — just iterate, call, parse, score. No schema work strictly required.
- **To do it well at 100k bars**, add four things over time: `model_id`/`prompt_version` columns, a `model_predictions` results table, a `backtest_runs` leaderboard table, and a multi-model `bar_embeddings` table. Build them when needed, not now.
- **`BACKTESTING_TEST.md` is fiction.** Delete or rewrite it before someone tries to follow it.
