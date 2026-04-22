# Backtesting Framework

Compare different LLM models and embedding models against your historical prediction data.

## Architecture

**Two independent leaderboards:**
1. **LLM leaderboard** — test DeepSeek vs Claude vs GPT-4 etc.
2. **Embedding leaderboard** — test Cohere v3 vs OpenAI vs Jina etc.

Each test:
- Runs against **historical prompts + outcomes** (no real trades, no cost during evaluation)
- Calculates **win rate** (how many correct predictions)
- Tracks **latency** and **estimated API cost**
- Stores detailed results for audit trail

## Usage

### Admin API

All endpoints require admin authentication. Trigger tests manually.

#### Test an LLM model

```bash
curl -X POST http://localhost:3000/backtesting/test-llm \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude",
    "api_key": "sk-ant-...",
    "samples": 200,
    "temperature": 0.1,
    "max_tokens": 500
  }'
```

**Available models** (from `config/llm_models.js`):
- `deepseek` — baseline
- `claude` — Claude Opus
- `gpt4` — GPT-4 Turbo
- `gpt35` — GPT-3.5 Turbo
- `cohere` — Cohere Command-R
- `llama` — Llama 2 70B

**Response:**
```json
{
  "job_id": "llm_claude_1713785700000",
  "status": "queued",
  "model": "claude",
  "message": "Testing claude against 200 prompts..."
}
```

#### Test an embedding model

```bash
curl -X POST http://localhost:3000/backtesting/test-embedding \
  -H "Content-Type: application/json" \
  -d '{
    "embedding": "openai_large",
    "api_key": "sk-...",
    "samples": 250
  }'
```

**Available embeddings** (from `config/embedding_models.js`):
- `cohere_v3` — baseline
- `cohere_v2` — older, cheaper
- `openai_large` — 3072 dims
- `openai_small` — 1536 dims, cheaper
- `jina` — open-source
- `voyage` — Voyage AI

**Response:**
```json
{
  "job_id": "emb_openai_large_1713785700000",
  "status": "queued",
  "embedding": "openai_large",
  "message": "Testing openai_large against 250 windows..."
}
```

#### Check test progress

```bash
curl http://localhost:3000/backtesting/status/llm_claude_1713785700000
```

**Response (while running):**
```json
{
  "job_id": "llm_claude_1713785700000",
  "status": "in_progress",
  "current": 87,
  "total": 200,
  "correct": 53,
  "accuracy": "60.9",
  "cost": "4.32"
}
```

**Response (completed):**
```json
{
  "job_id": "llm_claude_1713785700000",
  "status": "completed",
  "result": {
    "model_name": "claude-opus-4-1",
    "provider": "anthropic",
    "tested_at": "2026-04-22T14:35:00Z",
    "samples": 200,
    "correct": 122,
    "neutral": 25,
    "total": 200,
    "win_rate": "61.0",
    "neutral_rate": "12.5",
    "avg_latency_ms": "1800",
    "p95_latency_ms": "3200",
    "cost_usd": 8.50
  }
}
```

#### Get leaderboard

```bash
# Full results (both LLM and embedding)
curl http://localhost:3000/backtesting/results

# LLM leaderboard only
curl http://localhost:3000/backtesting/results/llm

# Embedding leaderboard only
curl http://localhost:3000/backtesting/results/embedding
```

**Response:**
```json
{
  "llm_results": [
    {
      "model_name": "claude-opus-4-1",
      "provider": "anthropic",
      "tested_at": "2026-04-22T14:35:00Z",
      "samples": 200,
      "correct": 122,
      "win_rate": "61.0",
      "avg_latency_ms": "1800",
      "cost_usd": 8.50
    },
    {
      "model_name": "deepseek-chat",
      "provider": "deepseek",
      "tested_at": "2026-04-22T09:15:00Z",
      "samples": 250,
      "correct": 145,
      "win_rate": "58.0",
      "avg_latency_ms": "2400",
      "cost_usd": 12.50
    }
  ],
  "embedding_results": [
    {
      "embedding_name": "cohere/embed-english-v3.0",
      "provider": "cohere",
      "tested_at": "2026-04-20T14:30:00Z",
      "samples": 250,
      "correct": 145,
      "win_rate": "58.0",
      "cost_usd": 0.005
    }
  ],
  "last_updated": "2026-04-22T14:35:00Z"
}
```

## Data Flow

### Recording (happens automatically)

Every DeepSeek prediction stores:
```json
{
  "window_start": 1713785700,
  "window_end": 1713786000,
  "full_prompt": "You are analyzing BTC...",
  "decision": "UP",
  "confidence": 75,
  "actual_direction": "UP",
  "correct": true,
  "start_price": 67000,
  "end_price": 67150
}
```

These go into `backtesting/records/prompts.jsonl`

### Testing

When you test a new LLM:
1. Load all prompts from `prompts.jsonl`
2. Send each prompt to the new model's API
3. Compare decision vs `actual_direction`
4. Calculate accuracy, latency, cost
5. Store individual decisions in `backtesting/llm_results/{model_name}.jsonl`
6. Update summary in `backtesting/results.json`

When you test a new embedding:
1. Load all windows from historical data
2. Re-embed microstructure signals with new embedding model
3. Recalculate ensemble weights
4. Get new predictions
5. Compare vs `actual_direction`
6. Update `backtesting/results.json`

## File Structure

```
backtesting/
├── config/
│   ├── llm_models.js              # Model definitions
│   └── embedding_models.js        # Embedding definitions
│
├── test_runner.js                 # Core logic (no HTTP)
├── api.js                         # Express routes
├── results.json                   # Leaderboard (updated per test)
│
└── records/
    ├── prompts.jsonl              # All historical prompts + outcomes (immutable)
    ├── llm_results/
    │   ├── deepseek-chat.jsonl
    │   ├── claude-opus-4-1.jsonl
    │   └── ...
    └── embedding_results/
        ├── cohere_embed-english-v3.0.jsonl
        └── ...
```

## Comparison Example

After testing Claude vs DeepSeek:

```
LLM Leaderboard:
┌──────────────────┬──────────┬────────┬─────────┬──────────┐
│ Model            │ Win Rate │ Samples│ Latency │ Cost     │
├──────────────────┼──────────┼────────┼─────────┼──────────┤
│ Claude Opus ⭐   │ 61.0%    │ 200    │ 1800ms  │ $8.50    │
│ DeepSeek         │ 58.0%    │ 250    │ 2400ms  │ $12.50   │
│ GPT-4 Turbo      │ 59.8%    │ 150    │ 3200ms  │ $18.00   │
└──────────────────┴──────────┴────────┴─────────┴──────────┘

Winner: Claude Opus (+3.0% vs DeepSeek, 33% cheaper)
```

## Cost Estimation

Each model has cost config ($ per 1M tokens):
- Cohere: $0.0014-0.002 per 1K tokens (cheapest)
- DeepSeek: similar pricing
- Claude: $15 per 1M input tokens
- GPT-4: $10-30 per 1M tokens
- Jina: free (open-source)

Test cost is **one-time per model**, not recurring. Testing 200 prompts costs ~$5-30 depending on model.

## Next Steps

1. **Integrate into main server** — add routes to your Express app
2. **Feed historical prompts** — ensure `prompts.jsonl` is populated from DeepSeek predictions
3. **Implement actual API calls** — fill in `callLLMAPI()` in `test_runner.js`
4. **Add dashboard UI** — show leaderboard in your React app
5. **Add admin auth** — protect `/backtesting/*` endpoints
