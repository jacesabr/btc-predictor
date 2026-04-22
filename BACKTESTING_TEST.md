# ⚡ BACKTESTING FRAMEWORK — HOW TO USE

> **IMPORTANT:** This is how you test different LLM & embedding models against historical predictions

---

## Quick Start (2 minutes)

### 1️⃣ Test an LLM Model

```bash
curl -X POST http://localhost:3000/backtesting/test-llm \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude",
    "api_key": "sk-ant-YOUR-KEY-HERE",
    "samples": 200,
    "temperature": 0.1
  }'
```

**Response:**
```json
{
  "job_id": "llm_claude_1713785700000",
  "status": "queued",
  "message": "Testing claude against 200 prompts..."
}
```

### 2️⃣ Check Progress

```bash
curl http://localhost:3000/backtesting/status/llm_claude_1713785700000
```

**While running:**
```json
{
  "status": "in_progress",
  "current": 87,
  "total": 200,
  "correct": 53,
  "accuracy": "60.9%",
  "cost": "$4.32"
}
```

**After completion:**
```json
{
  "status": "completed",
  "result": {
    "model_name": "claude-opus-4-1",
    "win_rate": "61.0%",
    "samples": 200,
    "avg_latency_ms": 1800,
    "cost_usd": 8.50
  }
}
```

### 3️⃣ View Leaderboard

```bash
curl http://localhost:3000/backtesting/results/llm
```

**You'll see:**
```json
{
  "llm_results": [
    {
      "model_name": "claude-opus-4-1",
      "win_rate": "61.0%",
      "samples": 200,
      "avg_latency_ms": 1800,
      "cost_usd": 8.50,
      "tested_at": "2026-04-22T14:35:00Z"
    },
    {
      "model_name": "deepseek-chat",
      "win_rate": "58.0%",
      "samples": 250,
      "avg_latency_ms": 2400,
      "cost_usd": 12.50,
      "tested_at": "2026-04-22T09:15:00Z"
    }
  ],
  "last_updated": "2026-04-22T14:35:00Z"
}
```

---

## Available Models to Test

### LLMs

```
"deepseek"   → deepseek-chat (current baseline)
"claude"     → claude-opus-4-1 (most capable)
"gpt4"       → gpt-4-turbo-preview
"gpt35"      → gpt-3.5-turbo (fastest, cheapest)
"cohere"     → command-r
"llama"      → llama2-70b (open-source)
```

### Embeddings

```
"cohere_v3"      → embed-english-v3.0 (current baseline)
"cohere_v2"      → embed-english-v2.0 (older, cheaper)
"openai_large"   → text-embedding-3-large (3072 dims)
"openai_small"   → text-embedding-3-small (1536 dims, faster)
"jina"           → jina-embeddings-v2-base-en (free, open-source)
"voyage"         → voyage-2
```

---

## Test Embedding Models

```bash
curl -X POST http://localhost:3000/backtesting/test-embedding \
  -H "Content-Type: application/json" \
  -d '{
    "embedding": "openai_large",
    "api_key": "sk-YOUR-KEY-HERE",
    "samples": 250
  }'
```

Same workflow: get job_id → check status → view results

---

## Cost Reference

**One-time cost to test a model against 200-250 prompts:**

| Model | Cost |
|-------|------|
| Claude Opus | $8-10 |
| GPT-4 Turbo | $15-30 |
| DeepSeek | $5-8 |
| Cohere | $1-2 |
| Jina | $0 (free) |

**NOT recurring. Only pay when you decide to test a new model.**

---

## Real Example: Switch to Claude

**Step 1: Test Claude against your 200 most recent prompts**
```bash
curl -X POST http://localhost:3000/backtesting/test-llm \
  -d '{"model": "claude", "api_key": "sk-ant-...", "samples": 200}'
```

**Step 2: Wait for completion, check leaderboard**
```bash
curl http://localhost:3000/backtesting/results/llm
```

**Result:**
```
Claude Opus:    61.0% win rate  →  +3.0% vs DeepSeek ✅
DeepSeek:       58.0% win rate  →  baseline
```

**Step 3: Decision**
- ✅ Claude wins? → Update your production code to use Claude API
- ❌ DeepSeek still better? → Test another model (GPT-4, Cohere, etc.)

---

## What Gets Tested?

The system tests against **historical prompts + actual outcomes**:

```
For each prompt:
  1. Send to new model (Claude, GPT-4, etc.)
  2. Compare decision (UP/DOWN) vs actual market direction
  3. Mark as WIN or LOSS
  4. Calculate accuracy

Result: Win rate % (how many predictions were correct)
```

**Example:**
```
Prompt 1: "BTC order book..." → Claude says "UP" → Actually went UP ✓ WIN
Prompt 2: "BTC funding..."    → Claude says "DOWN" → Actually went UP ✗ LOSS
Prompt 3: "BTC sentiment..."  → Claude says "NEUTRAL" → N/A (no trade)
...
200 prompts total → 122 correct → 61% win rate
```

---

## Before You Start

✅ **Make sure:**
1. Server is running (`npm start`)
2. You have API key for the model you want to test
3. Historical prompts are being saved to `backtesting/records/prompts.jsonl`

❌ **Not working?**
- Check `backtesting/SETUP.md` for integration steps
- Check `backtesting/INTEGRATION.md` for implementation details
- Verify `/backtesting` routes are mounted in your `server.js`

---

## Common Workflows

### Compare all available models

```bash
# Test 1: Claude
curl -X POST http://localhost:3000/backtesting/test-llm \
  -d '{"model": "claude", "api_key": "sk-ant-...", "samples": 200}'

# Wait ~1 min...

# Test 2: GPT-4
curl -X POST http://localhost:3000/backtesting/test-llm \
  -d '{"model": "gpt4", "api_key": "sk-...", "samples": 200}'

# Wait ~2 min...

# Test 3: Cohere
curl -X POST http://localhost:3000/backtesting/test-llm \
  -d '{"model": "cohere", "api_key": "...", "samples": 200}'

# View final leaderboard
curl http://localhost:3000/backtesting/results/llm
```

### Test if new embedding improves accuracy

```bash
# Current baseline (Cohere v3)
# Already has results in backtesting/results.json

# Test OpenAI Large embedding
curl -X POST http://localhost:3000/backtesting/test-embedding \
  -d '{"embedding": "openai_large", "api_key": "sk-...", "samples": 250}'

# Check leaderboard
curl http://localhost:3000/backtesting/results/embedding
```

### Monitor active tests

```bash
curl http://localhost:3000/backtesting/jobs
```

Shows all running/recent tests with progress.

---

## What Happens Behind the Scenes

1. **You provide:** model name + API key + how many prompts to test
2. **System loads:** historical prompts from `backtesting/records/prompts.jsonl`
3. **System runs:** sends each prompt to the model's API, captures response
4. **System compares:** prediction vs actual outcome, calculates accuracy
5. **System saves:** results to `backtesting/results.json` leaderboard
6. **Results show:** win_rate %, latency, estimated cost

**All runs stored permanently.** You can compare results weeks later.

---

## Files You Need to Know

| File | Purpose |
|------|---------|
| `backtesting/` | Main folder (all your testing stuff) |
| `backtesting/results.json` | **Leaderboard** (what you check after tests) |
| `backtesting/records/prompts.jsonl` | Historical data (gets populated automatically) |
| `backtesting/config/llm_models.js` | List of available LLM models |
| `backtesting/config/embedding_models.js` | List of available embedding models |
| `backtesting/SETUP.md` | Full setup instructions |
| `backtesting/INTEGRATION.md` | How to integrate with your server |
| `backtesting/README.md` | Complete documentation |

---

## Decision Tree

```
"I want to test if switching to Claude would improve accuracy"
        ↓
Test Claude: curl -X POST /backtesting/test-llm
        ↓
Check result: curl /backtesting/results/llm
        ↓
┌─ Claude win_rate > DeepSeek? ─┐
│                                │
YES                             NO
│                                │
✅ Switch to Claude        ❌ Keep DeepSeek
                           or test another model
```

---

## 🎯 TLDR

1. Want to test a new LLM? → `POST /backtesting/test-llm`
2. Want to test a new embedding? → `POST /backtesting/test-embedding`
3. Want to see results? → `GET /backtesting/results/llm` or `GET /backtesting/results/embedding`
4. Want to check progress? → `GET /backtesting/status/{job_id}`

**Cost:** One-time per model ($5-30). **No recurring charges.**

**Time:** 1-5 minutes per model depending on sample size.

**Decision:** Based on win_rate %, latency, and cost.

---

**Full docs:** `backtesting/README.md`  
**Setup help:** `backtesting/SETUP.md`  
**Integration help:** `backtesting/INTEGRATION.md`

---

## 📌 Bookmark This

This file is your reference. When you want to test a new model, just:
1. Come back here
2. Copy a curl command
3. Paste it in terminal
4. Check results

Done.
