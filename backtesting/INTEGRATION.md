# Integration Guide

## 1. Mount Backtesting Routes in Main Server

In your main `server.js` or `app.js`:

```javascript
const express = require("express");
const backtestingRoutes = require("./backtesting/api");

const app = express();

// ... other middleware ...

// Mount backtesting endpoints
app.use("/backtesting", backtestingRoutes);

// Now available:
// POST  /backtesting/test-llm
// POST  /backtesting/test-embedding
// GET   /backtesting/status/:job_id
// GET   /backtesting/results
// GET   /backtesting/results/llm
// GET   /backtesting/results/embedding
// GET   /backtesting/jobs
```

## 2. Populate Historical Prompts

Every DeepSeek prediction should write to `backtesting/records/prompts.jsonl`.

In your prediction storage code (where DeepSeek results are saved):

```javascript
const fs = require("fs");
const path = require("path");

async function saveDeepSeekPrediction(prediction, outcome) {
  // ... existing code to save to MongoDB ...

  // Also write to backtesting records
  const record = {
    window_start: prediction.window_start,
    window_end: prediction.window_end,
    full_prompt: prediction.full_prompt,  // Keep the EXACT prompt sent
    decision: prediction.signal,           // "UP", "DOWN", or "NEUTRAL"
    confidence: prediction.confidence,
    actual_direction: outcome.actual_direction,  // "UP" or "DOWN"
    correct: prediction.signal === outcome.actual_direction && prediction.signal !== "NEUTRAL",
    start_price: prediction.start_price,
    end_price: outcome.end_price
  };

  // Append to prompts.jsonl for backtesting
  const recordsPath = path.join(__dirname, "backtesting/records/prompts.jsonl");
  fs.appendFileSync(recordsPath, JSON.stringify(record) + "\n");
}
```

## 3. Add Admin Authentication

Protect backtesting endpoints (optional but recommended):

```javascript
const adminAuth = (req, res, next) => {
  const adminKey = process.env.ADMIN_KEY;
  const providedKey = req.headers["x-admin-key"] || req.query.admin_key;
  
  if (providedKey !== adminKey) {
    return res.status(403).json({ error: "Unauthorized" });
  }
  
  next();
};

// Protect routes
app.use("/backtesting", adminAuth, backtestingRoutes);
```

**Usage:**
```bash
curl -X POST http://localhost:3000/backtesting/test-llm \
  -H "X-Admin-Key: your-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"model": "claude", "api_key": "...", ...}'
```

## 4. Implement Actual API Calls

In `backtesting/test_runner.js`, fill in `callLLMAPI()`:

### For Anthropic (Claude)

```javascript
async callLLMAPI(config, prompt, apiKey, options) {
  if (config.provider === "anthropic") {
    const { Anthropic } = require("@anthropic-ai/sdk");
    const client = new Anthropic({ apiKey });

    const t0 = Date.now();
    const response = await client.messages.create({
      model: config.model,
      max_tokens: options.maxTokens,
      temperature: options.temperature,
      messages: [{ role: "user", content: prompt }]
    });
    const latency = Date.now() - t0;

    // Parse response for decision
    const text = response.content[0].text;
    const decision = text.includes("UP") ? "UP" : text.includes("DOWN") ? "DOWN" : "NEUTRAL";
    const confidence = this.extractConfidence(text);

    return {
      decision,
      confidence,
      latency,
      tokensInput: response.usage.input_tokens,
      tokensOutput: response.usage.output_tokens
    };
  }

  // ... similar for OpenAI, DeepSeek, etc.
}

extractConfidence(text) {
  const match = text.match(/confidence[:\s]+(\d+)/i);
  return match ? parseInt(match[1]) : 50;
}
```

### For OpenAI (GPT-4)

```javascript
async callLLMAPI(config, prompt, apiKey, options) {
  if (config.provider === "openai") {
    const OpenAI = require("openai");
    const client = new OpenAI({ apiKey });

    const t0 = Date.now();
    const response = await client.chat.completions.create({
      model: config.model,
      messages: [{ role: "user", content: prompt }],
      max_tokens: options.maxTokens,
      temperature: options.temperature
    });
    const latency = Date.now() - t0;

    const text = response.choices[0].message.content;
    const decision = text.includes("UP") ? "UP" : text.includes("DOWN") ? "DOWN" : "NEUTRAL";
    const confidence = this.extractConfidence(text);

    return {
      decision,
      confidence,
      latency,
      tokensInput: response.usage.prompt_tokens,
      tokensOutput: response.usage.completion_tokens
    };
  }
}
```

### For DeepSeek

```javascript
async callLLMAPI(config, prompt, apiKey, options) {
  if (config.provider === "deepseek") {
    const response = await fetch("https://api.deepseek.com/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: config.model,
        messages: [{ role: "user", content: prompt }],
        max_tokens: options.maxTokens,
        temperature: options.temperature
      })
    });

    const data = await response.json();
    const text = data.choices[0].message.content;
    const decision = text.includes("UP") ? "UP" : text.includes("DOWN") ? "DOWN" : "NEUTRAL";
    const confidence = this.extractConfidence(text);

    return {
      decision,
      confidence,
      latency: data.usage.total_time || 0,
      tokensInput: data.usage.prompt_tokens,
      tokensOutput: data.usage.completion_tokens
    };
  }
}
```

## 5. (Optional) Dashboard Integration

Add a backtesting tab to your React dashboard:

```jsx
function BacktestingTab() {
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [testModel, setTestModel] = useState("claude");
  const [apiKey, setApiKey] = useState("");
  const [samples, setSamples] = useState(200);

  const handleTestLLM = async () => {
    setLoading(true);
    try {
      const response = await fetch("/backtesting/test-llm", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: testModel,
          api_key: apiKey,
          samples
        })
      });
      const data = await response.json();
      console.log(`Test started: ${data.job_id}`);
      
      // Poll status
      pollStatus(data.job_id);
    } finally {
      setLoading(false);
    }
  };

  const pollStatus = async (jobId) => {
    const interval = setInterval(async () => {
      const response = await fetch(`/backtesting/status/${jobId}`);
      const status = await response.json();
      
      if (status.status === "completed" || status.status === "failed") {
        clearInterval(interval);
        // Reload results
        const resultsResponse = await fetch("/backtesting/results");
        setResults(await resultsResponse.json());
      }
    }, 2000);
  };

  return (
    <div>
      <h2>LLM Backtesting</h2>
      
      <div>
        <select value={testModel} onChange={e => setTestModel(e.target.value)}>
          <option value="claude">Claude Opus</option>
          <option value="gpt4">GPT-4 Turbo</option>
          <option value="deepseek">DeepSeek</option>
        </select>
        
        <input 
          type="password" 
          placeholder="API Key" 
          value={apiKey}
          onChange={e => setApiKey(e.target.value)}
        />
        
        <input 
          type="number" 
          value={samples}
          onChange={e => setSamples(parseInt(e.target.value))}
          min="10" max="500"
        />
        
        <button onClick={handleTestLLM} disabled={loading}>
          {loading ? "Testing..." : "Start Test"}
        </button>
      </div>

      {results && (
        <div>
          <h3>Leaderboard</h3>
          <table>
            <thead>
              <tr>
                <th>Model</th>
                <th>Win Rate</th>
                <th>Latency</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody>
              {results.llm_results.map(r => (
                <tr key={r.model_name}>
                  <td>{r.model_name}</td>
                  <td>{r.win_rate}%</td>
                  <td>{r.avg_latency_ms}ms</td>
                  <td>${r.cost_usd}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
```

## 6. Environment Variables

Add to `.env`:

```
# Backtesting admin
ADMIN_KEY=your-secret-admin-key-here

# Optional: test API keys (or provide at test time)
TEST_CLAUDE_KEY=sk-ant-...
TEST_GPT4_KEY=sk-...
TEST_DEEPSEEK_KEY=sk-...
```

## Example: Full Test Flow

```bash
# 1. Admin starts a test
curl -X POST http://localhost:3000/backtesting/test-llm \
  -H "X-Admin-Key: $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude",
    "api_key": "sk-ant-...",
    "samples": 200
  }'

# Response: {"job_id": "llm_claude_1713785700000", "status": "queued"}

# 2. Check progress
curl "http://localhost:3000/backtesting/status/llm_claude_1713785700000" \
  -H "X-Admin-Key: $ADMIN_KEY"

# Response: {"job_id": "llm_claude_1713785700000", "status": "in_progress", "current": 87, ...}

# 3. After completion, check results
curl http://localhost:3000/backtesting/results/llm \
  -H "X-Admin-Key: $ADMIN_KEY"

# Response:
# {
#   "llm_results": [
#     {"model_name": "claude-opus-4-1", "win_rate": "61.0%", ...},
#     {"model_name": "deepseek-chat", "win_rate": "58.0%", ...}
#   ]
# }
```

## Troubleshooting

**Prompts.jsonl is empty**
- Make sure DeepSeek predictions are writing to `backtesting/records/prompts.jsonl`
- Check that `full_prompt` is stored with each prediction

**Test hangs or times out**
- Check API credentials are correct
- Verify rate limits on the LLM API
- Check network connectivity

**Cost estimates seem wrong**
- Update token costs in `llm_models.js` and `embedding_models.js`
- Cost is estimated; actual may differ slightly

**Results not updating**
- Ensure admin auth is correct
- Check `/backtesting/jobs` to see if test completed
- Look at test output logs for errors
