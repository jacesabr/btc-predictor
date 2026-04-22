/**
 * Core backtesting runner
 * Tests LLMs and embedding models against historical prompts/outcomes
 */

const fs = require("fs");
const path = require("path");

const llmConfigs = require("./config/llm_models");
const embeddingConfigs = require("./config/embedding_models");

class BacktestRunner {
  constructor() {
    this.resultsFile = path.join(__dirname, "results.json");
    this.recordsDir = path.join(__dirname, "records");
    this.promptsFile = path.join(this.recordsDir, "prompts.jsonl");
    this.llmResultsDir = path.join(this.recordsDir, "llm_results");
    this.embeddingResultsDir = path.join(this.recordsDir, "embedding_results");

    this.ensureDirectories();
    this.ensureResultsFile();
  }

  ensureDirectories() {
    [this.recordsDir, this.llmResultsDir, this.embeddingResultsDir].forEach(dir => {
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    });
  }

  ensureResultsFile() {
    if (!fs.existsSync(this.resultsFile)) {
      fs.writeFileSync(this.resultsFile, JSON.stringify({
        llm_results: [],
        embedding_results: [],
        last_updated: new Date().toISOString()
      }, null, 2));
    }
  }

  // ─────────────────────────────────────────────────────────────
  // LLM TESTING
  // ─────────────────────────────────────────────────────────────

  async testLLMModel(modelName, apiKey, options = {}) {
    const config = llmConfigs[modelName];
    if (!config) throw new Error(`Unknown LLM model: ${modelName}`);

    const {
      samples = 200,
      temperature = config.defaultTemp,
      maxTokens = 500,
      onProgress = null
    } = options;

    console.log(`\n🧪 Testing LLM: ${config.id} (${config.provider})`);
    console.log(`📋 Loading historical prompts...`);

    const prompts = this.loadHistoricalPrompts(samples);
    if (prompts.length === 0) throw new Error("No historical prompts found");

    const results = {
      model_name: config.id,
      provider: config.provider,
      tested_at: new Date().toISOString(),
      samples: prompts.length,
      correct: 0,
      neutral: 0,
      total: 0,
      latencies: [],
      cost_usd: 0,
      decisions: [],
      error: null
    };

    let tokensUsed = 0;
    let costAccum = 0;

    // Test each prompt
    for (let i = 0; i < prompts.length; i++) {
      const prompt = prompts[i];
      const t0 = Date.now();

      try {
        // Call the LLM API
        const { decision, confidence, latency, tokensInput, tokensOutput } =
          await this.callLLMAPI(config, prompt.full_prompt, apiKey, { temperature, maxTokens });

        const latencyMs = Date.now() - t0;
        const isCorrect = decision === prompt.actual_direction && decision !== "NEUTRAL";
        const isNeutral = decision === "NEUTRAL";

        results.correct += isCorrect ? 1 : 0;
        results.neutral += isNeutral ? 1 : 0;
        results.total++;
        results.latencies.push(latencyMs);

        // Estimate cost
        const cost = this.estimateCost(config, tokensInput, tokensOutput);
        costAccum += cost;

        results.decisions.push({
          window_start: prompt.window_start,
          prediction: decision,
          confidence,
          actual: prompt.actual_direction,
          correct: isCorrect,
          latency_ms: latencyMs,
          tokens: { input: tokensInput, output: tokensOutput }
        });

        if (onProgress) {
          onProgress({
            current: i + 1,
            total: prompts.length,
            correct: results.correct,
            accuracy: (results.correct / (i + 1) * 100).toFixed(1),
            cost: costAccum.toFixed(2)
          });
        }

        console.log(`  [${i + 1}/${prompts.length}] ${decision} (${confidence}%) → ${prompt.actual_direction ? "✓" : "✕"} ${latencyMs}ms`);
      } catch (err) {
        console.error(`  ✗ Error on prompt ${i + 1}:`, err.message);
        results.error = err.message;
        results.total++;
      }
    }

    // Calculate statistics
    results.cost_usd = costAccum;
    results.win_rate = (results.correct / results.total * 100).toFixed(1);
    results.neutral_rate = (results.neutral / results.total * 100).toFixed(1);
    results.avg_latency_ms = results.total > 0 ? (results.latencies.reduce((a, b) => a + b, 0) / results.latencies.length).toFixed(0) : 0;
    results.p95_latency_ms = results.total > 0 ? this.percentile(results.latencies, 0.95).toFixed(0) : 0;

    // Save detailed results
    const resultsPath = path.join(this.llmResultsDir, `${config.id.replace(/\//g, "_")}.jsonl`);
    fs.appendFileSync(resultsPath, results.decisions.map(d => JSON.stringify(d)).join("\n") + "\n");

    // Update leaderboard
    this.updateLLMResults(results);

    console.log(`\n✅ Test complete for ${config.id}`);
    console.log(`   Win rate: ${results.win_rate}% (${results.correct}/${results.total})`);
    console.log(`   Avg latency: ${results.avg_latency_ms}ms`);
    console.log(`   Est. cost: $${results.cost_usd.toFixed(2)}`);

    return results;
  }

  async callLLMAPI(config, prompt, apiKey, options) {
    // Placeholder - implement per-provider
    // This would call the actual API (Anthropic, OpenAI, DeepSeek, etc.)

    // For now, return mock response
    const latency = Math.random() * 2000 + 1000;
    const decisions = ["UP", "DOWN", "NEUTRAL"];
    const decision = decisions[Math.floor(Math.random() * 3)];

    return {
      decision,
      confidence: 50 + Math.random() * 40,
      latency,
      tokensInput: prompt.length / 4,
      tokensOutput: 150
    };
  }

  // ─────────────────────────────────────────────────────────────
  // EMBEDDING TESTING
  // ─────────────────────────────────────────────────────────────

  async testEmbeddingModel(embeddingName, apiKey, options = {}) {
    const config = embeddingConfigs[embeddingName];
    if (!config) throw new Error(`Unknown embedding model: ${embeddingName}`);

    const { samples = 250, onProgress = null } = options;

    console.log(`\n🔤 Testing Embedding: ${config.id} (${config.provider})`);
    console.log(`📋 Loading historical windows...`);

    // Load historical windows with their actual outcomes
    // (This would re-embed signals and recalculate ensemble)
    const windows = this.loadHistoricalWindows(samples);
    if (windows.length === 0) throw new Error("No historical windows found");

    const results = {
      embedding_name: config.id,
      provider: config.provider,
      tested_at: new Date().toISOString(),
      samples: windows.length,
      correct: 0,
      neutral: 0,
      total: 0,
      cost_usd: 0,
      ensemble_stability: 0,
      decisions: [],
      error: null
    };

    // Re-embed signals with new embedding model
    console.log(`📊 Re-embedding all microstructure signals...`);
    let costAccum = 0;

    for (let i = 0; i < windows.length; i++) {
      const window = windows[i];

      try {
        // Re-embed the signals for this window
        const { newPrediction, cost } = await this.reEmbedAndPredict(
          config,
          window,
          apiKey
        );

        const isCorrect = newPrediction === window.actual_direction && newPrediction !== "NEUTRAL";
        const isNeutral = newPrediction === "NEUTRAL";

        results.correct += isCorrect ? 1 : 0;
        results.neutral += isNeutral ? 1 : 0;
        results.total++;
        costAccum += cost;

        results.decisions.push({
          window_start: window.window_start,
          prediction: newPrediction,
          actual: window.actual_direction,
          correct: isCorrect
        });

        if (onProgress) {
          onProgress({
            current: i + 1,
            total: windows.length,
            correct: results.correct,
            accuracy: (results.correct / (i + 1) * 100).toFixed(1),
            cost: costAccum.toFixed(2)
          });
        }

        console.log(`  [${i + 1}/${windows.length}] ${newPrediction} → ${window.actual_direction ? "✓" : "✕"}`);
      } catch (err) {
        console.error(`  ✗ Error on window ${i + 1}:`, err.message);
        results.error = err.message;
        results.total++;
      }
    }

    // Calculate stats
    results.cost_usd = costAccum;
    results.win_rate = (results.correct / results.total * 100).toFixed(1);
    results.neutral_rate = (results.neutral / results.total * 100).toFixed(1);

    // Save detailed results
    const resultsPath = path.join(this.embeddingResultsDir, `${config.id.replace(/\//g, "_")}.jsonl`);
    fs.appendFileSync(resultsPath, results.decisions.map(d => JSON.stringify(d)).join("\n") + "\n");

    // Update leaderboard
    this.updateEmbeddingResults(results);

    console.log(`\n✅ Test complete for ${config.id}`);
    console.log(`   Win rate: ${results.win_rate}% (${results.correct}/${results.total})`);
    console.log(`   Est. cost: $${results.cost_usd.toFixed(2)}`);

    return results;
  }

  async reEmbedAndPredict(embeddingConfig, window, apiKey) {
    // Placeholder - would:
    // 1. Extract microstructure signals from window
    // 2. Embed them with new embedding model
    // 3. Recalculate ensemble weights
    // 4. Get new prediction

    const cost = 0.001; // Mock cost
    const decisions = ["UP", "DOWN", "NEUTRAL"];
    const prediction = decisions[Math.floor(Math.random() * 3)];

    return { newPrediction: prediction, cost };
  }

  // ─────────────────────────────────────────────────────────────
  // HELPERS
  // ─────────────────────────────────────────────────────────────

  loadHistoricalPrompts(limit = 200) {
    const prompts = [];
    try {
      const data = fs.readFileSync(this.promptsFile, "utf8");
      data.split("\n").filter(Boolean).forEach(line => {
        if (prompts.length >= limit) return;
        try {
          prompts.push(JSON.parse(line));
        } catch (e) { /* skip malformed */ }
      });
    } catch (e) {
      console.warn("No prompts file found, using empty set");
    }
    return prompts;
  }

  loadHistoricalWindows(limit = 250) {
    // Similar to loadHistoricalPrompts but for embeddings test
    return this.loadHistoricalPrompts(limit); // For now, use prompts
  }

  estimateCost(config, tokensInput, tokensOutput) {
    if (config.costPerMTok) {
      return (tokensInput / 1e6 * config.costPerMTok) +
             (tokensOutput / 1e6 * (config.costPerOutputMTok || config.costPerMTok));
    }
    return 0;
  }

  percentile(arr, p) {
    const sorted = arr.sort((a, b) => a - b);
    const idx = Math.ceil(sorted.length * p) - 1;
    return sorted[Math.max(0, idx)];
  }

  updateLLMResults(newResult) {
    const allResults = JSON.parse(fs.readFileSync(this.resultsFile, "utf8"));

    // Remove old entry for same model if exists
    allResults.llm_results = allResults.llm_results.filter(r => r.model_name !== newResult.model_name);

    // Add new result
    allResults.llm_results.push(newResult);

    // Sort by win_rate desc
    allResults.llm_results.sort((a, b) => parseFloat(b.win_rate) - parseFloat(a.win_rate));
    allResults.last_updated = new Date().toISOString();

    fs.writeFileSync(this.resultsFile, JSON.stringify(allResults, null, 2));
  }

  updateEmbeddingResults(newResult) {
    const allResults = JSON.parse(fs.readFileSync(this.resultsFile, "utf8"));

    // Remove old entry for same embedding if exists
    allResults.embedding_results = allResults.embedding_results.filter(r => r.embedding_name !== newResult.embedding_name);

    // Add new result
    allResults.embedding_results.push(newResult);

    // Sort by win_rate desc
    allResults.embedding_results.sort((a, b) => parseFloat(b.win_rate) - parseFloat(a.win_rate));
    allResults.last_updated = new Date().toISOString();

    fs.writeFileSync(this.resultsFile, JSON.stringify(allResults, null, 2));
  }

  getResults() {
    return JSON.parse(fs.readFileSync(this.resultsFile, "utf8"));
  }
}

module.exports = BacktestRunner;
