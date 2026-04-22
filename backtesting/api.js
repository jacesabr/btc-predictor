/**
 * Backtesting API endpoints
 * Admin calls these to trigger tests
 */

const express = require("express");
const BacktestRunner = require("./test_runner");

const router = express.Router();
const runner = new BacktestRunner();

// Keep track of active jobs
const activeJobs = new Map();

/**
 * POST /backtesting/test-llm
 * Start testing an LLM model
 *
 * Body:
 * {
 *   "model": "claude",  // from llm_models.js keys
 *   "api_key": "sk-ant-...",
 *   "samples": 200,
 *   "temperature": 0.1,
 *   "max_tokens": 500
 * }
 */
router.post("/test-llm", async (req, res) => {
  try {
    const { model, api_key, samples = 200, temperature, max_tokens = 500 } = req.body;

    if (!model || !api_key) {
      return res.status(400).json({ error: "model and api_key required" });
    }

    const jobId = `llm_${model}_${Date.now()}`;

    // Start test async
    runner
      .testLLMModel(model, api_key, {
        samples,
        temperature,
        maxTokens: max_tokens,
        onProgress: (progress) => {
          activeJobs.set(jobId, { status: "in_progress", ...progress });
        }
      })
      .then((result) => {
        activeJobs.set(jobId, { status: "completed", result });
      })
      .catch((err) => {
        activeJobs.set(jobId, { status: "failed", error: err.message });
      });

    activeJobs.set(jobId, { status: "queued" });

    res.json({
      job_id: jobId,
      status: "queued",
      model,
      message: `Testing ${model} against ${samples} prompts...`
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /backtesting/test-embedding
 * Start testing an embedding model
 *
 * Body:
 * {
 *   "embedding": "openai_large",  // from embedding_models.js keys
 *   "api_key": "sk-...",
 *   "samples": 250
 * }
 */
router.post("/test-embedding", async (req, res) => {
  try {
    const { embedding, api_key, samples = 250 } = req.body;

    if (!embedding || !api_key) {
      return res.status(400).json({ error: "embedding and api_key required" });
    }

    const jobId = `emb_${embedding}_${Date.now()}`;

    // Start test async
    runner
      .testEmbeddingModel(embedding, api_key, {
        samples,
        onProgress: (progress) => {
          activeJobs.set(jobId, { status: "in_progress", ...progress });
        }
      })
      .then((result) => {
        activeJobs.set(jobId, { status: "completed", result });
      })
      .catch((err) => {
        activeJobs.set(jobId, { status: "failed", error: err.message });
      });

    activeJobs.set(jobId, { status: "queued" });

    res.json({
      job_id: jobId,
      status: "queued",
      embedding,
      message: `Testing ${embedding} against ${samples} windows...`
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /backtesting/status/:job_id
 * Check progress of a running test
 */
router.get("/status/:job_id", (req, res) => {
  const { job_id } = req.params;
  const job = activeJobs.get(job_id);

  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  res.json({
    job_id,
    ...job
  });
});

/**
 * GET /backtesting/results
 * Get the full leaderboard (both LLM and embedding results)
 */
router.get("/results", (req, res) => {
  try {
    const results = runner.getResults();
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /backtesting/results/llm
 * Get LLM leaderboard only
 */
router.get("/results/llm", (req, res) => {
  try {
    const results = runner.getResults();
    res.json({
      llm_results: results.llm_results,
      last_updated: results.last_updated
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /backtesting/results/embedding
 * Get embedding leaderboard only
 */
router.get("/results/embedding", (req, res) => {
  try {
    const results = runner.getResults();
    res.json({
      embedding_results: results.embedding_results,
      last_updated: results.last_updated
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /backtesting/jobs
 * List all active/recent jobs
 */
router.get("/jobs", (req, res) => {
  const jobs = Array.from(activeJobs.entries()).map(([id, data]) => ({
    job_id: id,
    ...data
  }));

  res.json({
    total: jobs.length,
    jobs: jobs.sort((a, b) => b.job_id.localeCompare(a.job_id)).slice(0, 20)
  });
});

module.exports = router;
