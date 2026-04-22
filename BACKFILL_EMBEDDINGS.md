# Backfill Embeddings Guide

This script embeds all valid bars (UP/DOWN/NEUTRAL signals) that don't have embeddings yet.

## Quick Start

```bash
# Embed ALL missing bars (takes 5-30 minutes depending on volume)
python3 backfill_embeddings.py

# Embed only recent bars (faster feedback)
python3 backfill_embeddings.py --start-from-end --limit 100

# Embed first 50 oldest bars
python3 backfill_embeddings.py --limit 50
```

## Requirements

1. **Database running**: PostgreSQL with `DATABASE_URL` configured
2. **Cohere API key**: `COHERE_API_KEY` in `.env`
3. **Python environment**: Same venv as the main app

## What It Does

1. **Loads all bars** from `pattern_history` table
2. **Filters for valid signals**: UP, DOWN, or NEUTRAL (skips ERROR/UNAVAILABLE/None)
3. **Finds missing embeddings**: Only processes bars without an `embedding` vector
4. **For each bar**:
   - Converts to rich text essay via `_bar_embed_text(bar)`
   - Sends to Cohere embed-english-v3.0 → 1024-dim vector
   - Stores vector back to `pattern_history.embedding` column
5. **Logs progress**: Shows bars/sec, ETA, success/failure counts

## Expected Output

```
Loading all bars from database...
Loaded 2847 total bars
Found 1523 bars needing embeddings
  Signals: 612 UP, 511 DOWN, 400 NEUTRAL
Processing newest bars first

[10/1523] Bar 1713XXX (UP) embedded ✓  (10 done, 2.5 bars/s, ~610s remaining)
[20/1523] Bar 1712XXX (DOWN) embedded ✓  (20 done, 2.4 bars/s, ~630s remaining)

==============================================================================
Backfill complete!
  Success:  1523/1523
  Failed:   0/1523
  Elapsed:  612.5s (0.40s per bar)
  Coverage: 2847/2847 bars now have embeddings
==============================================================================
```

## Options

| Flag | Default | Purpose |
|------|---------|---------|
| `--limit N` | None (all) | Only embed first N bars |
| `--start-from-end` | False | Process newest bars first (faster feedback) |

## Tips

1. **Start with a small batch**: `--limit 100 --start-from-end` to test
2. **Monitor Cohere usage**: Check your Cohere dashboard for API calls
3. **Can run while app is live**: Safe to backfill while prediction engine is running (reads don't interfere with writes)
4. **Partial failure is OK**: If it fails at bar 500/1523, just run again — already-embedded bars are skipped

## Verification

After backfill, check coverage:

```bash
# In psql:
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN embedding IS NOT NULL THEN 1 ELSE 0 END) as with_embeddings,
  ROUND(100.0 * SUM(CASE WHEN embedding IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as coverage_pct
FROM pattern_history
WHERE data::json->>'deepseek_signal' IN ('UP', 'DOWN', 'NEUTRAL');
```

Expected: 100% coverage (or close to it).

## Troubleshooting

### "No COHERE_API_KEY in environment"
Add to `.env`:
```
COHERE_API_KEY=xxx-your-key-xxx
```

### "connection to server at localhost:5432 failed"
Start PostgreSQL:
```bash
# On macOS with brew:
brew services start postgresql

# On Ubuntu:
sudo systemctl start postgresql

# On Windows (WSL):
sudo service postgresql start
```

### "Too many failures (>10). Stopping."
Likely a Cohere API issue. Check:
- Cohere API status
- Rate limits
- Account balance
- Then re-run (will skip already-embedded bars)

### Script is slow
1. Try `--start-from-end --limit 100` to test speed
2. Check network (Cohere API latency)
3. Increase system resources if available

## What Gets Embedded

Each bar is converted to a ~2000-word essay including:

- OHLCV + price movement + outcome
- All indicator values (RSI, MACD, Bollinger, Stochastic, etc.)
- All 12+ strategy signals + votes
- Specialist signals (alligator, acc/dist, Fib, Harmonic, etc.)
- Dashboard data (order book, L/S ratio, funding, fear & greed, etc.)
- DeepSeek reasoning + narrative + free observation
- Historical analyst signal (if available)
- Binance expert analysis (if available)
- Postmortem self-analysis (after bar closes)

**Result**: 1024-dim L2-normalized vector capturing the complete market fingerprint.

## Impact

After backfill:
- ✅ Historical analyst will find 10x more similar bars in pgvector search
- ✅ Top-20 reranked bars will be much more accurate
- ✅ Pattern recognition improves significantly
- ✅ Embedding audit will show 100% coverage
