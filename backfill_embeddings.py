#!/usr/bin/env python3
"""
Backfill Embeddings Script
===========================
Embeds all valid bars (UP/DOWN/NEUTRAL signals) that don't have embeddings yet.
Skips ERROR/UNAVAILABLE/None signals (by design).

Usage:
  python3 backfill_embeddings.py [--limit N] [--start-from-end]

  --limit N              Only embed first N bars (default: all)
  --start-from-end      Start from most recent bars instead of oldest (faster for recent data)
"""

import asyncio
import json
import logging
import pathlib
import sys
import time
from typing import List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv(pathlib.Path(__file__).parent / ".env")
except ImportError:
    pass

from config import Config
from ai import embed_text, _bar_embed_text
from semantic_store import load_all, store_embedding

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

config = Config()


async def backfill_embeddings(limit: Optional[int] = None, start_from_end: bool = False):
    """
    Load all bars, filter for valid signals without embeddings, and embed them.

    Args:
        limit: Max bars to embed (None = all)
        start_from_end: If True, process most recent bars first
    """
    if not config.cohere_api_key:
        logger.error("No COHERE_API_KEY in environment. Aborting.")
        return

    logger.info("Loading all bars from database...")
    all_bars = load_all(limit=99999)
    logger.info(f"Loaded {len(all_bars)} total bars")

    # Filter: valid signals, no embeddings yet
    candidates = [
        b for b in all_bars
        if b.get("deepseek_signal") in ("UP", "DOWN", "NEUTRAL")
        and not b.get("embedding")
        and not b.get("_has_embedding")
    ]

    if not candidates:
        logger.info("No bars to embed — all valid bars already have embeddings!")
        return

    logger.info(f"Found {len(candidates)} bars needing embeddings")
    logger.info(f"  Signals: {sum(1 for b in candidates if b.get('deepseek_signal') == 'UP')} UP, "
                f"{sum(1 for b in candidates if b.get('deepseek_signal') == 'DOWN')} DOWN, "
                f"{sum(1 for b in candidates if b.get('deepseek_signal') == 'NEUTRAL')} NEUTRAL")

    # Order: newest first if requested (faster feedback)
    if start_from_end:
        candidates = list(reversed(candidates))
        logger.info("Processing newest bars first")

    # Apply limit
    if limit:
        candidates = candidates[:limit]
        logger.info(f"Limited to {limit} bars")

    # Embed
    success = 0
    failed = 0
    skipped = 0
    t0_total = time.time()

    for idx, bar in enumerate(candidates, 1):
        ws = bar.get("window_start", 0)
        sig = bar.get("deepseek_signal", "?")

        try:
            # Generate text essay
            text = _bar_embed_text(bar)

            # Embed via Cohere
            vec = await embed_text(config.cohere_api_key, text, input_type="search_document")

            # Store in database
            store_embedding(ws, vec)

            success += 1
            elapsed = time.time() - t0_total
            rate = idx / elapsed if elapsed > 0 else 0
            remaining = (len(candidates) - idx) / rate if rate > 0 else 0

            if idx % 10 == 0 or idx == 1:
                logger.info(
                    f"[{idx}/{len(candidates)}] Bar {ws} ({sig}) embedded ✓  "
                    f"({success} done, {rate:.1f} bars/s, ~{remaining:.0f}s remaining)"
                )

        except Exception as exc:
            failed += 1
            logger.warning(f"[{idx}/{len(candidates)}] Bar {ws} ({sig}) failed: {exc}")
            if failed > 10:
                logger.error("Too many failures (>10). Stopping.")
                break

    elapsed_total = time.time() - t0_total
    logger.info(
        f"\n{'='*70}\nBackfill complete!\n"
        f"  Success:  {success}/{len(candidates)}\n"
        f"  Failed:   {failed}/{len(candidates)}\n"
        f"  Elapsed:  {elapsed_total:.1f}s ({elapsed_total/len(candidates):.2f}s per bar)\n"
        f"  Coverage: {success}/{len(all_bars)} bars now have embeddings\n"
        f"{'='*70}"
    )


def main():
    import argparse
    import os
    parser = argparse.ArgumentParser(description="Backfill missing embeddings")
    parser.add_argument("--limit", type=int, default=None, help="Max bars to embed")
    parser.add_argument("--start-from-end", action="store_true", help="Process newest bars first")
    args = parser.parse_args()

    logger.info("Backfill embeddings starting...")
    logger.info(f"Cohere API key present: {bool(config.cohere_api_key)}")
    logger.info(f"DATABASE_URL present: {bool(os.environ.get('DATABASE_URL'))}")

    asyncio.run(backfill_embeddings(limit=args.limit, start_from_end=args.start_from_end))


if __name__ == "__main__":
    main()
