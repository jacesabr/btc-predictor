"""
One-time migration: copy local NDJSON files → MongoDB Atlas.
Run once before deploying:  python migrate_to_mongo.py
"""
import json, os, sys
from pathlib import Path

# ── Load config ──────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from config import Config
config = Config()

if not config.mongodb_uri:
    print("ERROR: MONGODB_URI env var not set. Set it and try again.")
    sys.exit(1)

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

client = MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000)
client.admin.command("ping")
db = client[config.mongodb_db]
print(f"Connected to MongoDB: {config.mongodb_db}")

DATA_DIR = Path(__file__).parent / "results"

def load_ndjson(path):
    if not path.exists():
        print(f"  skipping {path.name} (not found)")
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass
    return records

# ── Migrate predictions ───────────────────────────────────────────────────────
preds = load_ndjson(DATA_DIR / "predictions.ndjson")
if preds:
    ops = [UpdateOne({"window_start": r["window_start"]}, {"$setOnInsert": r}, upsert=True) for r in preds]
    try:
        res = db.predictions.bulk_write(ops, ordered=False)
        print(f"Predictions: {res.upserted_count} inserted, {res.matched_count} already existed")
    except BulkWriteError as e:
        print(f"Predictions: partial write — {e.details['nInserted']} inserted")

# ── Migrate DeepSeek predictions ──────────────────────────────────────────────
ds_preds = load_ndjson(DATA_DIR / "deepseek_predictions.ndjson")
if ds_preds:
    ops = [UpdateOne({"window_start": r["window_start"]}, {"$setOnInsert": r}, upsert=True) for r in ds_preds]
    try:
        res = db.deepseek_predictions.bulk_write(ops, ordered=False)
        print(f"DeepSeek predictions: {res.upserted_count} inserted, {res.matched_count} already existed")
    except BulkWriteError as e:
        print(f"DeepSeek predictions: partial write — {e.details['nInserted']} inserted")

# ── Migrate pattern history ───────────────────────────────────────────────────
patterns = load_ndjson(DATA_DIR / "pattern_history.ndjson")
if patterns:
    ops = [UpdateOne({"window_start": r["window_start"]}, {"$setOnInsert": r}, upsert=True) for r in patterns]
    try:
        res = db.pattern_history.bulk_write(ops, ordered=False)
        print(f"Pattern history: {res.upserted_count} inserted, {res.matched_count} already existed")
    except BulkWriteError as e:
        print(f"Pattern history: partial write — {e.details['nInserted']} inserted")

print("\nMigration complete.")
client.close()
