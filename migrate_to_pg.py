"""
One-shot migration: local pattern_history.ndjson → Railway PostgreSQL.
Only seeds bar history used for vector similarity search.

Usage:
    DATABASE_URL=<railway_url> python migrate_to_pg.py
"""

import json
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set. Paste the Railway PostgreSQL public URL into .env as DATABASE_URL=...")
    sys.exit(1)

HIST_FILE = Path(__file__).parent / "results" / "pattern_history.ndjson"

DDL = """
CREATE TABLE IF NOT EXISTS pattern_history (
    window_start DOUBLE PRECISION PRIMARY KEY,
    data         TEXT NOT NULL,
    created_at   DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pattern_history_ws ON pattern_history (window_start);
"""


def main():
    if not HIST_FILE.exists():
        print(f"ERROR: {HIST_FILE} not found.")
        sys.exit(1)

    records = []
    with open(HIST_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    print(f"Loaded {len(records)} bars from local history.")

    print("Connecting to Railway PostgreSQL...")
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected.")

    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("Schema ready.")

    now = time.time()
    rows = [(r["window_start"], json.dumps(r, default=str), now) for r in records]

    print(f"Inserting {len(rows)} bars...")
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO pattern_history (window_start, data, created_at) VALUES %s "
            "ON CONFLICT (window_start) DO NOTHING",
            rows,
            page_size=500,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pattern_history")
        total = cur.fetchone()[0]

    conn.close()
    print(f"Done. Railway pattern_history now has {total} bars.")


if __name__ == "__main__":
    main()
