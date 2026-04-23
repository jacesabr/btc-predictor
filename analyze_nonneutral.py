"""One-off: recover pre-override DeepSeek signal from raw_response for last 40 predictions,
then compute win rate for the non-NEUTRAL subset."""
import os, re, sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
env_path = _ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

import psycopg2

url = os.environ["DATABASE_URL"]
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute(
    "SELECT window_start, confidence, signal, actual_direction, start_price, end_price, raw_response "
    "FROM deepseek_predictions "
    "WHERE actual_direction IS NOT NULL "
    "ORDER BY window_start DESC LIMIT 40"
)
rows = cur.fetchall()
cur.close(); conn.close()

def _dir_from(val: str) -> str:
    v = val.upper()
    if "ABOVE" in v or " UP" in f" {v}" or v.startswith("UP"):     return "UP"
    if "BELOW" in v or " DOWN" in f" {v}" or v.startswith("DOWN"): return "DOWN"
    if "NEUTRAL" in v: return "NEUTRAL"
    return "UNKNOWN"

def parse_raw_signal(raw: str) -> str:
    if not raw: return "UNKNOWN"
    for line in raw.splitlines():
        u = line.strip().upper()
        if u.startswith("POSITION:"):
            return _dir_from(u.split(":", 1)[1])
    return "UNKNOWN"

def parse_blind_baseline(raw: str) -> str:
    if not raw: return "UNKNOWN"
    for line in raw.splitlines():
        u = line.strip().upper()
        if u.startswith("BLIND_BASELINE:") or u.startswith("BLIND BASELINE:"):
            return _dir_from(u.split(":", 1)[1])
    return "UNKNOWN"

def score(label, extractor):
    up = down = neutral = unknown = wins = losses = 0
    detail = []
    for ws, conf, stored_sig, actual, sp, ep, raw in rows:
        s = extractor(raw)
        if   s == "UP":      up += 1
        elif s == "DOWN":    down += 1
        elif s == "NEUTRAL": neutral += 1
        else:                unknown += 1
        if s in ("UP", "DOWN"):
            pct = ((ep - sp) / sp * 100) if sp else 0.0
            correct = (s == actual)
            if correct: wins += 1
            else:       losses += 1
            detail.append((ws, conf, s, actual, pct, correct))
    directional = wins + losses
    print(f"\n=== {label} ===")
    print(f"  UP={up}  DOWN={down}  NEUTRAL={neutral}  UNKNOWN={unknown}")
    if directional:
        print(f"  Non-NEUTRAL: {directional} | wins {wins} | losses {losses} | win rate {wins/directional*100:.1f}%")
    return detail

print(f"Last {len(rows)} resolved predictions (stored-signal NEUTRALs: {sum(1 for r in rows if r[2]=='NEUTRAL')})")

score("FINAL POSITION (what DeepSeek committed to)", parse_raw_signal)
blind_detail = score("BLIND_BASELINE (DeepSeek's pre-specialist read)", parse_blind_baseline)

print("\nBLIND_BASELINE directional detail (most recent first):")
print(f"{'when':>14} {'conf':>4} {'blind':>6} {'actual':>6} {'move%':>8}  hit")
import datetime as dt
for ws, conf, s, actual, pct, correct in blind_detail:
    t = dt.datetime.utcfromtimestamp(ws).strftime("%m-%d %H:%M")
    print(f"{t:>14} {int(conf or 0):>3}% {s:>6} {actual:>6} {pct:>+7.3f}%  {'WIN' if correct else 'loss'}")
