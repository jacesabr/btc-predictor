"""Fetch N deepseek predictions via the admin endpoint and write them as a
fixture file for the offline_venice_audit harness.

Auth: reuses a cookie jar file (Netscape format) that's already been logged
in to the admin endpoint. Do NOT check in the cookie file.

Usage:
    python tools/fetch_fixtures.py \
        --base-url https://btc-predictor-1z8d.onrender.com \
        --cookie /c/tmp/render_cookie.txt \
        --n 200 \
        --out tools/fixtures/bars.json
"""
from __future__ import annotations

import argparse
import http.cookiejar as cj
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


# --- Regex extractors for the full_prompt (best-effort, skipped on mismatch)

_BINANCE_EXPERT_RE = re.compile(
    r"──+\s*\n\s*BINANCE MICROSTRUCTURE EXPERT.*?\n──+\s*\n(.*?)\n──+",
    re.S,
)

_HISTORICAL_ANALYST_RE = re.compile(
    r"──+\s*\n\s*HISTORICAL SIMILARITY ANALYST\s*\n──+\s*\n(.*?)(?:\n──+|$)",
    re.S,
)


def _parse_binance_expert(full_prompt: str) -> Dict[str, str]:
    """Extract the BINANCE MICROSTRUCTURE EXPERT block and split into its
    labelled sub-fields (Taker flow, Positioning, Whale flow, OI/Funding,
    Order book, Confluence, Key edge, Watch for)."""
    out: Dict[str, str] = {}
    m = _BINANCE_EXPERT_RE.search(full_prompt)
    if not m:
        return out
    block = m.group(1)
    # Pull "Signal : ..." line
    sig_m = re.search(r"Signal\s*:\s*([A-Z]+)(?:\s*\(([0-9]+)%\s*confidence\))?", block)
    if sig_m:
        out["signal"] = sig_m.group(1)
        if sig_m.group(2):
            out["confidence"] = sig_m.group(2)
    for label, key in [
        ("Taker flow",  "taker_flow"),
        ("Positioning", "positioning"),
        ("Whale flow",  "whale_flow"),
        ("OI/Funding",  "oi_funding"),
        ("Order book",  "order_book"),
        ("Confluence",  "confluence"),
        ("Key edge",    "edge"),
        ("Watch for",   "watch"),
    ]:
        rx = re.compile(rf"{re.escape(label)}\s*:\s*\[(.+?)\](?=\s*\n\s*[A-Za-z/ ]+:|\s*$)", re.S)
        mm = rx.search(block)
        if mm:
            out[key] = mm.group(1).strip()
    return out


def _parse_historical(full_prompt: str) -> str:
    m = _HISTORICAL_ANALYST_RE.search(full_prompt)
    if not m:
        return ""
    return m.group(1).strip()


def _build_fixture(record: Dict[str, Any], detail: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ws = record.get("window_start")
    pred = {
        "signal":           record.get("signal"),
        "confidence":       record.get("confidence"),
        "reasoning":        record.get("reasoning") or "",
        "narrative":        record.get("narrative") or "",
        "free_observation": record.get("free_observation") or "",
        "data_received":    record.get("data_received") or "",
        "data_requests":    record.get("data_requests") or "",
    }
    fix: Dict[str, Any] = {
        "window_start": ws,
        "pred":         pred,
    }
    full_prompt = ""
    if detail and isinstance(detail.get("prompting"), dict):
        full_prompt = detail["prompting"].get("full_prompt") or ""
    if full_prompt:
        be = _parse_binance_expert(full_prompt)
        if be:
            fix["binance_expert"] = be
        hist = _parse_historical(full_prompt)
        if hist:
            fix["historical"] = hist
    return fix


def _load_cookies(path: str) -> cj.CookieJar:
    jar = cj.MozillaCookieJar(path)
    jar.load(ignore_discard=True, ignore_expires=True)
    return jar


def _http_get(url: str, jar: cj.CookieJar) -> bytes:
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    req = urllib.request.Request(url)
    with opener.open(req, timeout=60) as resp:
        return resp.read()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", required=True,
                    help="Service base URL, e.g. https://btc-predictor-1z8d.onrender.com")
    ap.add_argument("--cookie", required=True,
                    help="Path to Netscape-format cookie file (from `curl -c`)")
    ap.add_argument("--n", type=int, default=200,
                    help="How many recent bars to fetch")
    ap.add_argument("--with-detail", action="store_true",
                    help="Also fetch /historical-analysis/{window_start} to pull full_prompt "
                         "and parse binance_expert + historical_similarity out of it")
    ap.add_argument("--out", required=True,
                    help="Output fixture JSON path")
    args = ap.parse_args()

    jar = _load_cookies(args.cookie)
    list_url = args.base_url.rstrip("/") + f"/deepseek/predictions?n={int(args.n)}"
    print(f"fetching {list_url}", file=sys.stderr)
    body = _http_get(list_url, jar)
    try:
        records = json.loads(body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: list parse failed ({exc}); first 300 bytes: {body[:300]!r}", file=sys.stderr)
        return 2
    if not isinstance(records, list):
        print(f"ERROR: expected a list, got {type(records).__name__}", file=sys.stderr)
        return 2
    print(f"got {len(records)} records", file=sys.stderr)

    # Keep only bars with a meaningful signal + reasoning
    records = [
        r for r in records
        if (r.get("signal") or "").upper() not in ("ERROR", "UNAVAILABLE", "")
           and r.get("reasoning")
    ]
    print(f"after filter: {len(records)} records with signal+reasoning", file=sys.stderr)

    fixtures: List[Dict[str, Any]] = []
    for i, rec in enumerate(records):
        detail = None
        if args.with_detail:
            ws = rec.get("window_start")
            if ws is None:
                continue
            det_url = args.base_url.rstrip("/") + f"/historical-analysis/{ws}"
            try:
                det_body = _http_get(det_url, jar)
                detail = json.loads(det_body.decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                print(f"  [{i}] detail fetch failed for ws={ws}: {exc}", file=sys.stderr)
                detail = None
        fixtures.append(_build_fixture(rec, detail))
        if (i + 1) % 25 == 0:
            print(f"  built {i+1}/{len(records)}", file=sys.stderr)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(fixtures, f, indent=2, ensure_ascii=False)
    print(f"wrote {len(fixtures)} fixtures → {out_path}", file=sys.stderr)

    # Quick shape summary
    with_be   = sum(1 for fx in fixtures if fx.get("binance_expert"))
    with_hist = sum(1 for fx in fixtures if fx.get("historical"))
    print(f"with binance_expert: {with_be}  with historical: {with_hist}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
