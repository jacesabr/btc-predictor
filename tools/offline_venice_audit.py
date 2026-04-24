"""Offline Venice audit harness.

Run the production Venice summarizer (trader_summary.get_or_build) against a
corpus of archived DeepSeek-shaped bar fixtures, using the same SYSTEM_PROMPT
and the same _validate / _completeness_score pipeline that ships to the
trader. Emits a per-bar audit report + aggregate summary so we can hammer on
coverage, source citation, fabrication, and density without waiting for the
live 5-min bar cadence.

Fixture JSON shape — list of objects, each with:
    {
      "window_start": 1777042800.0,
      "pred": {                     # DeepSeek prediction (required)
        "signal": "UP" | "DOWN" | "NEUTRAL",
        "confidence": int,
        "reasoning": str,
        "narrative": str,
        "free_observation": str,
        "data_received": str,
        "data_requests": str
      },
      "historical": str,            # bar_historical_analysis (optional)
      "historical_context": str,    # bar_historical_context (optional)
      "binance_expert": {           # bar_binance_expert (optional)
        "signal": str, "edge": str, "watch": str, "confluence": str,
        "taker_flow": str, "whale_flow": str, "oi_funding": str,
        "positioning": str, "order_book": str, "analysis": str,
        "narrative": str, "reasoning": str
      },
      "specialist_signals": {...},  # optional
      "ensemble_result": {...}      # optional
    }

Usage:
    python tools/offline_venice_audit.py path/to/fixtures.json [--limit N]

Env:
    VENICE_API_KEY  required
    VENICE_MODEL    optional, defaults to qwen3-next-80b
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow "from trader_summary import ..." when run as tools/offline_venice_audit.py
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

import trader_summary as ts  # noqa: E402


VENICE_MODEL_DEFAULT = "qwen3-next-80b"
DEEPSEEK_MODEL_DEFAULT = "deepseek-chat"
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"


async def _call_deepseek_chat(api_key, model, system_prompt, user_prompt,
                              timeout_s: float = 45.0, extra_messages=None):
    """Drop-in replacement for trader_summary._call_venice that hits the
    DeepSeek chat-completions endpoint. Same request shape (OpenAI-compatible),
    same response shape — so the rest of the trader_summary pipeline is
    untouched and applies the identical audit."""
    import aiohttp
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ]
    if extra_messages:
        messages.extend(extra_messages)
    payload = {
        "model":           model,
        "messages":        messages,
        "max_tokens":      1600,
        "temperature":     0.2,
        "response_format": {"type": "json_object"},
    }
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(DEEPSEEK_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"DeepSeek HTTP {resp.status}: {body[:400]}")
            data = json.loads(body)
            return data["choices"][0]["message"]["content"]


@dataclass
class BarReport:
    window_start: float
    signal: str
    confidence: Any
    success: bool
    generation_ms: int
    n_watch: int
    n_actions: int
    total_bullets: int
    edge_chars: int
    edge_sentences: int
    coverage_ratio: float
    coverage_expected: List[str] = field(default_factory=list)
    coverage_covered: List[str] = field(default_factory=list)
    coverage_missed: List[str] = field(default_factory=list)
    retry_reasons: List[str] = field(default_factory=list)
    dropped_values: List[str] = field(default_factory=list)
    fabricated_text: List[dict] = field(default_factory=list)
    family_mismatch: List[dict] = field(default_factory=list)
    bullets_dropped: List[dict] = field(default_factory=list)
    bullets_rescued: List[dict] = field(default_factory=list)
    missing_quotes: List[dict] = field(default_factory=list)
    # Custom per-bar checks we run on top of the in-product audit
    degenerate_conditions: List[dict] = field(default_factory=list)
    unit_mismatches: List[dict] = field(default_factory=list)
    text_threshold_no_condition: List[dict] = field(default_factory=list)
    error: Optional[str] = None


# Metrics we know produce degenerate thresholds when emitted with value == 0
# (these quantities are always positive in practice, so "> 0" is trivially met).
_ALWAYS_POSITIVE_METRICS = {
    "taker_buy_volume", "taker_sell_volume", "taker_volume",
    "open_interest",
    "bid_depth_05pct", "ask_depth_05pct",
    "spot_whale_buy_btc", "spot_whale_sell_btc",
    "aggregate_liquidations_usd",
}

_EXPECTED_UNITS = {
    "price": "USD",
    "price_change_pct": "%",
    "taker_buy_volume": "BTC", "taker_sell_volume": "BTC", "taker_volume": "BTC",
    "bid_depth_05pct": "BTC", "ask_depth_05pct": "BTC",
    "spot_whale_buy_btc": "BTC", "spot_whale_sell_btc": "BTC",
    "open_interest": "BTC",
    "taker_ratio": "", "bsr": "",
    "long_short_ratio": "",
    "bid_imbalance": "%", "ask_imbalance": "%",
    "funding_rate": "%", "aggregate_funding_rate": "%",
    "oi_velocity_pct": "%",
    "rsi": "",
    "basis_pct": "%",
    "perp_cvd_1h": "BTC", "spot_cvd_1h": "BTC", "aggregate_cvd_1h": "BTC",
    "rr_25d_30d": "%", "iv_30d_atm": "%",
    "aggregate_liquidations_usd": "USD",
}


def _check_degenerate(summary: dict) -> List[dict]:
    """Flag conditions that are mechanically always-met (like open_interest > 0
    for BTC-valued metrics). These slide past the fabrication audit because
    0 is trivially 'cited', but they give the trader no useful signal."""
    out = []
    for section in ("watch", "actions"):
        for i, b in enumerate(summary.get(section) or []):
            for c in b.get("conditions") or []:
                m, op, v = c.get("metric"), c.get("op"), c.get("value")
                if m in _ALWAYS_POSITIVE_METRICS and op in (">", ">=") and v in (0, 0.0):
                    out.append({
                        "where": f"{section}[{i}]",
                        "metric": m, "op": op, "value": v,
                        "why": f"{m} is always >= 0, so '{op} {v}' is trivially met",
                    })
                if op == "==" and m in _ALWAYS_POSITIVE_METRICS and v == 0:
                    out.append({
                        "where": f"{section}[{i}]",
                        "metric": m, "op": op, "value": v,
                        "why": f"equality on live numeric metric will rarely fire",
                    })
    return out


def _check_units(summary: dict) -> List[dict]:
    """Flag cases where the emitted unit doesn't match the expected unit for
    the metric. Cosmetic (UI overrides via metric()'s formatter) but worth
    tracking — the prompt tells Venice to use specific units."""
    out = []
    for section in ("watch", "actions"):
        for i, b in enumerate(summary.get(section) or []):
            for c in b.get("conditions") or []:
                m = c.get("metric")
                emitted = (c.get("unit") or "").strip()
                expected = _EXPECTED_UNITS.get(m)
                if expected is None:
                    continue
                if emitted != expected and not (emitted == "" and expected == ""):
                    out.append({
                        "where": f"{section}[{i}]",
                        "metric": m, "emitted_unit": emitted or "(none)", "expected_unit": expected or "(none)",
                    })
    return out


# Simple regex set: if bullet text mentions a numeric threshold + a metric-ish
# keyword, we expect conditions[] to contain a matching metric. This is a
# layer ABOVE the in-product family contract (which only checks the family,
# not whether a threshold exists).
import re
_KEYWORD_METRIC_MAP = [
    (re.compile(r"\btaker (buy|sell)[- ]?volume\b", re.I), ("taker_buy_volume", "taker_sell_volume", "taker_volume")),
    (re.compile(r"\bBSR\b|buy[-/ ]?sell ratio|taker ratio", re.I), ("taker_ratio", "bsr")),
    (re.compile(r"\b(bid|ask) imbalance\b", re.I), ("bid_imbalance", "ask_imbalance")),
    (re.compile(r"\bfunding\b", re.I), ("funding_rate", "aggregate_funding_rate")),
    (re.compile(r"\bOI\b|open interest", re.I), ("open_interest", "oi_velocity_pct")),
    (re.compile(r"\bRSI\b", re.I), ("rsi",)),
    (re.compile(r"\blong[/ ]short\b|\bL/S\b", re.I), ("long_short_ratio",)),
    (re.compile(r"\bliquidation", re.I), ("aggregate_liquidations_usd",)),
    (re.compile(r"\bwhale\b", re.I), ("spot_whale_buy_btc", "spot_whale_sell_btc")),
    (re.compile(r"\bprice\b|\$\d", re.I), ("price", "price_change_pct")),
]
_THRESHOLD_RE = re.compile(r"(above|below|under|over|>=|<=|>|<|at least|at most)\s*\$?-?\d", re.I)


def _check_text_threshold_coverage(summary: dict) -> List[dict]:
    """For each bullet: if text has a numeric threshold + keyword, ensure
    conditions[] contains a metric from the matching family. If not, flag."""
    out = []
    for section in ("watch", "actions"):
        for i, b in enumerate(summary.get(section) or []):
            text = (b.get("text") or "") + " " + (b.get("if_met") or "")
            if not _THRESHOLD_RE.search(text):
                continue
            cond_metrics = {c.get("metric") for c in (b.get("conditions") or [])}
            for rx, families in _KEYWORD_METRIC_MAP:
                if rx.search(text):
                    if not cond_metrics & set(families):
                        out.append({
                            "where": f"{section}[{i}]",
                            "text": text.strip()[:180],
                            "expected_family": list(families),
                            "actual_conds": list(cond_metrics),
                        })
    return out


async def audit_one(fx: dict, api_key: str, model: str) -> BarReport:
    window_start = float(fx.get("window_start") or time.time())
    pred = fx.get("pred") or {}
    signal = (pred.get("signal") or "?").upper()
    confidence = pred.get("confidence")

    # Clear any cache so we actually hit Venice
    ts.drop(window_start)

    started = time.time()
    summary = None
    error = None
    try:
        summary = await ts.get_or_build(
            window_start_time=window_start,
            pred=pred,
            historical=fx.get("historical") or "",
            binance_expert=fx.get("binance_expert") or {},
            api_key=api_key,
            model=model,
            historical_context=fx.get("historical_context") or "",
            specialist_signals=fx.get("specialist_signals") or {},
            ensemble_result=fx.get("ensemble_result") or {},
        )
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    dur_ms = int((time.time() - started) * 1000)

    if not summary:
        return BarReport(
            window_start=window_start, signal=signal, confidence=confidence,
            success=False, generation_ms=dur_ms,
            n_watch=0, n_actions=0, total_bullets=0,
            edge_chars=0, edge_sentences=0,
            coverage_ratio=0.0, error=error or "Venice returned None",
        )

    audit = summary.get("audit") or {}
    completeness = audit.get("completeness") or {}
    edge = summary.get("edge") or ""
    n_sentences = max(1, len(re.findall(r"[.!?]+", edge)))

    degen = _check_degenerate(summary)
    units = _check_units(summary)
    tx_cov = _check_text_threshold_coverage(summary)

    return BarReport(
        window_start=window_start, signal=signal, confidence=confidence,
        success=True, generation_ms=summary.get("generation_ms") or dur_ms,
        n_watch=len(summary.get("watch") or []),
        n_actions=len(summary.get("actions") or []),
        total_bullets=len(summary.get("watch") or []) + len(summary.get("actions") or []),
        edge_chars=len(edge), edge_sentences=n_sentences,
        coverage_ratio=completeness.get("ratio") or 0.0,
        coverage_expected=completeness.get("expected") or [],
        coverage_covered=completeness.get("covered") or [],
        coverage_missed=completeness.get("missed") or [],
        retry_reasons=audit.get("retry_reasons") or [],
        dropped_values=audit.get("dropped_values") or [],
        fabricated_text=audit.get("fabricated_text_numbers") or [],
        family_mismatch=audit.get("family_mismatch_conditions_cleared") or [],
        bullets_dropped=audit.get("bullets_dropped") or [],
        bullets_rescued=audit.get("bullets_rescued") or [],
        missing_quotes=audit.get("missing_quotes_bullets") or [],
        degenerate_conditions=degen,
        unit_mismatches=units,
        text_threshold_no_condition=tx_cov,
    )


def _print_bar_report(r: BarReport, idx: int) -> None:
    head = f"[bar {idx:3d}] {r.window_start:.0f} · {r.signal} {r.confidence}%"
    if not r.success:
        print(f"{head}  FAILED  ({r.error})")
        return
    density_tag = "OK" if r.total_bullets >= 3 else "SPARSE"
    cov_tag = "OK" if r.coverage_ratio >= 0.75 else "LOW"
    print(
        f"{head}  w={r.n_watch} a={r.n_actions} tot={r.total_bullets} [{density_tag}] "
        f"cov={r.coverage_ratio:.2f} [{cov_tag}] edge={r.edge_chars}c/{r.edge_sentences}s  "
        f"{r.generation_ms}ms"
    )
    if r.coverage_missed:
        print(f"   missed: {r.coverage_missed}")
    if r.retry_reasons:
        print(f"   retry_reasons: {r.retry_reasons}")
    if r.dropped_values:
        print(f"   dropped_values: {r.dropped_values}")
    for fab in r.fabricated_text:
        print(f"   fabricated_text in {fab.get('where')}: {fab.get('nums')}")
    for fm in r.family_mismatch:
        print(f"   family_mismatch: text='{fm.get('text')[:80]}...' used={fm.get('text_metrics_used')} expected={fm.get('expected_family')}")
    for dr in r.bullets_dropped:
        print(f"   bullet dropped: '{dr.get('text')[:80]}...' why={dr.get('why')}")
    for dg in r.degenerate_conditions:
        print(f"   degenerate_cond {dg['where']}: {dg['metric']} {dg['op']} {dg['value']} — {dg['why']}")
    for u in r.unit_mismatches:
        print(f"   unit_mismatch {u['where']}: {u['metric']} emitted '{u['emitted_unit']}' expected '{u['expected_unit']}'")
    for tx in r.text_threshold_no_condition:
        print(f"   text_threshold_no_cond {tx['where']}: expected {tx['expected_family']} got {tx['actual_conds']} in '{tx['text'][:80]}...'")


def _print_aggregate(reports: List[BarReport]) -> None:
    ok = [r for r in reports if r.success]
    n = len(reports)
    if not ok:
        print("\n=== AGGREGATE === no successful bars")
        return
    print("\n=== AGGREGATE ===")
    print(f"bars tested: {n}  success: {len(ok)}  failed: {n-len(ok)}")
    import statistics
    bullets = [r.total_bullets for r in ok]
    cov     = [r.coverage_ratio for r in ok]
    gen     = [r.generation_ms  for r in ok]
    print(f"bullets: mean={statistics.mean(bullets):.2f} min={min(bullets)} max={max(bullets)}  <3 count: {sum(1 for b in bullets if b<3)}")
    print(f"coverage: mean={statistics.mean(cov):.3f} min={min(cov):.3f} max={max(cov):.3f}  <0.75 count: {sum(1 for c in cov if c<0.75)}")
    print(f"gen_ms: mean={statistics.mean(gen):.0f} min={min(gen)} max={max(gen)}")
    n_degen     = sum(len(r.degenerate_conditions) for r in ok)
    n_units     = sum(len(r.unit_mismatches) for r in ok)
    n_txmiss    = sum(len(r.text_threshold_no_condition) for r in ok)
    n_fabtext   = sum(len(r.fabricated_text) for r in ok)
    n_dropval   = sum(len(r.dropped_values) for r in ok)
    n_family    = sum(len(r.family_mismatch) for r in ok)
    n_dropbull  = sum(len(r.bullets_dropped) for r in ok)
    n_rescued   = sum(len(r.bullets_rescued) for r in ok)
    n_missquote = sum(len(r.missing_quotes) for r in ok)
    print(f"degenerate conds:        {n_degen}")
    print(f"unit mismatches:         {n_units}")
    print(f"text-threshold no cond:  {n_txmiss}")
    print(f"fabricated text numbers: {n_fabtext}")
    print(f"dropped condition vals:  {n_dropval}")
    print(f"family mismatches:       {n_family}")
    print(f"bullets dropped by audit:{n_dropbull}")
    print(f"bullets rescued:         {n_rescued}")
    print(f"missing source_quotes:   {n_missquote}")

    # Topic-level coverage distribution
    from collections import Counter
    cov_miss_counter = Counter()
    cov_exp_counter  = Counter()
    for r in ok:
        for t in r.coverage_expected: cov_exp_counter[t] += 1
        for t in r.coverage_missed:   cov_miss_counter[t] += 1
    print("\nper-topic miss rate (missed / expected):")
    for topic, exp in sorted(cov_exp_counter.items(), key=lambda x: -x[1]):
        miss = cov_miss_counter.get(topic, 0)
        rate = miss / exp if exp else 0.0
        print(f"  {topic:15s}  {miss:3d} / {exp:3d}  ({rate*100:.0f}%)")


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("fixtures", help="Path to fixtures JSON (list of bar dicts)")
    ap.add_argument("--limit", type=int, default=0, help="Test only the first N bars")
    ap.add_argument("--concurrency", type=int, default=3,
                    help="Concurrent API calls (keep low to respect rate limits)")
    ap.add_argument("--backend", choices=["venice", "deepseek"], default="venice",
                    help="Which LLM backend to use for the summarizer call")
    ap.add_argument("--model", default="", help="Override the default model for the selected backend")
    ap.add_argument("--out", default="", help="Optional: write per-bar reports as JSON to this path")
    args = ap.parse_args()

    if args.backend == "venice":
        api_key = os.environ.get("VENICE_API_KEY", "").strip()
        if not api_key:
            print("ERROR: VENICE_API_KEY not set", file=sys.stderr)
            return 2
        model = args.model or os.environ.get("VENICE_MODEL", VENICE_MODEL_DEFAULT)
    else:  # deepseek
        api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
        if not api_key:
            print("ERROR: DEEPSEEK_API_KEY not set", file=sys.stderr)
            return 2
        model = args.model or os.environ.get("DEEPSEEK_MODEL", DEEPSEEK_MODEL_DEFAULT)
        # Swap trader_summary's LLM caller so get_or_build hits DeepSeek instead
        # of Venice. All other pipeline logic (validate, completeness, retry) is
        # shared, so this is a clean A/B on the schema-compliance behavior.
        ts._call_venice = _call_deepseek_chat  # type: ignore[attr-defined]
    print(f"backend={args.backend}  model={model}", file=sys.stderr)

    path = Path(args.fixtures)
    if not path.exists():
        print(f"ERROR: fixtures not found: {path}", file=sys.stderr)
        return 2
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        print("ERROR: fixtures must be a JSON list", file=sys.stderr)
        return 2
    if args.limit > 0:
        data = data[: args.limit]

    sem = asyncio.Semaphore(max(1, args.concurrency))
    reports: List[Optional[BarReport]] = [None] * len(data)

    async def run_one(i: int, fx: dict) -> None:
        async with sem:
            r = await audit_one(fx, api_key, model)
            reports[i] = r
            _print_bar_report(r, i + 1)

    await asyncio.gather(*(run_one(i, fx) for i, fx in enumerate(data)))

    _print_aggregate([r for r in reports if r is not None])

    if args.out:
        out_path = Path(args.out)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump([asdict(r) for r in reports if r is not None], f, indent=2, default=str)
        print(f"\nWrote per-bar report JSON → {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
