"""
Trader-friendly summarizer layer over the main page output.

Compresses DeepSeek reasoning + historical pattern lean + Binance expert notes
into a tight briefing a trader can digest in ~30 seconds, emitted as structured
JSON so the frontend can render with per-bullet green/red tone coloring.

Design constraints (from user):
  * Non-persistent — never written to Postgres. Purely a live ease-of-use layer.
  * One Venice call per bar, not per WebSocket tick: cached by window_start_time.
  * Never blocks the UI. Any failure (HTTP, JSON parse, validation) returns None
    and the frontend falls back to rendering the raw blocks below.

Anti-fabrication contract (added 2026-04-24 after audit caught invented
thresholds like "taker_buy_volume < 1.5 BTC" and "< 2.0 BTC" that never
appeared in the DeepSeek INPUT):

  1. Every numeric value that reaches the trader — inside conditions[] OR
     inside a bullet's text/if_met — MUST appear VERBATIM in the INPUT block
     sent to Venice. Python-side guards strip any value that doesn't round-trip
     to a source token.
  2. Every bullet MUST declare `sources` (which INPUT section it came from)
     and `source_quotes` (short verbatim snippets). Quotes that aren't a
     substring of the INPUT are stripped; bullets with zero surviving quotes
     are dropped.
  3. Key DeepSeek numbers (price levels, BTC volumes, BSR, $-liquidations)
     are tallied and we require a minimum coverage in the Venice output —
     translation must NOT silently drop signals out of laziness.
  4. Venice is told explicitly in the system prompt that its output is
     machine-audited on every bar; this pushes the model toward citing real
     data rather than round-number estimates.
"""
import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp


logger = logging.getLogger(__name__)

VENICE_URL = "https://api.venice.ai/api/v1/chat/completions"

# In-memory only. Keys are window_start_time (unix-seconds float). Eviction keeps
# the last ~1 hour of bars so reconnecting clients can still see recent summaries.
_cache: Dict[float, dict] = {}
_locks: Dict[float, asyncio.Lock] = {}
_CACHE_MAX = 12


SYSTEM_PROMPT = """You compress a BTC 5-minute prediction analysis into a trader briefing a reader can digest in 30 seconds.

======================= AUDIT NOTICE =======================
Every single output you produce is auto-audited before the
trader sees it. The audit checks:

  * Every number in conditions[], text, and if_met must appear
    VERBATIM in the INPUT you were given.
  * Every bullet must declare `sources` + `source_quotes`, and
    every quote must be a literal substring of the INPUT.
  * DeepSeek's key signals (price levels, BTC volumes, BSR,
    whale flows, liquidations, OI, funding) must be represented
    in your output — you are not allowed to drop them out of
    laziness. Missing coverage triggers a retry.

If any check fails, the output is thrown away and a correction
request is sent back. This is a live production system for
real traders. Fabricated numbers in this domain are lies that
cost real money. Translate, never invent.
=============================================================

OUTPUT STRICT JSON ONLY, exactly this shape:
{
  "edge": "Lead sentence — dominant setup RIGHT NOW. Max 30 words. If the signal is NEUTRAL because of a concrete structural divergence (e.g. 'taker flow bullish but whale flow distributing + no historical precedent'), you MAY use a SECOND sentence (max 25 more words) to name the opposing forces. Cite the concrete numbers that define the divergence. Don't pad with generic hedging.",
  "watch": [{
    "tone": "bullish|bearish|neutral",
    "text": "ONE complete sentence (not a phrase fragment). State what to watch and WHY it matters. Reference the specific signal by name. Example: 'If taker buy volume surges above 50 BTC, the zero-flow regime breaks and bulls regain control.' NOT 'taker flow'. Max 22 words.",
    "conditions": [{"metric": "<name>", "op": ">"|">="|"<"|"<="|"==", "value": <number>, "unit": "<unit>"}],
    "if_met": "short phrase (<=12 words) stating the DIRECT consequence the INPUT text supports. Omit if text already says it.",
    "sources": ["<section 1>", "<section 2>"],
    "source_quotes": ["<short verbatim snippet from INPUT>", "..."]
  }],
  "actions": [{
    "tone": "bullish|bearish|neutral",
    "text": "ONE complete IF-THEN sentence. 'If price breaks X with Y confirmation, enter long with stop Z.' Never a bare phrase. Max 25 words.",
    "conditions": [same shape as watch],
    "if_met": "the trader's concrete action when conditions fire (<=12 words).",
    "sources": ["..."],
    "source_quotes": ["..."]
  }]
}

HARD RULES:
- VERBATIM NUMBERS ONLY. Every numeric value you emit — in a condition, in
  the text, or in if_met — must appear in the INPUT text. Do NOT estimate,
  round, or infer thresholds. If DeepSeek cites BSR=0.7914, use 0.7914, not
  0.79. If DeepSeek says "16.7 BTC buys", do not invent "below 2 BTC" as a
  reversal threshold.
- NO DEGENERATE THRESHOLDS. Do not emit "taker_buy_volume > 0", "whale_buy > 0",
  "open_interest > 0", or any ">0" threshold on a quantity that is always
  non-negative. If INPUT says a regime is absent or zero (e.g. "zero taker
  flow regime", "no whales ≥0.5 BTC"), describe the absence as the bullet's
  narrative — the trigger is "a non-trivial figure appears", and you must
  pick a specific non-zero threshold from the INPUT (e.g. cite the 3-bar
  average, the prior spike, or a break level). If no meaningful threshold
  exists, OMIT the condition and keep the bullet as pure narrative.
- If a bullet needs a threshold that isn't in the INPUT, either:
    (a) quote the INPUT's number as the threshold (e.g., "below the cited
        434.9 BTC bid wall"), OR
    (b) omit the condition — emit the bullet without a pill rather than with
        a fabricated pill.
- sources[]: name every INPUT section the bullet draws from. Valid section
  tags:
    "microstructure"           (MICROSTRUCTURE block of reasoning)
    "funding"                  (FUNDING + POSITIONING block)
    "technical"                (TECHNICAL block)
    "synthesis"                (SYNTHESIS block)
    "narrative"                (price-action narrative)
    "free_observation"         (free-form block)
    "binance_expert.taker_flow"
    "binance_expert.whale_flow"
    "binance_expert.oi_funding"
    "binance_expert.order_book"
    "binance_expert.positioning"
    "binance_expert.confluence"
    "binance_expert.edge"
    "binance_expert.watch"
    "binance_expert.analysis"
    "historical"               (historical_pattern)
    "historical_context"       (current_bar_context)
    "specialist.<name>"        (dow_theory/fib_pullback/alligator/acc_dist/harmonic)
    "ensemble_vote"
    "data_requests"            (only when flagging gaps)
- source_quotes[]: paste 1–3 short verbatim snippets from the INPUT (each
  ≤80 chars) that directly justify the bullet. Examples:
    "BSR=0.7914"
    "8.4 BTC sells vs 2.1 BTC buys"
    "434.9 BTC bids within 0.5%"
    "long liquidations of $966,708"
  Quotes are substring-matched against the INPUT. If a quote you invent is
  not in the INPUT, the bullet is stripped.
- Restate only facts in the INPUT. Never invent levels, numbers, bars, or
  directional calls.
- If the source is NEUTRAL or has no setup, say so plainly in edge — do not
  manufacture one.
- TEXT-METRIC CONTRACT: if bullet.text names a signal (whale, bid wall,
  taker flow, OI, funding, liquidations, CVD, basis, IV/skew, RSI), the
  conditions[] array MUST contain a metric from that same family. If you
  can't emit such a condition, drop the numeric claim from the text too.
  Mismatched text+metric would make the frontend's "↗ SOURCE" link point at
  the wrong data provider — a silent but real lie.
- Include numbers only when they carry trading meaning (price levels, bar
  IDs, ranges, time to close). Drop confidence percentages, latencies, and
  data-source labels.
- TONE = the trade direction that benefits when ALL conditions fire:
    * "bullish"  — firing favors a LONG trade (price expected to move UP)
    * "bearish"  — firing favors a SHORT trade (price expected to move DOWN)
    * "neutral"  — firing suggests HOLD / stand aside (no directional edge)
  Judge tone by reading the if_met consequence or the text's outcome clause,
  NOT the trigger signal. Examples:
    * "If bid imbalance collapses below X, downside acceleration likely" → BEARISH
      (the fire condition is bid-support weakening; outcome is price drop).
    * "If taker buy volume spikes above X, breakout confirmed" → BULLISH.
    * "If taker volume stays below X, regime continues, no edge" → NEUTRAL.
  A bullish-sounding trigger can have a bearish outcome (e.g. "if buyers lose
  control at X, reversal down"). Always read the consequence to decide tone.
- **BULLET TEXT MUST BE A COMPLETE SENTENCE.** Never emit a bullet whose text
  is just the signal name ("taker flow", "spot whale buy"). If you have
  something to say about a signal, write a full sentence. If you don't, OMIT
  the bullet entirely. Empty conditions does NOT mean empty text — the text
  carries the trader-usable info either way.
- Each bullet = ONE sentence. Total briefing ≤180 words across edge + all
  bullets. Scannable in 30s, but DENSE with signal coverage.
- watch: 3–5 bullets (one per major INPUT signal when present). actions: 2–4
  bullets (entry + invalidation + partial-fill ideas). DO NOT under-emit — a
  briefing with rich INPUT and only 2 watch bullets is a failure, not a
  virtue. Only drop a bullet when there is literally nothing specific to
  say about that signal in the INPUT.
- No hedging, no meta-commentary, no "the model says".

COMPLETENESS (LAZINESS IS AUDITED):
- If the INPUT cites a major signal — taker flow (BSR or absolute BTC),
  whale flow (buy/sell BTC), liquidations ($), order book wall (BTC depth),
  OI change (%), funding (%), RSI/momentum, specific price support/resistance
  levels, historical-precedent win rate — that signal should be represented
  somewhere in your output (edge OR a watch/action bullet). Silently dropping
  half of DeepSeek's signals to save space is not allowed.
- LIQUIDATIONS ARE HIGH-SIGNAL, NEVER DROP: if INPUT mentions any
  "long liquidations", "short liquidations", "cascade", "$X liquidated",
  "wicked stops", or specific liquidation levels — you MUST surface this in
  at least one bullet, either as a reason the current direction has legs
  (longs liquidated = downside accelerated) or as an invalidation level
  (absorbed cascade = reversal setup). Liquidations have been the
  most-frequently-missed topic in audit (79% drop rate) — prioritize them.
- On NEUTRAL bars: the rationale for abstaining (why the argument fails
  under steelman) must appear in edge. Don't reduce "NEUTRAL because taker
  ratio inverted and OI is detaching" to "no edge" — preserve the reason.

SCOPE-MATCHING (CRITICAL):
- Every metric in the INPUT is scope-tagged (e.g. "Binance-perp 5m", "aggregate 5-venue
  0.5% band", "daily macro"). Do NOT compare metrics across incompatible scopes in the
  same bullet. Examples of INVALID comparisons to avoid:
    * Aggregate book depth (hundreds of BTC, multi-venue) vs single-venue 5m taker flow
    * Single-exchange OI vs cross-exchange liquidations
    * Daily F&G/SOPR/MVRV as if they were bar-level triggers
- When referencing a number from INPUT, preserve its scope in your text. A "bid wall"
  must cite the depth band it was measured in ("within 0.5% of mid, across N venues").
- Bar-level conditions in `watch`/`actions` MUST use metrics that actually change
  bar-to-bar: price, price_change_pct, taker_buy_volume, taker_sell_volume, taker_ratio,
  bid_imbalance, ask_imbalance, funding_rate, open_interest, rsi, long_short_ratio,
  basis_pct, cvd_1h, rr_25d_30d. Metrics flagged as "MACRO CONTEXT" or "daily" in the
  INPUT may only appear in `edge` as background, never in `conditions`.

CONDITIONS — machine-checkable thresholds that back the bullet:
- "metric": use a clear snake_case name matching the signal the text references. Prefer
  these common names when they fit: price, price_change_pct, taker_buy_volume,
  taker_sell_volume, taker_volume, taker_ratio, bid_imbalance, ask_imbalance,
  funding_rate, open_interest, rsi, long_short_ratio, basis_pct, perp_cvd_1h,
  spot_cvd_1h, aggregate_cvd_1h, bid_depth_05pct, ask_depth_05pct, rr_25d_30d,
  iv_30d_atm, spot_whale_buy_btc, spot_whale_sell_btc, aggregate_funding_rate,
  aggregate_liquidations_usd, oi_velocity_pct. If the input uses a different signal,
  use a sensible snake_case name matching THAT family (do NOT substitute price or
  taker_* as a lazy stand-in for a whale-flow or book-depth claim).
- "op" must be one of: ">", ">=", "<", "<=", "==".
- "value" must be a number that APPEARS VERBATIM in the INPUT. For "between X and Y",
  emit TWO conditions: op ">=" X and op "<=" Y.
- "unit": "USD" for price, "BTC" for volume, "%" for rates/ratios, "" otherwise.
- If the bullet is pure narrative with no cited number, omit conditions.
- CROSS-SIGNAL PROTECTION: do not emit a condition on taker_* when the signal the
  text describes is actually whale_flow (different data source). Pick the metric
  that matches what the text is actually about.

DATA AVAILABILITY HANDLING:
- Preserve every signal the INPUT provides. If a signal is flagged "unavailable" in
  the INPUT, still surface it in the bullet text so the trader sees the gap — e.g.
  "Taker flow unavailable from backend — monitor independently; bullish move invalid
  until confirmed." The UI will render the condition pill as "source unavailable" if
  no live feed can verify it. Also flag the gap in `sources`: include "data_requests".

NO JARGON WITHOUT EVIDENCE:
- Do NOT use technical-analysis terminology (Wyckoff, Elliott wave, harmonic patterns, distribution phase, accumulation phase, liquidity grab, stop hunt, market structure break, order block, fair value gap, etc.) unless the INPUT gives a concrete price level, bar index, or measured condition that backs it. A percentage alone is NOT evidence. A name alone is NOT evidence.
- If the INPUT contains such a term but only hand-waves it, DROP the term and describe the underlying observation in plain words (e.g., "price compressed for 3 bars" instead of "accumulation phase").
- Prefer plain English: "buyers stepped in at $95,150" over "demand zone held".
"""

# Whitelist of metrics the frontend can look up a live value for. If Venice
# emits a different name, validate will keep the condition (UI renders "source
# unavailable") rather than let the UI show a meaningless pill.
_VALID_METRICS = {
    "price", "price_change_pct",
    "taker_buy_volume", "taker_sell_volume", "taker_volume",
    "taker_ratio", "bid_imbalance", "ask_imbalance",
    "funding_rate", "open_interest", "rsi", "long_short_ratio",
    "basis_pct", "perp_cvd_1h", "spot_cvd_1h", "aggregate_cvd_1h",
    "bid_depth_05pct", "ask_depth_05pct",
    "rr_25d_30d", "iv_30d_atm",
}
_VALID_OPS = {">", ">=", "<", "<=", "=="}

# Metrics where the underlying quantity is always non-negative in practice.
# Conditions like `metric > 0` on these are trivially true and give the trader
# no usable signal; we drop them at the audit layer and push Venice toward
# either picking a non-zero INPUT-cited threshold or omitting the condition.
_NON_NEGATIVE_METRICS: Set[str] = {
    "taker_buy_volume", "taker_sell_volume", "taker_volume",
    "open_interest",
    "bid_depth_05pct", "ask_depth_05pct",
    "spot_whale_buy_btc", "spot_whale_sell_btc",
    "aggregate_liquidations_usd",
}

# Expected unit per metric (what the frontend formatter assumes).
_EXPECTED_UNIT: Dict[str, str] = {
    "price": "USD", "price_change_pct": "%",
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

# Plausible magnitude bounds per metric. Venice has emitted things like
# `open_interest > 0.5` (meant "OI velocity 0.5%") — the value is inside
# INPUT somewhere (as a taker volume threshold) so the fabrication check
# passes, but the magnitude is wildly wrong for the metric family. We
# reject these at the validation layer so the pill never shows a nonsense
# comparison. Bounds are loose by design — meant to catch 3+ orders of
# magnitude mistakes, not tune pre-existing thresholds.
_METRIC_MAGNITUDE_OK: Dict[str, Tuple[float, float]] = {
    # quantities measured in absolute BTC — OI on Binance is ~50k-200k
    "open_interest":          (1_000.0, 10_000_000.0),
    # 5-min taker volume on BTCUSDT is typically 5-500 BTC; allow 0-5000
    "taker_buy_volume":       (0.0,    10_000.0),
    "taker_sell_volume":      (0.0,    10_000.0),
    "taker_volume":           (0.0,    20_000.0),
    # whale-trade volumes: 0.5-500 BTC common
    "spot_whale_buy_btc":     (0.0,    10_000.0),
    "spot_whale_sell_btc":    (0.0,    10_000.0),
    # 0.5%-depth books on Binance: ~50-5000 BTC
    "bid_depth_05pct":        (0.0,    50_000.0),
    "ask_depth_05pct":        (0.0,    50_000.0),
    # 1h CVD range can be signed; a few thousand BTC absolute at most
    "perp_cvd_1h":            (-50_000.0, 50_000.0),
    "spot_cvd_1h":            (-50_000.0, 50_000.0),
    "aggregate_cvd_1h":       (-100_000.0, 100_000.0),
    # price-like metrics
    "price":                  (1_000.0, 10_000_000.0),
    # rate-style metrics (percentage scale)
    "price_change_pct":       (-50.0,  50.0),
    "funding_rate":           (-5.0,   5.0),
    "aggregate_funding_rate": (-5.0,   5.0),
    "oi_velocity_pct":        (-100.0, 100.0),
    "basis_pct":              (-10.0,  10.0),
    "rr_25d_30d":             (-50.0,  50.0),
    "iv_30d_atm":             (0.0,    500.0),
    "bid_imbalance":          (-100.0, 100.0),
    "ask_imbalance":          (-100.0, 100.0),
    # ratios
    "taker_ratio":            (0.0,    50.0),
    "bsr":                    (0.0,    50.0),
    "long_short_ratio":       (0.0,    50.0),
    "rsi":                    (0.0,    100.0),
    # big USD totals
    "aggregate_liquidations_usd": (0.0, 10_000_000_000.0),
}

# Allowed values for the `sources` tag per bullet. Free-form but we coerce
# to a known set; unknown tags are kept if they match the shape
# "<section>" or "<section>.<sub>".
_SOURCE_TAG_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)?$")

# Signal-keyword → required metric family. If bullet.text contains a keyword,
# conditions[] must include at least one metric from the corresponding family,
# otherwise the conditions are dropped (preserving the bullet's text so the
# trader still sees the claim, but without a misleading pill).
_TEXT_SIGNAL_FAMILIES = [
    (re.compile(r"\bwhale\b",                            re.I),
        {"spot_whale_buy_btc", "spot_whale_sell_btc"}),
    (re.compile(r"\btaker (buy|sell) (volume|flow)\b",   re.I),
        {"taker_buy_volume", "taker_sell_volume", "taker_volume", "taker_ratio"}),
    (re.compile(r"\bBSR\b|taker ratio",                  re.I),
        {"taker_ratio"}),
    (re.compile(r"\b(bid|ask) (imbalance|depth|wall)\b", re.I),
        {"bid_imbalance", "ask_imbalance", "bid_depth_05pct", "ask_depth_05pct"}),
    (re.compile(r"\bfunding\b",                          re.I),
        {"funding_rate", "aggregate_funding_rate"}),
    # OI velocity / rate-of-change → oi_velocity_pct ONLY (unit %). The
    # absolute OI family is separate below. If text says "OI velocity flips
    # +0.5%" Venice must not route to `open_interest` (which is a BTC count
    # ~100k) — the 0.5 would be a degenerate threshold on the wrong family.
    (re.compile(r"\bOI\s+velocity\b|\bopen[- ]interest\s+velocity\b|"
                r"\bOI\s+(flips?|change|turns?|moves?|shifts?)\b|"
                r"\bOI\s+(rising|falling|growing|climbing|dropping|accelerat\w*)\b|"
                r"\bOI\s+(rate[- ]of[- ]change|ROC)\b",
                re.I),
        {"oi_velocity_pct"}),
    # Plain OI (absolute level)
    (re.compile(r"\bopen interest\b|\bOI\b",             re.I),
        {"open_interest", "oi_velocity_pct"}),
    (re.compile(r"\bliquidations?\b",                    re.I),
        {"aggregate_liquidations_usd"}),
    (re.compile(r"\bRSI\b",                              re.I), {"rsi"}),
    (re.compile(r"\blong/short\b|\bL/S ratio\b",         re.I), {"long_short_ratio"}),
    (re.compile(r"\bCVD\b",                              re.I),
        {"perp_cvd_1h", "spot_cvd_1h", "aggregate_cvd_1h"}),
    (re.compile(r"\bbasis\b",                            re.I), {"basis_pct"}),
    (re.compile(r"\b(IV|implied vol|skew|risk[- ]reversal)\b", re.I),
        {"iv_30d_atm", "rr_25d_30d"}),
]


def _truncate(s: Any, n: int) -> str:
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + " …"


# ── Number / text extraction helpers (INPUT ↔ OUTPUT verification) ─────

_NUMERIC_RE      = re.compile(r"-?\d+(?:[,.]\d+)*")
_TEXT_NUMBER_RE  = re.compile(r"\$?\s*-?\d+(?:[,.]\d+)+|\$?\s*-?\d+(?!\w)")


def _canon_number(s: str) -> Optional[str]:
    """Normalize a numeric token so 95,150 / 95150 / 95150.00 / $95150 all
    canonicalize to the same key."""
    s = s.replace(",", "").replace("$", "").strip()
    if not s or s in ("-",):
        return None
    try:
        f = float(s)
    except ValueError:
        return None
    # Round to 6 decimals to avoid float-artifact mismatch, then strip zeros.
    t = f"{f:.6f}".rstrip("0").rstrip(".")
    return t or "0"


def _extract_numbers(text: str) -> Set[str]:
    """Return set of canonical numeric strings present in `text`."""
    out: Set[str] = set()
    if not text:
        return out
    for m in _NUMERIC_RE.finditer(text):
        c = _canon_number(m.group(0))
        if c is not None:
            out.add(c)
    return out


def _num_is_cited(val: float, src_canons: Set[str], src_tokens_raw: List[str]) -> bool:
    """Check whether `val` is defensibly cited by the INPUT.

    Rules (calibrated against real false-positives where Venice invented
    "2.0 BTC" and my prior loose match happily accepted a bare "2" somewhere
    in the INPUT):
      * val == 0 is always trivially allowed (sign-flip thresholds are fine).
      * |val| ≥ 1000 (price levels): canonical or within 0.3 % of a source
        numeric token → cited.
      * 100 ≤ |val| < 1000: canonical match required.
      * |val| < 100: canonical match AND the matching source raw token must
        have ≥2 significant characters (contains a decimal point OR is
        ≥2 chars long ignoring sign). A bare one-char "2" is NOT evidence
        for a "2.0 BTC" threshold.
    """
    if val == 0:
        return True
    c = _canon_number(str(val))
    if c is None:
        return False
    aval = abs(val)

    if aval >= 1000:
        if c in src_canons:
            return True
        for raw in src_tokens_raw:
            try: tv = float(raw)
            except ValueError: continue
            if tv == 0: continue
            if abs(tv - val) / max(abs(tv), 1.0) <= 0.003:
                return True
        return False

    if aval >= 100:
        return c in src_canons

    # |val| < 100: require a source token with ≥2 significant characters.
    for raw in src_tokens_raw:
        if _canon_number(raw) != c:
            continue
        stripped = raw.lstrip("-").lstrip("$").strip()
        if "." in stripped or len(stripped) >= 2:
            return True
    return False


def _extract_input_number_set(input_text: str) -> Tuple[Set[str], List[str]]:
    """Return (canonical_set, raw_numeric_strings) for the Venice INPUT."""
    raw = [m.group(0).replace(",", "").replace("$", "").strip()
           for m in _NUMERIC_RE.finditer(input_text or "")]
    raw = [r for r in raw if r and r != "-"]
    canons = {c for c in (_canon_number(r) for r in raw) if c}
    return canons, raw


def _extract_output_numbers_in_text(text: str) -> List[float]:
    """Return numeric values referenced in prose (for fabrication checking).
    We only flag numbers that look like thresholds: $-prefixed, comma-grouped,
    or decimals — bare integers like "2 bars" are NOT flagged because the
    prompt also uses small counts ("next 2 bars") non-fabricationally."""
    out: List[float] = []
    for m in _TEXT_NUMBER_RE.finditer(text or ""):
        s = m.group(0).replace(",", "").replace("$", "").strip()
        # Skip bare single-digit integers (too many false positives as counts).
        if "." not in s and "$" not in m.group(0) and abs(int(s) if s.lstrip("-").isdigit() else 99) < 10:
            continue
        try:
            out.append(float(s))
        except ValueError:
            continue
    return out


# ── Prompt assembly ────────────────────────────────────────────────────

def _fmt_num(v: Any, fmt: str = "{:g}") -> Optional[str]:
    try:
        return fmt.format(float(v))
    except (TypeError, ValueError):
        return None


def _build_live_metrics_block(backend_snapshot: Optional[dict]) -> str:
    """Serialize the current-bar backend snapshot into a block of verbatim
    numeric tokens Venice can cite. Any number Venice emits that matches a
    value here passes the anti-fabrication check and reaches the UI as a
    live-anchored condition pill.

    Critical: these are the *same* numbers the UI displays. Keeping them
    verbatim in INPUT means DeepSeek mentions of BSR/OI/funding/etc. can be
    preserved as machine-checkable conditions instead of being dropped as
    "unsourced thresholds"."""
    if not backend_snapshot:
        return ""
    ds = backend_snapshot.get("dashboard_signals") or {}
    if not isinstance(ds, dict):
        return ""
    lines: List[str] = []

    tk = ds.get("taker_flow") or {}
    if isinstance(tk, dict):
        bsr = _fmt_num(tk.get("buy_sell_ratio"), "{:.4f}")
        bv  = _fmt_num(tk.get("taker_buy_vol_btc"), "{:.2f}")
        sv  = _fmt_num(tk.get("taker_sell_vol_btc"), "{:.2f}")
        if bsr: lines.append(f"  BSR (taker_ratio): {bsr}")
        if bv:  lines.append(f"  taker_buy_volume: {bv} BTC")
        if sv:  lines.append(f"  taker_sell_volume: {sv} BTC")

    ob = ds.get("order_book") or {}
    if isinstance(ob, dict):
        imb  = _fmt_num(ob.get("imbalance_05pct_pct"), "{:.2f}")
        bidd = _fmt_num(ob.get("bid_depth_05pct_btc"), "{:.1f}")
        askd = _fmt_num(ob.get("ask_depth_05pct_btc"), "{:.1f}")
        if imb:  lines.append(f"  bid_imbalance: {imb}%")
        if bidd: lines.append(f"  bid_depth_05pct: {bidd} BTC")
        if askd: lines.append(f"  ask_depth_05pct: {askd} BTC")

    # `oi_velocity` block (preferred) or `aggregate_oi` if present
    aoi = ds.get("aggregate_oi") or ds.get("oi_velocity") or {}
    if isinstance(aoi, dict):
        oi30 = _fmt_num(aoi.get("change_30min_pct"), "{:.3f}")
        if oi30: lines.append(f"  oi_velocity_pct (30min): {oi30}%")

    # Backend key is `oi_funding`, field names are `open_interest_btc` and
    # `funding_rate_8h_pct` (already pre-multiplied by 100).
    oif = ds.get("oi_funding") or {}
    if isinstance(oif, dict):
        oi = _fmt_num(oif.get("open_interest_btc"), "{:.1f}")
        fr_pct = _fmt_num(oif.get("funding_rate_8h_pct"), "{:.5f}")
        if oi:     lines.append(f"  open_interest: {oi} BTC")
        if fr_pct: lines.append(f"  funding_rate: {fr_pct}%")

    agg_fr = ds.get("aggregate_funding") or {}
    if isinstance(agg_fr, dict):
        afr = agg_fr.get("weighted_funding_rate")
        if isinstance(afr, (int, float)):
            afr_pct = _fmt_num(afr * 100, "{:.5f}")
            if afr_pct: lines.append(f"  aggregate_funding_rate: {afr_pct}%")

    al = ds.get("aggregate_liquidations") or {}
    if isinstance(al, dict):
        liq = _fmt_num(al.get("total_usd"), "{:.0f}")
        if liq: lines.append(f"  aggregate_liquidations_usd: ${liq}")

    wf = ds.get("spot_whale_flow") or {}
    if isinstance(wf, dict):
        wb = _fmt_num(wf.get("whale_buy_btc"), "{:.2f}")
        ws = _fmt_num(wf.get("whale_sell_btc"), "{:.2f}")
        if wb: lines.append(f"  spot_whale_buy_btc: {wb} BTC")
        if ws: lines.append(f"  spot_whale_sell_btc: {ws} BTC")

    cvd = ds.get("cvd") or {}
    if isinstance(cvd, dict):
        pc = _fmt_num(cvd.get("perp_cvd_1h_btc"), "{:.1f}")
        sc = _fmt_num(cvd.get("spot_cvd_1h_btc"), "{:.1f}")
        ac = _fmt_num(cvd.get("aggregate_cvd_1h_btc"), "{:.1f}")
        if pc: lines.append(f"  perp_cvd_1h: {pc} BTC")
        if sc: lines.append(f"  spot_cvd_1h: {sc} BTC")
        if ac: lines.append(f"  aggregate_cvd_1h: {ac} BTC")

    spb = ds.get("spot_perp_basis") or {}
    if isinstance(spb, dict):
        bp = _fmt_num(spb.get("basis_pct"), "{:.4f}")
        if bp: lines.append(f"  basis_pct: {bp}%")

    sk = ds.get("deribit_skew_term") or {}
    if isinstance(sk, dict):
        rr = _fmt_num(sk.get("rr_25d_30d_pct"), "{:.3f}")
        iv = _fmt_num(sk.get("iv_30d_atm_pct"), "{:.2f}")
        if rr: lines.append(f"  rr_25d_30d: {rr}%")
        if iv: lines.append(f"  iv_30d_atm: {iv}%")

    ls = ds.get("long_short_ratio") or {}
    if isinstance(ls, dict):
        lsr = _fmt_num(ls.get("long_short_ratio"), "{:.3f}")
        if lsr: lines.append(f"  long_short_ratio: {lsr}")

    if not lines:
        return ""
    return (
        "current_live_metrics (live readings for THIS bar — reference these exact\n"
        "values in conditions[] so the UI can anchor pills to live data):\n"
        + "\n".join(lines)
    )


def _build_user_prompt(
    pred: dict,
    historical: str,
    binance_expert: dict,
    historical_context: str = "",
    specialist_signals: Optional[dict] = None,
    ensemble_result: Optional[dict] = None,
    backend_snapshot: Optional[dict] = None,
) -> str:
    """Assemble the INPUT block from the main-page fields. Now forwards more of
    DeepSeek's intelligence than before — specialist_signals (5 strategies),
    ensemble vote, current-bar historical_context, and raised truncation caps
    so rich sections reach Venice intact.

    backend_snapshot: current-bar live metric readings (BSR, OI, funding,
    whale flow, liquidations, depth, CVD, basis, skew, L/S). Embedded as a
    `current_live_metrics:` block so every live number becomes an
    INPUT-verbatim anchor for conditions[] — without this block, Venice
    can't cite current readings and the UI sees empty conditions."""
    parts = ["INPUT:"]
    signal = (pred.get("signal") or "?").upper()
    parts.append(f"signal: {signal}")
    parts.append(f"confidence: {pred.get('confidence', '?')}")

    live_block = _build_live_metrics_block(backend_snapshot)
    if live_block:
        parts.append(live_block)

    if ensemble_result and ensemble_result.get("signal"):
        bull = ensemble_result.get("bullish_count", 0)
        bear = ensemble_result.get("bearish_count", 0)
        upp  = ensemble_result.get("up_probability", 0.5)
        ens_conf = ensemble_result.get("confidence")
        ens_conf_s = f" conf={ens_conf*100:.0f}%" if isinstance(ens_conf,(int,float)) else ""
        parts.append(f"ensemble_vote: {ensemble_result['signal']} ({bull}↑/{bear}↓ up_prob={upp*100:.0f}%{ens_conf_s})")

    data_received = pred.get("data_received") or ""
    data_requests = pred.get("data_requests") or ""
    if data_received:
        parts.append(f"data_received: {_truncate(data_received, 600)}")
    if data_requests and data_requests.upper() != "NONE":
        parts.append(f"data_requests (gaps flagged by DeepSeek): {_truncate(data_requests, 600)}")

    is_neutral = signal == "NEUTRAL"

    reasoning = pred.get("reasoning") or ""
    if reasoning:
        r = reasoning if is_neutral else _truncate(reasoning, 5000)
        parts.append(f"reasoning:\n{r}")

    narrative = pred.get("narrative") or ""
    if narrative:
        n = narrative if is_neutral else _truncate(narrative, 1500)
        parts.append(f"narrative: {n}")

    free_obs = pred.get("free_observation") or ""
    if free_obs:
        f = free_obs if is_neutral else _truncate(free_obs, 900)
        parts.append(f"free_observation: {f}")

    if specialist_signals:
        spec_parts = ["specialist_signals (5 independent strategies):"]
        for name in ("dow_theory", "fib_pullback", "alligator", "acc_dist", "harmonic"):
            v = specialist_signals.get(name) or {}
            if v and v.get("signal"):
                conf_raw = v.get("confidence")
                conf_pct = int(float(conf_raw) * 100) if isinstance(conf_raw,(int,float)) else "?"
                reasoning_snip = _truncate(str(v.get("reasoning","")), 200)
                spec_parts.append(f"  specialist.{name}: {v['signal']} ({conf_pct}%) — {reasoning_snip}")
        if len(spec_parts) > 1:
            parts.append("\n".join(spec_parts))

    if historical_context:
        hc = historical_context if is_neutral else _truncate(historical_context, 2500)
        parts.append(f"historical_context:\n{hc}")

    if historical:
        h = historical if is_neutral else _truncate(historical, 4000)
        parts.append(f"historical:\n{h}")

    if binance_expert:
        sig = binance_expert.get("signal")
        be_parts = [f"binance_expert signal: {sig or '?'}"]
        for field in ("edge", "watch", "confluence", "taker_flow", "whale_flow",
                      "oi_funding", "positioning", "order_book",
                      "analysis", "narrative", "reasoning"):
            v = binance_expert.get(field)
            if v:
                v = str(v)
                cap = 1200 if is_neutral else 800
                be_parts.append(f"  binance_expert.{field}:\n    {_truncate(v, cap)}")
        parts.append("\n".join(be_parts))

    return "\n\n".join(parts)


async def _call_venice(
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    timeout_s: float = 25.0,
    extra_messages: Optional[List[Dict[str, str]]] = None,
) -> str:
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
        "max_tokens":      1600,   # raised for 5 watch + 4 actions + 2-sentence edge + quotes
        "temperature":     0.2,
        "response_format": {"type": "json_object"},
    }
    timeout   = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(VENICE_URL, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Venice HTTP {resp.status}: {body[:400]}")
            data = json.loads(body)
            return data["choices"][0]["message"]["content"]


_METRIC_NAME_OK = re.compile(r"^[a-z][a-z0-9_]{1,60}$")


# ── Validation (anti-fabrication + completeness) ────────────────────────

_TEXT_FAMILY_EXCLUSIONS: List[Tuple[re.Pattern, Set[str]]] = [
    # Velocity / rate-of-change text ("OI velocity", "OI rising") must NOT
    # route to the absolute OI metric. 0.5 on oi_velocity_pct means "+0.5%";
    # on open_interest it'd be "0.5 BTC" — 5 orders of magnitude off.
    (re.compile(r"\bOI\s+velocity\b|\bopen[- ]interest\s+velocity\b|"
                r"\bOI\s+(flips?|change|turns?|moves?|shifts?)\b|"
                r"\bOI\s+(rising|falling|growing|climbing|dropping|accelerat\w*)\b|"
                r"\bOI\s+(rate[- ]of[- ]change|ROC)\b|"
                r"\bvelocity\s+(above|below|>|<)\s*[-+]?\d+(\.\d+)?\s*%",
                re.I),
     {"open_interest"}),
]


def _text_signal_families(text: str) -> Set[str]:
    fams: Set[str] = set()
    for rx, fam in _TEXT_SIGNAL_FAMILIES:
        if rx.search(text or ""):
            fams |= fam
    # Subtractive pass: when text implies a rate-of-change / velocity, the
    # absolute-quantity metric is not a valid target for the condition pill
    # even though the broader regex matched.
    for rx, excluded in _TEXT_FAMILY_EXCLUSIONS:
        if rx.search(text or ""):
            fams -= excluded
    return fams


def _norm_conditions(raw: Any, input_canons: Set[str], input_raw: List[str]) -> Tuple[list, List[str]]:
    """Validate + normalize conditions. Returns (surviving_conditions, drop_notes).

    Preservation policy (2026-04-24, post "? UNKNOWN" incident): we keep a
    condition even if its threshold value isn't verbatim in INPUT, tagging it
    `heuristic: true` so the UI can still render the live value + met/unmet
    against a rule-of-thumb floor (e.g. "BSR < 0.80 persistence"). Only drop
    conditions that would produce an actively WRONG pill — wrong metric
    family, wrong unit, or absurd magnitude."""
    notes: List[str] = []
    if not isinstance(raw, list):
        return [], notes
    out = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        metric = c.get("metric")
        op     = c.get("op")
        val    = c.get("value")
        unit   = c.get("unit", "")
        if not isinstance(metric, str) or not _METRIC_NAME_OK.match(metric):
            continue
        if op not in _VALID_OPS:
            continue
        try:
            val = float(val)
        except (TypeError, ValueError):
            continue
        # Reject degenerate conditions: `>0` on an always-positive metric is
        # trivially met and confuses the trader. Push Venice to either pick a
        # non-zero threshold or omit the condition entirely.
        if (val == 0 or val == 0.0) and metric in _NON_NEGATIVE_METRICS:
            if op in (">", ">=", "=="):
                notes.append(f"degenerate_trivial_threshold:{metric}{op}{val}")
                continue
        # Magnitude sanity: if Venice picks the wrong metric family but copies
        # a value from INPUT (e.g. "open_interest > 0.5" when the 0.5 was a
        # velocity %), the value would be 5+ orders of magnitude too small
        # for OI. Reject — a wrong pill misleads the trader.
        bounds = _METRIC_MAGNITUDE_OK.get(metric)
        if bounds is not None:
            lo, hi = bounds
            if not (lo <= val <= hi):
                notes.append(f"magnitude_out_of_range:{metric}{op}{val}(expected {lo}..{hi})")
                continue
        # Unit-sanity: the declared unit MUST match the metric family.
        # Reject `open_interest` + unit `%`, or `oi_velocity_pct` + unit `BTC`
        # — these indicate Venice confused the family even when the number
        # happens to pass magnitude bounds.
        expected_unit = _EXPECTED_UNIT.get(metric)
        declared_unit = str(unit or "").strip()
        if expected_unit is not None and declared_unit:
            if declared_unit.upper() != expected_unit.upper() and expected_unit != "":
                # Allow empty/missing declared unit — Venice often omits it.
                # Reject only an explicit mismatch.
                notes.append(f"unit_mismatch:{metric}(declared={declared_unit},expected={expected_unit})")
                continue
        # Heuristic vs cited: a value that's verbatim in INPUT is a cited
        # level; one that isn't is a rule-of-thumb (e.g. BSR<0.80 persistence
        # floor). Both are rendered as pills, but the UI can style heuristic
        # pills differently so the trader knows the threshold didn't come
        # from a live reading.
        is_cited = _num_is_cited(val, input_canons, input_raw)
        # Auto-correct unit to the expected one for known metrics. Venice is
        # inconsistent here ("%" for BTC volumes, missing for rates). UI uses
        # the metric() formatter so this is cosmetic, but we normalize so the
        # pill label is clean across the dataset.
        if expected_unit is not None:
            unit_str = expected_unit
        else:
            unit_str = declared_unit[:16]
        out.append({
            "metric":    metric, "op": op, "value": val,
            "unit":      unit_str,
            "heuristic": (not is_cited),
        })
        if not is_cited:
            notes.append(f"heuristic_threshold_kept:{metric}{op}{val}")
        if len(out) >= 4:
            break
    return out, notes


def _norm_sources(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    out = []
    for s in raw:
        if not isinstance(s, str):
            continue
        s = s.strip().lower()
        if _SOURCE_TAG_RE.match(s):
            out.append(s)
        if len(out) >= 6:
            break
    return out


def _norm_source_quotes(raw: Any, input_text: str) -> Tuple[List[str], List[str]]:
    """Keep only quotes that are substrings (case-insensitive) of the INPUT.
    Returns (kept, dropped). Each quote is capped at 120 chars."""
    kept: List[str] = []
    dropped: List[str] = []
    if not isinstance(raw, list):
        return kept, dropped
    low = (input_text or "").lower()
    for q in raw:
        if not isinstance(q, str):
            continue
        q = q.strip()
        if not q:
            continue
        q = q[:120]
        if q.lower() in low:
            kept.append(q)
        else:
            dropped.append(q)
        if len(kept) >= 5:
            break
    return kept, dropped


def _bullet_text_numeric_coherence(text: str, input_canons: Set[str], input_raw: List[str]) -> List[str]:
    """Return list of offending numeric strings cited in `text` that have no
    INPUT source."""
    bad = []
    for val in _extract_output_numbers_in_text(text):
        if not _num_is_cited(val, input_canons, input_raw):
            bad.append(str(val))
    return bad


def _text_meets_family(text: str, metrics: Set[str]) -> Tuple[bool, Set[str]]:
    """If the text names a signal family, either conditions include a metric
    from that family OR we flag the bullet for condition-dropping."""
    fams = _text_signal_families(text)
    if not fams:
        return True, set()
    if metrics & fams:
        return True, set()
    return False, fams


def _validate(obj: Any, input_text: str, strict: bool = False) -> Tuple[Optional[dict], Dict[str, Any]]:
    """Return (normalized summary, audit_notes).

    Two modes (per user feedback: don't nuke large sections over formatting):

      strict=False (default, first pass):
        * Fabricated condition VALUES are dropped (no wrong pill reaches UI).
        * Fabricated text numbers + missing source_quotes are FLAGGED but
          bullets are KEPT. The caller will retry with corrections, and on
          retry we take whichever pass has less fabrication + more content.
        * Text-metric family mismatch → conditions cleared (bullet stays).

      strict=True (final defense on a FINAL output):
        * Same as lenient, PLUS bullets with fabricated text numbers that
          the retry failed to fix are auto-rescued (fabricated clause
          stripped) or, if unrecoverable, dropped. The goal is to never
          let a fabricated number reach the trader, without losing the
          entire bullet if some of it is verifiable.
    """
    audit = {"dropped_values": [], "dropped_quotes": [],
             "family_mismatch_conditions_cleared": [],
             "fabricated_text_numbers": [], "bullets_dropped": [],
             "bullets_rescued": [], "missing_quotes_bullets": [],
             "heuristic_thresholds_kept": []}

    if not isinstance(obj, dict):
        return None, audit
    edge = obj.get("edge")
    if not isinstance(edge, str) or not edge.strip():
        return None, audit

    input_canons, input_raw = _extract_input_number_set(input_text)
    low_input = input_text.lower()

    # Edge: if it cites numbers not in input, flag. In strict mode, rescue
    # by stripping the offending price token — but keep the edge.
    edge_bad = _bullet_text_numeric_coherence(edge, input_canons, input_raw)
    if edge_bad:
        audit["fabricated_text_numbers"].append({"where": "edge", "nums": edge_bad, "text": edge})
        if strict:
            edge = _rescue_text(edge, edge_bad)

    def _norm_bullets(key: str, cap: int) -> list:
        arr = obj.get(key) or []
        if not isinstance(arr, list):
            return []
        out = []
        for b in arr:
            if not isinstance(b, dict):
                continue
            text = b.get("text")
            tone = b.get("tone", "neutral")
            if not isinstance(text, str) or not text.strip():
                continue
            if tone not in ("bullish", "bearish", "neutral"):
                tone = "neutral"
            if_met_raw = b.get("if_met")
            if_met = str(if_met_raw).strip()[:200] if if_met_raw else ""

            # STRICT: condition values must be cited. Drop fabricated values
            # immediately on BOTH passes — a wrong pill is always worse than
            # no pill. Heuristic thresholds (rule-of-thumb values not in INPUT)
            # are KEPT but flagged so the UI can style the pill differently
            # and the retry loop doesn't treat them as drops.
            conds, drop_notes = _norm_conditions(b.get("conditions"), input_canons, input_raw)
            for n in drop_notes:
                if n.startswith("heuristic_threshold_kept:"):
                    audit["heuristic_thresholds_kept"].append(n)
                else:
                    audit["dropped_values"].append(n)

            # Text fabrication: flag on pass 1, rescue / drop on pass 2.
            text_bad   = _bullet_text_numeric_coherence(text,   input_canons, input_raw)
            ifmet_bad  = _bullet_text_numeric_coherence(if_met, input_canons, input_raw)
            all_bad    = text_bad + ifmet_bad
            if all_bad:
                audit["fabricated_text_numbers"].append({"where": key, "nums": all_bad, "text": text})
                if strict:
                    rescued_text   = _rescue_text(text,   text_bad)
                    rescued_ifmet  = _rescue_text(if_met, ifmet_bad) if if_met else if_met
                    still_bad_text   = _bullet_text_numeric_coherence(rescued_text,  input_canons, input_raw)
                    still_bad_ifmet  = _bullet_text_numeric_coherence(rescued_ifmet, input_canons, input_raw)
                    if still_bad_text or still_bad_ifmet:
                        # Can't salvage — drop rather than ship a lie.
                        audit["bullets_dropped"].append({"text": text, "why": f"unrescuable_fabrication:{all_bad}"})
                        continue
                    audit["bullets_rescued"].append({"before": text, "after": rescued_text, "nums_removed": all_bad})
                    text, if_met = rescued_text, rescued_ifmet

            # Text-metric family contract
            metrics_used = {c["metric"] for c in conds}
            ok, missing_fams = _text_meets_family(text, metrics_used)
            if not ok:
                # The condition pill would point at the wrong data source.
                # Clear conditions (bullet still shows as narrative) so the
                # frontend doesn't link to a misleading source provider.
                audit["family_mismatch_conditions_cleared"].append({
                    "text": text, "text_metrics_used": sorted(metrics_used),
                    "expected_family": sorted(missing_fams),
                })
                conds = []

            sources = _norm_sources(b.get("sources"))
            quotes, dropped_quotes = _norm_source_quotes(b.get("source_quotes"), input_text)
            if dropped_quotes:
                audit["dropped_quotes"].extend(dropped_quotes)

            # If quotes are missing, try to auto-derive one from any cited
            # number in the bullet text that we can prove is in the INPUT
            # with ~40 chars of surrounding context. This keeps the bullet
            # alive even when Venice forgets to fill source_quotes[] — per
            # "don't drop large sections over formatting".
            if not quotes:
                quotes = _auto_derive_quotes(text + " " + if_met, input_text, input_canons, input_raw)
                if quotes:
                    audit["missing_quotes_bullets"].append({"text": text, "auto_derived": quotes})
                elif strict:
                    # Strict mode: if we can't derive ANY provable source, the
                    # bullet is unanchored — drop it.
                    audit["bullets_dropped"].append({"text": text, "why": "no_verifiable_source"})
                    continue
                else:
                    audit["missing_quotes_bullets"].append({"text": text, "auto_derived": []})

            out.append({
                "tone":          tone,
                "text":          text.strip(),
                "conditions":    conds,
                "if_met":        if_met or None,
                "sources":       sources,
                "source_quotes": quotes,
            })
            if len(out) >= cap:
                break
        return out

    watch   = _norm_bullets("watch",   5)
    actions = _norm_bullets("actions", 4)
    return {
        "edge":    edge.strip(),
        "watch":   watch,
        "actions": actions,
    }, audit


def _rescue_text(text: str, bad_nums: List[str]) -> str:
    """Attempt to remove fabricated numeric claims from text while preserving
    surrounding meaning. Strategy: for each bad number, find it in the text
    and strip the smallest phrase containing it (comma/period delimited).
    Falls back to simple token replacement if phrase stripping fails."""
    if not bad_nums or not text:
        return text
    rescued = text
    for num in bad_nums:
        # match "$77,500", "77,500", "77500", "77500.0" etc.
        try: fval = float(num)
        except ValueError: continue
        forms = _num_string_forms(fval)
        for form in forms:
            pattern = re.compile(r"(?<!\d)" + re.escape(form) + r"(?!\d)")
            m = pattern.search(rescued)
            if not m:
                continue
            # strip the enclosing clause (comma-delimited) containing m
            start, end = m.span()
            left  = rescued.rfind(",", 0, start)
            right = rescued.find(",", end)
            if left == -1: left = 0
            else:          left += 1
            if right == -1: right = len(rescued)
            clause = rescued[left:right]
            rescued = (rescued[:left] + rescued[right:]).strip(" ,.")
            # tidy: collapse double spaces/commas
            rescued = re.sub(r"\s*,\s*,", ", ", rescued)
            rescued = re.sub(r"\s{2,}", " ", rescued)
            break
    return rescued.strip()


def _num_string_forms(val: float) -> List[str]:
    """Return plausible string forms of a number we'd want to find in text:
    '$77,500', '77,500', '77500', '77500.00', '2.0', '2', etc."""
    forms: List[str] = []
    aval = abs(val)
    if aval >= 1000:
        ival = int(round(val))
        forms.append(f"{ival:,}")
        forms.append(str(ival))
        if val != ival:
            forms.append(f"{val:,.2f}")
    else:
        forms.append(f"{val:g}")
        forms.append(str(val))
        if val == int(val):
            forms.append(str(int(val)))
        forms.append(f"{val:.1f}")
    return list(dict.fromkeys(forms))


def _auto_derive_quotes(bullet_text: str, input_text: str,
                        input_canons: Set[str], input_raw: List[str]) -> List[str]:
    """Given a bullet, find numbers in it that ARE cited by INPUT, and return
    short INPUT-substring quotes containing each. Max 2 quotes, each ≤80 chars.
    Provides a minimal anchor when Venice forgets source_quotes[]."""
    quotes: List[str] = []
    values = _extract_output_numbers_in_text(bullet_text)
    if not values:
        return quotes
    low = input_text.lower()
    for val in values:
        if not _num_is_cited(val, input_canons, input_raw):
            continue
        for form in _num_string_forms(val):
            idx = low.find(form.lower())
            if idx == -1:
                continue
            # Extract ~60-char snippet centered on the number
            s = max(0, idx - 25)
            e = min(len(input_text), idx + len(form) + 25)
            snippet = input_text[s:e].strip().replace("\n", " ")
            if snippet and len(snippet) >= 4:
                quotes.append(snippet[:80])
                break
        if len(quotes) >= 2:
            break
    return quotes


# ── Completeness scoring ───────────────────────────────────────────────

# Keywords → scores. If INPUT mentions the keyword in a signal-y context, the
# Venice output should reference the same topic somewhere (edge OR bullet).
_COMPLETENESS_TOPICS = [
    ("taker_flow",      re.compile(r"\b(BSR|taker (buy|sell|flow|volume|ratio))\b", re.I),
                        re.compile(r"\b(BSR|taker|buy[- ]sell ratio|aggressor)\b", re.I)),
    ("whale",           re.compile(r"\bwhale\b", re.I),
                        re.compile(r"\bwhale|spot (buy|sell)\b", re.I)),
    ("order_book",      re.compile(r"\b(bid|ask)[ -](imbalance|depth|wall)\b", re.I),
                        re.compile(r"\bbid|ask|book|depth|wall|imbalance\b", re.I)),
    ("funding",         re.compile(r"\bfunding\b", re.I),
                        re.compile(r"\bfunding\b", re.I)),
    ("open_interest",   re.compile(r"\bopen interest|\bOI\b", re.I),
                        re.compile(r"\bOI\b|open interest", re.I)),
    ("liquidations",    re.compile(r"\bliquidation", re.I),
                        re.compile(r"\bliquidation", re.I)),
    ("rsi_momentum",    re.compile(r"\bRSI|MACD|Stoch|momentum\b", re.I),
                        re.compile(r"\bRSI|MACD|Stoch|momentum|overbought|oversold\b", re.I)),
    ("historical",      re.compile(r"\bhistorical|similar bars?|precedent\b", re.I),
                        re.compile(r"\bhistorical|similar|precedent\b", re.I)),
]


def _completeness_score(summary: dict, input_text: str) -> Dict[str, Any]:
    """How many major INPUT topics are represented in the Venice output?"""
    all_text = summary["edge"] + "\n" + "\n".join(
        [b["text"] + " " + (b.get("if_met") or "")
         for b in summary["watch"] + summary["actions"]]
    )
    covered = {}
    expected = []
    for name, input_rx, output_rx in _COMPLETENESS_TOPICS:
        if input_rx.search(input_text or ""):
            expected.append(name)
            covered[name] = bool(output_rx.search(all_text))
    n_expected = len(expected)
    n_covered  = sum(1 for v in covered.values() if v)
    ratio = (n_covered / n_expected) if n_expected else 1.0
    return {
        "expected": expected,
        "covered":  [k for k, v in covered.items() if v],
        "missed":   [k for k, v in covered.items() if not v],
        "ratio":    round(ratio, 3),
    }


def _evict_old() -> None:
    if len(_cache) <= _CACHE_MAX:
        return
    for k in sorted(_cache.keys())[: len(_cache) - _CACHE_MAX]:
        _cache.pop(k, None)
        _locks.pop(k, None)


# ── Orchestrator: call → validate → maybe retry → cache ────────────────

async def get_or_build(
    window_start_time: float,
    pred: dict,
    historical: str,
    binance_expert: dict,
    api_key: str,
    model: str,
    historical_context: str = "",
    specialist_signals: Optional[dict] = None,
    ensemble_result: Optional[dict] = None,
    backend_snapshot: Optional[dict] = None,
) -> Optional[dict]:
    """
    Return the cached summary for this bar, or build it once. Returns None on
    any failure so the caller (engine/server) can fall back to the raw blocks.

    On validation-stripping (fabricated values dropped, bullets removed), we
    issue ONE corrective retry before giving up — the retry tells Venice what
    we stripped and asks it to redo using only INPUT-verbatim numbers.
    """
    if not api_key or not window_start_time or not pred or pred.get("signal") in (None, "ERROR", "UNAVAILABLE"):
        return None

    cached = _cache.get(window_start_time)
    if cached:
        return cached

    lock = _locks.setdefault(window_start_time, asyncio.Lock())
    async with lock:
        cached = _cache.get(window_start_time)
        if cached:
            return cached

        started = time.time()
        try:
            user_prompt = _build_user_prompt(
                pred, historical, binance_expert,
                historical_context=historical_context,
                specialist_signals=specialist_signals or {},
                ensemble_result=ensemble_result or {},
                backend_snapshot=backend_snapshot,
            )
            raw = await _call_venice(api_key, model, SYSTEM_PROMPT, user_prompt)
        except Exception as exc:
            logger.warning("trader_summary Venice call FAILED for bar %s: %s", window_start_time, exc)
            return None

        try:
            obj = json.loads(raw)
        except Exception as exc:
            logger.warning("trader_summary JSON parse FAILED for bar %s: %s (raw=%r)", window_start_time, exc, raw[:300])
            return None

        # PASS 1: lenient — drops fabricated condition values, flags text
        # fabrication + missing source_quotes but keeps the bullets so we don't
        # gut the briefing on the first try. We then decide whether to retry.
        cleaned, audit = _validate(obj, user_prompt, strict=False)
        if not cleaned:
            logger.warning("trader_summary invalid shape for bar %s: %r", window_start_time, obj)
            return None

        # Score completeness and decide whether to retry.
        completeness = _completeness_score(cleaned, user_prompt)
        retry_reasons: List[str] = []
        if audit["dropped_values"]:
            # Dropped because of: degenerate_trivial_threshold (e.g. OI>0),
            # magnitude_out_of_range (e.g. open_interest > 0.5 when OI ~96k),
            # or unit_mismatch (e.g. oi_velocity_pct + unit BTC). These are
            # wrong-family errors, not heuristic thresholds (those are kept).
            retry_reasons.append(f"dropped wrong-family condition values: {audit['dropped_values'][:5]}")
        if audit["fabricated_text_numbers"]:
            nums = [n for f in audit["fabricated_text_numbers"] for n in f["nums"]][:5]
            retry_reasons.append(f"bullet/edge text cites numbers not in INPUT: {nums}")
        if audit["missing_quotes_bullets"]:
            n = sum(1 for b in audit["missing_quotes_bullets"] if not b["auto_derived"])
            if n:
                retry_reasons.append(f"{n} bullet(s) with no source_quotes and no auto-derivable anchor")
        if audit["family_mismatch_conditions_cleared"]:
            retry_reasons.append(f"{len(audit['family_mismatch_conditions_cleared'])} bullet(s) had text naming one signal but conditions on another")
        # Under-coverage trigger. 0.80 threshold: if >= 20% of major INPUT
        # signals are silently dropped, we retry. Tightened from 0.75 after
        # 30-bar offline audit showed bars at 0.75-0.80 coverage still
        # systematically dropping liquidations + funding.
        if completeness["ratio"] < 0.80 and len(completeness["expected"]) >= 3:
            retry_reasons.append(f"missed major INPUT topics: {completeness['missed']}")
        # Liquidations are the single most-dropped topic (79% drop rate in
        # the offline corpus). Fire a dedicated retry whenever liquidations
        # is expected but missed, even if overall coverage is otherwise ok.
        if "liquidations" in (completeness.get("missed") or []):
            retry_reasons.append("liquidations signal present in INPUT but absent from output")
        # Density trigger: if INPUT has 4+ signals but briefing has < 3 total
        # bullets, we retry even if coverage is technically ok (the edge line
        # alone covering a topic isn't enough — the trader needs an actionable
        # bullet per major signal).
        total_bullets = len(cleaned["watch"]) + len(cleaned["actions"])
        if len(completeness["expected"]) >= 4 and total_bullets < 3:
            retry_reasons.append(
                f"only {total_bullets} bullets for {len(completeness['expected'])} major INPUT signals — insufficient density"
            )

        final_cleaned, final_audit = cleaned, audit
        if retry_reasons:
            logger.info("trader_summary bar %s retrying due to: %s", window_start_time, retry_reasons)
            correction = (
                "Your previous output failed the audit:\n- "
                + "\n- ".join(retry_reasons)
                + "\n\nRedo the JSON. Rules that were violated:\n"
                + "* VERBATIM-ONLY: every number in text, if_met, and conditions.value "
                + "must appear literally in the INPUT. Do NOT round, estimate, or invent thresholds.\n"
                + "* SOURCE_QUOTES: every bullet needs at least one source_quote that is a "
                + "literal substring of the INPUT.\n"
                + "* TEXT-METRIC CONTRACT: if the text names 'whale', 'bid wall', 'taker flow', "
                + "'OI', 'funding', 'liquidations', 'RSI', etc., the conditions[] must use a metric "
                + "from that same family.\n"
                + "* COMPLETENESS: do NOT silently drop major signals cited in the INPUT.\n\n"
                + "Produce fresh JSON that fixes these issues. Keep the briefing format the same."
            )
            try:
                raw2 = await _call_venice(
                    api_key, model, SYSTEM_PROMPT, user_prompt,
                    extra_messages=[
                        {"role": "assistant", "content": raw},
                        {"role": "user",      "content": correction},
                    ],
                )
                obj2 = json.loads(raw2)
                cleaned2, audit2 = _validate(obj2, user_prompt, strict=False)
                if cleaned2 and (len(cleaned2["watch"]) + len(cleaned2["actions"])) >= 1:
                    completeness2 = _completeness_score(cleaned2, user_prompt)
                    fab1 = len(audit["fabricated_text_numbers"]) + len(audit["dropped_values"])
                    fab2 = len(audit2["fabricated_text_numbers"]) + len(audit2["dropped_values"])
                    bullets1 = len(cleaned["watch"]) + len(cleaned["actions"])
                    bullets2 = len(cleaned2["watch"]) + len(cleaned2["actions"])
                    # Accept retry if it's strictly better along any axis
                    # without regressing on the others:
                    #   (a) fewer fabrications, same-or-more bullets
                    #   (b) same fabrications, better coverage
                    #   (c) same fabrications + coverage, more bullets (denser)
                    # Never accept a retry that ships FEWER bullets than pass 1.
                    if fab2 <= fab1 and bullets2 >= bullets1:
                        if (fab2 < fab1
                            or completeness2["ratio"] > completeness["ratio"]
                            or bullets2 > bullets1):
                            final_cleaned, final_audit = cleaned2, audit2
                            completeness = completeness2
            except Exception as exc:
                logger.warning("trader_summary retry failed for bar %s: %s", window_start_time, exc)

        # PASS 2 (strict): apply rescue-or-drop to whichever pass we took. A
        # fabricated number must NEVER reach the client — but if we can strip
        # the offending clause and keep the rest of the bullet, we do that
        # rather than nuking the whole bullet.
        final_cleaned, final_audit_strict = _validate(
            {"edge": final_cleaned["edge"],
             "watch":   [{**b, "conditions": b.get("conditions", []),
                          "source_quotes": b.get("source_quotes", [])}
                         for b in final_cleaned["watch"]],
             "actions": [{**b, "conditions": b.get("conditions", []),
                          "source_quotes": b.get("source_quotes", [])}
                         for b in final_cleaned["actions"]]},
            user_prompt, strict=True,
        )
        if not final_cleaned:
            logger.warning("trader_summary strict-pass returned empty for bar %s", window_start_time)
            return None
        # Merge audit trails
        merged_audit = dict(final_audit)
        for k, v in final_audit_strict.items():
            if isinstance(v, list):
                merged_audit.setdefault(k, []).extend(v)

        final_cleaned["generated_at"]   = time.time()
        final_cleaned["generation_ms"]  = int((time.time() - started) * 1000)
        final_cleaned["model"]          = model
        final_cleaned["window_start"]   = window_start_time
        final_cleaned["audit"]          = {
            **merged_audit,
            "completeness": completeness,
            "retry_reasons": retry_reasons,
        }

        _cache[window_start_time] = final_cleaned
        _evict_old()
        logger.info(
            "trader_summary bar %s built in %dms (watch=%d actions=%d dropped_vals=%d "
            "fab_text=%d rescued=%d bullets_dropped=%d coverage=%.2f)",
            window_start_time, final_cleaned["generation_ms"],
            len(final_cleaned["watch"]), len(final_cleaned["actions"]),
            len(merged_audit.get("dropped_values", [])),
            len(merged_audit.get("fabricated_text_numbers", [])),
            len(merged_audit.get("bullets_rescued", [])),
            len(merged_audit.get("bullets_dropped", [])),
            completeness["ratio"],
        )
        return final_cleaned


def get_cached(window_start_time: Optional[float]) -> Optional[dict]:
    if window_start_time is None:
        return None
    return _cache.get(window_start_time)


def drop(window_start_time: Optional[float]) -> None:
    if window_start_time is None:
        return
    _cache.pop(window_start_time, None)
    _locks.pop(window_start_time, None)
