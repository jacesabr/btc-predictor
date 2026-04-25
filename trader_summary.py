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


SYSTEM_PROMPT = """You compress a BTC 5-minute prediction analysis into a trader briefing a reader can digest in 30 seconds — ideally 10.

========================= WRITING STYLE =========================
Imagine you are an experienced trader sitting next to a smart friend
who is NEW to trading. You're not reading a report to them — you're
narrating what's happening on the screen in plain, conversational
English. Every bullet should sound like something you'd actually say
out loud, using "we", "buyers", "sellers", price action, and orders
you can see in the book. No jargon, no code words, no acronyms the
friend would have to look up.

TARGET TONE — the shape we want (DO NOT copy these angle-bracketed
placeholders as literal text; they are slots to fill from your INPUT):
  "<Buyers|Sellers> are pushing in for <the INPUT's reason>, but it
   looks like a pullback — we need to wait for a <downward|upward>
   entry because we can see orders waiting in the book at
   <the INPUT's price level>."

That one sentence carries observation, reason, interpretation,
guidance, and evidence. Every actionable bullet should flow like that
— NOT the fragmented "If X, then Y, bears regain control" robot
pattern. Anything inside <...> is a slot to be replaced with the real
figure or descriptor from your INPUT, not a literal word to echo.

Every bullet follows this structure:
      [READING with value]  →  [what it means, explained like a human]

The arrow "→" hands off from "here's the number" to
"here's what it actually means for you right now". The meaning half
MUST be in simple conversational English — NOT trader slang.

BANNED JARGON (find the plain-English substitute):
   fade          → "price likely drops back" or "the move runs out of gas"
   wall          → "big block of orders" / "stack of resting orders"
   squeeze       → "forced exits" / "traders trapped on the wrong side"
   chop          → "price drifts sideways" / "no clear direction"
   distributing  → "a big holder is selling into the move"
   accumulating  → "a big holder is quietly buying"
   thesis        → "the reason to be long/short" / "the setup"
   longs / shorts→ "buyers" / "sellers" (except as part of trade action "go long / go short")
   conviction    → "commitment" / "confidence"
   absorption    → "orders getting filled without moving price much"
   divergence    → "two signals pointing opposite ways"
   Wyckoff / accumulation phase / order block / liquidity grab
                 → drop these words entirely; describe the bar behaviour
   basis / skew / vol surface / term structure
                 → translate into what it implies for price next

GOOD EXAMPLES (plain-English, experienced-trader voice). All
<placeholders> are slots to be filled with real INPUT figures —
NEVER emit the literal word "N" or the brackets themselves:
  "RSI <N> → no momentum either way, a move now may not have power behind it"
  "BSR <N> → <buyers|sellers> pushing hard, but it's extreme; <they> are running out of steam so price likely drops back soon"
  "Order book almost even (<N> bids / <N> asks) → similar size on both sides, price can move either way without hitting resistance"
  "Funding flat (<N>%) → no crowd lopsided on one side, no forced exits likely"
  "A large holder sold <N> BTC → that seller is taking profit, which weakens the case for <buyers|sellers>"
  "Open interest climbing as price climbs → fresh money is committing, the up-move usually keeps going"
  "If price goes above $<level> and <buyers|sellers> keep stepping in (BSR > <N>) → go long, get out if price falls to $<stop>"

BAD EXAMPLES (and why):
  "RSI <N> — no edge"
        ← Too terse. Say what this means: "no momentum either way,
          a move now may not have power behind it".
  "Balanced book offers no structural support or resistance"
        ← Jargon-filled analyst prose. Rewrite as: "similar size on
          both sides, price can move either way without hitting resistance".
  "Funding neutral — no squeeze risk"
        ← "Squeeze" is jargon. Explain: "no crowd lopsided on one
          side, no forced exits likely".
  "Whales distributing, bullish thesis weakens"
        ← "Distributing" and "thesis" are jargon. Rewrite as: "a big
          holder is selling into the move, which weakens the case
          for <buyers>".
  "Price breaks $<level> + BSR > <N> → long, stop $<stop>"
        ← For actions this is OK, BUT introduce context for a newer
          trader: "If price goes above $<level> and <buyers> keep
          stepping in (BSR > <N>) → go long, get out if price falls
          to $<stop>".

Rules:
  * Lead with the metric + its value. Numbers first.
  * The arrow "→" is preferred. Em-dash "—" acceptable when the
    implication flows as one clause.
  * Max 22 words for watch bullets, 28 for action bullets. The
    implication half is where you spend words — explain, don't
    compress into jargon.
  * Cut these filler words entirely: may, suggests, appears, seems to,
    showing no, offers no, with no risk despite, it is worth noting,
    interestingly, notably, structurally, manages to, in order to.
  * Avoid "which", "that is", "given that" — use em-dash, comma, or arrow.
  * One observation per bullet.
  * When you feel reached for a trader-jargon word, STOP and write
    the plain-English substitute instead.
=================================================================



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
  "edge": "THE CHART STORY — told as a NARRATIVE, not a fact-dump. Aim for 4-5 short plain-English sentences (~80-130 words) covering: (1) what's happening right now, (2) what opposing forces are at play, (3) any extreme oscillator/momentum reading the trader should know about (RSI <30 or >70, Stoch at 0/100, MFI extreme), (4) what historical precedent says with EXACT counts if cited (e.g. 'all three similar bars went DOWN', NOT 'usually drifts'), (5) ONE counter-risk closer pulled from DeepSeek's PREMORTEM — what would invalidate the call. Structure example: 'Price has been testing $77,746 for the last three bars. Buyers put a stack of orders there but they haven't been active yet. Sellers are mildly pushing with low volume, and RSI at 18 is deeply oversold which usually triggers a bounce. The two most similar historical setups both bounced from a level like this. But if taker sell volume spikes above 3 BTC while bids stay un-filled, that bounce thesis breaks.' This is more than a one-liner — it's a real narrative. The TRADER NEEDS THE COUNTER-RISK so they know what to watch for. Numbers must appear verbatim in INPUT. Don't paraphrase historical counts (2/3 DOWN, 4/5 noise, 0/3 UP) into vague phrasing — those exact ratios are the trader's edge. Don't drop extreme RSI/Stoch/MFI/CVD readings — they're decisive context even if not the call's primary driver. When funding/OI is negative across multiple venues (Binance + OKX + Coinalyze), say 'across all three venues' — single-venue wording loses the cross-exchange confirmation.",
  "watch": [{
    "tone": "bullish|bearish|neutral",
    "text": "A NARRATIVE VALIDATOR — an if-then-means statement that will either confirm or invalidate the chart story in `edge` as it plays out live. Form: 'If <specific observable event, at whatever price / level / threshold it actually happens>, that means <plain-English consequence for the narrative or for the trader's next move>.' The space of observable events is OPEN and dynamic — pick whatever DeepSeek's INPUT actually observed or reasoned about for THIS bar, not a template list. Good candidates include (but are not limited to): price breaking or holding a cited level, a specific signal crossing a specific threshold, a pattern completing or failing, a volume event, an order-book event, a correlation breaking, a historical precedent playing out or not. Examples: 'If price holds $77,746 through the next two bars, that means the buyer defense is real and the bounce thesis confirms.' 'If taker sell volume spikes above 5 BTC while bids at $77,746 don't fill, that means buyers are getting overrun and the narrative breaks.' 'If the 10 historical matches DeepSeek cited at similar BSR keep reversing up, that means the statistical edge tilts long despite short-term pressure.' Plain English only — no jargon like fade/squeeze/wall/distribution/thesis/order-block. Numbers cited must appear verbatim in INPUT.",
    "conditions": [{"metric": "<name>", "op": ">"|">="|"<"|"<="|"==", "value": <number>, "unit": "<unit>"}],
    "if_met": "short phrase (<=12 words) stating the DIRECT consequence the INPUT text supports. Omit if text already says it.",
    "sources": ["<section 1>", "<section 2>"],
    "source_quotes": ["<short verbatim snippet from INPUT>", "..."]
  }],
  "actions": [{
    "tone": "bullish|bearish|neutral",
    "text": "IF-THEN, trader-pidgin. Max 20 words. 'If price breaks <level>, long with stop <level>.' Cut hedges ('may', 'could', 'possibly'). Good: 'Price > $78,050 + BSR > 2 — long, stop $77,900.' Bad: 'If price manages to break above $78,050 with taker buy volume confirmation...'",
    "conditions": [same shape as watch],
    "if_met": "the trader's concrete action when conditions fire (<=12 words).",
    "sources": ["..."],
    "source_quotes": ["..."]
  }]
}

HARD RULES:
- VERBATIM NUMBERS ONLY. Every numeric value you emit — in a condition, in
  the text, or in if_met — must appear in the INPUT text. Do NOT estimate,
  round, or infer thresholds. If DeepSeek cites a BSR figure, copy it
  digit-for-digit — do NOT round. If DeepSeek reports a specific BTC
  volume, do not invent a neighboring round number as a reversal threshold.
  All numbers must be copied verbatim from the INPUT body; numbers in
  THIS SYSTEM PROMPT are format illustrations, never usable as data.
- DON'T FLIP TONE FROM A WEAK OBSERVATION. If the observation itself is
  directionally one-sided (e.g. "spot whales are all buying, 0.63 BTC
  buys / 0.00 BTC sells"), the bullet MUST be tagged with the MATCHING
  tone — here, bullish or neutral. Do not manufacture a contrary tone
  by tacking on "but too small to matter, price will still drop".
  If the signal is genuinely too small to matter, either:
    (a) tag the bullet neutral and state the observation as context, OR
    (b) OMIT the bullet — a microscopic reading that needs a narrative
        crutch to become a trade signal isn't a trade signal.
  Bullish observations with bearish tone (and vice-versa) read as broken
  reasoning to the trader and erode trust in the whole briefing.
- NO EQUALITY THRESHOLDS ON CONTINUOUS FLOATS. Do not emit
  `whale_buy == 0.63` or `funding_rate == -0.00161` — equality on a live
  numeric that ticks continuously is either trivially true right now and
  false the next tick (brittle) or never true (useless). Use `>=` / `<=`
  for volume/ratio/rate triggers, and reserve `==` only for regime-
  boundary values like `BSR == 1.000` where the number has literal
  semantic meaning (equal buy/sell).
- NO DEGENERATE THRESHOLDS. Do not emit `taker_buy_volume > 0`, `whale_buy > 0`,
  `open_interest > 0`, or any `> 0` threshold on a quantity that is always
  non-negative. If INPUT says a regime is absent or zero (e.g. "zero taker
  flow regime", "no whales"), describe the absence as the bullet's
  narrative — the trigger is "a non-trivial figure appears", and you must
  pick a specific non-zero threshold from the INPUT (e.g. cite the 3-bar
  average, the prior spike, or a break level). If no meaningful threshold
  exists, OMIT the condition and keep the bullet as pure narrative.
- If a bullet needs a threshold that isn't in the INPUT, either:
    (a) quote the INPUT's own number as the threshold (refer to the
        specific figure the INPUT cites — bid-wall depth, support price,
        prior-bar volume), OR
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
  ≤80 chars) that directly justify the bullet. Pick SHORT token sequences
  that name the metric + its value as they appear in the INPUT text.
  Quotes are substring-matched against the INPUT. If a quote you invent is
  not in the INPUT, the bullet is stripped. DO NOT copy the example shapes
  below as literal values — they are format illustrations, not real data:
    shape like "<METRIC>=<number>" (e.g. the BSR and its value)
    shape like "<X> BTC sells vs <Y> BTC buys"
    shape like "<N> BTC bids within <Z>%"
    shape like "long liquidations of $<amount>"
  If you find yourself echoing "0.7914", "8.4", "434.9", or "966708" in a
  quote, STOP — those are placeholders, not real numbers from your INPUT.
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
- watch: 3–7 bullets (one IF-AT-MEANS statement per signal DeepSeek
  analyzed in the INPUT). actions: 2–4 bullets (entry + invalidation +
  partial-fill ideas). The watch array is the CHART STORY — each bullet
  maps to one signal type with its live-monitorable condition:
      - taker flow  → "If BSR <> <N>, that means ..."
      - order book  → "If price tests $<level> where <N> BTC orders sit, that means ..."
      - funding     → "If funding <> <N>% while price <> <level>, that means ..."
      - open int.   → "If OI <> <N> BTC as price <> <level>, that means ..."
      - RSI         → "If RSI <> <N> on the next bar, that means ..."
      - liquidations→ "If $<N> liquidations hit below $<level>, that means ..."
      - whale flow  → "If whale <buys/sells> exceed <N> BTC, that means ..."
  If DeepSeek's reasoning includes multiple of these, emit a bullet for
  EACH. The watch array read top-to-bottom should read like an expert
  narrating the chart: "at this level X is happening which means Y, and
  if Z happens next it means W." Don't lump signals into one bullet; one
  signal per bullet so each has its own live condition pill in the UI.
- BALANCE observation-bullets with trigger-bullets. An observation-bullet
  describes what IS happening now and has a condition whose threshold is
  already met (e.g. "Sellers are dominating — BSR <N> with <X> BTC sells vs
  <Y> BTC buys" + condition `taker_ratio < 1.0`). A trigger-bullet names a
  future state (e.g. "If BSR flips above <threshold> → buyers take over").
  A healthy briefing has BOTH: observations tell the trader what's happening
  right now (these show in ACTIONABLE since their condition is already
  satisfied), triggers tell them what to watch for (these show in WAITING).
  If you put every rich observation into the `edge` sentence and reserve
  bullets only for future triggers, the ACTIONABLE section goes empty and
  the trader has to expand WAITING to see any detail. Don't do that. At
  least 2 of your watch bullets should describe CURRENT state with a
  threshold the live value is already past.
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

NEUTRAL ACTIONS — STATE CHANGES, NOT TRADE ORDERS:
- On NEUTRAL DS bars, actions[] are still allowed AND useful — a manual
  trader watches them as conditional triggers ("if X happens, the regime
  shifts and a directional setup forms"). But the if_met / text MUST read
  as a STATE CHANGE the trader must reassess from, NOT an auto-execute
  directive.
- BAD (directive on NEUTRAL): "If price breaks $77,650 with BSR > 2.0,
  long with stop $77,500." → if_met: "Go long, stop $77,500."
- GOOD (state-change on NEUTRAL): "If price breaks $77,650 with BSR > 2.0,
  the bull thesis confirms — reassess and consider longs from this
  reclaim." → if_met: "Bull thesis confirmed — reassess directional bias."
- The trigger conditions are the same; only the framing changes. The
  trader still gets the live monitoring; they just aren't told to fire
  off a position DS explicitly declined.
- This rule does NOT apply when DS signal is UP or DOWN — there a
  directional action with stop is appropriate (DS endorsed a direction).

ACTION STOPS — DS-CITED LEVELS ONLY:
- A "stop at $X" clause inside an action requires that $X is a level DS
  itself named as support / resistance / swing low / swing high / wall /
  cluster / pivot / Fib in its narrative. DS effectively never authors
  stops directly — the stop must be ANCHORED to a structural level DS
  identified, not just any price token from the INPUT.
- If you can't find a DS-cited structural level for the stop, OMIT the
  "stop at $X" clause entirely. The action without a stop is still useful
  ("If price breaks $77,650 with BSR > 2.0 → long setup forms"); a
  fabricated stop misleads the trader into thinking $X is a tested
  invalidator.
- Do NOT round DS levels to nice numbers ($77,538.40 → $77,540 is a
  fabrication, $77,500 if DS cited $77,485 is a fabrication).

THRESHOLD ≠ CURRENT VALUE:
- A condition value that sits AT or within rounding distance of the
  current live reading fires on noise, not on a real regime change.
- BAD: current OI is 33,856 BTC, condition `open_interest > 33,856` —
  fires on +1 BTC of normal noise.
- BAD: current BSR is 0.580, condition `taker_ratio < 0.580` — fires on
  the next downtick.
- BAD: current funding is -0.00543%, condition `funding_rate > -0.00543%`
  — fires on display rounding.
- GOOD: current BSR is 0.580, condition `taker_ratio < 0.30` — meaningful
  regime shift to deeper sell-side dominance.
- GOOD: current OI is 33,856 BTC, condition `open_interest > 34,000` —
  meaningful upside breakout (+0.4% above current).
- Pick thresholds at LEVELS DS NAMED as flip / break / persistence
  triggers, not at the current reading.

SPECIALIST DISAGREEMENT MUST APPEAR IN EDGE:
- If DS's reasoning includes any of: "SPECIALIST_AGREEMENT: X/Y",
  "ensemble votes UP at N% but ...", "NO_TRADE veto", "Binance expert
  vetoes", "1 of 3 specialists agreed", "ensemble disagrees", or a
  "blind baseline overruled" line — that disagreement is the most
  load-bearing piece of context for the trader (it tells them this is a
  contrarian-to-the-models call, or that one specialist explicitly says
  don't trade).
- The edge sentence MUST mention the disagreement in one short clause:
  "ensemble UP 87% but Binance expert vetoes" / "2 of 3 specialists
  NEUTRAL" / "DS overruling its own bullish blind baseline".
- Without this, the trader has no idea why a NEUTRAL was called against
  a strongly bullish ensemble, or why a directional call is contrarian.

PREMORTEM COVERAGE — REQUIRED IF DS PROVIDES ONE:
- If DS reasoning contains a "PREMORTEM:" / "Most likely reason this
  call is wrong:" / "If wrong:" paragraph, at least ONE watch bullet
  MUST validate the premortem trigger — sharing its specific numeric
  thresholds verbatim. Example DS premortem: "the call is wrong if a
  spot whale cluster (≥3 trades ≥0.5 BTC in 2 minutes) appears."
  Required watch bullet: "If ≥3 spot whale buys ≥0.5 BTC fire in 2
  minutes, the call invalidates" with `spot_whale_buy_btc >= 0.5`.
- Do NOT weaken the premortem threshold (≥5 BTC → 0.5 BTC) — copy DS's
  number verbatim. Weakening means the watch fires on noise rather than
  on the actual invalidator.

N DISCLOSURE ON ABSOLUTE CLAIMS:
- "always", "never", "every time", "all of", "in every case",
  "consistently" — these are ABSOLUTE quantifiers. They are only
  allowed if the SAME SENTENCE includes the sample size DS gave
  (n=X or X/Y count).
- BAD: "history shows similar setups always drift sideways" — n hidden.
- GOOD: "DS's 4 prior similar bars all drifted under 0.1% (4/4)" —
  n=4 disclosed.
- If DS's analog is n=2 or n=3 (small sample), it is NOT "always" — say
  "2/3 prior", "3/3 noise (n=3)", etc.

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
- Prefer plain English over jargon, e.g. "buyers stepped in at <INPUT_price_level>"
  over "demand zone held" (replace <INPUT_price_level> with the actual level
  cited in the INPUT, never invent one).
"""

# Whitelist of metrics the frontend's metric() lookup can resolve to a live
# value. Conditions with a metric NOT in this set are dropped in
# _norm_conditions — otherwise Venice could invent arbitrary metric names
# like "spot_vol_bursts" and the UI would render a pointless "source
# unavailable" pill. Every metric here has a matching case in static/app.jsx
# metric() and a matching METRIC_META entry.
_VALID_METRICS = {
    "price", "price_change_pct",
    "taker_buy_volume", "taker_sell_volume", "taker_volume",
    "taker_ratio", "bsr",
    "bid_imbalance", "ask_imbalance",
    "funding_rate", "aggregate_funding_rate",
    "open_interest", "oi_velocity_pct",
    "rsi", "long_short_ratio",
    "basis_pct", "perp_cvd_1h", "spot_cvd_1h", "aggregate_cvd_1h",
    "bid_depth_05pct", "ask_depth_05pct",
    "rr_25d_30d", "iv_30d_atm",
    "spot_whale_buy_btc", "spot_whale_sell_btc",
    "aggregate_liquidations_usd",
    # Technical indicators (sourced from strategies[] on the client).
    "stoch_k", "macd_histogram", "adx", "ema_5_13_diff", "vwap_ref",
    # Dashboard-signal-sourced metrics.
    "mark_premium_pct", "dvol_pct", "btc_dominance_pct", "fear_greed",
    "mempool_fee", "kraken_premium_pct", "top_long_short_ratio",
    "spot_perp_cvd_div", "put_call_ratio",
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
    # Bounded-positive technical/dashboard metrics.
    "adx",                  # 0-100
    "stoch_k",              # 0-100
    "vwap_ref",             # USD price
    "dvol_pct",             # >= 0
    "btc_dominance_pct",    # 0-100
    "fear_greed",           # 0-100
    "mempool_fee",          # sat/vB, >= 0
    "top_long_short_ratio", # >= 0
    "put_call_ratio",       # >= 0
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
    # Technical indicators (unitless / USD).
    "stoch_k": "", "macd_histogram": "", "adx": "", "ema_5_13_diff": "USD",
    "vwap_ref": "USD",
    # Dashboard-signal metrics.
    "mark_premium_pct": "%", "dvol_pct": "%", "btc_dominance_pct": "%",
    "fear_greed": "", "mempool_fee": "sat/vB", "kraken_premium_pct": "%",
    "top_long_short_ratio": "", "spot_perp_cvd_div": "BTC", "put_call_ratio": "",
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
    # technical indicators
    "stoch_k":                (0.0,    100.0),
    "macd_histogram":         (-10_000.0, 10_000.0),  # signed dollars per bar
    "adx":                    (0.0,    100.0),
    "ema_5_13_diff":          (-10_000.0, 10_000.0),  # signed USD diff
    "vwap_ref":               (1_000.0, 10_000_000.0),
    # dashboard signals
    "mark_premium_pct":       (-10.0,  10.0),
    "dvol_pct":               (0.0,    500.0),
    "btc_dominance_pct":      (0.0,    100.0),
    "fear_greed":             (0.0,    100.0),
    "mempool_fee":            (0.0,    10_000.0),
    "kraken_premium_pct":     (-10.0,  10.0),
    "top_long_short_ratio":   (0.0,    50.0),
    "spot_perp_cvd_div":      (-100_000.0, 100_000.0),
    "put_call_ratio":         (0.0,    20.0),
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
    # Plural-tolerant: matches "whale" and "whales" (e.g. "Spot whales sold ...").
    (re.compile(r"\bwhales?\b",                          re.I),
        {"spot_whale_buy_btc", "spot_whale_sell_btc"}),
    # Catches "taker buy", "taker sell", "taker buy and sell volumes",
    # "taker aggression", "taker aggressor".
    (re.compile(r"\btaker\s+(?:buy|sell|aggression|aggressor)", re.I),
        {"taker_buy_volume", "taker_sell_volume", "taker_volume", "taker_ratio"}),
    # Catches "taker volume(s)", "taker flow", "taker ratio" (plural-safe).
    (re.compile(r"\btaker\s+(?:volumes?|flow|ratio)\b",  re.I),
        {"taker_buy_volume", "taker_sell_volume", "taker_volume", "taker_ratio"}),
    (re.compile(r"\bBSR\b",                              re.I),
        {"taker_ratio"}),
    (re.compile(r"\b(?:bid|ask)\s+(?:imbalance|depth|wall|side|book)\b", re.I),
        {"bid_imbalance", "ask_imbalance", "bid_depth_05pct", "ask_depth_05pct"}),
    # Plain "order book" mentions or "X BTC bids vs Y BTC asks" patterns.
    (re.compile(r"\border\s+book\b|\bBTC\s+(?:bids?|asks?)\b", re.I),
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
    (re.compile(r"\blong/short\b|\bL/S\s*ratio\b",       re.I),
        {"long_short_ratio"}),
    (re.compile(r"\btop\s+(?:trader|account)s?\s+(?:L/S|long/short|position)\b", re.I),
        {"top_long_short_ratio"}),
    (re.compile(r"\bCVD\b",                              re.I),
        {"perp_cvd_1h", "spot_cvd_1h", "aggregate_cvd_1h"}),
    (re.compile(r"\bspot[/-]perp\s+(?:CVD\s+)?(?:divergence|div)\b", re.I),
        {"spot_perp_cvd_div"}),
    (re.compile(r"\bbasis\b",                            re.I), {"basis_pct"}),
    (re.compile(r"\b(IV|implied vol|skew|risk[- ]reversal)\b", re.I),
        {"iv_30d_atm", "rr_25d_30d"}),
    # Technical indicators (live values come from strategies[]).
    (re.compile(r"\bstoch(?:astic)?\b|%\s*K\b|%\s*D\b",  re.I),
        {"stoch_k"}),
    (re.compile(r"\bMACD\b",                             re.I),
        {"macd_histogram"}),
    (re.compile(r"\bADX\b",                              re.I), {"adx"}),
    (re.compile(r"\bEMA\s*\d+|ema_cross|EMA\s*cross",    re.I),
        {"ema_5_13_diff"}),
    (re.compile(r"\bVWAP\b",                             re.I), {"vwap_ref"}),
    # Dashboard-signal-sourced metrics.
    (re.compile(r"\bmark\s+premium\b",                   re.I),
        {"mark_premium_pct"}),
    (re.compile(r"\bDVOL\b",                             re.I), {"dvol_pct"}),
    (re.compile(r"\bBTC\s+dominance\b|\bdominance\b",    re.I),
        {"btc_dominance_pct"}),
    (re.compile(r"\bfear\s*[\&]\s*greed\b|\bF\&G\b",     re.I),
        {"fear_greed"}),
    (re.compile(r"\bmempool\b",                          re.I), {"mempool_fee"}),
    (re.compile(r"\bKraken\s+premium\b",                 re.I),
        {"kraken_premium_pct"}),
    (re.compile(r"\bput[/-]call\b",                      re.I),
        {"put_call_ratio"}),
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
    # `funding_rate_8h_pct` (already pre-multiplied by 100). We also forward
    # the `venue` tag so Venice knows whether OI/funding came from Binance
    # (primary) or OKX (fallback, ~3x smaller OI pool). Without this,
    # Venice would cite Binance-labeled numbers even under OKX fallback.
    oif = ds.get("oi_funding") or {}
    if isinstance(oif, dict):
        oi = _fmt_num(oif.get("open_interest_btc"), "{:.1f}")
        fr_pct = _fmt_num(oif.get("funding_rate_8h_pct"), "{:.5f}")
        venue = oif.get("venue") or "binance_perp"
        if oi:     lines.append(f"  open_interest: {oi} BTC (venue: {venue})")
        if fr_pct: lines.append(f"  funding_rate: {fr_pct}% (venue: {venue})")

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
        "max_tokens":      2200,   # raised for 5 watch + 4 actions + 4-5-sentence edge (with premortem closer) + quotes
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


# ── Degenerate-near-current detection (Fix 5: thresholds set at the live
# reading fire on rounding noise rather than a real regime change) ─────
#
# Per-metric absolute tolerance. If |threshold − current| < tol, the
# condition is degenerate and gets dropped. Values picked to be "the
# smallest move that means anything" for each metric family.
_DEGENERATE_NEAR_CURRENT_ABS: Dict[str, float] = {
    "taker_ratio":                 0.05,    # BSR within 0.05 of current
    "bsr":                         0.05,
    "long_short_ratio":            0.05,
    "rsi":                         1.5,     # RSI within 1.5 points
    "funding_rate":                0.0005,  # within 0.0005% (display tick)
    "aggregate_funding_rate":      0.005,
    "bid_imbalance":               0.5,     # within 0.5 percentage points
    "ask_imbalance":               0.5,
    "oi_velocity_pct":             0.005,
    "basis_pct":                   0.005,
    "rr_25d_30d":                  0.05,
    "iv_30d_atm":                  0.5,
    "price_change_pct":            0.05,
}
# For metrics with no abs tolerance defined, fall back to relative %
# of |current|. Volumes and depths are noisy → larger band; price tight.
_DEGENERATE_NEAR_CURRENT_REL: Dict[str, float] = {
    "open_interest":               0.001,   # 0.1% of OI (~30 BTC on 33k)
    "bid_depth_05pct":             0.01,    # 1% of depth
    "ask_depth_05pct":             0.01,
    "taker_buy_volume":            0.10,    # 10% of taker volume
    "taker_sell_volume":           0.10,
    "taker_volume":                0.10,
    "spot_whale_buy_btc":          0.10,
    "spot_whale_sell_btc":         0.10,
    "perp_cvd_1h":                 0.01,
    "spot_cvd_1h":                 0.01,
    "aggregate_cvd_1h":            0.01,
    "price":                       0.0005,  # ~$40 on $80k BTC
    "aggregate_liquidations_usd":  0.10,
}


def _is_degenerate_near_current(metric: str, op: str, value: float,
                                 current_values: Dict[str, float]) -> bool:
    """True if |value − current[metric]| is below the meaningful-move band.
    Only applies to inequality ops (>, >=, <, <=). Equality is handled by
    the existing fragile-equality-on-float check.

    v3-D (2026-04-25): also flag round-number values within 0.5% of
    current — Venice loves to write `OI > 34,000` when current is 33,976
    BTC because 34,000 reads as a "level". The threshold is 0.07% above
    current and inside any normal noise band."""
    if op == "==":
        return False
    cur = current_values.get(metric)
    if cur is None:
        return False
    abs_tol = _DEGENERATE_NEAR_CURRENT_ABS.get(metric)
    if abs_tol is None:
        rel = _DEGENERATE_NEAR_CURRENT_REL.get(metric)
        if rel is None:
            return False
        abs_tol = rel * max(abs(cur), 1e-9)
    if abs(value - cur) < abs_tol:
        return True
    # Round-number near current: if value is a "nice" round number AND
    # within 0.5% of current, treat as degenerate. Catches 34,000 vs
    # 33,976 (0.07%), 33,800 vs 33,856 (0.16%), etc.
    if abs(cur) >= 100:
        rel_dist = abs(value - cur) / max(abs(cur), 1e-9)
        if rel_dist < 0.005:
            ival = int(round(value))
            if ival == value and (ival % 100 == 0 or ival % 1000 == 0):
                return True
    return False


def _extract_current_metric_values(backend_snapshot: Optional[dict]) -> Dict[str, float]:
    """Flat metric_name → current value map from the backend snapshot.
    Mirrors _build_live_metrics_block but returns numbers for the
    degenerate-near-current detector to compare against."""
    out: Dict[str, float] = {}
    if not backend_snapshot:
        return out
    ds = backend_snapshot.get("dashboard_signals") or {}
    if not isinstance(ds, dict):
        return out

    def _f(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    tk = ds.get("taker_flow") or {}
    if isinstance(tk, dict):
        bsr = _f(tk.get("buy_sell_ratio"))
        if bsr is not None:
            out["taker_ratio"] = bsr
            out["bsr"] = bsr
        bv = _f(tk.get("taker_buy_vol_btc"))
        sv = _f(tk.get("taker_sell_vol_btc"))
        if bv is not None: out["taker_buy_volume"] = bv
        if sv is not None: out["taker_sell_volume"] = sv
        if bv is not None and sv is not None: out["taker_volume"] = bv + sv

    ob = ds.get("order_book") or {}
    if isinstance(ob, dict):
        v = _f(ob.get("imbalance_05pct_pct"))
        if v is not None:
            out["bid_imbalance"] = v
            out["ask_imbalance"] = -v
        bd = _f(ob.get("bid_depth_05pct_btc"))
        ad = _f(ob.get("ask_depth_05pct_btc"))
        if bd is not None: out["bid_depth_05pct"] = bd
        if ad is not None: out["ask_depth_05pct"] = ad

    aoi = ds.get("aggregate_oi") or ds.get("oi_velocity") or {}
    if isinstance(aoi, dict):
        v = _f(aoi.get("change_30min_pct"))
        if v is not None: out["oi_velocity_pct"] = v

    oif = ds.get("oi_funding") or {}
    if isinstance(oif, dict):
        v = _f(oif.get("open_interest_btc"))
        if v is not None: out["open_interest"] = v
        v = _f(oif.get("funding_rate_8h_pct"))
        if v is not None: out["funding_rate"] = v

    afr = (ds.get("aggregate_funding") or {}).get("weighted_funding_rate")
    afr = _f(afr)
    if afr is not None: out["aggregate_funding_rate"] = afr * 100

    al = (ds.get("aggregate_liquidations") or {}).get("total_usd")
    al = _f(al)
    if al is not None: out["aggregate_liquidations_usd"] = al

    wf = ds.get("spot_whale_flow") or {}
    if isinstance(wf, dict):
        wb = _f(wf.get("whale_buy_btc"))
        ws = _f(wf.get("whale_sell_btc"))
        if wb is not None: out["spot_whale_buy_btc"] = wb
        if ws is not None: out["spot_whale_sell_btc"] = ws

    cvd = ds.get("cvd") or {}
    if isinstance(cvd, dict):
        for src, dst in [("perp_cvd_1h_btc", "perp_cvd_1h"),
                         ("spot_cvd_1h_btc", "spot_cvd_1h"),
                         ("aggregate_cvd_1h_btc", "aggregate_cvd_1h")]:
            v = _f(cvd.get(src))
            if v is not None: out[dst] = v

    spb = ds.get("spot_perp_basis") or {}
    if isinstance(spb, dict):
        v = _f(spb.get("basis_pct"))
        if v is not None: out["basis_pct"] = v

    sk = ds.get("deribit_skew_term") or {}
    if isinstance(sk, dict):
        for src, dst in [("rr_25d_30d_pct", "rr_25d_30d"),
                         ("iv_30d_atm_pct", "iv_30d_atm")]:
            v = _f(sk.get(src))
            if v is not None: out[dst] = v

    ls = ds.get("long_short_ratio") or {}
    if isinstance(ls, dict):
        v = _f(ls.get("long_short_ratio"))
        if v is not None: out["long_short_ratio"] = v

    return out


# ── Quality detectors (Fix 1, 2, 3, 4, 6) — flag-only, never strip ────
#
# These run after _validate has produced a cleaned summary. They populate
# new audit fields that get_or_build feeds into retry_reasons. None of
# them drop bullets or modify text — that's strictly Venice's job to fix
# on retry, so we don't accidentally remove a feature traders rely on.

_NEUTRAL_DIRECTIVE_RX = re.compile(
    r"\b("
    r"go long|go short|"
    r"long with stop|short with stop|"
    r"long entry|short entry|"
    r"short, ?stop|long, ?stop|"
    r"enter long|enter short|"
    # v3-B (2026-04-25): catch "long/short/bear/bull setup forms|confirmed"
    # patterns — the round-1 rewrite missed these because they're not
    # imperative verbs, they're declarative claims that still imply a
    # tradeable directive on a NEUTRAL bar.
    r"long setup (?:forms|confirmed|forming)|"
    r"short setup (?:forms|confirmed|forming)|"
    r"bull setup (?:forms|confirmed|forming)|"
    r"bear setup (?:forms|confirmed|forming)|"
    # "Long with stop $X" / "Short, stop $X" already covered above; also
    # cover "stop loss at $X" / "stop-loss $X"
    r"stop[ -]?loss"
    r")\b", re.I)

_STOP_PATTERN = re.compile(r"\bstop\s+(?:at\s+|level\s+|@\s*)?\$?([\d,]+(?:\.\d+)?)", re.I)

_LEVEL_CONTEXT_KEYWORDS = (
    "stop", "support", "resistance", "swing low", "swing high",
    "prior low", "prior high", "session low", "session high",
    "range low", "range high", "range top", "range bottom",
    "fib", "pivot", "structural", "wall", "cluster",
    "rejection", "bounce", "high of", "low of",
)

_ABSOLUTE_LANGUAGE_RX = re.compile(
    r"\b(always|never|every time|all of|each time|consistently|"
    r"in every case|invariably)\b", re.I)
_N_DISCLOSURE_RX = re.compile(
    r"(?:n\s*=\s*\d+|\b\d+\s*/\s*\d+\b|"
    r"\bin\s+(?:all\s+)?\d+\s+(?:of|prior|past|previous|similar)\s+"
    r"(?:\d+\s+)?(?:bars?|cases?|times|setups?|precedents?))", re.I)

_PREMORTEM_HEADER_RX = re.compile(
    r"(?:^|\n)\s*(?:PREMORTEM[:\s]+|"
    r"Most likely reason (?:this|the) call (?:is|would be) wrong[:\s]+|"
    r"Why this could be wrong[:\s]+|If wrong[:\s]+|"
    r"This call is wrong if[:\s]+)([^\n]{20,800})", re.I)

_DS_DISAGREEMENT_RX = re.compile(
    r"specialist_agreement\s*:\s*\d+\s*/?\s*\d+|"
    r"\bensemble\b[^.\n]{0,80}(disagree|contradict|overrul|outvot|"
    r"is the outlier|wrong rate|miss rate)|"
    r"\b(NO_TRADE|NO TRADE)\s*(veto|tier)|"
    r"my (blind\s+)?baseline[^.\n]{0,60}(disagree|overrul|overrid)|"
    r"\d+\s*specialists?\s*(agreed|disagreed|fired)|"
    r"\d+\s*/\s*\d+\s*(specialists?|agreed)",
    re.I)

_EDGE_DISAGREEMENT_RX = re.compile(
    r"\bensemble\b|\bspecialist\b|\bNO[ _]?TRADE\b|\bveto\b|"
    r"\bdisagree|\boverrul|\bcontradict|\bdissent|"
    r"\b\d+\s*of\s*\d+\b|"
    r"\bvotes?\s+(up|down)\b",
    re.I)


def _all_substring_indices(s: str, sub: str) -> List[int]:
    out, start = [], 0
    while True:
        i = s.find(sub, start)
        if i == -1:
            break
        out.append(i)
        start = i + 1
    return out


def _detect_neutral_action_directives(actions: List[dict], pred_signal: str) -> List[Dict[str, Any]]:
    """On NEUTRAL DS calls, action text/if_met shouldn't read as 'go long, stop X'.
    It should describe a thesis-flip state ('Bull thesis confirmed — reassess')
    so the trader knows the trigger means re-evaluate, not auto-execute.
    Per user (2026-04-25): actions on NEUTRAL bars ARE legitimate as conditional
    triggers — we just want the language to reflect that."""
    if (pred_signal or "").upper() != "NEUTRAL":
        return []
    out = []
    for b in actions:
        text = (b.get("text") or "") + " " + (b.get("if_met") or "")
        m = _NEUTRAL_DIRECTIVE_RX.search(text)
        if m:
            out.append({
                "text": b.get("text"),
                "if_met": b.get("if_met"),
                "directive": m.group(0),
            })
    return out


def _detect_unsourced_stops(actions: List[dict], ds_response: str) -> List[Dict[str, Any]]:
    """For each action, find 'stop at $X' patterns where X isn't in a DS
    support/resistance/swing context. DS effectively never authors stops, so
    these usually repurpose a narrative price level as a stop without
    endorsement. We FLAG for retry; we DO NOT strip — stripping a stop the
    trader expects would remove a real feature."""
    if not ds_response:
        return []
    low_ds = ds_response.lower()
    out: List[Dict[str, Any]] = []
    for b in actions:
        text = b.get("text") or ""
        for m in _STOP_PATTERN.finditer(text):
            raw = m.group(1).replace(",", "")
            try:
                val = float(raw)
            except ValueError:
                continue
            grounded = False
            for form in _num_string_forms(val):
                for idx in _all_substring_indices(low_ds, form.lower()):
                    window = low_ds[max(0, idx - 70):idx + len(form) + 70]
                    if any(kw in window for kw in _LEVEL_CONTEXT_KEYWORDS):
                        grounded = True
                        break
                if grounded:
                    break
            if not grounded:
                out.append({"text": text, "stop_value": raw})
    return out


_RATIO_PATTERN = re.compile(r"\b(\d+)\s*/\s*(\d+)\b")

def _detect_fabricated_ratios(briefing_text: str, ds_text: str) -> List[str]:
    """Detect 'X/Y' historical-count claims in the briefing whose literal
    string doesn't appear in DS. Round-2 audit caught multiple cases
    (bar20: '3/3' when DS said '2/3'; bar44: '2/2' when DS said '2/4';
    bar15: '3/3' when DS sample was n=10). Flag-only — does not strip;
    Venice gets a retry-feedback prompt to fix.

    Returns list of fabricated ratio strings."""
    if not briefing_text or not ds_text:
        return []
    # Normalize whitespace in DS text for substring lookup
    ds_norm = re.sub(r"\s*/\s*", "/", ds_text)
    # Also tolerate "X out of Y" phrasing
    ds_norm = ds_norm.lower()
    fabricated: List[str] = []
    for m in _RATIO_PATTERN.finditer(briefing_text):
        num, den = m.group(1), m.group(2)
        # Skip trivial like 5/5 dates, page numbers — only check if the
        # denominator is plausibly an n-count: 1-50 range
        try:
            den_int = int(den)
        except ValueError:
            continue
        if not (1 <= den_int <= 50):
            continue
        # Check if "X/Y" or "X out of Y" appears in DS
        ratio_str = f"{num}/{den}"
        out_of_str = f"{num} out of {den}"
        if ratio_str in ds_norm or out_of_str.lower() in ds_norm:
            continue
        # Also accept if Y appears as `n=Y` in DS (the count is sourced
        # even if the X is reframed)
        if f"n={den}" in ds_norm or f"n = {den}" in ds_norm:
            # n is sourced; check if X is consistent with DS-stated count
            # (any X/den is acceptable when the denominator matches)
            continue
        fabricated.append(ratio_str)
    return fabricated


def _detect_unsourced_absolutes(text: str) -> List[str]:
    """Find 'always/never/every time' claims without an n=X / X/Y disclosure
    in the same sentence. Flags the n=4-as-'always' failure mode."""
    out: List[str] = []
    if not text:
        return out
    for sentence in re.split(r"(?<=[.!?])\s+", text):
        m = _ABSOLUTE_LANGUAGE_RX.search(sentence)
        if m and not _N_DISCLOSURE_RX.search(sentence):
            out.append(m.group(0))
    return out


def _extract_premortem_text(ds_response: str) -> Optional[str]:
    if not ds_response:
        return None
    m = _PREMORTEM_HEADER_RX.search(ds_response)
    if not m:
        return None
    return m.group(1).strip()


def _check_premortem_validated(premortem: Optional[str], summary: dict) -> bool:
    """True if at least one watch/action bullet shares a numeric anchor with
    the premortem, OR if the premortem has no numbers worth matching. Catches
    the common 'Venice dropped the entire premortem' pattern; tolerates the
    case where the premortem is purely qualitative."""
    if not premortem:
        return True
    pm_nums = set(_extract_output_numbers_in_text(premortem))
    if not pm_nums:
        return True
    bullets = list(summary.get("watch") or []) + list(summary.get("actions") or [])
    for b in bullets:
        bullet_text = (b.get("text") or "") + " " + (b.get("if_met") or "")
        bullet_nums = set(_extract_output_numbers_in_text(bullet_text))
        if pm_nums & bullet_nums:
            return True
    return False


# ── Surgical fabricated-content removal (final defense, strict mode only)
#
# Used as a last-resort cleanup AFTER Venice's retry has had a chance to
# fix the issues itself. We surgically strip the fabricated parts of an
# action while preserving the conditional-trigger feature traders rely on.
#
# Two transformations:
#   1. Unsourced stop clause: " ... long with stop at $77,530" — the
#      `$77,530` was flagged by _detect_unsourced_stops as not in DS
#      support/resistance context. Strip the ", with stop at $X" tail.
#   2. NEUTRAL directive verb: on NEUTRAL DS bars only, rewrite the
#      "long with stop $X" / "go long" / "short, stop $X" tail into a
#      tone-appropriate state-change clause. The trigger conditions
#      stay verbatim — only the trade-execution language is reframed.
#
# UP/DOWN bars keep their directive language (DS endorsed direction);
# only the unsourced stop clause gets stripped on those.

_TONE_STATE_CHANGE = {
    "bullish": "the bull thesis confirms — reassess directional bias",
    "bearish": "sellers regain control — bearish setup forms",
    "neutral": "regime confirms — stand aside",
}

# Directive verb anchors. Match conservatively — only when followed by a
# stop clause OR appearing as a clear "go long / go short" directive.
# v3-B (2026-04-25): also catch declarative directives like
# "long setup confirmed, stop $X" / "bear setup forms" that the
# imperative-only regex previously missed but which still imply a trade
# action on NEUTRAL bars.
_DIRECTIVE_VERB_FIND = re.compile(
    r"\b("
    r"go\s+long|go\s+short|"
    r"long\s+with\s+stop\b[^.,;]*|"
    r"short\s+with\s+stop\b[^.,;]*|"
    r"long\s+entry\b[^.,;]*|"
    r"short\s+entry\b[^.,;]*|"
    r"enter\s+long\b[^.,;]*|"
    r"enter\s+short\b[^.,;]*|"
    r"long\s*,\s*stop\b[^.,;]*|"
    r"short\s*,\s*stop\b[^.,;]*|"
    r"long\s+setup\s+(?:forms|confirmed|forming)\b[^.,;]*|"
    r"short\s+setup\s+(?:forms|confirmed|forming)\b[^.,;]*|"
    r"bull\s+setup\s+(?:forms|confirmed|forming)\b[^.,;]*|"
    r"bear\s+setup\s+(?:forms|confirmed|forming)\b[^.,;]*"
    r")",
    re.I)

# Bare "with stop at $X" / ", stop $X" clause for stripping on UP/DOWN
# bars where the directive verb (long/short) is legitimate but the stop
# value is unsourced.
_STOP_CLAUSE_REMOVE = re.compile(
    r"\s*[,;—–-]?\s*(?:with\s+)?stop\s+(?:at\s+|level\s+|@\s*|near\s+)?"
    r"\$?[\d,]+(?:\.\d+)?\b",
    re.I)


def _rewrite_action_to_state_change(text: str, tone: str) -> Optional[str]:
    """If `text` contains a directive verb (long with stop / go short / etc),
    cut at the most recent punctuation BEFORE the verb and append a
    tone-appropriate state-change clause. Preserves the trigger sentence
    verbatim. Returns the rewritten text, or None if no directive found."""
    if not text:
        return None
    m = _DIRECTIVE_VERB_FIND.search(text)
    if not m:
        return None
    cut_at = m.start()
    # Walk back to most-recent clause separator
    for i in range(cut_at - 1, -1, -1):
        ch = text[i]
        if ch in (",", ";"):
            cut_at = i
            break
        # em-dash / en-dash / arrow markers (varying widths)
        if text[i:i+1] in ("—", "–"):
            cut_at = i
            break
    prefix = text[:cut_at].rstrip(" ,.;—–")
    state_change = _TONE_STATE_CHANGE.get(tone, _TONE_STATE_CHANGE["neutral"])
    if not prefix:
        return state_change.capitalize() + "."
    return f"{prefix} — {state_change}."


def _strip_unsourced_stop_clause(text: str, unsourced_values: Set[str]) -> Optional[str]:
    """Remove ', stop at $X' clauses where $X is in unsourced_values.
    Used on UP/DOWN bars where the directive verb stays but the fabricated
    stop is removed. Returns rewritten text or None if no change."""
    if not text or not unsourced_values:
        return None
    rewritten = text
    changed = False
    for m in list(_STOP_CLAUSE_REMOVE.finditer(text)):
        # Extract the numeric value
        nm = re.search(r"[\d,]+(?:\.\d+)?", m.group(0))
        if not nm:
            continue
        val_clean = nm.group(0).replace(",", "")
        if val_clean in unsourced_values:
            # Remove this clause from rewritten (find again since prior
            # subs shift indices)
            rewritten = _STOP_CLAUSE_REMOVE.sub(
                lambda mm: "" if val_clean in mm.group(0).replace(",", "") else mm.group(0),
                rewritten, count=1,
            )
            changed = True
    if not changed:
        return None
    rewritten = rewritten.rstrip(" ,.;—–")
    if rewritten and not rewritten.endswith((".", "?", "!")):
        rewritten += "."
    return rewritten


def _ds_has_specialist_disagreement(ds_text: str) -> bool:
    return bool(_DS_DISAGREEMENT_RX.search(ds_text or ""))


def _edge_mentions_disagreement(edge: str) -> bool:
    return bool(_EDGE_DISAGREEMENT_RX.search(edge or ""))


def _norm_conditions(raw: Any, input_canons: Set[str], input_raw: List[str],
                     current_values: Optional[Dict[str, float]] = None) -> Tuple[list, List[str]]:
    """Validate + normalize conditions. Returns (surviving_conditions, drop_notes).

    Preservation policy (2026-04-24, post "? UNKNOWN" incident): we keep a
    condition even if its threshold value isn't verbatim in INPUT, tagging it
    `heuristic: true` so the UI can still render the live value + met/unmet
    against a rule-of-thumb floor (e.g. "BSR < 0.80 persistence"). Only drop
    conditions that would produce an actively WRONG pill — wrong metric
    family, wrong unit, or absurd magnitude.

    Added 2026-04-25 (Fix 5): degenerate-near-current detector — if
    `current_values` is supplied and condition.value is within the per-metric
    tolerance of the live reading, the condition fires on rounding noise and
    is dropped so the trader doesn't get a meaningless trigger pill."""
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
        # Whitelist enforcement — the UI metric() lookup can only resolve
        # values for metrics in _VALID_METRICS. Anything else would render as
        # an unresolved "source unavailable" pill, which is worse than no
        # pill. Drop so the frontend's family-inference fallback can
        # synthesize a proper pill from the bullet prose instead.
        if metric not in _VALID_METRICS:
            notes.append(f"metric_not_in_whitelist:{metric}")
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
        # Fix 5: reject thresholds set at the current live reading. These
        # fire on rounding noise rather than a real regime change. The
        # bullet's narrative is preserved (only the misleading pill drops).
        if current_values and _is_degenerate_near_current(metric, op, val, current_values):
            notes.append(f"degenerate_near_current:{metric}{op}{val} (current={current_values.get(metric)})")
            continue
        # Reject `==` on a continuous float metric where the value isn't a
        # natural regime boundary. `whale_buy == 0.63` ticks false the
        # moment the live value moves to 0.70 — brittle and useless. Let
        # `==` through only for:
        #   * ratio metrics at their regime boundary (taker_ratio/bsr at 1.0,
        #     long_short_ratio at 1.0) — these have meaningful semantics
        #   * integer-like values (price rounds/levels, RSI rounded to int)
        # For everything else convert to a useful `>=` / `<=` equivalent
        # would require knowing direction intent, so just drop.
        if op == "==" and val != 0:
            _ratio_boundary = metric in ("taker_ratio", "bsr", "long_short_ratio") and abs(val - 1.0) < 0.001
            _integer_level = (abs(val - round(val)) < 0.001) and metric in ("price", "rsi")
            if not (_ratio_boundary or _integer_level):
                notes.append(f"fragile_equality_on_float:{metric}=={val}")
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


def _validate(obj: Any, input_text: str, strict: bool = False,
              current_values: Optional[Dict[str, float]] = None,
              pred: Optional[dict] = None) -> Tuple[Optional[dict], Dict[str, Any]]:
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

    Added 2026-04-25:
      * `current_values`: live metric readings; lets _norm_conditions drop
        thresholds that sit on the current value (degenerate-near-current).
      * `pred`: DS prediction record; enables flag-only quality detectors
        (NEUTRAL directives, fabricated stops, premortem coverage,
        specialist-disagreement surfacing, n-disclosure on absolutes).
    """
    audit = {"dropped_values": [], "dropped_quotes": [],
             "family_mismatch_conditions_cleared": [],
             "fabricated_text_numbers": [], "bullets_dropped": [],
             "bullets_rescued": [], "missing_quotes_bullets": [],
             "heuristic_thresholds_kept": [],
             # New (2026-04-25) — flag-only retry signals:
             "neutral_action_directives": [],
             "unsourced_action_stops":   [],
             "unsourced_absolutes":      [],
             "premortem_dropped":         False,
             "specialist_disagreement_unsurfaced": False}

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
            conds, drop_notes = _norm_conditions(b.get("conditions"), input_canons, input_raw,
                                                  current_values=current_values)
            # v3-C (2026-04-25): when a condition is dropped as
            # degenerate-near-current, also strip its numeric from the
            # bullet's prose. Otherwise the trader sees "BSR > 0.5689"
            # in text while the structured pill is gone — prose/JSON
            # desync that the round-2 audit flagged on 15+ bars.
            degen_near_current_vals: List[str] = []
            for n in drop_notes:
                if n.startswith("heuristic_threshold_kept:"):
                    audit["heuristic_thresholds_kept"].append(n)
                else:
                    audit["dropped_values"].append(n)
                if n.startswith("degenerate_near_current:"):
                    # Note shape: "degenerate_near_current:<metric><op><val> (current=...)"
                    # extract the threshold value before the " (current=..."
                    head = n.split(" (current=", 1)[0]
                    body = head[len("degenerate_near_current:"):]
                    m = re.search(r"(?:>=|<=|==|!=|>|<)(-?[\d.]+)$", body)
                    if m:
                        degen_near_current_vals.append(m.group(1))
            if strict and degen_near_current_vals:
                stripped_text = _rescue_text(text,   degen_near_current_vals)
                stripped_ifm  = _rescue_text(if_met, degen_near_current_vals) if if_met else if_met
                if stripped_text != text or stripped_ifm != if_met:
                    audit.setdefault("degenerate_near_current_prose_stripped", []).append(
                        {"text_before": text, "text_after": stripped_text,
                         "ifmet_before": if_met, "ifmet_after": stripped_ifm,
                         "values": degen_near_current_vals}
                    )
                    text, if_met = stripped_text, stripped_ifm

            # Text fabrication: flag on pass 1, rescue or soft-keep on pass 2.
            #
            # Asymmetry-fix (2026-04-25, post 50-bar audit): conditions[]
            # already keeps heuristic-threshold values with `heuristic: true`
            # — the trader sees a dashed-border pill. The strict pass used
            # to NUKE entire bullets when the SAME kind of heuristic numbers
            # (1.5 BTC, 0.9 BSR, $1.2M cluster) appeared in PROSE,
            # destroying legitimate trader heuristics. Audit confirmed
            # 18/19 strict drops were small heuristic values (<1000),
            # 0/19 were price-level anchors. New policy:
            #   * Try to rescue (strip clauses with bad numbers) as before.
            #   * If still-bad numbers are PRICE-LIKE (>=1000): drop. A
            #     wrong stop level is a real anchor risk to the trader.
            #   * If still-bad numbers are SMALL heuristics (<1000): KEEP
            #     the bullet, set heuristic_text=True. The UI flags it as
            #     rule-of-thumb prose, mirroring the heuristic-condition
            #     dashed-pill affordance.
            heuristic_text_flag = False
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
                    still_bad = still_bad_text + still_bad_ifmet
                    if still_bad:
                        # _bullet_text_numeric_coherence returns strings; cast
                        # to float so abs() works. Tokens that fail the cast
                        # are non-price (already filtered by the extractor).
                        def _abs_or_zero(s):
                            try: return abs(float(s))
                            except (TypeError, ValueError): return 0.0
                        price_like = [s for s in still_bad if _abs_or_zero(s) >= 1000.0]
                        if price_like:
                            # Real anchor risk — drop rather than ship a wrong
                            # price level the trader might use as a stop.
                            audit["bullets_dropped"].append({"text": text, "why": f"unrescuable_fabrication:{all_bad}"})
                            continue
                        # All remaining fabs are <1000 — heuristic regime
                        # boundaries. Keep the original (un-rescued) text so
                        # the trader sees full reasoning; flag it heuristic
                        # so the UI can render a "rule of thumb" badge.
                        heuristic_text_flag = True
                        audit["bullets_rescued"].append({
                            "before": text, "after": text,
                            "kind": "heuristic_text_kept",
                            "nums_kept": still_bad,
                        })
                    else:
                        # Rescue stripped all fabs cleanly — use cleaned prose.
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
            #
            # Soft-keep (2026-04-25, post 50-bar audit): the strict pass
            # used to drop bullets here when neither Venice-supplied nor
            # auto-derived quotes existed. Audit found these are nearly
            # always pure-narrative bullets like "If bid imbalance flips
            # positive above 0%" — qualitative, no fabrication risk, and
            # the UI already renders quote-less bullets as narrative cards.
            # Strict-drop here was gratuitous; we now flag and keep.
            if not quotes:
                quotes = _auto_derive_quotes(text + " " + if_met, input_text, input_canons, input_raw)
                if quotes:
                    audit["missing_quotes_bullets"].append({"text": text, "auto_derived": quotes})
                else:
                    audit["missing_quotes_bullets"].append({"text": text, "auto_derived": []})

            out.append({
                "tone":           tone,
                "text":           text.strip(),
                "conditions":     conds,
                "if_met":         if_met or None,
                "sources":        sources,
                "source_quotes":  quotes,
                # heuristic_text=True signals the UI to render a small
                # "rule of thumb" badge on the bullet — same affordance as
                # the dashed border on heuristic condition pills, just at
                # bullet-level granularity for prose containing
                # heuristic threshold numbers (e.g. "BSR < 0.9").
                "heuristic_text": heuristic_text_flag,
            })
            if len(out) >= cap:
                break
        return out

    watch   = _norm_bullets("watch",   5)
    actions = _norm_bullets("actions", 4)

    # Strict-mode final defense (2026-04-25, user-approved): surgically
    # strip fabricated parts from actions[]. The trigger conditions stay
    # verbatim — only the lying bits get rewritten:
    #   * NEUTRAL DS bars: replace directive verbs (long with stop $X /
    #     go short / etc) with a tone-appropriate state-change clause.
    #     The conditional trigger remains; only the trade-execution
    #     language is reframed.
    #   * UP/DOWN DS bars: directive verbs stay (DS endorsed direction),
    #     but unsourced stop clauses (where the stop value isn't in DS
    #     support/resistance context) are stripped.
    if strict and pred is not None:
        ds_response = "\n".join([
            str(pred.get("reasoning") or ""),
            str(pred.get("narrative") or ""),
            str(pred.get("free_observation") or ""),
        ])
        pred_signal = str(pred.get("signal") or "").upper()
        unsourced_stop_vals: Set[str] = {
            str(u.get("stop_value", ""))
            for u in _detect_unsourced_stops(actions, ds_response)
        }
        for b in actions:
            tone = b.get("tone", "neutral")
            text = b.get("text") or ""
            if_met = b.get("if_met") or ""
            if pred_signal == "NEUTRAL":
                # NEUTRAL: rewrite directive language to state-change.
                new_text = _rewrite_action_to_state_change(text, tone)
                if new_text and new_text != text:
                    audit.setdefault("actions_rewritten_state_change", []).append(
                        {"before": text, "after": new_text}
                    )
                    b["text"] = new_text
                    text = new_text
                # if_met: replace any directive entirely with the
                # tone state-change phrase.
                if if_met and _DIRECTIVE_VERB_FIND.search(if_met):
                    new_if = _TONE_STATE_CHANGE.get(tone,
                                _TONE_STATE_CHANGE["neutral"]).capitalize()
                    audit.setdefault("if_met_rewritten_state_change", []).append(
                        {"before": if_met, "after": new_if}
                    )
                    b["if_met"] = new_if
                    if_met = new_if
                # v3-A (2026-04-25): even after NEUTRAL rewrite, an
                # unsourced stop clause may survive in if_met or text
                # ("Long setup confirmed, stop $77,530"). Round-2 audit
                # showed if_met retained `stop $X` on bar27/44/46 even
                # after the directive rewrite. Strip those here too.
                if unsourced_stop_vals:
                    new_text2 = _strip_unsourced_stop_clause(text, unsourced_stop_vals)
                    if new_text2 and new_text2 != text:
                        audit.setdefault("unsourced_stops_stripped", []).append(
                            {"before": text, "after": new_text2, "after_neutral_rewrite": True}
                        )
                        b["text"] = new_text2
                    new_if2 = _strip_unsourced_stop_clause(if_met, unsourced_stop_vals)
                    if new_if2 and new_if2 != if_met:
                        audit.setdefault("unsourced_stops_stripped", []).append(
                            {"before": if_met, "after": new_if2, "field": "if_met",
                             "after_neutral_rewrite": True}
                        )
                        b["if_met"] = new_if2
            else:
                # UP/DOWN: directive verbs stay (DS endorsed direction);
                # but unsourced stop clauses still get stripped.
                if unsourced_stop_vals:
                    new_text = _strip_unsourced_stop_clause(text, unsourced_stop_vals)
                    if new_text and new_text != text:
                        audit.setdefault("unsourced_stops_stripped", []).append(
                            {"before": text, "after": new_text}
                        )
                        b["text"] = new_text
                    if if_met:
                        new_if = _strip_unsourced_stop_clause(if_met, unsourced_stop_vals)
                        if new_if and new_if != if_met:
                            audit.setdefault("unsourced_stops_stripped", []).append(
                                {"before": if_met, "after": new_if, "field": "if_met"}
                            )
                            b["if_met"] = new_if

    final = {
        "edge":    edge.strip(),
        "watch":   watch,
        "actions": actions,
    }

    # ── Flag-only quality detectors (2026-04-25, no auto-strip) ─────
    # These run AFTER the cleaned summary is built. They never modify
    # bullets — only populate audit fields the orchestrator turns into
    # retry feedback so Venice itself fixes the issues. Per user
    # (2026-04-25): "be careful not to overshoot and remove actual
    # features for our live traders."
    if pred is not None:
        ds_response = "\n".join([
            str(pred.get("reasoning") or ""),
            str(pred.get("narrative") or ""),
            str(pred.get("free_observation") or ""),
        ])
        pred_signal = str(pred.get("signal") or "").upper()

        # Fix 1 (reframed): NEUTRAL bars allow conditional triggers as
        # actions, but if_met / text shouldn't read like a directive
        # ("go long, stop $X"). Should describe state change so the
        # trader knows to reassess, not auto-execute.
        nd = _detect_neutral_action_directives(actions, pred_signal)
        if nd:
            audit["neutral_action_directives"] = nd

        # Fix 2: stop levels in actions must reference DS-cited
        # support/resistance/swing context. Flag (don't strip — a stop
        # the trader expects is a real feature).
        us = _detect_unsourced_stops(actions, ds_response)
        if us:
            audit["unsourced_action_stops"] = us

        # Fix 3: premortem coverage — at least one bullet should share
        # a numeric anchor with DS's PREMORTEM paragraph if DS supplied
        # one with numbers. Catches "Venice dropped the premortem".
        premortem = _extract_premortem_text(ds_response)
        if premortem and not _check_premortem_validated(premortem, final):
            audit["premortem_dropped"] = True
            audit["premortem_text"] = premortem[:200]

        # Fix 4: specialist disagreement must be visible in edge if DS
        # explicitly flagged ensemble dissent / NO_TRADE veto / 2-vs-1
        # specialist split. The "this is a contrarian-to-ensemble
        # call" framing is the most-load-bearing meta-signal.
        if _ds_has_specialist_disagreement(ds_response):
            if not _edge_mentions_disagreement(final["edge"]):
                audit["specialist_disagreement_unsurfaced"] = True

    # Fix 6: forbid "always/never/every time" without a sample-size
    # disclosure (n=X, X/Y) in the same sentence. Catches the
    # n=4-as-"always" failure mode regardless of pred availability.
    all_text_blob = final["edge"] + "\n" + "\n".join(
        [(b.get("text") or "") + " " + (b.get("if_met") or "")
         for b in (final["watch"] + final["actions"])]
    )
    abs_hits = _detect_unsourced_absolutes(all_text_blob)
    if abs_hits:
        audit["unsourced_absolutes"] = abs_hits

    # v3-E (2026-04-25): detect fabricated n-counts (X/Y not in DS).
    # Round-2 audit caught 5+ bars with invented historical ratios
    # (bar20 "3/3" vs DS "2/3"; bar44 "2/2" vs DS "2/4"; etc).
    if pred is not None:
        ds_full = "\n".join([
            str(pred.get("reasoning") or ""),
            str(pred.get("narrative") or ""),
            str(pred.get("free_observation") or ""),
        ])
        fab_ratios = _detect_fabricated_ratios(all_text_blob, ds_full)
        if fab_ratios:
            audit["fabricated_ratios"] = fab_ratios

    return final, audit


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

    # Max age for a cached Venice output: 15 minutes (3 bars). Beyond that we
    # regenerate — covers the edge case where an engine stall leaves an old
    # window's summary lingering and get_cached would serve stale data.
    _TTL_S = 900
    cached = _cache.get(window_start_time)
    if cached and (time.time() - (cached.get("generated_at") or 0)) < _TTL_S:
        return cached

    lock = _locks.setdefault(window_start_time, asyncio.Lock())
    async with lock:
        cached = _cache.get(window_start_time)
        if cached and (time.time() - (cached.get("generated_at") or 0)) < _TTL_S:
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

        # Build live metric snapshot once — used by Fix 5 (degenerate-near-
        # current detector) inside _norm_conditions to drop pills whose
        # threshold sits on the live value.
        current_values = _extract_current_metric_values(backend_snapshot)

        # PASS 1: lenient — drops fabricated condition values, flags text
        # fabrication + missing source_quotes but keeps the bullets so we don't
        # gut the briefing on the first try. We then decide whether to retry.
        cleaned, audit = _validate(obj, user_prompt, strict=False,
                                    current_values=current_values, pred=pred)
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

        # ── New retry triggers (2026-04-25, post 50-bar audit) ──────────
        # Fix 1: NEUTRAL bars allow conditional triggers as actions, but
        # the language must be state-change ("thesis flips bullish") not
        # directive ("go long, stop $X"). The trader needs to know the
        # trigger means reassess, not auto-execute.
        if audit.get("neutral_action_directives"):
            samples = [d.get("directive", "") for d in audit["neutral_action_directives"][:3]]
            retry_reasons.append(
                "NEUTRAL DS but action if_met / text reads as a trade directive "
                f"({samples}); rephrase as state change ('Bull thesis confirmed — reassess', "
                "'Sellers regain control')."
            )
        # Fix 2: stops must reference DS-cited support/resistance/swing
        # context. We don't strip stops (real feature), but flag.
        if audit.get("unsourced_action_stops"):
            samples = [u.get("stop_value", "") for u in audit["unsourced_action_stops"][:3]]
            retry_reasons.append(
                f"action stop levels {samples} are not anchored to a DS-cited "
                "support/resistance/swing/wall — either pick a level DS named "
                "as such, or omit the stop clause."
            )
        # Fix 3: premortem coverage.
        if audit.get("premortem_dropped"):
            pm_quote = audit.get("premortem_text", "")[:160]
            retry_reasons.append(
                f"DS PREMORTEM ('{pm_quote}…') dropped — at least one watch bullet "
                "must validate the premortem trigger (share its numeric thresholds)."
            )
        # Fix 4: specialist disagreement surfacing.
        if audit.get("specialist_disagreement_unsurfaced"):
            retry_reasons.append(
                "DS flagged specialist disagreement / NO_TRADE veto / ensemble dissent "
                "but edge text doesn't surface it — add one clause naming the "
                "ensemble vote, the agreement count, or the veto."
            )
        # Fix 6: 'always/never/every time' without n disclosure.
        if audit.get("unsourced_absolutes"):
            retry_reasons.append(
                f"absolute claim(s) {audit['unsourced_absolutes'][:3]} used without "
                "explicit sample size (n=X or X/Y in the same sentence) — "
                "either add the count or weaken the language."
            )
        # v3-E: fabricated X/Y historical-count claims.
        if audit.get("fabricated_ratios"):
            retry_reasons.append(
                f"historical-count claim(s) {audit['fabricated_ratios'][:3]} "
                "do not appear verbatim in DS — either use the exact ratio DS "
                "wrote (e.g., '2/3' not '3/3') or drop the count."
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
                + "* COMPLETENESS: do NOT silently drop major signals cited in the INPUT.\n"
                + "* THRESHOLD ≠ CURRENT: a watch/action condition's threshold must be a "
                + "MEANINGFUL distance from the live reading. Setting `OI > 33,856` when current "
                + "OI is 33,856 fires on rounding noise — pick a threshold the move would "
                + "actually have to cross to mean something.\n"
                + "* NEUTRAL ACTIONS: if DS signal is NEUTRAL, action `if_met` should describe a "
                + "STATE CHANGE the trader must reassess from ('Bull thesis confirmed — reassess', "
                + "'Sellers regain control — bearish setup forms'), NOT a directive ('Go long, "
                + "stop $X'). The action is a conditional trigger, not an order.\n"
                + "* ACTION STOPS: a stop level in an action must reference a price DS itself "
                + "named as support/resistance/swing/wall/cluster. If DS gave no such level, "
                + "OMIT the stop clause — do not repurpose a narrative price.\n"
                + "* PREMORTEM COVERAGE: if DS includes a PREMORTEM ('most likely reason this "
                + "call is wrong: ...'), at least one watch bullet MUST validate the premortem "
                + "trigger, sharing its specific numeric thresholds verbatim.\n"
                + "* SPECIALIST DISAGREEMENT: if DS surfaces SPECIALIST_AGREEMENT counts, ensemble "
                + "dissent, or a NO_TRADE veto, the edge sentence MUST mention it (one clause is "
                + "enough — 'ensemble UP 87% but Binance expert vetoes', '2 of 3 specialists "
                + "NEUTRAL'). The contrarian framing is the most load-bearing meta-signal.\n"
                + "* N DISCLOSURE: do not write 'always', 'never', 'every time', etc. without an "
                + "explicit n=X or X/Y count from DS in the same sentence. n=3 historical analogs "
                + "are NOT 'always' — say '3/3 prior bars'.\n\n"
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
                cleaned2, audit2 = _validate(obj2, user_prompt, strict=False,
                                              current_values=current_values, pred=pred)
                if cleaned2 and (len(cleaned2["watch"]) + len(cleaned2["actions"])) >= 1:
                    completeness2 = _completeness_score(cleaned2, user_prompt)
                    fab1 = len(audit["fabricated_text_numbers"]) + len(audit["dropped_values"])
                    fab2 = len(audit2["fabricated_text_numbers"]) + len(audit2["dropped_values"])
                    bullets1 = len(cleaned["watch"]) + len(cleaned["actions"])
                    bullets2 = len(cleaned2["watch"]) + len(cleaned2["actions"])
                    # 2026-04-25: composite "quality issues" score covers the
                    # new flag-only signals so a retry that fixes a directive
                    # / fabricated stop / dropped premortem / hidden ensemble
                    # disagreement / unsourced "always" claim gets accepted
                    # even if fabrications + coverage are unchanged.
                    def _qual_issues(a: dict) -> int:
                        return (
                            len(a.get("neutral_action_directives") or []) +
                            len(a.get("unsourced_action_stops") or []) +
                            len(a.get("unsourced_absolutes") or []) +
                            len(a.get("fabricated_ratios") or []) +
                            (1 if a.get("premortem_dropped") else 0) +
                            (1 if a.get("specialist_disagreement_unsurfaced") else 0)
                        )
                    qual1 = _qual_issues(audit)
                    qual2 = _qual_issues(audit2)
                    # Accept retry if it's strictly better along any axis
                    # without regressing on the others:
                    #   (a) fewer fabrications, same-or-more bullets
                    #   (b) same fabrications, better coverage
                    #   (c) same fabrications + coverage, more bullets (denser)
                    #   (d) same fabrications + coverage + bullets, fewer
                    #       quality issues (Fix 1/2/3/4/6)
                    # Never accept a retry that ships FEWER bullets than pass 1
                    # or MORE fabrications.
                    if fab2 <= fab1 and bullets2 >= bullets1 and qual2 <= qual1:
                        if (fab2 < fab1
                            or completeness2["ratio"] > completeness["ratio"]
                            or bullets2 > bullets1
                            or qual2 < qual1):
                            final_cleaned, final_audit = cleaned2, audit2
                            completeness = completeness2
            except Exception as exc:
                logger.warning("trader_summary retry failed for bar %s: %s", window_start_time, exc)

        # PASS 2 (strict): apply rescue-or-drop to whichever pass we took. A
        # fabricated number must NEVER reach the client — but if we can strip
        # the offending clause and keep the rest of the bullet, we do that
        # rather than nuking the whole bullet.
        lenient_cleaned = final_cleaned
        strict_cleaned, final_audit_strict = _validate(
            {"edge": final_cleaned["edge"],
             "watch":   [{**b, "conditions": b.get("conditions", []),
                          "source_quotes": b.get("source_quotes", [])}
                         for b in final_cleaned["watch"]],
             "actions": [{**b, "conditions": b.get("conditions", []),
                          "source_quotes": b.get("source_quotes", [])}
                         for b in final_cleaned["actions"]]},
            user_prompt, strict=True,
            current_values=current_values, pred=pred,
        )
        if not strict_cleaned:
            # Strict pass stripped the edge to empty or all bullets dropped.
            # Fall back to the lenient result BUT hard-drop any bullet whose
            # text still contains fabricated numbers — the earlier version
            # kept those bullets "because they're flagged", but the UI
            # doesn't render the audit flags, so the fabricated numbers
            # reached the trader unmarked. Safer: drop the flagged bullets
            # and rescue the edge text.
            logger.warning(
                "trader_summary strict-pass emptied briefing for bar %s — falling back to lenient with fabrication drop",
                window_start_time,
            )
            input_canons, input_raw = _extract_input_number_set(user_prompt)
            # Rescue edge text of fabricated numbers
            rescued_edge = lenient_cleaned["edge"]
            edge_bad = _bullet_text_numeric_coherence(rescued_edge, input_canons, input_raw)
            if edge_bad:
                rescued_edge = _rescue_text(rescued_edge, edge_bad)
            def _bullet_is_clean(b):
                txt = b.get("text") or ""
                ifm = b.get("if_met") or ""
                return (not _bullet_text_numeric_coherence(txt, input_canons, input_raw)
                        and not _bullet_text_numeric_coherence(ifm, input_canons, input_raw))
            final_cleaned = {
                "edge":    rescued_edge or lenient_cleaned["edge"],
                "watch":   [b for b in lenient_cleaned["watch"]   if _bullet_is_clean(b)],
                "actions": [b for b in lenient_cleaned["actions"] if _bullet_is_clean(b)],
            }
            final_audit_strict = {"strict_pass_fallback_to_lenient_with_drop": True}
        else:
            final_cleaned = strict_cleaned
        # Merge audit trails. Lists concat; bools OR-merge so we never
        # lose a flag that fired on either pass; strings prefer the
        # lenient pass (more context). Other scalars: prefer existing.
        merged_audit = dict(final_audit)
        for k, v in final_audit_strict.items():
            if isinstance(v, list):
                merged_audit.setdefault(k, []).extend(v)
            elif isinstance(v, bool):
                merged_audit[k] = merged_audit.get(k, False) or v
            elif isinstance(v, str) and not merged_audit.get(k):
                merged_audit[k] = v

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
