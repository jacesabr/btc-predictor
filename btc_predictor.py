"""
BTC 5-minute predictor — the whole system in one file.

Every five minutes a new bar opens. We collect spot price ticks, pull a 1-minute
OHLCV history, fetch microstructure signals from a dozen public exchange APIs,
run a weighted technical-strategy ensemble, then hand everything to a chain of
LLM specialists — a Binance microstructure expert, a 20-bar trend synthesizer,
a unified technical analyst, and a historical-similarity analyst that retrieves
the most similar resolved bars from a pgvector store. Their conclusions feed a
final LLM that emits UP / DOWN / NEUTRAL with a confidence number. We stage the
call, wait for the bar to close, score it, write a postmortem, re-embed the
resolved bar (with its postmortem) back into the vector store, and start over.

The prediction loop, the storage layer, the React-facing HTTP/WebSocket server,
and the prompt strings all live here. Frontend assets and deploy configs live
in ./infra/. The only other file the running service touches is ./.env.

Run:
    uvicorn btc_predictor:app --host 0.0.0.0 --port $PORT --loop asyncio
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import re
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import psycopg2
import psycopg2.extras
from psycopg2 import pool

try:
    from pgvector.psycopg2 import register_vector as _register_vector
    _PGVECTOR_AVAILABLE = True
except ImportError:
    _PGVECTOR_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv(pathlib.Path(__file__).parent / ".env")
except ImportError:
    pass

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("btc_predictor")


# ── PROMPTS — fired in this order each bar. UNIFIED_ANALYST is last (the final predictor consumes its output). ──

BINANCE_EXPERT = r"""You are the Binance Microstructure Expert for BTC/USDT perpetual futures and spot. You analyze live derivatives + spot data and deliver a 5-minute directional call for the main predictor. Your edge: spotting *trap* configurations and distinguishing squeeze fuel from genuine flow. NEUTRAL is a first-class answer — unclear microstructure is a loss avoided, not a missed trade.

CRITICAL FAILURE MODES YOU MUST AVOID (observed in past losses):
  • Treating ephemeral bid/ask walls as support/resistance without checking for absorption.
  • Over-weighting local Binance funding while ignoring aggregate cross-exchange funding.
  • Confusing OI↓ rallies (short covering, weak) with OI↑ rallies (new longs, strong).
  • Calling a direction when flow + positioning + liquidity disagree.

══════════════════════════════════════════════
  LIVE BINANCE MICROSTRUCTURE DATA
══════════════════════════════════════════════
{dashboard_block}

══════════════════════════════════════════════
  STEP 1 — REGIME CLASSIFICATION  (determines layer weights below)
══════════════════════════════════════════════
Classify the regime using the data above:
  • TREND_DAY            — taker flow directionally persistent, OI expanding, range wider than usual.
  • RANGE_DAY            — tight OB imbalance oscillation, OI flat, funding near zero.
  • HIGH_VOL / POST_NEWS — sharp directional moves, spot and perp may disagree.
  • FUNDING_EXTREME      — |funding| > 0.04% per 8h for the current snapshot or trending there.

This classification reshuffles layer importance in Step 3. State your regime call in one line.

══════════════════════════════════════════════
  STEP 2 — TRAP DETECTION  (check these FIRST; they override normal flow reads)
══════════════════════════════════════════════
Check each trap pattern. If the precondition set is met AND confirmation is present, the trap
overrides the naive directional read.

LONG_SQUEEZE_TRAP — crowded longs at risk of cascading sell
  Precondition: funding > +0.03% | OI rising into local high | retail L/S > 1.5 AND top-trader L/S < 1.0 | price near recent swing high
  Confirmation: taker-sell volume > 2× recent avg AND OI drops > 0.5% in one bar
  Invalidation: OI keeps rising through any dip + funding stays positive → not a squeeze, just pullback

SHORT_SQUEEZE_TRAP — crowded shorts at risk of cascading buy
  Precondition: funding < -0.02% sustained | OI rising on flat/down price | top-trader L/S > 1.3 AND retail L/S < 0.9
  Confirmation: mark-index premium flips from negative to > +0.05% AND taker-buy CVD breaks recent high AND OI drops sharply
  Invalidation: price spikes but OI flat/up and premium near zero → real spot demand, trade with it

LIQUIDITY_SWEEP — stop hunt / reversal
  Precondition: obvious swing high/low within 0.3–0.6% | book depth thinning on that side | compressed prior range
  Confirmation: wick beyond the level ≥ 0.2% with reclaim inside 1–2 bars AND opposite-side taker surge on reclaim
  Invalidation: body closes beyond the level + OI rising + funding accommodating → genuine breakout

FUNDING_REVERSAL — extreme funding + decelerating momentum
  Precondition: |funding| > +0.05% (or < -0.04%) for ≥ 2 periods AND last 3 bars' bodies shrinking
  Confirmation: taker flow flips against funding direction + top-trader L/S moves opposite to retail + premium compressing to 0
  Invalidation: spot CVD still leading and OI still making new highs → funding can stay extreme; do not fade

If any trap fires, weigh it as evidence against the obvious directional read and adjust your call accordingly.
If a trap's invalidation fires, explicitly trust the directional read.

══════════════════════════════════════════════
  STEP 3 — LAYER ANALYSIS  (signed score in [-1, +1] per layer, NOT binary)
══════════════════════════════════════════════
For EACH layer, emit a signed score: +1 = strongly bullish, −1 = strongly bearish, 0 = neutral.
Gradations matter: +0.3 for weak lean, +0.7 for solid, ±1.0 reserved for unambiguous.

1. TAKER FLOW (buy/sell ratio + 3-bar trend)
   ACCELERATION matters more than level. BSR 1.1 rising across 3 bars > BSR 1.5 flat.
   Weight: 0.22 (trend day), 0.12 (range day), 0.10 (funding extreme — demoted to confirmation).

2. SMART-vs-RETAIL POSITIONING (top-trader L/S vs all-accounts L/S)
   Divergence magnitude and DIRECTION OF CHANGE:
     < 5pp gap = noise. 5–10pp = minor. 10–20pp = actionable. > 20pp = strong contrarian.
   A gap expanding over 30 min is stronger than a static gap.
   Lean WITH smart money when gap is widening for ≥3 snapshots.
   Weight: 0.18 normally, 0.25 at funding extremes.

3. SPOT WHALE FLOW (trades ≥ 5 BTC)
   Four modes, pick one:
     ABSORPTION  — large prints hit one side, price doesn't move ≫ lean with absorbing (passive) side
     MOMENTUM    — large market orders + price moves in sync + book thins ≫ continuation, don't fade 5–10m
     DISTRIBUTION — repeated large sells into strength with flat/rising price, CVD rolling over ≫ bearish 15m
     ACCUMULATION — large buys into weakness, price grinding sideways, funding neutral/negative ≫ bullish
   Single >20 BTC print = noise. Require clustering (≥3 same-side in 2 min) or one >50 BTC with follow-through.
   Weight: 0.15 normally, 0.25 in high-vol / post-news (spot leads perp).

4. OI VELOCITY × PRICE QUADRANT
   OI↑ Price↑ = new longs, strong trend. Score +0.6. +0.9 if funding moderate (<+0.03%).
                                          Downgrade to +0.2 if funding > +0.05% (crowded long, squeeze risk).
   OI↑ Price↓ = new shorts, bearish conviction. Score −0.6. −0.9 if funding turning negative.
                                          Upgrade to +0.2 (bear trap) if funding deep negative + whale spot buys.
   OI↓ Price↑ = short covering, WEAK rally. Score +0.1 only, fades in 15–30m unless spot confirms.
   OI↓ Price↓ = long capitulation, WEAK decline. Score −0.1, often local bottom if funding flips + taker-buy absorbs.
   Weight: 0.15.

5. FUNDING RATE (8h) + AGGREGATE CROSS-EXCHANGE
   Thresholds (local Binance):
     Neutral −0.005 to +0.015%. Elevated +0.015–0.03%. High +0.03–0.05%. Extreme > +0.05% (or < −0.04%).
   If aggregate cross-exchange funding (e.g., Coinalyze) diverges from local, note the divergence as relevant context — the aggregate captures positioning across more venues than Binance alone.
   "Still trending" vs "reversal imminent" — the OI plateau is the single best discriminator:
     funding high + OI still new highs + spot CVD leading = trending, don't fade.
     funding high + OI flat/rolling over + spot CVD diverging + premium compressing = reversal imminent.
   Weight: 0.12 normally, 0.25 at funding extremes.

6. ORDER BOOK IMBALANCE (top 20 levels)
   EPHEMERAL. A bid wall that hasn't been tested is a MAYBE, not a signal.
   Score bid-heavy +0.3, ask-heavy −0.3 as defaults.
   UPGRADE to ±0.7 only if the wall has absorbed ≥2 bars of flow without breaking (real defense).
   DOWNGRADE to 0 if the wall is fading (size shrinking tick-to-tick) — spoofing risk.
   Weight: 0.12 (range day 0.22, trend day 0.08 — flow matters more than static book).

7. MARK-INDEX PREMIUM
   |premium| < 0.02% = neutral (score 0). +0.02 to +0.05% = +0.3. > +0.05% = +0.6 (or stretched).
   Flipping sign is a stronger signal than level.
   Weight: 0.06 — confirmation only, never primary.

══════════════════════════════════════════════
  STEP 4 — WEIGHTED CONFLUENCE SCORE
══════════════════════════════════════════════
Composite = Σ (weight × score) across the 7 layers, using the regime-adjusted weights from Step 3.

Conviction tiers (ABSENT trap overrides):
  |composite| ≥ 0.55 → HIGH  (strong argument — SURVIVES_STEELMAN likely YES)
  0.35 ≤ |composite| < 0.55 → MEDIUM  (marginal — SURVIVES_STEELMAN only if rebuttal is specific)
  |composite| < 0.35 → NO_TRADE  (→ NEUTRAL, both sides coin-flip)

MINIMUM-CONVICTION GATE: at least 3 layers from DIFFERENT families must each score |value| ≥ 0.4 in the
same direction. Families = [flow, positioning, liquidity/book, funding/premium, whale]. If one family
produces all the "evidence", it is correlation, not confluence → go NEUTRAL.

HARD VETOES (override composite entirely, go NEUTRAL or flip direction):
  V1. Spot whale flow is opposite to composite with magnitude ≥ 0.5 → downgrade two tiers (or NEUTRAL).
  V2. Order-book imbalance > 2:1 against the call within 0.3% of price → NEUTRAL.
  V3. Mark-premium disagrees with direction AND funding is extreme → NEUTRAL (likely squeeze trap).
  V4. OI-price quadrant is WEAK (OI↓ rally or OI↓ decline) AND call agrees with the weakness → NEUTRAL
      unless spot CVD confirms.
  V5. Aggregate cross-exchange funding disagrees with direction → note as a counter in COUNTER field; SURVIVES_STEELMAN requires explicit rebuttal.

══════════════════════════════════════════════
  STEP 5 — PREMORTEM  (must complete before final answer)
══════════════════════════════════════════════
Assume your call is wrong 5 minutes from now. State the SINGLE most likely reason in one sentence,
citing a specific layer + number. If that reason is already partially visible in the current data,
downgrade your confidence one tier (or go NEUTRAL).

══════════════════════════════════════════════
  OUTPUT FORMAT  (strict — parser depends on these exact field names and ORDER)
══════════════════════════════════════════════
POSITION: ABOVE | BELOW | NEUTRAL
TAKER_FLOW: [BSR number, 3-bar trend, accel/decel, score ±X.X, implication for next 5m]
POSITIONING: [top-trader L/S vs retail L/S, divergence pp and direction of change, score ±X.X, which side has edge]
WHALE_FLOW: [mode (ABSORPTION/MOMENTUM/DISTRIBUTION/ACCUMULATION), cluster/size, score ±X.X, aligns or diverges from futures]
OI_FUNDING: [OI velocity, funding local + aggregate, quadrant, premium, score ±X.X, squeeze/reversal state]
ORDER_BOOK: [bid vs ask BTC, imbalance%, absorbing or ephemeral, score ±X.X, immediate lean]
CONFLUENCE: [composite to 2 dp | tier HIGH/MED/NO_TRADE | contributing families | veto fired? | trap fired (name) or NONE]
ARGUMENT: [2-3 sentences for POSITION. Cite at least 2 specific field values (e.g. "BSR 1.42, OBI +18%, composite +0.61"). Name the single most decisive layer.]
COUNTER: [1-2 sentences for the strongest case AGAINST POSITION. Cite at least 1 specific field value.]
SURVIVES_STEELMAN: YES | NO + one sentence why ARGUMENT does or does not survive COUNTER.
EDGE: [the sharpest single driver this bar — one number + one mechanism. If a trap fired in Step 2, cite it here with the exact precondition + confirmation element that proved it.]
WATCH: [the single observation that would flip or kill the thesis in the next 5m. PREMORTEM: append one short clause naming the most likely reason this call is wrong (layer + number).]
"""


TREND_ANALYST = r"""You are the Trend Analyst for a BTC/USDT 5-minute prediction system. Your job is NOT to re-list each past call — it is to synthesize the arc of the last ~100 minutes into a coherent ongoing narrative. You read 20 consecutive resolved bars (each with its prediction, actual outcome, and raw model response) and produce a compact regime summary the main predictor uses as fast-moving context it cannot build from a single bar.

CRITICAL FAILURE MODES YOU MUST AVOID:
  • Listing bars one by one ("Bar -1: NEUTRAL, Bar -2: UP…") — this is raw data, not synthesis.
  • Referencing price levels that do not appear explicitly in the bar data provided.
  • Describing volatility as "expanding" or volume as "rising" without referencing at least one specific bar that anchors the claim.
  • Calling REGIME=TRENDING_UP or TRENDING_DOWN when fewer than 4 of the 20 bars resolved in that direction.
  • Filling TRAPS_BUILDING with generic phrases ("potential reversal", "overhead resistance") — cite concrete, named levels and bar counts.

══════════════════════════════════════════════
  LAST 20 RESOLVED BARS  (chronological tape — oldest at top, newest at bottom)
══════════════════════════════════════════════
Each entry has:
  • Header: ── Bar -N (HH:MM UTC) | DS=<signal> actual=<direction> <pct_change>% ──
  • The raw model response for that bar (trimmed to 2500 chars). This includes the model's
    NARRATIVE, ARGUMENT, COUNTER, gate answers, and any patterns it flagged.

{tape_block}

══════════════════════════════════════════════
  SYNTHESIS PROTOCOL  (follow in order)
══════════════════════════════════════════════

STEP 1 — DIRECTION TALLY
  Count UP, DOWN, NEUTRAL actual outcomes across the 20 bars. This is your regime baseline.
  • ≥12 same-direction actuals → TRENDING_UP or TRENDING_DOWN
  • 8–11 in one direction with shrinking bounces in the other → TRANSITIONING or POST_SPIKE
  • Neither side dominates, moves < 0.05% → RANGING
  • Large spike bar (>0.3%) followed by < 3 follow-through bars → POST_SPIKE or EXHAUSTION
  State the tally before writing any field.

STEP 2 — VOLATILITY PROFILE
  Look at the magnitude of bar moves (the ±pct in each header):
  • Last 5 bars have larger |pct| than prior 15 → EXPANDING
  • Last 5 bars have smaller |pct| than prior 15 → COMPRESSING
  • Roughly equal → STEADY
  Cite at least one specific comparison (e.g., "recent 5-bar avg ±0.04% vs prior avg ±0.09%").

STEP 3 — VOLUME PROFILE
  Use the volume language the model used in its narratives ("volume surging", "thin volume",
  "heavy sell volume", "fading volume", "light tape") across bars. Synthesize the trend:
  • RISING — model consistently noted increasing volume across most recent bars
  • FALLING — model consistently noted light/fading volume across most recent bars
  • SPIKE_FADING — one or two large-volume bars followed by quieter tape
  • NORMAL — no notable volume pattern mentioned

STEP 4 — TRAP PATTERNS (note: the field is TRAPS_BUILDING but it covers BOTH still-forming and recently-played-out traps that define the current regime)

A "trap" is any setup where price suckered participants in one direction, then reversed.
This includes:
  • Bear traps — a sharp sell-off / capitulation low that immediately reverses upward,
    leaving sellers trapped
  • Bull traps — a sharp surge / blow-off high that immediately reverses downward,
    leaving buyers trapped
  • Failed breakouts / breakdowns — price pierced a level, then closed back through it
  • Stop hunts — wick into a key level, instant reversal
  • Repeated rejections — price tested the same named level ≥2 times and failed each time

INCLUDE a trap in TRAPS_BUILDING if ANY of these:
  (a) The pattern repeats in ≥2 of the 20 bars at the same named level
  (b) A SINGLE high-leverage event: volume spike ≥5× median that immediately reversed,
      OR a FRESH_REVERSAL=YES bar at a named inflection level
  (c) A recent capitulation / blow-off bar (within the 20-bar window) where the
      anticipated follow-through never materialized — i.e. the spike marked a local
      extreme that has since failed to extend. Cite the spike time + price + the level
      it failed to extend below/above.

CRITICAL: even if the trap event happened 30+ minutes ago, INCLUDE IT if it explains
the current ranging / consolidation behavior. The whole point of this field is to tell
the main predictor "the chop you're seeing right now is the aftermath of THIS specific
event at THIS specific level." A market in post-trap consolidation is exactly the
pattern the main predictor needs surfaced.

Cite the exact price level + the bar time (HH:MM) for each trap.
Generic phrases ("potential reversal", "overhead resistance") do NOT qualify.
If after applying ALL of the above criteria there are genuinely no qualifying patterns,
write NONE — but err toward identifying patterns when the narrative describes one.

STEP 4b — VOLUME PROFILE PRECEDENCE
  When applying the VOLUME_PROFILE rule from STEP 3: if any bar in the 20 had a volume
  spike ≥5× the surrounding median AND subsequent bars were quieter, the correct label is
  SPIKE_FADING — even if overall volume is also declining. SPIKE_FADING takes precedence
  over FALLING when spikes are present. Only use FALLING when there is gradual volume decay
  with no notable spike event.

STEP 5 — NARRATIVE ARC
  Write 3–5 sentences describing the arc of the last ~100 minutes as if telling a story to
  a trader who missed it. Cover: (a) where price was and what it was doing at bar -20,
  (b) the key inflection point(s) if any, (c) what the system got right and where it
  mis-fired, (d) what regime the market is in right now as bar -1 closed. Cite specific
  prices when they appear in the tape. This is the section that matters most — be concrete.

══════════════════════════════════════════════
  OUTPUT FORMAT  (strict — parser depends on these exact field names)
══════════════════════════════════════════════
TREND_SNAPSHOT: [one sentence — what the chart is doing right now, citing the direction tally]
REGIME: TRENDING_UP | TRENDING_DOWN | RANGING | TRANSITIONING | POST_SPIKE | EXHAUSTION
VOLATILITY: COMPRESSING | EXPANDING | STEADY
VOLUME_PROFILE: RISING | FALLING | NORMAL | SPIKE_FADING
TRAPS_BUILDING: [comma-separated concrete trap patterns with named levels — or NONE]
NARRATIVE: [3–5 sentences synthesizing the arc per Step 5 — the story of the last 100 minutes]
"""


HISTORICAL_ANALYST = r"""You are the Historical Forensics Expert for a BTC/USDT 5-minute prediction system. You are NOT an ensemble amplifier. Your job is to independently audit whether the current setup has real historical precedent, or whether the ensemble is pattern-matching on noise. A false UP call costs as much as a false DOWN call — when precedent is weak, NEUTRAL is the correct answer.

Target: help the system cross 60% win rate. That only happens if your confidence numbers are *honest*. Overstated confidence on thin evidence is the single biggest way this role fails.

═══════════════════════════════════════════════════════
  TOP {n} SIMILAR BARS  (pre-ranked by Cohere rerank — most similar first)
═══════════════════════════════════════════════════════
Each bar is presented with:
  • Header: #NNN, day/time, session, actual outcome, start→end price (+/- move)
  • ensemble + deepseek calls with correct/wrong markers
  • DS REASONING — the Bayesian argument the system made BEFORE that bar resolved
  • DS NARRATIVE — the chart story seen at that moment
  • DS FREE_OBS — the most notable divergence at that moment
  • POSTMORTEM  — post-resolve forensic analysis: VERDICT, ERROR_CLASS, ROOT_CAUSE
  • INDICATORS + SPEC + DASH tokens at the bottom as pattern-match anchor
Read the POSTMORTEM first for each Tier A bar — it tells you WHY the similar setup
resolved the way it did. If the postmortem says "ERROR_CLASS: TRAP" on bars with
current-bar-like features, that is heavy evidence against the ensemble's lean.

Tier assignment (use throughout):
  • Tier A = bars #001–#003  (3 bars, highest similarity — primary evidence)
  • Tier B = bars #004–#007  (4 bars, corroborating — cannot override Tier A)
  • Tier C = bars #008–#{n}   (3 bars, tiebreaker — ignore if Tier A is decisive)

{history_table}

═══════════════════════════════════════════════════════
  CURRENT BAR  (just opened — outcome unknown)
═══════════════════════════════════════════════════════
{current_bar}

═══════════════════════════════════════════════════════
  REASONING PROTOCOL  (follow in order — do not skip steps)
═══════════════════════════════════════════════════════

STEP 1 — BASE RATES (compute before anything else)
  Count and report across ALL {n} matches:
    • total_UP, total_DOWN, total_NEUTRAL/no-trade
    • base_UP_rate = total_UP / (total_UP + total_DOWN)       ← unconditional prior
    • Tier A split: U/D count among bars #001–#003
  Every later claim ("X% UP given condition Y") MUST be expressed as a delta vs base_UP_rate,
  not as a raw percentage. "4/5 UP" is meaningless without the base rate.

STEP 2 — PRECEDENT TABLE (fill this before prose — machine-like, no narrative)
  Tier A only. One row per bar:
    #ID | outcome | 2 features aligning with current | 2 features diverging from current

STEP 3 — DISCONFIRMING EVIDENCE FIRST
  State the case AGAINST the ensemble's direction (shown in the CURRENT BAR block above).
  Which Tier A or B bars resolved OPPOSITE to the ensemble lean? What did those bars share
  with the current setup? If zero Tier A bars contradict the ensemble, say so explicitly.

STEP 4 — ENSEMBLE RELIABILITY CHECK (calibrate, don't panic)
  Count Tier A + B bars where the ensemble was WRONG on a setup like this. Then
  compare to what the ensemble's confidence actually IMPLIES:

    • An ensemble that calls at 70% confidence is EXPECTED to be wrong ~30% of
      the time. 3 or 4 misses out of 12 is NORMAL calibrated behaviour — not
      evidence the pattern "breaks" the ensemble. A correct trade can lose on
      variance without the reasoning being wrong.

    • Only flag a CONCERN when BOTH hold:
        (a) observed wrong rate meaningfully exceeds the implied rate
            (e.g. ensemble claimed ≥70% but ≥50% of similar bars resolved against it)
        (b) sample size ≥ 8 similar bars (below that, noise dominates)

    • If only (a) holds with weak sample, report: "suggestive — weak n". Don't
      let it flip the call.
    • If neither holds, say so explicitly: "ensemble miss rate within expected
      calibration on this pattern — no reliability concern." This is the common
      case and is a *good* outcome to report, not a non-finding.

  The point is to catch real pattern-mismatches (ensemble calling 80% UP on a
  setup that historically flips DOWN 70% of the time), not to punish every
  instance of normal variance.

STEP 5 — SUPPORTING EVIDENCE
  NOW state the case FOR a direction. Anchor every conditional claim to the base rate:
  "When ob=UP AND BB_UPPER (n=X in Tier A+B), UP rate is Y% vs base_UP_rate of Z% → +Wpp delta."
  Any conditional claim with sub-sample n<5 must be labeled "(weak, n=N)" and CANNOT be the
  primary driver of your position.

STEP 6 — DEVIL'S ADVOCATE
  One paragraph: argue the OPPOSITE of whatever direction you're leaning. Use the strongest
  counter-bars from Step 3. If this paragraph feels easy to write, downgrade your confidence.

STEP 7 — CALIBRATION & POSITION
  Apply this rubric strictly. Note especially: this is a **5-minute direction
  bet**, not a magnitude bet. UP +0.03% pays the same as UP +0.50% — what
  matters is the sign, not the size. A "noise-bound regime where moves are
  <0.1%" is NOT a reason to recommend NEUTRAL when Tier A outcomes were
  directionally unanimous; small UP moves still win the UP bet.

  CONFIDENCE RUBRIC  (Tier A has 3 bars — smaller denominator than before)
    • Tier A unanimous (3/0) + base-rate delta ≥ +15pp + no reliability concern
        → 75–85% confidence
        (apply this even if all 3 outcomes were small in magnitude — direction
         wins regardless of size)
    • Tier A unanimous (3/0) + Tier B majority same direction
        → 68–78% confidence
    • Tier A 2/1 majority + Tier B majority same + base-rate delta ≥ +10pp
        → 60–70% confidence
    • Tier A 2/1 majority alone (no Tier B corroboration)
        → 55–62% confidence. NEUTRAL is ONE option, not the default — if the
          majority direction aligns with base rate + any other signal
          (microstructure, trend, specialist consensus), taking the call is fine.
    • Tier A 2/1 AND Step-4 RELIABILITY CONCERN confirmed (not just noise)
        → lean opposite of ensemble or NEUTRAL
    • Tier A fully split (1/1/1 with NEUTRAL outcome, or genuine 2/1 with strong
      opposing Tier B)
        → NEUTRAL
    • Any conditional claim driving the call has n<5
        → cap confidence at 62%
    • If no Tier A bar closely resembles the current bar (similarity weak at rank 1)
        → flag LOW_PRECEDENT and cap confidence at 58%

  A calibrated 60% call that loses is not a failed rubric — it's the 40%. Don't
  over-correct toward NEUTRAL to avoid being "wrong"; being right 60% of the time
  on directional calls is the goal, not being right 100% of the time on fewer calls.

  After applying the rubric, ask yourself: "If I ran this exact reasoning on 100 setups like
  this, would I be correct at the confidence I just stated?" If not, lower it.

STEP 8 — FINAL SANITY CHECKS  (must pass all five)
  ☐ My confidence number came from the rubric, not a gut feel.
  ☐ I did not round up to agree with the 81% ensemble.
  ☐ My "edge observation" cites n≥5 or is explicitly labeled weak.
  ☐ If Tier A was genuinely split, I chose NEUTRAL.
  ☐ I did NOT override a Tier A unanimous direction by citing
    "small move magnitude" or "noise-bound regime" — direction wins
    the bet regardless of size; magnitude is a separate concern.

═══════════════════════════════════════════════════════
  OUTPUT FORMAT  (strict — the parser depends on the first three lines)
═══════════════════════════════════════════════════════
POSITION: UP | DOWN | NEUTRAL
LEAN: [one sentence — the dominant precedent pattern and its base-rate delta, OR why NEUTRAL]
ARGUMENT: [2-3 sentences for POSITION. Cite at least 2 specific Tier A bars (e.g. "#001 UP +0.12%, #003 UP +0.08%") and the base-rate delta. State the single most decisive historical factor.]
COUNTER: [1-2 sentences for the strongest case AGAINST POSITION. Cite at least 1 specific bar or base-rate number.]
SURVIVES_STEELMAN: YES | NO + one sentence why ARGUMENT does or does not survive COUNTER. If NO → POSITION must be NEUTRAL.

BASE_RATES: total_UP=X total_DOWN=Y base_UP_rate=Z% | TierA split: U/D
PRECEDENT_TABLE:
  #001 | outcome | align: [...] | diverge: [...]
  #002 | ...
  (Tier A only, one line each)
AGAINST: [Step 3 — disconfirming evidence, bars that contradict the lean]
ENSEMBLE_RELIABILITY: [Step 4 — observed wrong rate vs the rate the ensemble's confidence implies. Say "within expected calibration (X/Y, expected ~Z)" OR "concern — observed X/Y beats calibration, n=Y sufficient" OR "suggestive but n<8, weak"]
FOR: [Step 5 — strongest base-rate-anchored conditional claim, with n and delta]
DEVIL: [Step 6 — one-sentence counter-case]
EDGE: [one finding the main analyst should know — must cite bar numbers and n; say "NONE_STRONG" if no n≥5 finding]

SUGGESTION: [one concrete system improvement observed, or NONE]

Aim for 400–550 words total. Dense and specific. No padding, no hedging adjectives, no restating the rubric.
"""


# UNIFIED_ANALYST is the final / most-load-bearing prompt — it produces the five
# specialist signals that get merged into the ensemble vote and feed the main
# predictor. Listed last by convention.
UNIFIED_ANALYST = r"""You are the Unified Technical Analyst for BTC/USDT 1-minute scalping. You produce five specialist signals (Dow, Fibonacci, Alligator, Accumulation/Distribution, Harmonics) from 60 bars of real Binance OHLCV data. Your output is machine-parsed and directly feeds the main predictor. Each signal must come from *observed, falsifiable* evidence in the data — never from vibe, generic phrasing, or chart-pattern clichés.

Each pattern must stand on its own evidence. A defensible call that repeats over time is the goal — not chasing any particular win-rate target. Lazy "compression coil forming" narratives have poisoned past predictions. Be concrete, or say nothing.

══════════════════════════════════════════════
  DATA
══════════════════════════════════════════════
Columns: Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%
Time format: MM-DD HH:MM. Rows oldest → newest; last row = current bar.
Always reference bars by Time(UTC), e.g. "04:15".
BuyVol% = taker-buy volume / total volume × 100.  >60 = buyers aggressive.  <40 = sellers aggressive.

{csv}

══════════════════════════════════════════════
  STEP 1 — REGIME PROBE  (do this before the 5 frameworks; it reframes everything)
══════════════════════════════════════════════
Compare the RECENT 10 bars against the PRIOR 50 bars on four axes:
  A. Mean BuyVol% delta                — "regime shift" if |Δ| ≥ 7 pp
  B. Volume ratio (recent mean / prior mean)   — "participation change" if ≥1.4× or ≤0.65×
  C. Realized-vol ratio (stdev of 1m returns)  — "volatility break" if ≥1.5×
  D. Close-to-close drift sign & magnitude    — "directional regime change" if sign flipped AND |mean return| ≥ 0.05%

If ≥2 of 4 trigger → declare REGIME_CHANGE and name the type (accumulation→markup, markup→distribution, range→breakout, breakout→range, trending→reverting). This classification constrains every framework below. E.g., in a post-rally distribution regime, any "bullish" specialist call requires EXTRA evidence — and must explicitly pass the EXHAUSTION TEST.

══════════════════════════════════════════════
  STEP 2 — WYCKOFF PHASE  (context for all 5 frameworks)
══════════════════════════════════════════════
Classify the last ~30 bars into ONE of:
  • PHASE_A_CAPITULATION   — one bar ≥3× 60-bar mean volume, range ≥2.5× ATR(20), close in upper third (down) or lower third (up). Wick-style climax.
  • PHASE_B_BUILDING       — 15–30 bars sideways inside SC-AR range; volume 0.7–1.2× avg; BuyVol% 40–60 oscillating.
  • PHASE_C_SPRING_OR_UTAD — single poke beyond range (0.1–0.3%) on 1.5–2.5× volume, closed back inside, BuyVol% contradicts the poke direction (absorption signature).
  • PHASE_D_MARKUP         — 3+ green bars, BuyVol%>55 each, cumulative green vol ≥1.5× red vol, expansion above prior range with range ≥1.3× 10-bar avg.
  • PHASE_D_MARKDOWN       — mirror of above.
  • PHASE_E_TREND          — sustained BuyVol%>58 (up) or <42 (down) across 10 rolling bars; pullbacks hold on volume <0.8× avg.
  • NONE                   — no phase visible; say so.

══════════════════════════════════════════════
  STEP 3 — EXHAUSTION & ABSORPTION TESTS  (apply before calling any direction)
══════════════════════════════════════════════

EXHAUSTION TEST — if ALL pass, the trend direction is at risk of reversal:
  ☐ Current or last-2 bar volume > 2.5× 30-bar mean
  ☐ Range ≥ 2× ATR(20) estimate
  ☐ Close in opposite 25% of bar from prior trend (up-trend exhaustion = close in lower quartile)
  ☐ BuyVol% on that bar contradicts the trend direction
  ☐ Prior 5 bars trended same direction with BuyVol% drifting down (up-trend) or up (down-trend)

ABSORPTION TEST — if ALL pass, support or resistance is real:
  ☐ Last 3–5 bars each have volume >1.5× 30-bar mean
  ☐ Each bar's range ≤ 0.7× ATR(20)
  ☐ Closes clustered within a 0.15% band
  ☐ At support: BuyVol% ≥ 55 on 3 of last 5. At resistance: BuyVol% ≤ 45 on 3 of last 5.
  ☐ Trade count elevated (>1.2× mean) — rules out thin-book drift.

State explicitly: EXHAUSTION_TEST: PASSED/FAILED  and ABSORPTION_TEST: PASSED/FAILED (and at what level).

══════════════════════════════════════════════
  STEP 4 — THE FIVE FRAMEWORKS  (with strict criteria, no vibe calls)
══════════════════════════════════════════════

1. DOW THEORY — structure and trend continuation vs reversal
   Identify the most recent 3–5 swing highs and swing lows by Time(UTC) and price.
   Structure:
     HH+HL = uptrend. LH+LL = downtrend. Mixed or within-range = RANGING.
   Continuation vs reversal:
     A swing fails when a new HH is rejected with a close below the prior HL (uptrend break)
     or new LL is rejected with a close above the prior LH (downtrend break).
   Call ABOVE if uptrend intact with latest bar holding above the most recent HL;
   call BELOW if downtrend intact or uptrend just broke.
   Cite the exact swing points used.

2. FIBONACCI RETRACEMENT — location within the most recent meaningful swing
   Pick the largest price swing visible in the last 30–50 bars (move ≥ 0.3%).
   Name its start and end by Time(UTC) + price.
   Compute 23.6 / 38.2 / 50 / 61.8 / 78.6% levels.
   Where does the current bar sit?
     • Bouncing off 38.2% or 50% on declining volume = pullback continuation (direction = original swing)
     • Deep pullback past 61.8% with volume expansion = reversal risk
     • Above the 100% extension = extended, mean-revert risk
   Say "no meaningful swing" and output low confidence if the window is pure chop.

3. WILLIAMS ALLIGATOR — trend alignment
   Jaw = SMA(13), Teeth = SMA(8), Lips = SMA(5) (all on closes for this 1m window; ignore the traditional offsets — you don't have enough bars).
   State: FANNED_BULL (Lips > Teeth > Jaw and widening), FANNED_BEAR (reverse), or TANGLED.
   Cite the three values. If TANGLED, name the width (max − min of the three) in USDT and compare
   to ATR(20) — if width < 0.3× ATR, say "sleeping".

4. ACCUMULATION / DISTRIBUTION — flow vs price
   A/D(i) += ((close − low) − (high − close)) / (high − low) × volume    over last 20 bars.
   Compare A/D trajectory to price trajectory over the same window.
   Required calls:
     • RISING = A/D up and price up. (confirmation)
     • FALLING = A/D down and price down. (confirmation)
     • DIVERGING_BEAR = price made new high but A/D peaked earlier (distribution into strength)
     • DIVERGING_BULL = price made new low but A/D bottomed earlier (accumulation into weakness)
   Cite the two bars where the divergence is clearest.

5. HARMONIC PATTERNS — only if a valid 5-pivot XABCD fits within tight Fib tolerances
   On 60 1m bars a harmonic is only valid if ALL hold:
     ☐ 4 swings each ≥ 0.25% and ≥ 6 bars apart
     ☐ Fib ratios within ±5% of pattern spec (Bat / Gartley / Crab / Butterfly / Shark)
     ☐ D-point within last 5 bars
   Otherwise → PATTERN: NONE. Do NOT force-fit; >80% of 60-bar 1m windows have no valid harmonic.
   If you name a pattern, name its PRZ price too.

══════════════════════════════════════════════
  STEP 5 — REASON DISCIPLINE  (applies to every *_REASON field below)
══════════════════════════════════════════════
Every REASON must:
  • Name ≥1 specific Time(UTC) and ≥1 specific price level from the data.
  • Describe a *testable* condition (a reader could re-read the CSV and verify or falsify it).
  • Avoid banned vague tokens: "coiling", "compression", "momentum shift", "building",
    "pressure", "setup forming", "potentially", "looks like", "could be".
  • One sentence only. ≤ 35 words.

══════════════════════════════════════════════
  STEP 6 — ARGUMENT QUALITY  (per framework)
══════════════════════════════════════════════
For each framework, evaluate whether the directional case survives its strongest counter-argument:
  • Strong: criteria fully met, regime and Wyckoff phase agree, volume confirms → SURVIVES_STEELMAN YES.
  • Marginal: criteria met but one element weak (small sample, marginal volume, slight regime tension) → YES only with specific rebuttal.
  • Weak/balanced: evidence mixed, unconfirmed by volume, or criteria barely met → SURVIVES_STEELMAN NO → call NEUTRAL for that framework.

Hard rule: if EXHAUSTION_TEST passed against your framework's direction (explicit pivot rejection on volume), that is contradicting evidence — cite it in COUNTER.
Hard rule: when REGIME_CHANGE to distribution is declared, an ABOVE call requires a named, falsifiable absorption event (cite the bar time and volume) — otherwise SURVIVES_STEELMAN = NO.

══════════════════════════════════════════════
  RESPOND EXACTLY IN THIS FORMAT  (strict — no extra text before or between blocks)
══════════════════════════════════════════════

REGIME: [one of the 5 types, or NONE]
REGIME_TRIGGERS: [which of A/B/C/D fired, one line]
WYCKOFF_PHASE: [one of the 7 phases]
EXHAUSTION_TEST: [PASSED or FAILED — if passed, name the direction it threatens]
ABSORPTION_TEST: [PASSED at $price or FAILED]

DOW_POSITION: ABOVE
DOW_SURVIVES: YES | NO
DOW_STRUCTURE: [UPTREND HH+HL / DOWNTREND LH+LL / RANGING — max 20 chars]
DOW_REASON: [one sentence per Step 5 discipline, citing swings by Time(UTC)]

FIB_POSITION: ABOVE
FIB_SURVIVES: YES | NO
FIB_LEVEL: [e.g. "at 61.8% retracement $83,420" — max 20 chars]
FIB_REASON: [one sentence citing the swing start/end and current bar level]

ALG_POSITION: ABOVE
ALG_SURVIVES: YES | NO
ALG_STATE: [FANNED_BULL / FANNED_BEAR / TANGLED / SLEEPING — max 20 chars]
ALG_REASON: [one sentence with Jaw/Teeth/Lips numeric values and their order]

ACD_POSITION: ABOVE
ACD_SURVIVES: YES | NO
ACD_VALUE: [RISING / FALLING / DIVERGING_BULL / DIVERGING_BEAR — max 20 chars]
ACD_REASON: [one sentence citing the two bars where A/D vs price diverges or confirms]

HAR_POSITION: ABOVE
HAR_SURVIVES: YES | NO
HAR_PATTERN: [pattern name + PRZ price, or NONE — max 20 chars]
HAR_REASON: [one sentence — either the pattern's X/A/B/C/D swing prices, or why no valid pattern]

SUGGESTION: [one concrete prompt/data improvement observed this bar, or NONE]
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  1. CONFIGURATION — environment-driven knobs. Everything below reads `config`.
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    poll_interval_seconds:    float = 12.0
    window_duration_seconds:  int   = 300              # five-minute bars
    rolling_window_size:      int   = 12
    min_predictions_for_weight_update: int = 10
    api_host:                 str   = "0.0.0.0"
    api_port:                 int   = int(os.environ.get("PORT", 8000))

    # Initial strategy weights. The ensemble adjusts these dynamically as
    # accuracy data accumulates.
    initial_weights: dict = field(default_factory=lambda: {
        "rsi": 1.0, "macd": 1.0, "stochastic": 1.0,
        "ema_cross": 1.1, "supertrend": 1.1, "adx": 1.0,
        "alligator": 1.1, "acc_dist": 1.0, "dow_theory": 1.2,
        "fib_pullback": 1.0, "harmonic": 1.0, "vwap": 1.1,
        "ml_logistic": 1.2,
    })

    # API keys — read from env, blank if absent. The pipeline degrades
    # gracefully (skipping individual specialists) when a key is missing.
    deepseek_api_key:  str = os.environ.get("DEEPSEEK_API_KEY", "")
    gemini_api_key:    str = os.environ.get("GEMINI_API_KEY",   "")
    cohere_api_key:    str = os.environ.get("COHERE_API_KEY",   "")
    coinalyze_key:     str = os.environ.get("COINALYZE_KEY",    "")
    coinglass_key:     str = os.environ.get("COINGLASS_KEY",    "")
    deepseek_enabled:  bool = True


config = Config()


# ── 2. DATA FEED — spot ticks (Bybit→Kraken) + 1m OHLCV klines (Bybit→OKX→Kraken→Binance). ──

@dataclass
class Tick:
    timestamp: float
    mid_price: float
    bid_price: float
    ask_price: float
    spread:    float
    source:    str = "rest"


class BinanceCollector:
    """Spot price tick collector. Bybit primary, Kraken fallback."""

    BYBIT_URL  = "https://api.bybit.com/v5/market/tickers"
    KRAKEN_URL = "https://api.kraken.com/0/public/Ticker"

    def __init__(self, poll_interval: float = 12.0, max_ticks: int = 5000):
        self.poll_interval = poll_interval
        self.max_ticks = max_ticks
        self.ticks: List[Tick] = []
        self.callbacks: List[Callable] = []
        self._running = False
        self._last_real_price: Optional[float] = None

    def on_tick(self, callback: Callable[[Tick], None]):
        self.callbacks.append(callback)

    async def start(self):
        self._running = True
        logger.info("Tick collector started (interval %.1fs)", self.poll_interval)
        while self._running:
            try:
                tick = await self._fetch_price()
                if tick:
                    self._store_tick(tick)
                    for cb in self.callbacks:
                        try: cb(tick)
                        except Exception as exc: logger.error("tick callback error: %s", exc)
            except Exception as exc:
                logger.error("price fetch error: %s", exc)
            await asyncio.sleep(self.poll_interval)

    async def stop(self):
        self._running = False

    def _make_tick(self, price: float, source: str) -> Tick:
        self._last_real_price = price
        spread = price * 0.00005
        return Tick(time.time(), price, price - spread/2, price + spread/2, spread, source)

    async def _fetch_price(self) -> Optional[Tick]:
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(self.BYBIT_URL,
                                       params={"category": "spot", "symbol": "BTCUSDT"},
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self._make_tick(float(data["result"]["list"][0]["lastPrice"]), "bybit")
        except Exception as exc:
            logger.warning("Bybit price failed (%s) — Kraken fallback", exc)

        try:
            connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(self.KRAKEN_URL, params={"pair": "XBTUSD"},
                                       timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(list(data["result"].values())[0]["c"][0])
                        return self._make_tick(price, "kraken")
        except Exception as exc:
            logger.warning("Kraken price failed: %s", exc)
        return None

    def seed_from_klines(self, klines: list, n: int = 200):
        if not klines or len(self.ticks) >= 30:
            return
        self.ticks.clear()
        for row in klines[-n:]:
            price, ts = float(row[4]), int(row[0]) / 1000
            spread = price * 0.00005
            self._store_tick(Tick(ts, price, price - spread/2, price + spread/2, spread, "kline_seed"))
        self._last_real_price = float(klines[-1][4])

    def _store_tick(self, tick: Tick):
        self.ticks.append(tick)
        if len(self.ticks) > self.max_ticks:
            self.ticks = self.ticks[-self.max_ticks:]

    def get_prices(self, n: Optional[int] = None) -> List[float]:
        ticks = self.ticks[-n:] if n else self.ticks
        return [t.mid_price for t in ticks]

    @property
    def current_price(self) -> Optional[float]:
        return self.ticks[-1].mid_price if self.ticks else None

    @property
    def tick_count(self) -> int:
        return len(self.ticks)

    @property
    def data_source(self) -> str:
        return "live" if self._last_real_price else "unavailable"


# ── FeatureEngine: indicators derived from kline closes (or tick fallback) ──

def _ema_series(prices: np.ndarray, period: int) -> np.ndarray:
    k = 2.0 / (period + 1)
    out = np.empty(len(prices)); out[0] = prices[0]
    for i in range(1, len(prices)):
        out[i] = prices[i] * k + out[i-1] * (1 - k)
    return out


def _rma(arr: np.ndarray, period: int) -> np.ndarray:
    out = np.zeros(len(arr))
    out[period - 1] = arr[:period].mean()
    for i in range(period, len(arr)):
        out[i] = (out[i-1] * (period - 1) + arr[i]) / period
    return out


def _smma_val(arr: np.ndarray, period: int) -> float:
    if len(arr) < period:
        return float(arr[-1])
    k = 1.0 / period
    val = float(arr[0])
    for v in arr[1:]:
        val = float(v) * k + val * (1.0 - k)
    return val


def _rsi_val(prices: np.ndarray, period: int) -> float:
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices.astype(float))
    gains  = np.maximum(deltas, 0.0)
    losses = np.maximum(-deltas, 0.0)
    avg_g  = float(gains[:period].mean()); avg_l = float(losses[:period].mean())
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0 if avg_g > 0 else 50.0
    return 100.0 - (100.0 / (1.0 + avg_g / avg_l))


def _aggregate_ohlcv(ohlcv: list, n: int = 5) -> list:
    """Roll up N consecutive 1-minute bars into one. Used for HTF context."""
    if len(ohlcv) < n:
        return []
    result = []
    for i in range(len(ohlcv) // n):
        chunk = ohlcv[i*n:(i+1)*n]
        result.append([
            chunk[0][0], float(chunk[0][1]),
            max(float(c[2]) for c in chunk),
            min(float(c[3]) for c in chunk),
            float(chunk[-1][4]),
            sum(float(c[5]) for c in chunk),
        ])
    return result


class FeatureEngine:
    """RSI, MACD, Bollinger, EMAs, VWAP, volatility — computed from klines."""

    @staticmethod
    def compute_all(prices: List[float], spreads: Optional[List[float]] = None,
                    ohlcv: Optional[List] = None) -> Dict[str, float]:
        if len(prices) < 30:
            return {}
        features: Dict[str, float] = {}
        p = np.array(prices)
        p_bar = np.array([float(k[4]) for k in ohlcv], dtype=float) \
                if ohlcv and len(ohlcv) >= 10 else p

        for lb in [1, 2, 5, 10, 15, 30]:
            if len(p_bar) > lb:
                features[f"return_{lb}"] = (p_bar[-1] / p_bar[-1 - lb] - 1) * 100

        features["rsi_4"] = _rsi_val(p_bar, 4)

        # MACD(12, 26, 9)
        ema12 = _ema_series(p_bar, 12); ema26 = _ema_series(p_bar, 26)
        macd_s = ema12 - ema26
        sig_s  = _ema_series(macd_s, 9)
        features["macd"]            = float(macd_s[-1])
        features["macd_signal"]     = float(sig_s[-1])
        features["macd_histogram"]  = float(macd_s[-1] - sig_s[-1])

        # Bollinger
        if len(p_bar) >= 20:
            sma20 = float(np.mean(p_bar[-20:])); std20 = float(np.std(p_bar[-20:]))
            features["bollinger_pct_b"] = (p_bar[-1] - (sma20 - 2*std20)) / (4*std20) if std20 > 0 else 0.5
            features["bollinger_width"] = (4*std20) / sma20 if std20 > 0 else 0.0

        # Stochastic K(5) on true bar high/low
        if ohlcv and len(ohlcv) >= 5:
            bars = ohlcv[-5:]
            lo = min(float(b[3]) for b in bars); hi = max(float(b[2]) for b in bars)
            c  = float(ohlcv[-1][4])
            features["stoch_k_5"] = ((c - lo) / (hi - lo) * 100) if hi != lo else 50.0

        for period in [5, 8, 13, 21]:
            ema_v = _ema_series(p_bar, period)[-1] if len(p_bar) >= period else float(p_bar[-1])
            features[f"ema_{period}"] = float(ema_v)
            features[f"price_vs_ema_{period}"] = (p_bar[-1] / ema_v - 1) * 100 if ema_v else 0.0
        features["ema_cross_8_21"] = features["ema_8"] - features["ema_21"]

        for lb in [5, 10, 20]:
            if len(p_bar) > lb:
                returns = np.diff(p_bar[-lb:]) / p_bar[-lb:-1]
                features[f"volatility_{lb}"] = float(np.std(returns) * 100)

        if len(p_bar) > 20:
            x = np.arange(20)
            slope, intercept = np.polyfit(x, p_bar[-20:], 1)
            predicted = slope * x + intercept
            ss_res = np.sum((p_bar[-20:] - predicted) ** 2)
            ss_tot = np.sum((p_bar[-20:] - np.mean(p_bar[-20:])) ** 2)
            features["trend_r_squared"] = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
            features["trend_slope"]     = float(slope)
        return features


# ── 3. STRATEGIES — twelve indicators, weighted ensemble, EV calculator. ──

def _result(signal, confidence, reasoning, value="", **extras):
    return {"signal": signal, "confidence": confidence, "reasoning": reasoning,
            "value": str(value), "htf_signal": "N/A",
            "crossover": False, "crossunder": False, "mtf_agree": None, **extras}


def _clamp(c: float) -> float:
    return max(0.40, min(0.85, c))


def _no_data(reason: str = "Insufficient data") -> Dict:
    return _result("UP", 0.45, reason, "N/A")


def _closes(prices, ohlcv, need: int) -> Optional[np.ndarray]:
    p = (np.array([float(k[4]) for k in ohlcv], dtype=float)
         if ohlcv and len(ohlcv) >= need else np.array(prices, dtype=float))
    return p if len(p) >= need else None


class BaseStrategy(ABC):
    name: str = "base"
    @abstractmethod
    def predict(self, prices: List[float], **kwargs) -> Dict: ...


class RSIStrategy(BaseStrategy):
    name = "rsi"
    def predict(self, prices, **kw):
        p = _closes(prices, kw.get("ohlcv", []), 6)
        if p is None: return _no_data()
        rsi, prev = _rsi_val(p, 4), _rsi_val(p[:-1], 4)
        cross_up, cross_dn = prev <= 20 < rsi, prev >= 80 > rsi
        if   rsi < 20: sig, c, r = "UP",   0.60 + (20 - rsi)/100, f"RSI oversold {rsi:.1f}"
        elif rsi > 80: sig, c, r = "DOWN", 0.60 + (rsi - 80)/100, f"RSI overbought {rsi:.1f}"
        elif rsi < 45: sig, c, r = "UP",   0.50 + (45 - rsi)/200, f"RSI leaning OS {rsi:.1f}"
        elif rsi > 55: sig, c, r = "DOWN", 0.50 + (rsi - 55)/200, f"RSI leaning OB {rsi:.1f}"
        else:          sig, c, r = "UP",   0.45,                  f"RSI neutral {rsi:.1f}"
        if cross_up or cross_dn: c, r = c + 0.05, r + " [CROSS]"
        return _result(sig, _clamp(c), r, f"{rsi:.1f}", crossover=cross_up, crossunder=cross_dn)


class MACDStrategy(BaseStrategy):
    name = "macd"
    def predict(self, prices, **kw):
        p = _closes(prices, kw.get("ohlcv", []), 28)
        if p is None: return _no_data()
        macd_line = _ema_series(p, 3) - _ema_series(p, 10)
        sig_line = _ema_series(macd_line, 16)
        hist, prev = macd_line[-1] - sig_line[-1], macd_line[-2] - sig_line[-2]
        cross_up, cross_dn = prev < 0 <= hist, prev > 0 >= hist
        sig = "UP" if hist >= 0 else "DOWN"
        c = 0.52 + min(abs(hist) * 10, 0.30) + (0.07 if (cross_up or cross_dn) else 0)
        r = f"{'Bullish' if hist >= 0 else 'Bearish'} MACD hist {hist:+.4f}" + (" [CROSS]" if (cross_up or cross_dn) else "")
        return _result(sig, _clamp(c), r, f"{hist:.4f}", crossover=cross_up, crossunder=cross_dn)


class StochasticStrategy(BaseStrategy):
    name = "stochastic"
    def predict(self, prices, **kw):
        ohlcv = kw.get("ohlcv", [])
        if not (ohlcv and len(ohlcv) >= 9): return _no_data()
        ks = []
        for i in range(4, len(ohlcv)):
            w = ohlcv[i - 4: i + 1]
            lo, hi = min(float(b[3]) for b in w), max(float(b[2]) for b in w)
            ks.append((float(ohlcv[i][4]) - lo) / (hi - lo) * 100 if hi != lo else 50.0)
        if len(ks) < 4: return _no_data()
        k, d = ks[-1], float(np.mean(ks[-3:]))
        prev_k, prev_d = ks[-2], float(np.mean(ks[-4:-1]))
        cross_up, cross_dn = (prev_k <= prev_d and k > d), (prev_k >= prev_d and k < d)
        if   k < 20: sig, c, r = "UP",   0.62, f"Stoch oversold K={k:.1f}"
        elif k > 80: sig, c, r = "DOWN", 0.62, f"Stoch overbought K={k:.1f}"
        elif k > d:  sig, c, r = "UP",   0.50 + min(abs(k-d)/100, 0.20), f"K above D ({k:.1f}/{d:.1f})"
        else:        sig, c, r = "DOWN", 0.50 + min(abs(k-d)/100, 0.20), f"K below D ({k:.1f}/{d:.1f})"
        if cross_up or cross_dn: c, r = c + 0.07, r + " [CROSS]"
        return _result(sig, _clamp(c), r, f"{k:.1f}", crossover=cross_up, crossunder=cross_dn)


class EMACrossStrategy(BaseStrategy):
    name = "ema_cross"
    def predict(self, prices, **kw):
        p = _closes(prices, kw.get("ohlcv", []), 57)
        if p is None: return _no_data()
        f_diff = float(_ema_series(p, 5)[-1] - _ema_series(p, 13)[-1])
        f_prev = float(_ema_series(p, 5)[-2] - _ema_series(p, 13)[-2])
        s_diff = float(_ema_series(p, 21)[-1] - _ema_series(p, 55)[-1])
        cross_up, cross_dn = f_prev <= 0 < f_diff, f_prev >= 0 > f_diff
        fast = "UP" if f_diff > 0 else "DOWN"
        slow = "UP" if s_diff > 0 else "DOWN"
        c = 0.52 + min(abs(f_diff)/100, 0.30) + (0.08 if (cross_up or cross_dn) else 0) + (0.04 if fast == slow else 0)
        return _result(fast, _clamp(c), f"EMA5 {'>' if f_diff > 0 else '<'} EMA13 by {abs(f_diff):.2f}",
                       f"{f_diff:.2f}", htf_signal=slow, crossover=cross_up, crossunder=cross_dn,
                       mtf_agree=fast == slow, slow_diff=s_diff)


class VWAPStrategy(BaseStrategy):
    """Anchored VWAP, σ-band classified. Anchor = highest-volume bar in last 50."""
    name = "vwap"
    def predict(self, prices, **kw):
        ohlcv = kw.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < 5: return _no_data("No OHLCV")
        bars = ohlcv[-50:]
        vols = [float(k[5]) for k in bars]
        anchor = bars[max(range(len(vols)), key=lambda i: vols[i]):]
        if len(anchor) < 5: anchor = bars
        tps = np.array([(float(k[2])+float(k[3])+float(k[4]))/3 for k in anchor])
        va  = np.array([float(k[5]) for k in anchor])
        if va.sum() == 0: return _no_data("Zero volume")
        vwap = float(np.dot(tps, va) / va.sum())
        sigma = float(np.sqrt(max(np.dot(va, (tps - vwap)**2) / va.sum(), 0.0)))
        cur, prev = prices[-1], (prices[-2] if len(prices) > 1 else prices[-1])
        z = (cur - vwap) / sigma if sigma > 1e-8 else 0.0
        az = abs(z)
        c, label = ((0.78, "3σ") if az >= 3 else (0.68, "2σ") if az >= 2
                     else (0.57, "1σ") if az >= 1 else (0.50 + az * 0.06, "VWAP"))
        cross_up, cross_dn = prev <= vwap < cur, prev >= vwap > cur
        if cross_up or cross_dn: c += 0.05
        return _result("UP" if cur > vwap else "DOWN", _clamp(c),
                       f"AVWAP ${vwap:.0f} {label} z={z:+.2f}", f"${vwap:.0f}",
                       crossover=cross_up, crossunder=cross_dn)


class SupertrendStrategy(BaseStrategy):
    """Supertrend ATR(10, ×3) — flips when price closes through the band."""
    name = "supertrend"
    def predict(self, prices, **kw):
        ohlcv = kw.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < 15: return _no_data("Need 15+ bars")
        h = np.array([float(c[2]) for c in ohlcv])
        l = np.array([float(c[3]) for c in ohlcv])
        c_arr = np.array([float(c[4]) for c in ohlcv])
        n = len(c_arr)
        tr = np.zeros(n); tr[0] = h[0] - l[0]
        for i in range(1, n):
            tr[i] = max(h[i] - l[i], abs(h[i] - c_arr[i-1]), abs(l[i] - c_arr[i-1]))
        atr = _rma(tr, 10)
        bu, bl = (h + l) / 2 + 3 * atr, (h + l) / 2 - 3 * atr
        fu, fl = bu.copy(), bl.copy()
        d = np.ones(n, dtype=int)
        for i in range(1, n):
            fu[i] = bu[i] if (bu[i] < fu[i-1] or c_arr[i-1] > fu[i-1]) else fu[i-1]
            fl[i] = bl[i] if (bl[i] > fl[i-1] or c_arr[i-1] < fl[i-1]) else fl[i-1]
            if   d[i-1] == -1 and c_arr[i] > fu[i-1]: d[i] =  1
            elif d[i-1] ==  1 and c_arr[i] < fl[i-1]: d[i] = -1
            else: d[i] = d[i-1]
        cur, prev = int(d[-1]), int(d[-2])
        cross_up, cross_dn = (prev == -1 and cur == 1), (prev == 1 and cur == -1)
        cur_atr = float(atr[-1])
        band = float(fl[-1] if cur == 1 else fu[-1])
        dist = abs(c_arr[-1] - band) / cur_atr if cur_atr > 0 else 0.0
        c = 0.55 + min(dist * 0.06, 0.25) + (0.08 if (cross_up or cross_dn) else 0)
        return _result("UP" if cur == 1 else "DOWN", _clamp(c),
                       f"ST {'bull' if cur == 1 else 'bear'} dist={dist:.2f}×ATR",
                       f"{cur_atr:.1f}", crossover=cross_up, crossunder=cross_dn)


class ADXStrategy(BaseStrategy):
    """ADX/DMI(14) — trend strength + direction."""
    name = "adx"
    def predict(self, prices, **kw):
        ohlcv = kw.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < 42: return _no_data("Need 42+ bars")
        h = np.array([float(c[2]) for c in ohlcv])
        l = np.array([float(c[3]) for c in ohlcv])
        c_arr = np.array([float(c[4]) for c in ohlcv])
        n = len(c_arr)
        tr = np.zeros(n); dmp = np.zeros(n); dmm = np.zeros(n)
        for i in range(1, n):
            up, dn = h[i] - h[i-1], l[i-1] - l[i]
            tr[i] = max(h[i] - l[i], abs(h[i] - c_arr[i-1]), abs(l[i] - c_arr[i-1]))
            dmp[i] = up if (up > dn and up > 0)   else 0.0
            dmm[i] = dn if (dn > up and dn > 0)   else 0.0
        tr_r, dp_r, dm_r = _rma(tr[1:], 14), _rma(dmp[1:], 14), _rma(dmm[1:], 14)
        with np.errstate(divide="ignore", invalid="ignore"):
            dip = 100 * np.where(tr_r > 0, dp_r / tr_r, 0)
            dim = 100 * np.where(tr_r > 0, dm_r / tr_r, 0)
            dxs = 100 * np.where((dip + dim) > 0, np.abs(dip - dim) / (dip + dim), 0)
        adx = float(_rma(dxs, 14)[-1])
        c = 0.72 if adx >= 30 else 0.60 if adx >= 20 else 0.52 if adx >= 12 else 0.44
        return _result("UP" if dip[-1] > dim[-1] else "DOWN", _clamp(c),
                       f"ADX={adx:.1f} +DI={dip[-1]:.1f} −DI={dim[-1]:.1f}", f"{adx:.1f}")


class WilliamsAlligatorStrategy(BaseStrategy):
    """Jaw(13)/Teeth(8)/Lips(5) SMMA. Sleeping = ranging."""
    name = "alligator"
    def predict(self, prices, **kw):
        p = _closes(prices, kw.get("ohlcv", []), 15)
        if p is None: return _no_data()
        jaw, teeth, lips = _smma_val(p, 13), _smma_val(p, 8), _smma_val(p, 5)
        spread = abs(lips - jaw) / jaw * 100 if jaw else 0
        if lips > teeth > jaw:
            sig, c, r = "UP",   0.56 + min(spread * 4, 0.24), f"Alligator bullish (spread {spread:.3f}%)"
        elif lips < teeth < jaw:
            sig, c, r = "DOWN", 0.56 + min(spread * 4, 0.24), f"Alligator bearish (spread {spread:.3f}%)"
        else:
            sig, c, r = ("UP" if lips > jaw else "DOWN"), 0.45, "Alligator sleeping"
        return _result(sig, _clamp(c), r, f"{spread:.3f}%")


class AccDistStrategy(BaseStrategy):
    """A/D Line — 4-bar CLV×Volume slope."""
    name = "acc_dist"
    def predict(self, prices, **kw):
        ohlcv = kw.get("ohlcv", [])
        if not ohlcv or len(ohlcv) < 10: return _no_data("No OHLCV")
        ad, vals = 0.0, []
        for k in ohlcv:
            h, l, c, v = float(k[2]), float(k[3]), float(k[4]), float(k[5])
            ad += ((c - l) - (h - c)) / (h - l) * v if h != l else 0.0
            vals.append(ad)
        slope = vals[-1] - vals[-5]
        prev_slope = vals[-5] - vals[-9] if len(vals) >= 9 else 0.0
        cross_up, cross_dn = prev_slope <= 0 < slope, prev_slope >= 0 > slope
        ref = max(abs(vals[-1]), 1.0)
        c = 0.52 + min(abs(slope) / ref * 0.5, 0.28)
        sig = "UP" if slope > 0 else "DOWN"
        r = f"A/D {'accumulation' if slope > 0 else 'distribution'} {slope:+.0f}"
        return _result(sig, _clamp(c), r, f"{vals[-1]:.0f}",
                       crossover=cross_up, crossunder=cross_dn)


class DowTheoryStrategy(BaseStrategy):
    """HH+HL = uptrend, LH+LL = downtrend."""
    name = "dow_theory"
    def predict(self, prices, **kw):
        p = _closes(prices, kw.get("ohlcv", []), 10)
        if p is None: return _no_data()
        sh, sl = [], []
        for i in range(4, len(p) - 4):
            w = p[i - 4: i + 5]
            if p[i] >= w.max(): sh.append((i, float(p[i])))
            if p[i] <= w.min(): sl.append((i, float(p[i])))
        if len(sh) < 2 or len(sl) < 2: return _no_data("Insufficient swings")
        h1, h2 = sh[-2][1], sh[-1][1]
        l1, l2 = sl[-2][1], sl[-1][1]
        hh, hl, lh, ll = h2 > h1, l2 > l1, h2 < h1, l2 < l1
        if hh and hl:   sig, c, st = "UP",   0.65, "UPTREND"
        elif lh and ll: sig, c, st = "DOWN", 0.65, "DOWNTREND"
        elif hh:        sig, c, st = "UP",   0.54, "HH+LL"
        elif ll:        sig, c, st = "DOWN", 0.54, "LH+LL"
        else:           sig, c, st = ("UP" if p[-1] > float(np.mean(p[-10:])) else "DOWN"), 0.45, "RANGING"
        return _result(sig, _clamp(c), f"Dow {st} H:{h1:.0f}→{h2:.0f}", st[:8],
                       crossover=hh and hl, crossunder=lh and ll)


class FibPullbackStrategy(BaseStrategy):
    """38.2/50/61.8% retracements as continuation signals."""
    name = "fib_pullback"
    FIBS = [0.236, 0.382, 0.500, 0.618, 0.786]
    def predict(self, prices, **kw):
        p = _closes(prices, kw.get("ohlcv", []), 30)
        if p is None: return _no_data()
        w = p[-30:]
        sw_hi, sw_lo, current = float(w.max()), float(w.min()), float(p[-1])
        rng = sw_hi - sw_lo
        if rng < current * 0.0005: return _no_data("Range too small")
        uptrend = int(w.argmax()) > int(w.argmin())
        pb = (sw_hi - current) / rng if uptrend else (current - sw_lo) / rng
        at_fib = next((lvl for lvl in self.FIBS if abs(pb - lvl) < 0.003), None)
        if at_fib and at_fib >= 0.382:
            sig, c, r = ("UP" if uptrend else "DOWN"), 0.62 + at_fib * 0.10, f"{'Bounce' if uptrend else 'Reject'} at {at_fib*100:.0f}% Fib"
        elif pb > 0.786:
            sig, c, r = ("DOWN" if uptrend else "UP"), 0.58, f"Failed {'bounce' if uptrend else 'breakdown'} ({pb*100:.1f}%)"
        else:
            sig, c, r = ("UP" if uptrend else "DOWN"), 0.47, f"{'Up' if uptrend else 'Down'}trend pullback {pb*100:.1f}%"
        return _result(sig, _clamp(c), r, f"{pb*100:.1f}%",
                       crossover=bool(at_fib and uptrend),
                       crossunder=bool(at_fib and not uptrend))


class HarmonicPatternStrategy(BaseStrategy):
    """XABCD Fib-ratio patterns."""
    name = "harmonic"
    PATTERNS = {"GARTLEY":   ((0.55, 0.68), (0.35, 0.90)),
                "BAT":       ((0.35, 0.52), (0.35, 0.90)),
                "CRAB":      ((0.35, 0.65), (0.35, 0.90)),
                "BUTTERFLY": ((0.68, 0.82), (0.35, 0.90))}
    def predict(self, prices, **kw):
        p = _closes(prices, kw.get("ohlcv", []), 30)
        if p is None: return _no_data()
        w = p[-min(50, len(p)):]
        pivots = []
        for i in range(3, len(w) - 3):
            ww = w[i-3: i+4]
            if w[i] >= ww.max():   pivots.append(("H", float(w[i])))
            elif w[i] <= ww.min(): pivots.append(("L", float(w[i])))
        filt: List = []
        for pt in pivots:
            if not filt or filt[-1][0] != pt[0]: filt.append(pt)
        if len(filt) < 4: return _no_data("Not enough pivots")
        X, A, B, C = filt[-4][1], filt[-3][1], filt[-2][1], filt[-1][1]
        XA, AB, BC = abs(A - X), abs(B - A), abs(C - B)
        if XA < 1e-6 or AB < 1e-6: return _no_data("Zero swing")
        ab_xa, bc_ab = AB / XA, BC / AB
        detected = next((n for n, (rab, rbc) in self.PATTERNS.items()
                          if rab[0] <= ab_xa <= rab[1] and rbc[0] <= bc_ab <= rbc[1]), None)
        bullish = A < X
        if detected:
            sig, c, r = ("UP" if bullish else "DOWN"), 0.62, f"{detected} {'bull' if bullish else 'bear'}"
        else:
            sig, c, r = ("UP" if p[-1] > float(np.mean(p[-10:])) else "DOWN"), 0.44, f"No harmonic (AB/XA={ab_xa:.2f})"
        return _result(sig, _clamp(c), r, detected or "—",
                       crossover=bool(detected and bullish),
                       crossunder=bool(detected and not bullish))


class LinearRegressionChannel(BaseStrategy):
    """Linear regression slope on last 30 closes. Key kept as 'ml_logistic' for back-compat."""
    name = "ml_logistic"
    def predict(self, prices, **kw):
        ohlcv = kw.get("ohlcv", [])
        src = (np.array([float(c[4]) for c in ohlcv[-35:]], dtype=float)
                if ohlcv and len(ohlcv) >= 35
                else np.array(prices[-35:], dtype=float) if len(prices) >= 35 else None)
        if src is None: return _no_data()
        recent = src[-30:]
        x = np.arange(30, dtype=float)
        slope = np.sum((x - x.mean()) * (recent - recent.mean())) / np.sum((x - x.mean()) ** 2)
        y_hat = slope * x + (recent.mean() - slope * x.mean())
        ss_res = np.sum((recent - y_hat) ** 2); ss_tot = np.sum((recent - recent.mean()) ** 2)
        r2 = max(0.0, 1.0 - ss_res / ss_tot) if ss_tot > 1e-10 else 0.0
        slope_pct = slope / recent[-1] * 100
        c = max(0.40, min(0.85, 0.50 + r2 * 0.25 + min(abs(slope_pct) * 15, 0.10)))
        return _result("UP" if slope > 0 else "DOWN", round(c, 4),
                       f"LR slope={slope_pct:+.4f}%/bar R²={r2:.3f}", f"{r2:.3f}")


ALL_STRATEGIES: List[BaseStrategy] = [
    RSIStrategy(), MACDStrategy(), StochasticStrategy(),
    EMACrossStrategy(), VWAPStrategy(),
    SupertrendStrategy(), ADXStrategy(),
    WilliamsAlligatorStrategy(), AccDistStrategy(), DowTheoryStrategy(),
    FibPullbackStrategy(), HarmonicPatternStrategy(),
]


def get_all_predictions(prices: List[float], **kwargs) -> Dict[str, Dict]:
    """Run every strategy. Strategy errors degrade to a defensive UP/0.45 — they
    are voters, not single points of failure."""
    results = {}
    for strategy in ALL_STRATEGIES:
        try:
            results[strategy.name] = strategy.predict(prices, **kwargs)
        except Exception as exc:
            results[strategy.name] = {"signal": "UP", "confidence": 0.45,
                                       "reasoning": f"Error: {exc}", "value": "ERR",
                                       "htf_signal": "N/A", "crossover": False,
                                       "crossunder": False, "mtf_agree": None}
    return results


# ── Ensemble: weighted vote across strategies. Weights live-update from
#    accuracy stats — strategies whose calls historically agreed with the
#    bar's actual direction earn larger weight, ones below random get muted. ──

_DISABLED_THRESH, _WEAK_THRESH, _STRONG_THRESH, _EXCELLENT_THRESH = 0.40, 0.50, 0.60, 0.65


def accuracy_to_label(accuracy: float, total: int, min_samples: int = 10) -> str:
    if total < min_samples:           return "LEARNING"
    if accuracy < _DISABLED_THRESH:   return "DISABLED"
    if accuracy < _WEAK_THRESH:       return "WEAK"
    if accuracy < _STRONG_THRESH:     return "MARGINAL"
    if accuracy < _EXCELLENT_THRESH:  return "RELIABLE"
    return "EXCELLENT"


def accuracy_to_target_weight(accuracy: float, total: int, min_samples: int = 10) -> float:
    if total < min_samples: return 1.0
    if accuracy < _DISABLED_THRESH: return 0.05
    if accuracy < _WEAK_THRESH:
        t = (accuracy - _DISABLED_THRESH) / (_WEAK_THRESH - _DISABLED_THRESH)
        return round(0.10 + t * 0.40, 3)
    if accuracy < _STRONG_THRESH:
        t = (accuracy - _WEAK_THRESH) / (_STRONG_THRESH - _WEAK_THRESH)
        return round(0.50 + t * 0.70, 3)
    if accuracy < _EXCELLENT_THRESH:
        t = (accuracy - _STRONG_THRESH) / (_EXCELLENT_THRESH - _STRONG_THRESH)
        return round(1.20 + t * 0.80, 3)
    t = min(1.0, (accuracy - _EXCELLENT_THRESH) / 0.15)
    return round(min(3.0, 2.00 + t * 1.00), 3)


class EnsemblePredictor:
    """Weighted voting + dynamic weight adjustment from rolling accuracy."""

    def __init__(self, initial_weights: Optional[Dict[str, float]] = None):
        self.weights = dict(initial_weights or {})
        self.default_weight = 1.0

    def predict(self, strategy_predictions: Dict[str, Dict]) -> Dict:
        if not strategy_predictions:
            return {"signal": "UP", "confidence": 0.5, "up_probability": 0.5,
                    "bullish_count": 0, "bearish_count": 0}
        up_score = down_score = 0.0
        for name, pred in strategy_predictions.items():
            w = self.weights.get(name, self.default_weight)
            conf = pred.get("confidence", 0.5)
            if pred["signal"] == "UP":   up_score   += conf * w
            elif pred["signal"] == "DOWN": down_score += conf * w
        total = up_score + down_score
        up_prob = up_score / total if total > 0 else 0.5
        confidence = max(up_prob, 1 - up_prob)
        # Below 65% confidence the signal is too close to a coin-flip to bet on.
        signal = "NEUTRAL" if confidence < 0.65 else ("UP" if up_prob > 0.5 else "DOWN")
        return {
            "signal": signal, "confidence": confidence, "up_probability": up_prob,
            "bullish_count":  sum(1 for p in strategy_predictions.values() if p["signal"] == "UP"),
            "bearish_count":  sum(1 for p in strategy_predictions.values() if p["signal"] == "DOWN"),
            "weighted_up_score":   up_score,
            "weighted_down_score": down_score,
        }

    def update_weights(self, accuracies: Dict[str, float], min_samples: int = 10,
                       learning_rate: float = 0.15,
                       counts: Optional[Dict[str, int]] = None):
        for name, accuracy in accuracies.items():
            count  = (counts or {}).get(name, min_samples)
            target = accuracy_to_target_weight(accuracy, count, min_samples)
            current = self.weights.get(name, self.default_weight)
            self.weights[name] = round(current * (1 - learning_rate) + target * learning_rate, 3)

    def update_weights_from_full_stats(self, stats: Dict[str, Dict],
                                       min_samples: int = 10, learning_rate: float = 0.15):
        accs   = {n: s["accuracy"] for n, s in stats.items()}
        counts = {n: s["total"]    for n, s in stats.items()}
        self.update_weights(accs, min_samples, learning_rate, counts)

    def get_weights(self) -> Dict[str, float]:
        return dict(self.weights)


# ── EV calculator — translates a model probability + market odds into expected
#    value, edge, Kelly fraction, and a discrete signal tier. ──

@dataclass
class EVResult:
    expected_value:      float
    edge:                float
    implied_probability: float
    model_probability:   float
    kelly_fraction:      float
    signal:              str
    reasoning:           str


def calculate_ev(model_probability: float, market_odds: float, max_kelly: float = 0.25,
                 min_ev: float = 0.05, strong_ev: float = 0.15) -> EVResult:
    implied = 1.0 / (1.0 + market_odds) if market_odds > 0 else 1.0
    edge = model_probability - implied
    ev   = (model_probability * market_odds) - (1 - model_probability)
    kelly = min(edge / market_odds, max_kelly) if (market_odds > 0 and edge > 0) else 0.0
    if ev >= strong_ev:
        signal, reasoning = "STRONG_ENTER", f"+EV {ev:.3f} above strong threshold; Kelly {kelly*100:.1f}%."
    elif ev >= min_ev:
        signal, reasoning = "MARGINAL", f"+EV {ev:.3f} positive but marginal."
    elif ev > 0:
        signal, reasoning = "WEAK", f"Slight +EV ({ev:.3f}) below threshold."
    else:
        signal, reasoning = "PASS", f"Negative EV ({ev:.3f})."
    return EVResult(ev, edge, implied, model_probability, kelly, signal, reasoning)


# ── 4. MICROSTRUCTURE SIGNALS — public-API fetchers gathered in parallel; failures degrade individually. ──

_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=9)


SIGNAL_SOURCES: Dict[str, Dict[str, str]] = {
    "order_book":          {"api": "Binance + Bybit + Coinbase + Kraken depth", "scope": "Aggregate book within 0.25/0.5/1% of mid"},
    "long_short":          {"api": "Binance global+top long/short ratio",       "scope": "Binance perp account ratios"},
    "taker_flow":          {"api": "Binance takerlongshortRatio",                "scope": "Binance perp 5m taker volume"},
    "oi_funding":          {"api": "Binance openInterest + premiumIndex",        "scope": "Binance perp OI + funding"},
    "oi_velocity":         {"api": "Binance openInterestHist",                   "scope": "OI 30m change rate"},
    "liquidations":        {"api": "OKX liquidation-orders cross",               "scope": "OKX perp last 5m"},
    "bybit_liquidations":  {"api": "OKX liquidation-orders isolated",            "scope": "OKX isolated last 15m"},
    "coinglass_liquidations":{"api":"CoinGlass aggregated-history",              "scope": "Cross-exchange aggregated 5m"},
    "fear_greed":          {"api": "alternative.me/fng",                         "scope": "Daily macro sentiment"},
    "deribit_dvol":        {"api": "Deribit btcdvol_usdc",                       "scope": "Deribit 30d implied vol"},
    "deribit_options":     {"api": "Deribit get_book_summary_by_currency",       "scope": "P/C OI + max pain"},
    "deribit_skew_term":   {"api": "Deribit get_book_summary_by_currency",       "scope": "25Δ RR + ATM term + P/C volume"},
    "spot_perp_basis":     {"api": "Binance spot bookTicker + premiumIndex",     "scope": "Perp mark − spot mid"},
    "cvd":                 {"api": "Binance klines spot+perp",                   "scope": "1h CVD"},
    "btc_onchain":         {"api": "bitcoin-data.com SOPR + MVRV-zscore",        "scope": "Daily on-chain"},
    "top_position_ratio":  {"api": "Binance topLongShortPositionRatio",          "scope": "Top 20% by margin notional"},
    "funding_trend":       {"api": "Binance fundingRate history",                "scope": "6-period funding moving average"},
    "coinalyze":           {"api": "Coinalyze funding-rate",                     "scope": "Cross-exchange aggregate funding"},
}


def _attach_source(payload: Optional[Dict], key: str) -> Optional[Dict]:
    if isinstance(payload, dict):
        src = SIGNAL_SOURCES.get(key)
        if src: payload["source"] = src
    return payload


async def _http_get(url: str, headers: Optional[Dict] = None, params: Optional[Dict] = None) -> Any:
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(connector=connector, timeout=_HTTP_TIMEOUT) as session:
        async with session.get(url, headers=headers or {}, params=params) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} from {url}")
            return await resp.json(content_type=None)


def _band_depth(bids: list, asks: list, mid: float, pct: float):
    lo, hi = mid * (1 - pct/100), mid * (1 + pct/100)
    bid_btc = sum(float(b[1]) for b in bids if float(b[0]) >= lo)
    ask_btc = sum(float(a[1]) for a in asks if float(a[0]) <= hi)
    return bid_btc, ask_btc


async def _fetch_venue_book(venue: str) -> Optional[Dict]:
    try:
        if venue == "binance_spot":
            d = await _http_get("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5000")
            bids = [(float(b[0]), float(b[1])) for b in d.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in d.get("asks", [])]
        elif venue == "bybit_spot":
            d = await _http_get("https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200")
            r = d.get("result") or {}
            bids = [(float(b[0]), float(b[1])) for b in r.get("b", [])]
            asks = [(float(a[0]), float(a[1])) for a in r.get("a", [])]
        elif venue == "coinbase":
            d = await _http_get("https://api.exchange.coinbase.com/products/BTC-USD/book?level=2")
            bids = [(float(b[0]), float(b[1])) for b in d.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in d.get("asks", [])]
        elif venue == "kraken":
            d = await _http_get("https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=500")
            book = (d.get("result") or {}).get("XXBTZUSD", {})
            bids = [(float(b[0]), float(b[1])) for b in book.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in book.get("asks", [])]
        else:
            return None
        if not bids or not asks:
            return None
        return {"venue": venue, "bids": bids, "asks": asks,
                "mid": (bids[0][0] + asks[0][0]) / 2}
    except Exception as exc:
        logger.debug("depth fetch %s: %s", venue, exc)
        return None


async def _fetch_order_book() -> Dict:
    """Multi-venue depth, 0.5% band imbalance is the conventional defense zone."""
    venues = ["binance_spot", "bybit_spot", "coinbase", "kraken"]
    books = [b for b in await asyncio.gather(*(_fetch_venue_book(v) for v in venues)) if b]
    if not books:
        return {"signal": "UNAVAILABLE", "data_available": False,
                "interpretation": "All venue fetches failed."}
    agg_mid = sum(b["mid"] for b in books) / len(books)
    tot = {p: [0.0, 0.0] for p in (0.25, 0.5, 1.0)}
    for b in books:
        for p in tot:
            bb, aa = _band_depth(b["bids"], b["asks"], b["mid"], p)
            tot[p][0] += bb; tot[p][1] += aa
    bid_05, ask_05 = tot[0.5]
    imb_05 = (bid_05 - ask_05) / (bid_05 + ask_05) * 100 if (bid_05 + ask_05) > 0 else 0.0
    sig = "BULLISH" if imb_05 > 8 else "BEARISH" if imb_05 < -8 else "NEUTRAL"
    interp = (
        f"Aggregate {len(books)}-venue book within 0.5% of ${agg_mid:,.0f}: "
        f"{bid_05:.1f} BTC bids vs {ask_05:.1f} BTC asks ({imb_05:+.1f}%)."
    )
    return {
        "mid_usd":               round(agg_mid, 2),
        "bid_depth_025pct_btc":  round(tot[0.25][0], 2),
        "ask_depth_025pct_btc":  round(tot[0.25][1], 2),
        "bid_depth_05pct_btc":   round(bid_05, 2),
        "ask_depth_05pct_btc":   round(ask_05, 2),
        "bid_depth_1pct_btc":    round(tot[1.0][0], 2),
        "ask_depth_1pct_btc":    round(tot[1.0][1], 2),
        "imbalance_05pct_pct":   round(imb_05, 2),
        "venues_included":       [b["venue"] for b in books],
        "bid_vol_btc":           round(bid_05, 2),
        "ask_vol_btc":           round(ask_05, 2),
        "imbalance_pct":         round(imb_05, 2),
        "signal":                sig,
        "data_available":        True,
        "interpretation":        interp,
    }


async def _fetch_long_short() -> Dict:
    try:
        gl, tp = await asyncio.gather(
            _http_get("https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1"),
            _http_get("https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1"),
        )
        g, tp0 = (gl[0] if gl else {}), (tp[0] if tp else {})
        lsr = float(g.get("longShortRatio", 1.0))
        lp  = float(g.get("longAccount", 0.5)) * 100; sp = 100 - lp
        tlp = float(tp0.get("longAccount", 0.5)) * 100; tsp = 100 - tlp
    except Exception as exc:
        logger.debug("Binance L/S failed: %s — Bybit fallback", exc)
        try:
            data = await _http_get("https://api.bybit.com/v5/market/account-ratio?category=linear&symbol=BTCUSDT&period=5min&limit=1")
            row = ((data.get("result") or {}).get("list") or [{}])[0]
            lp = float(row.get("buyRatio", 0.5)) * 100; sp = 100 - lp
            lsr = lp / sp if sp > 0 else 1.0; tlp, tsp = lp, sp
        except Exception as exc2:
            return {"signal": "UNAVAILABLE", "data_available": False,
                    "interpretation": f"L/S unavailable: {exc2}"}
    div = lp - tlp
    r_sig = "BEARISH_CONTRARIAN" if lsr > 1.35 else "BULLISH_CONTRARIAN" if lsr < 0.75 else "NEUTRAL"
    s_sig = "BULLISH" if tlp > 60 else "BEARISH" if tlp < 40 else "NEUTRAL"
    interp = (f"Top accounts {tlp:.0f}% long vs all accounts {lp:.0f}% long "
              f"({abs(div):.1f}% divergence)." if abs(div) > 10 else
              f"Account tiers aligned ({abs(div):.1f}% diff).")
    return {"retail_lsr": round(lsr, 4),
            "retail_long_pct": round(lp, 1),  "retail_short_pct": round(sp, 1),
            "top_accounts_long_pct": round(tlp, 1), "top_accounts_short_pct": round(tsp, 1),
            "retail_signal_contrarian": r_sig, "top_accounts_signal": s_sig,
            "top_vs_all_div_pct": round(div, 1),
            # Back-compat aliases
            "smart_money_long_pct": round(tlp, 1), "smart_money_short_pct": round(tsp, 1),
            "smart_money_signal": s_sig, "smart_vs_retail_div_pct": round(div, 1),
            "data_available": True,
            "interpretation": interp, "signal": s_sig}


async def _fetch_taker_flow() -> Dict:
    try:
        data = await _http_get("https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=3")
        latest = data[-1] if data else {}
        if not latest or "buySellRatio" not in latest:
            raise ValueError("missing buySellRatio")
        bsr = float(latest["buySellRatio"])
        bv  = float(latest.get("buyVol", 0)); sv = float(latest.get("sellVol", 0))
        trend_data = data
    except Exception as exc:
        logger.debug("Binance taker flow failed: %s — OKX fallback", exc)
        try:
            raw = await _http_get("https://www.okx.com/api/v5/rubik/stat/taker-volume?ccy=BTC&instType=SWAP&period=5m&limit=3")
            rows = raw.get("data") or []
            if not rows:
                raise ValueError("empty")
            bv, sv = float(rows[0][1]), float(rows[0][2])
            bsr = bv / sv if sv > 0 else 1.0
            trend_data = [{"buySellRatio": float(r[1]) / float(r[2]) if float(r[2]) > 0 else 1.0}
                           for r in reversed(rows)]
        except Exception:
            return {"signal": "UNAVAILABLE", "data_available": False,
                    "interpretation": "Taker flow unavailable."}
    sig = "BULLISH" if bsr > 1.12 else "BEARISH" if bsr < 0.90 else "NEUTRAL"
    if len(trend_data) >= 3:
        ratios = [float(d.get("buySellRatio", 1)) for d in trend_data]
        rising  = ratios[-1] > ratios[-2] * 1.02 and ratios[-2] > ratios[-3] * 1.02
        falling = ratios[-1] < ratios[-2] * 0.98 and ratios[-2] < ratios[-3] * 0.98
        trend = "ACCELERATING_BULLISH" if rising else "ACCELERATING_BEARISH" if falling else "MIXED"
    else:
        trend = "INSUFFICIENT_DATA"
    interp = f"BSR={bsr:.3f} buy={bv:.1f} sell={sv:.1f} BTC."
    return {"buy_sell_ratio": round(bsr, 4),
            "taker_buy_vol_btc": round(bv, 1), "taker_sell_vol_btc": round(sv, 1),
            "signal": sig, "data_available": True, "trend_3bars": trend,
            "interpretation": interp}


async def _fetch_oi_velocity() -> Dict:
    try:
        data = await _http_get("https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=6")
        oi_vals = [float(d.get("sumOpenInterest", 0)) for d in data]
        if len(oi_vals) < 2:
            raise ValueError("Insufficient")
    except Exception as exc:
        logger.debug("Binance OI velocity failed: %s — Bybit fallback", exc)
        try:
            raw = await _http_get("https://api.bybit.com/v5/market/open-interest?category=linear&symbol=BTCUSDT&intervalTime=5min&limit=6")
            rows = (raw.get("result") or {}).get("list") or []
            if len(rows) < 2: raise ValueError("Insufficient")
            oi_vals = [float(r.get("openInterest", 0)) for r in reversed(rows)]
        except Exception:
            return {"signal": "UNAVAILABLE", "data_available": False,
                    "interpretation": "OI velocity unavailable."}
    chg = (oi_vals[-1] - oi_vals[0]) / oi_vals[0] * 100 if oi_vals[0] else 0.0
    bar_chg = (oi_vals[-1] - oi_vals[-2]) / oi_vals[-2] * 100 if oi_vals[-2] else 0.0
    sig = "BULLISH" if chg > 0.3 else "BEARISH" if chg < -0.3 else "NEUTRAL"
    return {"oi_current_btc": round(oi_vals[-1], 1),
            "oi_change_30m_pct": round(chg, 4),
            "oi_change_1bar_pct": round(bar_chg, 4),
            "signal": sig, "data_available": True,
            "interpretation": f"OI {chg:+.2f}% over 30m; last bar {bar_chg:+.2f}%."}


async def _fetch_liquidations() -> Dict:
    """OKX cross-margin BTC perp, last 5 minutes."""
    try:
        data = await _http_get(
            "https://www.okx.com/api/v5/public/liquidation-orders"
            "?instType=SWAP&mgnMode=cross&instId=BTC-USDT-SWAP&state=filled&limit=100")
    except Exception as exc:
        return {"signal": "UNAVAILABLE", "data_available": False,
                "interpretation": f"Liq fetch failed: {exc}"}
    rows = []
    for ev in (data.get("data") or []):
        rows.extend(ev.get("details") or [])
    if not rows:
        return {"total": 0, "long_liq_count": 0, "short_liq_count": 0,
                "long_liq_usd": 0, "short_liq_usd": 0, "velocity_per_min": 0.0,
                "signal": "NEUTRAL", "data_available": True,
                "interpretation": "No recent liquidations."}
    now_ms = time.time() * 1000; cutoff = now_ms - 300_000
    recent = [r for r in rows if float(r.get("ts", 0)) >= cutoff] or rows
    CTV = 0.01  # OKX BTC-USDT-SWAP contract value
    longs  = [r for r in recent if r.get("posSide", "").lower() == "long"]
    shorts = [r for r in recent if r.get("posSide", "").lower() == "short"]
    lvol = sum(float(r.get("sz", 0)) * CTV * float(r.get("bkPx", 0)) for r in longs)
    svol = sum(float(r.get("sz", 0)) * CTV * float(r.get("bkPx", 0)) for r in shorts)
    velocity = round(len(recent) / 5.0, 1)
    sig = "BEARISH" if lvol > svol * 1.5 else "BULLISH" if svol > lvol * 1.5 else "NEUTRAL"
    return {"total": len(recent),
            "long_liq_count": len(longs), "short_liq_count": len(shorts),
            "long_liq_usd": round(lvol, 0), "short_liq_usd": round(svol, 0),
            "velocity_per_min": velocity,
            "signal": sig, "data_available": True,
            "interpretation": f"{len(longs)} long / {len(shorts)} short forced; ${lvol:,.0f} L / ${svol:,.0f} S."}


async def _fetch_okx_isolated_liquidations() -> Dict:
    """OKX isolated-margin BTC perp — independent confirmation against cross."""
    try:
        data = await _http_get("https://www.okx.com/api/v5/public/liquidation-orders"
                               "?instType=SWAP&mgnMode=isolated&instId=BTC-USDT-SWAP&state=filled&limit=100")
    except Exception:
        return {"signal": "NEUTRAL", "interpretation": "No isolated-margin liqs."}
    rows = [d for ev in (data.get("data") or []) for d in (ev.get("details") or [])]
    if not rows:
        return {"total": 0, "long_liq_usd": 0, "short_liq_usd": 0,
                "signal": "NEUTRAL", "interpretation": "No isolated liquidations."}
    cutoff = time.time() * 1000 - 900_000
    recent = [r for r in rows if float(r.get("ts", 0)) >= cutoff] or rows
    CTV = 0.01
    l_usd = sum(float(r.get("sz", 0)) * CTV * float(r.get("bkPx", 0))
                for r in recent if r.get("posSide", "").lower() == "long")
    s_usd = sum(float(r.get("sz", 0)) * CTV * float(r.get("bkPx", 0))
                for r in recent if r.get("posSide", "").lower() == "short")
    sig = "BEARISH" if l_usd > s_usd * 1.5 else "BULLISH" if s_usd > l_usd * 1.5 else "NEUTRAL"
    return {"total": len(recent),
            "long_liq_usd": round(l_usd, 0), "short_liq_usd": round(s_usd, 0),
            "signal": sig,
            "interpretation": f"OKX isolated: ${l_usd:,.0f}L / ${s_usd:,.0f}S."}


async def _fetch_fear_greed() -> Dict:
    data = await _http_get("https://api.alternative.me/fng/?limit=2")
    items = data.get("data", [])
    cur, prev = (items[0] if items else {}), (items[1] if len(items) > 1 else {})
    v, label = int(cur.get("value", 50)), cur.get("value_classification", "Neutral")
    pv = int(prev.get("value", v))
    sig = "BULLISH_CONTRARIAN" if v < 30 else "BEARISH_CONTRARIAN" if v > 75 else "NEUTRAL"
    return {"value": v, "label": label, "previous_day": pv, "daily_delta": v - pv,
            "signal": sig,
            "interpretation": f"F&G {v} ({label}); regime context only."}


async def _fetch_deribit_dvol() -> Dict:
    data = await _http_get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btcdvol_usdc")
    dvol = float((data.get("result") or {}).get("index_price", 60))
    sig = "BEARISH" if dvol > 80 else "BULLISH" if dvol < 40 else "NEUTRAL"
    return {"dvol_pct": round(dvol, 2), "signal": sig,
            "interpretation": f"DVOL {dvol:.1f}% — {'extreme' if dvol > 80 else 'calm' if dvol < 40 else 'normal'} vol regime."}


async def _fetch_deribit_options() -> Dict:
    summaries, idx_r = await asyncio.gather(
        _http_get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"),
        _http_get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd"),
    )
    rows = summaries.get("result") or []
    spot = float((idx_r.get("result") or {}).get("index_price", 0))
    if not rows:
        return {"signal": "UNAVAILABLE", "interpretation": "Options unavailable."}
    call_oi = put_oi = 0.0; strikes: Dict[float, Dict] = {}
    for s in rows:
        parts = s.get("instrument_name", "").split("-")
        if len(parts) < 4: continue
        try: strike = float(parts[-2])
        except ValueError: continue
        oi = float(s.get("open_interest", 0))
        if parts[-1] == "C":
            call_oi += oi; strikes.setdefault(strike, {"c": 0, "p": 0})["c"] += oi
        elif parts[-1] == "P":
            put_oi += oi; strikes.setdefault(strike, {"c": 0, "p": 0})["p"] += oi
    pcr = put_oi / call_oi if call_oi > 0 else 1.0
    max_pain = spot
    if strikes and spot > 0:
        min_pain = float("inf")
        for ts in sorted(strikes):
            pain = sum(max(0, ts - k) * v["c"] + max(0, k - ts) * v["p"]
                       for k, v in strikes.items())
            if pain < min_pain: min_pain, max_pain = pain, ts
    dist = (max_pain - spot) / spot * 100 if spot > 0 else 0
    sig = "BEARISH_CONTRARIAN" if pcr > 1.3 else "BULLISH_CONTRARIAN" if pcr < 0.6 else "NEUTRAL"
    return {"put_oi_btc": round(put_oi, 1), "call_oi_btc": round(call_oi, 1),
            "put_call_ratio": round(pcr, 4), "max_pain_usd": round(max_pain, 0),
            "dist_to_pain_pct": round(dist, 2), "signal": sig,
            "interpretation": f"P/C ratio {pcr:.3f}; max pain ${max_pain:,.0f} ({dist:+.1f}%)."}


async def _fetch_deribit_skew_term() -> Dict:
    """25Δ risk reversal + ATM IV term structure + P/C volume."""
    try:
        s_r, idx_r = await asyncio.gather(
            _http_get("https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"),
            _http_get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd"),
        )
    except Exception:
        return {"signal": "UNAVAILABLE", "data_available": False,
                "interpretation": "Skew/term unavailable."}
    rows = s_r.get("result") or []
    spot = float((idx_r.get("result") or {}).get("index_price", 0))
    if not rows or spot <= 0:
        return {"signal": "UNAVAILABLE", "data_available": False,
                "interpretation": "Skew/term empty."}
    now_ts = time.time()
    per_expiry: Dict[str, Dict] = {}
    call_vol = put_vol = 0.0
    for s in rows:
        parts = s.get("instrument_name", "").split("-")
        if len(parts) < 4: continue
        try:
            strike = float(parts[2])
            expiry_dt = datetime.strptime(parts[1], "%d%b%y").replace(tzinfo=timezone.utc)
            days = (expiry_dt.timestamp() - now_ts) / 86400
        except Exception:
            continue
        if days < 0 or days > 365: continue
        iv = float(s.get("mark_iv") or 0)
        delta = s.get("greeks", {}).get("delta") if isinstance(s.get("greeks"), dict) else s.get("delta")
        try: delta = float(delta) if delta is not None else None
        except (TypeError, ValueError): delta = None
        v = float(s.get("volume") or 0)
        if parts[3] == "C": call_vol += v
        elif parts[3] == "P": put_vol += v
        bucket = per_expiry.setdefault(parts[1], {"days": days, "calls": [], "puts": []})
        entry = {"strike": strike, "iv": iv, "delta": delta, "volume": v}
        if parts[3] == "C": bucket["calls"].append(entry)
        elif parts[3] == "P": bucket["puts"].append(entry)

    def _atm(bucket):
        opts = bucket["calls"] + bucket["puts"]
        if not opts: return None
        nearest = min(opts, key=lambda o: abs(o["strike"] - spot))
        ivs = [o["iv"] for o in opts if o["strike"] == nearest["strike"] and o["iv"] > 0]
        return sum(ivs) / len(ivs) if ivs else None

    def _near(target):
        if not per_expiry: return None
        return min(per_expiry.values(), key=lambda b: abs(b["days"] - target))

    e7, e30, e90 = _near(7), _near(30), _near(90)
    iv7  = _atm(e7)  if e7  else None
    iv30 = _atm(e30) if e30 else None
    iv90 = _atm(e90) if e90 else None
    inverted = iv7 is not None and iv30 is not None and iv7 > iv30 + 3
    rr_30d = None
    if e30:
        cd = [c for c in e30["calls"] if c["delta"] is not None and c["iv"] > 0]
        pd = [p for p in e30["puts"]  if p["delta"] is not None and p["iv"] > 0]
        if cd and pd:
            c25 = min(cd, key=lambda o: abs(o["delta"] - 0.25))
            p25 = min(pd, key=lambda o: abs(o["delta"] + 0.25))
            rr_30d = c25["iv"] - p25["iv"]
    skew_sig = "BULLISH" if (rr_30d is not None and rr_30d > 1.5) else \
               "BEARISH" if (rr_30d is not None and rr_30d < -1.5) else "NEUTRAL"
    pcv = put_vol / call_vol if call_vol > 0 else 1.0
    sig = skew_sig if skew_sig != "NEUTRAL" else ("BEARISH" if inverted else "NEUTRAL")
    parts: List[str] = []
    if rr_30d is not None: parts.append(f"30d 25Δ RR {rr_30d:+.1f}%")
    if iv7 and iv30: parts.append(f"7d IV {iv7:.1f} / 30d {iv30:.1f}")
    if call_vol: parts.append(f"P/C vol {pcv:.2f}")
    return {"rr_25d_30d_pct": round(rr_30d, 2) if rr_30d is not None else None,
            "iv_7d_atm_pct":  round(iv7,  2) if iv7  is not None else None,
            "iv_30d_atm_pct": round(iv30, 2) if iv30 is not None else None,
            "iv_90d_atm_pct": round(iv90, 2) if iv90 is not None else None,
            "term_inverted": inverted,
            "put_volume_btc": round(put_vol, 1), "call_volume_btc": round(call_vol, 1),
            "put_call_volume_ratio": round(pcv, 4),
            "skew_signal": skew_sig, "signal": sig, "data_available": True,
            "interpretation": "; ".join(parts) if parts else "skew/term flat"}


async def _fetch_spot_perp_basis() -> Dict:
    try:
        spot_r, perp_r = await asyncio.gather(
            _http_get("https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT"),
            _http_get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"),
        )
        spot_mid = (float(spot_r["bidPrice"]) + float(spot_r["askPrice"])) / 2
        perp_mark = float(perp_r["markPrice"])
    except Exception as exc:
        return {"signal": "UNAVAILABLE", "data_available": False,
                "interpretation": f"Basis unavailable: {exc}"}
    basis_usd = perp_mark - spot_mid
    basis_pct = basis_usd / spot_mid * 100 if spot_mid else 0
    sig = "BULLISH" if basis_pct > 0.08 else "BEARISH" if basis_pct < -0.08 else "NEUTRAL"
    return {"spot_mid": round(spot_mid, 2), "perp_mark": round(perp_mark, 2),
            "basis_usd": round(basis_usd, 2), "basis_pct": round(basis_pct, 4),
            "signal": sig, "data_available": True,
            "interpretation": f"Perp {basis_usd:+.2f} vs spot ({basis_pct:+.3f}%)."}


async def _fetch_cvd() -> Dict:
    """Cumulative Volume Delta — divergence between price + flow direction."""
    try:
        perp_k, spot_k = await asyncio.gather(
            _http_get("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=12"),
            _http_get("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=12"),
        )
    except Exception:
        return {"signal": "UNAVAILABLE", "data_available": False,
                "interpretation": "CVD unavailable."}
    def _sum(klines):
        cum = 0.0
        for k in klines:
            vol, taker_buy = float(k[5]), float(k[9])
            cum += taker_buy - (vol - taker_buy)
        return cum
    perp = _sum(perp_k); spot = _sum(spot_k)
    total = perp + spot
    sig = "BULLISH" if total > 400 else "BEARISH" if total < -400 else "NEUTRAL"
    p_open = float(perp_k[0][1]) if perp_k else 0.0
    p_close = float(perp_k[-1][4]) if perp_k else 0.0
    move = (p_close - p_open) / p_open * 100 if p_open else 0
    div = spot - perp
    return {"perp_cvd_1h_btc": round(perp, 1), "spot_cvd_1h_btc": round(spot, 1),
            "aggregate_cvd_1h_btc": round(total, 1),
            "spot_perp_divergence_btc": round(div, 1),
            "price_move_1h_pct": round(move, 3),
            "signal": sig, "data_available": True,
            "interpretation": f"1h CVD {total:+.0f} BTC, price {move:+.2f}%, spot-perp div {div:+.0f}."}


async def _fetch_top_position_ratio() -> Dict:
    try:
        data = await _http_get("https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=1")
        row = data[0] if data else {}
        lsr = float(row.get("longShortRatio", 1.0))
        lp  = float(row.get("longAccount", 0.5)) * 100
    except Exception as exc:
        return {"signal": "UNAVAILABLE", "interpretation": f"Top pos ratio failed: {exc}"}
    sig = "BULLISH" if lsr > 1.3 else "BEARISH" if lsr < 0.77 else "NEUTRAL"
    return {"long_short_ratio": round(lsr, 4),
            "long_position_pct": round(lp, 1), "short_position_pct": round(100 - lp, 1),
            "signal": sig,
            "interpretation": f"Top accounts {lp:.0f}% long (ratio {lsr:.3f})."}


async def _fetch_funding_trend() -> Dict:
    try:
        data = await _http_get("https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=6")
        if not data or len(data) < 2: raise ValueError("insufficient")
        rates = [float(d.get("fundingRate", 0)) for d in data]
    except Exception:
        return {"signal": "UNAVAILABLE", "interpretation": "Funding history unavailable."}
    latest, avg, trend = rates[-1], sum(rates) / len(rates), rates[-1] - rates[0]
    sig = "BEARISH" if latest > 0.0005 and trend > 0 else \
          "BULLISH" if latest < 0 or (trend < -0.0002 and latest < 0.0003) else "NEUTRAL"
    return {"funding_latest_pct": round(latest * 100, 5),
            "funding_avg_6p_pct": round(avg * 100, 5),
            "funding_trend":      round(trend * 100, 5),
            "signal": sig,
            "interpretation": f"Funding {latest*100:+.4f}%, trend {trend*100:+.4f}%."}


async def _fetch_btc_onchain() -> Dict:
    """SOPR + MVRV-Z. Cached 1h — bitcoin-data.com rate-limits hard."""
    if not hasattr(_fetch_btc_onchain, "_cache"):
        _fetch_btc_onchain._cache = (None, 0.0)
    cached, ts = _fetch_btc_onchain._cache
    if cached and (time.time() - ts) < 3600:
        return cached
    try:
        sopr_r, mvrv_r = await asyncio.gather(
            _http_get("https://api.bitcoin-data.com/v1/sopr"),
            _http_get("https://api.bitcoin-data.com/v1/mvrv-zscore"),
        )
    except Exception:
        return {"signal": "UNAVAILABLE", "interpretation": "On-chain unavailable."}
    sopr_row = sopr_r[-1] if sopr_r else {}; mvrv_row = mvrv_r[-1] if mvrv_r else {}
    sopr = float(sopr_row.get("sopr", 1.0))
    mvrv = float(mvrv_row.get("mvrvZscore", 1.0))
    sopr_sig = "BULLISH" if sopr > 1.02 else "BEARISH_CONTRARIAN" if sopr < 0.98 else "NEUTRAL"
    mvrv_sig = "BEARISH_CONTRARIAN" if mvrv > 3.5 else "BULLISH" if mvrv < 0.5 else "NEUTRAL"
    result = {"sopr": round(sopr, 5), "sopr_date": sopr_row.get("d", "N/A"),
              "sopr_signal": sopr_sig,
              "sopr_interpretation": f"SOPR {sopr:.4f} — {'profit-taking' if sopr > 1.02 else 'capitulation' if sopr < 0.98 else 'breakeven'}",
              "mvrv_zscore": round(mvrv, 4), "mvrv_date": mvrv_row.get("d", "N/A"),
              "mvrv_signal": mvrv_sig,
              "mvrv_interpretation": f"MVRV {mvrv:.3f} — {'overvalued' if mvrv > 3.5 else 'undervalued' if mvrv < 0.5 else 'fair'}",
              "signal": sopr_sig, "interpretation": f"SOPR {sopr:.4f} / MVRV {mvrv:.3f}"}
    _fetch_btc_onchain._cache = (result, time.time())
    return result


async def _fetch_coinalyze(api_key: str) -> Dict:
    if not api_key:
        return {"signal": "UNAVAILABLE", "interpretation": "No Coinalyze key."}
    data = await _http_get(f"https://api.coinalyze.net/v1/funding-rate?symbols=BTCUSDT_PERP.A&api_key={api_key}")
    items = data if isinstance(data, list) else data.get("data", [])
    if not items: return {"signal": "UNAVAILABLE", "interpretation": "Empty Coinalyze response."}
    fr = float(items[0].get("fr") or items[0].get("value") or items[0].get("funding_rate") or 0)
    sig = "BEARISH" if fr > 0.0005 else "BULLISH" if fr < 0 else "NEUTRAL"
    return {"funding_rate_8h_pct": round(fr * 100, 5),
            "signal": sig,
            "interpretation": f"Aggregate cross-exchange funding {fr*100:+.4f}%."}


async def _fetch_coinglass_liquidations(api_key: str) -> Dict:
    if not api_key:
        return {"signal": "UNAVAILABLE", "interpretation": "No CoinGlass key."}
    data = await _http_get("https://open-api.coinglass.com/api/futures/liquidation/aggregated-history?symbol=BTC&interval=5m&limit=3",
                            headers={"CG-API-KEY": api_key})
    rows = data.get("data") or []
    if isinstance(rows, dict): rows = rows.get("list") or []
    if not rows: return {"signal": "UNAVAILABLE", "interpretation": "Empty CoinGlass response."}
    latest = rows[-1]
    long_usd  = float(latest.get("longLiqUsd",  latest.get("long",  0)) or 0)
    short_usd = float(latest.get("shortLiqUsd", latest.get("short", 0)) or 0)
    sig = "BEARISH" if long_usd > short_usd * 1.5 else "BULLISH" if short_usd > long_usd * 1.5 else "NEUTRAL"
    return {"long_liq_usd": round(long_usd, 0), "short_liq_usd": round(short_usd, 0),
            "signal": sig,
            "interpretation": f"Cross-exchange ${long_usd:,.0f}L / ${short_usd:,.0f}S (5m)."}


def extract_signal_directions(ds: Dict[str, Any]) -> Dict[str, str]:
    """Map every dashboard signal to UP / DOWN / NEUTRAL for the ensemble vote."""
    def _map(raw: str) -> str:
        s = (raw or "").upper()
        if s in ("BULLISH", "BULLISH_CONTRARIAN"): return "UP"
        if s in ("BEARISH", "BEARISH_CONTRARIAN"): return "DOWN"
        return "NEUTRAL"
    result: Dict[str, str] = {}
    for key in ("order_book", "long_short", "taker_flow", "liquidations", "fear_greed",
                "coinalyze", "deribit_dvol", "oi_velocity", "bybit_liquidations",
                "top_position_ratio", "funding_trend", "deribit_options",
                "deribit_skew_term", "spot_perp_basis", "cvd",
                "coinglass_liquidations"):
        v = ds.get(key)
        if not v: continue
        if key == "long_short":
            result[key] = _map(v.get("retail_signal_contrarian", ""))
        else:
            result[key] = _map(v.get("signal", ""))
    oc = ds.get("btc_onchain")
    if oc: result["btc_onchain"] = _map(oc.get("sopr_signal", ""))
    return result


async def fetch_dashboard_signals(coinalyze_key: str = "", coinglass_key: str = "") -> Dict[str, Any]:
    """Fan out to every dashboard fetcher in parallel; missing fetchers come back as None."""
    tasks: Dict[str, Any] = {
        "order_book":          _fetch_order_book(),
        "long_short":          _fetch_long_short(),
        "taker_flow":          _fetch_taker_flow(),
        "liquidations":        _fetch_liquidations(),
        "fear_greed":          _fetch_fear_greed(),
        "oi_velocity":         _fetch_oi_velocity(),
        "bybit_liquidations":  _fetch_okx_isolated_liquidations(),
        "top_position_ratio":  _fetch_top_position_ratio(),
        "funding_trend":       _fetch_funding_trend(),
        "deribit_dvol":        _fetch_deribit_dvol(),
        "deribit_options":     _fetch_deribit_options(),
        "deribit_skew_term":   _fetch_deribit_skew_term(),
        "spot_perp_basis":     _fetch_spot_perp_basis(),
        "cvd":                 _fetch_cvd(),
        "btc_onchain":         _fetch_btc_onchain(),
    }
    if coinalyze_key:
        tasks["coinalyze"] = _fetch_coinalyze(coinalyze_key)
    if coinglass_key:
        tasks["coinglass_liquidations"] = _fetch_coinglass_liquidations(coinglass_key)

    keys = list(tasks.keys()); coros = list(tasks.values())
    raw = await asyncio.gather(*coros, return_exceptions=True)
    result: Dict[str, Any] = {}
    for key, val in zip(keys, raw):
        if isinstance(val, Exception):
            logger.warning("dashboard '%s' failed: %s", key, val)
            result[key] = None
        else:
            result[key] = _attach_source(val, key)
    result["fetched_at"] = time.time()
    n_ok = sum(1 for v in result.values() if v is not None and not isinstance(v, float))
    logger.info("Dashboard signals: %d/%d ok", n_ok, len(keys))
    return result


# ── 5. STORAGE — Postgres pool + schema + StoragePG class. ──

_pool: Optional[pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = pool.ThreadedConnectionPool(2, 20, dsn=os.environ["DATABASE_URL"])
                logger.info("Postgres pool created")
    return _pool


@contextmanager
def _db(dict_cursor: bool = False):
    """Borrow a connection, register pgvector, yield it, return it on exit."""
    conn = _get_pool().getconn()
    if _PGVECTOR_AVAILABLE:
        try: _register_vector(conn)
        except Exception: pass
    try:
        if dict_cursor:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield conn, cur
        else:
            with conn.cursor() as cur:
                yield conn, cur
    finally:
        _get_pool().putconn(conn)


_DDL = """
CREATE TABLE IF NOT EXISTS ticks (
    id BIGSERIAL PRIMARY KEY, timestamp DOUBLE PRECISION NOT NULL,
    mid_price DOUBLE PRECISION NOT NULL,
    bid_price DOUBLE PRECISION, ask_price DOUBLE PRECISION, spread DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS predictions (
    window_start DOUBLE PRECISION PRIMARY KEY, window_end DOUBLE PRECISION,
    start_price DOUBLE PRECISION, signal TEXT, confidence DOUBLE PRECISION,
    strategy_votes TEXT, market_odds DOUBLE PRECISION, ev DOUBLE PRECISION,
    end_price DOUBLE PRECISION, actual_direction TEXT, correct INTEGER,
    created_at DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS deepseek_predictions (
    window_start DOUBLE PRECISION PRIMARY KEY, window_end DOUBLE PRECISION,
    start_price DOUBLE PRECISION, end_price DOUBLE PRECISION,
    signal TEXT, confidence DOUBLE PRECISION,
    reasoning TEXT, narrative TEXT, free_observation TEXT,
    data_received TEXT, data_requests TEXT,
    latency_ms INTEGER, window_count INTEGER,
    actual_direction TEXT, correct BOOLEAN, created_at DOUBLE PRECISION,
    raw_response TEXT, full_prompt TEXT,
    strategy_snapshot TEXT, indicators_snapshot TEXT, dashboard_signals_snapshot TEXT,
    postmortem TEXT, model_id TEXT, prompt_version TEXT
);
-- pattern_history is the vector store. JSON blob in `data`, embedding in REAL[].
CREATE TABLE IF NOT EXISTS pattern_history (
    window_start DOUBLE PRECISION PRIMARY KEY, data TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    embedding REAL[], embed_text TEXT, embed_model TEXT
);
CREATE INDEX IF NOT EXISTS idx_pattern_history_ws ON pattern_history (window_start);
"""

_MAX_TICKS = 5000
_schema_ready = False


def _ensure_schema():
    global _schema_ready
    if _schema_ready: return
    with _db() as (conn, cur):
        cur.execute(_DDL); conn.commit()
    _schema_ready = True
    logger.info("Postgres schema ready")


class StoragePG:
    def __init__(self): _ensure_schema()

    def store_tick(self, timestamp, mid, bid, ask, spread):
        with _db() as (conn, cur):
            cur.execute("INSERT INTO ticks (timestamp, mid_price, bid_price, ask_price, spread) "
                        "VALUES (%s,%s,%s,%s,%s)", (timestamp, mid, bid, ask, spread))
            cur.execute("DELETE FROM ticks WHERE id NOT IN ("
                        "SELECT id FROM ticks ORDER BY timestamp DESC LIMIT %s)", (_MAX_TICKS,))
            conn.commit()

    def store_prediction(self, *, window_start, window_end, start_price, signal,
                         confidence, strategy_votes, market_odds=None, ev=None):
        with _db() as (conn, cur):
            cur.execute(
                "INSERT INTO predictions (window_start, window_end, start_price, signal, "
                "confidence, strategy_votes, market_odds, ev, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (window_start) DO NOTHING",
                (float(window_start), float(window_end), float(start_price),
                 signal, float(confidence), json.dumps(strategy_votes),
                 float(market_odds) if market_odds is not None else None,
                 float(ev) if ev is not None else None, time.time()))
            conn.commit()

    def resolve_prediction(self, window_start, end_price):
        with _db() as (conn, cur):
            cur.execute("SELECT signal, start_price FROM predictions WHERE window_start = %s",
                        (window_start,))
            row = cur.fetchone()
            if not row: return
            signal, start_price = row
            actual = "UP" if end_price >= start_price else "DOWN"
            correct = None if signal == "NEUTRAL" else (1 if actual == signal else 0)
            cur.execute("UPDATE predictions SET end_price=%s, actual_direction=%s, correct=%s "
                        "WHERE window_start=%s", (end_price, actual, correct, window_start))
            conn.commit()

    def get_rolling_accuracy(self, n: int = 12) -> Tuple[int, int, float]:
        with _db() as (_, cur):
            cur.execute("SELECT correct FROM predictions WHERE correct IS NOT NULL "
                        "AND signal != 'NEUTRAL' ORDER BY window_start DESC LIMIT %s", (n,))
            rows = cur.fetchall()
        if not rows: return 0, 0, 0.0
        return len(rows), sum(r[0] for r in rows), sum(r[0] for r in rows) / len(rows)

    def get_total_accuracy(self) -> Tuple[int, int, float, int]:
        with _db() as (_, cur):
            cur.execute("SELECT COUNT(*), SUM(correct) FROM predictions "
                        "WHERE correct IS NOT NULL AND signal != 'NEUTRAL'")
            total, correct = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM predictions WHERE signal = 'NEUTRAL'")
            neutral = cur.fetchone()[0] or 0
        total, correct = (total or 0), int(correct or 0)
        return total, correct, correct / total if total else 0.0, neutral

    def _strategy_acc_query(self, n: int):
        with _db() as (_, cur):
            cur.execute("SELECT strategy_votes, actual_direction FROM predictions "
                        "WHERE actual_direction IS NOT NULL ORDER BY window_start DESC LIMIT %s", (n,))
            return cur.fetchall()

    def get_strategy_rolling_accuracy(self, n: int = 20) -> Dict[str, float]:
        accuracy: Dict[str, Dict] = {}
        for votes_raw, actual in self._strategy_acc_query(n):
            try: votes = json.loads(votes_raw) if isinstance(votes_raw, str) else (votes_raw or {})
            except Exception: votes = {}
            for name, vote in votes.items():
                sig = vote.get("signal") if isinstance(vote, dict) else vote
                if sig not in ("UP", "DOWN"): continue
                accuracy.setdefault(name, {"correct": 0, "total": 0})
                accuracy[name]["total"] += 1
                if sig == actual: accuracy[name]["correct"] += 1
        return {k: s["correct"] / s["total"] if s["total"] > 0 else 0.5
                for k, s in accuracy.items()}

    def get_strategy_accuracy_full(self, n: int = 100) -> Dict[str, Dict]:
        stats: Dict[str, Dict] = {}
        for votes_raw, actual in self._strategy_acc_query(n):
            try: votes = json.loads(votes_raw) if isinstance(votes_raw, str) else (votes_raw or {})
            except Exception: votes = {}
            for name, vote in votes.items():
                sig = vote.get("signal", "") if isinstance(vote, dict) else ""
                if sig not in ("UP", "DOWN"): continue
                stats.setdefault(name, {"correct": 0, "total": 0, "directional": 0})
                stats[name]["total"] += 1; stats[name]["directional"] += 1
                if sig == actual: stats[name]["correct"] += 1
        return {k: {"accuracy": s["correct"] / s["directional"] if s["directional"] else 0.5,
                    "correct": s["correct"], "total": s["total"], "directional": s["directional"]}
                for k, s in stats.items()}

    def get_agree_accuracy(self) -> Dict:
        with _db() as (_, cur):
            cur.execute(
                "SELECT p.actual_direction, p.signal, d.signal "
                "FROM predictions p JOIN deepseek_predictions d USING (window_start) "
                "WHERE p.actual_direction IS NOT NULL "
                "AND p.signal NOT IN ('ERROR','NEUTRAL') AND d.signal NOT IN ('ERROR','NEUTRAL')")
            rows = cur.fetchall()
        total = correct = 0
        for actual, e, d in rows:
            if e == d:
                total += 1
                if e == actual: correct += 1
        return {"total_agree": total, "correct_agree": correct,
                "accuracy_agree": correct / total if total else 0.0}

    def store_deepseek_prediction(self, **rec):
        with _db() as (conn, cur):
            cur.execute(
                "INSERT INTO deepseek_predictions (window_start, window_end, start_price, signal, "
                "confidence, reasoning, narrative, free_observation, data_received, data_requests, "
                "latency_ms, window_count, created_at, raw_response, full_prompt, strategy_snapshot, "
                "indicators_snapshot, dashboard_signals_snapshot, model_id, prompt_version) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (window_start) DO NOTHING",
                (float(rec.get("window_start") or 0), float(rec.get("window_end") or 0),
                 float(rec.get("start_price") or 0), rec.get("signal"),
                 float(rec.get("confidence") or 0), rec.get("reasoning"),
                 rec.get("narrative"), rec.get("free_observation"),
                 rec.get("data_received"), rec.get("data_requests"),
                 rec.get("latency_ms"), rec.get("window_count"), time.time(),
                 rec.get("raw_response", ""), rec.get("full_prompt", ""),
                 rec.get("strategy_snapshot", ""), rec.get("indicators_snapshot", ""),
                 rec.get("dashboard_signals_snapshot", ""),
                 rec.get("model_id", ""), rec.get("prompt_version", "")))
            conn.commit()

    def store_postmortem(self, window_start, postmortem):
        with _db() as (conn, cur):
            cur.execute("UPDATE deepseek_predictions SET postmortem=%s WHERE window_start=%s",
                        (postmortem, float(window_start)))
            conn.commit()

    def resolve_deepseek_prediction(self, window_start, end_price, actual: Optional[str] = None):
        with _db() as (conn, cur):
            cur.execute("SELECT signal, start_price FROM deepseek_predictions WHERE window_start=%s",
                        (window_start,))
            row = cur.fetchone()
            if not row: return
            signal, start_price = row
            if not start_price or not end_price or start_price <= 0 or end_price <= 0: return
            if actual is None:
                if abs(end_price - start_price) < 1e-6:
                    cur.execute("UPDATE deepseek_predictions SET end_price=%s, actual_direction=NULL, "
                                "correct=NULL WHERE window_start=%s", (end_price, window_start))
                    conn.commit(); return
                actual = "UP" if end_price > start_price else "DOWN"
            correct = None if signal == "NEUTRAL" else (actual == signal)
            cur.execute("UPDATE deepseek_predictions SET end_price=%s, actual_direction=%s, correct=%s "
                        "WHERE window_start=%s", (end_price, actual, correct, window_start))
            conn.commit()

    def update_deepseek_start_price(self, window_start, start_price):
        with _db() as (conn, cur):
            cur.execute("UPDATE deepseek_predictions SET start_price=%s WHERE window_start=%s",
                        (float(start_price), float(window_start)))
            conn.commit()

    def get_deepseek_accuracy(self) -> Dict:
        with _db() as (_, cur):
            cur.execute("SELECT correct FROM deepseek_predictions "
                        "WHERE correct IS NOT NULL AND signal != 'NEUTRAL'")
            rows = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM deepseek_predictions WHERE signal = 'NEUTRAL'")
            neutrals = cur.fetchone()[0] or 0
        total = len(rows); correct = sum(1 for (c,) in rows if c)
        return {"total": total, "correct": correct,
                "accuracy": correct / total if total else 0.0,
                "neutrals": neutrals, "directional": total}

    def get_recent_deepseek_predictions(self, n: int = 50) -> List[Dict]:
        with _db(dict_cursor=True) as (_, cur):
            cur.execute("SELECT * FROM deepseek_predictions ORDER BY window_start DESC LIMIT %s", (n,))
            return [dict(r) for r in cur.fetchall()]

    def get_neutral_analysis(self) -> Dict:
        with _db(dict_cursor=True) as (_, cur):
            cur.execute("SELECT actual_direction FROM deepseek_predictions "
                        "WHERE signal = 'NEUTRAL' AND actual_direction IS NOT NULL")
            rows = [dict(r) for r in cur.fetchall()]
        total = len(rows)
        if not total:
            return {"total": 0, "market_went_up": 0, "market_went_down": 0,
                    "pct_up": 0.0, "pct_down": 0.0,
                    "would_have_won_if_traded_up": 0, "would_have_won_if_traded_down": 0}
        up = sum(1 for r in rows if r["actual_direction"] == "UP")
        down = total - up
        return {"total": total, "market_went_up": up, "market_went_down": down,
                "pct_up": round(up / total * 100, 1), "pct_down": round(down / total * 100, 1),
                "would_have_won_if_traded_up": up, "would_have_won_if_traded_down": down}

    def get_recent_predictions(self, n: int = 50) -> List[Dict]:
        with _db(dict_cursor=True) as (_, cur):
            cur.execute("SELECT window_start, window_end, start_price, end_price, signal, "
                        "confidence, actual_direction, correct, market_odds, ev, strategy_votes "
                        "FROM predictions ORDER BY window_start DESC LIMIT %s", (n,))
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                if isinstance(d.get("strategy_votes"), str):
                    try: d["strategy_votes"] = json.loads(d["strategy_votes"])
                    except Exception: d["strategy_votes"] = {}
                rows.append(d)
            return rows

    def get_recent_responses_for_tape(self, n: int = 20) -> List[Dict]:
        with _db(dict_cursor=True) as (_, cur):
            cur.execute("SELECT window_start, signal, actual_direction, start_price, end_price, raw_response "
                        "FROM deepseek_predictions WHERE actual_direction IS NOT NULL "
                        "AND raw_response IS NOT NULL AND raw_response != '' "
                        "ORDER BY window_start DESC LIMIT %s", (n,))
            return [dict(r) for r in cur.fetchall()]


storage = StoragePG()


# ── Semantic store (pgvector layer over pattern_history) -----------------------

_DAYS     = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_SESSIONS = [(0, 8, "ASIA"), (8, 13, "LONDON"), (13, 16, "OVERLAP"), (16, 21, "NY"), (21, 24, "LATE")]


def _session_label(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    for start, end, label in _SESSIONS:
        if start <= dt.hour < end:
            return label
    return "LATE"


def append_resolved_window(*, window_start, window_end, actual_direction, start_price, end_price,
                            ensemble_signal, ensemble_conf, ensemble_correct,
                            deepseek_signal, deepseek_conf, deepseek_correct,
                            deepseek_reasoning, deepseek_narrative, deepseek_free_obs,
                            specialist_signals=None, historical_analysis="",
                            binance_expert_analysis=None, strategy_votes=None,
                            indicators=None, dashboard_signals_raw=None,
                            accuracy_snapshot=None, full_prompt="", trade_action="",
                            window_count=0):
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    record = {
        "window_start": window_start, "window_end": window_end, "window_count": window_count,
        "actual_direction": actual_direction,
        "start_price": start_price, "end_price": end_price,
        "session": _session_label(window_start),
        "day_of_week": _DAYS[dt.weekday()], "hour_utc": dt.hour,
        "ensemble_signal": ensemble_signal, "ensemble_conf": round(ensemble_conf, 4),
        "ensemble_correct": ensemble_correct,
        "deepseek_signal": deepseek_signal, "deepseek_conf": deepseek_conf,
        "deepseek_correct": deepseek_correct,
        "deepseek_reasoning": deepseek_reasoning, "deepseek_narrative": deepseek_narrative,
        "deepseek_free_obs": deepseek_free_obs,
        "specialist_signals": specialist_signals or {},
        "historical_analysis": historical_analysis,
        "binance_expert_analysis": binance_expert_analysis or {},
        "strategy_votes": strategy_votes or {}, "indicators": indicators or {},
        "dashboard_signals_raw": dashboard_signals_raw or {},
        "accuracy_snapshot": accuracy_snapshot or {},
        "full_prompt": full_prompt, "trade_action": trade_action,
    }
    with _db() as (conn, cur):
        cur.execute("INSERT INTO pattern_history (window_start, data, created_at) "
                    "VALUES (%s, %s, %s) ON CONFLICT (window_start) DO UPDATE SET data=EXCLUDED.data",
                    (window_start, json.dumps(record, default=str), time.time()))
        conn.commit()


def load_pattern_history(limit: int = 10000) -> List[Dict]:
    with _db() as (_, cur):
        cur.execute("SELECT data FROM pattern_history ORDER BY window_start DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
    records: List[Dict] = []
    for (data_str,) in rows:
        try: records.append(json.loads(data_str))
        except Exception: pass
    records.sort(key=lambda r: r.get("window_start", 0))
    return records


def embedded_window_starts() -> set:
    """Bars that already have a vector — used to dedup before calling Cohere."""
    with _db() as (_, cur):
        cur.execute("SELECT window_start FROM pattern_history WHERE embedding IS NOT NULL")
        return {float(r[0]) for r in cur.fetchall()}


def store_bar_embedding(window_start: float, vector: np.ndarray,
                         embed_text: str = "", embed_model: str = ""):
    """Idempotent at the SQL layer — UPDATE...WHERE embedding IS NULL refuses to
    overwrite an existing vector. Last line of defense against budget-burning
    re-embed bugs."""
    if vector is None: return
    with _db() as (conn, cur):
        cur.execute("UPDATE pattern_history SET embedding=%s, embed_text=%s, embed_model=%s "
                    "WHERE window_start=%s AND embedding IS NULL",
                    (vector.astype(np.float32).tolist(), embed_text or "", embed_model or "",
                     float(window_start)))
        conn.commit()


def fetch_postmortems(window_starts: List[float]) -> Dict[float, str]:
    if not window_starts: return {}
    try:
        with _db() as (_, cur):
            cur.execute("SELECT window_start, postmortem FROM deepseek_predictions "
                        "WHERE window_start = ANY(%s) AND postmortem IS NOT NULL AND LENGTH(postmortem) > 50",
                        (list(window_starts),))
            return {float(ws): pm for ws, pm in cur.fetchall()}
    except Exception as exc:
        logger.warning("fetch_postmortems failed: %s", exc)
        return {}


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    na, nb = np.linalg.norm(a), np.linalg.norm(b)
    return float(np.dot(a, b) / (na * nb)) if na > 0 and nb > 0 else 0.0


def search_similar(query_vec: np.ndarray, k: int = 50) -> List[Dict]:
    """Top-k cosine similarity against stored embeddings. Falls back to recent
    bars when nothing is embedded yet (cold-start)."""
    with _db() as (_, cur):
        cur.execute("SELECT data, embedding FROM pattern_history "
                    "WHERE embedding IS NOT NULL ORDER BY window_start DESC LIMIT 2000")
        rows = cur.fetchall()
    if not rows:
        return load_pattern_history()[-k:]
    q = query_vec.astype(np.float32)
    scored = []
    for data_str, emb_list in rows:
        try:
            bar = json.loads(data_str)
            bar["_similarity"] = round(_cosine(q, np.array(emb_list, dtype=np.float32)), 4)
            scored.append((bar["_similarity"], bar))
        except Exception: pass
    scored.sort(key=lambda x: x[0], reverse=True)
    return [bar for _, bar in scored[:k]]


def compute_dashboard_accuracy_from_records(records: List[Dict]) -> Dict:
    """Dashboard signal accuracy from already-loaded rows — saves a DB scan."""
    resolved = [r for r in records if r.get("actual_direction")]
    if not resolved: return {}
    counts: Dict[str, Dict] = {}
    for r in resolved:
        actual = r["actual_direction"]
        for key, val in (r.get("dashboard_signals_raw") or {}).items():
            if val not in ("UP", "DOWN"): continue
            counts.setdefault(key, {"correct": 0, "total": 0})
            counts[key]["total"] += 1
            if val == actual: counts[key]["correct"] += 1
    return {k: {"accuracy": v["correct"] / v["total"], "correct": v["correct"], "total": v["total"]}
            for k, v in counts.items() if v["total"] > 0}


def compute_all_indicator_accuracy(n: Optional[int] = None) -> Dict:
    """Per-indicator accuracy across strategies, specialists, dashboards, and AI.
    Used by the live dashboard's accuracy-of-everything panel."""
    limit = n if n and n > 0 else 10000
    records = load_pattern_history(limit)
    resolved = [r for r in records if r.get("actual_direction") in ("UP", "DOWN")]
    if not resolved: return {"best_indicator": None}
    counts: Dict[str, Dict] = {}

    def _tally(name, pred, actual):
        counts.setdefault(name, {"wins": 0, "losses": 0, "total": 0})
        counts[name]["total"] += 1
        if pred not in ("UP", "DOWN"): return
        if pred == actual: counts[name]["wins"]   += 1
        else:              counts[name]["losses"] += 1

    def _sig_of(val) -> str:
        if isinstance(val, dict): val = val.get("signal")
        return val.upper() if isinstance(val, str) else ""

    for rec in resolved:
        actual = rec["actual_direction"]
        for sn, vote in (rec.get("strategy_votes") or {}).items():
            if str(sn).startswith(("dash:", "spec:")): continue
            _tally(f"strat:{sn}", _sig_of(vote), actual)
        for spn, sp in (rec.get("specialist_signals") or {}).items():
            _tally(f"spec:{spn}", _sig_of(sp), actual)
        for dn, ds in (rec.get("dashboard_signals_raw") or {}).items():
            _tally(f"dash:{dn}", _sig_of(ds), actual)
        _tally("deepseek", _sig_of(rec.get("deepseek_signal")), actual)
        _tally("ensemble", _sig_of(rec.get("ensemble_signal")), actual)

    result: Dict = {}
    for name, c in counts.items():
        directional = c["wins"] + c["losses"]
        result[name] = {"wins": c["wins"], "losses": c["losses"], "total": c["total"],
                         "directional": directional,
                         "accuracy": round(c["wins"] / directional, 4) if directional > 0 else 0.5}
    qualified = {k: v for k, v in result.items() if v["directional"] > 0}
    result["best_indicator"] = max(qualified, key=lambda k: qualified[k]["accuracy"]) if qualified else None
    return result


# ── 6. AI PIPELINE — Gemini (if GEMINI_API_KEY) → DeepSeek. Cohere for embed + rerank. ──

DEEPSEEK_API_URL    = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_FAST_MODEL = "deepseek-chat"

GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_MODEL        = "gemma-3-27b-it"

COHERE_EMBED_URL    = "https://api.cohere.com/v2/embed"
COHERE_RERANK_URL   = "https://api.cohere.com/v2/rerank"
COHERE_EMBED_MODEL  = "embed-english-v3.0"
COHERE_RERANK_MODEL = "rerank-english-v3.0"
COHERE_EMBED_MODEL_ID = f"cohere/{COHERE_EMBED_MODEL}"
COHERE_PRE_FILTER_K = 50    # cosine candidates before reranking
COHERE_FINAL_K      = 10    # final bars sent to LLM after reranking

MAIN_PREDICTOR_PROMPT_VERSION = "v1"

SPECIALIST_KEYS = {"alligator", "acc_dist", "dow_theory", "fib_pullback", "harmonic"}

# Statuses that mean "primary LLM unavailable, try the next one." Anything else
# (400, 422 — bad prompt) is permanent and re-raises immediately.
_LLM_FALLBACK_STATUS = {401, 402, 429, 500, 502, 503, 504}


class CohereUnavailableError(RuntimeError):
    """Cohere is the only embedder; when it's down the historical analyst pauses."""


def _fmt_exc(exc: BaseException) -> str:
    msg = str(exc).strip()
    return f"{type(exc).__name__}: {msg}" if msg else type(exc).__name__


# ── Non-fatal flag plumbing ────────────────────────────────────────────────────
# When an LLM emits FREE_OBSERVATION / DATA_REQUESTS / SUGGESTION lines we push
# them into an in-memory error log so the dashboard's ERRORS tab can surface
# them. Survives the bar; reset on restart.

_FLAG_PREFIXES: List[Tuple[str, Tuple[str, ...]]] = [
    ("DATA_GAP",   ("DATA_REQUESTS:", "DATA_REQUEST:", "DATA_GAPS:")),
    ("FREE_OBS",   ("FREE_OBSERVATION:",)),
    ("SUGGESTION", ("SUGGESTION:", "SUGGESTION_1:", "SUGGESTION_2:", "SUGGESTION_3:")),
]
_FLAG_TERMINATORS = (
    "POSITION:", "CONFIDENCE:", "REASONS:", "REASON:", "NARRATIVE:",
    "ARGUMENT:", "COUNTER:", "SURVIVES_STEELMAN:",
    "PREMORTEM:", "TRAP_CHECK:", "SPECIALIST_AGREEMENT:",
    "DATA_RECEIVED:", "VERDICT:",
    "ERROR_CLASS:", "ROOT_CAUSE:", "MISLEADING_SIGNAL:",
    "RELIABLE_SIGNAL:", "LESSON_NAME:", "LESSON_RULE:", "LESSON_EFFECT:",
)

_error_log: List[Dict] = []
_ERROR_LOG_MAX = 200


def _emit_flags(source: str, raw_text: str, **ctx: Any) -> None:
    if not raw_text: return
    captured: List[Tuple[str, str]] = []
    current_kind: Optional[str] = None
    current_buf: List[str] = []

    def _flush():
        if current_kind and current_buf:
            msg = " ".join(p.strip() for p in current_buf if p.strip()).strip()
            if msg and msg.upper() != "NONE":
                captured.append((current_kind, msg))

    for line in raw_text.splitlines():
        s = line.strip()
        if not s: continue
        u = s.upper()
        matched = False
        for kind, prefixes in _FLAG_PREFIXES:
            for p in prefixes:
                if u.startswith(p):
                    _flush()
                    current_kind = kind
                    current_buf = [s[len(p):].strip()]
                    matched = True
                    break
            if matched: break
        if matched: continue
        if any(u.startswith(t) for t in _FLAG_TERMINATORS):
            _flush(); current_kind = None; current_buf = []
            continue
        if current_kind:
            current_buf.append(s)
    _flush()
    if not captured: return

    ws = float(ctx.get("window_start_time") or 0.0)
    bar_ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ws)) if ws else ""
    excerpt = raw_text[:1200]
    for kind, message in captured:
        _error_log.append({
            "window_start": ws, "bar_time": bar_ts,
            "bar_num":      ctx.get("window_count") or "",
            "signal":       kind, "source": source, "message": message,
            "reasoning":    message, "raw_response": excerpt,
            "logged_at":    time.time(),
        })
    if len(_error_log) > _ERROR_LOG_MAX:
        del _error_log[:len(_error_log) - _ERROR_LOG_MAX]


# ── HTTP layer for chat completions ────────────────────────────────────────────

async def _post_openai_chat(url: str, api_key: str, model: str, prompt: str,
                              max_tokens: int, timeout_s: Optional[float]) -> str:
    """Vanilla OpenAI-compatible POST — DeepSeek's /chat/completions endpoint."""
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model,
               "messages": [{"role": "user", "content": prompt}],
               "max_tokens": max_tokens, "temperature": 0.1}
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(url, headers=headers, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {body[:300]}")
            data = await resp.json(content_type=None)
            return data["choices"][0].get("message", {}).get("content") or ""


async def _post_gemini(api_key: str, model: str, prompt: str,
                        max_tokens: int, timeout_s: Optional[float]) -> str:
    """Google Gen-Lang API has its own schema (contents/parts, generationConfig)."""
    url = f"{GEMINI_API_BASE_URL}/{model}:generateContent?key={api_key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}],
               "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.1}}
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async with session.post(url, headers={"Content-Type": "application/json"}, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}: {body[:300]}")
            data = await resp.json(content_type=None)
            cands = data.get("candidates") or []
            if not cands: return ""
            cand = cands[0]
            if cand.get("finishReason") in {"SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT"}:
                return ""
            parts = (cand.get("content") or {}).get("parts") or []
            return "".join(p.get("text", "") for p in parts if isinstance(p, dict))


def _is_fallback_error(exc: BaseException) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientError)): return True
    msg = str(exc)
    if msg.startswith("HTTP "):
        try: return int(msg.split()[1].rstrip(":")) in _LLM_FALLBACK_STATUS
        except (ValueError, IndexError): return False
    return False


async def _llm_call(api_key: str, prompt: str, *, max_tokens: int = 1500,
                     timeout_s: Optional[float] = 90.0,
                     model: str = DEEPSEEK_FAST_MODEL) -> Tuple[str, str]:
    """Primary chat completion. Returns (text, model_id_used)."""
    gemini_key = config.gemini_api_key
    if gemini_key:
        try:
            text = await _post_gemini(gemini_key, GEMINI_MODEL, prompt, max_tokens, timeout_s)
            return text, f"gemini/{GEMINI_MODEL}"
        except Exception as exc:
            if not _is_fallback_error(exc): raise
            logger.warning("Gemini unavailable (%s) — falling back to DeepSeek", _fmt_exc(exc))
    if not api_key:
        raise RuntimeError("No LLM provider configured (set GEMINI_API_KEY or DEEPSEEK_API_KEY)")
    text = await _post_openai_chat(DEEPSEEK_API_URL, api_key, model, prompt, max_tokens, timeout_s)
    return text, f"deepseek/{model}"


# ── Cohere embed + rerank ──────────────────────────────────────────────────────

async def embed_text(cohere_key: str, text: str,
                      input_type: str = "search_document") -> np.ndarray:
    """1024-dim normalized embedding. Raises CohereUnavailableError on any failure."""
    if not cohere_key:
        raise CohereUnavailableError("COHERE_API_KEY not configured")
    headers = {"Authorization": f"Bearer {cohere_key}",
               "Content-Type": "application/json", "Accept": "application/json"}
    payload = {"model": COHERE_EMBED_MODEL, "texts": [text],
               "input_type": input_type, "embedding_types": ["float"]}
    timeout = aiohttp.ClientTimeout(total=30.0)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(COHERE_EMBED_URL, headers=headers, json=payload) as resp:
                body = await resp.text()
                if resp.status != 200:
                    raise CohereUnavailableError(f"Cohere embed HTTP {resp.status}: {body[:300]}")
                data = await resp.json(content_type=None)
                vec = np.array(data["embeddings"]["float"][0], dtype=np.float32)
                norm = np.linalg.norm(vec)
                if norm < 1e-8:
                    raise CohereUnavailableError("zero-norm embedding")
                return vec / norm
    except CohereUnavailableError: raise
    except Exception as exc:
        raise CohereUnavailableError(f"Cohere embed failed: {exc}") from exc


async def rerank_bars(cohere_key: str, query_text: str,
                       candidate_texts: List[str], top_n: int = COHERE_FINAL_K) -> List[int]:
    if not cohere_key:
        raise CohereUnavailableError("COHERE_API_KEY not configured")
    if not candidate_texts: return []
    headers = {"Authorization": f"Bearer {cohere_key}", "Content-Type": "application/json"}
    payload = {"model": COHERE_RERANK_MODEL, "query": query_text[:2048],
               "documents": [t[:16000] for t in candidate_texts],
               "top_n": min(top_n, len(candidate_texts))}
    timeout = aiohttp.ClientTimeout(total=30.0)
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(COHERE_RERANK_URL, headers=headers, json=payload) as resp:
                body = await resp.text()
                if resp.status != 200:
                    raise CohereUnavailableError(f"Cohere rerank HTTP {resp.status}: {body[:300]}")
                data = await resp.json(content_type=None)
                return [r["index"] for r in data["results"]]
    except CohereUnavailableError: raise
    except Exception as exc:
        raise CohereUnavailableError(f"Cohere rerank failed: {exc}") from exc


# ── Bar essay (the prose vector store input) ──────────────────────────────────

def _bar_embed_text(bar: Dict) -> str:
    """Render a resolved bar (or the current bar) as one prose essay. The
    embedding works on prose, not feature vectors — similarity reflects narrative
    overlap, not Euclidean distance over numbers. Postmortems are appended when
    available so retrieval surfaces *why* the prior setup resolved how it did."""
    parts: List[str] = []
    sp = bar.get("start_price"); ep = bar.get("end_price")
    actual = bar.get("actual_direction") or "?"
    move_pct = ((float(ep) / float(sp) - 1) * 100) if sp and ep else 0.0

    sess = bar.get("session") or _session_label(bar.get("window_start") or time.time())
    parts.append(f"BAR: {sess} session, actual direction {actual}, move {move_pct:+.3f}%, "
                 f"start ${sp or 0:,.0f} → end ${ep or 0:,.0f}.")

    ind = bar.get("indicators") or {}
    rsi   = ind.get("rsi_4")
    macd  = ind.get("macd_histogram")
    bbb   = ind.get("bollinger_pct_b")
    stoch = ind.get("stoch_k_5")
    ec    = ind.get("ema_cross_8_21")
    vol5  = ind.get("volatility_5"); vol10 = ind.get("volatility_10")
    tech: List[str] = ["INDICATORS:"]
    if rsi   is not None: tech.append(f"RSI(4)={rsi:.1f}")
    if macd  is not None: tech.append(f"MACD-hist={macd:+.3f}")
    if stoch is not None: tech.append(f"Stoch-K={stoch:.1f}")
    if bbb   is not None: tech.append(f"BB%B={bbb:.2f}")
    if ec    is not None: tech.append(f"EMA8-21={ec:+.2f}")
    if vol5  is not None: tech.append(f"vol5={vol5:.3f}%")
    if vol10 is not None: tech.append(f"vol10={vol10:.3f}%")
    parts.append(" ".join(tech))

    votes = bar.get("strategy_votes") or {}
    bull = [k for k, v in votes.items() if isinstance(v, dict) and v.get("signal") == "UP"]
    bear = [k for k, v in votes.items() if isinstance(v, dict) and v.get("signal") == "DOWN"]
    parts.append(f"STRATEGIES: {len(bull)} bullish, {len(bear)} bearish.")

    spec = bar.get("specialist_signals") or {}
    if spec:
        spec_parts = [f"{n}={(s or {}).get('signal','?')}" for n, s in spec.items()]
        parts.append("SPECIALISTS: " + ", ".join(spec_parts))

    dash = bar.get("dashboard_signals_raw") or {}
    if dash:
        dash_parts = [f"{n}={v}" for n, v in dash.items() if v in ("UP", "DOWN")]
        if dash_parts:
            parts.append("DASHBOARD: " + ", ".join(dash_parts[:10]))

    if bar.get("ensemble_signal"):
        parts.append(f"ENSEMBLE: {bar['ensemble_signal']} @ {(bar.get('ensemble_conf') or 0)*100:.0f}%, "
                     f"DEEPSEEK: {bar.get('deepseek_signal','?')} @ {bar.get('deepseek_conf',0)}%.")

    if bar.get("deepseek_reasoning"):
        parts.append(f"DS REASONING: {bar['deepseek_reasoning'][:600]}")
    if bar.get("deepseek_narrative"):
        parts.append(f"DS NARRATIVE: {bar['deepseek_narrative'][:400]}")
    if bar.get("postmortem"):
        parts.append(f"POSTMORTEM: {bar['postmortem'][:600]}")

    return "\n".join(parts)


# ── History table rendered into the historical-analyst prompt ─────────────────

def _build_history_table(bars: List[Dict], compact: bool = True) -> str:
    if not bars:
        return "  (no similar bars available)"
    lines: List[str] = []
    for i, b in enumerate(bars, start=1):
        ws = b.get("window_start") or 0
        ts = time.strftime("%a %H:%M", time.gmtime(ws)) if ws else "??"
        sess = b.get("session") or "?"
        sp = b.get("start_price") or 0; ep = b.get("end_price") or 0
        move = ((ep / sp - 1) * 100) if sp else 0
        actual = b.get("actual_direction") or "?"
        ens = b.get("ensemble_signal") or "?"; ens_c = (b.get("ensemble_conf") or 0)
        ds  = b.get("deepseek_signal")  or "?"; ds_c  = b.get("deepseek_conf") or 0
        sim = b.get("_similarity")
        sim_str = f" sim={sim:.3f}" if sim is not None else ""
        lines.append(f"#{i:03d} {ts} [{sess}] {actual} ${sp:,.0f}→${ep:,.0f} ({move:+.3f}%){sim_str}")
        lines.append(f"     ENS={ens}@{ens_c*100:.0f}%   DS={ds}@{ds_c}%")
        if compact:
            if b.get("deepseek_reasoning"):
                lines.append(f"     DS REASONING: {b['deepseek_reasoning'][:400]}")
            if b.get("deepseek_narrative"):
                lines.append(f"     DS NARRATIVE: {b['deepseek_narrative'][:300]}")
            if b.get("deepseek_free_obs"):
                lines.append(f"     DS FREE_OBS:  {b['deepseek_free_obs'][:200]}")
            if b.get("postmortem"):
                lines.append(f"     POSTMORTEM:   {b['postmortem'][:500]}")
            ind = b.get("indicators") or {}
            tokens = []
            for k in ("rsi_4", "macd_histogram", "stoch_k_5", "bollinger_pct_b", "ema_cross_8_21"):
                v = ind.get(k)
                if v is not None: tokens.append(f"{k}={v:.2f}" if isinstance(v, (int, float)) else f"{k}={v}")
            if tokens:
                lines.append("     IND: " + " ".join(tokens))
        lines.append("")
    return "\n".join(lines)


def _build_current_bar(features: Dict, strategy_votes: Dict, window_start_time: float,
                        specialist_signals: Optional[Dict], ensemble_signal: str,
                        ensemble_conf: float, dashboard_directions: Optional[Dict],
                        dashboard_signals_raw: Optional[Dict] = None,
                        binance_expert_analysis: Optional[Dict] = None) -> str:
    """Same prose format as a stored bar — the current-bar query embedding needs
    to look like the documents it's searching against, otherwise rerank misfires."""
    ws = window_start_time or time.time()
    sess = _session_label(ws)
    parts: List[str] = []
    parts.append(f"CURRENT BAR: {sess} session, just opened at "
                 f"{time.strftime('%H:%M UTC', time.gmtime(ws))}.")

    rsi   = features.get("rsi_4")
    macd  = features.get("macd_histogram")
    bbb   = features.get("bollinger_pct_b")
    stoch = features.get("stoch_k_5")
    ec    = features.get("ema_cross_8_21")
    tech: List[str] = ["INDICATORS:"]
    if rsi   is not None: tech.append(f"RSI(4)={rsi:.1f}")
    if macd  is not None: tech.append(f"MACD-hist={macd:+.3f}")
    if stoch is not None: tech.append(f"Stoch-K={stoch:.1f}")
    if bbb   is not None: tech.append(f"BB%B={bbb:.2f}")
    if ec    is not None: tech.append(f"EMA8-21={ec:+.2f}")
    parts.append(" ".join(tech))

    bull = [k for k, v in (strategy_votes or {}).items()
            if isinstance(v, dict) and v.get("signal") == "UP"]
    bear = [k for k, v in (strategy_votes or {}).items()
            if isinstance(v, dict) and v.get("signal") == "DOWN"]
    parts.append(f"STRATEGIES: {len(bull)} bullish, {len(bear)} bearish.")

    if specialist_signals:
        spec_parts = []
        for n, s in specialist_signals.items():
            if isinstance(s, dict):
                spec_parts.append(f"{n}={s.get('signal','?')}@{int((s.get('confidence') or 0)*100)}%")
        if spec_parts:
            parts.append("SPECIALISTS: " + ", ".join(spec_parts))

    if dashboard_directions:
        d = [f"{k}={v}" for k, v in dashboard_directions.items() if v in ("UP", "DOWN")]
        if d:
            parts.append("DASHBOARD: " + ", ".join(d[:10]))

    if isinstance(binance_expert_analysis, dict) and binance_expert_analysis.get("signal"):
        parts.append(f"BINANCE EXPERT: {binance_expert_analysis['signal']} "
                     f"@ {binance_expert_analysis.get('confidence', 0)}%. "
                     f"{(binance_expert_analysis.get('edge') or '')[:300]}")

    parts.append(f"ENSEMBLE: {ensemble_signal or '?'} @ {int(ensemble_conf*100)}%.")
    return "\n\n".join(parts)


# ── Dashboard block rendering for prompts ─────────────────────────────────────

def _fmt_usd(n: float) -> str:
    if n >= 1e9: return f"${n/1e9:.2f}B"
    if n >= 1e6: return f"${n/1e6:.1f}M"
    if n >= 1e3: return f"${n/1e3:.1f}K"
    return f"${n:.0f}"


def _build_dashboard_block(ds: Optional[Dict]) -> str:
    """Microstructure dashboard rendered for the main predictor's prompt.
    Each block is followed by a 'source:' line so the LLM can cite venues."""
    if not ds:
        return "  (dashboard signals unavailable this bar)"
    lines: List[str] = []

    def _src(key: str) -> str:
        v = (ds.get(key) or {}).get("source") if isinstance(ds.get(key), dict) else None
        if not v: return ""
        return f" [source: {v.get('scope','')} — {v.get('api','')}]"

    ob = ds.get("order_book") or {}
    if ob.get("signal") == "UNAVAILABLE" or ob.get("data_available") is False:
        lines += [f"  [ORDER BOOK] unavailable — {ob.get('interpretation','fetch failed')}", ""]
    elif ob:
        venues = ob.get("venues_included") or []
        lines += [
            f"  [ORDER BOOK DEPTH — {len(venues)}-venue aggregate, around mid ${ob.get('mid_usd',0):,.0f}]",
            f"  Within 0.5%: {ob.get('bid_depth_05pct_btc',0):.1f} BTC bids vs "
            f"{ob.get('ask_depth_05pct_btc',0):.1f} BTC asks "
            f"(imbalance {ob.get('imbalance_05pct_pct',0):+.2f}%)",
            f"  Signal: {ob.get('signal','NEUTRAL')} — {ob.get('interpretation','')}",
            "",
        ]

    ls = ds.get("long_short") or {}
    if ls.get("signal") == "UNAVAILABLE":
        lines += [f"  [LONG/SHORT] unavailable", ""]
    elif ls:
        lines += [
            "  [LONG/SHORT RATIO — Binance Futures 5m]",
            f"  All accounts: L/S {ls.get('retail_lsr',1):.3f}  Long {ls.get('retail_long_pct',50):.1f}%",
            f"  Top 20% by margin: Long {ls.get('top_accounts_long_pct',50):.1f}%   "
            f"Divergence: {ls.get('top_vs_all_div_pct',0):+.1f}pp",
            f"  → {ls.get('interpretation','')}", "",
        ]

    tk = ds.get("taker_flow") or {}
    if tk.get("signal") == "UNAVAILABLE":
        lines += [f"  [TAKER FLOW] unavailable", ""]
    elif tk:
        lines += [
            "  [TAKER AGGRESSOR FLOW — Binance Futures 5m]",
            f"  BSR: {tk.get('buy_sell_ratio',1):.4f}   Buys: {tk.get('taker_buy_vol_btc',0):.1f} BTC   "
            f"Sells: {tk.get('taker_sell_vol_btc',0):.1f} BTC",
            f"  Trend: {tk.get('trend_3bars','MIXED')}  Signal: {tk.get('signal','NEUTRAL')}",
            f"  → {tk.get('interpretation','')}", "",
        ]

    lq = ds.get("liquidations") or {}
    if lq:
        lines += [
            "  [LIQUIDATIONS — OKX cross, last 5m]",
            f"  Long: {lq.get('long_liq_count',0)} (${lq.get('long_liq_usd',0):,.0f})   "
            f"Short: {lq.get('short_liq_count',0)} (${lq.get('short_liq_usd',0):,.0f})",
            f"  Velocity: {lq.get('velocity_per_min',0):.1f}/min   Signal: {lq.get('signal','NEUTRAL')}",
            f"  → {lq.get('interpretation','')}", "",
        ]

    cz = ds.get("coinalyze")
    if cz:
        lines += [
            "  [COINALYZE — cross-exchange aggregate funding]",
            f"  Funding (8h): {cz.get('funding_rate_8h_pct',0):+.5f}%   Signal: {cz.get('signal','NEUTRAL')}",
            f"  → {cz.get('interpretation','')}", "",
        ]

    dv = ds.get("deribit_dvol")
    if dv:
        lines += [
            "  [DERIBIT DVOL — 30d implied vol]",
            f"  DVOL: {dv.get('dvol_pct', 0):.1f}%   Signal: {dv.get('signal', 'NEUTRAL')}",
            f"  → {dv.get('interpretation', '')}", "",
        ]

    cvd = ds.get("cvd")
    if cvd and cvd.get("aggregate_cvd_1h_btc") is not None:
        lines += [
            "  [CVD — 1h Binance spot+perp]",
            f"  Aggregate {cvd.get('aggregate_cvd_1h_btc',0):+.0f} BTC, "
            f"perp {cvd.get('perp_cvd_1h_btc',0):+.0f} / spot {cvd.get('spot_cvd_1h_btc',0):+.0f}",
            f"  Spot-perp divergence: {cvd.get('spot_perp_divergence_btc',0):+.0f} BTC   "
            f"Signal: {cvd.get('signal','NEUTRAL')}",
            f"  → {cvd.get('interpretation','')}", "",
        ]

    spb = ds.get("spot_perp_basis")
    if spb and spb.get("basis_pct") is not None:
        lines += [
            "  [SPOT-PERP BASIS — Binance]",
            f"  Basis: {spb.get('basis_pct',0):+.3f}% ({spb.get('basis_usd',0):+.2f} USD)   "
            f"Signal: {spb.get('signal','NEUTRAL')}",
            f"  → {spb.get('interpretation','')}", "",
        ]

    for key, label in [
        ("oi_velocity",       "[OI VELOCITY — Binance Futures 30m]"),
        ("top_position_ratio", "[TOP-ACCOUNTS POSITION RATIO — Binance, notional-weighted]"),
        ("funding_trend",      "[FUNDING TREND — Binance 6-period history]"),
        ("bybit_liquidations", "[OKX ISOLATED-MARGIN LIQUIDATIONS]"),
        ("deribit_options",    "[DERIBIT OPTIONS — P/C OI + max pain]"),
        ("deribit_skew_term",  "[DERIBIT SKEW + IV TERM]"),
    ]:
        v = ds.get(key)
        if v and v.get("signal") not in (None, "UNAVAILABLE"):
            lines += [f"  {label}",
                       f"  Signal: {v.get('signal','NEUTRAL')} — {v.get('interpretation','')}", ""]

    fg = ds.get("fear_greed")
    if fg:
        lines += [
            "  [FEAR & GREED — daily macro context]",
            f"  Score: {fg.get('value',50)} ({fg.get('label','Neutral')})   "
            f"Signal: {fg.get('signal','NEUTRAL')}",
            "",
        ]
    oc = ds.get("btc_onchain")
    if oc:
        lines += [
            f"  [BTC ON-CHAIN — daily, {oc.get('sopr_date','N/A')}]",
            f"  SOPR {oc.get('sopr',1):.5f}  Signal: {oc.get('sopr_signal','NEUTRAL')}",
            f"  MVRV-Z {oc.get('mvrv_zscore',0):.4f}  Signal: {oc.get('mvrv_signal','NEUTRAL')}",
            "",
        ]

    return "\n".join(lines).rstrip()


def _build_binance_expert_block(ds: Optional[Dict]) -> str:
    """Same dashboard data, condensed for the Binance microstructure expert prompt."""
    if not ds:
        return "  (no Binance data available this bar)"
    return _build_dashboard_block(ds)  # the same renderer is fine here


# ── Main predictor prompt (the "everything" prompt) ────────────────────────────

def _analyze_structure(klines: list, prices: list) -> Dict:
    """Compute key levels, micro/macro trend slopes, range position. Used by the
    main prompt to give the LLM swing-anchored context, not just raw klines."""
    use = bool(klines and len(klines) >= 20)
    if use:
        rows = klines[-100:]
        closes = np.array([float(r[4]) for r in rows])
        highs  = np.array([float(r[2]) for r in rows])
        lows   = np.array([float(r[3]) for r in rows])
    else:
        closes = np.array(prices[-100:])
        highs  = closes * 1.0001; lows = closes * 0.9999
    n = len(closes); mid = float(closes[-1])

    sh, sl = [], []
    for i in range(3, n - 3):
        w = closes[i-3:i+4]
        if closes[i] >= w.max(): sh.append((i, float(closes[i])))
        if closes[i] <= w.min(): sl.append((i, float(closes[i])))
    res = sorted([p for _, p in sh if p > mid])[:4]
    sup = sorted([p for _, p in sl if p < mid])[-4:]

    def _linreg(y):
        if len(y) < 2: return 0.0, 0.0
        x = np.arange(len(y), dtype=float)
        slope, intercept = np.polyfit(x, y, 1)
        y_hat = slope * x + intercept
        ss_res = np.sum((y - y_hat) ** 2); ss_tot = np.sum((y - y.mean()) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
        return float(slope), float(max(0.0, r2))

    macro_n = min(80, n); m_mac, r2_mac = _linreg(closes[-macro_n:])
    micro_n = min(20, n); m_mic, r2_mic = _linreg(closes[-micro_n:])
    range_high, range_low = float(highs.max()), float(lows.min())
    range_pos = (mid - range_low) / (range_high - range_low) if range_high > range_low else 0.5

    def _label(slope, r2, price):
        slope_pct = slope / price * 100 if price else 0.0
        strength = "strong" if r2 > 0.70 else "moderate" if r2 > 0.35 else "weak"
        direction = "UP" if slope_pct > 0.001 else "DOWN" if slope_pct < -0.001 else "FLAT"
        return f"{direction} {strength} (slope {slope_pct:+.4f}%/bar R²={r2:.2f})"

    return {"mid": mid, "res": res, "sup": sup,
            "macro_label": _label(m_mac, r2_mac, mid),
            "micro_label": _label(m_mic, r2_mic, mid),
            "macro_slope_pct": m_mac / mid * 100 if mid else 0,
            "micro_slope_pct": m_mic / mid * 100 if mid else 0,
            "range_high": range_high, "range_low": range_low,
            "range_pos": range_pos, "n_bars": n}


def build_main_prompt(*, prices, klines, features, strategy_preds, recent_accuracy,
                       deepseek_accuracy, window_num, window_start_price, window_start_time,
                       ensemble_result=None, dashboard_signals=None,
                       indicator_accuracy=None, ensemble_weights=None,
                       historical_analysis=None, dashboard_accuracy=None,
                       neutral_analysis=None, binance_expert_analysis=None,
                       trend_analyst_analysis=None, historical_failure_note: str = "") -> str:
    """The big one. Assembles every input the final predictor sees: price
    structure, key levels, indicator track records, ensemble vote, specialist
    signals, microstructure dashboard, historical analyst conclusion, trend
    summary, NEUTRAL track record."""
    sa = _analyze_structure(klines or [], prices)
    now = prices[-1]
    ts_start = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(window_start_time))
    ts_end   = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time + 300))

    res_lines = []
    for lvl in reversed(sa["res"]):
        res_lines.append(f"  RESISTANCE  ${lvl:>10,.2f}   (+{(lvl-now)/now*100:.2f}%)")
    res_lines.append(f"  ── current  ${now:>10,.2f} ──")
    for lvl in reversed(sa["sup"]):
        res_lines.append(f"  SUPPORT     ${lvl:>10,.2f}   (-{(now-lvl)/now*100:.2f}%)")
    levels_block = "\n".join(res_lines)

    rp = sa["range_pos"]
    rp_str = "upper third" if rp > 0.66 else "lower third" if rp < 0.33 else "mid range"

    dashboard_block = _build_dashboard_block(dashboard_signals)

    # CSV of last 50 1m bars
    def _na(v, fmt="{:.0f}"):
        if v is None or v == "": return "NA"
        if isinstance(v, str) and v.upper() in ("NA", "NONE", "NULL"): return "NA"
        try: return fmt.format(float(v))
        except (ValueError, TypeError): return "NA"

    csv_block = "(no kline data)"
    if klines and len(klines) >= 5:
        rows = ["Time(UTC),Open,High,Low,Close,Volume,QuoteVol,Trades,BuyVol%"]
        for k in klines[-50:]:
            try:
                ts_s = time.strftime("%m-%d %H:%M", time.gmtime(int(k[0]) / 1000))
                vol  = float(k[5]) if len(k) > 5 else 0.0
                quote = _na(k[7]) if len(k) > 7 else "NA"
                trades = _na(k[8], "{:.0f}") if len(k) > 8 else "NA"
                bv_raw = k[9] if len(k) > 9 else None
                if bv_raw is None or bv_raw == "" or (isinstance(bv_raw, str) and bv_raw.upper() in ("NA", "NONE", "NULL")):
                    buy_pct = "NA"
                else:
                    try: buy_pct = f"{round(float(bv_raw) / vol * 100, 1)}" if vol > 0 else "NA"
                    except (ValueError, TypeError): buy_pct = "NA"
                rows.append(f"{ts_s},{float(k[1]):.2f},{float(k[2]):.2f},"
                             f"{float(k[3]):.2f},{float(k[4]):.2f},{vol:.1f},{quote},{trades},{buy_pct}")
            except Exception: continue
        csv_block = "\n".join(rows)

    bullish = sum(1 for p in strategy_preds.values() if p.get("signal") == "UP")
    bearish = sum(1 for p in strategy_preds.values() if p.get("signal") == "DOWN")
    strat_lines = "\n".join(
        f"  {name:<18} {'↑' if p.get('signal')=='UP' else '↓'} "
        f"{p.get('confidence',0)*100:4.0f}%  {(p.get('reasoning') or '')[:60]}"
        for name, p in strategy_preds.items()
    )

    if ensemble_result:
        ensemble_block = (
            f"  Signal: {ensemble_result.get('signal','?')}   "
            f"Confidence: {ensemble_result.get('confidence',0)*100:.1f}%   "
            f"UP-prob: {ensemble_result.get('up_probability',0.5)*100:.1f}%\n"
            f"  Votes: {ensemble_result.get('bullish_count',bullish)}↑ / "
            f"{ensemble_result.get('bearish_count',bearish)}↓"
        )
    else:
        ensemble_block = f"  {bullish}↑ / {bearish}↓"

    ds_total   = (deepseek_accuracy or {}).get("total", 0)
    ds_correct = (deepseek_accuracy or {}).get("correct", 0)
    ds_acc_str = (f"{ds_correct}/{ds_total} ({(deepseek_accuracy or {}).get('accuracy', 0)*100:.1f}%)"
                  if ds_total > 0 else "no prior predictions")

    # Historical block (with anti-hallucination guard when analyst didn't fire)
    if historical_analysis and historical_analysis.strip():
        historical_block = historical_analysis.strip()
    else:
        reason = (
            f"  (historical analyst did not produce output — {historical_failure_note.strip()})"
            if historical_failure_note and historical_failure_note.strip()
            else "  (historical analyst did not fire — no resolved bars yet or cold-start)"
        )
        historical_block = (
            f"{reason}\n\n"
            "  ⚠️  WARNING: You have NO historical similarity data this window.\n"
            "  Do NOT invent or reference specific bar numbers (#001, #002, etc.)\n"
            "  or claim patterns 'resolved X% of the time' — that is hallucination.\n"
            "  Only reference patterns if they appear in the data shown above."
        )

    # Binance expert block
    bx = binance_expert_analysis or {}
    if bx and bx.get("signal"):
        be_lines = [f"  Signal: {bx.get('signal','?')} ({bx.get('confidence', 0)}%)"]
        for label, key in [("Taker flow", "taker_flow"), ("Positioning", "positioning"),
                            ("Whale flow", "whale_flow"), ("OI/funding", "oi_funding"),
                            ("Order book", "order_book"), ("Confluence", "confluence"),
                            ("Edge", "edge"), ("Watch", "watch")]:
            v = bx.get(key)
            if v: be_lines.append(f"  {label}: {v}")
        binance_expert_block = "\n".join(be_lines)
    else:
        binance_expert_block = "  (Binance expert did not complete this window)"

    # Trend analyst block
    ta = trend_analyst_analysis or {}
    if ta.get("available") and ta.get("regime"):
        trend_block = (
            f"  Snapshot:  {ta.get('trend_snapshot','')}\n"
            f"  Regime:    {ta.get('regime','')}\n"
            f"  Volatility: {ta.get('volatility','')}\n"
            f"  Volume:    {ta.get('volume_profile','')}\n"
            f"  Traps:     {ta.get('traps_building','NONE')}\n\n"
            f"  {ta.get('narrative','')}"
        )
    else:
        trend_block = "  (Trend analyst did not complete this window)"

    # NEUTRAL abstention performance
    na = neutral_analysis or {}
    if na.get("total", 0) > 0:
        dominant = "UP" if na["market_went_up"] > na["market_went_down"] else "DOWN"
        neutral_block = (
            f"  Total NEUTRAL calls: {na['total']}\n"
            f"  After abstaining the market went UP {na['market_went_up']} ({na['pct_up']:.0f}%) "
            f"/ DOWN {na['market_went_down']} ({na['pct_down']:.0f}%).\n"
            f"  Dominant post-neutral direction: {dominant}.\n"
            f"  Implication: in {max(na['pct_up'], na['pct_down']):.0f}% of past abstentions "
            f"committing to {dominant} would have won."
        )
    else:
        neutral_block = "  No NEUTRAL abstentions on record yet."

    # Indicator track record
    track_lines = []
    if indicator_accuracy:
        for name, stats in sorted(indicator_accuracy.items(),
                                    key=lambda kv: kv[1].get("accuracy", 0.5),
                                    reverse=True)[:25]:
            acc = stats.get("accuracy", 0.5); tot = stats.get("total", 0)
            cor = stats.get("correct", stats.get("wins", 0))
            w   = (ensemble_weights or {}).get(name, 1.0)
            label = accuracy_to_label(acc, tot)
            track_lines.append(f"  {name:<22} {acc*100:5.1f}%  ({cor}/{tot})  weight={w:.2f}  [{label}]")
    track_record = "\n".join(track_lines) or "  (no resolved predictions yet)"

    return f"""\
You are the final decision analyst for a BTC/USDT 5-minute directional prediction system.
All data below is REAL, computed from live exchange OHLCV + live market microstructure feeds.

You are NOT a vote-tallier. You synthesize evidence from independent specialists (Binance
microstructure expert, historical similarity analyst, unified technical analyst, trend
analyst, ensemble vote) into a single probabilistic call. Each specialist is fallible in
predictable ways; your job is to know which one's call to trust when they disagree.

PAYOFF: Correct UP/DOWN = +1. Wrong UP/DOWN = −1. NEUTRAL = 0. NEUTRAL is not a free win — it
is a deliberate choice to preserve capital when the data genuinely does not support either side.
Every NEUTRAL is a passed opportunity. If after weighing all the data you have a defensible
argument for one side, take the call.

A directional call should be a committed, defended position. Steelman the opposing direction:
what is the strongest case for the other side? If your call survives that with a concrete
rebuttal grounded in specific fields and numbers, commit. Confidence reflects the robustness
of the argument *after* weighing the opposing case — not raw enthusiasm. There is NO confidence
floor; a genuine 55% edge with a clean rebuttal is a call you take.

══════════════════════════════════════════════
  WINDOW #{window_num}
  START : {ts_start}
  END   : {ts_end}  (5-minute window closes here)
  Entry price  : ${window_start_price:,.2f}
  QUESTION     : ABOVE or BELOW ${window_start_price:,.2f} at {ts_end}?
══════════════════════════════════════════════

──────────────────────────────────────────────
  PRICE STRUCTURE  (last {sa['n_bars']} bars)
──────────────────────────────────────────────
  100-bar range : ${sa['range_low']:,.2f} – ${sa['range_high']:,.2f}
  Range pos     : {rp_str}  ({sa['range_pos']:.0%} from bottom)
  Macro trend (80 bars) : {sa['macro_label']}
  Micro trend (20 bars) : {sa['micro_label']}

──────────────────────────────────────────────
  KEY LEVELS  (swing-point clusters)
──────────────────────────────────────────────
{levels_block}

──────────────────────────────────────────────
  RAW 1-MINUTE OHLCV (last 50 bars)
──────────────────────────────────────────────
{csv_block}

──────────────────────────────────────────────
  STRATEGY VOTES  (rule-based ensemble)
──────────────────────────────────────────────
{strat_lines}

──────────────────────────────────────────────
  ENSEMBLE RESULT
──────────────────────────────────────────────
{ensemble_block}

──────────────────────────────────────────────
  INDICATOR TRACK RECORD  (rolling-100-bar accuracy)
──────────────────────────────────────────────
{track_record}

──────────────────────────────────────────────
  MICROSTRUCTURE DASHBOARD
──────────────────────────────────────────────
{dashboard_block}

──────────────────────────────────────────────
  BINANCE MICROSTRUCTURE EXPERT  (separate DeepSeek call)
──────────────────────────────────────────────
{binance_expert_block}

──────────────────────────────────────────────
  TREND ANALYST  (last-20-bar regime synthesis)
──────────────────────────────────────────────
{trend_block}

──────────────────────────────────────────────
  HISTORICAL ANALYST  (top similar bars from pgvector + Cohere)
──────────────────────────────────────────────
{historical_block}

──────────────────────────────────────────────
  YOUR OWN PRIOR PREDICTIONS
──────────────────────────────────────────────
  Rolling 12-bar accuracy: {(recent_accuracy or 0)*100:.1f}%
  All-time DeepSeek      : {ds_acc_str}

──────────────────────────────────────────────
  NEUTRAL ABSTENTION TRACK RECORD
──────────────────────────────────────────────
{neutral_block}

══════════════════════════════════════════════
  RESPONSE FORMAT  (strict — parser depends on these field names)
══════════════════════════════════════════════
POSITION: UP | DOWN | NEUTRAL
CONFIDENCE: 55-95
REASONS: 2-3 sentences. Cite specific field values. Name the most decisive factor.
NARRATIVE: One paragraph telling the story of this bar — what's happening, why your call.
FREE_OBSERVATION: One observation about something surprising in this data, or NONE.
DATA_REQUESTS: Anything you wished you had, or NONE.
PREMORTEM: One sentence — the single most likely reason this call is wrong.
SUGGESTION: One concrete prompt/data improvement, or NONE.
"""


# ── Response parser (extracts structured fields from any of the LLM answers) ──

def parse_response(text: str) -> Tuple[str, int, str, str, str, str, str]:
    """Returns (signal, confidence, reasoning, data_received, data_requests,
    narrative, free_observation). Tolerant of missing fields."""
    signal, confidence = "NEUTRAL", 50
    fields = {"reasons": "", "data_received": "", "data_requests": "",
              "narrative": "", "free_observation": ""}
    current = None
    for line in text.splitlines():
        s = line.strip(); u = s.upper()
        if u.startswith("POSITION:"):
            v = u.replace("POSITION:", "").strip()
            if   "UP" in v:   signal = "UP"
            elif "DOWN" in v: signal = "DOWN"
            elif "ABOVE" in v: signal = "UP"
            elif "BELOW" in v: signal = "DOWN"
            else: signal = "NEUTRAL"
            current = None
        elif u.startswith("CONFIDENCE:"):
            try: confidence = int(float(u.replace("CONFIDENCE:", "").replace("%", "").strip()))
            except Exception: pass
            current = None
        elif u.startswith("REASONS:") or u.startswith("REASON:"):
            fields["reasons"] = s.split(":", 1)[1].strip()
            current = "reasons"
        elif u.startswith("NARRATIVE:"):
            fields["narrative"] = s.split(":", 1)[1].strip(); current = "narrative"
        elif u.startswith("FREE_OBSERVATION:"):
            fields["free_observation"] = s.split(":", 1)[1].strip(); current = "free_observation"
        elif u.startswith("DATA_RECEIVED:"):
            fields["data_received"] = s.split(":", 1)[1].strip(); current = "data_received"
        elif u.startswith("DATA_REQUESTS:"):
            fields["data_requests"] = s.split(":", 1)[1].strip(); current = "data_requests"
        elif any(u.startswith(t) for t in ("PREMORTEM:", "SUGGESTION:", "ARGUMENT:",
                                            "COUNTER:", "SURVIVES_STEELMAN:")):
            current = None
        elif current and s:
            fields[current] += " " + s
    return (signal, max(50, min(95, confidence)),
            fields["reasons"].strip(), fields["data_received"].strip(),
            fields["data_requests"].strip(), fields["narrative"].strip(),
            fields["free_observation"].strip())


# ── Specialist runners ────────────────────────────────────────────────────────

def _parse_unified_specialists(text: str) -> Dict[str, Dict]:
    """Pull the 5 specialist signals out of the unified-analyst response.
    Each block has POSITION, SURVIVES, VALUE/STATE/PATTERN, REASON."""
    fields: Dict[str, Dict] = {}
    KEYS = {"DOW": "dow_theory", "FIB": "fib_pullback",
             "ALG": "alligator", "ACD": "acc_dist", "HAR": "harmonic"}
    for line in text.splitlines():
        s = line.strip()
        for prefix, name in KEYS.items():
            if s.upper().startswith(f"{prefix}_POSITION:"):
                v = s.split(":", 1)[1].strip().upper()
                signal = "UP" if "ABOVE" in v else "DOWN" if "BELOW" in v else "NEUTRAL"
                fields.setdefault(name, {})["signal"] = signal
            elif s.upper().startswith(f"{prefix}_SURVIVES:"):
                fields.setdefault(name, {})["survives"] = "YES" in s.upper()
            elif s.upper().startswith(f"{prefix}_REASON:"):
                fields.setdefault(name, {})["reasoning"] = s.split(":", 1)[1].strip()[:200]
    # Normalize: NEUTRAL or non-survives → low confidence
    for name in KEYS.values():
        f = fields.get(name) or {}
        if f.get("signal") not in ("UP", "DOWN") or not f.get("survives", True):
            f["signal"] = "NEUTRAL"; f["confidence"] = 0.45
        else:
            f["confidence"] = 0.65
        f.setdefault("reasoning", "")
        fields[name] = f
    return fields


async def run_specialists(api_key: str, klines: list) -> Dict[str, Dict]:
    """Send the unified analyst prompt with 60 bars of OHLCV and parse the five
    pattern signals (Dow, Fib, Alligator, Acc/Dist, Harmonic)."""
    if not klines or len(klines) < 30:
        return {}
    rows = ["Time(UTC),Open,High,Low,Close,Volume(BTC),QuoteVol(USDT),Trades,BuyVol%"]
    for k in klines[-60:]:
        try:
            ts = time.strftime("%m-%d %H:%M", time.gmtime(int(k[0]) / 1000))
            vol = float(k[5]) if len(k) > 5 else 0.0
            quote = k[7] if len(k) > 7 else "NA"
            trades = k[8] if len(k) > 8 else "NA"
            bv = k[9] if len(k) > 9 else None
            buy_pct = round(float(bv) / vol * 100, 1) if (bv is not None and vol > 0) else "NA"
            rows.append(f"{ts},{float(k[1]):.2f},{float(k[2]):.2f},{float(k[3]):.2f},"
                          f"{float(k[4]):.2f},{vol:.1f},{quote},{trades},{buy_pct}")
        except Exception: continue
    csv = "\n".join(rows)
    prompt = UNIFIED_ANALYST.replace("{csv}", csv)
    try:
        raw, _ = await _llm_call(api_key, prompt, max_tokens=2500, timeout_s=90.0)
        _emit_flags("specialists", raw)
        return _parse_unified_specialists(raw)
    except Exception as exc:
        logger.warning("Unified specialist call failed: %s", _fmt_exc(exc))
        return {}


def _parse_binance_expert_response(text: str) -> Dict:
    signal, confidence = "NEUTRAL", 50
    fields = {"taker_flow": "", "positioning": "", "whale_flow": "",
              "oi_funding": "", "order_book": "", "confluence": "",
              "edge": "", "watch": "", "argument": "", "counter": ""}
    KEY_MAP = {
        "POSITION": None, "CONFIDENCE": None,
        "TAKER_FLOW": "taker_flow", "POSITIONING": "positioning",
        "WHALE_FLOW": "whale_flow", "OI_FUNDING": "oi_funding",
        "ORDER_BOOK": "order_book", "CONFLUENCE": "confluence",
        "EDGE": "edge", "WATCH": "watch",
        "ARGUMENT": "argument", "COUNTER": "counter",
    }
    current = None
    for line in text.splitlines():
        s = line.strip()
        if not s: continue
        colon = s.find(":")
        if colon > 0:
            raw_key = re.sub(r"[^A-Z0-9 _]", "", s[:colon].upper()).strip().replace(" ", "_")
            if raw_key in KEY_MAP:
                value = s[colon+1:].strip()
                if raw_key == "POSITION":
                    v = value.upper()
                    signal = "UP" if "ABOVE" in v else "DOWN" if "BELOW" in v else "NEUTRAL"
                    current = None; continue
                if raw_key == "CONFIDENCE":
                    try: confidence = int(float(value.replace("%", "").strip()))
                    except Exception: pass
                    current = None; continue
                fields[KEY_MAP[raw_key]] = value; current = KEY_MAP[raw_key]
                continue
        if current and s:
            fields[current] += " " + s
    return {"signal": signal, "confidence": confidence, **fields}


async def run_binance_expert(api_key: str, dashboard_signals: Optional[Dict]) -> Optional[Dict]:
    """Microstructure-only LLM call. Output feeds the historical analyst's query
    AND the main predictor as a separate evidence block."""
    if not api_key or not dashboard_signals:
        return None
    block = _build_binance_expert_block(dashboard_signals)
    prompt = BINANCE_EXPERT.replace("{dashboard_block}", block)
    t0 = time.time()
    try:
        raw, _ = await _llm_call(api_key, prompt, max_tokens=4000, timeout_s=90.0)
        _emit_flags("binance_expert", raw)
        result = _parse_binance_expert_response(raw)
        logger.info("Binance expert %.1fs → %s %d%%", time.time() - t0,
                     result["signal"], result["confidence"])
        return result
    except Exception as exc:
        logger.warning("Binance expert failed: %s", _fmt_exc(exc))
        return None


def _parse_trend_analyst_response(text: str) -> Dict:
    fields = {"trend_snapshot": "", "regime": "", "volatility": "",
              "volume_profile": "", "traps_building": "", "narrative": ""}
    KEY_MAP = {"TREND_SNAPSHOT": "trend_snapshot", "REGIME": "regime",
                "VOLATILITY": "volatility", "VOLUME_PROFILE": "volume_profile",
                "TRAPS_BUILDING": "traps_building", "NARRATIVE": "narrative"}
    current = None
    for line in text.splitlines():
        s = line.strip()
        if not s: continue
        colon = s.find(":")
        if colon > 0:
            key = re.sub(r"[^A-Z0-9 _]", "", s[:colon].upper()).strip().replace(" ", "_")
            if key in KEY_MAP:
                fields[KEY_MAP[key]] = s[colon+1:].strip()
                current = KEY_MAP[key]; continue
        if current and s:
            fields[current] += " " + s
    return {**fields, "raw_response": text}


def _build_trend_tape(past_responses: List[Dict]) -> str:
    rows = list(reversed(past_responses))
    lines: List[str] = []
    n = len(rows)
    for i, row in enumerate(rows):
        bar_idx = -(n - i)
        ws = row.get("window_start")
        ts_str = (time.strftime("%H:%M UTC", time.gmtime(float(ws)))
                  if ws else "??:?? UTC")
        ds = (row.get("signal") or "?").upper()
        actual = (row.get("actual_direction") or "?").upper()
        sp, ep = row.get("start_price"), row.get("end_price")
        try: pct = f"{((float(ep) / float(sp)) - 1) * 100:+.3f}%"
        except Exception: pct = "?"
        lines.append(f"── Bar {bar_idx} ({ts_str}) | DS={ds} actual={actual} {pct} ──")
        raw = (row.get("raw_response") or "").strip()
        if len(raw) > 2500: raw = raw[:2500] + "\n… [truncated]"
        if raw: lines.append(raw)
        lines.append("")
    return "\n".join(lines)


async def run_trend_analyst(api_key: str, past_responses: List[Dict]) -> Dict:
    """Synthesizes the last 20 resolved-bar responses into regime + narrative.
    Soft-fails when there's <5 bars of history (cold-start)."""
    if len(past_responses) < 5:
        return {"available": False, "reason": f"insufficient ({len(past_responses)} bars)"}
    tape = _build_trend_tape(past_responses)
    prompt = TREND_ANALYST.replace("{tape_block}", tape)
    try:
        raw, _ = await _llm_call(api_key, prompt, max_tokens=1500, timeout_s=90.0)
        result = _parse_trend_analyst_response(raw)
        result["available"] = True
        return result
    except Exception as exc:
        logger.warning("Trend analyst failed: %s", _fmt_exc(exc))
        return {"available": False, "reason": _fmt_exc(exc)}


def _parse_historical_signal(raw: str) -> Dict:
    signal, confidence, lean = "NEUTRAL", 50, ""
    for line in raw.splitlines():
        s = line.strip(); u = s.upper()
        if u.startswith("POSITION:"):
            v = u.replace("POSITION:", "").strip()
            if "UP" in v: signal = "UP"
            elif "DOWN" in v: signal = "DOWN"
            else: signal = "NEUTRAL"
        elif u.startswith("CONFIDENCE:"):
            try: confidence = int(float(u.replace("CONFIDENCE:", "").replace("%", "").strip()))
            except Exception: pass
        elif u.startswith("LEAN:"):
            lean = s[5:].strip()
        elif lean and signal != "NEUTRAL":
            break
    return {"signal": signal,
            "confidence": max(0.45, min(0.95, confidence / 100)),
            "reasoning": lean, "value": f"{confidence}%",
            "htf_signal": "N/A", "crossover": False, "crossunder": False, "mtf_agree": None}


async def run_historical_analyst(api_key: str, history_records: List[Dict],
                                   current_indicators: Dict, current_strategy_votes: Dict,
                                   *, window_start_time: float = 0.0,
                                   specialist_signals: Optional[Dict] = None,
                                   ensemble_signal: str = "", ensemble_conf: float = 0.0,
                                   dashboard_directions: Optional[Dict] = None,
                                   dashboard_signals_raw: Optional[Dict] = None,
                                   binance_expert_analysis: Optional[Dict] = None,
                                   cohere_api_key: str = "") -> Tuple[Optional[Dict], Optional[str]]:
    """The full RAG pipeline:
        1. Embed current bar via Cohere
        2. pgvector cosine search → top 50
        3. Cohere rerank → top 10
        4. Pull postmortems for the top 10
        5. Hand to LLM as "historical analyst" context
    Raises CohereUnavailableError if Cohere is unreachable."""
    if len(history_records) < 5:
        return None, None

    current_bar = _build_current_bar(
        current_indicators, current_strategy_votes, window_start_time,
        specialist_signals, ensemble_signal, ensemble_conf,
        dashboard_directions, dashboard_signals_raw, binance_expert_analysis)

    # Embed
    current_vec = await embed_text(cohere_api_key, current_bar, input_type="search_query")

    # pgvector search
    pre_bars = search_similar(current_vec, COHERE_PRE_FILTER_K) or history_records[-COHERE_PRE_FILTER_K:]
    pre_texts = [_bar_embed_text(b) for b in pre_bars]

    # Cohere rerank
    if len(pre_bars) > COHERE_FINAL_K:
        ranked_idx = await rerank_bars(cohere_api_key, current_bar, pre_texts, top_n=COHERE_FINAL_K)
        similar_bars = [pre_bars[i] for i in ranked_idx]
    else:
        similar_bars = pre_bars

    # Attach postmortems for the top bars
    ts_list = [b.get("window_start") for b in similar_bars if b.get("window_start")]
    pms = fetch_postmortems(ts_list)
    for b in similar_bars:
        pm = pms.get(float(b.get("window_start") or 0))
        if pm: b["postmortem"] = pm

    history_table = _build_history_table(similar_bars, compact=True)
    n = len(similar_bars)
    prompt = (HISTORICAL_ANALYST
              .replace("{n}", str(n))
              .replace("{history_table}", history_table)
              .replace("{current_bar}", current_bar))

    t0 = time.time()
    try:
        raw, _ = await _llm_call(api_key, prompt, max_tokens=4000, timeout_s=120.0)
        _emit_flags("historical_analyst", raw, window_start_time=window_start_time)
        sig = _parse_historical_signal(raw)
        logger.info("Historical analyst %.1fs | %s %.0f%% (top-%d of %d bars)",
                     time.time() - t0, sig["signal"], sig["confidence"] * 100, n,
                     len(history_records))
        return sig, raw.strip()
    except Exception as exc:
        logger.error("Historical analyst LLM call failed: %s", _fmt_exc(exc))
        return None, None


# ── Postmortem (fired after the bar resolves; embedding picks it up) ──────────

async def run_postmortem(api_key: str, ds_record: Dict, actual_direction: str,
                           end_price: float, klines: list, features: Dict,
                           dashboard_signals: Optional[Dict] = None) -> Optional[str]:
    """One LLM call that walks back the bar: VERDICT, ERROR_CLASS, ROOT_CAUSE,
    LESSON_RULE etc. Result is stored alongside the bar and re-embedded so
    future similar setups surface the lesson."""
    if not api_key:
        return None
    sp = ds_record.get("start_price", 0)
    move_pct = ((end_price / sp - 1) * 100) if sp else 0.0
    pred_signal = ds_record.get("signal", "?")
    correct = (pred_signal == actual_direction) if actual_direction in ("UP", "DOWN") else None
    verdict = "CORRECT" if correct else ("WRONG" if correct is False else "FLAT")

    prompt = f"""You are running the post-mortem for a single 5-minute BTC prediction. Read the
prediction and what actually happened, then write a structured forensic analysis the system
will store and embed for future retrieval.

PREDICTION
  Signal:     {pred_signal}
  Confidence: {ds_record.get('confidence', 0)}%
  Reasoning:  {ds_record.get('reasoning', '')[:600]}
  Narrative:  {ds_record.get('narrative', '')[:400]}
  Free obs:   {ds_record.get('free_observation', '')[:200]}

WHAT HAPPENED
  Actual direction: {actual_direction}
  Move: {move_pct:+.3f}%   Verdict: {verdict}

OUTPUT (strict — these field names are parser-readable):
VERDICT:        CORRECT | WRONG | FLAT
ERROR_CLASS:    NONE | TRAP | NOISE | INDICATOR_FAIL | MICROSTRUCTURE_MISREAD | REGIME_BREAK | OTHER
ROOT_CAUSE:     One sentence.
MISLEADING_SIGNAL: Which input most led the system astray (or NONE).
RELIABLE_SIGNAL:  Which input quietly got it right (or NONE).
LESSON_NAME:    Short pattern label (or NONE).
LESSON_RULE:    One sentence imperative — what should the system do next time.
LESSON_PRECONDITIONS: When this rule applies (one phrase).
LESSON_EFFECT:  Expected effect (one phrase).
LESSON_FALSIFIER: What would invalidate the rule.
"""
    try:
        raw, _ = await _llm_call(api_key, prompt, max_tokens=1200, timeout_s=90.0)
        _emit_flags("postmortem", raw,
                     window_start_time=ds_record.get("window_start"))
        return raw.strip() if raw else None
    except Exception as exc:
        logger.warning("Postmortem failed: %s", _fmt_exc(exc))
        return None


# ── Main DeepSeekPredictor class ──────────────────────────────────────────────

class DeepSeekPredictor:
    """Generates the final UP/DOWN/NEUTRAL call at bar open."""

    def __init__(self, api_key: str, initial_bar_count: int = 0):
        self.api_key = api_key
        self.window_count = initial_bar_count

    async def predict(self, *, prices, klines, features, strategy_preds,
                       recent_accuracy, deepseek_accuracy, window_start_time,
                       window_start_price, ensemble_result=None,
                       dashboard_signals=None, indicator_accuracy=None,
                       ensemble_weights=None, historical_analysis=None,
                       dashboard_accuracy=None, neutral_analysis=None,
                       binance_expert_analysis=None, trend_analyst_analysis=None,
                       historical_failure_note: str = "") -> Dict:
        self.window_count += 1
        t0 = time.time()
        prompt = build_main_prompt(
            prices=prices, klines=klines, features=features,
            strategy_preds=strategy_preds, recent_accuracy=recent_accuracy,
            window_num=self.window_count, deepseek_accuracy=deepseek_accuracy,
            window_start_price=window_start_price, window_start_time=window_start_time,
            ensemble_result=ensemble_result, dashboard_signals=dashboard_signals,
            indicator_accuracy=indicator_accuracy, ensemble_weights=ensemble_weights,
            historical_analysis=historical_analysis,
            dashboard_accuracy=dashboard_accuracy, neutral_analysis=neutral_analysis,
            binance_expert_analysis=binance_expert_analysis,
            trend_analyst_analysis=trend_analyst_analysis,
            historical_failure_note=historical_failure_note)

        try:
            raw, model_used = await _llm_call(self.api_key, prompt,
                                                max_tokens=5000, timeout_s=90.0)
        except Exception as exc:
            logger.error("Main predictor LLM call failed: %s", _fmt_exc(exc))
            return {"signal": "ERROR", "confidence": 0, "reasoning": _fmt_exc(exc),
                    "data_received": "", "data_requests": "", "narrative": "",
                    "free_observation": "", "raw_response": "", "full_prompt": prompt,
                    "latency_ms": int((time.time() - t0) * 1000),
                    "completed_at": time.time(), "window_count": self.window_count,
                    "model_id": "", "prompt_version": MAIN_PREDICTOR_PROMPT_VERSION}

        signal, confidence, reasoning, data_received, data_requests, narrative, free_obs = \
            parse_response(raw)
        _emit_flags("main_predictor", raw,
                     window_start_time=window_start_time, window_count=self.window_count)
        latency_ms = int((time.time() - t0) * 1000)
        logger.info("DeepSeek #%d → %s conf=%d%% latency=%dms",
                     self.window_count, signal, confidence, latency_ms)
        return {"signal": signal, "confidence": confidence, "reasoning": reasoning,
                "data_received": data_received, "data_requests": data_requests,
                "narrative": narrative, "free_observation": free_obs,
                "raw_response": raw, "full_prompt": prompt,
                "latency_ms": latency_ms, "completed_at": time.time(),
                "window_count": self.window_count, "model_id": model_used,
                "prompt_version": MAIN_PREDICTOR_PROMPT_VERSION}


# ── 7. ENGINE — five-minute drumbeat: predict → resolve → postmortem → re-embed. ──

collector      = BinanceCollector(poll_interval=config.poll_interval_seconds)
ensemble       = EnsemblePredictor(config.initial_weights)
lr_strategy    = LinearRegressionChannel()
feature_engine = FeatureEngine()

# Bar counter persists across restarts — read the highest stored count from
# Postgres so window_count never resets after a deploy.
_ds_bar_init = 0
try:
    _recs = storage.get_recent_deepseek_predictions(9999)
    if _recs:
        _ds_bar_init = max((r.get("window_count") or 0 for r in _recs), default=0)
except Exception as exc:
    logger.warning("could not read initial bar count: %s", exc)

deepseek = (DeepSeekPredictor(api_key=config.deepseek_api_key,
                                initial_bar_count=_ds_bar_init)
            if config.deepseek_enabled else None)

binance_klines: List = []
ws_clients: set = set()


# Live state — read by REST endpoints + WebSocket pump.
current_state: Dict[str, Any] = {
    "price":                       None,
    "window_start_price":          None,
    "window_start_time":           None,
    "prediction":                  None,
    "ensemble_prediction":         None,
    "strategies":                  {},
    "deepseek_prediction":         None,
    "pending_deepseek_prediction": None,
    "pending_deepseek_ready":      False,
    "agree_accuracy":              None,
    "specialist_completed_at":     None,
    "backend_snapshot":            None,
    "bar_specialist_signals":      {},
    "bar_historical_analysis":     "",
    "bar_historical_context":      "",
    "bar_historical_analyst_fired": False,
    "bar_historical_failure_note": "",
    "bar_binance_expert":          {},
    "bar_trend_analyst":           {},
    "service_unavailable":         False,
    "service_unavailable_reason":  "",
}


# ── Utilities ─────────────────────────────────────────────────────────────────

def _json_safe(obj):
    """Convert numpy types → native Python before JSON serialization."""
    if isinstance(obj, dict):    return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [_json_safe(v) for v in obj]
    if isinstance(obj, np.integer):    return int(obj)
    if isinstance(obj, np.floating):   return float(obj)
    if isinstance(obj, np.bool_):      return bool(obj)
    if isinstance(obj, np.ndarray):    return obj.tolist()
    return obj


def _safe_storage(fn, *args, default=None, **kwargs):
    try: return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("storage %s failed: %s", fn.__name__, exc)
        return default


async def _safe_storage_async(fn, *args, default=None, **kwargs):
    try: return await asyncio.to_thread(fn, *args, **kwargs)
    except Exception as exc:
        logger.warning("storage %s failed: %s", fn.__name__, exc)
        return default


_WS_STRIP_KEYS = {"full_prompt", "raw_response"}

def _pred_for_ws(pred: Optional[Dict]) -> Optional[Dict]:
    if pred is None: return None
    return _json_safe({k: v for k, v in pred.items() if k not in _WS_STRIP_KEYS})


def _dashboard_signals_to_preds(dashboard_signals: Optional[Dict]) -> Dict:
    """Treat each microstructure signal as a vote in the ensemble — same shape as
    a strategy prediction. Confidence fixed at 0.65 because each indicator's
    individual accuracy is reweighted by the ensemble after the next resolution."""
    if not dashboard_signals: return {}
    directions = extract_signal_directions(dashboard_signals)
    return {f"dash:{name}": {"signal": direction, "confidence": 0.65,
                               "reasoning": f"microstructure:{name}"}
            for name, direction in directions.items() if direction in ("UP", "DOWN")}


# ── Postmortem + embedding background tasks ───────────────────────────────────

async def _embed_bar_background(window_start: float, bar_record: Dict):
    """Cohere-embed a resolved bar into pgvector. Idempotent at the SQL level —
    a UPDATE...WHERE embedding IS NULL refuses to overwrite a stored vector,
    last line of defense against budget-burning re-embed bugs."""
    if not config.cohere_api_key:
        return
    try:
        if float(window_start) in embedded_window_starts():
            return
    except Exception as exc:
        logger.warning("dedup check failed for bar %.0f — fail-closed: %s",
                        window_start, exc)
        return
    try:
        text = _bar_embed_text(bar_record)
        vec = await embed_text(config.cohere_api_key, text, input_type="search_document")
        store_bar_embedding(window_start, vec, embed_text=text,
                              embed_model=COHERE_EMBED_MODEL_ID)
        logger.info("embed stored for bar %.0f (%d dims)", window_start, len(vec))
    except CohereUnavailableError as exc:
        logger.warning("Cohere embed failed for bar %.0f: %s", window_start, exc)
    except Exception as exc:
        logger.warning("embed background task failed: %s", exc)


async def _run_postmortem_background(ds_record: Dict, actual: str, end_price: float,
                                       klines: list, features: Dict,
                                       dashboard_signals, embed_rec: Dict):
    """Fire postmortem, store it, then embed the bar with the postmortem
    appended. Ordering matters — embedding without the postmortem misses the
    'why this resolved this way' context."""
    if not config.deepseek_api_key:
        return
    try:
        text = await run_postmortem(api_key=config.deepseek_api_key,
                                      ds_record=ds_record, actual_direction=actual,
                                      end_price=end_price, klines=klines,
                                      features=features or {},
                                      dashboard_signals=dashboard_signals or {})
        ws = ds_record.get("window_start", 0)
        if text:
            _safe_storage(storage.store_postmortem, ws, text)
        asyncio.create_task(_embed_bar_background(
            ws, {**embed_rec, "postmortem": text or ""}))
    except Exception as exc:
        logger.warning("postmortem failed: %s", exc)
        ws = ds_record.get("window_start", 0)
        asyncio.create_task(_embed_bar_background(ws, {**embed_rec, "postmortem": ""}))


# ── DeepSeek async wrapper ────────────────────────────────────────────────────

async def _run_deepseek(*, prices, klines, features, strategy_preds, rolling_acc,
                          ds_acc, window_start_time, window_end_time,
                          window_start_price, ensemble_result=None,
                          dashboard_signals=None, indicator_accuracy=None,
                          ensemble_weights=None, historical_analysis=None,
                          dashboard_accuracy=None, binance_expert_analysis=None,
                          trend_analyst_analysis=None):
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))
    if not deepseek:
        return
    try:
        neutral_analysis = _safe_storage(storage.get_neutral_analysis, default={})
        result = await deepseek.predict(
            prices=prices, klines=klines, features=features,
            strategy_preds=strategy_preds, recent_accuracy=rolling_acc,
            deepseek_accuracy=ds_acc, window_start_time=window_start_time,
            window_start_price=window_start_price, ensemble_result=ensemble_result,
            dashboard_signals=dashboard_signals,
            indicator_accuracy=indicator_accuracy, ensemble_weights=ensemble_weights,
            historical_analysis=historical_analysis,
            dashboard_accuracy=dashboard_accuracy, neutral_analysis=neutral_analysis,
            binance_expert_analysis=binance_expert_analysis or current_state.get("bar_binance_expert") or None,
            trend_analyst_analysis=trend_analyst_analysis or current_state.get("bar_trend_analyst") or None,
            historical_failure_note=current_state.get("bar_historical_failure_note", ""),
        )
        # Discard if the bar already closed — we ran past budget.
        if time.time() > window_end_time:
            logger.warning("Pipeline overran bar %s — discarding prediction", bar_ts)
            return
        # Discard if a new bar already started.
        cur_window = current_state.get("window_start_time")
        if cur_window is not None and abs(cur_window - window_start_time) > 30:
            logger.warning("Stale prediction discarded for bar %s", bar_ts)
            return

        if result["signal"] not in ("ERROR", "UNAVAILABLE"):
            _safe_storage(
                storage.store_deepseek_prediction,
                window_start=window_start_time, window_end=window_end_time,
                start_price=window_start_price, signal=result["signal"],
                confidence=result.get("confidence", 50),
                reasoning=result.get("reasoning", ""),
                raw_response=result.get("raw_response", ""),
                full_prompt=result.get("full_prompt", ""),
                strategy_snapshot=json.dumps(strategy_preds, default=str),
                latency_ms=result.get("latency_ms", 0),
                window_count=result.get("window_count", 0),
                data_received=result.get("data_received", ""),
                data_requests=result.get("data_requests", ""),
                indicators_snapshot=json.dumps(_json_safe(features)),
                narrative=result.get("narrative", ""),
                free_observation=result.get("free_observation", ""),
                dashboard_signals_snapshot=json.dumps(_json_safe(dashboard_signals or {}), default=str),
                model_id=result.get("model_id", ""),
                prompt_version=result.get("prompt_version", ""),
            )
            current_state["pending_deepseek_prediction"] = result
            current_state["pending_deepseek_ready"]      = True
        else:
            logger.warning("DeepSeek returned %s for bar %s — not staging",
                            result.get("signal"), bar_ts)
    except Exception as exc:
        logger.error("_run_deepseek failed for bar %s: %s", bar_ts, exc, exc_info=True)


# ── Per-bar orchestration ─────────────────────────────────────────────────────

def _data_quality_check(prices: list, klines: list) -> Tuple[bool, str]:
    if len(prices) < 30:
        return False, f"insufficient ticks ({len(prices)}/30)"
    if not klines or len(klines) < 20:
        return False, f"insufficient klines ({len(klines) if klines else 0}/20)"
    try:
        last_bar_ts = int(klines[-1][0]) / 1000
        if time.time() - last_bar_ts > 180:
            return False, f"klines stale ({time.time() - last_bar_ts:.0f}s old)"
    except Exception: pass
    return True, ""


async def _run_full_prediction(prices: list):
    """Open a bar: fan out parallel specialists + dashboard + binance expert +
    trend analyst, await each, run ensemble, then fire main predictor."""
    window_start_price = prices[-1]
    now = time.time()
    window_start_time = now - (now % 300)

    current_state.update({
        "window_start_price":          window_start_price,
        "window_start_time":           window_start_time,
        "specialist_completed_at":     None,
        "pending_deepseek_prediction": None,
        "pending_deepseek_ready":      False,
        "bar_historical_analysis":     "",
        "bar_historical_context":      "",
        "bar_historical_analyst_fired": False,
        "bar_historical_failure_note": "",
        "bar_binance_expert":          {},
        "bar_trend_analyst":           {},
    })

    bar_count = (deepseek.window_count + 1) if deepseek else 0
    logger.info("=== BAR OPEN #%d === %s | price=$%.2f ===",
                 bar_count, time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time)),
                 window_start_price)

    klines = list(binance_klines)
    strategy_preds = get_all_predictions(prices, ohlcv=klines)
    features = feature_engine.compute_all(prices, ohlcv=klines or None)
    try:
        strategy_preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=klines)
    except Exception as exc:
        logger.warning("LR predict error: %s", exc)

    if not (features and len(features) > 5):
        logger.warning("Feature compute degraded — proceeding without specialists")
        return None, _json_safe(strategy_preds), window_start_time, window_start_price

    # Dashboard fetch + unified specialists in parallel (independent inputs)
    dashboard_task = asyncio.create_task(
        fetch_dashboard_signals(coinalyze_key=config.coinalyze_key,
                                  coinglass_key=config.coinglass_key))
    spec_task = (asyncio.create_task(asyncio.wait_for(
                     run_specialists(config.deepseek_api_key, klines), timeout=100.0))
                  if deepseek and klines else None)

    # One pattern-history load per bar — reused for similarity + dashboard accuracy
    all_history = load_pattern_history()[-10000:]
    dashboard_acc = compute_dashboard_accuracy_from_records(all_history[-200:])

    dashboard_signals = None
    try:
        dashboard_signals = await asyncio.wait_for(dashboard_task, timeout=10.0)
        n_ok = sum(1 for k, v in dashboard_signals.items() if v is not None and k != "fetched_at")
        logger.info("Dashboard %d sources ok", n_ok)
    except asyncio.TimeoutError:
        logger.warning("Dashboard timed out")
        dashboard_task.cancel()
    except Exception as exc:
        logger.warning("Dashboard error: %s", exc)

    if dashboard_signals:
        strategy_preds.update(_dashboard_signals_to_preds(dashboard_signals))

    # Binance expert sees the dashboard data — runs serially before historical
    # analyst so historical retrieval can include the expert's read in its query.
    dash_directions = extract_signal_directions(dashboard_signals) if dashboard_signals else {}
    binance_expert_result = None
    if deepseek and dashboard_signals:
        try:
            binance_expert_result = await asyncio.wait_for(
                run_binance_expert(config.deepseek_api_key, dashboard_signals),
                timeout=75.0)
            if binance_expert_result:
                current_state["bar_binance_expert"] = _json_safe(binance_expert_result)
        except asyncio.TimeoutError:
            logger.warning("Binance expert timed out")
        except Exception as exc:
            logger.warning("Binance expert error: %s", exc)

    # Trend analyst — reads only historical responses, can run in parallel with
    # historical analyst's pre-work but we keep it serial for prompt simplicity.
    trend_analyst_result = None
    if deepseek:
        try:
            past = storage.get_recent_responses_for_tape(20) or []
            trend_analyst_result = await asyncio.wait_for(
                run_trend_analyst(config.deepseek_api_key, past), timeout=75.0)
            if trend_analyst_result and trend_analyst_result.get("available"):
                current_state["bar_trend_analyst"] = _json_safe(trend_analyst_result)
        except Exception as exc:
            logger.warning("Trend analyst error: %s", exc)

    # JOIN: await unified specialists. The historical analyst & main predictor
    # MUST not fire before this completes; merging specialist votes into the
    # ensemble is the only reason the analyst sees them.
    specialist_results: Dict = {}
    if spec_task is not None:
        try:
            specialist_results = await spec_task or {}
            for key, result in specialist_results.items():
                if result is not None:
                    strategy_preds[key] = result
            current_state["bar_specialist_signals"] = _json_safe(specialist_results)
        except Exception as exc:
            logger.warning("Specialists error: %s", exc)
        current_state["specialist_completed_at"] = time.time()

    # Ensemble vote — now that everything has merged into strategy_preds.
    pred = _json_safe(ensemble.predict(strategy_preds))
    pred["source"] = "ensemble"
    strategy_preds = _json_safe(strategy_preds)
    current_state["prediction"]          = pred
    current_state["ensemble_prediction"] = pred
    current_state["strategies"]          = strategy_preds
    current_state["agree_accuracy"]      = _safe_storage(storage.get_agree_accuracy, default={})

    # Historical analyst — full RAG pipeline. CohereUnavailableError pauses
    # the service rather than silently degrading, because retrieval is the
    # single biggest source of edge.
    historical_analyst_fired = False
    historical_analysis = None
    if deepseek and features:
        try:
            sig, historical_analysis = await run_historical_analyst(
                api_key=config.deepseek_api_key,
                history_records=all_history,
                current_indicators=features, current_strategy_votes=strategy_preds,
                window_start_time=window_start_time,
                specialist_signals=specialist_results or None,
                ensemble_signal=pred["signal"], ensemble_conf=float(pred.get("confidence", 0)),
                dashboard_directions=dash_directions or None,
                dashboard_signals_raw=dashboard_signals or None,
                binance_expert_analysis=binance_expert_result,
                cohere_api_key=config.cohere_api_key,
            )
            current_state["service_unavailable"]        = False
            current_state["service_unavailable_reason"] = ""
            if sig and historical_analysis:
                historical_analyst_fired = True
                current_state["bar_historical_analysis"] = historical_analysis
                current_state["bar_historical_context"]  = _build_current_bar(
                    features, strategy_preds, window_start_time, specialist_results,
                    pred["signal"], pred["confidence"], dash_directions)
                strategy_preds["historical_analyst"] = sig
        except CohereUnavailableError as exc:
            logger.error("Cohere unavailable — pausing predictions: %s", exc)
            current_state["service_unavailable"]        = True
            current_state["service_unavailable_reason"] = str(exc)
            return None, strategy_preds, window_start_time, window_start_price
        except Exception as exc:
            logger.warning("Historical analyst error: %s", exc)
            current_state["bar_historical_failure_note"] = _fmt_exc(exc)

    current_state["bar_historical_analyst_fired"] = historical_analyst_fired

    # Roll up accuracy stats and update ensemble weights.
    rolling_acc = 0.0; ds_acc = {}; indicator_acc_full = {}
    if deepseek and features:
        _, _, rolling_acc = _safe_storage(storage.get_rolling_accuracy,
                                            config.rolling_window_size,
                                            default=(0, 0, 0.0))
        rolling_acc = rolling_acc or 0.0
        ds_acc = _safe_storage(storage.get_deepseek_accuracy, default={})
    indicator_acc_full = _safe_storage(storage.get_strategy_accuracy_full, 100, default={})
    for name, stats in dashboard_acc.items():
        indicator_acc_full[f"dash:{name}"] = stats
    if indicator_acc_full:
        ensemble.update_weights_from_full_stats(indicator_acc_full)

    current_state["backend_snapshot"] = _json_safe({
        "window_num": bar_count, "window_start": window_start_time,
        "window_start_price": window_start_price,
        "prices_last20": list(prices[-20:]) if len(prices) >= 20 else list(prices),
        "features": features, "strategy_preds": strategy_preds,
        "dashboard_signals": dashboard_signals,
        "ensemble_result": {
            "signal": pred["signal"], "confidence": pred["confidence"],
            "bullish_count": pred["bullish_count"], "bearish_count": pred["bearish_count"],
            "up_probability": pred.get("up_probability", 0.5),
            "weighted_up_score": pred.get("weighted_up_score", 0),
            "weighted_down_score": pred.get("weighted_down_score", 0),
        },
        "rolling_acc": rolling_acc, "ds_acc": ds_acc,
        "captured_at": time.time(),
    })

    # Fire main DeepSeek prediction in a background task so we can sleep until
    # bar close instead of blocking. The main predictor stages its result; the
    # bar-close handler reveals it to the UI.
    if deepseek and features:
        ds_strategy_preds = {
            k: v for k, v in strategy_preds.items()
            if k not in SPECIALIST_KEYS and not k.startswith("dash:")
            and k != "historical_analyst"
        }
        asyncio.create_task(_run_deepseek(
            prices=list(prices), klines=list(binance_klines), features=features,
            strategy_preds=ds_strategy_preds, rolling_acc=rolling_acc, ds_acc=ds_acc,
            window_start_time=window_start_time,
            window_end_time=window_start_time + config.window_duration_seconds,
            window_start_price=window_start_price,
            ensemble_result=pred, dashboard_signals=dashboard_signals,
            indicator_accuracy=indicator_acc_full,
            ensemble_weights=ensemble.get_weights(),
            historical_analysis=historical_analysis,
            dashboard_accuracy=dashboard_acc,
            binance_expert_analysis=binance_expert_result,
            trend_analyst_analysis=trend_analyst_result,
        ))

    return pred, strategy_preds, window_start_time, window_start_price


async def _fetch_bar_ohlc(window_start_time: int) -> Optional[Tuple[float, float, str]]:
    """Score the bar against the exchange's 5m kline OHLC — never the stale collector tick."""
    target_ms = int(window_start_time) * 1000

    async def _try(url, params):
        try:
            connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
            async with aiohttp.ClientSession(connector=connector) as s:
                async with s.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200: return None
                    return await r.json()
        except Exception: return None

    venues = [
        ("bybit",   "https://api.bybit.com/v5/market/kline",
         {"category": "spot", "symbol": "BTCUSDT", "interval": "5",
          "start": str(target_ms), "end": str(target_ms + 300_000), "limit": "1"},
         lambda d: (d.get("result", {}) or {}).get("list") or []),
        ("okx",     "https://www.okx.com/api/v5/market/history-candles",
         {"instId": "BTC-USDT", "bar": "5m",
          "after": str(target_ms + 300_000), "before": str(target_ms - 1), "limit": "5"},
         lambda d: d.get("data") or []),
        ("binance", "https://api.binance.com/api/v3/klines",
         {"symbol": "BTCUSDT", "interval": "5m",
          "startTime": str(target_ms), "endTime": str(target_ms + 299_999), "limit": "1"},
         lambda d: d or []),
    ]
    for src, url, params, extract in venues:
        data = await _try(url, params)
        if not data: continue
        for b in extract(data):
            if int(b[0]) == target_ms:
                return float(b[1]), float(b[4]), src
    return None


async def _fetch_bar_ohlc_with_retry(window_start_time: int, retries: int = 3):
    for i in range(retries):
        ohlc = await _fetch_bar_ohlc(window_start_time)
        if ohlc: return ohlc
        if i < retries - 1: await asyncio.sleep(5)
    return None


async def _resolve_window(window_start_time, window_start_price, pred, strategy_preds):
    """Bar closed. Reveal staged DeepSeek result, score against the kline OHLC
    (authoritative — not the last collected tick), persist the resolved bar to
    pattern_history, and fire the postmortem-then-embed background pipeline."""
    bar_ts = time.strftime("%H:%M:%S UTC", time.gmtime(window_start_time))
    if pred is None or not isinstance(pred, dict):
        return

    pending = current_state.get("pending_deepseek_prediction")
    if pending and pending.get("signal") not in (None, "ERROR", "UNAVAILABLE"):
        current_state["deepseek_prediction"]         = pending
        current_state["pending_deepseek_prediction"] = None
        current_state["pending_deepseek_ready"]      = False
        logger.info(">>> Bar %s closed — DS revealed: %s %d%%", bar_ts,
                     pending["signal"], pending.get("confidence", 0))

    bar_ohlc = await _fetch_bar_ohlc_with_retry(int(window_start_time))
    if bar_ohlc:
        bar_open, bar_close, src = bar_ohlc
        if abs(bar_close - bar_open) < 1e-6:
            actual = None; correct = None
        else:
            actual = "UP" if bar_close > bar_open else "DOWN"
            correct = None if pred["signal"] == "NEUTRAL" else (actual == pred["signal"])
        end_price = bar_close
        resolved_start = bar_open
        logger.info("Bar %s OHLC[%s]: O=%.2f C=%.2f Δ=%+.2f → %s",
                     bar_ts, src, bar_open, bar_close, bar_close - bar_open, actual)
    else:
        # Last-resort tick fallback — kline sources all unreachable.
        end_prices = collector.get_prices(1)
        end_price = end_prices[-1] if end_prices else None
        if not window_start_price or not end_price or \
            abs(end_price - window_start_price) < 1e-6:
            actual = None; correct = None
        else:
            actual = "UP" if end_price > window_start_price else "DOWN"
            correct = None if pred["signal"] == "NEUTRAL" else (actual == pred["signal"])
        resolved_start = window_start_price

    if end_price is None:
        return

    await _safe_storage_async(storage.store_prediction,
                                window_start=window_start_time,
                                window_end=window_start_time + config.window_duration_seconds,
                                start_price=resolved_start, signal=pred["signal"],
                                confidence=pred["confidence"], strategy_votes=strategy_preds)
    await _safe_storage_async(storage.resolve_prediction, window_start_time, end_price)

    snap = current_state.get("backend_snapshot") or {}
    snap_indicators = snap.get("features", {})
    snap_dash_raw = snap.get("dashboard_signals") or {}
    if isinstance(snap_dash_raw, str):
        try: snap_dash_raw = json.loads(snap_dash_raw)
        except Exception: snap_dash_raw = {}

    ds_pred = current_state.get("deepseek_prediction") or {}
    ds_correct = (None if ds_pred.get("signal") in (None, "ERROR", "UNAVAILABLE", "NEUTRAL")
                  else (actual == ds_pred["signal"]))

    await _safe_storage_async(storage.resolve_deepseek_prediction,
                                window_start_time, end_price, actual)
    if bar_ohlc:
        await _safe_storage_async(storage.update_deepseek_start_price,
                                    window_start_time, resolved_start)

    embed_rec = {
        "window_start": window_start_time,
        "window_count": ds_pred.get("window_count") or (deepseek.window_count if deepseek else 0),
        "actual_direction": actual, "start_price": resolved_start, "end_price": end_price,
        "ensemble_signal": pred.get("signal", ""),
        "ensemble_conf":   pred.get("confidence", 0),
        "deepseek_signal": ds_pred.get("signal", ""),
        "deepseek_conf":   ds_pred.get("confidence", 0),
        "deepseek_correct": ds_correct,
        "deepseek_reasoning": ds_pred.get("reasoning", ""),
        "deepseek_narrative": ds_pred.get("narrative", ""),
        "deepseek_free_obs":  ds_pred.get("free_observation", ""),
        "indicators":     snap_indicators, "strategy_votes": strategy_preds,
        "specialist_signals": current_state.get("bar_specialist_signals", {}),
        "dashboard_signals_raw": extract_signal_directions(snap_dash_raw),
        "historical_analysis": current_state.get("bar_historical_analysis", ""),
        "binance_expert_analysis": current_state.get("bar_binance_expert", {}),
        "session": _session_label(window_start_time),
        "postmortem": "",
    }

    if ds_pred.get("signal") not in (None, "ERROR", "UNAVAILABLE"):
        pm_record = {**ds_pred, "window_start": window_start_time,
                     "start_price": resolved_start}
        asyncio.create_task(_run_postmortem_background(
            ds_record=pm_record, actual=actual, end_price=end_price,
            klines=list(binance_klines), features=dict(snap_indicators),
            dashboard_signals=snap_dash_raw, embed_rec=embed_rec))

    # Append to pattern_history (without postmortem — postmortem handler
    # re-embeds with it once it lands).
    try:
        ens_t, ens_c, ens_a, *_ = (await _safe_storage_async(
            storage.get_total_accuracy, default=(0, 0, 0.0, 0))) or (0, 0, 0.0, 0)
        ds_acc_snap = (await _safe_storage_async(
            storage.get_deepseek_accuracy, default={"total": 0, "correct": 0, "accuracy": 0.0})) or {}
        agree_snap = (await _safe_storage_async(
            storage.get_agree_accuracy, default={})) or {}
        accuracy_snap = {
            "ensemble_accuracy": round(ens_a * 100, 2),
            "ensemble_total": ens_t, "ensemble_correct": ens_c,
            "deepseek_accuracy": round(ds_acc_snap.get("accuracy", 0.0) * 100, 2),
            "deepseek_total":    ds_acc_snap.get("total", 0),
            "deepseek_correct":  ds_acc_snap.get("correct", 0),
            "agree_accuracy": round(agree_snap.get("accuracy_agree", 0.0) * 100, 2),
            "agree_total":    agree_snap.get("total_agree", 0),
            "agree_correct":  agree_snap.get("correct_agree", 0),
        }
        ds_signal = ds_pred.get("signal", "")
        trade_action = ds_signal if ds_signal in ("UP", "DOWN", "NEUTRAL") else "NEUTRAL"
        append_resolved_window(
            window_start=window_start_time,
            window_end=window_start_time + config.window_duration_seconds,
            actual_direction=actual,
            start_price=window_start_price, end_price=end_price,
            ensemble_signal=pred["signal"], ensemble_conf=pred["confidence"],
            ensemble_correct=(None if pred["signal"] == "NEUTRAL" else (actual == pred["signal"])),
            deepseek_signal=ds_signal, deepseek_conf=ds_pred.get("confidence", 0),
            deepseek_correct=ds_correct,
            deepseek_reasoning=ds_pred.get("reasoning", ""),
            deepseek_narrative=ds_pred.get("narrative", ""),
            deepseek_free_obs=ds_pred.get("free_observation", ""),
            specialist_signals=current_state.get("bar_specialist_signals", {}),
            historical_analysis=current_state.get("bar_historical_analysis", ""),
            binance_expert_analysis=current_state.get("bar_binance_expert", {}),
            strategy_votes=strategy_preds, indicators=snap_indicators,
            dashboard_signals_raw=extract_signal_directions(snap_dash_raw),
            accuracy_snapshot=accuracy_snap,
            full_prompt=ds_pred.get("full_prompt", ""), trade_action=trade_action,
            window_count=ds_pred.get("window_count") or (deepseek.window_count if deepseek else 0),
        )
    except Exception as exc:
        logger.warning("pattern_history append failed: %s", exc)

    current_state["bar_specialist_signals"] = {}
    current_state["bar_binance_expert"]     = {}
    logger.info("Window closed | actual=%s pred=%s %s | Δ%.2f",
                 actual, pred["signal"],
                 ("WIN" if correct else ("NEUTRAL" if correct is None else "LOSS")),
                 end_price - window_start_price)

    current_state["agree_accuracy"] = await _safe_storage_async(storage.get_agree_accuracy, default={})
    total_resolved, *_ = await _safe_storage_async(
        storage.get_rolling_accuracy, default=(0, 0, 0.0)) or (0, 0, 0.0)
    if total_resolved >= config.min_predictions_for_weight_update:
        acc = await _safe_storage_async(storage.get_strategy_rolling_accuracy, default={})
        for name, stats in compute_dashboard_accuracy_from_records(load_pattern_history()[-20:]).items():
            acc[f"dash:{name}"] = stats["accuracy"]
        if acc:
            ensemble.update_weights(acc)


# ── Background tasks ──────────────────────────────────────────────────────────

async def _refresh_indicators():
    prices = collector.get_prices(400)
    if len(prices) < 30:
        return
    try:
        klines = list(binance_klines)
        preds = get_all_predictions(prices, ohlcv=klines)
        try: preds["ml_logistic"] = lr_strategy.predict(prices, ohlcv=klines)
        except Exception: pass
        existing = current_state.get("strategies") or {}
        for key in SPECIALIST_KEYS:
            if key in existing: preds[key] = existing[key]
        for key, val in existing.items():
            if key.startswith("dash:"):
                preds[key] = val
        current_state["strategies"] = _json_safe(preds)
    except Exception as exc:
        logger.warning("indicator refresh error: %s", exc)


async def _kline_fetch(url: str, params: Dict, transform) -> Optional[List]:
    """One kline fetch + transform. Returns raw list[[t,o,h,l,c,v,_,quote,trades,buy_base]] or None."""
    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200: return None
            return transform(await resp.json())


# Each entry: (label, url, params, transform). Tried in order; first success wins.
_KLINE_VENUES: List[Tuple[str, str, Dict, Callable]] = [
    ("Bybit",
     "https://api.bybit.com/v5/market/kline",
     {"category": "spot", "symbol": "BTCUSDT", "interval": "1", "limit": "500"},
     lambda d: [[int(b[0]), b[1], b[2], b[3], b[4], b[5], None,
                  b[6] if len(b) > 6 else None, None, None]
                 for b in reversed(d["result"]["list"])]),
    ("OKX",
     "https://www.okx.com/api/v5/market/history-candles",
     {"instId": "BTC-USDT", "bar": "1m", "limit": "300"},
     lambda d: [[int(b[0]), b[1], b[2], b[3], b[4], b[5], None,
                  b[7] if len(b) > 7 else None, None, None]
                 for b in reversed(d["data"])]),
    ("Kraken",
     "https://api.kraken.com/0/public/OHLC",
     {"pair": "XBTUSD", "interval": "1"},
     lambda d: [[int(b[0]) * 1000, b[1], b[2], b[3], b[4], b[6], None,
                  str(float(b[5]) * float(b[6])) if b[5] and b[6] else None,
                  b[7] if len(b) > 7 else None, None]
                 for b in list(d["result"].values())[0]]),
    ("Binance",
     "https://api.binance.com/api/v3/klines",
     {"symbol": "BTCUSDT", "interval": "1m", "limit": "500"},
     lambda d: d),
]


async def run_binance_feed():
    """Pull 1m klines every 60s. Bybit primary; OKX/Kraken/Binance fallback."""
    while True:
        for label, url, params, transform in _KLINE_VENUES:
            try:
                rows = await _kline_fetch(url, params, transform)
                if rows:
                    binance_klines.clear(); binance_klines.extend(rows)
                    logger.info("%s klines updated: %d candles", label, len(rows))
                    collector.seed_from_klines(binance_klines)
                    await _refresh_indicators()
                    break
            except Exception as exc:
                logger.warning("%s klines failed: %s", label, exc)
        await asyncio.sleep(60)


async def run_indicator_refresh():
    await asyncio.sleep(20)
    while True:
        await asyncio.sleep(15)
        await _refresh_indicators()


async def run_collector():
    def _on_tick(tick):
        current_state["price"] = tick.mid_price
        _safe_storage(storage.store_tick, tick.timestamp, tick.mid_price,
                       tick.bid_price, tick.ask_price, tick.spread)
    collector.on_tick(_on_tick)
    await collector.start()


async def run_prediction_loop():
    """The five-minute drumbeat. Predict, sleep until close, resolve."""
    await asyncio.sleep(5)
    last_processed: Optional[int] = None
    while True:
        try:
            prices = collector.get_prices(400)
            klines = list(binance_klines)
            ok, reason = _data_quality_check(prices, klines)
            if not ok:
                logger.warning("SKIP — %s — sleeping 15s", reason)
                await asyncio.sleep(15); continue
            now_ts = time.time()
            current_window = int(now_ts - (now_ts % 300))
            # If a prior crash put us back in the same bar, sleep until close —
            # don't wipe the bar's already-collected specialist results.
            if current_window == last_processed:
                close_ts = current_window + config.window_duration_seconds
                wait = max(1, close_ts - time.time())
                logger.warning("Same bar still in flight — sleeping %.1fs", wait)
                await asyncio.sleep(wait); continue
            last_processed = current_window
            pred, strategy_preds, ws_time, ws_price = await _run_full_prediction(prices)
            close_ts = ws_time + config.window_duration_seconds
            await asyncio.sleep(max(1, close_ts - time.time()))
            asyncio.create_task(_resolve_window(ws_time, ws_price, pred, strategy_preds))
            await asyncio.sleep(0)
        except Exception as exc:
            logger.error("Prediction loop crashed: %s — recovering in 10s", exc, exc_info=True)
            await asyncio.sleep(10)


# ── 8. SERVER — FastAPI + WebSocket. Mounts the React UI from ./infra/. ──

_INFRA_DIR = pathlib.Path(__file__).parent / "infra"

app = FastAPI(title="BTC 5-minute predictor", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                    allow_methods=["*"], allow_headers=["*"])

if _INFRA_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_INFRA_DIR)), name="static")


@app.on_event("startup")
async def _startup():
    asyncio.create_task(run_collector())
    asyncio.create_task(run_prediction_loop())
    asyncio.create_task(run_binance_feed())
    asyncio.create_task(run_indicator_refresh())


class BacktestResponse(BaseModel):
    total_predictions: int
    correct_predictions: int
    accuracy: float
    all_time_total: int
    all_time_correct: int
    all_time_accuracy: float
    all_time_neutral: int
    strategy_accuracies: Dict[str, float]


@app.get("/")
async def serve_dashboard():
    index = _INFRA_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"status": "running", "infra_dir": str(_INFRA_DIR), "found": False}


@app.get("/price")
async def get_price():
    return {
        "price":              collector.current_price,
        "tick_count":         collector.tick_count,
        "data_source":        collector.data_source,
        "window_start_price": current_state["window_start_price"],
    }


@app.get("/deepseek-status")
async def get_deepseek_status():
    return {
        "pending_deepseek_ready":      current_state.get("pending_deepseek_ready", False),
        "pending_deepseek_prediction": _pred_for_ws(current_state.get("pending_deepseek_prediction")),
        "deepseek_prediction":         _pred_for_ws(current_state.get("deepseek_prediction")),
        "deepseek_enabled":            deepseek is not None,
        "window_start_time":           current_state.get("window_start_time"),
        "specialist_completed_at":     current_state.get("specialist_completed_at"),
        "bar_historical_analysis":     current_state.get("bar_historical_analysis", ""),
        "bar_historical_context":      current_state.get("bar_historical_context", ""),
        "bar_binance_expert":          current_state.get("bar_binance_expert", {}),
        "bar_trend_analyst":           current_state.get("bar_trend_analyst", {}),
        "service_unavailable":         current_state.get("service_unavailable", False),
        "service_unavailable_reason":  current_state.get("service_unavailable_reason", ""),
    }


@app.get("/backtest", response_model=BacktestResponse)
async def get_backtest():
    total, correct, accuracy = _safe_storage(storage.get_rolling_accuracy,
                                                config.rolling_window_size,
                                                default=(0, 0, 0.0))
    at_total, at_correct, at_accuracy, at_neutral = _safe_storage(
        storage.get_total_accuracy, default=(0, 0, 0.0, 0))
    strategy_acc = _safe_storage(storage.get_strategy_rolling_accuracy, default={})
    return BacktestResponse(
        total_predictions=total, correct_predictions=correct, accuracy=accuracy,
        all_time_total=at_total, all_time_correct=at_correct,
        all_time_accuracy=at_accuracy, all_time_neutral=at_neutral,
        strategy_accuracies=strategy_acc)


@app.get("/predictions/recent")
async def get_recent_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_predictions, n, default=[])


@app.get("/weights")
async def get_weights():
    weights = ensemble.get_weights()
    acc_stats = _safe_storage(storage.get_strategy_accuracy_full, 100, default={})
    result: Dict[str, Dict] = {}
    for name, w in weights.items():
        stats = acc_stats.get(name, {})
        accuracy = stats.get("accuracy"); total = stats.get("total", 0)
        result[name] = {
            "weight": w,
            "accuracy": round(accuracy * 100, 1) if accuracy is not None else None,
            "correct":  stats.get("correct", 0),
            "total":    total,
            "label":    accuracy_to_label(accuracy or 0.5, total) if total > 0 else "LEARNING",
        }
    for name, stats in acc_stats.items():
        if name not in result:
            accuracy = stats.get("accuracy", 0.5); total = stats.get("total", 0)
            result[name] = {"weight": 1.0, "accuracy": round(accuracy * 100, 1),
                             "correct": stats.get("correct", 0), "total": total,
                             "label": accuracy_to_label(accuracy, total)}
    return result


@app.get("/deepseek/accuracy")
async def get_deepseek_accuracy():
    acc = _safe_storage(storage.get_deepseek_accuracy,
                          default={"total": 0, "correct": 0, "accuracy": 0.0})
    return {**acc, "current_prediction": current_state.get("deepseek_prediction"),
             "enabled": config.deepseek_enabled}


@app.get("/accuracy/agree")
async def get_agree_accuracy():
    return _safe_storage(storage.get_agree_accuracy, default={})


@app.get("/backend")
async def get_backend_snapshot():
    return {"snapshot": current_state.get("backend_snapshot") or {},
             "deepseek": current_state.get("deepseek_prediction") or {}}


@app.get("/deepseek/predictions")
async def get_deepseek_predictions(n: int = 50):
    return _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])


@app.get("/deepseek/predictions/{window_start}")
async def get_deepseek_prediction_detail(window_start: float):
    docs = _safe_storage(storage.get_recent_deepseek_predictions, 9999, default=[])
    for doc in docs:
        if doc.get("window_start") == window_start:
            return doc
    return {}


@app.get("/historical-analysis/{window_start}")
async def get_historical_analysis(window_start: float):
    """Full pipeline audit for one window — raw prompt, response, postmortem."""
    docs = _safe_storage(storage.get_recent_deepseek_predictions, 9999, default=[])
    doc = next((d for d in docs if d.get("window_start") == window_start), None)
    if not doc: return {"status": "not_found", "window_start": window_start}
    try:
        strategy_snap   = json.loads(doc.get("strategy_snapshot") or "{}")
        indicators_snap = json.loads(doc.get("indicators_snapshot") or "{}")
        dashboard_snap  = json.loads(doc.get("dashboard_signals_snapshot") or "{}")
    except Exception:
        strategy_snap, indicators_snap, dashboard_snap = {}, {}, {}
    return {
        "status": "ok",
        "window_start": doc.get("window_start"), "window_end": doc.get("window_end"),
        "start_price":  doc.get("start_price"),  "end_price":  doc.get("end_price"),
        "actual_direction": doc.get("actual_direction"), "correct": doc.get("correct"),
        "prediction": {"signal": doc.get("signal"), "confidence": doc.get("confidence"),
                        "reasoning": doc.get("reasoning", ""),
                        "narrative": doc.get("narrative", ""),
                        "free_observation": doc.get("free_observation", "")},
        "input_data": {"strategies": strategy_snap, "indicators": indicators_snap,
                        "dashboard_signals": dashboard_snap},
        "pipeline": {"latency_ms": doc.get("latency_ms", 0),
                      "window_count": doc.get("window_count", 0)},
        "prompting": {"full_prompt": doc.get("full_prompt", ""),
                       "raw_response": doc.get("raw_response", "")},
        "metadata": {"postmortem": doc.get("postmortem", "")},
    }


@app.get("/deepseek/source-history")
async def get_deepseek_source_history(n: int = 20):
    docs = _safe_storage(storage.get_recent_deepseek_predictions, n, default=[])
    out = []
    for doc in docs:
        for field in ("dashboard_signals_snapshot", "strategy_snapshot",
                       "indicators_snapshot"):
            raw = doc.get(field)
            if raw and isinstance(raw, str):
                try: doc[field] = json.loads(raw)
                except Exception: pass
        out.append(doc)
    return out


@app.get("/accuracy/all")
async def get_all_accuracy(n: int = 100):
    """Public — powers the SOURCES tab. No PII, just signal performance stats."""
    try:
        all_stats = compute_all_indicator_accuracy(n if n > 0 else None)
        wts = ensemble.get_weights()
    except Exception as exc:
        return {"ai": [], "strategies": [], "specialists": [], "microstructure": [],
                 "error": str(exc)}
    all_stats.pop("best_indicator", None)

    def _row(key, name, stats, weight=None):
        wins = stats.get("wins", stats.get("correct", 0))
        total = stats.get("total", 0); directional = stats.get("directional", total)
        acc = stats.get("accuracy", 0.5)
        label = accuracy_to_label(acc, directional) if directional >= 3 else "LEARNING"
        return {"key": key, "name": name, "accuracy": round(acc * 100, 1),
                 "correct": wins, "total": total, "label": label,
                 "weight": round(weight, 3) if weight is not None else None}

    STRAT_NAMES = {"rsi": "RSI", "macd": "MACD", "stochastic": "Stochastic",
                    "ema_cross": "EMA", "supertrend": "Supertrend", "adx": "ADX",
                    "alligator": "Alligator", "acc_dist": "Acc/Dist",
                    "dow_theory": "Dow", "fib_pullback": "Fibonacci",
                    "harmonic": "Harmonic", "vwap": "AVWAP", "ml_logistic": "LinReg"}
    SPEC_NAMES = {"spec:dow_theory": "DOW", "spec:fib_pullback": "FIB",
                   "spec:alligator": "ALG", "spec:acc_dist": "ACD", "spec:harmonic": "HAR"}
    DASH_NAMES = {"dash:order_book": "Order Book", "dash:long_short": "L/S",
                   "dash:taker_flow": "Taker Flow", "dash:liquidations": "Liquidations",
                   "dash:fear_greed": "F&G", "dash:coinalyze": "Coinalyze",
                   "dash:deribit_dvol": "DVOL", "dash:cvd": "CVD",
                   "dash:spot_perp_basis": "Basis"}
    ai = [_row(k, n2, all_stats[k], wts.get(k)) for k, n2 in
            [("deepseek", "DeepSeek"), ("ensemble", "Ensemble")] if k in all_stats]
    strategies = [_row(f"strat:{k}", n2, all_stats[f"strat:{k}"], wts.get(k))
                   for k, n2 in STRAT_NAMES.items() if f"strat:{k}" in all_stats]
    specialists = [_row(k, n2, all_stats[k]) for k, n2 in SPEC_NAMES.items() if k in all_stats]
    microstructure = [_row(k, n2, all_stats[k]) for k, n2 in DASH_NAMES.items() if k in all_stats]
    for lst in (ai, strategies, specialists, microstructure):
        lst.sort(key=lambda r: (r["total"] >= 3, r["accuracy"]), reverse=True)
    return {"ai": ai, "strategies": strategies,
             "specialists": specialists, "microstructure": microstructure}


@app.get("/errors")
async def get_errors():
    out = []
    for e in reversed(_error_log):
        dt = datetime.fromtimestamp(e["logged_at"], tz=timezone.utc)
        out.append({**e, "logged_at_str": dt.strftime("%Y-%m-%d %H:%M:%S UTC")})
    return {"errors": out, "count": len(out)}


@app.get("/api/suggestions")
async def get_suggestions(limit: int = 30):
    """Distilled lessons from postmortems + non-fatal flags emitted by specialists."""
    _LESSON_FIELDS = (("LESSON_NAME:", "name"), ("LESSON_RULE:", "rule"),
                       ("LESSON_EFFECT:", "effect"), ("LESSON_PRECONDITIONS:", "preconds"),
                       ("ROOT_CAUSE:", "root"), ("ERROR_CLASS:", "err_class"))
    lessons: List[Dict] = []
    try:
        with _db() as (_, cur):
            cur.execute("SELECT window_start, signal, correct, postmortem FROM deepseek_predictions "
                        "WHERE postmortem IS NOT NULL AND LENGTH(postmortem) > 200 "
                        "ORDER BY window_start DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
        seen: set = set()
        for ws, sig, correct, pm in rows:
            fields = {k: "" for _, k in _LESSON_FIELDS}
            for line in pm.splitlines():
                s = line.strip()
                for prefix, attr in _LESSON_FIELDS:
                    if s.startswith(prefix):
                        fields[attr] = s.split(":", 1)[1].strip()
                        break
            name = fields["name"]
            if not name or name.upper() in ("NONE", "N/A") or name in seen:
                continue
            seen.add(name)
            lessons.append({
                "window_start": float(ws),
                "window_start_str": datetime.fromtimestamp(float(ws), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                "signal": sig, "correct": correct,
                "name": name, "rule": fields["rule"], "effect": fields["effect"],
                "preconditions": fields["preconds"],
                "root_cause": fields["root"], "error_class": fields["err_class"],
            })
    except Exception as exc:
        logger.warning("postmortem lessons fetch failed: %s", exc)

    hist_sugg, uni_sugg = [], []
    for e in _error_log:
        if e.get("signal") != "SUGGESTION": continue
        src = (e.get("source") or "").lower()
        line = f"[{e.get('bar_time','')}] {e.get('message','')}"
        if "historical" in src and len(hist_sugg) < 20: hist_sugg.append(line)
        elif "specialists" in src and len(uni_sugg) < 20: uni_sugg.append(line)

    return {"lessons": lessons,
             "historical_analyst_suggestions": hist_sugg,
             "unified_analyst_suggestions":    uni_sugg,
             "counts": {"lessons": len(lessons),
                         "historical_analyst": len(hist_sugg),
                         "unified_analyst": len(uni_sugg)}}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.add(websocket)
    logger.info("WS connected (%d total)", len(ws_clients))
    try:
        while True:
            if current_state["price"] is not None:
                try:
                    bs = current_state.get("backend_snapshot") or {}
                    dash = bs.get("dashboard_signals") if isinstance(bs, dict) else None
                    payload = _json_safe({
                        "type":                        "tick",
                        "price":                       current_state["price"],
                        "window_start_price":          current_state["window_start_price"],
                        "window_start_time":           current_state["window_start_time"],
                        "prediction":                  current_state["prediction"],
                        "ensemble_prediction":         current_state.get("ensemble_prediction"),
                        "strategies":                  current_state["strategies"],
                        "deepseek_prediction":         _pred_for_ws(current_state.get("deepseek_prediction")),
                        "pending_deepseek_prediction": _pred_for_ws(current_state.get("pending_deepseek_prediction")),
                        "pending_deepseek_ready":      current_state.get("pending_deepseek_ready", False),
                        "agree_accuracy":              current_state.get("agree_accuracy"),
                        "specialist_completed_at":     current_state.get("specialist_completed_at"),
                        "bar_historical_analysis":     current_state.get("bar_historical_analysis", ""),
                        "bar_historical_context":      current_state.get("bar_historical_context", ""),
                        "bar_binance_expert":          current_state.get("bar_binance_expert", {}),
                        "bar_trend_analyst":           current_state.get("bar_trend_analyst", {}),
                        "service_unavailable":         current_state.get("service_unavailable", False),
                        "service_unavailable_reason":  current_state.get("service_unavailable_reason", ""),
                        "dashboard_signals":           dash,
                    })
                    await websocket.send_json(payload)
                except Exception as exc:
                    logger.warning("WS send failed: %r", exc)
                    break
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.api_host, port=config.api_port, loop="asyncio")

