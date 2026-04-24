# Live Monitor — Post-Deploy Audit Guide

Scan this while a 5-minute bar rolls through. Each section maps to a concrete check you can run against `/deepseek-status`, the rendered briefing, or grepping source.

---

## 1. Bug-fix verification

| Fix | What to check | Where |
|---|---|---|
| **OKX liquidations 100× scale** | `lq.long_liq_usd` / `lq.short_liq_usd` reasonable (not billions in calm market); velocity_per_min < 50 baseline | `/deepseek-status → backend_snapshot → dashboard_signals → liquidations` |
| **"smart money" language removed** | grep briefings/prompts — language should say "top traders" / "notional-weighted", not "smart money" | `grep -i "smart money" ai.py signals.py` → should be 0 |
| **Bybit relabel** | L/S fallback now labels itself "Bybit fallback" in the interp string | `signals.py:_fetch_long_short` |
| **10 BTC → 100s BTC bid wall** | order book aggregates top-20 at 0.5% band; wall values in hundreds of BTC, not single-level tens | `ds.order_book.bid_vol_btc` and `ask_vol_btc` |
| **Taker-flow UNAVAILABLE leak** | prompt no longer shows `BSR=1.000, 0 BTC` when fetch failed; instead `[TAKER FLOW] unavailable` | `full_prompt` of latest `deepseek_predictions` row |
| **Flashing briefing** | no re-render on every 500ms timer tick; same-content WS updates suppressed via JSON equality | DevTools → React profiler should show no Bullet remounts |

---

## 2. New signals to verify are populating

Post Phase 2.5 / 6.5 rollout, these MUST appear in every prompt block + `backendSnap.snapshot.dashboard_signals`:

- `basis` / `basis_pct` — spot-perp premium
- `cvd` — cumulative volume delta (perp 1h + spot 1h + aggregate 1h)
- `aggregate_oi` — cross-exchange open interest
- `aggregate_liquidations` — cascade across Binance/OKX/Bybit/Deribit/Kraken
- `deribit_skew_term` — 30d ATM IV + 25-delta risk reversal
- `bid_depth_05pct` / `ask_depth_05pct` — depth within ±0.5% of mid, aggregated

**Check**: `curl /deepseek-status | jq '.backend_snapshot.snapshot.dashboard_signals | keys'` — each should be present.

---

## 3. Expected DeepSeek prompt structure

Blocks must appear in this order, each with a scope tag:

1. **Window header** — timestamp, entry price, question
2. **PRICE STRUCTURE** (last N bars) — Dow theory, range, position, macro/micro slope
3. **DASHBOARD SIGNALS (microstructure)** — each section scope-tagged:
   - `[ORDER BOOK DEPTH — Kraken spot, top-20 levels]` (with fallback disclosure)
   - `[LONG / SHORT RATIO — Binance Futures 5m]`
   - `[TAKER AGGRESSOR FLOW — Binance Futures 5m, last 3 bars]`
   - `[LIQUIDATIONS — aggregate, last 5 min]`
   - `[OI + FUNDING — Binance perpetual]`
   - `[COINALYZE — cross-exchange aggregate]`
   - `[DERIBIT DVOL / SKEW]`
   - `[FEAR & GREED — daily]` ← MACRO, tagged as daily
   - `[MEMPOOL — on-chain fee pressure]`
4. **STRATEGY VOTES** — RSI, MACD, EMA, Supertrend, Alligator, etc.
5. **HISTORICAL SIMILARITY ANALYST** — top-K similar bars with outcomes
6. **BINANCE MICROSTRUCTURE EXPERT** — synthesized directional call + confidence tier
7. **INSTRUCTIONS** — steelman, NEUTRAL-is-a-cost, output format

Each microstructure block must carry a **SCOPE TAG** distinguishing bar-level (`5m`) from macro (`daily`).

---

## 4. Venice trader-summary audit

For each bar's briefing, verify:

- **No cross-scope comparisons** — e.g. "aggregate book depth beats single-venue 5m taker flow" is a category mistake (aggregate multi-venue vs single-venue bar-level). Venice prompt forbids this explicitly; enforce on audit.
- **No macro signals in watch/actions** — daily F&G / SOPR / MVRV may only appear in `edge` as background. If a `conditions[].metric` references a daily metric, that's a violation.
- **Every numeric threshold in text has a matching condition** — "above $X" / "below N BTC" must produce a `conditions` entry.
- **All `conditions[].metric` values in whitelist**:
  ```
  price, price_change_pct,
  taker_buy_volume, taker_sell_volume, taker_volume, taker_ratio,
  bid_imbalance, ask_imbalance,
  funding_rate, open_interest, rsi, long_short_ratio,
  basis_pct, perp_cvd_1h, spot_cvd_1h, aggregate_cvd_1h,
  bid_depth_05pct, ask_depth_05pct,
  rr_25d_30d, iv_30d_atm
  ```
- **`if_met` present** on every bullet that has `conditions`.
- **No fabricated terminology** — "Wyckoff / accumulation phase / order block / liquidity grab" only allowed if backed by a named price level or measured condition.

---

## 5. Banned strings — regression grep

If ANY of these appear in a new DeepSeek prompt or Venice briefing, it's a regression:

| Banned string | Why it's banned |
|---|---|
| `smart money` | renamed to "top traders" (2026-04 audit) |
| `BSR=1.000, 0 BTC aggressor volume` | taker-flow UNAVAILABLE leak via `.get(key, default)` — fixed `45b4c00`, `3bd1178` |
| `zero-taker-flow regime` | historical-analyst hallucination from stale pattern_history embeddings |
| `Polymarket crowd` / `polymarket_prob` | removed as prediction input `257131a` |
| `ACCUMULATION PHASE` / `WYCKOFF` with no price level | no-jargon-without-evidence rule |
| `cross 60% win rate` | unjustified empirical claim, stripped `3bd1178` |
| `I lean UP but only at 58% — better to abstain` | NEUTRAL-is-a-cost rule |

---

## 6. Cross-check URLs — verify any number manually

| Metric class | Live dashboard |
|---|---|
| Price | https://www.binance.com/en/trade/BTC_USDT |
| Taker buy/sell volume, BSR | https://www.coinglass.com/BitcoinTakerBuySellVolume |
| Funding rate | https://www.coinglass.com/FundingRate |
| Open interest | https://www.coinglass.com/BitcoinOpenInterest |
| Long/Short ratio | https://www.coinglass.com/LongShortRatio |
| Liquidations cascade | https://www.coinglass.com/BitcoinLiquidations |
| IV / skew / DVOL | https://metrics.deribit.com |
| Order book depth | https://www.binance.com/en/trade/BTC_USDT (Depth view) |
| Fear & Greed | https://alternative.me/crypto/fear-and-greed-index |

---

## 7. Troubleshooting table

| Symptom | Likely cause | Fix |
|---|---|---|
| Page blank, `ReferenceError` in console | undefined var in IIFE scope (e.g. `data is not defined`) | grep for the unresolved name, replace with an in-scope path (`backendSnap?.snapshot?.X`) |
| Briefing says "zero taker flow" but pill reads 200+ BTC | backend fetch failing on Render geo/rate, frontend Binance-direct still works | wait for pattern_history to age out post `45b4c00`, OR reprocess old embeddings |
| All bullets yellow / nothing fires | conditions' thresholds too tight, OR Venice echoing a broken backend "0 BTC" baseline | check one bar's `full_prompt`: if `BSR=1.000, 0 BTC` still appears, backend UNAVAILABLE leak re-regressed |
| Condition pill shows `now —` (greyed) | metric name in Venice output not in frontend `metric()` lookup | add to `METRIC_META` + `metric()` switch in `app.jsx` |
| `/deepseek-status` returns 200 but summary null | DeepSeek + Venice still running, normal for 30-60s post bar-open | wait |
| DeepSeek card flashes / unmounts every second | `activeDeepseekPred` ref instability from 500ms timer ticks | ensure `useMemo` dep list on activeDeepseekPred; check JSON equality on traderSummary setState |
| `ACTIONABLE · 0` when DeepSeek said UP | conditions all evaluated to null (no live data) → bullet stayed pending | verify the metric's source feed (client-side fetch) is succeeding (check `dot('tk','live')`) |

---

## 8. First-clean-bar calibration

After the first bar where Venice emits clean conditions matching real live values:

- **Record the typical ranges** for a calm 5-min bar:
  - taker_volume: ___ BTC total (buy + sell)
  - bid_imbalance: ___ %
  - ask_imbalance: ___ %
  - funding_rate: ___ % per 8h
  - long_short_ratio: ___
- **Note any threshold Venice emits that's trivially always-met** (e.g. "> 0 BTC"). If this happens every bar, the upstream default is still leaking somewhere.
- **Note any threshold that's trivially never-met** (e.g. "> 100 BTC" when max is 5 BTC). Indicates Venice misreading the scope tag.
- **Note the price-delta threshold** where the briefing transitions NEUTRAL → directional. That's a useful signal-sensitivity baseline.

---

## Current prompts (reference)

### DeepSeek main prompt
See `ai.py` → `build_prompt()` (approx lines 930-1200). Key framing:
> *"PAYOFF: Correct UP/DOWN = +1. Wrong UP/DOWN = −1. NEUTRAL = 0. NEUTRAL is not a free win — it is a deliberate choice to preserve capital when the data genuinely does not support either side. Before you commit, steelman the opposing direction…"*

### Venice trader-summary prompt
See `trader_summary.py` → `SYSTEM_PROMPT` (lines 34-96). Key sections:
- Output schema (edge / watch / actions, each with conditions + if_met)
- Hard rules (restate only INPUT facts, <=2 sentences, tone tagging)
- Scope-matching (no cross-scope comparisons, no macro in watch/actions)
- No jargon without evidence
- Conditions whitelist + operators

---

*Generated for post-deploy monitoring of commit ed12a71+ (trader briefing + action chips).*
