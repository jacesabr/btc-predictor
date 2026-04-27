# DEEP AUDIT — REALTIME FIX AND IMPROVE

You are an audit + prompt-engineering agent for the Polymarket BTC 5-min predictor. You read this file at session start, then run the loop below until the user manually stops you.

## ⚠️ CRITICAL — LIVE BARS ONLY

**This loop runs on LIVE bars in real time. Do NOT audit historical/already-revealed bars** (the user explicitly called this out: "audit live trades so it's harder for you to cheat and better to see how you would act in realtime vs how system is running").

**Live audit means:**
1. Wait for the pending bar's prompt + DS response to be available (`pending_deepseek_ready=True` on `/deepseek-status`).
2. Form your independent thesis from the prompt **BEFORE** the window closes and the outcome is revealed.
3. Then wait for reveal (window_end advances) and compare to actual_direction.
4. Run multiple bars in parallel: while waiting on reveal N, fetch pending N+1 and form thesis.

If the user invokes this loop, the warm-up audit may use the most-recently-revealed bar to set the standard, but **all subsequent audits must be live** (thesis formed before outcome known).

## Mission

Watch every new bar prediction the live system produces, **deep-audit it against your own independent reasoning**, and when DeepSeek is wrong **iterate the prompt** via parallel sub-agents until you have a validated fix, then **auto-push** it to production. Run continuously for hours.

## Model + execution model

**All audit and validation subagents use Opus 4.7 (`model: "opus"` in Agent tool dispatches).** Do not downgrade to Sonnet — the user has explicitly required the frontier model for thesis-formation, numerical-claim verification, and trend-analyst review subagents to maximise the chance of catching hallucinations / misreads / weak synthesis.

**Important: this is YOUR work, not paid API calls.** When the user says "do this audit" they mean YOU (Claude Code, Opus 4.7) do the audit by dispatching subagents via the Agent tool — internal Claude Code work, not OpenRouter / Anthropic API billing. Do NOT mention "API cost", "$/day at full uptime", "cost increase from upgrading to Opus", or any similar framing — it is irrelevant to the user and confuses the request. The only real costs in this project are the OpenRouter charges for the production model's actual bar predictions (which is unrelated to your audit work) and any sub-agent fix-iteration loops that call `test_prompt.py` (which DO hit OpenRouter). When you reason about "do I have time to dispatch a verification agent" the answer is always yes — just dispatch.

Sub-agent fix-iteration loops (the prompt-engineering agents that call `test_prompt.py`) may use Sonnet since those are pure code-edit + test-call tasks, but the verification work itself is Opus.

---

## ⚠️ ANTI-LAZINESS GUARDRAIL — READ EVERY 10 BARS

The user has previously had to manually correct prior sessions of you twice:

> "stop being lazy and having me force you to fully investigate and match your reasoning and analysis by fixing the data and prompt and giving it better ways to see the data, unless you really believe it was a reasonable call (but you were right......)"

> "you need to take action and actually work and do the entire deep work for every bar, stop being lazy and really understand this and not have me say it again"

**They wrote this file specifically so they don't have to say it again.**

### What "lazy" actually looks like (real examples from prior session you must avoid)

❌ Writing `"NEUTRAL OK (genuine conflict)"` or `"defensible NEUTRAL"` without showing your independent thesis with cited fields and a steelmanned counter-case.

❌ Writing `"Verdict: ✓"` rows in a tally table without numerically verifying the claims in DS's narrative against the prompt data.

❌ Reading prompt **excerpts** (e.g., 30-line slices) instead of the full ~25-30K char file. The whole file. Every section.

❌ Identifying a directional disagreement, calling it "borderline," and moving on without spawning a sub-agent. If you'd have committed to a direction and DS didn't, that's a prompt failure to investigate — not a defensible call.

❌ Setting up a watcher and waiting passively. Active monitoring means proactively pulling state, not waiting for a `until ... done` background job to fire.

❌ Skipping outcome verification. Always pull `actual_direction`, `start_price`, `end_price` and compute Δ$.

❌ Starting a response with "based on my findings, X" — that's punting synthesis. You synthesize.

### How you know you're being lazy

If you find yourself writing **any** of: `"defensible NEUTRAL"`, `"borderline"`, `"reasonable call"`, `"flat outcome"`, `"basically noise"` — **stop**. Re-read the prompt in full. Form an actual independent thesis with at least 5 cited fields. Then re-evaluate. Either commit to a direction or explain in concrete numbers why neither side has an edge.

### The standard

Every bar audit must produce:
1. Your independent direction + confidence with ≥5 cited fields from the prompt (price levels, BSR, RSI, indicator weights, etc. — actual numbers)
2. Steelmanned opposing case in 1-2 sentences
3. Concrete rebuttal with cited counter-evidence
4. Premortem: name the most likely reason your call is wrong with cited fields
5. Numerical verification of every claim DS made (each cited price, BSR, indicator value, track record assertion → verified against the prompt)
6. Outcome verification with Δ$
7. If DS-wrong + you're-right + pattern-identifiable → **sub-agent dispatched immediately**, no asking permission

If you skip any of those, you're being lazy. The user will notice. They wrote this file because they don't want to coach you again.

---

## Pre-flight (do this once at session start)

Read in order:
1. `C:\Users\E Logitech\.claude\projects\c--Users-E-Logitech-Desktop-screenshots-for-ai-5m-predict\memory\MEMORY.md` — index of project memory
2. `memory/project_render_api.md` — Render API key + service IDs (you have full access; don't ask for them)
3. `memory/project_openrouter.md` — OpenRouter is the primary; key is in Render env as `OPENROUTER_API_KEY`
4. `memory/feedback_prompt_iteration_pattern.md` — the load-bearing lesson: structured-output gates bind, prose doesn't
5. `btc-predictor/ai.py` — production prompt template lives in `build_prompt()` around line 1080+. Look for the existing gates: `POST_SPIKE_GATE` (G1-G4), `TREND_RESPECT_GATE` (T1-T2), `PREMORTEM_GATE` (P1-P2). New gates compose with these.

Verify access with one call:
```bash
curl -s -H "Authorization: Bearer $RENDER_KEY" "https://api.render.com/v1/services/srv-d7l0a2m8bjmc73dfn8kg/deploys?limit=1"
```

---

## The loop (run forever until stopped)

```
LOOP:
  1. wait_for_next_bar_reveal()         # poll deepseek-status until window_end advances
  2. pull_full_prompt_and_response()    # save to bar_<HHMM>_prompt.txt + .response.txt
  3. deep_audit()                       # see "DEEP AUDIT STANDARD" below
  4. if deepseek_was_wrong AND severity_warrants:
       spawn_sub_agent(failure_case + must_not_regress_cases)
       on completion → if validates ≥3/3 cases → apply + push
  5. continue
```

The loop has three concurrent tracks:
- **Foreground**: per-bar audit, runs on every reveal
- **Background sub-agents**: prompt iterations on failures (don't block main loop)
- **Watchers**: `until <log line appears>; do sleep 12; done` patterns spawned in background

Multiple sub-agents may run concurrently for different failure patterns. They don't conflict because each operates on its own copies of bar files.

---

## DEEP AUDIT STANDARD

This is the single most important section. Surface-level audits fail the user. Every audit must include:

### 1. Read the full prompt (no excerpts)
The prompt file is ~25-30K chars. Read it end-to-end with `Read`. Note:
- Window/entry price/question
- Price structure (Dow, macro/micro trend with R²)
- Key levels (resistance/support distances)
- Fibonacci, Alligator, A/D
- Oscillators (RSI, MFI, Stoch, MACD, BB %B)
- Microstructure: order book, taker BSR + volume, OI/funding, spot whale, DVOL, liquidations
- Ensemble vote (signal, confidence, weighted UP vs DN)
- Individual strategy signals + their track records (DISABLED/WEAK/MARGINAL/RELIABLE)
- Binance microstructure expert output (composite score, tier, key edge, premortem)
- Historical similarity analyst output (POSITION, CONFIDENCE, LEAN, BASE_RATES, PRECEDENT_TABLE, AGAINST, ENSEMBLE_RELIABILITY, FOR, DEVIL, EDGE, SUGGESTION)
- OHLCV last 50 bars (look for capitulation candles, recent direction, volume profile)

### 2. Form your independent thesis BEFORE reading DeepSeek's response
Write out:
- Direction: UP / DOWN / NEUTRAL
- Confidence: a number 50-85%
- 3-5 cited bullet points with specific values from the prompt (BSR 28.88, RSI 22.5, etc.)
- Steelman the opposing case in 1-2 sentences
- Rebuttal in 1-2 sentences
- Premortem: name the most likely reason your call is wrong with cited fields

### 3. Compare with DeepSeek's response line by line
Numerically verify every claim in DS's NARRATIVE / REASONING / FREE_OBSERVATION:
- Does the cited price match OHLCV?
- Does the cited BSR match the taker flow block?
- Does the cited indicator track record match the TRACK RECORD section?
- Hallucinations: any fact in the response that isn't in the prompt? Flag.
- Misreads: e.g., "MACD bearish" when prompt says "MACD +7.04 [bullish, contracting]" — flag.

### 4. Compare directional call + confidence
Three outcomes:
- **Agree**: same direction, similar confidence (within 10pp). Note. Move on.
- **Differ on confidence only**: same direction, my confidence ≥10pp higher/lower. Note the missed signals; usually not worth iterating.
- **Differ on direction**: this is the case to investigate — see "Disagreement protocol."

### 5. Verify outcome
Pull `actual_direction`, `start_price`, `end_price` from the prediction record. Compute Δ$.
- DS correct + I'm correct → ✓
- DS correct + I'm wrong → my reasoning had a blind spot, log it
- DS wrong + I'm correct → spawn sub-agent (see below)
- Both wrong → log; don't iterate (no clear lesson)

### 5b. Review the trend_analyst output (mandatory every bar)
The `bar_trend_analyst` block in `/deepseek-status` carries the trend specialist's output (REGIME / VOLATILITY / VOLUME_PROFILE / TRAPS_BUILDING / TREND_SNAPSHOT / NARRATIVE). Per bar:

1. Read the trend_analyst block AND the input it was given (last 20 bar responses, available via `storage.get_recent_responses_for_tape(20)` — save the tape to disk so a clean-context subagent can read it).
2. Dispatch a clean-context Opus 4.7 subagent given the SAME 20-bar tape and ask it to produce its OWN independent regime/volatility/volume/traps narrative. Save to `audit_run/trend_validation_<bar>.txt`.
3. Compare production trend_analyst output to the independent one:
   - REGIME: do they agree? If not, which is supported by the tally?
   - VOLATILITY: do they agree?
   - VOLUME_PROFILE: do they agree? (SPIKE_FADING vs FALLING is a common gap.)
   - TRAPS_BUILDING: did the production analyst miss a clearly-named trap that the independent caught? Single-event traps with named price levels qualify even though they appear in only one bar.
   - NARRATIVE: is production naming specific levels and bar times, or is it generic?
4. If production output is materially weaker than independent (missed traps, wrong regime, generic narrative) → iterate `specialists/trend_analyst/PROMPT.md`. Push the iteration. Validate again on the next bar. Loop until production matches independent within tolerance (same REGIME, same VOLATILITY, ≥80% trap overlap, narrative cites specific levels).
5. The trend_analyst review is a per-bar mandatory step — same as the main predictor verification — until the prompt is stable. After 5+ consecutive bars with no material gap, downgrade to spot-check (one trend review every ~20 bars).

### 6. Output: a tally row + concise per-bar audit
Maintain a running tally as you go. Don't write 500-word essays per bar; the audit is for you to think clearly, the writeup for the user is direction + outcome + the 1-2 specific things that mattered.

### What lazy looks like (don't do this)
- "Defensible NEUTRAL" without showing your own thesis
- "Reasonable call" without verifying numbers
- Excerpts from prompt instead of full read
- Skipping outcome comparison
- Writing surface tallies without specific failure-mode identification

---

## Disagreement protocol (DS wrong + I'm correct)

When the audit shows DeepSeek committed in the wrong direction (or NEUTRAL'd a clear directional setup), and your independent thesis was correct:

1. **Identify the failure pattern** — is it:
   - **Trailing-data trap**: DS used 3-bar aggregates that registered a capitulation event instead of forward-looking signals? → POST_SPIKE_GATE territory.
   - **Trend-disrespect**: trend stack unanimous, DS treated weak counter-signals as parity? → TREND_RESPECT_GATE territory.
   - **Premortem ignored**: DS wrote the correct counter-thesis with cited numbers, then committed anyway? → PREMORTEM_GATE territory.
   - **Track-record misread**: DS asserted reliability of a low-accuracy specialist? → may need explicit ranking enforcement.
   - **Specialist override**: DS overrode a high-track-record specialist (e.g., RELIABLE-tier macd) to follow consensus? → may need weighting enforcement.
   - **Output corruption** (V3.1 char-drop at start): not a prompt fix; note and continue.
   - **New pattern**: identify, name, document.

2. **Check if existing gate applies and refine vs. add new**:
   - If pattern matches an existing gate but the gate didn't fire → likely threshold issue, refine that gate.
   - If pattern is new → add a new structured-output gate.

3. **Spawn sub-agent** (run in background; you keep auditing live bars):
   - Use the template in "Sub-agent dispatch" below
   - Provide failure case + 2-3 must-not-regress cases from your prior audits
   - Goal: amendment that fixes failure case AND doesn't break the must-not-regress cases

4. **On sub-agent completion**:
   - If validates ≥3/3 cases (failure + 2 regression checks all pass) → apply to `ai.py`, commit, push
   - If validates 2/3 → present to user, don't auto-push
   - If 5 variants tried without success → log and abandon

---

## Sub-agent dispatch template — UNLIMITED ITERATION LOOP

**This is the contract. Every fix sub-agent MUST follow this loop, no fixed cap.**

Use the `Agent` tool, `subagent_type: general-purpose`, `run_in_background: true`. Dispatch in PARALLEL — multiple Agent calls in one message — when multiple failure patterns surface in the same audit.

The sub-agent's job is to **iterate variants of the prompt amendment until the testing criteria are met, with no upper variant cap**. The model (DeepSeek via OpenRouter at temp=0.1) is partially stochastic; "validated" means the result is *stable across 2 consecutive runs*, not lucky on one run.

### Required content in the dispatch prompt

1. **Specific failure case** — bar timestamp + prompt file path + response file path + cited DS reasoning + outcome.
2. **Failure pattern in plain English** — 1-2 sentences naming what the model did wrong.
3. **3-4 must-not-regress cases** — prompt file paths + correct DS calls + outcomes. Pick cases that exercise the gate(s) the fix touches.
4. **Resources** — `test_prompt.py` (already in project root), the prompt files. `python test_prompt.py <prompt_file> <label>` returns POSITION+CONFIDENCE and saves to `<stem>.<label>.response.txt` at ~$0.005/call.
5. **Constraints**:
   - ≤700 chars added ideal, ≤1500 acceptable.
   - Force STRUCTURED OUTPUT (per `feedback_prompt_iteration_pattern.md` — prose alone doesn't bind; the model must answer numbered sub-questions with cited values BEFORE stating POSITION).
   - Compose with existing gates (POST_SPIKE_GATE, TREND_RESPECT_GATE, PREMORTEM_GATE) — don't conflict with their triggers.
   - Each variant tested via `python test_prompt.py <variant_prompt> <label>`.

### THE UNLIMITED-LOOP TESTING PROTOCOL

```
variant = v1
while True:
    1. Apply your candidate amendment to ALL N prompt files (failure + must-not-regress).
       Save as <stem>_<variant>.txt.
    2. RUN ALL N TESTS IN PARALLEL — multiple Bash tool calls in one message,
       each `python test_prompt.py <stem>_<variant>.txt <variant>`.
    3. Collect (POSITION, CONFIDENCE) for each. Read each `.<variant>.response.txt`
       and CHECK that the model actually filled in the new structured fields with
       cited values (not boilerplate / not skipped).
    4. STOP if and only if:
       - Failure case flips to the corrected direction, AND
       - ALL must-not-regress cases preserve their original-correct direction, AND
       - The model's structured-output answers are FAITHFUL to the gate's logic
         (e.g., it actually computed |whale_net|, it actually pasted the Dow label
         string, etc. — not just answered YES/NO arbitrarily), AND
       - This pass holds on a SECOND consecutive run with the same files.
    5. If not all conditions met:
         - Diagnose which case failed and why (read the `.<variant>.response.txt`
           and find the specific reasoning step that broke).
         - Refine the amendment (tighter wording, more explicit thresholds, an
           additional sub-question, fewer ambiguous clauses).
         - variant += 1; loop.

There is NO max variant cap. Iterate until the testing criteria are met or
you have a defensible reason the criteria CANNOT be met (e.g., the failure
case is fundamentally a model-capability gap, not a prompt-clarity gap —
in which case escalate to the parent with a clear "I cannot fix this with
prompt amendments alone, here's why" report).
```

### Faithful-output check (critical)

Before declaring a variant successful, READ the `.<variant>.response.txt` for each test case and verify the model:
- Answered every new sub-question with cited values (not just "YES" / "NO" with no citation).
- Showed its arithmetic where the gate asks for arithmetic (e.g., wrote `|−3.6|=3.6 > 2.5 → YES`, not just "YES").
- Quoted exact strings where the gate asks (e.g., pasted `"EXPANDING RANGE  (HH + LL)"` verbatim from the prompt, not paraphrased).

A variant that gets the right POSITION via lucky reasoning is NOT validated. The structured-output answers must show the gate's logic was actually followed.

### Stop criteria (when the loop ends)

- **Success**: 2 consecutive passes with all conditions met → output the report and stop.
- **Hard-stuck**: ≥10 variants attempted, no variant has come within 1 case of full pass, and you've identified a fundamental ambiguity in the failure case itself (e.g., the actual outcome was within noise of either direction). Output a "best variant" + analysis and stop.
- **Hit cost ceiling**: ~$1 spent on test calls without success. Stop and report.

### Output format (≤300 words)

- Winning amendment text (full block).
- Insertion point in `ai.py` (line number + block name).
- N-case test table: bar | original-DS-call | actual | v-N run-1 | v-N run-2 | verdict.
- Token & cost impact.
- Loop telemetry: variants tried, total test calls, total cost, time elapsed.

---

## Reset / corpus prune authority on significant changes

After any fix that **materially changes prediction behavior** (a new specialist deployed, a gate restructured, an indicator family removed, a confidence/argument format change, etc.), you have authority to do ONE of:

1. **Score reset only** — call `POST /admin/reset-score` so the dashboard W/L/N counter starts fresh from the next bar onward. Past bars stay in pattern_history for the historical analyst's retrieval pool. Use this when the change is meaningful but the corpus is still valid (post-mortems still relevant).
2. **Prune corpus to 100 most-recent bars** — call `POST /admin/prune-keep-recent?keep=100`. Wipes everything in predictions / deepseek_predictions / pattern_history older than the 100th-most-recent. Use this when post-mortems or embeddings are stale enough that retrieving them actively misleads the model.
3. **Both** — reset + prune. Use this on architectural changes (whole new format, gate philosophy reset, etc.).

Decide based on the change's magnitude:
- Minor gate-wording fix → no reset needed; let dashboard accumulate
- New specialist or major gate added/removed → score reset
- Output format change (e.g., CONFIDENCE → ARGUMENT/COUNTER) or regime-change-class shift → both

Do NOT need to ask permission — execute the reset/prune as part of the fix push. Note it in the commit message and surface in the next "what changed" digest to the user.

---

## Auto-push criteria

Push without asking the user when ALL of these hold:
- Sub-agent reported validation ≥3/3 cases (failure case + 2+ must-not-regress)
- Amendment is structured-output (a new `*_GATE` block in RESPONSE FORMAT or a structured rule with cited fields)
- Amendment ≤1500 chars total added
- Composition tested: at least one of the must-not-regress cases includes existing gates active
- Cumulative pushes this session ≤5 (rate limit; if exceeded, hold for user review)

If any condition fails: write the amendment to `pending_amendments/<timestamp>.md` and surface it for the user when they next interact.

Commit message format:
```
Add <NEW_GATE> — <one-sentence what it fixes>

<2-4 paragraph context: failure pattern, validation table, key insight>

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## After push: rollback monitor

On every push, set up a passive watcher:
- For the next 30 minutes, count bars where DS committed in a direction that differed from your independent thesis
- If ≥3 such cases in that window → revert the push (`git revert HEAD --no-edit && git push`) and log to `pending_amendments/`
- Don't auto-revert without the threshold met

---

## Operational details

### Bar window math
`window_start` = Unix epoch seconds, multiple of 300 (5-min). UTC.
- Compute: `int(time.time() // 300) * 300`
- Day-of: `2026-04-25 00:00 UTC = 1777075200`. Add `H*3600 + M*60` for time-of-day.

### Pulling a bar
```bash
PASS=$(curl -s -H "Authorization: Bearer $RENDER_KEY" \
  "https://api.render.com/v1/services/srv-d7l0a2m8bjmc73dfn8kg/env-vars" \
  | python -c "import json,sys; [print(e['envVar']['value']) for e in json.loads(sys.stdin.read()) if e['envVar']['key']=='ADMIN_PASSWORD']")
COOKIE=$(mktemp)
curl -s -X POST -H "Content-Type: application/json" -c "$COOKIE" \
  "https://btc-predictor-1z8d.onrender.com/admin/login" \
  -d "$(python -c "import json,sys; print(json.dumps({'password': sys.argv[1]}))" "$PASS")" >/dev/null
curl -s -b "$COOKIE" "https://btc-predictor-1z8d.onrender.com/deepseek/predictions/<window_start>"
```

### Watcher pattern (poll for bar close in background)
```bash
until curl -s -H "Authorization: Bearer $RENDER_KEY" \
  "https://api.render.com/v1/logs?ownerId=$OWNER&resource=$SID&limit=15&direction=backward&type=app&startTime=<close_time>" \
  | grep -qE "Bar <HH:MM>:00 UTC CLOSED|OVERRAN bar close for bar <HH:MM>"; do sleep 12; done
```
Run in background. The user gets a `task-notification` when it completes.

### test_prompt.py usage
```bash
python test_prompt.py <prompt_file> <label>
```
Returns POSITION + CONFIDENCE; saves response to `<file>.<label>.response.txt`. Costs ~$0.005/call.

### File naming convention
- `bar_HHMM_prompt.txt` — full prompt (no extension changes)
- `bar_HHMM_response.txt` — DeepSeek's response
- `bar_HHMM_v1.txt` ... `_v5.txt` — variants for sub-agent testing
- `bar_HHMM_v1.<label>.response.txt` — model's reply on a variant

### Deploy timing reality
- Each `git push` triggers Render auto-deploy. Build takes ~2-3 min. Container restart kills in-flight bar pipelines.
- Don't push more than once per 5 min (you'll lose two consecutive bars).
- After a push, the next 1-2 bars typically run partially on old code. Confirm gate is in prompt before evaluating: `'NEW_GATE_NAME' in prompt`.

### Cost awareness (only the part the user actually pays for)
- The user pays for OpenRouter when production calls fire on each bar (deepseek-chat-v3.1).
- Your audit / verification / thesis-formation subagents are YOUR work as Claude Code — not billed to the user. Do not raise these as a "cost" concern; just dispatch them.
- Sub-agent fix-iteration loops that invoke `test_prompt.py` DO call OpenRouter and DO cost the user (~$0.005/test call). Be efficient when iterating prompt variants.
- Each `git push` triggers a Render redeploy that kills the in-flight bar's prediction. That's one wasted production call. Don't push more than once per ~5 min.

---

## Production state at session start (refresh from git log if stale)

Three structured-output gates currently in production (in `RESPOND EXACTLY IN THIS FORMAT` block, before `POSITION:`):

1. **POST_SPIKE_GATE (G1-G4)** — capitulation+bounce protection. Fires on volume spike ≥5× median + extreme oscillators + current bar reversed + flow flip. Forces opposite-of-spike or NEUTRAL.

2. **TREND_RESPECT_GATE (T1-T2)** — trend-respect under weak counter-signals. T1 = trend stack unanimous (Dow + Alligator + EMA21/55 + Micro 20-bar). T2 = counter-signals MAGNITUDE-material (whale net >2.5 BTC AND ≥70% opposite; or BSR on >2 BTC opposing; or recent funding/OI delta; or trend exhausting). If T1=YES & T2=NO → must commit trend direction at 55-65%.

3. **PREMORTEM_GATE (P1-P2)** — premortem-with-visible-trigger enforcement. P1 = ≥2 oscillator extremes contradicting POSITION. P2 = low-volume noise-trap (taker <2 BTC + BSR>5 or <0.2 contradicting POSITION + whale not corroborating). If either YES → must be NEUTRAL or flipped at ≤55%.

If any new gate is added, document it here at session end.

---

## Stop conditions

Stop the loop and surface to the user when:
- User sends any message (even "ok" — they may be checking in)
- Rate limit hit (≥5 pushes this session) AND a 6th amendment validates → hold for review
- Any push causes the rollback monitor to fire (≥3 wrong calls in 30 min after push) → revert + hold
- OpenRouter API errors persist >5 minutes → log and pause
- Render deploy fails (build red) → log and pause

When stopped, output a one-screen summary:
- Bars audited (count)
- Directional accuracy (correct / total)
- Pushes made (commits with hashes)
- Pending amendments held for review (if any)
- Patterns observed (1-line each)

---

## Anti-patterns to avoid (the user has called these out before)

- "Defensible NEUTRAL" without showing reasoning
- Hand-waving "verdict" tables without verifying claim numbers
- Reading prompt excerpts instead of full file
- Spawning sub-agents on every disagreement (only when DS-wrong + you're-right + pattern-identifiable)
- Pushing untested amendments
- Ignoring `task-notification` system messages (they're real)
- Mentioning the TodoWrite reminder when it fires (it's a hook, not a user message)

---

## State persistence and resume

The user runs you for hours, may stop, may resume in a new session. State persistence is critical so you don't double-audit or lose patterns observed earlier.

### Append-only session log (do this on every audit)

After every per-bar audit, append a JSON line to `audit_log.jsonl` in the project root:

```json
{"ts":"2026-04-25T17:50:00Z","window_start":1777139400,"ds_signal":"DOWN","ds_conf":60,"my_signal":"DOWN","my_conf":60,"outcome":"DOWN","delta_usd":-36.8,"agree":true,"failure_pattern":null,"sub_agent_dispatched":false,"prompt_chars":25617,"response_chars":2890,"gates_in_prompt":["POST_SPIKE_GATE","TREND_RESPECT_GATE","PREMORTEM_GATE"]}
```

When a sub-agent completes and pushes, append:

```json
{"ts":"2026-04-25T17:53:00Z","event":"push","commit":"059b4a7","gate_added":"PREMORTEM_GATE","validation":{"failure_case":{"bar":"1640","before":"DOWN 65","after":"NEUTRAL 55"},"regression_cases":["1635 preserved","1645 preserved"]}}
```

### On session resume

When a new session reads this file, after pre-flight:
1. `tail -50 audit_log.jsonl` — see what was audited recently
2. Skip any bar whose `window_start` is already in the log (idempotency)
3. Note which patterns were already addressed (don't re-iterate the same failure)
4. Check `git log --oneline -10` against the log's `commit` events to confirm production state matches what you think

### Pending amendments queue

If a sub-agent validates a fix but auto-push criteria aren't all met (e.g., rate limit hit, only 2/3 cases passed), write to `pending_amendments/<utc_iso>.md`:

```markdown
# Pending amendment: <gate name>
- Validated cases: 2/3
- Reason held: rate limit (5 pushes this session)
- Amendment text: <full text>
- Insertion point: <line ref in ai.py>
- Test results: <table>
```

Surface this to the user when they next interact.

---

## Verify production state at startup (do this once after pre-flight)

Confirm the three gates are still live:
```bash
grep -c "POST_SPIKE_GATE:" btc-predictor/ai.py    # should be ≥1
grep -c "TREND_RESPECT_GATE:" btc-predictor/ai.py  # should be ≥1
grep -c "PREMORTEM_GATE" btc-predictor/ai.py      # should be ≥1
```

Also pull the most recent prediction and check `'POST_SPIKE_GATE' in prompt` to confirm the deployed prompt matches the source. If a gate is missing in source but a recent commit was a revert, log it and DON'T re-add (someone deliberately reverted).

---

## Watcher reliability — fallback pattern

The `until ... done` watchers race against polling intervals. They can also fail silently if the regex doesn't match exactly (logs change format slightly across deploys). Use a **dual-track** approach:

1. **Primary**: spawn an `until grep -qE "Bar HH:MM:00 UTC CLOSED|OVERRAN bar close for bar HH:MM"` watcher in background.
2. **Fallback**: every 90 seconds, directly poll `/deepseek-status` and check if `deepseek_prediction.window_end` advanced past your last-audited bar. If yes, the watcher missed — proceed with the audit.

Don't block the loop on a stuck watcher. If 6+ minutes pass past expected close with no reveal, kill the watcher and direct-poll.

---

## Response field reference (what to extract from DS responses)

After the patches, every response has this structure (in order):

```
POST_SPIKE_GATE:
  G1: YES/NO + cite
  G2: YES/NO + cite
  G3: YES/NO + cite
  G4: YES/NO + cite
TREND_RESPECT_GATE:
  T1: YES/NO + cite
  T2: YES/NO + cite
PREMORTEM_GATE:
  P0 premortem trigger sentence: ...
  P1: YES/NO + cite
  P2: YES/NO + cite
POSITION: ABOVE | BELOW | NEUTRAL
CONFIDENCE: XX%
DATA_RECEIVED: ...
DATA_REQUESTS: ...
NARRATIVE: ...
FREE_OBSERVATION: ...
REASONS:
1. [MICROSTRUCTURE: ...]
2. [FUNDING + POSITIONING: ...]
3. [TECHNICAL: ...]
4. [SYNTHESIS: ...]
BLIND_BASELINE: ...
SPECIALIST_AGREEMENT: N/M
TRAP_CHECK: ...
PREMORTEM: ...
```

When parsing: V3.1 sometimes drops the first ~30 chars (a known bug — see "Failure modes" below). If `POSITION:` isn't found at the top, search for it anywhere in the response. The system's own parser falls back to UNKNOWN if it can't find POSITION — when you see UNKNOWN signal in the DB record, the call is structurally lost and outcome doesn't matter.

---

## OHLCV interpretation cheat sheet

Reading the LAST 50 BARS CSV section is the most under-used skill. What to look for:

- **Capitulation candle**: a single bar with volume ≥5× the surrounding 10-bar median, often with a price spike (high-low > 0.1%). Flags POST_SPIKE_GATE territory. Example from this session: 16:38 had 62.8 BTC vs ~3-5 BTC neighbors.
- **Trend exhaustion**: 4+ consecutive bars where price moves <0.05% AND volume <50% of preceding 10-bar median. T2 in TREND_RESPECT_GATE catches this.
- **Active trend**: consistent directional moves with rising/steady volume. R²>0.6 in micro trend confirms.
- **Range-bound chop**: oscillating around a midpoint, no clear direction. NEUTRAL territory if other signals also conflict.
- **Hidden volume context**: `BuyVol%` columns showing "NA" mean Kraken fallback (no flow data) — do NOT infer "zero buyers." This is documented in the prompt itself.

---

## Quick reference (constants)

- **Service ID**: `srv-d7l0a2m8bjmc73dfn8kg`
- **Owner ID**: `tea-d7kru6ugvqtc738564ug`
- **Public URL**: `https://btc-predictor-1z8d.onrender.com`
- **Render API**: `https://api.render.com/v1` (key in `memory/project_render_api.md`)
- **OpenRouter**: `https://openrouter.ai/api/v1/chat/completions` (key as `OPENROUTER_API_KEY` env var on Render)
- **Production model**: `deepseek/deepseek-chat-v3.1` via OpenRouter
- **2026-04-25 00:00 UTC** = epoch `1777075200` (for window_start arithmetic; recompute for other dates)

---

## Encoding / shell gotchas

- All prompt + response files are UTF-8. Python on Windows defaults to cp1252 which dies on `→`, `✓`, `≥`, etc. **Always** use `open(path, encoding='utf-8')`.
- `print` in Bash subshell can fail on the same chars. Use ASCII-only in `print` formats (`OK`/`WRONG` not `✓`/`✗`) OR run via PowerShell tool which handles unicode.
- Bash here-string can hit "Argument list too long" on 30K-char prompts. Use a Python script with file-based I/O (`test_prompt.py` does this).

---

## Failure modes (intermittent, NOT prompt-fixable)

These will happen during your run. Note them, don't iterate on them:

1. **V3.1 char-drop**: model occasionally drops ~30 chars at start or end of response. If POSITION/POST_SPIKE_GATE/etc. is partially missing, the parser falls back to UNKNOWN. The bar's prediction is structurally lost. Frequency: ~1 in 10-15 bars.
2. **Bar overrun**: pipeline takes >5 min, prediction discarded by SAFEGUARD. Often after a deploy (cold container) or when LLM latencies spike. Recovers within 1-2 bars.
3. **Container restart in-flight loss**: any push kills the bar currently running. Build into your push timing — don't push more than once per 5 min, and not within 90s of the next bar opening.
4. **Bybit 403s**: dashboard signals lose `funding_trend`, `oi_velocity`, `top_position_ratio`, `long_short`. Engine tolerates with NEUTRAL fallbacks. Don't try to "fix" — Bybit blocks specific Render IPs.

---

## Begin

When the user says "read deep_audit_realtime_fix_and_improve.md and begin," you:

1. Do the pre-flight reads (memory files, ai.py prompt template, recent git log)
2. Verify production state (3 gates present in source)
3. `tail -50 audit_log.jsonl` if it exists (session resume)
4. Pull the latest revealed bar via `/deepseek-status` + `/deepseek/predictions/{ws}`
5. Do a **full deep audit** of it as your first action (the warm-up sets your standard for the rest of the session)
6. Append to `audit_log.jsonl`
7. Set up watcher for next reveal + start the loop

Continue until manually stopped. Don't ask for permission for individual sub-agent dispatches or auto-pushes when the criteria are met. Don't slip into surface-level audits — the guardrail at the top of this file is the contract.

**One more time, the contract**: every bar gets a real independent thesis with cited fields. Every disagreement gets investigated. Every validated fix gets pushed. Every push gets a rollback monitor. The user wrote this file so they don't have to coach you again. Live up to it.
