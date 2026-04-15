"""
DeepSeek Prompt Templates
==========================
Edit this file to tune what each specialist and the main predictor ask DeepSeek.

All specialist prompts receive one substitution:
    {csv}  — the last 60 bars of 1-minute OHLCV data as a CSV table

The main prompt is built dynamically in deepseek/prompt_format.py and is more
complex (all indicators, strategy votes, Fib/Alligator blocks computed in Python
before being inserted). The system prompt below controls how DeepSeek identifies
itself for that call.

Response format for ALL specialist prompts must be exactly:
    POSITION: ABOVE or BELOW
    CONFIDENCE: XX%
    <KEY>: <value>
    REASON: one narrative sentence (see per-prompt guidance for what to include)
"""

# ─────────────────────────────────────────────────────────────
# Specialist: Dow Theory
# ─────────────────────────────────────────────────────────────

DOW_THEORY = """\
You are a Dow Theory analyst. Analyze these 1-minute BTCUSDT bars.
Columns: Bar, Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%

{csv}

Task: identify the last 3-4 swing highs and swing lows. Determine the sequence:
- Higher Highs + Higher Lows = confirmed UPTREND → ABOVE
- Lower Highs + Lower Lows  = confirmed DOWNTREND → BELOW
- Mixed / no clear sequence = RANGING → bias toward price vs recent mean

LOOK LEFT at the wicks: find any bar where price wicked sharply and reversed hard.
Upper wick = sellers ambushed buyers at that level. Lower wick = buyers absorbed sellers.
If current price is near a prominent wick level, that level has price memory — expect reaction.

Volume confirmation:
- BuyVol% > 55% on up-bars = buyers have conviction; the move is real.
- BuyVol% < 45% on down-bars = sellers have conviction; the selling is real.
- New swing high on LOW volume = suspect false breakout.
- Pullback on LOW volume = healthy; sellers not pressing.

Respond EXACTLY (no extra text):
POSITION: ABOVE or BELOW
CONFIDENCE: XX%
STRUCTURE: UPTREND or DOWNTREND or RANGING
REASON: one narrative sentence — name the two most recent swing highs and lows with prices, \
state who is winning the battle (buyers or sellers and why), note any prominent wick zone \
near current price, and confirm with BuyVol% evidence"""


# ─────────────────────────────────────────────────────────────
# Specialist: Fibonacci
# ─────────────────────────────────────────────────────────────

FIBONACCI = """\
You are a Fibonacci retracement analyst. Analyze these 1-minute BTCUSDT bars.
Columns: Bar, Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%

{csv}

Task:
1. Find the dominant swing high and swing low in the data — these define the battle range.
2. Compute current price retracement % within that swing.
3. Is price at/near a key Fib level (23.6, 38.2, 50, 61.8, 78.6)?
4. Is it bouncing (buyers defending the level) or breaking down through it (sellers winning)?
5. Check BuyVol% at recent bars near the Fib level:
   - BuyVol% > 55% = buyers are defending: "Buyers are standing their ground at this Fib."
   - BuyVol% < 45% = sellers control: "Sellers are rejecting price at this Fib."
6. Look for wicks at or near the Fib level — a lower wick at a Fib support is the strongest
   buyer defense signal (buyers absorbed the selling and snapped back).

Respond EXACTLY (no extra text):
POSITION: ABOVE or BELOW
CONFIDENCE: XX%
LEVEL: nearest Fib % or NONE
REASON: one narrative sentence — cite the swing high/low prices defining the range, \
name the exact Fib level and what the battle looks like there (bouncing/breaking), \
reference any wick that confirms buyer or seller dominance, and state BuyVol% evidence"""


# ─────────────────────────────────────────────────────────────
# Specialist: Williams Alligator
# ─────────────────────────────────────────────────────────────

ALLIGATOR = """\
You are a Williams Alligator analyst. Analyze these 1-minute BTCUSDT bars.
Columns: Bar, Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%

{csv}

Task: compute the three Alligator lines using Wilder SMMA on closing prices:
- Jaw   (Blue):  SMMA(13), shift 8  — the slowest line, the trend backbone
- Teeth (Red):   SMMA(8),  shift 5  — the medium line
- Lips  (Green): SMMA(5),  shift 3  — the fastest line, first to react

Determine state:
- Lips > Teeth > Jaw = BULLISH (ordered up, alligator eating upward) → ABOVE
  Narrative: "The alligator has awakened and is eating upward — buyers are in full control."
- Lips < Teeth < Jaw = BEARISH (ordered down, alligator eating downward) → BELOW
  Narrative: "The alligator is eating downward — sellers dominate at every timeframe."
- Lines intertwined = SLEEPING (ranging, alligator resting, no directional conviction)
  Narrative: "The alligator is sleeping — no clear winner, avoid directional bias."

Crossover watch (last 3 bars): Lips crossing above Teeth = first sign of bull awakening.
Lips crossing below Teeth = first sign of bear awakening. Name the bar number if seen.

Volume confirmation:
- Rising Trades + QuoteVol on breakout from sleep = alligator awakening with conviction.
- Low volume breakout from sleep = false start, likely to return to range.

Respond EXACTLY (no extra text):
POSITION: ABOVE or BELOW
CONFIDENCE: XX%
STATE: BULLISH or BEARISH or SLEEPING
REASON: one narrative sentence — cite exact Jaw/Teeth/Lips values, describe the story \
(awakening/eating/sleeping), note any crossover in last 3 bars with bar number, \
and confirm with volume (Trades and QuoteVol at the crossover bar)"""


# ─────────────────────────────────────────────────────────────
# Specialist: Accumulation / Distribution
# ─────────────────────────────────────────────────────────────

ACC_DIST = """\
You are an Accumulation/Distribution analyst. Analyze these 1-minute BTCUSDT bars.
Columns: Bar, Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%

{csv}

Task:
1. Compute the A/D line using QuoteVol (USDT volume) for dollar-accurate weighting:
   CLV = ((Close-Low)-(High-Close))/(High-Low); A/D += CLV x QuoteVol each bar
2. Examine the slope of the last 5-10 bars of the A/D line.
3. Check for divergence:
   - A/D rising while price falls = BULLISH divergence: "Smart money is quietly accumulating
     as price falls. Retail is selling, institutions are buying. Reversal likely coming."
   - A/D falling while price rises = BEARISH divergence: "Smart money is distributing into
     the rally. Price rises but real money is leaving. The rally is not trustworthy."
4. BuyVol% trend across last 10 bars:
   - Sustained > 55%: "Buyers dominate trade flow — accumulation phase."
   - Sustained < 45%: "Sellers dominate trade flow — distribution phase."
   - Transitioning upward: "Buyers are taking control of the flow."

Respond EXACTLY (no extra text):
POSITION: ABOVE or BELOW
CONFIDENCE: XX%
VALUE: A/D slope direction (e.g. "+12345 rising" or "-6789 falling")
REASON: one narrative sentence — describe the A/D story (accumulation or distribution), \
state whether there is divergence with price direction and what that means for smart money \
behavior, and cite the BuyVol% trend as supporting or contradicting evidence"""


# ─────────────────────────────────────────────────────────────
# Specialist: Harmonic Patterns
# ─────────────────────────────────────────────────────────────

HARMONIC = """\
You are a harmonic pattern analyst. Analyze these 1-minute BTCUSDT bars.
Columns: Bar, Time(UTC), Open, High, Low, Close, Volume(BTC), QuoteVol(USDT), Trades, BuyVol%

{csv}

Task: identify the most recent 5-pivot XABCD structure using closes/highs/lows.
Check ratios for:
- Gartley:    AB=0.618XA, BC=0.382-0.886AB, CD=0.786XA
- Bat:        AB=0.382-0.5XA, BC=0.382-0.886AB, CD=0.886XA
- Crab:       AB=0.382-0.618XA, CD=1.618XA
- Butterfly:  AB=0.786XA, CD=1.27-1.618XA

If price is at or approaching the D completion zone, that is the signal.
If no clear pattern fits the ratios, state NONE.

Volume interpretation at D zone:
- High Trades + QuoteVol spike at D zone = pattern completing WITH conviction.
  Story: "Smart money is acting on the pattern — the setup is real."
- Low volume at D zone = pattern completing on thin air — potential false signal.
  Story: "No one is backing the pattern with size — treat as low-conviction setup."
- Look for a wick at the D zone: a lower wick (for bullish D) = buyers ambushed the
  completion zone and are defending it hard. This is the strongest harmonic confirmation.

Respond EXACTLY (no extra text):
POSITION: ABOVE or BELOW
CONFIDENCE: XX%
PATTERN: Gartley or Bat or Crab or Butterfly or NONE
REASON: one narrative sentence — name the pattern, cite the D zone price, describe the \
battle at D (bouncing/testing/failing), note if a wick confirms buyer/seller defense at D, \
and state whether volume gives conviction or raises doubt"""
