/* @jsxRuntime classic */
// Simple Analysis — integrated dashboard + microstructure

const { useState, useEffect, useRef, useCallback, useMemo } = React;

// Strategy colors
const STRATEGY_META = [
  { key: "rsi",          name: "RSI",          color: "#92400E" },
  { key: "macd",         name: "MACD",         color: "#5B21B6" },
  { key: "stochastic",   name: "Stochastic",   color: "#9F1239" },
  { key: "ema_cross",    name: "EMA Fast",     color: "#155E75", splitSlow: true },
  { key: "supertrend",   name: "Supertrend",   color: "#0F766E" },
  { key: "adx",          name: "ADX",          color: "#B45309" },
  { key: "alligator",    name: "Alligator",    color: "#0E7490" },
  { key: "acc_dist",     name: "Acc/Dist",     color: "#7C3AED" },
  { key: "dow_theory",   name: "Dow Theory",   color: "#78350F" },
  { key: "fib_pullback", name: "Fibonacci",    color: "#047857" },
  { key: "harmonic",     name: "Harmonic",     color: "#9D174D" },
  { key: "vwap",         name: "AVWAP",        color: "#0369A1" },
  { key: "ml_logistic",  name: "Lin Reg",      color: "#6B21A8" },
];

// Per-strategy explanations shown inside each indicator card
const STRATEGY_DESC = {
  rsi:          { short:"Fast RSI(4) on 1m closed bars.", how:"Above 80 = overbought, may reverse down. Below 20 = oversold, bounce likely. Crossovers at OB/OS boundaries are the strongest signal." },
  macd:         { short:"EMA difference reveals momentum shifts early.", how:"Histogram crossing zero or diverging from price = high-conviction trend change before price confirms." },
  stochastic:   { short:"Fast Stochastic K(5)/D(3) on 1m H/L/C.", how:"Above 80 = exhausted buyers (sell pressure). Below 20 = exhausted sellers (buyers step in). K crossing D = entry trigger." },
  ema_cross:    { short:"Fast EMA (5/13) filters near-term direction.", how:"5 EMA crossing above 13 EMA = bullish momentum shift. Below = bearish. HTF alignment multiplies conviction." },
  ema_slow:     { short:"Slower EMA (21/55) tracks institutional trend.", how:"When aligned with Fast EMA the signal has multi-timeframe confirmation, dramatically reducing false entries." },
  supertrend:   { short:"Volatility-adaptive trailing stop that flips on breakouts.", how:"Green (price above band) = bull trend. Red (below band) = bear. Self-adjusts to ATR so it works across volatility regimes." },
  adx:          { short:"Measures trend strength, not direction.", how:"Above 25 = real trend (signals reliable, follow them). Below 20 = choppy range (fade extremes). ADX rising = trend accelerating." },
  alligator:    { short:"Three smoothed MAs (jaw/teeth/lips) show trend 'eating'.", how:"Lips > Teeth > Jaw and fanning = strong uptrend in motion. Lines tangled together = no trend, stay flat." },
  acc_dist:     { short:"Volume-weighted smart-money flow indicator.", how:"A/D rising with price confirms bulls. A/D falling while price rises = bearish divergence — institutions distributing." },
  dow_theory:   { short:"Classical market structure: HH/HL = uptrend, LH/LL = down.", how:"Consecutive higher highs and higher lows confirm trend health. First lower low after a run = early reversal warning." },
  fib_pullback: { short:"Price naturally gravitates to Fibonacci retracement levels.", how:"Bounce off 61.8% = strong trend continuation. Break through 61.8% = deeper pullback to 78.6% or full retrace." },
  harmonic:     { short:"Geometric patterns (Bat, Gartley, Crab) predict reversals.", how:"A pattern completing near the D leg PRZ (reversal zone) combines timing + price with a pre-defined tight stop." },
  vwap:         { short:"Anchored VWAP at peak-volume bar + 1/2/3σ bands.", how:"Anchor is the highest-volume bar in 50 bars — where fair value was most contested. Price above VWAP = long bias; below = short. σ band shows extension: 1σ=momentum, 2σ=extended, 3σ=extreme." },
  ml_logistic:  { short:"Logistic regression trained on 100+ historical features.", how:"Slope shows trend angle; coefficient magnitude shows confidence. Feature agreement across multiple inputs = high edge." },
};

const C = {
  bg:          "#F9F8F6",
  surface:     "#FFFFFF",
  border:      "#E6E4DF",
  borderSoft:  "#EEECE8",
  text:        "#1A1A1A",
  textSec:     "#6B6866",
  muted:       "#A09D99",
  green:       "#15803D",
  greenBg:     "#F0FDF4",
  greenBorder: "#86EFAC",
  red:         "#B91C1C",
  redBg:       "#FFF1F2",
  redBorder:   "#FECDD3",
  amber:       "#C2410C",
  amberBg:     "#FFF7ED",
  amberBorder: "#FED7AA",
  indigo:      "#3730A3",
  blue:        "#1D4ED8",
  blueBg:      "#EFF6FF",
  blueBorder:  "#BFDBFE",
};

const card = {
  background: C.surface,
  border: `1px solid ${C.border}`,
  borderRadius: 8,
  padding: "10px 12px",
};

const label = {
  fontSize: 9,
  fontWeight: 700,
  color: C.muted,
  letterSpacing: 1.5,
  textTransform: "uppercase",
};

const td = { padding: "4px 8px", color: C.textSec };

// ── Helpers ──────────────────────────────────────────────────
function fmtK(n) {
  if (!n) return "—";
  if (n >= 1e9) return `$${(n/1e9).toFixed(2)}B`;
  if (n >= 1e6) return `$${(n/1e6).toFixed(1)}M`;
  if (n >= 1e3) return `$${(n/1e3).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
}

function fmtP(v, d=2) { return `${v>=0?"+":""}${v.toFixed(d)}%`; }

function sigColors(sig) {
  const bull = ["BULLISH","BULLISH_CONTRARIAN","BULLISH_ARBI"].includes(sig);
  const bear = ["BEARISH","BEARISH_CONTRARIAN","BEARISH_ARBI"].includes(sig);
  return {
    color:  bull ? C.green : bear ? C.red : C.amber,
    bg:     bull ? C.greenBg : bear ? C.redBg : C.amberBg,
    border: bull ? C.greenBorder : bear ? C.redBorder : C.amberBorder,
  };
}

function getStratOBOS(key, rawValue) {
  const v = parseFloat(rawValue);
  if (isNaN(v)) return null;
  const T = {
    rsi:        { ob:80, os:20, lob:65, los:35 },
    stochastic: { ob:80, os:20, lob:65, los:35 },
  };
  const t = T[key];
  if (!t) return null;
  if (v >= t.ob)  return { label:"OVERBOUGHT", color:"#B91C1C", bg:"#FFF1F2", border:"#FECDD3" };
  if (v <= t.os)  return { label:"OVERSOLD",   color:"#15803D", bg:"#F0FDF4", border:"#86EFAC" };
  if (v >= t.lob) return { label:"LEAN OB",    color:"#C2410C", bg:"#FFF7ED", border:"#FED7AA" };
  if (v <= t.los) return { label:"LEAN OS",    color:"#1D4ED8", bg:"#EFF6FF", border:"#BFDBFE" };
  return null;
}

// ── TradingView Chart ─────────────────────────────────────────
let _tvLoaded = false, _tvCbs = [];
function loadTV(cb) {
  if (typeof TradingView !== "undefined") { cb(); return; }
  _tvCbs.push(cb);
  if (_tvLoaded) return;
  _tvLoaded = true;
  const s = document.createElement("script");
  s.src = "https://s3.tradingview.com/tv.js";
  s.async = true;
  s.onload = () => { _tvCbs.forEach(f=>f()); _tvCbs=[]; };
  document.head.appendChild(s);
}

class ErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { error: null }; }
  static getDerivedStateFromError(err) { return { error: err }; }
  componentDidCatch(err, info) { console.error("[ErrorBoundary]", err, info); }
  render() {
    if (this.state.error) {
      return (
        <div style={{ height:"100%", padding:20, background:"#F9F8F6", color:"#ef4444", fontFamily:"monospace", fontSize:12 }}>
          <div style={{ fontWeight:700, marginBottom:6 }}>Render error — tab crashed</div>
          <div style={{ opacity:0.7 }}>{String(this.state.error)}</div>
          <button onClick={()=>this.setState({error:null})}
            style={{ marginTop:10, padding:"4px 12px", background:"#F9F8F6", border:"1px solid #ef4444",
              color:"#ef4444", borderRadius:4, cursor:"pointer", fontFamily:"inherit", fontSize:11 }}>
            Retry
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

function PriceChart() {
  const ref = useRef(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const id = "tv_" + Math.random().toString(36).slice(2,7);
    el.id = id;
    loadTV(() => {
      if (!document.getElementById(id)) return;
      new TradingView.widget({
        autosize:true, symbol:"BINANCE:BTCUSDT", interval:"5",
        timezone:"Etc/UTC", theme:"light", style:"1", locale:"en",
        toolbar_bg:C.bg, enable_publishing:false, hide_side_toolbar:false,
        allow_symbol_change:false, save_image:false, hide_volume:false,
        withdateranges:true, container_id:id,
        hide_top_toolbar:false,
        // Lock the interval selector to 5m only — matches our prediction
        // window so chart + briefing always agree on "what bar is this".
        disabled_features: ["header_resolutions", "header_compare",
                            "header_symbol_search", "header_interval_dialog_button",
                            "show_interval_dialog_on_key_press"],
        // Keep core timescale + chart tools enabled.
        enabled_features: [],
      });
    });
    return () => { el.innerHTML=""; el.id=""; };
  }, []);
  return <div ref={ref} style={{ width:"100%", height:"100%" }} />;
}

// ── Text formatting ───────────────────────────────────────────
const BOLD_TERMS = /(\$[\d,]+(?:\.\d+)?(?:k|K)?|\d+(?:\.\d+)?%|\b(?:bullish|bearish|uptrend|downtrend|upward|downward|support|resistance|breakout|breakdown|reversal|rejection|bounce|overbought|oversold|divergence|crossover|HH|HL|LH|LL|RSI|MACD|EMA|Stochastic|volume|strong|weak|moderate|holding|failing|momentum|aligned|neutral|above|below)\b)/gi;
function BoldAnalysis({ text, color }) {
  const parts = text.split(BOLD_TERMS);
  return (
    <span>{parts.map((p,i) => i%2===1
      ? <strong key={i} style={{ color:color||C.text, fontWeight:800 }}>{p}</strong>
      : <span key={i}>{p}</span>
    )}</span>
  );
}

// Map UP/DOWN/NEUTRAL/FLAT/BULLISH/BEARISH to the green/red/amber palette.
function posColors(pos) {
  const p = (pos || "").toUpperCase();
  if (/^(UP|BULLISH)/.test(p))   return { color: C.green, bg: C.greenBg, border: C.greenBorder };
  if (/^(DOWN|BEARISH)/.test(p)) return { color: C.red,   bg: C.redBg,   border: C.redBorder };
  return { color: C.amber, bg: C.amberBg, border: C.amberBorder };
}

// Parse the new ARGUMENT/COUNTER/SURVIVES_STEELMAN schema from reasoning.
// Multi-line aware: each field spans until the next field prefix (or end of string).
// Returns { argument, counter, survives, survivesReason, isStructured }.
// If none of the new fields appear, isStructured=false and caller falls back
// to the legacy numbered-list renderer.
function parseStructuredReasoning(reasoning) {
  const raw = (reasoning || "").trim();
  const FIELDS = ["ARGUMENT", "COUNTER", "SURVIVES_STEELMAN"];
  const fieldRe = new RegExp("^(" + FIELDS.join("|") + "):\\s*", "m");
  if (!fieldRe.test(raw)) return { isStructured: false };

  // Build a map: find each FIELD: prefix and collect text until next prefix or end.
  const result = { isStructured: true, argument: "", counter: "", survives: "", survivesReason: "" };
  const lines = raw.split("\n");
  let current = null;
  const acc = { ARGUMENT: [], COUNTER: [], SURVIVES_STEELMAN: [] };
  for (const line of lines) {
    const m = line.match(/^(ARGUMENT|COUNTER|SURVIVES_STEELMAN):\s*(.*)/);
    if (m) {
      current = m[1];
      if (m[2].trim()) acc[current].push(m[2].trim());
    } else if (current) {
      acc[current].push(line);
    }
  }
  result.argument = acc.ARGUMENT.join(" ").trim();
  result.counter  = acc.COUNTER.join(" ").trim();
  const svRaw = acc.SURVIVES_STEELMAN.join(" ").trim();
  // svRaw may be "YES — because X" or "NO — because X" or just "YES"/"NO"
  const svMatch = svRaw.match(/^(YES|NO)[^a-zA-Z]*(.*)/i);
  if (svMatch) {
    result.survives       = svMatch[1].toUpperCase();
    result.survivesReason = svMatch[2].replace(/^[—–\-\s]+/, "").trim();
  } else {
    result.survives       = svRaw.slice(0, 3).toUpperCase().startsWith("YES") ? "YES" : "NO";
    result.survivesReason = svRaw;
  }
  return result;
}

// Parse the strict Historical Similarity Analyst schema (POSITION/CONFIDENCE/
// LEAN/BASE_RATES/PRECEDENT_TABLE/AGAINST/ENSEMBLE_RELIABILITY/FOR/DEVIL/EDGE/
// SUGGESTION). The model alternates between this and free-form markdown, so
// callers should detect format with `isStrictHistSchema` first.
const HIST_KEYS = ["POSITION","CONFIDENCE","LEAN","BASE_RATES","PRECEDENT_TABLE",
  "AGAINST","ENSEMBLE_RELIABILITY","FOR","DEVIL","EDGE","SUGGESTION"];
const HIST_KEY_RE = new RegExp("^(" + HIST_KEYS.join("|") + ")\\s*:\\s*(.*)$");

// The model sometimes wraps schema keys in markdown bold (`**LEAN:**`,
// `**POSITION: NEUTRAL**`). Strip those wrappers so the parser sees the
// canonical `KEY: value` shape.
function normalizeHistSchema(raw) {
  if (!raw) return raw;
  const K = HIST_KEYS.join("|");
  return raw
    // `**KEY: VALUE**` — whole line bolded
    .replace(new RegExp("^\\s*\\*\\*\\s*(" + K + ")\\s*:\\s*([^*\\n]*?)\\s*\\*\\*\\s*$", "gm"), "$1: $2")
    // `**KEY:**` — just the key bolded
    .replace(new RegExp("^\\s*\\*\\*\\s*(" + K + ")\\s*:\\s*\\*\\*\\s*", "gm"), "$1: ")
    // Stray leading `**KEY:` with no closer (rare, but defensive)
    .replace(new RegExp("^\\s*\\*\\*\\s*(" + K + ")\\s*:\\s*", "gm"), "$1: ");
}
function isStrictHistSchema(raw) {
  if (!raw) return false;
  const norm = normalizeHistSchema(raw);
  const first = norm.split("\n").map(l => l.trim()).find(Boolean) || "";
  return /^POSITION\s*:/.test(first);
}
function parseHistAnalysis(raw) {
  raw = normalizeHistSchema(raw);
  const out = { position:"", confidence:"", lean:"", baseRates:"",
    precedents:[], against:"", ensemble:"", forText:"", devil:"", edge:"",
    suggestion:"", extra:"" };
  if (!raw) return out;
  const buckets = {};
  let cur = null, buf = [];
  const flush = () => {
    const v = buf.join("\n").trim();
    if (cur == null) { if (v) out.extra = (out.extra ? out.extra + "\n" : "") + v; }
    else buckets[cur] = v;
    buf = [];
  };
  for (const line of raw.split("\n")) {
    const m = line.match(HIST_KEY_RE);
    if (m) { flush(); cur = m[1]; if (m[2]) buf.push(m[2]); }
    else buf.push(line);
  }
  flush();

  out.position   = (buckets.POSITION   || "").trim();
  out.confidence = (buckets.CONFIDENCE || "").trim();
  out.lean       = (buckets.LEAN       || "").trim();
  out.baseRates  = (buckets.BASE_RATES || "").trim();
  out.against    = (buckets.AGAINST    || "").trim();
  out.ensemble   = (buckets.ENSEMBLE_RELIABILITY || "").trim();
  out.forText    = (buckets.FOR        || "").trim();
  out.devil      = (buckets.DEVIL      || "").trim();
  out.edge       = (buckets.EDGE       || "").trim();
  out.suggestion = (buckets.SUGGESTION || "").trim();

  out.precedents = (buckets.PRECEDENT_TABLE || "").split("\n")
    .map(l => l.trim())
    .filter(l => /^#?\d+\s*\|/.test(l))
    .map(l => {
      const parts = l.split("|").map(s => s.trim());
      const num = (parts[0] || "").replace(/^#/, "");
      const outcomeRaw = parts[1] || "";
      const oMatch = outcomeRaw.match(/^(UP|DOWN|NEUTRAL|FLAT)\b\s*(.*)$/i);
      const outcome = oMatch ? oMatch[1].toUpperCase() : outcomeRaw;
      const outcomeMeta = oMatch ? oMatch[2].trim() : "";
      let align = "", diverge = "";
      for (let i = 2; i < parts.length; i++) {
        const p = parts[i];
        if (/^align:/i.test(p))         align   = p.replace(/^align:\s*/i, "");
        else if (/^diverge:/i.test(p))  diverge = p.replace(/^diverge:\s*/i, "");
        else if (!align)                align   = p;
        else                            diverge = (diverge ? diverge + " | " : "") + p;
      }
      return { num, outcome, outcomeMeta, align, diverge };
    });

  return out;
}

// ── CurrentOngoingTrendPanel ─────────────────────────────────
// Surfaces the bar_trend_analyst specialist's 20-bar qualitative read.
// Returns null when the specialist has insufficient history (available=false)
// or when the field is absent — fully backwards compatible.
function CurrentOngoingTrendPanel({ trendAnalyst }) {
  if (!trendAnalyst || !trendAnalyst.available) return null;

  const ta = trendAnalyst;

  // REGIME pill colour
  let regimeBg, regimeClr, regimeBorder;
  if (ta.regime === "TRENDING_UP") {
    regimeBg = C.greenBg; regimeClr = C.green; regimeBorder = C.greenBorder;
  } else if (ta.regime === "TRENDING_DOWN") {
    regimeBg = C.redBg; regimeClr = C.red; regimeBorder = C.redBorder;
  } else {
    regimeBg = C.amberBg; regimeClr = C.amber; regimeBorder = C.amberBorder;
  }

  const pillBase = {
    display:"inline-block", fontSize:10, fontWeight:800, padding:"2px 9px",
    borderRadius:4, letterSpacing:0.7, textTransform:"uppercase",
  };

  const hasTraps = ta.traps_building && ta.traps_building.toUpperCase() !== "NONE"
    && ta.traps_building.trim() !== "";

  return (
    <div style={{
      background:C.amberBg,
      border:`1px solid ${C.amberBorder}`,
      borderLeft:`3px solid ${C.amber}`,
      borderRadius:6,
      padding:"10px 12px",
      marginBottom:12,
    }}>
      {/* Header row */}
      <div style={{ display:"flex", alignItems:"baseline", justifyContent:"space-between", marginBottom:8 }}>
        <span style={{ fontSize:10, fontWeight:900, color:C.amber,
          letterSpacing:1.2, textTransform:"uppercase" }}>
          Current Ongoing Trend
        </span>
        <span style={{ fontSize:9, color:C.muted, fontStyle:"italic" }}>
          last 20-bar synthesizer
        </span>
      </div>

      {/* Pills row: REGIME + VOLATILITY + VOLUME */}
      <div style={{ display:"flex", flexWrap:"wrap", gap:6, marginBottom:8 }}>
        {ta.regime && (
          <span style={{ ...pillBase,
            background:regimeBg, color:regimeClr, border:`1px solid ${regimeBorder}` }}>
            {ta.regime.replace(/_/g," ")}
          </span>
        )}
        {ta.volatility && (
          <span style={{ ...pillBase,
            background:"#F5F4F2", color:C.textSec, border:`1px solid ${C.border}` }}>
            Vol: {ta.volatility}
          </span>
        )}
        {ta.volume_profile && (
          <span style={{ ...pillBase,
            background:"#F5F4F2", color:C.textSec, border:`1px solid ${C.border}` }}>
            Volume: {ta.volume_profile.replace(/_/g," ")}
          </span>
        )}
      </div>

      {/* TRAPS line */}
      {(ta.traps_building !== undefined) && (
        <div style={{ fontSize:11, color: hasTraps ? C.red : C.muted,
          marginBottom:8, lineHeight:1.5 }}>
          {hasTraps
            ? <span><span style={{ fontWeight:700 }}>&#9888; Traps:</span> {ta.traps_building}</span>
            : <span style={{ fontStyle:"italic" }}>No traps identified</span>
          }
        </div>
      )}

      {/* SNAPSHOT — bold one-liner */}
      {ta.trend_snapshot && (
        <div style={{
          fontSize:14, fontWeight:700, color:C.text, lineHeight:1.5,
          marginBottom: ta.narrative ? 8 : 0,
          overflowWrap:"anywhere", wordBreak:"break-word",
        }}>
          {ta.trend_snapshot}
        </div>
      )}

      {/* NARRATIVE — full story paragraph */}
      {ta.narrative && (
        <div style={{
          fontSize:12, fontWeight:400, color:C.textSec, lineHeight:1.65,
          overflowWrap:"anywhere", wordBreak:"break-word",
        }}>
          {ta.narrative}
        </div>
      )}
    </div>
  );
}

// ── Briefing section helpers ────────────────────────────────
// A peer top-level section in the analysis card: bold uppercase title with a
// thin accent bar and an optional verdict pill / confidence on the right.
function BriefingSection({ title, verdict, confidence, accent, first, children }) {
  const accentColor = accent || (verdict ? posColors(verdict).color : C.text);
  return (
    <div style={{
      paddingTop: first ? 0 : 14,
      borderTop: first ? "none" : `1px solid ${C.borderSoft}`,
      marginBottom: 14
    }}>
      <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:10 }}>
        <span style={{ width:3, height:16, background:accentColor, borderRadius:2,
          flexShrink:0 }} />
        <span style={{
          fontSize:12, fontWeight:900, color:C.text,
          letterSpacing:1.4, textTransform:"uppercase", flex:1
        }}>{title}</span>
        {verdict && <VerdictPill verdict={verdict} />}
        {confidence && (
          <span style={{ fontSize:13, fontWeight:800, color:C.textSec,
            fontVariantNumeric:"tabular-nums" }}>
            {confidence}
          </span>
        )}
      </div>
      {children}
    </div>
  );
}

function VerdictPill({ verdict }) {
  const c = posColors(verdict);
  return (
    <span style={{
      fontSize:11, fontWeight:900, color:c.color, background:c.bg,
      border:`1px solid ${c.border}`, borderRadius:4, padding:"2px 10px",
      letterSpacing:0.8
    }}>{String(verdict).toUpperCase()}</span>
  );
}

// Smaller tracked label for sub-sections inside a BriefingSection.
function SubLabel({ children, color, marginTop }) {
  return (
    <div style={{
      fontSize:9, fontWeight:800, color: color || C.muted,
      letterSpacing:1.2, textTransform:"uppercase",
      marginTop: marginTop != null ? marginTop : 10,
      marginBottom:5
    }}>{children}</div>
  );
}

// Label-and-paragraph block. Used for AGAINST / FOR / DEVIL / EDGE / etc.
function KvBlock({ label, color, children }) {
  return (
    <div style={{ marginTop:10 }}>
      <SubLabel color={color} marginTop={0}>{label}</SubLabel>
      <div style={{ fontSize:12, color:C.text, lineHeight:1.55 }}>{children}</div>
    </div>
  );
}

// ── SteelmanDecisionBlock ─────────────────────────────────────
// Renders the ARGUMENT / COUNTER / SURVIVES_STEELMAN structured output
// from the new DeepSeek prompt schema. Shows:
//   • Position badge + TAKE/PASS pill
//   • Green-tinted ARGUMENT box
//   • Red-tinted COUNTER box
//   • Compact SURVIVES pill + one-line reason
// Falls back to nothing when `sr.isStructured` is false (legacy bars).
function SteelmanDecisionBlock({ signal, sr }) {
  if (!sr || !sr.isStructured) return null;
  const isUp      = signal === "UP";
  const isDown    = signal === "DOWN";
  const isNeutral = signal === "NEUTRAL";
  const survived  = sr.survives === "YES";

  // TAKE pill = directional + survives. PASS = survives but we've passed (shouldn't happen).
  // NEUTRAL = model abstained.
  let decisionLabel, decisionBg, decisionColor, decisionBorder;
  if (isNeutral) {
    decisionLabel  = "NEUTRAL";
    decisionBg     = C.amberBg;
    decisionColor  = C.amber;
    decisionBorder = C.amberBorder;
  } else if (survived) {
    decisionLabel  = "TAKE";
    decisionBg     = C.greenBg;
    decisionColor  = C.green;
    decisionBorder = C.greenBorder;
  } else {
    decisionLabel  = "PASS";
    decisionBg     = C.amberBg;
    decisionColor  = C.amber;
    decisionBorder = C.amberBorder;
  }

  const posClr = isUp ? C.green : isDown ? C.red : C.amber;

  return (
    <div style={{ marginBottom:14 }}>
      {/* Position + decision pill row */}
      <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:10 }}>
        <span style={{ fontSize:20, fontWeight:900, color:posClr, lineHeight:1 }}>
          {isUp ? "▲ UP" : isDown ? "▼ DOWN" : "— NEUTRAL"}
        </span>
        <span style={{
          fontSize:11, fontWeight:900, padding:"2px 10px", borderRadius:4,
          color:decisionColor, background:decisionBg, border:`1px solid ${decisionBorder}`,
          letterSpacing:0.8,
        }}>{decisionLabel}</span>
      </div>

      {/* ARGUMENT box */}
      {sr.argument && (
        <div style={{
          background:C.greenBg, border:`1px solid ${C.greenBorder}`,
          borderLeft:`3px solid ${C.green}`, borderRadius:5,
          padding:"8px 10px", marginBottom:8,
        }}>
          <div style={{ fontSize:9, fontWeight:800, color:C.green,
            letterSpacing:1.2, textTransform:"uppercase", marginBottom:4 }}>
            Why this call
          </div>
          <div style={{ fontSize:12, color:"#14532D", lineHeight:1.6 }}>
            {sr.argument}
          </div>
        </div>
      )}

      {/* COUNTER box */}
      {sr.counter && (
        <div style={{
          background:C.redBg, border:`1px solid ${C.redBorder}`,
          borderLeft:`3px solid ${C.red}`, borderRadius:5,
          padding:"8px 10px", marginBottom:8,
        }}>
          <div style={{ fontSize:9, fontWeight:800, color:C.red,
            letterSpacing:1.2, textTransform:"uppercase", marginBottom:4 }}>
            Strongest case against
          </div>
          <div style={{ fontSize:12, color:"#7F1D1D", lineHeight:1.6 }}>
            {sr.counter}
          </div>
        </div>
      )}

      {/* SURVIVES_STEELMAN pill + reason */}
      <div style={{ display:"flex", alignItems:"flex-start", gap:8, flexWrap:"wrap" }}>
        <span style={{
          fontSize:10, fontWeight:900, padding:"2px 8px", borderRadius:4, flexShrink:0,
          color: survived ? C.green : C.amber,
          background: survived ? C.greenBg : C.amberBg,
          border:`1px solid ${survived ? C.greenBorder : C.amberBorder}`,
        }}>
          {survived ? "✓ SURVIVES STEELMAN" : "⚠ DOES NOT SURVIVE"}
        </span>
        {sr.survivesReason && (
          <span style={{ fontSize:11, color:C.textSec, lineHeight:1.5, fontStyle:"italic" }}>
            {sr.survivesReason}
          </span>
        )}
      </div>
    </div>
  );
}

// Single Tier-A precedent card — #ID + outcome pill on top, ALIGN / DIVERGE
// rows below with green / red micro-labels.
function PrecedentCard({ p }) {
  const opc = posColors(p.outcome);
  return (
    <div style={{
      background:"#FFFFFF", border:`1px solid ${C.borderSoft}`,
      borderRadius:6, padding:"7px 10px"
    }}>
      <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:5 }}>
        <span style={{ fontSize:11, fontWeight:900, color:C.text,
          fontVariantNumeric:"tabular-nums" }}>#{p.num}</span>
        <span style={{ fontSize:10, fontWeight:900, color:opc.color,
          background:opc.bg, border:`1px solid ${opc.border}`, borderRadius:3,
          padding:"1px 7px", letterSpacing:0.5 }}>
          {p.outcome || "?"}
        </span>
        {p.outcomeMeta && (
          <span style={{ fontSize:11, color:C.textSec,
            fontVariantNumeric:"tabular-nums" }}>{p.outcomeMeta}</span>
        )}
      </div>
      {p.align && (
        <div style={{ display:"flex", gap:6, marginBottom:p.diverge ? 3 : 0 }}>
          <span style={{ fontSize:9, fontWeight:900, color:C.green,
            letterSpacing:0.8, minWidth:50, paddingTop:2 }}>ALIGN</span>
          <span style={{ fontSize:12, color:C.text, lineHeight:1.5, flex:1 }}>
            <BoldAnalysis text={p.align} color={C.text} />
          </span>
        </div>
      )}
      {p.diverge && (
        <div style={{ display:"flex", gap:6 }}>
          <span style={{ fontSize:9, fontWeight:900, color:C.red,
            letterSpacing:0.8, minWidth:50, paddingTop:2 }}>DIVERGE</span>
          <span style={{ fontSize:12, color:C.text, lineHeight:1.5, flex:1 }}>
            <BoldAnalysis text={p.diverge} color={C.text} />
          </span>
        </div>
      )}
    </div>
  );
}

// Lightweight markdown block parser. Handles headings (# … ######), horizontal
// rules (---/***/___), pipe tables, bullet lists (- / *), and paragraphs. The
// historical analyst sometimes follows the strict `POSITION:` schema and
// sometimes emits the step-structured markdown from the prompt's reasoning
// protocol — this parser handles either since strict-schema lines just look
// like paragraphs.
function parseMarkdownBlocks(raw) {
  if (!raw) return [];
  const lines = raw.split("\n");
  const blocks = [];
  const isHeader = (t) => /^#{1,6}\s+/.test(t);
  const isHr     = (t) => /^([-*_])\1{2,}\s*$/.test(t);
  const isBullet = (t) => /^[-*]\s+/.test(t);
  const isTableLine = (t) => t.startsWith("|") && t.endsWith("|");
  const isTableSep  = (t) => /^\|[\s:|-]+\|$/.test(t) && t.includes("-");
  const splitRow    = (l) => l.trim().replace(/^\||\|$/g, "").split("|").map(s => s.trim());

  let i = 0;
  while (i < lines.length) {
    const t = lines[i].trim();

    if (!t) { i++; continue; }

    if (isHeader(t)) {
      const m = t.match(/^(#{1,6})\s+(.+?)\s*#*\s*$/);
      blocks.push({ type:"heading", level:m[1].length, text:m[2] });
      i++;
      continue;
    }

    if (isHr(t)) {
      blocks.push({ type:"hr" });
      i++;
      continue;
    }

    if (isTableLine(t) && i+1 < lines.length && isTableSep(lines[i+1].trim())) {
      const header = splitRow(t);
      i += 2;
      const rows = [];
      while (i < lines.length) {
        const l = lines[i].trim();
        if (!isTableLine(l) || isTableSep(l)) break;
        rows.push(splitRow(l));
        i++;
      }
      blocks.push({ type:"table", header, rows });
      continue;
    }

    if (isBullet(t)) {
      const items = [];
      while (i < lines.length && isBullet(lines[i].trim())) {
        items.push(lines[i].trim().replace(/^[-*]\s+/, ""));
        i++;
      }
      blocks.push({ type:"list", items });
      continue;
    }

    // Paragraph — gather until blank line or block-starting line.
    const para = [];
    while (i < lines.length) {
      const tt = lines[i].trim();
      if (!tt) break;
      if (isHeader(tt) || isHr(tt) || isBullet(tt)) break;
      if (isTableLine(tt) && i+1 < lines.length && isTableSep(lines[i+1].trim())) break;
      para.push(tt);
      i++;
    }
    if (para.length) blocks.push({ type:"para", text: para.join(" ") });
  }
  return blocks;
}

// Render an inline string with **bold** highlighting. Non-bold segments fall
// through to BoldAnalysis for term-level styling consistent with the rest of
// the card.
function MdInline({ text, color }) {
  if (!text) return null;
  const parts = String(text).split(/(\*\*[^*\n]+?\*\*)/g);
  return (
    <>{parts.map((p, idx) => {
      if (!p) return null;
      if (p.startsWith("**") && p.endsWith("**")) {
        return <strong key={idx} style={{ fontWeight:800, color: color || C.text }}>
          {p.slice(2, -2)}
        </strong>;
      }
      return <BoldAnalysis key={idx} text={p} color={color} />;
    })}</>
  );
}

// Render a markdown table. If a column header matches "outcome", that cell is
// rendered as a colored UP/DOWN/NEUTRAL badge. Each row is a card so it stays
// readable in a narrow sidebar.
function MdTable({ header, rows }) {
  const outcomeIdx = header.findIndex(h => /\boutcome\b/i.test(h));
  return (
    <div style={{ display:"flex", flexDirection:"column", gap:6, marginBottom:8 }}>
      {rows.map((r, i) => (
        <div key={i} style={{ background:"#FFFFFF",
          border:`1px solid ${C.borderSoft}`, borderRadius:5,
          padding:"6px 8px" }}>
          {r.map((cell, j) => {
            const lab = header[j] || "";
            if (j === outcomeIdx) {
              const m = String(cell).match(/^\s*(UP|DOWN|NEUTRAL)\b/i);
              const tag = m ? m[1].toUpperCase() : String(cell);
              const rest = m ? String(cell).slice(m[0].length).trim() : "";
              const opc = posColors(tag);
              return (
                <div key={j} style={{ display:"flex", gap:6, alignItems:"center",
                  marginBottom: j === r.length-1 ? 0 : 4 }}>
                  <span style={{ fontSize:9, fontWeight:700, color:C.muted,
                    letterSpacing:0.8, minWidth:64, flexShrink:0,
                    textTransform:"uppercase" }}>
                    {lab}
                  </span>
                  <span style={{ fontSize:10, fontWeight:900, color:opc.color,
                    background:opc.bg, border:`1px solid ${opc.border}`,
                    borderRadius:3, padding:"1px 6px", letterSpacing:0.5 }}>
                    {m ? tag : <MdInline text={tag} />}
                  </span>
                  {rest && <span style={{ fontSize:11, color:C.text }}>
                    <MdInline text={rest} />
                  </span>}
                </div>
              );
            }
            return (
              <div key={j} style={{ display:"flex", gap:6,
                alignItems:"flex-start",
                marginBottom: j === r.length-1 ? 0 : 4 }}>
                <span style={{ fontSize:9, fontWeight:700, color:C.muted,
                  letterSpacing:0.8, minWidth:64, flexShrink:0,
                  paddingTop:1, textTransform:"uppercase" }}>
                  {lab}
                </span>
                <span style={{ fontSize:11, color:C.text, lineHeight:1.5, flex:1 }}>
                  <MdInline text={String(cell)} />
                </span>
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

// Render markdown blocks as JSX, in the visual style of the rest of the
// briefing card. Top-level headings get a dividing line above; paragraphs and
// lists use a consistent text color.
function MdBlocks({ blocks }) {
  return (
    <>{blocks.map((b, i) => {
      if (b.type === "heading") {
        const isTop = b.level <= 2;
        const fontSize = b.level === 1 ? 12 : b.level === 2 ? 11 : 10;
        return (
          <div key={i} style={{
            fontSize, fontWeight:900, color:C.textSec,
            letterSpacing:1, textTransform:"uppercase",
            marginTop: i === 0 ? 0 : (isTop ? 10 : 6),
            marginBottom: 6,
            paddingTop: i > 0 && isTop ? 8 : 0,
            borderTop: i > 0 && isTop ? `1px solid ${C.borderSoft}` : "none"
          }}>
            <MdInline text={b.text} color={C.textSec} />
          </div>
        );
      }
      if (b.type === "hr") {
        return <div key={i} style={{ height:1,
          background:C.borderSoft, margin:"6px 0" }} />;
      }
      if (b.type === "list") {
        return (
          <ul key={i} style={{ margin:"4px 0 8px 18px", padding:0,
            fontSize:13, color:C.text, lineHeight:1.55 }}>
            {b.items.map((it, j) => (
              <li key={j} style={{ marginBottom:2 }}>
                <MdInline text={it} />
              </li>
            ))}
          </ul>
        );
      }
      if (b.type === "para") {
        return (
          <div key={i} style={{ fontSize:13, color:C.text,
            lineHeight:1.55, marginBottom:6 }}>
            <MdInline text={b.text} />
          </div>
        );
      }
      if (b.type === "table") {
        return <MdTable key={i} header={b.header} rows={b.rows} />;
      }
      return null;
    })}</>
  );
}

function fmtUtc(ts) {
  if (!ts) return null;
  const d = new Date(ts*1000);
  return `${String(d.getUTCHours()).padStart(2,"0")}:${String(d.getUTCMinutes()).padStart(2,"0")}:${String(d.getUTCSeconds()).padStart(2,"0")} UTC`;
}
// legacy alias
const fmtUtc7 = fmtUtc;

// ── Microstructure card component ─────────────────────────────
function MicroCard({ title, source, sig, dot, children }) {
  const sc = sig ? sigColors(sig) : null;
  const dotColor = dot==="live" ? C.green : dot==="err" ? C.red : dot==="pend" ? C.amber : C.border;
  return (
    <div style={{ ...card, padding:"7px 9px", display:"flex", flexDirection:"column", gap:4 }}>
      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start" }}>
        <div>
          <div style={{ fontSize:9, fontWeight:700, color:C.textSec, letterSpacing:0.5, textTransform:"uppercase" }}>{title}</div>
          {source && <div style={{ fontSize:8, color:C.muted, marginTop:1 }}>{source}</div>}
        </div>
        <div style={{ display:"flex", alignItems:"center", gap:5, flexShrink:0 }}>
          {sc && (
            <span style={{ fontSize:8, fontWeight:700, padding:"1px 5px", borderRadius:3,
              background:sc.bg, color:sc.color, border:`1px solid ${sc.border}` }}>
              {sig.replace(/_CONTRARIAN|_ARBI/,"")}
            </span>
          )}
          <div style={{ width:7, height:7, borderRadius:"50%", background:dotColor, flexShrink:0,
            boxShadow: dot==="live" ? `0 0 4px ${C.green}66` : "none" }} />
        </div>
      </div>
      {children}
    </div>
  );
}

// ── Microstructure Summary (dashboard compact view) ──────────
function MicroSummary({ ob, tk, ls, lq, oif, cz, fg, mp, cg, dots, collapsed, onToggle, noCard }) {
  var tileBase = { padding:"6px 4px", borderRadius:6, textAlign:"center" };
  function tileStyle(sig) {
    var sc = sig ? sigColors(sig) : null;
    return Object.assign({}, tileBase, {
      background: sc ? sc.bg : C.bg,
      border: "1px solid " + (sc ? sc.border : C.borderSoft)
    });
  }
  function arrowChar(dir) { return dir==="up" ? "▲" : dir==="down" ? "▼" : "—"; }
  function arrowColor(sig) { var sc = sig ? sigColors(sig) : null; return sc ? sc.color : C.muted; }

  // Situation summary
  var sit = [];
  if (ob && ob.imb!=null) sit.push(ob.imb>5 ? "Book bid-heavy (+"+ob.imb.toFixed(1)+"%) — buyers defending"
    : ob.imb<-5 ? "Book ask-heavy ("+ob.imb.toFixed(1)+"%) — sellers capping"
    : "Book balanced ("+(ob.imb>=0?"+":"")+ob.imb.toFixed(1)+"%)");
  if (tk && tk.bsr!=null) sit.push(tk.bsr>1.12 ? "aggressive buyers (BSR "+tk.bsr.toFixed(3)+")"
    : tk.bsr<0.9 ? "aggressive sellers (BSR "+tk.bsr.toFixed(3)+")"
    : "taker flow balanced (BSR "+tk.bsr.toFixed(3)+")");
  if (ls && ls.div!=null) sit.push(Math.abs(ls.div)>10
    ? "smart $ "+(ls.smartLong>ls.retailLong?"more long":"more short")+" by "+Math.abs(ls.div).toFixed(0)+"%"
    : "smart/retail aligned "+ls.smartLong.toFixed(0)+"% long");
  if (lq && lq.total>0) sit.push(lq.lvol>lq.svol*1.5 ? "long cascade ("+(lq.longCount||0)+" liqs)"
    : lq.svol>lq.lvol*1.5 ? "short squeeze ("+(lq.shortCount||0)+" liqs)" : "mixed liqs ("+lq.total+")");
  if (oif && oif.fr!=null) sit.push(oif.fr>0.0006 ? "high funding — longs paying"
    : oif.fr<0 ? "neg funding — shorts paying" : "funding neutral");
  if (fg && fg.value!=null) sit.push(fg.value<30 ? "extreme fear ("+fg.value+")"
    : fg.value>75 ? "extreme greed ("+fg.value+")" : "sentiment "+fg.value);

  var inner = (<>
      <div style={{ display:"flex", gap:4, alignItems:"center", marginBottom:6 }}>
        {[["ob","OB"],["tk","TF"],["ls","LS"],["lq","LQ"],["oif","OI"],["cz","CZ"],["fg","FG"],["mp","MP"],["cg","GK"]].map(function(pair){
          var d = dots[pair[0]];
          return React.createElement("span", { key:pair[0], title:pair[1], style:{
            width:6, height:6, borderRadius:"50%", display:"inline-block",
            background: d==="live" ? C.green : d==="err" ? C.red : C.amber }});
        })}
      </div>

      <div style={{ display:"grid", gridTemplateColumns:"repeat(5,1fr)", gap:4 }}>

        {/* Order Book */}
        <div style={tileStyle(ob && ob.imb!=null ? ob.sig : null)}>
          <div style={{ fontSize:9, color:"#0369A1", fontWeight:700 }}>Order Book</div>
          <div style={{ fontSize:8, color:C.muted }}>Depth-20</div>
          {ob && ob.imb!=null ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:arrowColor(ob.sig), lineHeight:1, marginTop:2 }}>{arrowChar(ob.imb>5?"up":ob.imb<-5?"down":"flat")}</div>
            <div style={{ fontSize:11, fontWeight:800, color:arrowColor(ob.sig) }}>{(ob.imb>=0?"+":"")+ob.imb.toFixed(1)+"%"}</div>
            {ob.bv!=null && ob.av!=null ? (<>
              <div style={{ display:"flex", height:3, borderRadius:2, overflow:"hidden", margin:"2px 0" }}>
                <div style={{ width:(ob.bv/(ob.bv+ob.av)*100)+"%", background:C.green }} />
                <div style={{ flex:1, background:C.red }} />
              </div>
              <div style={{ fontSize:8, color:C.muted }}>{ob.bv.toFixed(0)+"B / "+ob.av.toFixed(0)+"A"}</div>
            </>) : null}
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* Taker Flow */}
        <div style={tileStyle(tk && tk.bsr!=null ? tk.sig : null)}>
          <div style={{ fontSize:9, color:"#7C3AED", fontWeight:700 }}>Taker Flow</div>
          <div style={{ fontSize:8, color:C.muted }}>Aggressor</div>
          {tk && tk.bsr!=null ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:arrowColor(tk.sig), lineHeight:1, marginTop:2 }}>{arrowChar(tk.bsr>1.12?"up":tk.bsr<0.9?"down":"flat")}</div>
            <div style={{ fontSize:11, fontWeight:800, color:arrowColor(tk.sig) }}>{"BSR "+tk.bsr.toFixed(3)}</div>
            <div style={{ fontSize:9, fontWeight:700, color:tk.trend==="ACC↑"?C.green:tk.trend==="ACC↓"?C.red:C.amber, marginTop:1 }}>{tk.trend||"—"}</div>
            <div style={{ fontSize:8, color:C.muted }}>{(tk.bv||0).toFixed(0)+"B / "+(tk.sv||0).toFixed(0)+"S"}</div>
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* L/S Ratio */}
        <div style={tileStyle(ls && ls.smartLong!=null ? ls.sSig : null)}>
          <div style={{ fontSize:9, color:"#B45309", fontWeight:700 }}>L/S Ratio</div>
          <div style={{ fontSize:8, color:C.muted }}>Smart $</div>
          {ls && ls.smartLong!=null ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:arrowColor(ls.sSig), lineHeight:1, marginTop:2 }}>{arrowChar(ls.smartLong>60?"up":ls.smartLong<40?"down":"flat")}</div>
            <div style={{ fontSize:11, fontWeight:800, color:arrowColor(ls.sSig) }}>{ls.smartLong.toFixed(0)+"% L"}</div>
            <div style={{ fontSize:8, color:Math.abs(ls.div||0)>10?C.amber:C.muted }}>{"Δ"+((ls.div||0)>0?"+":"")+(ls.div||0).toFixed(0)+"% vs retail"}</div>
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* Liquidations */}
        <div style={tileStyle(lq && lq.total!=null ? lq.sig : null)}>
          <div style={{ fontSize:9, color:"#DC2626", fontWeight:700 }}>Liquidations</div>
          <div style={{ fontSize:8, color:C.muted }}>Force orders</div>
          {lq && lq.total!=null ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:arrowColor(lq.sig), lineHeight:1, marginTop:2 }}>{arrowChar(lq.lvol>lq.svol*1.5?"down":lq.svol>lq.lvol*1.5?"up":"flat")}</div>
            <div style={{ fontSize:11, fontWeight:800, color:arrowColor(lq.sig) }}>{lq.total+" liqs"}</div>
            <div style={{ fontSize:8, color:C.muted }}>{(lq.longCount||0)+"L / "+(lq.shortCount||0)+"S"}</div>
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* OI + Funding */}
        <div style={tileStyle(oif && oif.fr!=null ? oif.frSig : null)}>
          <div style={{ fontSize:9, color:"#0E7490", fontWeight:700 }}>OI + Fund</div>
          <div style={{ fontSize:8, color:C.muted }}>Perp</div>
          {oif && oif.fr!=null ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:arrowColor(oif.frSig), lineHeight:1, marginTop:2 }}>{arrowChar(oif.fr>0.0006?"down":oif.fr<0?"up":"flat")}</div>
            <div style={{ fontSize:11, fontWeight:800, color:arrowColor(oif.frSig) }}>{(oif.fr*100).toFixed(4)+"%"}</div>
            {oif.oi!=null ? <div style={{ fontSize:8, color:C.muted }}>{oif.oi.toLocaleString("en-US",{maximumFractionDigits:0})+" BTC"}</div> : null}
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* Cross-Exchange Funding */}
        <div style={tileStyle(cz && cz.fr!=null ? cz.sig : null)}>
          <div style={{ fontSize:9, color:"#4338CA", fontWeight:700 }}>X-Fund</div>
          <div style={{ fontSize:8, color:C.muted }}>Coinalyze</div>
          {cz && cz.fr!=null ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:arrowColor(cz.sig), lineHeight:1, marginTop:2 }}>{arrowChar(cz.fr>0.0005?"down":cz.fr<0?"up":"flat")}</div>
            <div style={{ fontSize:11, fontWeight:800, color:arrowColor(cz.sig) }}>{(cz.fr*100).toFixed(4)+"%"}</div>
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* Fear & Greed */}
        <div style={tileStyle(fg ? fg.sig : null)}>
          <div style={{ fontSize:9, color:"#92400E", fontWeight:700 }}>Fear/Greed</div>
          <div style={{ fontSize:8, color:C.muted }}>Sentiment</div>
          {fg ? (<>
            <div style={{ fontSize:22, fontWeight:900, color:fg.sig?arrowColor(fg.sig):C.amber, lineHeight:1, marginTop:2 }}>{fg.value}</div>
            <div style={{ fontSize:9, fontWeight:700, color:fg.sig?arrowColor(fg.sig):C.muted }}>{fg.label}</div>
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* Mempool */}
        <div style={tileStyle(mp ? mp.sig : null)}>
          <div style={{ fontSize:9, color:"#4F46E5", fontWeight:700 }}>Mempool</div>
          <div style={{ fontSize:8, color:C.muted }}>On-chain</div>
          {mp ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:arrowColor(mp.sig), lineHeight:1, marginTop:2 }}>{arrowChar(mp.fastest>50?"down":mp.fastest<10?"up":"flat")}</div>
            <div style={{ fontSize:11, fontWeight:800, color:arrowColor(mp.sig) }}>{mp.fastest+" sat/vB"}</div>
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

        {/* CoinGecko Market */}
        <div style={tileStyle(cg ? (cg.ch>=0?"BULLISH":"BEARISH") : null)}>
          <div style={{ fontSize:9, color:"#047857", fontWeight:700 }}>Market</div>
          <div style={{ fontSize:8, color:C.muted }}>CoinGecko</div>
          {cg ? (<>
            <div style={{ fontSize:18, fontWeight:800, color:cg.ch>=0?C.green:C.red, lineHeight:1, marginTop:2 }}>{cg.ch>=0?"▲":"▼"}</div>
            <div style={{ fontSize:11, fontWeight:800, color:cg.ch>=0?C.green:C.red }}>{(cg.ch>=0?"+":"")+cg.ch.toFixed(1)+"%"}</div>
          </>) : <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>...</div>}
        </div>

      </div>

      {sit.length > 0 ? (
        <div style={{ marginTop:6, padding:"5px 8px", background:C.bg, borderRadius:5,
          border:"1px solid "+C.borderSoft, fontSize:10, color:C.textSec, lineHeight:1.6 }}>
          <strong style={{ color:C.text }}>Situation: </strong>
          {sit.join(". ")+"."}
        </div>
      ) : null}
  </>);

  if (noCard) return inner;
  return (
    <div style={Object.assign({}, card, { flexShrink:0 })}>
      <button onClick={onToggle} style={{
        display:"flex", justifyContent:"space-between", alignItems:"center",
        width:"100%", background:"none", border:"none", cursor:"pointer", padding:0,
        color:"inherit", fontFamily:"inherit", marginBottom: collapsed ? 0 : 6 }}>
        <span style={label}>Market Microstructure · live</span>
        <span style={{ fontSize:11, color:C.muted }}>{collapsed?"▼ show":"▲ hide"}</span>
      </button>
      {!collapsed && inner}
    </div>
  );
}

// ── DeepSeek Audit Tab ────────────────────────────────────────
function DeepSeekAuditTab({ deepseekLog, deepseekAcc, deepseekPred, ensembleAccuracy, totalPreds, correctPreds, agreeAcc }) {
  const [expanded, setExpanded]     = useState({});
  const [detailCache, setDetailCache] = useState({});

  async function toggleBar(ws) {
    if (expanded[ws]) {
      setExpanded(e => ({ ...e, [ws]: false }));
      return;
    }
    if (!detailCache[ws]) {
      try {
        const r = await fetch(`/deepseek/predictions/${ws}`);
        if (r.ok) {
          const data = await r.json();
          setDetailCache(c => ({ ...c, [ws]: data }));
        }
      } catch (_) {}
    }
    setExpanded(e => ({ ...e, [ws]: true }));
  }





  const pending = deepseekPred && deepseekPred.signal !== "ERROR";
  const total   = deepseekLog ? deepseekLog.length : 0;

  return (
    <div style={{ height:"100%", overflowY:"auto", display:"flex", flexDirection:"column", gap:3 }}>
      {/* Pending banner */}
      {pending && (
        <div style={{ ...card, flexShrink:0, display:"flex", alignItems:"center", gap:12, padding:"7px 12px" }}>
          <div style={{ fontSize:20, fontWeight:900, color:deepseekPred.signal==="UP"?C.green:deepseekPred.signal==="NEUTRAL"?C.amber:C.red }}>
            {deepseekPred.signal==="UP"?"▲ UP":deepseekPred.signal==="NEUTRAL"?"— NEUTRAL":"▼ DOWN"}
          </div>
          <div>
            <div style={{ fontSize:16, fontWeight:800, color:C.text }}>{deepseekPred.confidence}%</div>
            <div style={{ fontSize:9, color:C.muted }}>#{deepseekPred.window_count} · {deepseekPred.latency_ms}ms</div>
          </div>
          <div style={{ marginLeft:"auto", fontSize:10, fontWeight:700, color:C.amber,
            background:C.amberBg, border:`1px solid ${C.amberBorder}`, borderRadius:4, padding:"2px 8px" }}>PENDING</div>
        </div>
      )}

      {/* Count header */}
      {total > 0 && (
        <div style={{ ...card, flexShrink:0, padding:"4px 12px", display:"flex", alignItems:"center", gap:10 }}>
          <span style={{ fontSize:9, fontWeight:700, color:C.muted, textTransform:"uppercase", letterSpacing:1 }}>
            {total} bars logged
          </span>
          <span style={{ fontSize:9, color:C.muted }}>· click any row to expand full record</span>
        </div>
      )}

      {(!deepseekLog || !deepseekLog.length) ? (
        <div style={{ ...card, textAlign:"center", padding:30 }}>
          <div style={{ color:C.muted, fontSize:13, fontWeight:700, marginBottom:6 }}>No historical data found</div>
          <div style={{ color:C.muted, fontSize:11 }}>DeepSeek fires at each 5-minute bar open — results will appear here once the first bar resolves.</div>
        </div>
      ) : deepseekLog.map((row, idx) => {
        const ws         = row.window_start;
        const isUp       = row.signal === "UP";
        const isNeutral  = row.signal === "NEUTRAL";
        const result     = isNeutral ? "NO TRADE" : row.correct == null ? "PENDING" : row.correct ? "WIN" : "LOSS";
        const isExp      = !!expanded[ws];
        const detail     = detailCache[ws] || null;
        const barNum     = row.window_count || (total - idx);
        const d          = new Date(ws * 1000);
        const wDate      = d.toLocaleDateString([], { month:"short", day:"numeric" });
        const wTime      = String(d.getUTCHours()).padStart(2,"0") + ":" + String(d.getUTCMinutes()).padStart(2,"0") + " UTC";
        const delta      = row.end_price != null ? ((row.end_price - row.start_price) / row.start_price * 100) : null;
        const bdrColor   = result==="WIN"?C.green:result==="LOSS"?C.red:result==="NO TRADE"?C.muted:C.amberBorder;
        // For NEUTRAL bars: what actually happened and what would have been correct
        const actualDir  = row.actual_direction;  // "UP" or "DOWN" or null
        const wouldWin   = isNeutral && actualDir != null;  // if we had picked actualDir we'd have won
        const neutralTag = isNeutral && actualDir
          ? (actualDir === "UP" ? "went ▲ UP" : "went ▼ DN")
          : null;

        return (
          <div key={ws} style={{ background:C.surface, border:`1px solid ${C.border}`, borderRadius:8,
            borderLeft:`3px solid ${bdrColor}`, flexShrink:0, overflow:"hidden" }}>

            {/* ── Collapsed summary row (always visible) ── */}
            <div onClick={() => toggleBar(ws)} style={{
              display:"flex", alignItems:"center", gap:6, padding:"6px 10px",
              cursor:"pointer", userSelect:"none",
              background: isExp ? (result==="WIN"?C.greenBg:result==="LOSS"?C.redBg:result==="NO TRADE"?C.bg:C.amberBg) : C.surface,
            }}>
              <span style={{ fontSize:10, fontWeight:900, color:C.muted, minWidth:38, flexShrink:0 }}>
                #{String(barNum).padStart(3,"0")}
              </span>
              <span style={{ fontSize:9, color:C.textSec, minWidth:96, flexShrink:0 }}>
                {wDate} {wTime}
              </span>
              <span style={{ fontSize:11, fontWeight:800, color:isUp?C.green:isNeutral?C.amber:C.red, minWidth:48, flexShrink:0 }}>
                {isUp?"▲ UP":isNeutral?"— N":"▼ DN"}
              </span>
              <span style={{ fontSize:10, color:C.textSec, minWidth:36, flexShrink:0 }}>
                {row.confidence ?? "—"}%
              </span>
              {delta != null
                ? <span style={{ fontSize:10, fontWeight:700, color:delta>=0?C.green:C.red, minWidth:58, flexShrink:0 }}>
                    {delta>=0?"+":""}{delta.toFixed(3)}%
                  </span>
                : <span style={{ minWidth:58, flexShrink:0 }} />
              }
              <span style={{ fontSize:9, fontWeight:700, padding:"1px 6px", borderRadius:3, flexShrink:0,
                background:result==="WIN"?C.greenBg:result==="LOSS"?C.redBg:result==="NO TRADE"?C.bg:C.amberBg,
                color:result==="WIN"?C.green:result==="LOSS"?C.red:result==="NO TRADE"?C.muted:C.amber,
                border:`1px solid ${result==="WIN"?C.greenBorder:result==="LOSS"?C.redBorder:result==="NO TRADE"?C.borderSoft:C.amberBorder}` }}>
                {result==="WIN"?"✓ WIN":result==="LOSS"?"✕ LOSS":result==="NO TRADE" ? (neutralTag ? `— N/A (${neutralTag})` : "— N/A") :"● PEND"}
              </span>
              {row.latency_ms
                ? <span style={{ fontSize:9, color:C.muted, flexShrink:0 }}>{row.latency_ms}ms</span>
                : null}
              <span style={{ marginLeft:"auto", fontSize:9, color:C.muted, flexShrink:0 }}>
                {isExp ? "▲" : "▼"}
              </span>
            </div>

            {/* ── Expanded full record ── */}
            {isExp && (
              <div style={{ borderTop:`1px solid ${C.borderSoft}`, padding:"10px 12px" }}>
                {!detail
                  ? <div style={{ color:C.muted, fontSize:10, fontStyle:"italic", padding:"8px 0" }}>Loading full record…</div>
                  : <BarDetail row={detail} ws={ws} expanded={expanded} setExpanded={setExpanded} />
                }
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ── Full bar detail panel (rendered inside expanded row) ───────
function BarDetail({ row, ws, expanded, setExpanded }) {
  const showPrompt = !!expanded[`${ws}_prompt`];
  const showRaw    = !!expanded[`${ws}_raw`];
  function toggle(key) { setExpanded(e => ({ ...e, [key]: !e[key] })); }

  const snap     = (() => { try { return JSON.parse(row.strategy_snapshot || "{}"); } catch(_) { return {}; } })();
  const bullish  = Object.values(snap).filter(s => s?.signal === "UP").length;
  const bearish  = Object.values(snap).filter(s => s?.signal === "DOWN").length;
  const reasons  = (String(row.reasoning || "")).split("\n").filter(Boolean);
  const imgUrl   = row.chart_path ? ("/charts/" + String(row.chart_path).replace(/\\/g,"/").split("/").pop()) : null;
  const dataReq  = row.data_requests && String(row.data_requests).toUpperCase() !== "NONE" ? row.data_requests : "";
  const isUp     = row.signal === "UP";
  const isNeutral= row.signal === "NEUTRAL";

  // Outcome derivation for the result banner
  const resolved = row.end_price != null;
  const movePct  = resolved ? ((row.end_price - row.start_price) / row.start_price * 100) : null;
  const correct  = row.correct;  // true | false | null
  let outcomeKind = "PENDING";
  if (isNeutral && resolved) outcomeKind = "ABSTAINED";
  else if (correct === true)  outcomeKind = "WIN";
  else if (correct === false) outcomeKind = "LOSS";
  const outcomeStyle = {
    WIN:       { bg:C.greenBg, border:C.greenBorder, color:C.green,  label:"✓ CORRECT CALL" },
    LOSS:      { bg:C.redBg,   border:C.redBorder,   color:C.red,    label:"✕ WRONG CALL"   },
    ABSTAINED: { bg:C.bg,      border:C.borderSoft,  color:C.muted,  label:"— NO TRADE"     },
    PENDING:   { bg:C.amberBg, border:C.amberBorder, color:C.amber,  label:"● PENDING"      },
  }[outcomeKind];

  // Pull LESSON_NAME / LESSON_RULE / ERROR_CLASS from postmortem for a prominent callout
  const pmLines = String(row.postmortem || "").split("\n").map(l => l.trim());
  const lesson = {
    name:       (pmLines.find(l => l.startsWith("LESSON_NAME:"))       || "").replace("LESSON_NAME:", "").trim(),
    rule:       (pmLines.find(l => l.startsWith("LESSON_RULE:"))       || "").replace("LESSON_RULE:", "").trim(),
    effect:     (pmLines.find(l => l.startsWith("LESSON_EFFECT:"))     || "").replace("LESSON_EFFECT:", "").trim(),
    errorClass: (pmLines.find(l => l.startsWith("ERROR_CLASS:"))       || "").replace("ERROR_CLASS:", "").trim(),
    rootCause:  (pmLines.find(l => l.startsWith("ROOT_CAUSE:"))        || "").replace("ROOT_CAUSE:", "").trim(),
  };
  const hasLesson = lesson.name && lesson.name.toUpperCase() !== "NONE";

  return (
    <div>
      {/* ── Result banner (prominent outcome + key numbers) ── */}
      <div style={{ display:"flex", alignItems:"center", gap:12, marginBottom:12, flexWrap:"wrap",
        padding:"10px 14px", borderRadius:8,
        background:outcomeStyle.bg, border:`1px solid ${outcomeStyle.border}`, borderLeft:`4px solid ${outcomeStyle.color}` }}>
        <div style={{ fontSize:13, fontWeight:900, color:outcomeStyle.color, letterSpacing:0.5, whiteSpace:"nowrap" }}>
          {outcomeStyle.label}
        </div>
        <div style={{ height:24, width:1, background:outcomeStyle.border }} />
        <div style={{ display:"flex", alignItems:"baseline", gap:6 }}>
          <span style={{ fontSize:18, fontWeight:900, color:isUp?C.green:isNeutral?C.amber:C.red }}>
            {isUp?"▲ UP":isNeutral?"— NEUTRAL":"▼ DOWN"}
          </span>
          <span style={{ fontSize:14, fontWeight:800, color:C.text }}>{row.confidence ?? "—"}%</span>
        </div>
        {resolved && (
          <div style={{ display:"flex", alignItems:"baseline", gap:6 }}>
            <span style={{ fontSize:10, fontWeight:700, color:C.muted, letterSpacing:1, textTransform:"uppercase" }}>Move</span>
            <span style={{ fontSize:14, fontWeight:900, color:movePct>=0?C.green:C.red }}>
              {movePct>=0?"+":""}{movePct.toFixed(3)}%
            </span>
            <span style={{ fontSize:11, color:C.muted }}>
              ${row.start_price?.toLocaleString(undefined,{maximumFractionDigits:2})}
              &nbsp;→&nbsp;
              ${row.end_price?.toLocaleString(undefined,{maximumFractionDigits:2})}
            </span>
          </div>
        )}
        <div style={{ marginLeft:"auto", display:"flex", gap:10, fontSize:9, color:C.muted }}>
          {row.latency_ms ? <span>{row.latency_ms}ms</span> : null}
          {snap && Object.keys(snap).length ? <span>{bullish}↑/{bearish}↓ strategies</span> : null}
        </div>
      </div>

      {/* ── LESSON callout — extracted from postmortem ── */}
      {hasLesson && (
        <div style={{ marginBottom:10, padding:"10px 14px", borderRadius:6,
          background:"#EEF2FF", border:"1px solid #C7D2FE", borderLeft:`4px solid ${C.indigo}` }}>
          <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:4, flexWrap:"wrap" }}>
            <span style={{ fontSize:9, fontWeight:800, color:C.indigo, letterSpacing:1.5, textTransform:"uppercase" }}>
              💡 Lesson — {lesson.name}
            </span>
            {lesson.errorClass && (
              <span style={{ fontSize:8, fontWeight:700, padding:"1px 6px", borderRadius:3,
                color:C.indigo, border:`1px solid ${C.indigo}`, background:C.surface }}>
                {lesson.errorClass}
              </span>
            )}
          </div>
          {lesson.rule   && <div style={{ fontSize:11, color:"#3730A3", lineHeight:1.6, marginBottom:3 }}><strong>Rule:</strong> {lesson.rule}</div>}
          {lesson.effect && <div style={{ fontSize:11, color:"#3730A3", lineHeight:1.6, marginBottom:3 }}><strong>Effect:</strong> {lesson.effect}</div>}
          {lesson.rootCause && <div style={{ fontSize:10, color:"#4338CA", lineHeight:1.5, fontStyle:"italic" }}>{lesson.rootCause}</div>}
        </div>
      )}

      {/* NEUTRAL outcome info */}
      {isNeutral && (
        <div style={{ background:C.amberBg, border:`1px solid ${C.amberBorder}`,
          borderLeft:`3px solid ${C.amber}`, borderRadius:5, padding:"6px 10px", marginBottom:8 }}>
          <div style={{ fontSize:8, fontWeight:700, color:C.amber, textTransform:"uppercase", letterSpacing:1, marginBottom:3 }}>
            Abstention Outcome
          </div>
          {row.actual_direction ? (
            <div style={{ fontSize:11, color:C.amber, lineHeight:1.75 }}>
              DeepSeek abstained. The market actually went{" "}
              <strong style={{ color: row.actual_direction==="UP" ? C.green : C.red }}>
                {row.actual_direction==="UP" ? "▲ UP" : "▼ DOWN"}
              </strong>.
              {" "}Committing to <strong>{row.actual_direction}</strong> would have been a{" "}
              <strong style={{ color:C.green }}>WIN</strong>.
              {" "}This signal is fed back to DeepSeek so it can learn whether abstaining was optimal.
            </div>
          ) : (
            <div style={{ fontSize:11, color:C.muted }}>Bar not yet resolved — outcome unknown.</div>
          )}
        </div>
      )}

      {/* Strategy snapshot */}
      {Object.keys(snap).length > 0 && (
        <div style={{ background:C.bg, border:`1px solid ${C.borderSoft}`, borderRadius:5, padding:"5px 8px", marginBottom:8 }}>
          <div style={{ display:"flex", alignItems:"center", gap:10, flexWrap:"wrap" }}>
            <span style={{ fontSize:9, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Strategies</span>
            <span style={{ fontSize:10, fontWeight:800, color:C.green }}>{bullish}↑</span>
            <span style={{ fontSize:10, fontWeight:800, color:C.red }}>{bearish}↓</span>
            <div style={{ marginLeft:"auto", display:"flex", gap:3, flexWrap:"wrap" }}>
              {Object.entries(snap).slice(0,13).map(([k,s]) => {
                const meta = STRATEGY_META.find(m => m.key === k);
                if (!s?.signal) return null;
                return (
                  <span key={k} style={{ fontSize:8, fontWeight:700, padding:"1px 4px", borderRadius:3,
                    color:s.signal==="UP"?C.green:C.red,
                    background:s.signal==="UP"?C.greenBg:C.redBg,
                    border:`1px solid ${s.signal==="UP"?C.greenBorder:C.redBorder}` }}>
                    {meta?.name||k} {s.signal==="UP"?"▲":"▼"}
                  </span>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* Chart */}
      {imgUrl && (
        <div style={{ marginBottom:8 }}>
          <div style={{ fontSize:9, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase", marginBottom:4 }}>
            Chart at prediction time · 30 min 1m candles
          </div>
          <img src={imgUrl} alt="BTC chart at prediction time"
            style={{ width:"100%", borderRadius:5, border:`1px solid ${C.borderSoft}`, display:"block", cursor:"pointer" }}
            onClick={() => window.open(imgUrl, "_blank")} />
        </div>
      )}

      {/* Analysis */}
      {reasons.length > 0 && (
        <div style={{ marginBottom:8 }}>
          <div style={{ fontSize:9, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase", marginBottom:5 }}>Analysis</div>
          {reasons.map((line, i) => (
            <div key={i} style={{ display:"flex", gap:8, marginBottom:i<reasons.length-1?6:0, alignItems:"flex-start" }}>
              <span style={{ fontSize:10, fontWeight:900, color:C.amber, minWidth:16, flexShrink:0 }}>{i+1}.</span>
              <span style={{ fontSize:11, color:C.textSec, lineHeight:1.6 }}><BoldAnalysis text={line} color={C.text} /></span>
            </div>
          ))}
        </div>
      )}

      {/* Narrative */}
      {row.narrative && (
        <div style={{ background:C.blueBg, border:`1px solid ${C.blueBorder}`,
          borderLeft:`3px solid ${C.blue}`, borderRadius:5, padding:"6px 10px", marginBottom:8 }}>
          <div style={{ fontSize:8, fontWeight:700, color:C.blue, textTransform:"uppercase", letterSpacing:1, marginBottom:3 }}>Price Narrative</div>
          <div style={{ fontSize:11, color:"#1E40AF", lineHeight:1.75, fontStyle:"italic" }}>{row.narrative}</div>
        </div>
      )}

      {/* Free observation */}
      {row.free_observation && (
        <div style={{ background:C.amberBg, border:`1px solid ${C.amberBorder}`,
          borderLeft:`3px solid ${C.amber}`, borderRadius:5, padding:"6px 10px", marginBottom:8 }}>
          <div style={{ fontSize:8, fontWeight:700, color:C.amber, textTransform:"uppercase", letterSpacing:1, marginBottom:3 }}>AI Free Observation</div>
          <div style={{ fontSize:11, color:C.amber, lineHeight:1.75 }}>{row.free_observation}</div>
        </div>
      )}

      {/* Postmortem */}
      {row.postmortem ? (
        <div style={{ background:"#F5F3FF", border:"1px solid #C4B5FD",
          borderLeft:"3px solid #7C3AED", borderRadius:5, padding:"6px 10px", marginBottom:8 }}>
          <div style={{ fontSize:8, fontWeight:700, color:"#7C3AED", textTransform:"uppercase", letterSpacing:1, marginBottom:5 }}>
            Post-Mortem · DeepSeek Self-Analysis
          </div>
          {row.postmortem.split("\n").filter(Boolean).map((line, i) => {
            const ci = line.indexOf(":");
            if (ci > 0 && ci < 25) {
              return (
                <div key={i} style={{ marginBottom:4, lineHeight:1.6 }}>
                  <span style={{ fontSize:9, fontWeight:900, color:"#7C3AED", textTransform:"uppercase", letterSpacing:0.5 }}>{line.slice(0,ci)}: </span>
                  <span style={{ fontSize:11, color:"#4C1D95" }}>{line.slice(ci+1).trim()}</span>
                </div>
              );
            }
            return <div key={i} style={{ fontSize:11, color:"#4C1D95", lineHeight:1.6, marginBottom:2 }}>{line}</div>;
          })}
        </div>
      ) : row.actual_direction && row.signal && row.signal !== "NEUTRAL" && (
        <div style={{ background:"#F5F3FF", border:"1px dashed #C4B5FD", borderRadius:5,
          padding:"5px 10px", marginBottom:8, display:"flex", alignItems:"center", gap:6 }}>
          <span style={{ fontSize:9, color:"#A78BFA" }}>⏳</span>
          <span style={{ fontSize:9, color:"#7C3AED", fontWeight:700 }}>Post-mortem pending — DeepSeek is analyzing this result in background</span>
        </div>
      )}

      {/* Data received / requested */}
      {(row.data_received || dataReq) && (
        <div style={{ display:"flex", gap:8, flexWrap:"wrap", marginBottom:8 }}>
          {row.data_received && (
            <div style={{ flex:1, background:C.blueBg, border:`1px solid ${C.blueBorder}`, borderRadius:4, padding:"4px 7px" }}>
              <div style={{ fontSize:8, fontWeight:700, color:C.blue, letterSpacing:1, textTransform:"uppercase", marginBottom:2 }}>AI confirmed data</div>
              <div style={{ fontSize:10, color:"#1E40AF" }}>{row.data_received}</div>
            </div>
          )}
          {dataReq && (
            <div style={{ flex:1, background:C.amberBg, border:`1px solid ${C.amberBorder}`, borderRadius:4, padding:"4px 7px" }}>
              <div style={{ fontSize:8, fontWeight:700, color:C.amber, letterSpacing:1, textTransform:"uppercase", marginBottom:2 }}>AI requested additional data</div>
              <div style={{ fontSize:10, color:C.amber }}>{dataReq}</div>
            </div>
          )}
        </div>
      )}

      {/* Prompt / Raw toggle buttons */}
      <div style={{ display:"flex", gap:5, borderTop:`1px solid ${C.borderSoft}`, paddingTop:6, flexWrap:"wrap", alignItems:"center" }}>
        <button onClick={() => toggle(`${ws}_prompt`)} style={{
          fontSize:9, fontWeight:700, padding:"2px 8px", borderRadius:4, cursor:"pointer",
          background:showPrompt?C.indigo:C.bg, color:showPrompt?"#fff":C.textSec,
          border:`1px solid ${showPrompt?C.indigo:C.border}`, fontFamily:"inherit" }}>
          {showPrompt?"▲ Full Prompt":"▼ Full Prompt"}
        </button>
        <button onClick={() => toggle(`${ws}_raw`)} style={{
          fontSize:9, fontWeight:700, padding:"2px 8px", borderRadius:4, cursor:"pointer",
          background:showRaw?C.indigo:C.bg, color:showRaw?"#fff":C.textSec,
          border:`1px solid ${showRaw?C.indigo:C.border}`, fontFamily:"inherit" }}>
          {showRaw?"▲ Raw Response":"▼ Raw Response"}
        </button>
      </div>

      {showPrompt && (
        <div style={{ marginTop:8 }}>
          <div style={{ fontSize:8, color:C.muted, marginBottom:3, fontWeight:700, textTransform:"uppercase", letterSpacing:1 }}>
            Full prompt sent to DeepSeek{row.full_prompt ? ` · ${row.full_prompt.length.toLocaleString()} chars` : ""}
          </div>
          {row.full_prompt
            ? <pre style={{ fontSize:9, color:C.textSec, background:C.bg, border:`1px solid ${C.borderSoft}`,
                borderRadius:5, padding:8, overflowX:"auto", whiteSpace:"pre-wrap", wordBreak:"break-word",
                maxHeight:400, overflowY:"auto", lineHeight:1.55 }}>{row.full_prompt}</pre>
            : <div style={{ fontSize:10, color:C.muted, fontStyle:"italic" }}>Not stored</div>}
        </div>
      )}
      {showRaw && (
        <div style={{ marginTop:8 }}>
          <div style={{ fontSize:8, color:C.muted, marginBottom:3, fontWeight:700, textTransform:"uppercase", letterSpacing:1 }}>
            Raw response from DeepSeek{row.raw_response ? ` · ${row.raw_response.length.toLocaleString()} chars` : ""}
          </div>
          {row.raw_response
            ? <pre style={{ fontSize:9, color:C.textSec, background:C.bg, border:`1px solid ${C.borderSoft}`,
                borderRadius:5, padding:8, overflowX:"auto", whiteSpace:"pre-wrap", wordBreak:"break-word",
                maxHeight:300, overflowY:"auto", lineHeight:1.55 }}>{row.raw_response}</pre>
            : <div style={{ fontSize:10, color:C.muted, fontStyle:"italic" }}>Not stored</div>}
        </div>
      )}
    </div>
  );
}

// ── Ensemble Accuracy + Weights + Orderbook Tab ───────────────
const TIER_STYLE = {
  LEARNING:  { bg:"#FFF7ED", border:"#FED7AA", color:"#C2410C" },
  DISABLED:  { bg:"#FFF1F2", border:"#FECDD3", color:"#B91C1C" },
  WEAK:      { bg:"#FFF1F2", border:"#FECDD3", color:"#B91C1C" },
  MARGINAL:  { bg:"#FFF7ED", border:"#FED7AA", color:"#C2410C" },
  RELIABLE:  { bg:"#F0FDF4", border:"#86EFAC", color:"#15803D" },
  EXCELLENT: { bg:"#F0FDF4", border:"#86EFAC", color:"#15803D" },
};

function AccuracyRow({ r, showWeight }) {
  const tc = TIER_STYLE[r.label] || TIER_STYLE.LEARNING;
  const accColor = r.total < 3 ? C.muted : r.accuracy >= 60 ? C.green : r.accuracy >= 50 ? C.textSec : C.red;
  return (
    <tr style={{ borderBottom:`1px solid ${C.borderSoft}` }}>
      <td style={{ ...td, fontWeight:700, color:C.text, maxWidth:110, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{r.name}</td>
      <td style={{ ...td, color:accColor, fontWeight:700, fontSize:12 }}>
        {r.total >= 3 ? `${r.accuracy.toFixed(1)}%` : "—"}
      </td>
      <td style={{ ...td, color:C.textSec, fontSize:10 }}>
        {r.total > 0 ? `${r.correct}/${r.total}` : "—"}
      </td>
      <td style={{ ...td }}>
        <span style={{ fontSize:8, fontWeight:700, padding:"1px 5px", borderRadius:3,
          background:tc.bg, border:`1px solid ${tc.border}`, color:tc.color }}>
          {r.label}
        </span>
      </td>
      {showWeight && (
        <td style={{ ...td, color:C.amber, fontWeight:700 }}>
          {r.weight != null ? `${r.weight.toFixed(2)}x` : "—"}
          {r.weight != null && (
            <div style={{ marginTop:2, height:3, background:C.borderSoft, borderRadius:2, overflow:"hidden", width:40 }}>
              <div style={{ width:`${Math.min(r.weight/3,1)*100}%`, height:"100%", background:C.amber, opacity:0.7 }} />
            </div>
          )}
        </td>
      )}
    </tr>
  );
}

function AccuracySection({ title, rows, showWeight, emptyMsg }) {
  if (!rows || rows.length === 0) return null;
  return (
    <div style={{ marginBottom:12 }}>
      <div style={{ ...label, marginBottom:4, paddingBottom:3, borderBottom:`1px solid ${C.border}` }}>{title}</div>
      <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11 }}>
        <thead>
          <tr style={{ borderBottom:`1px solid ${C.border}` }}>
            <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Name</th>
            <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Acc%</th>
            <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>W/Total</th>
            <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Tier</th>
            {showWeight && <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Weight</th>}
          </tr>
        </thead>
        <tbody>
          {rows.map(r => <AccuracyRow key={r.key} r={r} showWeight={showWeight} />)}
        </tbody>
      </table>
      {rows.length === 0 && <div style={{ fontSize:10, color:C.muted, padding:"6px 0" }}>{emptyMsg||"No data yet"}</div>}
    </div>
  );
}

function EnsembleTab({ weights, ob, ls, tk, oif, lq, fg, mp, cz, cg, dots, price, allAccuracy, allAccuracyErr, onRefreshAccuracy }) {
  // Build micro accuracy lookup for inline display: dash key → {accuracy, correct, total}
  const microAcc = {};
  (allAccuracy?.microstructure || []).forEach(r => {
    // strip "dash:" prefix to match micro row keys
    const shortKey = r.key.replace(/^dash:/, "");
    microAcc[shortKey] = r;
  });

  // Aggressive-precision BTC formatter for live microstructure values.
  // Never round a non-zero value to "0"; add decimal precision as the value
  // gets smaller so the trader can see there's a real reading even when it's
  // a fraction of a BTC.
  const btcFmt = (v) => {
    if (v == null || !isFinite(v)) return "—";
    if (v === 0) return "0";
    const a = Math.abs(v);
    if (a >= 1000)  return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
    if (a >= 10)    return v.toFixed(1);
    if (a >= 1)     return v.toFixed(2);
    if (a >= 0.01)  return v.toFixed(3);
    return v.toFixed(4);
  };
  const microRows = [
    { key:"order_book",  uiKey:"ob",  name:"Order Book",   dot:dots.ob,  signal:ob?.sig,
      kv: ob ? [["Bid",`${btcFmt(ob.bv)} BTC`],["Imb",`${ob.imb>=0?"+":""}${ob.imb?.toFixed(1)}%`],["Ask",`${btcFmt(ob.av)} BTC`]] : [] },
    { key:"long_short",  uiKey:"ls",  name:"Long/Short",   dot:dots.ls,  signal:ls?.rSig,
      kv: ls ? [["L/S",ls.lsr?.toFixed(3)],["Retail",`${ls.retailLong?.toFixed(0)}%L`],["Smart",`${ls.smartLong?.toFixed(0)}%L`],["Div",`${ls.div>=0?"+":""}${ls.div?.toFixed(1)}%`]] : [] },
    { key:"taker_flow",  uiKey:"tk",  name:"Taker Flow",   dot:dots.tk,  signal:tk?.sig,
      kv: tk ? [["BSR",tk.bsr?.toFixed(4)],["Buy",`${btcFmt(tk.bv)} BTC`],["Sell",`${btcFmt(tk.sv)} BTC`],["Trend",tk.trend]] : [] },
    { key:"oi_funding",  uiKey:"oif", name:"OI + Funding", dot:dots.oif, signal:oif?.frSig,
      kv: oif ? [["OI",`${btcFmt(oif.oi)} BTC`],["FR",`${(oif.fr*100)?.toFixed(4)}%`],["Prem",`${oif.premium?.toFixed(4)}%`],["Next",oif.nextFund]] : [] },
    { key:"liquidations",uiKey:"lq",  name:"Liquidations", dot:dots.lq,  signal:lq?.sig,
      kv: lq ? [["Total",lq.total],["Long",`${lq.longCount} ($${(lq.lvol||0).toLocaleString(undefined,{maximumFractionDigits:0})})`],["Short",`${lq.shortCount} ($${(lq.svol||0).toLocaleString(undefined,{maximumFractionDigits:0})})`]] : [] },
    { key:"fear_greed",  uiKey:"fg",  name:"Fear & Greed", dot:dots.fg,  signal:fg?.sig,
      kv: fg ? [["Index",`${fg.value} — ${fg.label}`],["Prev",fg.prev],["Δ",`${fg.delta>=0?"+":""}${fg.delta}`]] : [] },
    { key:"mempool",     uiKey:"mp",  name:"Mempool",      dot:dots.mp,  signal:mp?.sig,
      kv: mp ? [["Fast",`${mp.fastest} sat/vB`],["Half",`${mp.halfHour} sat/vB`],["Pending",mp.count?.toLocaleString()]] : [] },
    { key:"coinalyze",   uiKey:"cz",  name:"Coinalyze",    dot:dots.cz,  signal:cz?.sig,
      kv: cz ? [["X-ex FR",`${(cz.fr*100)?.toFixed(4)}%`]] : [] },
    { key:"coingecko",   uiKey:"cg",  name:"CoinGecko",    dot:dots.cg,  signal:null,
      kv: cg ? [["24h Δ",`${cg.ch>=0?"+":""}${cg.ch?.toFixed(2)}%`],["Vol/MCap",`${cg.vm?.toFixed(2)}%`]] : [] },
  ];

  const hasAccuracy = allAccuracy && (
    (allAccuracy.ai?.length || allAccuracy.strategies?.length ||
     allAccuracy.specialists?.length || allAccuracy.microstructure?.length)
  );

  return (
    <div style={{ display:"flex", gap:8, height:"100%", overflow:"hidden" }}>

      {/* LEFT: comprehensive accuracy table */}
      <div style={{ flex:"0 0 54%", overflowY:"auto", paddingRight:2 }}>
        <div style={{ ...card, borderLeft:`3px solid ${C.amber}` }}>
          <div style={{ marginBottom:10 }}>
            <div style={{ ...label, fontSize:10 }}>Prediction Accuracy — All Sources</div>
          </div>

          {!hasAccuracy ? (
            <div style={{ color: allAccuracyErr ? C.red : C.muted, fontSize:10, padding:"12px 0", display:"flex", alignItems:"center", gap:8 }}>
              {allAccuracyErr
                ? <>Failed to load accuracy data — <span style={{cursor:"pointer",textDecoration:"underline"}} onClick={onRefreshAccuracy}>retry</span></>
                : allAccuracy
                  ? "No accuracy data yet — predictions are still accumulating…"
                  : "Loading accuracy data…"}
            </div>
          ) : (() => {
            // Flatten all categories into one list with a Category column, sorted by accuracy desc
            const CAT_LABEL = { ai:"AI", strategies:"Strategy", specialists:"Specialist", microstructure:"Micro" };
            const allRows = [];
            for (const [cat, rows] of Object.entries(allAccuracy)) {
              if (!Array.isArray(rows)) continue;
              rows.forEach(r => allRows.push({ ...r, cat: CAT_LABEL[cat] || cat }));
            }
            allRows.sort((a, b) => {
              // Rows with <3 total go to bottom; among the rest sort by accuracy desc, ties by total desc
              const aQ = a.total >= 3, bQ = b.total >= 3;
              if (aQ !== bQ) return aQ ? -1 : 1;
              if (b.accuracy !== a.accuracy) return b.accuracy - a.accuracy;
              return b.total - a.total;
            });
            const bestAcc = allRows.find(r => r.total >= 3)?.accuracy ?? null;
            return (
              <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11 }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${C.border}` }}>
                    <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase", width:18 }}>#</th>
                    <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Name</th>
                    <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Cat</th>
                    <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Acc%</th>
                    <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>W/Total</th>
                    <th style={{ ...td, fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }}>Tier</th>
                  </tr>
                </thead>
                <tbody>
                  {allRows.map((r, i) => {
                    const tc = TIER_STYLE[r.label] || TIER_STYLE.LEARNING;
                    const accColor = r.total < 3 ? C.muted : r.accuracy >= 60 ? C.green : r.accuracy >= 50 ? C.textSec : C.red;
                    const isBest = bestAcc !== null && r.accuracy === bestAcc && r.total >= 3;
                    return (
                      <tr key={r.key} style={{ borderBottom:`1px solid ${C.borderSoft}`, background: isBest ? "#FFFBEB" : "transparent" }}>
                        <td style={{ ...td, color:C.muted, fontSize:9 }}>
                          {isBest ? <span style={{ color:C.amber, fontWeight:900 }}>★</span> : i + 1}
                        </td>
                        <td style={{ ...td, fontWeight:700, color:C.text, maxWidth:110, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{r.name}</td>
                        <td style={{ ...td, fontSize:9, color:C.muted }}>{r.cat}</td>
                        <td style={{ ...td, color:accColor, fontWeight:700, fontSize:12 }}>
                          {r.total >= 3 ? `${r.accuracy.toFixed(1)}%` : "—"}
                        </td>
                        <td style={{ ...td, color:C.textSec, fontSize:10 }}>
                          {r.total > 0 ? `${r.correct}/${r.total}` : "—"}
                        </td>
                        <td style={{ ...td }}>
                          <span style={{ fontSize:8, fontWeight:700, padding:"1px 5px", borderRadius:3,
                            background:tc.bg, border:`1px solid ${tc.border}`, color:tc.color }}>
                            {r.label}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            );
          })()}
        </div>
      </div>

      {/* RIGHT: microstructure live data with accuracy inline */}
      <div style={{ flex:1, display:"flex", flexDirection:"column", gap:5, overflowY:"auto" }}>
        <div style={label}>Live Microstructure Signals</div>
        {microRows.map(r => {
          const sc = r.signal ? sigColors(r.signal) : null;
          const acc = microAcc[r.key];
          const accColor = !acc || acc.total < 3 ? C.muted : acc.accuracy >= 60 ? C.green : acc.accuracy >= 50 ? C.textSec : C.red;
          return (
            <div key={r.key} style={{ ...card, flexShrink:0,
              ...(r.dot==="err" ? { border:`1px solid ${C.red}88`, background:"#2a0a0a" } : {}) }}>
              <div style={{ display:"flex", alignItems:"center", gap:6, marginBottom:3 }}>
                <span style={{ fontSize:10, fontWeight:700, color:r.dot==="err"?C.red:C.text, minWidth:90 }}>{r.name}</span>
                {sc && (
                  <span style={{ fontSize:8, fontWeight:700, padding:"1px 6px", borderRadius:3,
                    background:sc.bg, border:`1px solid ${sc.border}`, color:sc.color }}>
                    {r.signal}
                  </span>
                )}
                {r.dot==="err" && (
                  <span style={{ fontSize:8, fontWeight:700, padding:"1px 6px", borderRadius:3,
                    background:"#3a0a0a", border:`1px solid ${C.red}`, color:C.red }}>
                    ✕ NO DATA
                  </span>
                )}
                {acc && acc.total >= 3 && r.dot !== "err" && (
                  <span style={{ fontSize:9, fontWeight:700, color:accColor, marginLeft:2 }}>
                    {acc.accuracy.toFixed(0)}% <span style={{ color:C.muted, fontWeight:400 }}>({acc.correct}/{acc.total})</span>
                  </span>
                )}
                <span style={{ marginLeft:"auto", fontSize:8,
                  color:r.dot==="live"?C.green:r.dot==="err"?C.red:C.muted }}>
                  {r.dot==="live"?"● live":r.dot==="err"?"✕ err":"○ —"}
                </span>
              </div>
              {r.kv.length > 0 ? (
                <div style={{ display:"flex", gap:10, flexWrap:"wrap" }}>
                  {r.kv.map(([k,v])=>(
                    <span key={k} style={{ fontSize:10 }}>
                      <span style={{ color:C.muted }}>{k} </span>
                      <strong style={{ color:C.text }}>{v}</strong>
                    </span>
                  ))}
                </div>
              ) : (
                <div style={{ fontSize:10, color:C.muted }}>Loading…</div>
              )}
            </div>
          );
        })}
      </div>

    </div>
  );
}

// ── Backend Tab ───────────────────────────────────────────────
const MICRO_SRC_DEFS = [
  ["order_book",           "Order Book Depth",   "5-venue · 0.5% band"],
  ["long_short",           "Long/Short Ratio",   "Binance Futures · accounts"],
  ["taker_flow",           "Taker Flow",         "Binance-perp aggressor · 5m"],
  ["oi_funding",           "OI + Funding",       "Binance Futures"],
  ["coinalyze_aggregate",  "Aggregate OI + Liqs","Coinalyze · 7-venue"],
  ["spot_perp_basis",      "Spot-Perp Basis",    "Binance spot vs perp mark"],
  ["cvd",                  "CVD (1h)",           "Binance spot+perp delta"],
  ["liquidations",         "Liquidations",       "OKX cross · last 5m"],
  ["bybit_liquidations",   "OKX Isolated Liqs",  "OKX isolated-margin"],
  ["fear_greed",           "Fear & Greed",       "Alternative.me · daily (macro)"],
  ["mempool",              "Mempool",            "mempool.space"],
  ["coingecko",            "CoinGecko",          "24h market data (macro)"],
  ["btc_dominance",        "BTC Dominance",      "CoinGecko global (macro)"],
  ["deribit_dvol",         "DVOL",               "Deribit 30d implied vol"],
  ["deribit_skew_term",    "Skew + Term + P/C",  "Deribit 25Δ / 7-30-90d"],
  ["kraken_premium",       "Kraken Premium",     "Kraken vs OKX spread"],
  ["oi_velocity",          "OI Velocity",        "Binance OI hist · 30m"],
  ["spot_whale_flow",      "Spot Whale Flow",    "3-venue aggTrades ≥0.5 BTC · 5m"],
  ["okx_funding",          "OKX Funding",        "OKX funding rate"],
  ["top_position_ratio",   "Top-Account Ratio",  "Binance top-20% by margin"],
  ["funding_trend",        "Funding Trend",      "Binance 6-period avg"],
  ["coinalyze",            "Coinalyze Funding",  "Cross-ex funding"],
];

function microSignalKey(key, d) {
  if (!d) return null;
  if (key === "long_short") return d.retail_signal_contrarian || d.signal;
  if (key === "oi_funding") return d.funding_signal || d.signal;
  return d.signal || null;
}

function microKV(key, d) {
  if (!d) return [];
  if (key === "order_book")         return [["Bid 0.5%",`${d.bid_depth_05pct_btc?.toFixed(0) ?? d.bid_vol_btc?.toFixed(0)} BTC`],["Imb",`${(d.imbalance_05pct_pct ?? d.imbalance_pct)>=0?"+":""}${(d.imbalance_05pct_pct ?? d.imbalance_pct)?.toFixed(1)}%`],["Ask 0.5%",`${d.ask_depth_05pct_btc?.toFixed(0) ?? d.ask_vol_btc?.toFixed(0)} BTC`],["Venues",`${d.venues_included?.length ?? 1}`]];
  if (key === "long_short")         return [["L/S",d.retail_lsr?.toFixed(3)],["All",`${d.retail_long_pct?.toFixed(0)}%L`],["Top 20%",`${(d.top_accounts_long_pct ?? d.smart_money_long_pct)?.toFixed(0)}%L`],["Δ",`${((d.top_vs_all_div_pct ?? d.smart_vs_retail_div_pct)>=0?"+":"")}${(d.top_vs_all_div_pct ?? d.smart_vs_retail_div_pct)?.toFixed(1)}%`]];
  if (key === "taker_flow")         return [["BSR",d.buy_sell_ratio?.toFixed(4)],["Buy",`${d.taker_buy_vol_btc?.toFixed(0)} BTC`],["Sell",`${d.taker_sell_vol_btc?.toFixed(0)} BTC`],["3-bar",d.trend_3bars]];
  if (key === "oi_funding")         return [["OI",`${d.open_interest_btc?.toFixed(0)} BTC`],["FR",`${(d.funding_rate_8h_pct??0).toFixed(4)}%`],["Prem",`${d.mark_premium_vs_index_pct?.toFixed(4)}%`]];
  if (key === "liquidations")       return [["Total",d.total],["Longs",`${d.long_liq_count} ($${(d.long_liq_usd??0).toLocaleString()})`],["Shorts",`${d.short_liq_count} ($${(d.short_liq_usd??0).toLocaleString()})`],["Vel",`${(d.velocity_per_min??0).toFixed(1)}/min`]];
  if (key === "bybit_liquidations") return [["Total",d.total],["Long $",`$${(d.long_liq_usd??0).toLocaleString()}`],["Short $",`$${(d.short_liq_usd??0).toLocaleString()}`]];
  if (key === "fear_greed")         return [["Index",`${d.value} — ${d.label}`],["Prev",d.previous_day],["Δ",`${d.daily_delta>=0?"+":""}${d.daily_delta}`]];
  if (key === "mempool")            return [["Fast",`${d.fastest_fee_sat_vb} sat/vB`],["Pending",`${d.pending_tx_count?.toLocaleString()} tx`],["Size",`${d.mempool_size_mb} MB`]];
  if (key === "coingecko")          return [["24h Δ",`${d.change_24h_pct>=0?"+":""}${d.change_24h_pct?.toFixed(2)}%`],["Vol/MCap",`${d.vol_to_mcap_ratio_pct?.toFixed(2)}%`]];
  if (key === "btc_dominance")      return [["Dom",`${d.btc_dominance_pct?.toFixed(1)}%`],["Mkt 24h",`${d.market_change_24h_pct>=0?"+":""}${d.market_change_24h_pct?.toFixed(2)}%`]];
  if (key === "deribit_dvol")       return [["DVOL",`${d.dvol_pct?.toFixed(1)}%`]];
  if (key === "kraken_premium")     return [["Kraken",`$${d.kraken_price?.toLocaleString()}`],["OKX",`$${d.okx_price?.toLocaleString()}`],["Spread",`${(d.spread_pct??0).toFixed(4)}%`]];
  if (key === "oi_velocity")        return [["30m Δ",`${(d.oi_change_30m_pct??0)>=0?"+":""}${(d.oi_change_30m_pct??0).toFixed(3)}%`],["1bar Δ",`${(d.oi_change_1bar_pct??0)>=0?"+":""}${(d.oi_change_1bar_pct??0).toFixed(3)}%`]];
  if (key === "spot_whale_flow")    return [["Buy",`${d.whale_buy_btc?.toFixed(0)} BTC`],["Sell",`${d.whale_sell_btc?.toFixed(0)} BTC`],["Buy%",`${d.whale_buy_pct?.toFixed(0)}%`]];
  if (key === "okx_funding")        return [["FR",`${(d.funding_rate_pct??0).toFixed(4)}%`]];
  if (key === "top_position_ratio") return [["L/S",d.long_short_ratio?.toFixed(3)],["Long%",`${d.long_position_pct?.toFixed(0)}%`]];
  if (key === "funding_trend")      return [["Latest",`${(d.funding_latest_pct??0).toFixed(4)}%`],["6p avg",`${(d.funding_avg_6p_pct??0).toFixed(4)}%`],["Trend",d.funding_trend]];
  if (key === "coinalyze")          return [["X-ex FR",`${(d.funding_rate_8h_pct??0).toFixed(4)}%`]];
  if (key === "coinalyze_aggregate")return [["OI",`$${((d.agg_oi_usd??0)/1e9).toFixed(2)}B`],["Long L$",`$${(d.agg_long_liq_usd_5m??0).toLocaleString()}`],["Short L$",`$${(d.agg_short_liq_usd_5m??0).toLocaleString()}`],["Venues",`${d.agg_oi_venues_count??0}`]];
  if (key === "spot_perp_basis")    return [["Basis",`${(d.basis_pct??0)>=0?"+":""}${(d.basis_pct??0).toFixed(3)}%`],["USD",`${(d.basis_usd??0)>=0?"+":""}${(d.basis_usd??0).toFixed(1)}`]];
  if (key === "cvd")                return [["Agg CVD",`${(d.aggregate_cvd_1h_btc??0)>=0?"+":""}${(d.aggregate_cvd_1h_btc??0).toFixed(0)} BTC`],["Perp",`${(d.perp_cvd_1h_btc??0)>=0?"+":""}${(d.perp_cvd_1h_btc??0).toFixed(0)}`],["Spot",`${(d.spot_cvd_1h_btc??0)>=0?"+":""}${(d.spot_cvd_1h_btc??0).toFixed(0)}`],["Div",`${(d.spot_perp_divergence_btc??0)>=0?"+":""}${(d.spot_perp_divergence_btc??0).toFixed(0)}`]];
  if (key === "deribit_skew_term")  return [["25ΔRR",d.rr_25d_30d_pct!=null?`${d.rr_25d_30d_pct>=0?"+":""}${d.rr_25d_30d_pct.toFixed(1)}%`:"n/a"],["IV30d",`${(d.iv_30d_atm_pct??0).toFixed(1)}%`],["Term",d.term_inverted?"INV":d.term_contango?"CON":"flat"],["P/C vol",(d.put_call_volume_ratio??0).toFixed(2)]];
  return [];
}

const FEAT_GROUPS = [
  { label:"Returns",  keys:[{k:"return_1",n:"1m"},{k:"return_2",n:"2m"},{k:"return_5",n:"5m"},{k:"return_10",n:"10m"},{k:"return_15",n:"15m"},{k:"return_30",n:"30m"}],
    fmt:(k,v)=>`${v>=0?"+":""}${v.toFixed(3)}%`, color:(k,v)=>v>0?C.green:v<0?C.red:C.muted },
  { label:"RSI(4)",   keys:[{k:"rsi_4",n:"RSI 4"}],
    fmt:(k,v)=>v.toFixed(1), color:(k,v)=>v>80?C.red:v<20?C.green:C.textSec },
  { label:"Stoch K(5)", keys:[{k:"stoch_k_5",n:"K(5)"}],
    fmt:(k,v)=>v.toFixed(1), color:(k,v)=>v>80?C.red:v<20?C.green:C.textSec },
  { label:"MACD",     keys:[{k:"macd",n:"MACD"},{k:"macd_signal",n:"Signal"},{k:"macd_histogram",n:"Hist"}],
    fmt:(k,v)=>v.toFixed(4), color:(k,v)=>v>0?C.green:C.red },
  { label:"Bollinger",keys:[{k:"bollinger_pct_b",n:"%B"},{k:"bollinger_width",n:"Width"}],
    fmt:(k,v)=>v.toFixed(4), color:(k,v)=>k==="bollinger_pct_b"?(v>0.8?C.red:v<0.2?C.green:C.textSec):C.textSec },
  { label:"Vol/VWAP", keys:[{k:"vwap_ref",n:"VWAP"},{k:"price_vs_vwap",n:"Δ%"}],
    fmt:(k,v)=>k==="vwap_ref"?`$${v.toFixed(2)}`:`${v>=0?"+":""}${v.toFixed(3)}%`, color:(k,v)=>k==="price_vs_vwap"?(v>0?C.green:v<0?C.red:C.muted):C.textSec },
];
const FEAT_KNOWN = new Set(FEAT_GROUPS.flatMap(g=>g.keys.map(x=>x.k)));

function BackendTab({ backendSnap, deepseekPred }) {
  const snap = backendSnap?.snapshot || {};
  const features = snap.features || {};
  const stratPreds = snap.strategy_preds || {};
  const dashSigs = snap.dashboard_signals || {};
  const ensRes = snap.ensemble_result || {};
  const ds = deepseekPred || {};

  const stratKeys = Object.keys(stratPreds);
  const bullCount = stratKeys.filter(k => stratPreds[k]?.signal === "UP").length;
  const bearCount = stratKeys.filter(k => stratPreds[k]?.signal === "DOWN").length;
  const hasDash = Object.keys(dashSigs).some(k => k !== "fetched_at" && dashSigs[k]);

  if (!backendSnap && !ds.full_prompt) {
    return (
      <div style={{ display:"flex", alignItems:"center", justifyContent:"center", height:"100%", flexDirection:"column", gap:8 }}>
        <div style={{ fontSize:24, color:C.muted }}>⏳</div>
        <div style={{ fontSize:12, color:C.muted }}>No snapshot yet — waiting for the first prediction window to complete…</div>
      </div>
    );
  }

  return (
    <div style={{ display:"flex", gap:6, height:"100%", overflow:"hidden" }}>

      {/* ── LEFT: Data Pipeline ── */}
      <div style={{ flex:"0 0 42%", overflowY:"auto", display:"flex", flexDirection:"column", gap:5, paddingRight:2 }}>

        {/* Window metadata */}
        <div style={{ ...card, flexShrink:0, borderLeft:`3px solid ${C.amber}` }}>
          <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
            <div style={{ ...label }}>Data Pipeline</div>
            {snap.window_num != null && (
              <span style={{ fontSize:9, fontWeight:700, color:C.amber, background:C.amberBg,
                border:`1px solid ${C.amberBorder}`, borderRadius:3, padding:"1px 6px" }}>
                Window #{snap.window_num}
              </span>
            )}
            {snap.captured_at && (
              <span style={{ fontSize:9, color:C.muted, marginLeft:"auto" }}>
                captured {new Date(snap.captured_at*1000).toLocaleTimeString()}
              </span>
            )}
          </div>
          <div style={{ display:"flex", gap:12, flexWrap:"wrap" }}>
            {snap.window_start_price && (
              <div>
                <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase", fontWeight:700 }}>Start Price</div>
                <div style={{ fontSize:18, fontWeight:900, color:C.text }}>${snap.window_start_price?.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
              </div>
            )}
            {snap.rolling_acc != null && (
              <div>
                <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase", fontWeight:700 }}>Rolling Acc</div>
                <div style={{ fontSize:18, fontWeight:900, color:snap.rolling_acc>=0.5?C.green:C.red }}>{(snap.rolling_acc*100).toFixed(1)}%</div>
              </div>
            )}
            {snap.ds_acc?.total > 0 && (
              <div>
                <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase", fontWeight:700 }}>DS Acc</div>
                <div style={{ fontSize:18, fontWeight:900, color:snap.ds_acc.accuracy>=0.5?C.green:C.red }}>{(snap.ds_acc.accuracy*100).toFixed(1)}%</div>
              </div>
            )}
          </div>
          {snap.prices_last20?.length > 0 && (
            <div style={{ marginTop:6 }}>
              <div style={{ fontSize:8, color:C.muted, fontWeight:700, textTransform:"uppercase", marginBottom:3 }}>Last 20 Prices</div>
              <div style={{ display:"flex", gap:1, height:24, alignItems:"flex-end" }}>
                {snap.prices_last20.map((p,i,arr) => {
                  const mn=Math.min(...arr), mx=Math.max(...arr), range=mx-mn||1;
                  const h=Math.max(2,Math.round(((p-mn)/range)*22));
                  const isLast=i===arr.length-1;
                  return <div key={i} style={{ flex:1, height:`${h}px`, borderRadius:1,
                    background:isLast?C.amber:p>=arr[Math.max(0,i-1)]?C.green:C.red, opacity:isLast?1:0.7 }} />;
                })}
              </div>
            </div>
          )}
        </div>

        {/* Technical Features */}
        {Object.keys(features).length > 0 && (
          <div style={{ ...card, flexShrink:0 }}>
            <div style={{ ...label, marginBottom:6 }}>Technical Indicators</div>
            <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
              {FEAT_GROUPS.map(g => {
                const items = g.keys.filter(({k}) => features[k] !== undefined);
                if (!items.length) return null;
                return (
                  <div key={g.label}>
                    <div style={{ fontSize:8, color:C.muted, fontWeight:700, textTransform:"uppercase", marginBottom:3 }}>{g.label}</div>
                    <div style={{ display:"flex", gap:4, flexWrap:"wrap" }}>
                      {items.map(({k,n}) => (
                        <div key={k} style={{ background:C.bg, border:`1px solid ${C.borderSoft}`, borderRadius:4, padding:"3px 8px", minWidth:52, textAlign:"center" }}>
                          <div style={{ fontSize:7, color:C.muted, textTransform:"uppercase", letterSpacing:0.5 }}>{n}</div>
                          <div style={{ fontSize:11, fontWeight:800, color:g.color(k,features[k]), marginTop:1 }}>{g.fmt(k,features[k])}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
              {/* Other features */}
              {(() => {
                const others = Object.keys(features).filter(k=>!FEAT_KNOWN.has(k)).slice(0,24);
                if (!others.length) return null;
                return (
                  <div>
                    <div style={{ fontSize:8, color:C.muted, fontWeight:700, textTransform:"uppercase", marginBottom:3 }}>Other</div>
                    <div style={{ display:"flex", gap:4, flexWrap:"wrap" }}>
                      {others.map(k => (
                        <div key={k} style={{ background:C.bg, border:`1px solid ${C.borderSoft}`, borderRadius:4, padding:"3px 7px", textAlign:"center" }}>
                          <div style={{ fontSize:7, color:C.muted, textTransform:"uppercase", maxWidth:72, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{k.replace(/_/g," ")}</div>
                          <div style={{ fontSize:11, fontWeight:800, color:features[k]>0?C.green:features[k]<0?C.red:C.textSec }}>{typeof features[k]==="number"?features[k].toFixed(3):"—"}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })()}
            </div>
          </div>
        )}

        {/* Strategy Votes */}
        {stratKeys.length > 0 && (
          <div style={{ ...card, flexShrink:0 }}>
            <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
              <div style={{ ...label }}>Strategy Votes</div>
              <span style={{ fontSize:11, fontWeight:800, color:C.green }}>{bullCount}↑</span>
              <span style={{ fontSize:11, fontWeight:800, color:C.red }}>{bearCount}↓</span>
              <div style={{ marginLeft:"auto", display:"flex", gap:3 }}>
                <div style={{ height:6, borderRadius:3, background:C.green, width:`${bullCount/(bullCount+bearCount||1)*60}px`, transition:"width 0.3s" }} />
                <div style={{ height:6, borderRadius:3, background:C.red,   width:`${bearCount/(bullCount+bearCount||1)*60}px`, transition:"width 0.3s" }} />
              </div>
            </div>
            <div style={{ display:"flex", flexDirection:"column", gap:3 }}>
              {STRATEGY_META.filter(m=>stratPreds[m.key]).map(m => {
                const s = stratPreds[m.key];
                if (!s) return null;
                const isUp = s.signal === "UP";
                return (
                  <div key={m.key} style={{ display:"flex", alignItems:"center", gap:6 }}>
                    <span style={{ fontSize:9, fontWeight:700, color:m.color, minWidth:68 }}>{m.name}</span>
                    <span style={{ fontSize:9, fontWeight:800, padding:"1px 6px", borderRadius:3,
                      background:isUp?C.greenBg:C.redBg, color:isUp?C.green:C.red,
                      border:`1px solid ${isUp?C.greenBorder:C.redBorder}` }}>
                      {isUp?"▲ UP":"▼ DOWN"}
                    </span>
                    {s.confidence != null && (
                      <div style={{ flex:1, height:3, background:C.borderSoft, borderRadius:2, overflow:"hidden" }}>
                        <div style={{ width:`${Math.min((s.confidence??0.5)*100,100)}%`, height:"100%", background:isUp?C.green:C.red, borderRadius:2 }} />
                      </div>
                    )}
                    {s.value != null && (
                      <span style={{ fontSize:8, color:C.muted, minWidth:40, textAlign:"right", fontFamily:"inherit" }}>
                        {typeof s.value==="number"?s.value.toFixed(2):String(s.value).slice(0,8)}
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Microstructure signals */}
        {hasDash && (
          <div style={{ ...card, flexShrink:0 }}>
            <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
              <div style={{ ...label }}>Microstructure · embedding inputs</div>
              {(() => { const n = MICRO_SRC_DEFS.filter(([k])=>dashSigs[k]).length; const tot = MICRO_SRC_DEFS.length; return (
                <span style={{ fontSize:9, fontWeight:700, padding:"1px 6px", borderRadius:3,
                  background:n>=14?C.greenBg:n>=8?C.amberBg:C.redBg,
                  color:n>=14?C.green:n>=8?C.amber:C.red,
                  border:`1px solid ${n>=14?C.greenBorder:n>=8?C.amberBorder:C.redBorder}` }}>
                  {n}/{tot} live
                </span>
              ); })()}
            </div>
            <div style={{ display:"flex", flexDirection:"column", gap:5 }}>
              {MICRO_SRC_DEFS.map(([key, name, src]) => {
                const d = dashSigs[key];
                const sig = microSignalKey(key, d);
                const kvs = microKV(key, d);
                const sc = sig ? sigColors(sig) : null;
                return (
                  <div key={key} style={{ background:C.bg, border:`1px solid ${d?C.borderSoft:"#e5e7eb"}`,
                    borderRadius:5, padding:"5px 8px",
                    borderLeft:`2px solid ${d?(sc?.border||C.border):C.borderSoft}` }}>
                    <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                      <div style={{ flex:1 }}>
                        <div style={{ fontSize:9, fontWeight:700, color:C.textSec }}>{name}</div>
                        <div style={{ fontSize:7, color:C.muted }}>{src}</div>
                      </div>
                      {sc && (
                        <span style={{ fontSize:8, fontWeight:700, padding:"1px 5px", borderRadius:3,
                          background:sc.bg, color:sc.color, border:`1px solid ${sc.border}`, flexShrink:0 }}>
                          {sig.replace(/_CONTRARIAN|_ARBI/,"")}
                        </span>
                      )}
                      <div style={{ width:6, height:6, borderRadius:"50%", flexShrink:0,
                        background:d?"#22c55e":"#d1d5db",
                        boxShadow:d?"0 0 4px #22c55e55":"none" }} />
                    </div>
                    {d && kvs.length > 0 && (
                      <div style={{ display:"flex", gap:10, flexWrap:"wrap", marginTop:3 }}>
                        {kvs.map(([lbl,val]) => (
                          <span key={lbl} style={{ fontSize:9 }}>
                            <span style={{ color:C.muted }}>{lbl}: </span>
                            <span style={{ color:C.textSec, fontWeight:700 }}>{val}</span>
                          </span>
                        ))}
                      </div>
                    )}
                    {d?.interpretation && (
                      <div style={{ fontSize:8, color:C.muted, marginTop:3, lineHeight:1.4, fontStyle:"italic" }}>{d.interpretation}</div>
                    )}
                    {!d && <div style={{ fontSize:8, color:C.muted, marginTop:2 }}>Not available</div>}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Ensemble Vote */}
        <div style={{ display:"flex", gap:5, flexShrink:0 }}>
          {ensRes.signal && (
            <div style={{ ...card, flex:1, borderLeft:`3px solid ${ensRes.signal==="UP"?C.green:ensRes.signal==="NEUTRAL"?C.amber:C.red}` }}>
              <div style={{ ...label, marginBottom:4 }}>Ensemble Vote</div>
              <div style={{ fontSize:20, fontWeight:900, color:ensRes.signal==="UP"?C.green:ensRes.signal==="NEUTRAL"?C.amber:C.red }}>
                {ensRes.signal==="UP"?"▲ UP":ensRes.signal==="NEUTRAL"?"— NEUTRAL":"▼ DOWN"}
              </div>
              <div style={{ fontSize:14, fontWeight:800, color:C.text }}>{((ensRes.confidence||0)*100).toFixed(1)}%</div>
              <div style={{ height:3, background:C.borderSoft, borderRadius:2, margin:"4px 0" }}>
                <div style={{ width:`${(ensRes.confidence||0)*100}%`, height:"100%", background:ensRes.signal==="UP"?C.green:ensRes.signal==="NEUTRAL"?C.amber:C.red }} />
              </div>
              <div style={{ fontSize:9, color:C.muted }}>{ensRes.bullish_count}↑ {ensRes.bearish_count}↓</div>
            </div>
          )}
        </div>
      </div>

      {/* ── RIGHT: DeepSeek I/O ── */}
      <div style={{ flex:1, overflowY:"auto", display:"flex", flexDirection:"column", gap:5, paddingLeft:2 }}>

        {/* Result header */}
        <div style={{ ...card, flexShrink:0, borderLeft:`3px solid ${ds.signal==="UP"?C.green:ds.signal==="DOWN"?C.red:C.muted}` }}>
          <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
            <div style={{ ...label }}>DeepSeek I/O</div>
            {ds.window_count && (
              <span style={{ fontSize:9, fontWeight:700, color:C.indigo, background:C.blueBg,
                border:`1px solid ${C.blueBorder}`, borderRadius:3, padding:"1px 6px" }}>
                Window #{ds.window_count}
              </span>
            )}
            {ds.latency_ms && <span style={{ fontSize:9, color:C.muted }}>{ds.latency_ms}ms</span>}
            {ds.window_start && (
              <span style={{ fontSize:9, color:C.muted, marginLeft:"auto" }}>
                {ds.window_start} → {ds.window_end}
              </span>
            )}
          </div>
          {ds.signal && ds.signal !== "ERROR" ? (
            <div style={{ display:"flex", gap:12, alignItems:"center", flexWrap:"wrap" }}>
              <div style={{ fontSize:26, fontWeight:900, color:ds.signal==="UP"?C.green:ds.signal==="NEUTRAL"?C.amber:C.red }}>
                {ds.signal==="UP"?"▲ UP":ds.signal==="NEUTRAL"?"— NEUTRAL":"▼ DOWN"}
              </div>
              {(() => {
                const _sr = parseStructuredReasoning(ds.reasoning || "");
                const _isNeutral = ds.signal === "NEUTRAL";
                let _lbl, _bg, _clr, _bdr;
                if (_isNeutral) { _lbl="NEUTRAL"; _bg=C.amberBg; _clr=C.amber; _bdr=C.amberBorder; }
                else if (_sr.isStructured) {
                  const _ok = _sr.survives === "YES";
                  _lbl=_ok?"TAKE":"PASS"; _bg=_ok?C.greenBg:C.amberBg; _clr=_ok?C.green:C.amber; _bdr=_ok?C.greenBorder:C.amberBorder;
                } else { _lbl="TAKE"; _bg=C.greenBg; _clr=C.green; _bdr=C.greenBorder; }
                return (
                  <div style={{ display:"flex", alignItems:"center", gap:8 }}>
                    <span style={{ fontSize:13, fontWeight:900, padding:"3px 10px", borderRadius:4,
                      color:_clr, background:_bg, border:`1px solid ${_bdr}`, letterSpacing:0.8 }}>{_lbl}</span>
                    {ds.confidence != null && (
                      <span style={{ fontSize:9, color:C.muted, fontVariantNumeric:"tabular-nums" }}>{ds.confidence}%</span>
                    )}
                  </div>
                );
              })()}
              {ds.data_received && (
                <div style={{ flex:1, minWidth:180, background:C.blueBg, border:`1px solid ${C.blueBorder}`, borderRadius:4, padding:"4px 8px" }}>
                  <div style={{ fontSize:8, color:C.blue, fontWeight:700, textTransform:"uppercase", marginBottom:1 }}>AI confirmed receiving</div>
                  <div style={{ fontSize:9, color:"#1E40AF" }}>{ds.data_received}</div>
                </div>
              )}
            </div>
          ) : ds.signal === "ERROR" ? (
            <div style={{ color:C.red, fontSize:11 }}>⚠ API error: {ds.reasoning}</div>
          ) : (
            <div style={{ color:C.muted, fontSize:11 }}>No prediction yet — DeepSeek fires at each 5-minute bar open</div>
          )}
        </div>

        {/* Reasoning */}
        {ds.reasoning && ds.signal !== "ERROR" && (
          <div style={{ ...card, flexShrink:0 }}>
            <div style={{ ...label, marginBottom:6 }}>Reasoning</div>
            {(ds.reasoning||"").split("\n").filter(Boolean).map((line,i,arr)=>(
              <div key={i} style={{ display:"flex", gap:8, marginBottom:i<arr.length-1?6:0, alignItems:"flex-start" }}>
                <span style={{ fontSize:10, fontWeight:900, color:C.amber, minWidth:16, flexShrink:0, paddingTop:1 }}>{i+1}.</span>
                <span style={{ fontSize:11, color:C.textSec, lineHeight:1.6 }}><BoldAnalysis text={line} /></span>
              </div>
            ))}
            {ds.data_requests && ds.data_requests.toUpperCase() !== "NONE" && (
              <div style={{ marginTop:8, background:C.amberBg, border:`1px solid ${C.amberBorder}`, borderRadius:4, padding:"4px 8px" }}>
                <div style={{ fontSize:8, color:C.amber, fontWeight:700, textTransform:"uppercase", marginBottom:1 }}>AI requested additional data</div>
                <div style={{ fontSize:9, color:C.amber }}>{ds.data_requests}</div>
              </div>
            )}
          </div>
        )}

        {/* Narrative and Free Observation removed from live view — both feed into DeepSeek main prompt and are visible in History tab */}

        {/* Full Prompt — always shown; error state if missing after a real prediction */}
        {ds.signal && ds.signal !== "ERROR" ? (
          ds.full_prompt ? (
            <div style={{ ...card, flexShrink:0 }}>
              <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
                <div style={{ ...label }}>Full Prompt Sent to DeepSeek</div>
                <span style={{ fontSize:9, color:C.muted }}>{ds.full_prompt.length.toLocaleString()} chars · exact data sent</span>
              </div>
              <pre style={{ fontSize:8.5, color:C.textSec, background:C.bg, border:`1px solid ${C.borderSoft}`,
                borderRadius:5, padding:10, overflowX:"auto", whiteSpace:"pre-wrap", wordBreak:"break-word",
                maxHeight:450, overflowY:"auto", lineHeight:1.6, margin:0, fontFamily:"inherit" }}>{ds.full_prompt}</pre>
            </div>
          ) : (
            <div style={{ ...card, flexShrink:0, borderLeft:`3px solid ${C.red}`, background:C.redBg }}>
              <div style={{ fontSize:11, fontWeight:700, color:C.red }}>⚠ Full prompt not stored for this window</div>
              <div style={{ fontSize:9, color:C.red, marginTop:3 }}>Cannot verify what data was sent to DeepSeek.</div>
            </div>
          )
        ) : null}

        {/* Raw Response — always shown; error state if missing after a real prediction */}
        {ds.signal && ds.signal !== "ERROR" ? (
          ds.raw_response ? (
            <div style={{ ...card, flexShrink:0 }}>
              <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
                <div style={{ ...label }}>Raw Response from DeepSeek</div>
                <span style={{ fontSize:9, color:C.muted }}>{ds.raw_response.length.toLocaleString()} chars · exact response received</span>
              </div>
              <pre style={{ fontSize:8.5, color:C.textSec, background:C.bg, border:`1px solid ${C.borderSoft}`,
                borderRadius:5, padding:10, overflowX:"auto", whiteSpace:"pre-wrap", wordBreak:"break-word",
                maxHeight:300, overflowY:"auto", lineHeight:1.6, margin:0, fontFamily:"inherit" }}>{ds.raw_response}</pre>
            </div>
          ) : (
            <div style={{ ...card, flexShrink:0, borderLeft:`3px solid ${C.red}`, background:C.redBg }}>
              <div style={{ fontSize:11, fontWeight:700, color:C.red }}>⚠ Raw response not stored for this window</div>
              <div style={{ fontSize:9, color:C.red, marginTop:3 }}>Cannot verify what DeepSeek actually returned.</div>
            </div>
          )
        ) : null}

        {!ds.signal && (
          <div style={{ ...card, flex:1, display:"flex", alignItems:"center", justifyContent:"center", color:C.muted, fontSize:11 }}>
            Prompt/response appear here after the first 5-minute window fires.
          </div>
        )}
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
//  Source History Tab
// ══════════════════════════════════════════════════════════════

const SOURCE_DEFS = MICRO_SRC_DEFS.map(([key, label]) => ({ key, label }));


function SourceCard({ def, data }) {
  if (!data) return (
    <div style={{ ...card, opacity:0.35, borderLeft:`2px solid ${C.borderSoft}` }}>
      <div style={{ fontSize:10, fontWeight:700, color:C.muted }}>{def.label}</div>
      <div style={{ fontSize:8, color:C.muted, marginTop:3 }}>No data for this window</div>
    </div>
  );

  const sig   = microSignalKey(def.key, data);
  const kvs   = microKV(def.key, data);
  const interp = data.interpretation || "";
  const sc    = sig ? sigColors(sig) : null;

  return (
    <div style={{ ...card, borderLeft:`2px solid ${sc?.border || C.borderSoft}` }}>
      <div style={{ display:"flex", alignItems:"center", gap:6, marginBottom:4 }}>
        <span style={{ fontSize:10, fontWeight:700, color:C.text, flex:1 }}>{def.label}</span>
        {sc && <span style={{ fontSize:8, fontWeight:700, padding:"1px 5px", borderRadius:3,
          background:sc.bg, color:sc.color, border:`1px solid ${sc.border}`, flexShrink:0 }}>
          {sig.replace(/_CONTRARIAN|_ARBI/,"")}
        </span>}
      </div>
      {kvs.length > 0 && (
        <div style={{ display:"flex", gap:8, flexWrap:"wrap", marginBottom: interp ? 4 : 0 }}>
          {kvs.map(([lbl,val]) => (
            <span key={lbl} style={{ fontSize:9 }}>
              <span style={{ color:C.muted }}>{lbl}: </span>
              <span style={{ color:C.textSec, fontWeight:700 }}>{val}</span>
            </span>
          ))}
        </div>
      )}
      {interp && (
        <div style={{ fontSize:8, color:C.muted, lineHeight:1.4, fontStyle:"italic" }}>{interp}</div>
      )}
    </div>
  );
}


// ── Historical Analysis Audit Tab ─────────────────────────────
function HistoricalAnalysisAuditTab({ deepseekLog }) {
  const [selectedWindow, setSelectedWindow] = React.useState(null);
  const [auditData, setAuditData] = React.useState(null);
  const [loading, setLoading] = React.useState(false);

  const fetchAudit = async (windowStart) => {
    setLoading(true);
    try {
      const res = await fetch(`/historical-analysis/${windowStart}`);
      const data = await res.json();
      setAuditData(data);
    } catch (e) {
      console.error("Failed to load audit:", e);
      setAuditData({ status: "error", message: e.message });
    } finally {
      setLoading(false);
    }
  };

  const handleSelectWindow = (windowStart) => {
    setSelectedWindow(windowStart);
    fetchAudit(windowStart);
  };

  if (!deepseekLog || deepseekLog.length === 0) {
    return (
      <div style={{ padding: 24, textAlign: "center", color: C.muted }}>
        <div style={{ fontSize: 12 }}>No prediction history available yet.</div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", height: "100%", gap: 8, padding: "8px" }}>
      {/* LEFT: List of predictions */}
      <div style={{ flex: "0 0 28%", overflowY: "auto", borderRight: `1px solid ${C.borderSoft}`, paddingRight: 8 }}>
        <div style={{ fontSize: 9, color: C.muted, fontWeight: 700, marginBottom: 6, textTransform: "uppercase", letterSpacing: 1 }}>
          Predictions ({deepseekLog.length})
        </div>
        {deepseekLog.map((pred, i) => (
          <div
            key={i}
            onClick={() => handleSelectWindow(pred.window_start)}
            style={{
              padding: "6px 8px",
              marginBottom: 4,
              background: selectedWindow === pred.window_start ? C.cardBg : "transparent",
              border: selectedWindow === pred.window_start ? `1px solid ${C.amber}` : `1px solid ${C.borderSoft}`,
              borderRadius: 4,
              cursor: "pointer",
              fontSize: 10,
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 2 }}>
              <span style={{ fontWeight: 700, color: C.amber }}>{pred.signal}</span>
              <span style={{ color: pred.correct ? C.green : pred.correct === false ? C.red : C.muted, fontSize: 9, fontWeight: 700 }}>
                {pred.correct === true ? "✓" : pred.correct === false ? "✗" : "~"}
              </span>
            </div>
            <div style={{ color: C.muted, fontSize: 9 }}>
              {new Date(pred.window_start * 1000).toLocaleTimeString()}
            </div>
            <div style={{ color: C.muted, fontSize: 9 }}>
              {pred.confidence ? `${(pred.confidence * 100).toFixed(0)}%` : "—"}
            </div>
          </div>
        ))}
      </div>

      {/* RIGHT: Audit details */}
      <div style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column" }}>
        {!selectedWindow ? (
          <div style={{ padding: 12, color: C.muted, fontSize: 11 }}>
            Select a prediction to view pipeline audit
          </div>
        ) : loading ? (
          <div style={{ padding: 12, color: C.muted, fontSize: 11 }}>Loading...</div>
        ) : auditData?.status === "not_found" ? (
          <div style={{ padding: 12, color: C.muted, fontSize: 11 }}>Prediction not found</div>
        ) : auditData?.status === "ok" ? (
          <>
            {/* Header info */}
            <div style={{ ...card, marginBottom: 8, padding: "8px 10px" }}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 10 }}>
                <div>
                  <div style={{ color: C.muted, fontSize: 9 }}>START PRICE</div>
                  <div style={{ fontWeight: 700, fontSize: 11 }}>${auditData.start_price?.toFixed(2)}</div>
                </div>
                <div>
                  <div style={{ color: C.muted, fontSize: 9 }}>END PRICE</div>
                  <div style={{ fontWeight: 700, fontSize: 11, color: auditData.end_price > auditData.start_price ? C.green : C.red }}>
                    ${auditData.end_price?.toFixed(2)} {auditData.end_price > auditData.start_price ? "↑" : "↓"}
                  </div>
                </div>
                <div>
                  <div style={{ color: C.muted, fontSize: 9 }}>SIGNAL</div>
                  <div style={{ fontWeight: 700, fontSize: 11, color: auditData.prediction.signal === "UP" ? C.green : C.red }}>
                    {auditData.prediction.signal} ({(auditData.prediction.confidence * 100).toFixed(0)}%)
                  </div>
                </div>
                <div>
                  <div style={{ color: C.muted, fontSize: 9 }}>RESULT</div>
                  <div style={{ fontWeight: 700, fontSize: 11, color: auditData.correct ? C.green : C.red }}>
                    {auditData.correct === true ? "✓ CORRECT" : auditData.correct === false ? "✗ WRONG" : "PENDING"}
                  </div>
                </div>
              </div>
            </div>

            {/* Pipeline: Requests & Received */}
            <div style={{ ...card, marginBottom: 8, padding: "8px 10px" }}>
              <div style={{ fontSize: 9, fontWeight: 700, color: C.amber, marginBottom: 6, textTransform: "uppercase", letterSpacing: 1 }}>
                📊 Data Pipeline ({auditData.pipeline.latency_ms}ms)
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                <div>
                  <div style={{ fontSize: 9, color: C.muted, marginBottom: 4 }}>REQUESTS ({auditData.pipeline.data_requests.length})</div>
                  <div style={{ fontSize: 9, lineHeight: 1.4, color: C.textSec, maxHeight: 120, overflowY: "auto" }}>
                    {auditData.pipeline.data_requests.length > 0 ? (
                      auditData.pipeline.data_requests.map((req, i) => (
                        <div key={i} style={{ marginBottom: 2, padding: "2px 4px", background: C.borderSoft, borderRadius: 2 }}>
                          {req.substring(0, 80)}{req.length > 80 ? "…" : ""}
                        </div>
                      ))
                    ) : (
                      <div style={{ color: C.muted }}>—</div>
                    )}
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 9, color: C.muted, marginBottom: 4 }}>RECEIVED ({auditData.pipeline.data_received.length})</div>
                  <div style={{ fontSize: 9, lineHeight: 1.4, color: C.green, maxHeight: 120, overflowY: "auto" }}>
                    {auditData.pipeline.data_received.length > 0 ? (
                      auditData.pipeline.data_received.map((item, i) => (
                        <div key={i} style={{ marginBottom: 2 }}>✓ {item.substring(0, 60)}{item.length > 60 ? "…" : ""}</div>
                      ))
                    ) : (
                      <div style={{ color: C.muted }}>—</div>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Input Data: Strategies, Indicators, Dashboard */}
            <div style={{ ...card, marginBottom: 8, padding: "8px 10px" }}>
              <div style={{ fontSize: 9, fontWeight: 700, color: C.amber, marginBottom: 6, textTransform: "uppercase", letterSpacing: 1 }}>
                🎯 Input Signals
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 9 }}>
                <div>
                  <div style={{ color: C.muted, marginBottom: 2 }}>Strategies ({Object.keys(auditData.input_data.strategies || {}).length})</div>
                  <div style={{ maxHeight: 100, overflowY: "auto", fontSize: 8 }}>
                    {Object.entries(auditData.input_data.strategies || {}).map(([k, v], i) => (
                      <div key={i} style={{ padding: "1px 2px" }}>
                        <span style={{ color: C.amber }}>{k}:</span> {typeof v === "object" ? JSON.stringify(v).substring(0, 30) : String(v).substring(0, 30)}
                      </div>
                    ))}
                  </div>
                </div>
                <div>
                  <div style={{ color: C.muted, marginBottom: 2 }}>Dashboard ({Object.keys(auditData.input_data.dashboard_signals || {}).length})</div>
                  <div style={{ maxHeight: 100, overflowY: "auto", fontSize: 8 }}>
                    {Object.entries(auditData.input_data.dashboard_signals || {}).map(([k, v], i) => (
                      <div key={i} style={{ padding: "1px 2px" }}>
                        <span style={{ color: C.green }}>{k}:</span> {String(v).substring(0, 30)}
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>

            {/* Full Prompt */}
            <div style={{ ...card, marginBottom: 8, padding: "8px 10px" }}>
              <div style={{ fontSize: 9, fontWeight: 700, color: C.amber, marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>
                💭 Full Prompt Sent to DeepSeek
              </div>
              <div
                style={{
                  background: C.borderSoft,
                  padding: 6,
                  borderRadius: 3,
                  fontSize: 8,
                  color: C.textSec,
                  fontFamily: "monospace",
                  maxHeight: 180,
                  overflowY: "auto",
                  whiteSpace: "pre-wrap",
                  wordWrap: "break-word",
                  lineHeight: 1.3,
                }}
              >
                {auditData.prompting.full_prompt || "No prompt captured"}
              </div>
            </div>

            {/* Raw Response */}
            <div style={{ ...card, padding: "8px 10px" }}>
              <div style={{ fontSize: 9, fontWeight: 700, color: C.amber, marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>
                🤖 DeepSeek Raw Response
              </div>
              <div
                style={{
                  background: C.borderSoft,
                  padding: 6,
                  borderRadius: 3,
                  fontSize: 8,
                  color: C.green,
                  fontFamily: "monospace",
                  maxHeight: 150,
                  overflowY: "auto",
                  whiteSpace: "pre-wrap",
                  wordWrap: "break-word",
                  lineHeight: 1.3,
                }}
              >
                {auditData.prompting.raw_response || "No response captured"}
              </div>
            </div>
          </>
        ) : (
          <div style={{ padding: 12, color: C.red, fontSize: 11 }}>Error loading audit data</div>
        )}
      </div>
    </div>
  );
}

// ── Embedding Audit Tab ────────────────────────────────────────
const INSPECT_FILE_ORDER = [
  ["main_predictor.last_prompt",       "Main Predictor — Prompt Sent"],
  ["main_predictor.last_response",     "Main Predictor — DeepSeek Response"],
  ["historical_analyst.last_sent",     "Historical Analyst — Top-20 Reranked Bars (the 'embed')"],
  ["historical_analyst.last_prompt",   "Historical Analyst — Prompt Sent"],
  ["historical_analyst.last_response", "Historical Analyst — DeepSeek Response"],
  ["unified_analyst.last_sent",        "Unified Analyst — Context Sent"],
  ["unified_analyst.last_prompt",      "Unified Analyst — Prompt Sent"],
  ["unified_analyst.last_response",    "Unified Analyst — DeepSeek Response"],
  ["binance_expert.last_response",     "Binance Expert — DeepSeek Response"],
  ["embedding_audit.last_raw",         "Embedding Audit — Last Raw Output"],
];

function InspectFileRow({ fileKey, title, file }) {
  const [open, setOpen] = React.useState(false);
  const exists = file && file.exists;
  const empty  = !exists || !file.content || file.content.trim().length === 0;
  return (
    <div style={{ borderBottom:`1px solid ${C.borderSoft}` }}>
      <div onClick={()=>!empty && setOpen(o=>!o)}
        style={{ padding:"8px 12px", cursor:empty?"default":"pointer", display:"flex", gap:10, alignItems:"center",
                 opacity: empty ? 0.5 : 1 }}>
        <div style={{ flex:1, minWidth:0 }}>
          <div style={{ fontSize:11, fontWeight:600, color:C.text }}>{title}</div>
          <div style={{ fontSize:9, color:C.muted, marginTop:2, fontFamily:"monospace" }}>
            {file?.path || fileKey}
            {exists ? ` · ${file.size_bytes}B · ${file.mtime_str}` : " · (not written yet)"}
          </div>
        </div>
        <div style={{ color:C.muted, fontSize:12, flexShrink:0 }}>
          {empty ? "—" : (open ? "▼" : "▶")}
        </div>
      </div>
      {open && !empty && (
        <pre style={{ margin:0, padding:"10px 14px", background:C.bg, color:C.text, fontSize:10, lineHeight:1.5,
                      whiteSpace:"pre-wrap", wordBreak:"break-word", maxHeight:500, overflowY:"auto",
                      borderTop:`1px solid ${C.borderSoft}` }}>
          {file.content}
        </pre>
      )}
    </div>
  );
}

function TimingTab({ timings }) {
  const current = timings && timings.current ? timings.current : {};
  const history = timings && Array.isArray(timings.history) ? timings.history : [];
  const currentStages = current.stages || {};
  const stageKeys = React.useMemo(() => {
    const s = new Set(Object.keys(currentStages));
    history.forEach(b => Object.keys(b.stages || {}).forEach(k => s.add(k)));
    // Keep a sensible column order
    const ordered = [
      "specialists", "dashboard_signals",
      "historical_total", "historical_cohere_embed", "historical_pgvector_search",
      "historical_cohere_rerank", "historical_deepseek_call",
      "binance_expert", "main_deepseek",
    ];
    const rest = [...s].filter(k => !ordered.includes(k));
    return [...ordered.filter(k => s.has(k)), ...rest];
  }, [currentStages, history]);

  const fmtSec = (v) => (v == null ? "—" : `${Number(v).toFixed(1)}s`);
  const cellForStage = (stage) => {
    if (!stage) return { text: "—", color: C.muted, title: "" };
    if (!stage.ok) {
      return {
        text:  fmtSec(stage.elapsed_s),
        color: C.red,
        title: stage.error || "failed",
      };
    }
    const s = Number(stage.elapsed_s || 0);
    const color = s > 60 ? C.red : s > 30 ? C.amber : C.green;
    return { text: fmtSec(s), color, title: "ok" };
  };

  const th = { padding:"6px 8px", textAlign:"left", fontSize:9, fontWeight:700,
               color:C.muted, letterSpacing:1.2, borderBottom:`1px solid ${C.border}` };
  const td2 = { padding:"4px 8px", fontSize:10, color:C.textSec,
                borderBottom:`1px solid ${C.borderSoft}`, whiteSpace:"nowrap" };

  return (
    <div style={{ padding: "16px 18px" }}>
      <div style={{ fontSize:11, color:C.textSec, marginBottom:12 }}>
        Per-stage elapsed time for each bar. Historical analyst no longer expires —
        if the pipeline runs past the 5-minute bar close, the prediction is
        discarded but the timings are kept here so you can see which stage blew
        the budget. Red = stage failed or &gt; 60s. Amber = &gt; 30s.
      </div>

      {/* Current bar */}
      <div style={{ ...card, marginBottom:16 }}>
        <div style={{ ...label, marginBottom:8 }}>CURRENT BAR</div>
        {Object.keys(currentStages).length === 0 ? (
          <div style={{ fontSize:11, color:C.muted }}>No timing data yet — waiting for the next bar to start.</div>
        ) : (
          <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill, minmax(180px, 1fr))", gap:8 }}>
            {stageKeys.map(k => {
              const s = currentStages[k];
              const cell = cellForStage(s);
              return (
                <div key={k} title={cell.title} style={{
                  border:`1px solid ${C.border}`, borderRadius:6, padding:"6px 8px",
                  background: s && !s.ok ? C.redBg : C.bg }}>
                  <div style={{ fontSize:9, color:C.muted, letterSpacing:1 }}>{k}</div>
                  <div style={{ fontSize:14, fontWeight:700, color:cell.color }}>{cell.text}</div>
                  {s && !s.ok && s.error ? (
                    <div style={{ fontSize:9, color:C.red, marginTop:2,
                                  overflow:"hidden", textOverflow:"ellipsis" }}>{s.error}</div>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}
        {current.overran_bar_close ? (
          <div style={{ marginTop:8, padding:"6px 8px", background:C.redBg,
                         border:`1px solid ${C.redBorder}`, borderRadius:4,
                         fontSize:10, color:C.red, fontWeight:700 }}>
            ⚠ Pipeline overran bar close by {current.overran_by_s}s — prediction was discarded.
          </div>
        ) : null}
        {current.pipeline_error ? (
          <div style={{ marginTop:8, padding:"6px 8px", background:C.redBg,
                         border:`1px solid ${C.redBorder}`, borderRadius:4,
                         fontSize:10, color:C.red }}>
            pipeline error: {current.pipeline_error}
          </div>
        ) : null}
      </div>

      {/* History table */}
      <div style={card}>
        <div style={{ ...label, marginBottom:8 }}>
          LAST {history.length} BARS {timings && timings.history_total ? `(of ${timings.history_total} total)` : ""}
        </div>
        {history.length === 0 ? (
          <div style={{ fontSize:11, color:C.muted }}>No history yet.</div>
        ) : (
          <div style={{ overflowX:"auto" }}>
            <table style={{ width:"100%", borderCollapse:"collapse", fontFamily:"inherit" }}>
              <thead>
                <tr>
                  <th style={th}>Bar</th>
                  <th style={th}>Total</th>
                  <th style={th}>Overran</th>
                  {stageKeys.map(k => <th key={k} style={th}>{k}</th>)}
                </tr>
              </thead>
              <tbody>
                {history.map((b, i) => {
                  const stages = b.stages || {};
                  return (
                    <tr key={i}>
                      <td style={td2}>{b.bar || "—"}</td>
                      <td style={{ ...td2,
                                   color: (b.total_elapsed_s > 300) ? C.red : (b.total_elapsed_s > 240 ? C.amber : C.textSec),
                                   fontWeight:700 }}>
                        {fmtSec(b.total_elapsed_s)}
                      </td>
                      <td style={td2}>
                        {b.overran_bar_close
                          ? <span style={{ color:C.red, fontWeight:700 }}>+{b.overran_by_s}s</span>
                          : <span style={{ color:C.green }}>✓</span>}
                      </td>
                      {stageKeys.map(k => {
                        const cell = cellForStage(stages[k]);
                        return (
                          <td key={k} title={cell.title} style={{ ...td2, color:cell.color,
                                                                   fontWeight: stages[k] ? 600 : 400 }}>
                            {cell.text}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

function EmbeddingAuditTab({ embeddingAuditLog, setEmbeddingAuditLog, deepseekInspect, setDeepseekInspect }) {
  const [expandedAudit, setExpandedAudit] = React.useState(null);

  // Auto-poll both data sources every 30s while the tab is visible. No manual
  // trigger surface — audits fire on their 4h schedule; file inspect refreshes
  // whenever a bar completes on the server.
  React.useEffect(() => {
    const load = () => {
      fetch("/api/embedding-audit").then(r => r.ok ? r.json() : null)
        .then(d => { if (d) setEmbeddingAuditLog(d.audit_log || []); }).catch(()=>{});
      fetch("/api/inspect/last-deepseek").then(r => r.ok ? r.json() : null)
        .then(d => { if (d) setDeepseekInspect(d); }).catch(()=>{});
    };
    load();
    const id = setInterval(load, 30000);
    return () => clearInterval(id);
  }, [setEmbeddingAuditLog, setDeepseekInspect]);

  const files = deepseekInspect?.files || {};
  const anyFilePresent = Object.values(files).some(f => f && f.exists && f.content && f.content.trim().length > 0);

  // ── Build the "flow proof" from the most recent audit ──────────
  const latest = (embeddingAuditLog || [])[embeddingAuditLog?.length - 1] || null;
  const stats  = latest?.stats || {};
  const sim    = stats.sim_stats || {};
  const sig    = latest?.audit_signal || "UNKNOWN";
  const sigColor =
    sig === "GOOD"              ? C.green :
    sig === "NEEDS_IMPROVEMENT" ? C.amber :
    sig === "CRITICAL"          ? C.red   : C.muted;
  const sigBg =
    sig === "GOOD"              ? C.greenBg :
    sig === "NEEDS_IMPROVEMENT" ? C.amberBg :
    sig === "CRITICAL"          ? C.redBg   : C.bg;
  const sigBorder =
    sig === "GOOD"              ? C.greenBorder :
    sig === "NEEDS_IMPROVEMENT" ? C.amberBorder :
    sig === "CRITICAL"          ? C.redBorder   : C.border;

  // Each stage: { name, did, proof, ok, detail }
  const haAccPct = stats.ha_accuracy != null ? stats.ha_accuracy * 100 : null;
  const stages = [
    {
      name: "1. Bar closes",
      did:  "collect OHLCV + indicators + strategy votes + DeepSeek reasoning + postmortem",
      proof: stats.total_bars != null ? `${stats.total_bars} resolved bars in history` : null,
      ok:    (stats.total_bars || 0) > 5,
      fail:  "need ≥ 5 resolved bars",
    },
    {
      name: "2. Compose essay",
      did:  "render each bar as a rich natural-language essay (full context, no truncation)",
      proof: latest ? "essay built per bar → see 'last_sent.txt' below" : null,
      ok:    !!latest,
      fail:  "no audit run yet",
    },
    {
      name: "3. Cohere embed",
      did:  "encode essay via embed-english-v3.0 → 1024-dim L2-normalized vector",
      proof: stats.embedded_bars != null
        ? `${stats.embedded_bars}/${stats.total_bars} embedded (${stats.coverage_pct}% coverage)`
        : null,
      ok:    (stats.coverage_pct || 0) >= 80,
      fail:  (stats.coverage_pct || 0) < 50
               ? "low coverage — bootstrap may still be running"
               : "coverage below 80%",
    },
    {
      name: "4. Store in pgvector",
      did:  "insert vector into PostgreSQL pgvector with HNSW cosine index",
      proof: stats.embedded_bars != null ? `${stats.embedded_bars} vectors indexed` : null,
      ok:    (stats.embedded_bars || 0) > 0,
      fail:  "no embeddings stored yet",
    },
    {
      name: "5. Query top-50",
      did:  "at next bar open, embed current conditions and cosine-search pgvector",
      proof: sim.count != null
        ? `last search: ${sim.count} hits, similarity ${sim.min?.toFixed(3)}…${sim.max?.toFixed(3)} (p50=${sim.p50?.toFixed(3)})`
        : "no similarity stats yet",
      ok:    (sim.count || 0) >= 20,
      fail:  "<20 similarity scores observed — query stage may be stubbed",
    },
    {
      name: "6. Cohere rerank",
      did:  "send the 50 candidates to rerank-english-v3.0, keep top 20",
      proof: latest ? "rerank runs on every bar — inspect 'historical_analyst.last_sent' below" : null,
      ok:    !!latest,
      fail:  "verify in files panel",
    },
    {
      name: "7. Send to DeepSeek",
      did:  "inject top-20 bars (with DS reasoning + postmortem) into the prompt",
      proof: haAccPct != null
        ? `historical analyst accuracy: ${haAccPct.toFixed(1)}% (${stats.ha_correct}/${stats.ha_total})`
        : "no HA votes graded yet — need more resolved bars",
      ok:    haAccPct != null && haAccPct >= 50,
      fail:  haAccPct != null && haAccPct < 50
               ? `HA accuracy ${haAccPct.toFixed(1)}% — worse than coin flip, retrieval not contributing`
               : "not enough HA-voted resolved bars yet",
    },
  ];

  return (
    <div style={{ display:"flex", flexDirection:"column", height:"100%", overflow:"hidden" }}>
      {/* Sticky header with verdict + controls */}
      <div style={{ ...card, flexShrink:0, padding:"12px 16px",
        display:"flex", gap:12, alignItems:"flex-start", flexWrap:"wrap" }}>
        <div style={{ flex:"1 1 320px", minWidth:0 }}>
          <div style={label}>Embedding Pipeline Audit</div>
          <div style={{ fontSize:10, color:C.muted, marginTop:2 }}>
            What the retrieval pipeline did and whether it worked, with numbers as proof.
            Auto-audits every 4 h via deepseek-reasoner.
          </div>
          {latest && (
            <div style={{ marginTop:8, display:"flex", alignItems:"center", gap:8, flexWrap:"wrap" }}>
              <span style={{ padding:"3px 10px", borderRadius:4, fontSize:10, fontWeight:800,
                background:sigBg, color:sigColor, border:`1px solid ${sigBorder}` }}>{sig}</span>
              <span style={{ fontSize:10, color:C.muted }}>last audit: {latest.timestamp_str}</span>
              {latest.summary && (
                <div style={{ fontSize:11, color:C.textSec, flex:"1 1 100%", marginTop:4 }}>{latest.summary}</div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Scroll body */}
      <div style={{ flex:1, overflowY:"auto" }}>

        {/* ── Pipeline flow diagram (7 stages with proof) ── */}
        <div style={{ ...card, margin:"12px 16px", padding:"14px 16px" }}>
          <div style={{ ...label, marginBottom:10 }}>
            Pipeline flow · input → action → proof
            {!latest && <span style={{ marginLeft:8, color:C.muted, fontWeight:400 }}>(run an audit to populate)</span>}
          </div>
          <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(280px, 1fr))", gap:8 }}>
            {stages.map((s, i) => {
              const showOk = latest != null;
              const okColor     = showOk && s.ok        ? C.green   : showOk ? C.red      : C.muted;
              const okBg        = showOk && s.ok        ? C.greenBg : showOk ? C.redBg    : C.bg;
              const okBorder    = showOk && s.ok        ? C.greenBorder : showOk ? C.redBorder : C.border;
              const proofText   = s.proof;
              return (
                <div key={i} style={{ border:`1px solid ${okBorder}`, borderRadius:6, padding:"10px 12px",
                  background:okBg, display:"flex", flexDirection:"column", gap:4 }}>
                  <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                    <span style={{ fontSize:11, fontWeight:800, color:okColor }}>
                      {showOk ? (s.ok ? "✓" : "✗") : "–"}
                    </span>
                    <span style={{ fontSize:11, fontWeight:800, color:C.text }}>{s.name}</span>
                  </div>
                  <div style={{ fontSize:10, color:C.textSec, lineHeight:1.4 }}>{s.did}</div>
                  {proofText ? (
                    <div style={{ fontSize:10, color:okColor, fontFamily:"monospace",
                      background:C.surface, padding:"4px 6px", borderRadius:3,
                      border:`1px solid ${okBorder}`, marginTop:2 }}>
                      {proofText}
                    </div>
                  ) : (
                    <div style={{ fontSize:9, color:C.muted, fontStyle:"italic", marginTop:2 }}>
                      {showOk ? s.fail : "awaiting audit"}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          {latest && (
            <div style={{ marginTop:10, fontSize:10, color:C.muted, textAlign:"right" }}>
              audit ran in {latest.elapsed_s}s · {latest.issues?.length || 0} issue(s) · {latest.suggestions?.length || 0} suggestion(s)
            </div>
          )}
        </div>

        {/* ── Latest audit detail (issues + suggestions + full analysis) ── */}
        {latest && (
          <div style={{ ...card, margin:"0 16px 12px 16px", padding:"12px 16px" }}>
            <div style={{ ...label, marginBottom:8 }}>Latest audit — verdict detail</div>
            {latest.issues && latest.issues.length > 0 && (
              <div style={{ marginBottom:10 }}>
                <div style={{ fontSize:10, fontWeight:700, color:C.red, marginBottom:4 }}>
                  ⚠ Issues found ({latest.issues.length})
                </div>
                {latest.issues.map((iss, i) => (
                  <div key={i} style={{ fontSize:11, color:C.textSec, padding:"6px 8px",
                    marginBottom:4, background:C.redBg, borderLeft:`3px solid ${C.red}`, borderRadius:3,
                    whiteSpace:"pre-wrap" }}>{iss}</div>
                ))}
              </div>
            )}
            {latest.suggestions && latest.suggestions.length > 0 && (
              <div style={{ marginBottom:10 }}>
                <div style={{ fontSize:10, fontWeight:700, color:C.blue, marginBottom:4 }}>
                  💡 Suggestions ({latest.suggestions.length})
                </div>
                {latest.suggestions.map((sg, i) => (
                  <div key={i} style={{ fontSize:11, color:C.textSec, padding:"6px 8px",
                    marginBottom:4, background:C.blueBg, borderLeft:`3px solid ${C.blue}`, borderRadius:3,
                    whiteSpace:"pre-wrap" }}>{sg}</div>
                ))}
              </div>
            )}
            {latest.full_analysis && (
              <details>
                <summary style={{ cursor:"pointer", fontSize:10, fontWeight:700, color:C.textSec }}>
                  Full analysis (DeepSeek chain-of-thought)
                </summary>
                <pre style={{ fontSize:10, color:C.textSec, whiteSpace:"pre-wrap",
                  background:C.bg, padding:10, borderRadius:4, border:`1px solid ${C.borderSoft}`,
                  maxHeight:320, overflowY:"auto", marginTop:6 }}>{latest.full_analysis}</pre>
              </details>
            )}
          </div>
        )}

        {/* ── Live pipeline files (prompts/responses) ── */}
        <div style={{ ...card, margin:"0 16px 12px 16px", padding:0 }}>
          <div style={{ padding:"10px 16px", borderBottom:`1px solid ${C.border}` }}>
            <div style={label}>Pipeline file inspection · actual bytes DeepSeek saw last bar</div>
            <div style={{ fontSize:10, color:C.muted, marginTop:2 }}>
              <span style={{ fontFamily:"monospace" }}>specialists/*/last_*.txt</span> — click a row to expand.
              {!anyFilePresent && " — None written yet; wait for a bar to close."}
            </div>
          </div>
          {INSPECT_FILE_ORDER.map(([k, title]) => (
            <InspectFileRow key={k} fileKey={k} title={title} file={files[k]} />
          ))}
        </div>

        {/* ── Full audit history (collapsed by default) ── */}
        {(embeddingAuditLog || []).length > 1 && (
          <div style={{ ...card, margin:"0 16px 12px 16px", padding:"12px 16px" }}>
            <div style={{ ...label, marginBottom:6 }}>Audit history ({embeddingAuditLog.length})</div>
            {embeddingAuditLog.slice().reverse().slice(1).map((audit, idx) => {
              const realIdx = embeddingAuditLog.length - 2 - idx;
              const isExpanded = expandedAudit === realIdx;
              const s = audit.stats || {};
              const c =
                audit.audit_signal === "GOOD" ? C.green :
                audit.audit_signal === "NEEDS_IMPROVEMENT" ? C.amber :
                audit.audit_signal === "CRITICAL" ? C.red : C.muted;
              return (
                <div key={realIdx} style={{ borderTop:`1px solid ${C.borderSoft}`, padding:"6px 0" }}>
                  <div onClick={()=>setExpandedAudit(isExpanded ? null : realIdx)}
                       style={{ cursor:"pointer", display:"flex", gap:8, alignItems:"center" }}>
                    <span style={{ fontSize:9, fontWeight:800, color:c, letterSpacing:1 }}>{audit.audit_signal}</span>
                    <span style={{ fontSize:10, color:C.muted }}>{audit.timestamp_str}</span>
                    <span style={{ fontSize:10, color:C.textSec, flex:1, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{audit.summary}</span>
                    <span style={{ fontSize:9, color:C.muted, flexShrink:0 }}>cov {s.coverage_pct}% · ha {s.ha_accuracy != null ? (s.ha_accuracy*100).toFixed(0)+"%" : "—"}</span>
                    <span style={{ fontSize:9, color:C.muted }}>{isExpanded ? "▲" : "▼"}</span>
                  </div>
                  {isExpanded && (
                    <div style={{ marginTop:6, fontSize:10, color:C.textSec, lineHeight:1.5 }}>
                      {(audit.issues || []).map((x, i) => <div key={"i"+i} style={{ marginBottom:3 }}>• {x.split("\n")[0]}</div>)}
                      {(audit.suggestions || []).map((x, i) => <div key={"s"+i} style={{ marginBottom:3, color:C.blue }}>→ {x.split("\n")[0]}</div>)}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {(!embeddingAuditLog || embeddingAuditLog.length === 0) && (
          <div style={{ padding:"16px", color:C.muted, fontSize:11, textAlign:"center" }}>
            No embedding audits in the log yet. Auto-audit fires every 4 hours via deepseek-reasoner.
          </div>
        )}
      </div>
    </div>
  );
}

// ── Errors Tab ────────────────────────────────────────────────
const _FLAG_KINDS = new Set(["DATA_GAP", "FREE_OBS", "SUGGESTION"]);
const _FLAG_LABEL = {
  ERROR:       "ERROR",
  UNAVAILABLE: "UNAVAILABLE",
  NONE:        "NONE",
  DATA_GAP:    "DATA GAP",
  FREE_OBS:    "FREE OBS",
  SUGGESTION:  "SUGGESTION",
};

function ErrorsTab({ errors }) {
  const [expanded, setExpanded] = React.useState(null);
  const [suggestions, setSuggestions] = React.useState(null);
  const [suggExpanded, setSuggExpanded] = React.useState(null);

  // Fetch system-improvement suggestions on mount and every 60s
  React.useEffect(() => {
    const load = () => fetch("/api/suggestions").then(r=>r.json()).then(setSuggestions).catch(()=>{});
    load();
    const id = setInterval(load, 60000);
    return () => clearInterval(id);
  }, []);

  const lessons    = suggestions?.lessons || [];
  const haSug      = suggestions?.historical_analyst_suggestions || [];
  const uaSug      = suggestions?.unified_analyst_suggestions || [];
  const suggTotal  = lessons.length + haSug.length + uaSug.length;

  const errCount  = (errors || []).filter(e => !_FLAG_KINDS.has(e.signal)).length;
  const flagCount = (errors?.length || 0) - errCount;
  const hasErrors = (errors || []).length > 0;

  // If no errors AND no suggestions, show empty state
  if (!hasErrors && suggTotal === 0) return (
    <div style={{ padding:24, color:C.muted, fontSize:12, textAlign:"center" }}>
      No errors, flags, or system-improvement suggestions recorded yet.
    </div>
  );

  return (
    <div style={{ overflow:"auto", height:"100%", padding:"6px 0" }}>
      <div style={{ fontSize:10, color:C.muted, letterSpacing:1, marginBottom:8, paddingLeft:4 }}>
        {errCount} ERROR/UNAVAILABLE BAR{errCount!==1?"S":""} · {flagCount} DEEPSEEK FLAG{flagCount!==1?"S":""}
        {suggTotal > 0 && <> · {suggTotal} SYSTEM-IMPROVEMENT SUGGESTION{suggTotal!==1?"S":""}</>}
      </div>

      {/* SYSTEM-IMPROVEMENT SUGGESTIONS — derived from postmortems + specialists */}
      {suggTotal > 0 && (
        <div style={{ ...card, marginBottom:12, borderLeft:`3px solid ${C.blue}` }}>
          <div style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
            <span style={{ fontSize:11, fontWeight:800, color:C.blue, letterSpacing:1.5 }}>💡 SYSTEM IMPROVEMENTS</span>
            <span style={{ fontSize:9, color:C.muted }}>rules the system has derived from its own mistakes; refreshes every 60s</span>
          </div>

          {/* Postmortem lessons — the most actionable */}
          {lessons.length > 0 && (
            <div style={{ marginBottom: haSug.length || uaSug.length ? 10 : 0 }}>
              <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:4, textTransform:"uppercase" }}>
                Postmortem lessons ({lessons.length}) — ex-ante rules the system says it should have applied
              </div>
              {lessons.map((L, i) => {
                const isOpen = suggExpanded === "L" + i;
                const ecColor = (L.error_class || "").includes("IRREDUCIBLE") ? C.muted :
                                (L.error_class || "").includes("TRAP")        ? C.red   :
                                (L.error_class || "").includes("BIAS")        ? C.amber : C.blue;
                return (
                  <div key={"L"+i} style={{ border:`1px solid ${C.borderSoft}`, borderRadius:4,
                    marginBottom:4, overflow:"hidden", background: isOpen ? C.blueBg : C.surface }}>
                    <div style={{ display:"flex", alignItems:"center", gap:8, padding:"6px 8px", cursor:"pointer", flexWrap:"wrap" }}
                         onClick={() => setSuggExpanded(isOpen ? null : "L" + i)}>
                      <span style={{ fontSize:10, fontWeight:800, color:C.blue }}>{L.name}</span>
                      {L.error_class && <span style={{ fontSize:8, fontWeight:700, padding:"1px 5px", borderRadius:3,
                        color:ecColor, border:`1px solid ${ecColor}`, background:C.surface }}>{L.error_class}</span>}
                      <span style={{ fontSize:9, color:C.muted }}>{L.window_start_str}</span>
                      <span style={{ fontSize:9, fontWeight:700, color: L.signal==="UP"?C.green:L.signal==="DOWN"?C.red:C.muted }}>
                        {L.signal}{L.correct===true ? " ✓" : L.correct===false ? " ✗" : ""}
                      </span>
                      <span style={{ marginLeft:"auto", fontSize:9, color:C.muted }}>{isOpen?"▲":"▼"}</span>
                    </div>
                    {isOpen && (
                      <div style={{ padding:"8px 10px", borderTop:`1px solid ${C.borderSoft}`, fontSize:11, color:C.textSec, lineHeight:1.5 }}>
                        {L.rule         && <div style={{ marginBottom:6 }}><strong style={{ color:C.text }}>Rule:</strong> {L.rule}</div>}
                        {L.preconditions&& <div style={{ marginBottom:6 }}><strong style={{ color:C.text }}>When:</strong> {L.preconditions}</div>}
                        {L.effect       && <div style={{ marginBottom:6 }}><strong style={{ color:C.text }}>Effect:</strong> {L.effect}</div>}
                        {L.root_cause   && <div style={{ marginBottom:0 }}><strong style={{ color:C.text }}>Root cause:</strong> {L.root_cause}</div>}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {haSug.length > 0 && (
            <div style={{ marginBottom: uaSug.length ? 10 : 0 }}>
              <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:4, textTransform:"uppercase" }}>
                Historical analyst suggestions ({haSug.length})
              </div>
              {haSug.slice(-10).reverse().map((s, i) => (
                <div key={"h"+i} style={{ fontSize:10, color:C.textSec, padding:"3px 6px",
                  background:C.bg, border:`1px solid ${C.borderSoft}`, borderRadius:3, marginBottom:3 }}>
                  {s}
                </div>
              ))}
            </div>
          )}

          {uaSug.length > 0 && (
            <div>
              <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:4, textTransform:"uppercase" }}>
                Unified analyst suggestions ({uaSug.length})
              </div>
              {uaSug.slice(-10).reverse().map((s, i) => (
                <div key={"u"+i} style={{ fontSize:10, color:C.textSec, padding:"3px 6px",
                  background:C.bg, border:`1px solid ${C.borderSoft}`, borderRadius:3, marginBottom:3 }}>
                  {s}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ERRORS + FLAGS below the suggestions */}
      {hasErrors && (
        <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1.5, margin:"8px 4px 4px", textTransform:"uppercase" }}>
          Error log
        </div>
      )}
      {(errors || []).map((e, i) => {
        const isOpen = expanded === i;
        const isFlag = _FLAG_KINDS.has(e.signal);
        const isErr  = e.signal === "ERROR";
        const accent = isErr ? C.red : C.amber;
        const label  = _FLAG_LABEL[e.signal] || e.signal || "?";
        const message = e.message || (isFlag ? e.reasoning : "");
        return (
          <div key={i} style={{ ...card, marginBottom:6, borderLeft:`3px solid ${accent}` }}>
            <div style={{ display:"flex", alignItems:"center", gap:10, cursor:"pointer", flexWrap:"wrap" }}
                 onClick={() => setExpanded(isOpen ? null : i)}>
              <span style={{ fontSize:9, fontWeight:700, color:accent, letterSpacing:1 }}>
                {label}
              </span>
              {e.source && (
                <span style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1,
                  background:C.bg, padding:"1px 6px", borderRadius:3, border:`1px solid ${C.borderSoft}` }}>
                  {e.source}
                </span>
              )}
              {e.bar_time && <span style={{ fontSize:10, color:C.muted }}>{e.bar_time}</span>}
              {e.bar_num !== "" && e.bar_num != null && (
                <span style={{ fontSize:10, color:C.muted }}>Bar #{e.bar_num}</span>
              )}
              {isFlag && message && (
                <span style={{ fontSize:10, color:C.text, flex:"1 1 200px", minWidth:0,
                  whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis" }}>
                  {message}
                </span>
              )}
              <span style={{ marginLeft:"auto", fontSize:9, color:C.muted }}>{isOpen?"▲":"▼"}</span>
            </div>
            {isOpen && (
              <div style={{ marginTop:8, borderTop:`1px solid ${C.border}`, paddingTop:8 }}>
                {isFlag && message ? (
                  <div style={{ marginBottom:6 }}>
                    <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:3 }}>FLAG MESSAGE</div>
                    <pre style={{ fontSize:11, color:C.text, whiteSpace:"pre-wrap", margin:0,
                      background:C.amberBg, border:`1px solid ${C.amberBorder}`, padding:8, borderRadius:4 }}>{message}</pre>
                  </div>
                ) : (
                  e.reasoning && (
                    <div style={{ marginBottom:6 }}>
                      <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:3 }}>REASONING</div>
                      <pre style={{ fontSize:10, color:C.text, whiteSpace:"pre-wrap", margin:0 }}>{e.reasoning}</pre>
                    </div>
                  )
                )}
                {e.raw_response && (
                  <div>
                    <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:3 }}>
                      {isFlag ? "RAW RESPONSE EXCERPT" : "RAW RESPONSE"}
                    </div>
                    <pre style={{ fontSize:10, color:isErr?C.red:C.text, whiteSpace:"pre-wrap", margin:0, maxHeight:300, overflow:"auto",
                      background:isErr?"#1a0a0a":C.bg, padding:8, borderRadius:4, border:`1px solid ${C.borderSoft}` }}>{e.raw_response}</pre>
                  </div>
                )}
                <div style={{ marginTop:6, fontSize:9, color:C.muted }}>Logged {e.logged_at_str}</div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
//  Main App
// ══════════════════════════════════════════════════════════════
function App() {
  // ── Core state ───────────────────────────────────────────────
  const [connected,      setConnected]      = useState(false);
  const [price,          setPrice]          = useState(null);
  const [winStartPrice,  setWinStartPrice]  = useState(null);
  const [winStartTime,   setWinStartTime]   = useState(null);
  const [timeLeft,       setTimeLeft]       = useState(300);
  const [strategies,     setStrategies]     = useState({});
  const [deepseekPred,   setDeepseekPred]   = useState(null);
  const [deepseekAcc,    setDeepseekAcc]    = useState(null);
  const [agreeAcc,       setAgreeAcc]       = useState(null);
  const [deepseekLog,    setDeepseekLog]    = useState([]);
  const [backtest,       setBacktest]       = useState(null);
  const [preds,          setPreds]          = useState([]);
  const [weights,        setWeights]        = useState({});

  const [pendingDeepseekReady,  setPendingDeepseekReady]  = useState(false);
  const [pendingDeepseekPred,   setPendingDeepseekPred]   = useState(null);
  const [serviceUnavailable,    setServiceUnavailable]    = useState(false);
  const [serviceUnavailReason,  setServiceUnavailReason]  = useState("");
  const [binanceExpert,         setBinanceExpert]         = useState(null);
  const [historicalAnalysis,    setHistoricalAnalysis]    = useState("");
  const [barTrendAnalyst,       setBarTrendAnalyst]       = useState(null);
  const [tab,                   setTab]                   = useState(
    // If the URL arrives with #sources (from a briefing pill), land the
    // user on the public SOURCES tab — no admin login required.
    (typeof window !== "undefined" && window.location.hash === "#sources") ? "sources" : "live"
  );
  const [isAdmin,               setIsAdmin]               = useState(false);
  const [adminChecked,          setAdminChecked]          = useState(false);
  const [adminLoginError,       setAdminLoginError]       = useState("");
  const [adminPasswordInput,    setAdminPasswordInput]    = useState("");
  const [adminLoginBusy,        setAdminLoginBusy]        = useState(false);
  const [expandedAdminSection,  setExpandedAdminSection]  = useState("");   // "" | history | ensemble | timing | embed_audit | errors
  const [errorLog,              setErrorLog]              = useState([]);
  const [backendSnap,    setBackendSnap]    = useState(null);
  const [allAccuracy,    setAllAccuracy]    = useState(null);
  const [allAccuracyErr, setAllAccuracyErr] = useState(false);
  const [embeddingAuditLog, setEmbeddingAuditLog] = useState([]);
  const [deepseekInspect,   setDeepseekInspect]   = useState(null);
  const [timings,           setTimings]           = useState({ current: {}, history: [], history_total: 0 });
  const wsRef           = useRef(null);
  const reconnectRef    = useRef(null);
  const prevDsWindowRef = useRef(null);

  // ── Microstructure live state ─────────────────────────────────
  // (displayed live; DeepSeek gets a fresh snapshot at each bar open via Python backend)
  const [ob,  setOb]  = useState(null);  // order book
  const [ls,  setLs]  = useState(null);  // long/short ratio
  const [tk,  setTk]  = useState(null);  // taker flow
  const [oif, setOif] = useState(null);  // OI + funding
  const [lq,  setLq]  = useState(null);  // liquidations
  const [fg,  setFg]  = useState(null);  // fear & greed
  const [mp,  setMp]  = useState(null);  // mempool
  const [cz,  setCz]  = useState(null);  // Coinalyze (via proxy)
  const [cg,  setCg]  = useState(null);  // CoinGecko

  // dot status: null=pending, "live", "err"
  const [dots, setDots] = useState({});
  function dot(key, st) { setDots(d => ({ ...d, [key]: st })); }

  // ── WebSocket ──────────────────────────────────────────────────
  useEffect(() => {
    function connect() {
      const proto = location.protocol==="https:"?"wss:":"ws:";
      const ws = new WebSocket(`${proto}//${location.host}/ws`);
      wsRef.current = ws;
      ws.onopen  = () => { setConnected(true); clearTimeout(reconnectRef.current); };
      ws.onclose = () => { setConnected(false); reconnectRef.current=setTimeout(connect,3000); };
      ws.onerror = () => ws.close();
      ws.onmessage = (e) => {
        let d;
        try { d = JSON.parse(e.data); } catch(_) { return; }
        if (d.type!=="tick" || !d.price) return;
        setPrice(d.price);
        if (d.window_start_price!=null) setWinStartPrice(d.window_start_price);
        if (d.window_start_time!=null)  setWinStartTime(d.window_start_time);
        if (d.strategies && Object.keys(d.strategies).length) setStrategies(d.strategies);
        if (d.deepseek_prediction)                    setDeepseekPred(d.deepseek_prediction);
        if (d.pending_deepseek_prediction)            setPendingDeepseekPred(d.pending_deepseek_prediction);
        if (d.agree_accuracy)                         setAgreeAcc(d.agree_accuracy);
        if (d.pending_deepseek_ready !== undefined)   setPendingDeepseekReady(d.pending_deepseek_ready);
        if (d.service_unavailable !== undefined)      setServiceUnavailable(!!d.service_unavailable);
        if (d.service_unavailable_reason !== undefined) setServiceUnavailReason(d.service_unavailable_reason || "");
        if (d.bar_binance_expert && d.bar_binance_expert.signal) setBinanceExpert(d.bar_binance_expert);
        if (typeof d.bar_historical_analysis === "string") setHistoricalAnalysis(d.bar_historical_analysis);
        if (d.bar_trend_analyst !== undefined) setBarTrendAnalyst(d.bar_trend_analyst);
        // Live dashboard_signals from the WS tick — every user (admin OR
        // anon) gets the current order_book / whale_flow / funding / OI /
        // liquidations / basis / skew values the metric() lookup reads.
        // Previously only /backend (admin-only) populated backendSnap, so
        // non-admin viewers saw "source unavailable" on every pill whose
        // metric lived under dashboard_signals.
        if (d.dashboard_signals !== undefined) {
          setBackendSnap(prev => {
            const prevDash = prev?.snapshot?.dashboard_signals;
            // Skip setState if content hasn't changed — avoids forcing a
            // re-render on every 1s tick when dashboard values are stable.
            if (prevDash && JSON.stringify(prevDash) === JSON.stringify(d.dashboard_signals)) {
              return prev;
            }
            return {
              ...(prev || {}),
              snapshot: {
                ...((prev && prev.snapshot) || {}),
                dashboard_signals: d.dashboard_signals,
              },
            };
          });
        }
      };
    }
    connect();
    return () => { wsRef.current?.close(); clearTimeout(reconnectRef.current); };
  }, []);

  useEffect(() => {
    const id = setInterval(() => {
      const ref = winStartTime ?? (Math.floor(Date.now()/1000/300)*300);
      setTimeLeft(Math.max(0, Math.round(300-(Date.now()/1000-ref))));
    }, 500);
    return () => clearInterval(id);
  }, [winStartTime]);

  // ── DeepSeek status poll (2s fallback — catches whatever WS misses) ─────────
  useEffect(() => {
    async function pollDS() {
      try {
        const r = await fetch("/deepseek-status");
        if (!r.ok) return;
        const d = await r.json();
        if (d.pending_deepseek_prediction) setPendingDeepseekPred(d.pending_deepseek_prediction);
        if (d.pending_deepseek_ready !== undefined) setPendingDeepseekReady(d.pending_deepseek_ready);
        if (d.deepseek_prediction)         setDeepseekPred(d.deepseek_prediction);
        if (d.service_unavailable !== undefined)     setServiceUnavailable(!!d.service_unavailable);
        if (d.service_unavailable_reason !== undefined) setServiceUnavailReason(d.service_unavailable_reason || "");
        if (d.bar_trend_analyst !== undefined) setBarTrendAnalyst(d.bar_trend_analyst);
      } catch(_) {}
    }
    pollDS();
    const id = setInterval(pollDS, 2000);
    return () => clearInterval(id);
  }, []);

  // ── Admin auth status (checked on load, refreshed periodically) ───
  useEffect(() => {
    const check = () => fetch("/admin/status")
      .then(r => r.ok ? r.json() : { authenticated: false })
      .then(d => { setIsAdmin(!!d.authenticated); setAdminChecked(true); })
      .catch(() => { setIsAdmin(false); setAdminChecked(true); });
    check();
    const id = setInterval(check, 60000);
    return () => clearInterval(id);
  }, []);

  // ── REST polling (30s) ────────────────────────────────────────
  // Public endpoints always poll. Admin-gated endpoints only poll when
  // logged in — without that, they 401 and pollute the console.
  useEffect(() => {
    const safe = (url, setter) =>
      fetch(url).then(r => r.ok ? r.json() : null).then(d => { if (d != null) setter(d); }).catch(()=>{});
    function poll() {
      safe("/weights",                 setWeights);
      safe("/deepseek/accuracy",       setDeepseekAcc);
      safe("/accuracy/agree",          setAgreeAcc);
      if (isAdmin) {
        safe("/backtest",                 setBacktest);
        safe("/predictions/recent?n=500", setPreds);
        safe("/deepseek/predictions?n=500", setDeepseekLog);
      }
    }
    poll();
    const id = setInterval(poll, 30000);
    // If the browser throttles the tab while hidden (Chrome clamps
    // setInterval to ~1 min when backgrounded), stats freeze at the last
    // poll. Force a fresh poll the instant the user refocuses the tab so
    // they see up-to-date W/L counts immediately on return.
    const onVis = () => { if (!document.hidden) poll(); };
    document.addEventListener("visibilitychange", onVis);
    window.addEventListener("focus", onVis);
    return () => {
      clearInterval(id);
      document.removeEventListener("visibilitychange", onVis);
      window.removeEventListener("focus", onVis);
    };
  }, [isAdmin]);

  // ── Backend snapshot — fetch on tab switch + new DS window ───
  useEffect(() => {
    if (tab !== "backend") return;
    fetch("/backend").then(r=>r.json()).then(setBackendSnap).catch(()=>{});
  }, [tab]);

  // ── Error log — fetch when ERRORS section expanded ───────────
  useEffect(() => {
    if (expandedAdminSection !== "errors") return;
    fetch("/errors").then(r=>r.ok?r.json():null).then(d=>{ if(d) setErrorLog(d.errors||[]); }).catch(()=>{});
  }, [expandedAdminSection]);

  // ── All accuracy — fetch on ensemble tab switch + refresh every 20s ──
  const fetchAllAccuracy = React.useCallback(() => {
    fetch("/accuracy/all?n=200")
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => { if (d?.error) { console.error("[accuracy/all server error]", d.error); setAllAccuracyErr(true); } else if (d) { setAllAccuracy(d); setAllAccuracyErr(false); } else { setAllAccuracyErr(true); } })
      .catch(e => { console.error("[accuracy/all]", e); setAllAccuracyErr(true); });
  }, []);
  // Fetch accuracy data when EITHER the public SOURCES tab is open OR an
  // admin-panel section that needs it is expanded. Previously only the
  // admin path triggered the fetch, so the public SOURCES tab hung forever
  // on "Loading accuracy data…".
  const accuracyNeeded = (tab === "sources")
                      || (expandedAdminSection === "ensemble")
                      || (expandedAdminSection === "history");
  useEffect(() => {
    if (!accuracyNeeded) return;
    fetchAllAccuracy();
  }, [accuracyNeeded, fetchAllAccuracy]);
  useEffect(() => {
    if (!accuracyNeeded) return;
    const id = setInterval(fetchAllAccuracy, 20000);
    return () => clearInterval(id);
  }, [accuracyNeeded, fetchAllAccuracy]);

  // ── Embedding audit — fetch when EMBED AUDIT section expanded ──
  useEffect(() => {
    if (expandedAdminSection !== "embed_audit") return;
    fetch("/api/embedding-audit").then(r=>r.ok?r.json():null).then(d=>{ if(d) setEmbeddingAuditLog(d.audit_log||[]); }).catch(()=>{});
    fetch("/api/inspect/last-deepseek").then(r=>r.ok?r.json():null).then(d=>{ if(d) setDeepseekInspect(d); }).catch(()=>{});
  }, [expandedAdminSection]);

  // ── Timing — fetch when TIMING section expanded; poll every 3s ─
  useEffect(() => {
    if (expandedAdminSection !== "timing") return;
    const load = () => fetch("/api/timings").then(r=>r.ok?r.json():null).then(d=>{ if(d) setTimings(d); }).catch(()=>{});
    load();
    const id = setInterval(load, 3000);
    return () => clearInterval(id);
  }, [expandedAdminSection]);

  useEffect(() => {
    const wc = deepseekPred?.window_count;
    if (!wc || wc === prevDsWindowRef.current) return;
    prevDsWindowRef.current = wc;
    fetch("/backend").then(r=>r.json()).then(setBackendSnap).catch(()=>{});
  }, [deepseekPred?.window_count]);

  // ── Microstructure fetch functions ────────────────────────────
  // These mirror the btc-dashboard logic (now integrated here).
  // The Python backend independently fetches fresh snapshots at each bar open
  // for DeepSeek; these functions keep the UI live between bar opens.

  const fetchOB = useCallback(async () => {
    try {
      const [t, d] = await Promise.all([
        fetch("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT").then(r=>r.json()),
        fetch("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20").then(r=>r.json()),
      ]);
      const bv = d.bids.reduce((a,[,q])=>a+parseFloat(q),0);
      const av = d.asks.reduce((a,[,q])=>a+parseFloat(q),0);
      const imb = (bv+av)>0 ? ((bv-av)/(bv+av))*100 : 0;
      setOb({ bv, av, imb,
        price: +t.lastPrice, ch:+t.priceChangePercent,
        hi:+t.highPrice, lo:+t.lowPrice, vol:+t.quoteVolume,
        sig: imb>5?"BULLISH":imb<-5?"BEARISH":"NEUTRAL" });
      dot("ob","live");
    } catch { dot("ob","err"); }
  }, []);

  const fetchLS = useCallback(async () => {
    try {
      const [gl, tp] = await Promise.all([
        fetch("https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1").then(r=>r.json()),
        fetch("https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1").then(r=>r.json()),
      ]);
      const g=gl[0]||{}, tp0=tp[0]||{};
      const lsr  = parseFloat(g.longShortRatio||1);
      const lp   = parseFloat(g.longAccount||0.5)*100;
      const tlp  = parseFloat(tp0.longAccount||0.5)*100;
      // Contrarian: too many retail longs = fade signal
      const rSig = lsr>1.35?"BEARISH_CONTRARIAN":lsr<0.75?"BULLISH_CONTRARIAN":"NEUTRAL";
      const sSig = tlp>60?"BULLISH":tlp<40?"BEARISH":"NEUTRAL";
      setLs({ lsr, retailLong:lp, retailShort:100-lp, smartLong:tlp, smartShort:100-tlp, rSig, sSig, div:lp-tlp });
      dot("ls","live");
    } catch { dot("ls","err"); }
  }, []);

  const fetchTaker = useCallback(async () => {
    // Mirror the backend chain — taker flow uses Kraken spot Trades so frontend
    // pill values match the numbers DeepSeek sees. Binance fAPI would return a
    // different market (perp) AND inconsistently reachable from Render. Kraken
    // side flag: "b" = market-buy (buyer was taker), "s" = market-sell (taker sold).
    try {
      const sinceNs = Math.floor((Date.now()/1000 - 360) * 1e9);   // last ~6 min, buffer
      const d = await fetch(`https://api.kraken.com/0/public/Trades?pair=XBTUSD&since=${sinceNs}`).then(r=>r.json());
      const rows = (d?.result?.XXBTZUSD || d?.result?.XBTUSD || []);
      if (!rows.length) throw new Error("empty Kraken trades");
      const cutoff = Date.now()/1000 - 300;   // strict 5-min aggregation window
      let bv = 0, sv = 0;
      for (const t of rows) {
        const ts = parseFloat(t[2]); if (ts < cutoff) continue;
        const vol = parseFloat(t[1]);
        if (t[3] === "b") bv += vol;
        else if (t[3] === "s") sv += vol;
      }
      if (bv + sv < 0.01) throw new Error("Kraken volume too small");
      const bsr = sv > 0 ? bv / sv : 1.0;
      const sig = bsr > 1.12 ? "BULLISH" : bsr < 0.90 ? "BEARISH" : "NEUTRAL";
      setTk({ bsr, bv, sv, sig, trend: "N/A" });
      dot("tk","live");
    } catch { dot("tk","err"); }
  }, []);

  const fetchOIF = useCallback(async () => {
    try {
      const [oi, pi] = await Promise.all([
        fetch("https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT").then(r=>r.json()),
        fetch("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT").then(r=>r.json()),
      ]);
      const oiv  = +oi.openInterest;
      const fr   = +pi.lastFundingRate;
      const mp_  = +pi.markPrice;
      const ip   = +pi.indexPrice;
      const prem = ip ? ((mp_-ip)/ip)*100 : 0;
      const frSig  = fr>0.0006?"BEARISH":fr<0?"BULLISH":"NEUTRAL";
      const pSig   = prem>0.03?"BEARISH":prem<-0.03?"BULLISH":"NEUTRAL";
      setOif({ oi:oiv, fr, markPrice:mp_, indexPrice:ip, premium:prem, frSig, pSig,
        nextFund:new Date(+pi.nextFundingTime).toLocaleTimeString() });
      dot("oif","live");
    } catch { dot("oif","err"); }
  }, []);

  const fetchLQ = useCallback(async () => {
    try {
      const d = await fetch("/api/proxy/okx-liquidations").then(r=>r.json());
      const rows = (d.data||[]).flatMap(e=>e.details||[]);
      const cutoff = Date.now() - 300_000;
      const window = rows.filter(r=>+r.ts>=cutoff).length ? rows.filter(r=>+r.ts>=cutoff) : rows;
      const longs  = window.filter(r=>(r.posSide||"").toLowerCase()==="long");
      const shorts = window.filter(r=>(r.posSide||"").toLowerCase()==="short");
      const lvol   = longs.reduce((s,r)=>s+parseFloat(r.sz||0)*parseFloat(r.bkPx||0),0);
      const svol   = shorts.reduce((s,r)=>s+parseFloat(r.sz||0)*parseFloat(r.bkPx||0),0);
      const sig    = lvol>svol*1.5?"BEARISH":svol>lvol*1.5?"BULLISH":"NEUTRAL";
      setLq({ total:window.length, longCount:longs.length, shortCount:shorts.length, lvol, svol, sig });
      dot("lq","live");
    } catch { dot("lq","err"); }
  }, []);

  const fetchFG = useCallback(async () => {
    try {
      const d = await fetch("https://api.alternative.me/fng/?limit=2").then(r=>r.json());
      const cur=d.data[0], prev=d.data[1];
      const v=+cur.value, pv=prev?+prev.value:v;
      const sig = v<30?"BULLISH_CONTRARIAN":v>75?"BEARISH_CONTRARIAN":"NEUTRAL";
      setFg({ value:v, label:cur.value_classification, prev:pv, delta:v-pv, sig });
      dot("fg","live");
    } catch { dot("fg","err"); }
  }, []);

  const fetchMP = useCallback(async () => {
    try {
      const [fees, mp_] = await Promise.all([
        fetch("https://mempool.space/api/v1/fees/recommended").then(r=>r.json()),
        fetch("https://mempool.space/api/mempool").then(r=>r.json()),
      ]);
      const ff=fees.fastestFee, hf=fees.halfHourFee, of_=fees.hourFee;
      const sig = ff>50?"BEARISH":ff<10?"BULLISH":"NEUTRAL";
      setMp({ fastest:ff, halfHour:hf, hour:of_, count:mp_.count, size:mp_.vsize/1e6, sig });
      dot("mp","live");
    } catch { dot("mp","err"); }
  }, []);

  const fetchCZ = useCallback(async () => {
    try {
      const d = await fetch("/api/proxy/coinalyze").then(r=>r.json());
      if (d.error) throw new Error(d.error);
      const items = Array.isArray(d)?d:d?.data||[];
      if (!items.length) throw new Error("empty");
      const frv=parseFloat(items[0].fr??items[0].value??items[0].funding_rate??0);
      const sig=frv>0.0005?"BEARISH":frv<0?"BULLISH":"NEUTRAL";
      setCz({ fr:frv, sig });
      dot("cz","live");
    } catch { dot("cz","err"); }
  }, []);

  const fetchGecko = useCallback(async () => {
    try {
      const d = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true").then(r=>r.json());
      const b=d.bitcoin;
      setCg({ mcap:b.usd_market_cap, vol:b.usd_24h_vol, ch:b.usd_24h_change, vm:b.usd_24h_vol/b.usd_market_cap*100 });
      dot("cg","live");
    } catch { dot("cg","err"); }
  }, []);

  // ── Initial fetch + intervals ─────────────────────────────────
  useEffect(() => {
    fetchOB(); fetchLS(); fetchTaker(); fetchOIF(); fetchLQ(); fetchFG(); fetchMP(); fetchCZ(); fetchGecko();
    const ids = [
      setInterval(fetchOB,    5000),
      setInterval(fetchLS,   15000),
      setInterval(fetchTaker,15000),
      setInterval(fetchOIF,  15000),
      setInterval(fetchLQ,   30000),
      setInterval(fetchFG,  300000),
      setInterval(fetchMP,   60000),
      setInterval(fetchCZ,   60000),
      setInterval(fetchGecko,60000),
    ];
    return () => ids.forEach(clearInterval);
  }, [fetchOB,fetchLS,fetchTaker,fetchOIF,fetchLQ,fetchFG,fetchMP,fetchCZ,fetchGecko]);

  // ── Derived values ────────────────────────────────────────────
  const priceDelta = (price&&winStartPrice) ? price-winStartPrice : 0;
  const pricePct   = winStartPrice ? priceDelta/winStartPrice*100 : 0;
  const totalPreds     = backtest?.total_predictions??0;
  const correctPreds   = backtest?.correct_predictions??0;
  const accuracy       = totalPreds>0?correctPreds/totalPreds*100:0;
  const allTimeTotal   = backtest?.all_time_total??0;
  const allTimeCorrect = backtest?.all_time_correct??0;
  const allTimeNeutral = backtest?.all_time_neutral??0;
  const allTimeAccuracy= allTimeTotal>0?allTimeCorrect/allTimeTotal*100:0;
  const mins  = String(Math.floor(timeLeft/60)).padStart(2,"0");
  const secs  = String(timeLeft%60).padStart(2,"0");
  // Bar close time in HH:MM UTC — matches TradingView format
  const barCloseUTC = winStartTime
    ? (() => {
        const d = new Date((winStartTime + 300) * 1000);
        return String(d.getUTCHours()).padStart(2,"0") + ":" + String(d.getUTCMinutes()).padStart(2,"0") + " UTC";
      })()
    : "--:-- UTC";
  // useMemo stabilizes the reference across 500ms timer re-renders — without this,
  // every tick creates a new activeDeepseekPred even when the underlying data is
  // unchanged, which caused the DeepSeek card content to flash (React saw the
  // conditional branches as unstable and remounted SignalRow).
  const activeDeepseekPred = useMemo(
    () => (pendingDeepseekReady && pendingDeepseekPred) ? pendingDeepseekPred : deepseekPred,
    [pendingDeepseekReady, pendingDeepseekPred, deepseekPred]
  );

  // briefingJSX — memoized rendering of the live DeepSeek analysis card.
  // Top: NARRATIVE split into bold takeaway + detail (same visual format
  // as before). Below: Binance microstructure expert + Historical
  // similarity analyst summaries. Below: numbered REASONS list. Below:
  // FREE_OBSERVATION. Plain text + BoldAnalysis highlighting — no
  // structured pills, no condition machinery, no Venice translation.
  const briefingJSX = useMemo(() => {
    const _dsBad = (s) => s === "ERROR" || s === "UNAVAILABLE";
    const ds = activeDeepseekPred;
    const ready = pendingDeepseekReady && ds && !_dsBad(ds.signal);

    if (!ready) {
      return (
        <div style={{ ...card, flexShrink:0, padding:"12px 14px",
          display:"flex", alignItems:"center", justifyContent:"space-between", gap:10, flexWrap:"wrap" }}>
          <span style={{ fontSize:14, lineHeight:1.4 }}>
            {pendingDeepseekReady && ds?.signal === "ERROR" ? (
              <span style={{ color:C.red, fontWeight:700 }}>⚠️ Analysis error this bar — detail in History tab</span>
            ) : (
              <span style={{ color:C.amber }}>⟳ Analyzing new bar — analysis in ~30–60s</span>
            )}
          </span>
          {barCloseUTC && (
            <span style={{ fontSize:14, fontWeight:800, color:"#15803D",
              background:"#F0FDF4", border:"1px solid #86EFAC",
              borderRadius:5, padding:"3px 10px" }}>
              Closes {barCloseUTC}
            </span>
          )}
        </div>
      );
    }

    // Split NARRATIVE into a bold takeaway + a regular-weight detail
    // paragraph at the earliest of (em-dash, en-dash, sentence-end) past
    // position 20, so the eye lands on a single punchy idea first.
    const narrative = (ds.narrative || "").trim();
    let takeaway = narrative, detail = "";
    if (narrative) {
      const cands = [];
      const em   = narrative.indexOf(" — ");
      const dash = narrative.indexOf(" – ");
      if (em   >= 20) cands.push({ at: em,   skip: 3 });
      if (dash >= 20) cands.push({ at: dash, skip: 3 });
      const sent = narrative.match(/[.!?]\s+[A-Z0-9$]/);
      if (sent && sent.index >= 20) cands.push({ at: sent.index + 1, skip: 1 });
      cands.sort((a, b) => a.at - b.at);
      const first = cands[0];
      if (first && first.at < narrative.length - 4) {
        takeaway = narrative.slice(0, first.at).trim().replace(/[.!?]$/, "");
        detail   = narrative.slice(first.at + first.skip).trim();
      }
    }

    // REASONS — DeepSeek emits either:
    //   (new)  ARGUMENT: ... / COUNTER: ... / SURVIVES_STEELMAN: YES|NO ...
    //   (old)  4 lines like "MICROSTRUCTURE: ...", "SYNTHESIS: ...", etc.
    // Try new structured schema first; fall back to numbered-list if not found.
    const reasoningRaw = (ds.reasoning || "").trim();
    const structuredReasoning = parseStructuredReasoning(reasoningRaw);
    const reasonItems = structuredReasoning.isStructured
      ? []   // new bars: ARGUMENT/COUNTER rendered by SteelmanDecisionBlock above
      : reasoningRaw
          .split("\n")
          .map(s => s.trim())
          .filter(Boolean)
          .map((s, i) => {
            const stripped = s.replace(/^\d+\.\s*/, "");
            const m = stripped.match(/^([A-Z][A-Z0-9 +/_-]{1,40}):\s*(.+)$/);
            return {
              num:   String(i + 1),
              label: m ? m[1].trim() : "",
              text:  m ? m[2].trim() : stripped,
            };
          });

    // Binance microstructure expert — render every populated field as its
    // own labeled row. Falls back to legacy `analysis`/`reasoning` blob if
    // the structured fields are missing (older stored bars).
    const beRows = [];
    if (binanceExpert) {
      const beFieldOrder = [
        ["taker_flow",  "Taker Flow"],
        ["positioning", "Positioning"],
        ["whale_flow",  "Whale Flow"],
        ["oi_funding",  "OI / Funding"],
        ["order_book",  "Order Book"],
        ["confluence",  "Confluence"],
      ];
      for (const [k, lab] of beFieldOrder) {
        const v = (binanceExpert[k] || "").toString().trim();
        if (v) beRows.push({ key: k, label: lab, text: v });
      }
    }
    const beEdge   = (binanceExpert?.edge  || "").toString().trim();
    const beWatch  = (binanceExpert?.watch || "").toString().trim();
    const beLegacy = !beRows.length && !beEdge && !beWatch
      ? ((binanceExpert?.analysis || binanceExpert?.reasoning || binanceExpert?.narrative || "").toString().trim())
      : "";
    const beHasContent = beRows.length > 0 || beEdge || beWatch || beLegacy;
    const histSummary = (historicalAnalysis || "").trim();

    return (
      <div style={{ width:"100%", minWidth:360, display:"flex", flexDirection:"column", gap:8 }}>
        <div style={{ ...card, flexShrink:0, padding:"14px 16px",
          background:"#FAFAF9", border:`2px solid ${C.borderSoft}` }}>

          {/* Header */}
          <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:10 }}>
            <span style={{ fontSize:12, fontWeight:900, color:C.text,
              letterSpacing:1.2, textTransform:"uppercase" }}>⚡ DeepSeek Analysis</span>
            <span style={{ fontSize:10, color:C.muted, fontStyle:"italic" }}>
              5-minute directional read
            </span>
          </div>

          {/* CURRENT ONGOING TREND — 20-bar qualitative synthesizer.
              Renders before the DeepSeek narrative so it's the first
              qualitative thing the user reads on each bar. */}
          <CurrentOngoingTrendPanel trendAnalyst={barTrendAnalyst} />

          {/* TOP — narrative takeaway + detail */}
          {narrative && (
            <div style={{ marginBottom: ds.free_observation ? 10 : 14 }}>
              <div style={{
                width:"100%", maxWidth:"100%", overflowWrap:"anywhere", wordBreak:"break-word",
                fontSize:17, color:C.text, lineHeight:1.5, fontWeight:700
              }}>
                <BoldAnalysis text={takeaway} color={C.text} />
              </div>
              {detail && (
                <div style={{
                  width:"100%", maxWidth:"100%", overflowWrap:"anywhere", wordBreak:"break-word",
                  fontSize:15, color:C.text, lineHeight:1.6, fontWeight:400, marginTop:8
                }}>
                  <BoldAnalysis text={detail} color={C.text} />
                </div>
              )}
            </div>
          )}

          {/* FREE OBSERVATION — sits with the narrative at the top */}
          {ds.free_observation && (
            <div style={{ marginBottom:14, padding:"8px 10px",
              background:C.amberBg, border:`1px solid ${C.amberBorder}`,
              borderRadius:5 }}>
              <div style={{ fontSize:9, fontWeight:800, color:C.amber,
                letterSpacing:1, marginBottom:3, textTransform:"uppercase" }}>
                Free Observation
              </div>
              <div style={{ fontSize:12, color:C.amber, lineHeight:1.55,
                fontStyle:"italic" }}>
                {ds.free_observation}
              </div>
            </div>
          )}

          {/* STEELMAN DECISION — new ARGUMENT/COUNTER/SURVIVES block.
              Rendered AFTER the narrative, BEFORE Binance Microstructure.
              SteelmanDecisionBlock returns null for legacy bars. */}
          <SteelmanDecisionBlock signal={ds.signal} sr={structuredReasoning} />

          {/* BINANCE MICROSTRUCTURE — peer section.
              One KvBlock per populated field so each driver gets its own
              labeled row instead of being collapsed into a single span. */}
          {beHasContent && (
            <BriefingSection
              title="Binance Microstructure"
              verdict={binanceExpert?.signal}
            >
              {beRows.map(r => (
                <KvBlock key={r.key} label={r.label}>
                  <BoldAnalysis text={r.text} color={C.text} />
                </KvBlock>
              ))}
              {beEdge && (
                <KvBlock label="Edge"
                  color={posColors(binanceExpert?.signal).color}>
                  <BoldAnalysis text={beEdge} color={C.text} />
                </KvBlock>
              )}
              {beWatch && (
                <KvBlock label="Watch" color={C.muted}>
                  <span style={{ fontStyle:"italic", color:C.textSec }}>
                    <BoldAnalysis text={beWatch} color={C.textSec} />
                  </span>
                </KvBlock>
              )}
              {beLegacy && (
                <div style={{ fontSize:13, color:C.text, lineHeight:1.6 }}>
                  <BoldAnalysis text={beLegacy} color={C.text} />
                </div>
              )}
            </BriefingSection>
          )}

          {/* HISTORICAL SIMILARITY — peer section. Strict POSITION:/CONFIDENCE:
              schema gets the structured render; everything else falls through
              to the generic markdown renderer. */}
          {histSummary && (() => {
            if (isStrictHistSchema(histSummary)) {
              const h = parseHistAnalysis(histSummary);
              return (
                <BriefingSection
                  title="Historical Similarity"
                  verdict={h.position}
                  confidence={h.confidence}
                >
                  {h.lean && (
                    <div style={{ fontSize:14, fontWeight:600, color:C.text,
                      lineHeight:1.55, marginBottom:10 }}>
                      <BoldAnalysis text={h.lean} color={C.text} />
                    </div>
                  )}
                  {h.baseRates && (
                    <div style={{ fontSize:11, color:C.textSec,
                      background:"#FFFFFF", border:`1px solid ${C.borderSoft}`,
                      borderRadius:5, padding:"6px 10px",
                      fontVariantNumeric:"tabular-nums" }}>
                      <BoldAnalysis text={h.baseRates} color={C.textSec} />
                    </div>
                  )}
                  {h.precedents.length > 0 && (
                    <>
                      <SubLabel>Precedents · Tier A</SubLabel>
                      <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
                        {h.precedents.map((p, i) => (
                          <PrecedentCard key={i} p={p} />
                        ))}
                      </div>
                    </>
                  )}
                  {h.against && (
                    <KvBlock label="Against" color={C.red}>
                      <BoldAnalysis text={h.against} color={C.text} />
                    </KvBlock>
                  )}
                  {h.ensemble && (
                    <KvBlock label="Ensemble Reliability">
                      <BoldAnalysis text={h.ensemble} color={C.text} />
                    </KvBlock>
                  )}
                  {h.forText && (
                    <KvBlock label="For" color={C.green}>
                      <BoldAnalysis text={h.forText} color={C.text} />
                    </KvBlock>
                  )}
                  {h.devil && (
                    <KvBlock label="Devil's Advocate">
                      <BoldAnalysis text={h.devil} color={C.text} />
                    </KvBlock>
                  )}
                  {h.edge && (
                    <KvBlock label="Edge">
                      <BoldAnalysis text={h.edge} color={C.text} />
                    </KvBlock>
                  )}
                  {h.suggestion && h.suggestion.toUpperCase() !== "NONE" && (
                    <KvBlock label="Suggestion">
                      <BoldAnalysis text={h.suggestion} color={C.text} />
                    </KvBlock>
                  )}
                  {h.extra && (
                    <div style={{ marginTop:10, fontSize:12, color:C.textSec,
                      lineHeight:1.55, whiteSpace:"pre-wrap" }}>
                      <BoldAnalysis text={h.extra} color={C.textSec} />
                    </div>
                  )}
                </BriefingSection>
              );
            }
            return (
              <BriefingSection title="Historical Similarity">
                <MdBlocks blocks={parseMarkdownBlocks(histSummary)} />
              </BriefingSection>
            );
          })()}

          {/* REASONING — peer section. Each item is a numbered row whose
              right side stacks an optional uppercase LABEL above the body,
              matching the KvBlock pattern used elsewhere in the briefing. */}
          {reasonItems.length > 0 && (
            <BriefingSection title="Reasoning">
              <div style={{ display:"flex", flexDirection:"column", gap:14 }}>
                {reasonItems.map((it, i) => (
                  <div key={i} style={{ display:"flex", gap:12, alignItems:"flex-start" }}>
                    <span style={{
                      fontSize:11, fontWeight:900, color:C.muted,
                      minWidth:18, marginTop: it.label ? 0 : 3,
                      fontVariantNumeric:"tabular-nums",
                      letterSpacing:0.5,
                    }}>
                      {it.num}.
                    </span>
                    <div style={{ flex:1, minWidth:0 }}>
                      {it.label && (
                        <div style={{
                          fontSize:9, fontWeight:800, color:C.muted,
                          letterSpacing:1.2, textTransform:"uppercase",
                          marginBottom:5,
                        }}>{it.label}</div>
                      )}
                      <div style={{ fontSize:13, color:C.text, lineHeight:1.6 }}>
                        <BoldAnalysis text={it.text} color={C.text} />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </BriefingSection>
          )}

          {/* Bar close badge */}
          {barCloseUTC && (
            <div style={{ marginTop:10, display:"flex", justifyContent:"flex-end" }}>
              <span style={{ fontSize:11, fontWeight:800, color:"#15803D",
                background:"#F0FDF4", border:"1px solid #86EFAC",
                borderRadius:5, padding:"3px 10px" }}>
                Closes {barCloseUTC}
              </span>
            </div>
          )}
        </div>
      </div>
    );
  // `price` and the live microstructure feeds (tk/ob/oif/ls) intentionally
  // omitted from deps — they tick every ~1s and putting them in deps would
  // rebuild the entire card tree on every tick.
  }, [pendingDeepseekReady, activeDeepseekPred, binanceExpert, historicalAnalysis, barCloseUTC, barTrendAnalyst]);

  // Cross-exchange divergence (for microstructure display)
  if (serviceUnavailable) return (
    <div style={{ fontFamily:"'JetBrains Mono','Fira Code',monospace", background:"#0F0E0D",
      height:"100vh", display:"flex", alignItems:"center", justifyContent:"center", flexDirection:"column", gap:16 }}>
      <div style={{ fontSize:40 }}>🔧</div>
      <div style={{ fontSize:20, fontWeight:900, color:"#F5F5F4", letterSpacing:2, textTransform:"uppercase" }}>
        Service Temporarily Unavailable
      </div>
      <div style={{ fontSize:12, color:"#A09D99", maxWidth:480, textAlign:"center", lineHeight:1.8 }}>
        The embedding service (Cohere) is currently unreachable. Predictions are paused until connectivity is restored.
        The system will resume automatically — no action needed.
      </div>
      {serviceUnavailReason && (
        <div style={{ fontSize:10, color:"#6B6866", background:"#1C1A18", border:"1px solid #2E2C29",
          borderRadius:6, padding:"6px 14px", maxWidth:520, textAlign:"center", fontFamily:"monospace" }}>
          {serviceUnavailReason}
        </div>
      )}
      <div style={{ marginTop:8, display:"flex", alignItems:"center", gap:8, fontSize:11, color:"#78716C" }}>
        <div style={{ width:6, height:6, borderRadius:"50%", background:"#EF4444",
          animation:"pulse 1.5s ease-in-out infinite" }} />
        Retrying on next bar open…
      </div>
    </div>
  );

  return (
    <div style={{ fontFamily:"'JetBrains Mono','Fira Code',monospace", background:C.bg, color:C.text,
      height:"100vh", overflow:"hidden", fontSize:15, display:"flex", flexDirection:"column" }}>

      {/* (compact top bar removed — timer + tabs moved INTO the DeepSeek AI Analysis
          card header below, next to the accuracy %, per user request. Admin tab
          has its own absolute "← Back to Live" button in its top-right.) */}

      {/* ── BODY ── */}
      <div style={{ flex:1, overflow:"hidden", padding:"6px 10px", background:C.bg }}>

        {/* ══ DASHBOARD ══ */}
        {tab==="live" && (
          <div style={{ display:"flex", gap:6, height:"100%" }}>

            {/* LEFT: Chart — 50/50 split with analysis. Chart is still the
                priority viewport item but the analysis column now gets
                enough room to breathe. */}
            <div style={{ flex:"0 0 50%", minWidth:0, display:"flex", flexDirection:"column" }}>
              <div style={{ ...card, flex:1, display:"flex", flexDirection:"column", minHeight:0 }}>
                <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1.5,
                  textTransform:"uppercase", marginBottom:4, flexShrink:0 }}>
                  BTC/USD · 5m · Binance via TradingView
                </div>
                <div style={{ flex:1, minHeight:0 }}><PriceChart /></div>
              </div>
            </div>

            {/* RIGHT: DeepSeek analysis card — 50% of screen, with a subtle
                zoom:0.85 to scale everything down ~15% since user noted the
                text was feeling big at native size. */}
            <div style={{ flex:"0 0 50%", minWidth:0, display:"flex", flexDirection:"column", gap:5, overflowY:"auto", zoom:0.85 }}>

              {/* ① PREDICTION BAR — DeepSeek only */}
              {(() => {
                function AccuracyRow({ pct, wins, losses, total, label: lbl, noData }) {
                  const neutral = (total != null && total > (wins||0) + (losses||0)) ? total - (wins||0) - (losses||0) : null;
                  const barTotal = (wins||0) + (losses||0) + (neutral||0);
                  const wPct = barTotal > 0 ? (wins||0) / barTotal * 100 : 0;
                  const lPct = barTotal > 0 ? (losses||0) / barTotal * 100 : 0;
                  const nPct = barTotal > 0 && neutral ? neutral / barTotal * 100 : 0;
                  return (<>
                    <div style={{ borderTop:`1px solid ${C.borderSoft}`, margin:"6px 0 4px" }} />
                    <div style={{ ...label, marginBottom:2 }}>{lbl}</div>
                    {noData
                      ? <div style={{ fontSize:10, color:C.muted }}>No historical data</div>
                      : <>
                          <div style={{ display:"flex", alignItems:"baseline", gap:5, flexWrap:"nowrap", overflow:"hidden" }}>
                            <span style={{ fontSize:22, fontWeight:900, color:pct>=50?C.green:C.red }}>{pct.toFixed(1)}%</span>
                            <span style={{ fontSize:10, fontWeight:700, color:C.green }}>{wins||0}W</span>
                            <span style={{ fontSize:10, color:C.muted }}>/</span>
                            <span style={{ fontSize:10, fontWeight:700, color:C.red }}>{losses||0}L</span>
                            {neutral != null && <><span style={{ fontSize:10, color:C.muted }}>/</span><span style={{ fontSize:10, fontWeight:700, color:C.muted }}>{neutral}N</span></>}
                          </div>
                          <div style={{ display:"flex", height:5, borderRadius:3, overflow:"hidden", margin:"4px 0 0", background:C.borderSoft }}>
                            {wPct > 0 && <div style={{ width:`${wPct}%`, background:C.green }} />}
                            {lPct > 0 && <div style={{ width:`${lPct}%`, background:C.red }} />}
                            {nPct > 0 && <div style={{ width:`${nPct}%`, background:C.muted }} />}
                          </div>
                        </>
                    }
                  </>);
                }
                function SignalRow({ sig, reasoning, conf }) {
                  const up      = sig === "UP";
                  const neutral = sig === "NEUTRAL";
                  const clr     = neutral ? C.amber : up ? C.green : C.red;
                  // Derive TAKE/PASS/NEUTRAL from structured reasoning if present.
                  // Fall back to a debug confidence label (small, muted) for compat.
                  const sr = parseStructuredReasoning(reasoning || "");
                  let decisionLabel, decisionBg, decisionColor, decisionBorder;
                  if (neutral) {
                    decisionLabel = "NEUTRAL"; decisionBg = C.amberBg; decisionColor = C.amber; decisionBorder = C.amberBorder;
                  } else if (sr.isStructured) {
                    const survived = sr.survives === "YES";
                    decisionLabel = survived ? "TAKE" : "PASS";
                    decisionBg    = survived ? C.greenBg : C.amberBg;
                    decisionColor = survived ? C.green   : C.amber;
                    decisionBorder= survived ? C.greenBorder : C.amberBorder;
                  } else {
                    // Legacy bar: no structured reasoning — just show TAKE if directional
                    decisionLabel = "TAKE"; decisionBg = C.greenBg; decisionColor = C.green; decisionBorder = C.greenBorder;
                  }
                  return (
                    <div style={{ display:"flex", alignItems:"center", gap:10 }}>
                      <span style={{ fontSize:26, fontWeight:900, color:clr, lineHeight:1 }}>
                        {neutral ? "— NEUTRAL" : up ? "▲ UP" : "▼ DOWN"}
                      </span>
                      <span style={{
                        fontSize:12, fontWeight:900, padding:"3px 10px", borderRadius:4,
                        color:decisionColor, background:decisionBg, border:`1px solid ${decisionBorder}`,
                        letterSpacing:0.8,
                      }}>{decisionLabel}</span>
                      {conf != null && (
                        <span style={{ fontSize:9, color:C.muted, fontVariantNumeric:"tabular-nums" }}>{conf}%</span>
                      )}
                    </div>
                  );
                }
                const colTitle = { fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1.5, textTransform:"uppercase", marginBottom:6 };
                const metaRow = { height:26, display:"flex", alignItems:"center", gap:6, flexWrap:"nowrap", overflow:"hidden" };

                // While pending_deepseek_ready=false (fresh analysis running after bar close)
                // we intentionally render nothing — showing the previous bar's signal during
                // the analysis window can mislead fast traders into trading on a stale call.
                const dsLive = pendingDeepseekReady && activeDeepseekPred
                  && activeDeepseekPred.signal !== "ERROR"
                  && activeDeepseekPred.signal !== "UNAVAILABLE";
                const dsErr  = pendingDeepseekReady && activeDeepseekPred?.signal==="ERROR";
                const c2src  = dsLive ? activeDeepseekPred : null;
                const c2sig  = c2src?.signal || null;
                const c2conf = c2src?.confidence ?? 0;
                const c2meta = c2src ? { label:`#${c2src.window_count} · ${c2src.latency_ms}ms`, aiReq: c2src.data_requests&&c2src.data_requests.toUpperCase()!=="NONE"&&c2src.data_requests.trim()!=="" } : null;

                // Pre-compute accuracy display numbers
                const accPct = (deepseekAcc?.accuracy ?? 0) * 100;
                const wins = deepseekAcc?.correct ?? 0;
                const total = (deepseekAcc?.directional ?? 0) + (deepseekAcc?.neutrals ?? 0);
                const losses = (deepseekAcc?.directional ?? deepseekAcc?.total ?? 0) - wins;
                const neutral = Math.max(0, total - wins - losses);
                const barTotal = wins + losses + neutral;
                const wBarPct = barTotal > 0 ? wins / barTotal * 100 : 0;
                const lBarPct = barTotal > 0 ? losses / barTotal * 100 : 0;
                const nBarPct = barTotal > 0 ? neutral / barTotal * 100 : 0;
                return (
                  <div style={{ ...card, flexShrink:0, padding:"10px 14px" }}>
                    {/* TITLE + signal + meta (lightweight) */}
                    <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", gap:10, marginBottom:4 }}>
                      <div style={colTitle}>DeepSeek AI Analysis</div>
                      {c2meta && (
                        <div style={{ display:"flex", gap:6, alignItems:"center" }}>
                          <span style={{ fontSize:9, color:C.muted }}>{c2meta.label}</span>
                          {c2meta.aiReq && <span style={{ fontSize:9, fontWeight:700, padding:"1px 5px", borderRadius:3, color:C.amber, background:C.amberBg, border:`1px solid ${C.amberBorder}` }}>⚡ AI req</span>}
                        </div>
                      )}
                    </div>
                    {/* Reserved min-height wrapper so the chart below doesn't
                        jump up/down when the signal-row appears/disappears
                        during bar-close → new-bar transitions. Height is
                        sized to SignalRow's natural height (~32px for the
                        text + 3px bar + 3px gap = ~38px). */}
                    <div style={{ minHeight:38, display:"flex", flexDirection:"column", justifyContent:"center" }}>
                      {dsErr ? <div style={{ fontSize:11, color:C.red }}>{activeDeepseekPred.reasoning||"API error"}</div>
                        : c2sig ? <SignalRow sig={c2sig} conf={c2conf} reasoning={c2src?.reasoning} />
                        : <div style={{ fontSize:11, color:C.muted }}>Analyzing…</div>}
                    </div>

                    {/* BIG ROW — accuracy % + wins/losses INLINE WITH countdown + tabs
                        All at matching size so the trader's eye takes everything in at once. */}
                    <div style={{ borderTop:`1px solid ${C.borderSoft}`, marginTop:8, paddingTop:6 }} />
                    <div style={{ display:"flex", alignItems:"baseline", justifyContent:"space-between", gap:14, flexWrap:"wrap" }}>
                      {/* LEFT — DeepSeek accuracy % + counts (always shown so 0/0/0 is the visible starting state) */}
                      <div style={{ display:"flex", alignItems:"baseline", gap:10 }}>
                        <span style={{ fontSize:11, fontWeight:700, color:C.muted, letterSpacing:1.2, textTransform:"uppercase" }}>Accuracy</span>
                        <span style={{ fontSize:28, fontWeight:900, color:(deepseekAcc?.directional ?? 0) === 0 ? C.muted : (accPct>=50?C.green:C.red), letterSpacing:1, lineHeight:1 }}>{(deepseekAcc?.directional ?? 0) === 0 ? "—" : `${accPct.toFixed(1)}%`}</span>
                        <span style={{ fontSize:12, fontWeight:800, color:C.green }}>{wins}W</span>
                        <span style={{ fontSize:12, color:C.muted }}>/</span>
                        <span style={{ fontSize:12, fontWeight:800, color:C.red }}>{losses}L</span>
                        <span style={{ fontSize:12, color:C.muted }}>/</span>
                        <span style={{ fontSize:12, fontWeight:800, color:C.muted }}>{neutral}N</span>
                      </div>
                      {/* RIGHT — countdown + LIVE/ADMIN tabs at matching large size */}
                      <div style={{ display:"flex", alignItems:"baseline", gap:16 }}>
                        <div style={{ display:"flex", alignItems:"baseline", gap:7 }}>
                          <span style={{ fontSize:28, fontWeight:900, fontVariantNumeric:"tabular-nums", lineHeight:1,
                            color:timeLeft<60?C.red:timeLeft<120?C.amber:C.green, letterSpacing:1.5 }}>{mins}:{secs}</span>
                          <span style={{ fontSize:10, color:C.muted, letterSpacing:1.2, textTransform:"uppercase", fontWeight:700 }}>bar closes</span>
                        </div>
                        <div style={{ display:"flex" }}>
                          {[["live","LIVE"],["sources","SOURCES"],["admin","ADMIN"]].map(([t,label])=>{
                            const active = t==="admin" ? (tab==="admin" || !!expandedAdminSection) : tab===t;
                            return (
                              <button key={t} onClick={()=>{
                                if (t === "admin") { setTab("admin"); setExpandedAdminSection(""); }
                                else if (t === "sources") { setTab("sources"); setExpandedAdminSection(""); }
                                else { setTab("live"); setExpandedAdminSection(""); }
                              }} style={{
                                background:"none", border:"none",
                                borderBottom:active?`3px solid ${C.amber}`:"3px solid transparent",
                                color:active?C.amber:C.muted, fontWeight:active?800:500,
                                padding:"4px 12px", cursor:"pointer",
                                fontSize:16, fontFamily:"inherit", letterSpacing:2 }}>{label}</button>
                            );
                          })}
                        </div>
                      </div>
                    </div>
                    {/* Accuracy progress bar — always rendered; segments only appear once data arrives */}
                    <div style={{ display:"flex", height:5, borderRadius:3, overflow:"hidden", marginTop:4, background:C.borderSoft }}>
                      {wBarPct > 0 && <div style={{ width:`${wBarPct}%`, background:C.green }} />}
                      {lBarPct > 0 && <div style={{ width:`${lBarPct}%`, background:C.red }} />}
                      {nBarPct > 0 && <div style={{ width:`${nBarPct}%`, background:C.muted }} />}
                    </div>
                  </div>
                );
              })()}

              {/* DeepSeek analysis card. Wrapped in ErrorBoundary so a render
                  exception (bad data, missing field, etc.) doesn't crash the
                  whole live tab. */}
              <ErrorBoundary key="briefing">
              {briefingJSX}
              </ErrorBoundary>

              {/* Strategy indicators + microstructure moved to ENSEMBLE tab. LIVE is DeepSeek-only. */}
            </div>
          </div>
        )}

        {/* ══ SOURCES TAB (public — no admin required) ══
             Lives alongside LIVE/ADMIN in the top-right tab strip. Shows the
             same EnsembleTab content that admins can see under ADMIN > SOURCES,
             but without the auth gate so briefing pills can link here via
             /#sources and any trader can verify the raw live metric
             readings behind each condition. */}
        {tab==="sources" && (
          <ErrorBoundary key="sources-tab">
            <div style={{ padding:"12px 14px" }}>
              <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between",
                marginBottom:10, paddingBottom:8, borderBottom:`1px solid ${C.borderSoft}` }}>
                <div>
                  <div style={{ fontSize:14, fontWeight:900, color:C.text, letterSpacing:1.5, textTransform:"uppercase" }}>Sources</div>
                  <div style={{ fontSize:11, color:C.muted, marginTop:3 }}>
                    Live microstructure signals — the raw data behind every briefing pill.
                  </div>
                </div>
                <button
                  onClick={() => { setTab("live"); }}
                  style={{ background:"none", border:`1px solid ${C.border}`, borderRadius:5,
                    padding:"4px 12px", cursor:"pointer",
                    fontSize:11, fontFamily:"inherit", letterSpacing:1.5, fontWeight:700,
                    color:C.muted, textTransform:"uppercase" }}>
                  ← Back to live
                </button>
              </div>
              <EnsembleTab
                weights={weights}
                ob={ob} ls={ls} tk={tk} oif={oif} lq={lq}
                fg={fg} mp={mp} cz={cz} cg={cg}
                dots={dots} price={price}
                allAccuracy={allAccuracy}
                allAccuracyErr={allAccuracyErr}
                onRefreshAccuracy={fetchAllAccuracy}
              />
            </div>
          </ErrorBoundary>
        )}

        {/* ══ ADMIN TAB ══ */}
        {tab==="admin" && (
          <ErrorBoundary key="admin-tab">
            {/* Top-right "Back to Live" — always present in admin panel for a one-click exit */}
            <button
              onClick={() => { setTab("live"); setExpandedAdminSection(""); }}
              style={{
                position:"absolute", top:50, right:14, zIndex:10,
                background:C.surface, border:`1px solid ${C.border}`, borderRadius:5,
                padding:"4px 12px", cursor:"pointer",
                fontSize:11, fontFamily:"inherit", letterSpacing:1.5, fontWeight:700,
                color:C.textSec,
              }}
              onMouseEnter={(e)=>{ e.currentTarget.style.background = C.bg; e.currentTarget.style.color = C.green; }}
              onMouseLeave={(e)=>{ e.currentTarget.style.background = C.surface; e.currentTarget.style.color = C.textSec; }}
            >
              ← BACK TO LIVE
            </button>
            {!adminChecked ? (
              <div style={{ padding:"20px", textAlign:"center", color:C.muted, fontSize:11 }}>
                Checking admin session…
              </div>
            ) : !isAdmin ? (
              <div style={{ display:"flex", alignItems:"flex-start", justifyContent:"center", paddingTop:40 }}>
                <div style={{ ...card, width:360, padding:"20px 24px" }}>
                  <div style={{ fontSize:11, fontWeight:700, color:C.amber, letterSpacing:2,
                    textTransform:"uppercase", marginBottom:12 }}>Admin Login</div>
                  <div style={{ fontSize:9, color:C.muted, letterSpacing:0.8, marginBottom:10, lineHeight:1.5 }}>
                    Public view (LIVE tab) remains open. Admin panel needs a password.
                    After 3 failed attempts the lockout is permanent until a code change.
                  </div>
                  <form onSubmit={(e) => {
                    e.preventDefault();
                    if (adminLoginBusy) return;
                    setAdminLoginBusy(true); setAdminLoginError("");
                    fetch("/admin/login", {
                      method:"POST", headers:{"Content-Type":"application/json"},
                      body: JSON.stringify({ password: adminPasswordInput })
                    }).then(async r => {
                      const body = await r.json().catch(() => ({}));
                      if (r.ok) { setIsAdmin(true); setAdminPasswordInput(""); setAdminLoginError(""); }
                      else { setAdminLoginError(body.detail || `HTTP ${r.status}`); }
                    }).catch(e => setAdminLoginError(String(e)))
                      .finally(() => setAdminLoginBusy(false));
                  }}>
                    <input type="password" value={adminPasswordInput} autoFocus
                      onChange={e => setAdminPasswordInput(e.target.value)}
                      placeholder="password"
                      style={{ width:"100%", padding:"8px 10px", fontSize:12, fontFamily:"inherit",
                        background:C.bg, color:C.text, border:`1px solid ${C.border}`,
                        borderRadius:4, marginBottom:10 }} />
                    <button type="submit" disabled={adminLoginBusy || !adminPasswordInput}
                      style={{ width:"100%", padding:"7px", fontSize:11, fontFamily:"inherit",
                        fontWeight:700, letterSpacing:1.5, textTransform:"uppercase",
                        color:C.amber, background:C.amberBg, border:`1px solid ${C.amberBorder}`,
                        borderRadius:4, cursor: adminLoginBusy ? "default" : "pointer",
                        opacity: adminLoginBusy ? 0.6 : 1 }}>
                      {adminLoginBusy ? "Authenticating…" : "Unlock"}
                    </button>
                    {adminLoginError && (
                      <div style={{ marginTop:10, fontSize:10, color:C.red,
                        padding:"6px 8px", background:C.redBg, border:`1px solid ${C.redBorder}`,
                        borderRadius:4, lineHeight:1.4 }}>
                        {adminLoginError}
                      </div>
                    )}
                  </form>
                </div>
              </div>
            ) : (
              <div style={{ height:"100%", overflowY:"auto", display:"flex",
                flexDirection:"column", gap:6, paddingBottom:8 }}>
                {/* Header bar with logout */}
                <div style={{ ...card, padding:"6px 12px", flexShrink:0,
                  display:"flex", alignItems:"center", justifyContent:"space-between" }}>
                  <span style={{ fontSize:10, fontWeight:700, color:C.amber,
                    letterSpacing:2, textTransform:"uppercase" }}>Admin panel</span>
                  <button onClick={() => {
                    fetch("/admin/logout", { method:"POST" })
                      .finally(() => { setIsAdmin(false); setExpandedAdminSection(""); });
                  }} style={{ fontSize:9, padding:"3px 10px", fontFamily:"inherit",
                    fontWeight:700, letterSpacing:1, textTransform:"uppercase",
                    color:C.muted, background:"none", border:`1px solid ${C.border}`,
                    borderRadius:3, cursor:"pointer" }}>Logout</button>
                </div>

                {[
                  ["history",     "HISTORY",      "Bar-by-bar prediction outcomes + per-signal accuracy"],
                  ["ensemble",    "SOURCES",      "Live microstructure signals + strategy weights — the data behind every briefing pill"],
                  ["timing",      "TIMING",       "Per-bar pipeline stage latencies"],
                  ["embed_audit", "EMBED AUDIT",  "Embedding coverage + last DeepSeek prompts"],
                  ["errors",      "ERRORS",       "Persisted error/flag/suggestion log"],
                ].map(([key, label, blurb]) => {
                  const open = expandedAdminSection === key;
                  return (
                    <div key={key} style={{ ...card, padding:0, flexShrink: open ? 1 : 0,
                      display:"flex", flexDirection:"column",
                      minHeight: open ? 300 : "auto" }}>
                      <div onClick={() => setExpandedAdminSection(open ? "" : key)}
                        style={{ padding:"8px 12px", cursor:"pointer",
                          display:"flex", alignItems:"center", gap:8,
                          borderBottom: open ? `1px solid ${C.borderSoft}` : "none" }}>
                        <span style={{ fontSize:11, fontWeight:700, color:open?C.amber:C.muted,
                          letterSpacing:1.5, minWidth:14 }}>{open ? "▼" : "▶"}</span>
                        <span style={{ fontSize:11, fontWeight:800, color:open?C.amber:C.text,
                          letterSpacing:2 }}>{label}</span>
                        <span style={{ fontSize:9, color:C.muted, marginLeft:8 }}>{blurb}</span>
                      </div>
                      {open && (
                        <div style={{ flex:1, minHeight:0, padding:"6px 8px", overflow:"hidden",
                          display:"flex", flexDirection:"column" }}>
                          {key==="history" && (
                            <div style={{ flex:1, minHeight:0, display:"flex", flexDirection:"column", gap:6 }}>
                              <div style={{ ...card, flexShrink:0, padding:"6px 10px" }}>
                                {allAccuracy && (() => {
                                  const cats = [
                                    { key:"strategies", label:"Strategies" },
                                    { key:"specialists", label:"Specialists" },
                                    { key:"microstructure", label:"Micro" },
                                    { key:"ai", label:"AI" },
                                  ];
                                  const allRows = [];
                                  cats.forEach(({ key: k, label: catLabel }) => {
                                    (allAccuracy[k] || []).forEach(r => {
                                      if (r.total >= 3) allRows.push({ ...r, cat: catLabel });
                                    });
                                  });
                                  if (!allRows.length) return null;
                                  allRows.sort((a,b) => b.accuracy - a.accuracy);
                                  return (
                                    <div>
                                      <div style={{ fontSize:8, color:C.muted, fontWeight:700,
                                        letterSpacing:1.2, textTransform:"uppercase", marginBottom:4 }}>
                                        Per-signal accuracy ({allRows.length} tracked) — sorted best→worst
                                      </div>
                                      <div style={{ display:"flex", flexWrap:"wrap", gap:4 }}>
                                        {allRows.map(r => {
                                          const col = r.accuracy >= 60 ? C.green : r.accuracy >= 50 ? C.textSec : C.red;
                                          const bg  = r.accuracy >= 60 ? C.greenBg : r.accuracy >= 50 ? C.bg : C.redBg;
                                          const bdr = r.accuracy >= 60 ? C.greenBorder : r.accuracy >= 50 ? C.borderSoft : C.redBorder;
                                          return (
                                            <div key={r.key} style={{ fontSize:9, padding:"2px 7px", borderRadius:4,
                                              color:col, background:bg, border:`1px solid ${bdr}`,
                                              display:"flex", alignItems:"baseline", gap:4 }}>
                                              <span style={{ fontWeight:700 }}>{r.name}</span>
                                              <span style={{ fontWeight:900 }}>{r.accuracy.toFixed(1)}%</span>
                                              <span style={{ opacity:0.6 }}>{r.correct}/{r.total}</span>
                                              <span style={{ fontSize:7, opacity:0.5 }}>{r.cat}</span>
                                            </div>
                                          );
                                        })}
                                      </div>
                                    </div>
                                  );
                                })()}
                              </div>
                              <div style={{ flex:1, minHeight:0 }}>
                                <DeepSeekAuditTab
                                  deepseekLog={deepseekLog} deepseekAcc={deepseekAcc}
                                  deepseekPred={deepseekPred} ensembleAccuracy={allTimeAccuracy}
                                  totalPreds={allTimeTotal} correctPreds={allTimeCorrect} agreeAcc={agreeAcc}
                                />
                              </div>
                            </div>
                          )}
                          {key==="ensemble" && (
                            <EnsembleTab
                              weights={weights}
                              ob={ob} ls={ls} tk={tk} oif={oif} lq={lq}
                              fg={fg} mp={mp} cz={cz} cg={cg}
                              dots={dots} price={price}
                              allAccuracy={allAccuracy}
                              allAccuracyErr={allAccuracyErr}
                              onRefreshAccuracy={fetchAllAccuracy}
                            />
                          )}
                          {key==="timing" && (<TimingTab timings={timings} />)}
                          {key==="embed_audit" && (
                            <EmbeddingAuditTab
                              embeddingAuditLog={embeddingAuditLog}
                              setEmbeddingAuditLog={setEmbeddingAuditLog}
                              deepseekInspect={deepseekInspect}
                              setDeepseekInspect={setDeepseekInspect}
                            />
                          )}
                          {key==="errors" && (<ErrorsTab errors={errorLog} />)}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </ErrorBoundary>
        )}

      </div>

      <div style={{ textAlign:"center", fontSize:8, color:C.muted, letterSpacing:2, padding:"3px 0",
        flexShrink:0, borderTop:`1px solid ${C.borderSoft}`, background:C.surface }}>
        SIMPLE ANALYSIS · NOT FINANCIAL ADVICE · performance recorded at bar close only
      </div>

      <style>{`
        *{box-sizing:border-box}
        html,body{margin:0;padding:0;overflow:hidden;background:${C.bg}}
        ::-webkit-scrollbar{width:12px;height:12px}
        ::-webkit-scrollbar-track{background:#E6E4DF;border-radius:8px}
        ::-webkit-scrollbar-thumb{background:#A09D99;border-radius:8px;border:3px solid #E6E4DF;min-height:48px}
        ::-webkit-scrollbar-thumb:hover{background:#6B6866}
        ::-webkit-scrollbar-corner{background:#E6E4DF}
        *{scrollbar-width:auto;scrollbar-color:#A09D99 #E6E4DF}
        input[type=range]{height:3px}
      `}</style>
    </div>
  );
}



// ── Sub-components ────────────────────────────────────────────
function Row({ label, val, c }) {
  return (
    <div style={{ marginBottom:1, display:"flex", gap:6 }}>
      <span style={{ color:C.muted, minWidth:90 }}>{label}:</span>
      <span style={{ color:c||C.textSec, fontWeight:c?600:400 }}>{val}</span>
    </div>
  );
}

ReactDOM.render(<App />, document.getElementById("root"));
