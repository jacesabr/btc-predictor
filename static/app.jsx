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

  // Aggressive-precision BTC formatter — matches the briefing pill's fmt.btc.
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
              <div>
                <div style={{ fontSize:20, fontWeight:900, color:C.text }}>{ds.confidence}%</div>
                <div style={{ height:4, background:C.borderSoft, borderRadius:2, width:100, marginTop:2 }}>
                  <div style={{ width:`${ds.confidence}%`, height:"100%", background:ds.signal==="UP"?C.green:ds.signal==="NEUTRAL"?C.amber:C.red, borderRadius:2 }} />
                </div>
              </div>
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
  const [dataSource,     setDataSource]     = useState("—");
  const [price,          setPrice]          = useState(null);
  const [winStartPrice,  setWinStartPrice]  = useState(null);
  const [winStartTime,   setWinStartTime]   = useState(null);
  const [timeLeft,       setTimeLeft]       = useState(300);
  const [strategies,     setStrategies]     = useState({});
  const [ensemblePred,   setEnsemblePred]   = useState(null);
  const [deepseekPred,   setDeepseekPred]   = useState(null);
  const [deepseekAcc,    setDeepseekAcc]    = useState(null);
  const [agreeAcc,       setAgreeAcc]       = useState(null);
  const [deepseekLog,    setDeepseekLog]    = useState([]);
  const [backtest,       setBacktest]       = useState(null);
  const [preds,          setPreds]          = useState([]);
  const [weights,        setWeights]        = useState({});

  const [pendingDeepseekReady,  setPendingDeepseekReady]  = useState(false);
  const [pendingDeepseekPred,   setPendingDeepseekPred]   = useState(null);
  const [traderSummary,         setTraderSummary]         = useState(null);
  // Collapsed by default — users said "waiting" section was too much scroll.
  // Keep one dense summary visible with expand toggle for the full list.
  // INFO defaults VISIBLE — these are the current-state expert observations
  // (whale flow, taker flow narrative, book shape, funding regime) that
  // explain the chart story. They pair with the edge sentence to form the
  // full "here's what's happening right now" read. Only WAITING (future
  // conditional triggers) stays collapsed.
  const [infoOpen,              setInfoOpen]              = useState(true);
  const [serviceUnavailable,    setServiceUnavailable]    = useState(false);
  const [serviceUnavailReason,  setServiceUnavailReason]  = useState("");
  const [binanceExpert,         setBinanceExpert]         = useState(null);
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
  const [sourceHistory,  setSourceHistory]  = useState([]);
  const [selectedSource, setSelectedSource] = useState(0);
  const [allAccuracy,    setAllAccuracy]    = useState(null);
  const [allAccuracyErr, setAllAccuracyErr] = useState(false);
  const [embeddingAuditLog, setEmbeddingAuditLog] = useState([]);
  const [deepseekInspect,   setDeepseekInspect]   = useState(null);
  const [timings,           setTimings]           = useState({ current: {}, history: [], history_total: 0 });
  const wsRef           = useRef(null);
  const reconnectRef    = useRef(null);
  const prevDsWindowRef = useRef(null);
  // Hysteresis state for condition pill met/unmet. A condition must be met for
  // HYSTERESIS_MS continuously before the pill flips to "met", and unmet for
  // HYSTERESIS_MS continuously before flipping back — prevents flashing when
  // live values bounce around the threshold.
  const hysteresisRef   = useRef({});
  const HYSTERESIS_MS   = 10000;

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
        if (d.ensemble_prediction) {
          const ep=d.ensemble_prediction;
          setEnsemblePred({ signal:ep.signal, confidence:ep.confidence, bullish:ep.bullish_count, bearish:ep.bearish_count, upProb:ep.up_probability });
        }
        if (d.deepseek_prediction)                    setDeepseekPred(d.deepseek_prediction);
        if (d.pending_deepseek_prediction)            setPendingDeepseekPred(d.pending_deepseek_prediction);
        if (d.agree_accuracy)                         setAgreeAcc(d.agree_accuracy);
        if (d.pending_deepseek_ready !== undefined)   setPendingDeepseekReady(d.pending_deepseek_ready);
        if (d.trader_summary) {
          // Only accept a TRUTHY summary — ignore null sent by the backend
          // briefly between bar-open and Venice completion. The JSON-equality
          // guard still suppresses re-renders when the same summary ships on
          // every 1-sec tick. The earlier fix (commit 14d0ca5) was only
          // applied to the REST poll; mirror it here so both code paths agree.
          setTraderSummary(prev => {
            const a = JSON.stringify(prev), b = JSON.stringify(d.trader_summary);
            return a === b ? prev : d.trader_summary;
          });
        }
        if (d.service_unavailable !== undefined)      setServiceUnavailable(!!d.service_unavailable);
        if (d.service_unavailable_reason !== undefined) setServiceUnavailReason(d.service_unavailable_reason || "");
        if (d.bar_binance_expert && d.bar_binance_expert.signal) setBinanceExpert(d.bar_binance_expert);
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
    fetch("/price").then(r=>r.json()).then(d=>setDataSource(d.data_source||"—")).catch(()=>{});
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
        // Only accept TRUTHY trader_summary updates. If the backend briefly
        // reports null (new bar open, Venice hasn't generated yet), DON'T
        // clobber the previous bar's summary — that was causing the
        // "analysis appearing and disappearing" flash at every bar
        // boundary. The WS handler bound at line ~2551 also guards via
        // JSON-equality; this poll now mirrors that safety.
        if (d.trader_summary)                        setTraderSummary(d.trader_summary);
        if (d.service_unavailable !== undefined)     setServiceUnavailable(!!d.service_unavailable);
        if (d.service_unavailable_reason !== undefined) setServiceUnavailReason(d.service_unavailable_reason || "");
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

  // ── Source history — fetch on tab switch + refresh every 60s ──
  useEffect(() => {
    if (tab !== "sources") return;
    fetch("/deepseek/source-history?n=20").then(r=>r.json()).then(d=>{ setSourceHistory(d); setSelectedSource(0); }).catch(()=>{});
  }, [tab]);
  useEffect(() => {
    if (tab !== "sources") return;
    const id = setInterval(()=>{
      fetch("/deepseek/source-history?n=20").then(r=>r.json()).then(setSourceHistory).catch(()=>{});
    }, 60000);
    return () => clearInterval(id);
  }, [tab]);

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

  // briefingJSX — memoized rendering of the trader briefing. Computed once per
  // meaningful state change, cached across 500ms timer ticks so the
  // Bullet/ConditionPill/BullBearText components defined inside the IIFE don't
  // get re-instantiated on every render (root cause of the "entire tab flashes"
  // symptom). `timeLeft` intentionally OMITTED from deps — the Closes-badge
  // color in the status strip may lag up to 500ms, an acceptable trade for
  // component-identity stability.
  const briefingJSX = useMemo(() => {
    const BULL_WORDS = new Set([
      "bullish","uptrend","upside","upward","breakout","bounce","rally","surge",
      "buyers","buying","accumulation","accumulating","support","higher","hh","hl",
      "strong","long","longs","rising","rose","up","aligned","holding",
    ]);
    const BEAR_WORDS = new Set([
      "bearish","downtrend","downside","downward","breakdown","rejection","selloff",
      "drop","drops","falling","fell","sellers","selling","distribution","resistance",
      "lower","ll","lh","weak","short","shorts","down","failing","losing",
    ]);
    const BullBearText = ({ text, size, baseColor }) => {
      const refSize = size + 2;
      // Bold numeric references ($-prices, percentages, and BTC amounts). All
      // three forms need to support COMMA-grouped integers (e.g. "34,642.1 BTC",
      // "1,234.5%", "$77,500"); otherwise "34,642.1 BTC" splits to "34," (prose)
      // + "642.1 BTC" (bold) and the first digits lose their bold.
      const REF = /(\$\d[\d,]*(?:\.\d+)?[kKmM]?|\d[\d,]*(?:\.\d+)?%|\d[\d,]*(?:\.\d+)?\s*BTC\b)/gi;
      const segments = [];
      let last = 0, m;
      while ((m = REF.exec(text)) !== null) {
        if (m.index > last) segments.push({ k: "prose", t: text.slice(last, m.index) });
        segments.push({ k: "ref", t: m[0] });
        last = REF.lastIndex;
      }
      if (last < text.length) segments.push({ k: "prose", t: text.slice(last) });
      let key = 0;
      return (
        <span style={{ fontSize:size, color:baseColor, lineHeight:1.55 }}>
          {segments.map((seg) => seg.k === "ref"
            ? <strong key={key++} style={{ color:C.text, fontWeight:900, fontSize:refSize }}>{seg.t}</strong>
            : <span key={key++}>{seg.t}</span>
          )}
        </span>
      );
    };
    const fmt = {
      // Aggressive precision — never let a non-zero reading round to "0".
      // A "0 BTC" or "0.00%" pill made the trader think the source was
      // dead; show whatever decimal count is needed to prove there's an
      // actual value there.
      //   value == 0           → "0 BTC"
      //   |v| < 0.01           → 4 decimals (e.g. "0.0045 BTC")
      //   |v| < 1              → 3 decimals (e.g. "0.152 BTC")
      //   |v| < 10             → 2 decimals (e.g. "5.42 BTC")
      //   |v| < 1000           → 1 decimal  (e.g. "320.5 BTC")
      //   |v| ≥ 1000           → integer + commas (e.g. "34,839 BTC")
      btc:  (v) => {
        if (v === 0 || v == null) return "0 BTC";
        const a = Math.abs(v);
        if (a >= 1000)  return `${v.toLocaleString(undefined,{maximumFractionDigits:0})} BTC`;
        if (a >= 10)    return `${v.toFixed(1)} BTC`;
        if (a >= 1)     return `${v.toFixed(2)} BTC`;
        if (a >= 0.01)  return `${v.toFixed(3)} BTC`;
        return `${v.toFixed(4)} BTC`;
      },
      usd:  (v) => {
        if (v === 0 || v == null) return "$0";
        const a = Math.abs(v);
        if (a >= 1)     return `$${v.toLocaleString(undefined,{maximumFractionDigits:0})}`;
        return `$${v.toFixed(4)}`;
      },
      pct:  (v) => {
        if (v === 0 || v == null) return "0%";
        const a = Math.abs(v);
        if (a >= 1)     return `${v.toFixed(2)}%`;
        if (a >= 0.01)  return `${v.toFixed(3)}%`;
        return `${v.toFixed(5)}%`;
      },
      num:  (v) => {
        if (v === 0 || v == null) return "0";
        const a = Math.abs(v);
        if (a >= 1)     return v.toFixed(2);
        if (a >= 0.01)  return v.toFixed(3);
        return v.toFixed(4);
      },
    };
    const ds     = (backendSnap?.snapshot?.dashboard_signals) || {};
    const spb    = ds.spot_perp_basis;
    const cvdBlk = ds.cvd;
    const obFull = ds.order_book;
    const skew   = ds.deribit_skew_term;
    const metric = (m) => {
      switch (m) {
        case "price":             return price != null ? { v: price, f: fmt.usd } : null;
        case "price_change_pct":  return (price != null && winStartPrice) ? { v: (price - winStartPrice) / winStartPrice * 100, f: fmt.pct } : null;
        case "taker_buy_volume":  return tk?.bv  != null ? { v: tk.bv,  f: fmt.btc } : null;
        case "taker_sell_volume": return tk?.sv  != null ? { v: tk.sv,  f: fmt.btc } : null;
        case "taker_volume":      return tk?.bv!=null && tk?.sv!=null ? { v: tk.bv + tk.sv, f: fmt.btc } : null;
        case "taker_ratio":       return tk?.bsr != null ? { v: tk.bsr, f: fmt.num } : null;
        case "bsr":               return tk?.bsr != null ? { v: tk.bsr, f: fmt.num } : null;
        case "bid_imbalance":     return (obFull?.imbalance_05pct_pct != null) ? { v: obFull.imbalance_05pct_pct, f: fmt.pct } : (ob?.imb != null ? { v: ob.imb, f: fmt.pct } : null);
        case "ask_imbalance":     return (obFull?.imbalance_05pct_pct != null) ? { v: -obFull.imbalance_05pct_pct, f: fmt.pct } : (ob?.imb != null ? { v: -ob.imb, f: fmt.pct } : null);
        case "funding_rate":
          // Same rationale as open_interest above: prefer the backend-relayed
          // value so pill matches bullet text even under OKX-fallback.
          if (ds?.oi_funding?.funding_rate_8h_pct != null)
            return { v: ds.oi_funding.funding_rate_8h_pct, f: fmt.pct };
          return oif?.fr != null ? { v: oif.fr*100, f: fmt.pct } : null;
        case "open_interest":
          // Prefer backend's open_interest_btc (same source DeepSeek
          // reasoned over) so the pill matches the bullet text even when
          // the backend is on OKX-fallback because Binance fAPI is
          // geo-blocked from Render's datacenter. Fall back to the
          // browser's direct-Binance fetch when the backend value is
          // unavailable — those paths produce different numbers (Binance
          // perp OI ≈ 3× OKX perp OI), which was causing visible
          // disagreement between "OI 34,839 BTC" in the bullet text and
          // "now 96,771 BTC" in the pill for the same bar.
          if (ds?.oi_funding?.open_interest_btc != null)
            return { v: ds.oi_funding.open_interest_btc, f: fmt.btc };
          return oif?.oi != null ? { v: oif.oi, f: fmt.btc } : null;
        case "rsi":               return strategies?.rsi?.value != null ? { v: parseFloat(strategies.rsi.value), f: fmt.num } : null;
        case "long_short_ratio":  return ls?.lsr != null ? { v: ls.lsr, f: fmt.num } : null;
        case "basis_pct":         return spb?.basis_pct != null ? { v: spb.basis_pct, f: fmt.pct } : null;
        case "perp_cvd_1h":       return cvdBlk?.perp_cvd_1h_btc != null ? { v: cvdBlk.perp_cvd_1h_btc, f: fmt.btc } : null;
        case "spot_cvd_1h":       return cvdBlk?.spot_cvd_1h_btc != null ? { v: cvdBlk.spot_cvd_1h_btc, f: fmt.btc } : null;
        case "aggregate_cvd_1h":  return cvdBlk?.aggregate_cvd_1h_btc != null ? { v: cvdBlk.aggregate_cvd_1h_btc, f: fmt.btc } : null;
        case "bid_depth_05pct":   return obFull?.bid_depth_05pct_btc != null ? { v: obFull.bid_depth_05pct_btc, f: fmt.btc } : null;
        case "ask_depth_05pct":   return obFull?.ask_depth_05pct_btc != null ? { v: obFull.ask_depth_05pct_btc, f: fmt.btc } : null;
        case "rr_25d_30d":        return skew?.rr_25d_30d_pct != null ? { v: skew.rr_25d_30d_pct, f: fmt.pct } : null;
        case "iv_30d_atm":        return skew?.iv_30d_atm_pct != null ? { v: skew.iv_30d_atm_pct, f: fmt.pct } : null;
        case "spot_whale_buy_btc":  return ds.spot_whale_flow?.whale_buy_btc  != null ? { v: ds.spot_whale_flow.whale_buy_btc,  f: fmt.btc } : null;
        case "spot_whale_sell_btc": return ds.spot_whale_flow?.whale_sell_btc != null ? { v: ds.spot_whale_flow.whale_sell_btc, f: fmt.btc } : null;
        case "aggregate_funding_rate":    return ds.aggregate_funding?.weighted_funding_rate != null ? { v: ds.aggregate_funding.weighted_funding_rate*100, f: fmt.pct } : null;
        case "aggregate_liquidations_usd":return ds.aggregate_liquidations?.total_usd != null ? { v: ds.aggregate_liquidations.total_usd, f: fmt.usd } : null;
        case "oi_velocity_pct":   return ds.aggregate_oi?.change_30min_pct != null ? { v: ds.aggregate_oi.change_30min_pct, f: fmt.pct } : (oif?.oi_change_pct != null ? { v: oif.oi_change_pct, f: fmt.pct } : null);
        // Technical indicators sourced from strategies[] (live-pushed via WS).
        // .value is a string (e.g. "9.1", "1.3168", "-0.64"); parseFloat
        // tolerates a leading "$" so vwap_ref ("$77508.08") parses cleanly.
        case "stoch_k":           return strategies?.stochastic?.value != null ? { v: parseFloat(strategies.stochastic.value), f: fmt.num } : null;
        case "macd_histogram":    return strategies?.macd?.value != null ? { v: parseFloat(strategies.macd.value), f: fmt.num } : null;
        case "adx":               return strategies?.adx?.value != null ? { v: parseFloat(strategies.adx.value), f: fmt.num } : null;
        case "ema_5_13_diff":     return strategies?.ema_cross?.value != null ? { v: parseFloat(strategies.ema_cross.value), f: fmt.usd } : null;
        case "vwap_ref":          return strategies?.vwap?.value != null ? { v: parseFloat(String(strategies.vwap.value).replace(/[$,]/g,"")), f: fmt.usd } : null;
        // Dashboard-signal-sourced metrics (live-pushed to all viewers).
        case "mark_premium_pct":   return ds?.oi_funding?.mark_premium_vs_index_pct != null ? { v: ds.oi_funding.mark_premium_vs_index_pct, f: fmt.pct } : null;
        case "dvol_pct":           return ds?.deribit_dvol?.dvol_pct != null ? { v: ds.deribit_dvol.dvol_pct, f: fmt.pct } : null;
        case "btc_dominance_pct":  return ds?.btc_dominance?.btc_dominance_pct != null ? { v: ds.btc_dominance.btc_dominance_pct, f: fmt.pct } : null;
        case "fear_greed":         return ds?.fear_greed?.value != null ? { v: ds.fear_greed.value, f: fmt.num } : null;
        case "mempool_fee":        return ds?.mempool?.fastest_fee_sat_vb != null ? { v: ds.mempool.fastest_fee_sat_vb, f: (v)=>`${v} sat/vB` } : null;
        case "kraken_premium_pct": return ds?.kraken_premium?.spread_pct != null ? { v: ds.kraken_premium.spread_pct, f: fmt.pct } : null;
        case "top_long_short_ratio": return ds?.top_position_ratio?.long_short_ratio != null ? { v: ds.top_position_ratio.long_short_ratio, f: fmt.num } : null;
        case "spot_perp_cvd_div":  return ds?.cvd?.spot_perp_divergence_btc != null ? { v: ds.cvd.spot_perp_divergence_btc, f: fmt.btc } : null;
        case "put_call_ratio":     return ds?.deribit_skew_term?.put_call_volume_ratio != null ? { v: ds.deribit_skew_term.put_call_volume_ratio, f: fmt.num } : null;
        default: return null;
      }
    };
    const opCheck = { ">": (a,b)=>a>b, ">=": (a,b)=>a>=b, "<": (a,b)=>a<b, "<=": (a,b)=>a<=b, "==": (a,b)=>Math.abs(a-b)<1e-9 };
    // Every layman below is written in "experienced trader calling in from a
    // game-show helpline to a friend who doesn't know trader lingo" voice.
    // No jargon. Every reading is followed by what it means for the next
    // move / what to watch for. Traders who know the terms can still infer
    // them from the plain-English description.
    const METRIC_META = {
      price:             { label: "price",      layman: "The live BTC price right now on Binance. That's it — no fancy math, just what a single coin costs this second.",
                           source: { label: "Binance spot", url: "/#sources" } },
      price_change_pct:  { label: "Δ price",    layman: "How much the price has moved since this 5-minute candle started. Up 0.2% means it's 0.2% higher than where the candle opened; negative means it's dropped since then.",
                           source: { label: "Binance spot", url: "/#sources" } },
      taker_buy_volume:  { label: "taker buy",  layman: "How much BTC got bought at market price in the last 5 minutes — people who clicked BUY and didn't care about the exact price, they just wanted in now. A big number here means buyers are eager; a tiny number means nobody's chasing.",
                           source: { label: "Coinglass", url: "/#sources" } },
      taker_sell_volume: { label: "taker sell", layman: "How much BTC got sold at market price in the last 5 minutes — people who clicked SELL and didn't wait for a better price, they wanted out now. Big number = sellers panicking or rushing; tiny number = nobody dumping.",
                           source: { label: "Coinglass", url: "/#sources" } },
      taker_volume:      { label: "taker vol",  layman: "Total BTC that changed hands by people hitting the market (buys plus sells) in the last 5 minutes. Near zero means nobody's acting — the market is just waiting. High number means traders are actively moving size.",
                           source: { label: "Coinglass", url: "/#sources" } },
      taker_ratio:       { label: "BSR",        layman: "BSR tells you who's actually pressing the trigger right now. Every second some people click BUY at the current price and some click SELL — BSR just divides those two. Above 1 means more people are clicking BUY than SELL (buyers in charge, price likely keeps going up). Below 1 means SELL is winning (price pressure is down). Extreme readings (like 3 or 0.3) mean one side is dominating hard — but also watch for exhaustion; the side pushing too hard often runs out of steam.",
                           source: { label: "Coinglass", url: "/#sources" } },
      bsr:               { label: "BSR",        layman: "BSR tells you who's actually pressing the trigger right now. Every second some people click BUY at the current price and some click SELL — BSR just divides those two. Above 1 means more people are clicking BUY than SELL (buyers in charge, price likely keeps going up). Below 1 means SELL is winning (price pressure is down). Extreme readings (like 3 or 0.3) mean one side is dominating hard — but also watch for exhaustion; the side pushing too hard often runs out of steam.",
                           source: { label: "Coinglass", url: "/#sources" } },
      bid_imbalance:     { label: "bid imb",    layman: "Near the current price, are there more BUY orders waiting, or SELL orders waiting? Positive = more buyers lined up below price waiting to catch it (cushion if it drops). Negative = the opposite — more sellers lined up above. Think of it as where the ready-to-act money is parked.",
                           source: { label: "Binance depth", url: "/#sources" } },
      ask_imbalance:     { label: "ask imb",    layman: "Mirror of bid imbalance — positive here means more SELL orders are waiting near the current price than BUY orders. Lots of sellers lined up above = harder for price to push through upward without first eating them.",
                           source: { label: "Binance depth", url: "/#sources" } },
      funding_rate:      { label: "funding",    layman: "On leveraged futures, one side has to pay the other a fee every 8 hours to keep the price tied to spot. Positive = people betting on UP are paying (too many of them crowded in — often a sign the up-trade is getting over-crowded). Negative = people betting on DOWN are paying (crowded short — sometimes signals an upcoming snap-back up). Near zero = balanced, no squeeze setup.",
                           source: { label: "Coinglass", url: "/#sources" } },
      open_interest:     { label: "OI",         layman: "Total BTC currently tied up in leveraged price bets on Binance. Think of it as 'how much money is actively gambling on the next move'. Rising while price rises = fresh buyers committing real money, trend has fuel. Falling while price rises = people taking profits / closing shorts, rally might not have legs.",
                           source: { label: "Coinglass", url: "/#sources" } },
      rsi:               { label: "RSI",        layman: "A 0-to-100 gauge of how fast price just ran. Over 70 = price rallied really fast, usually means a pullback or breather is coming. Under 30 = price dropped fast, bounce often follows. Near 50 = nothing happening either direction, moves are weak.",
                           source: { label: "TradingView", url: "/#sources" } },
      long_short_ratio:  { label: "L/S",        layman: "Out of the everyday traders on Binance futures, how many are betting UP vs betting DOWN. When it swings to extremes it's usually wrong — if 80% of the crowd is betting UP, price often goes DOWN (because 'the crowd' tends to be late and crowded).",
                           source: { label: "Coinglass", url: "/#sources" } },
      basis_pct:             { label: "basis",           layman: "Leveraged bet price vs. regular spot price. Positive = the leveraged market is paying extra over spot (traders bullish, expecting more up). Negative = leveraged is trading cheaper than spot (bearish sentiment in futures). The bigger the gap, the more one-sided the sentiment.",
                               source: { label: "Coinglass", url: "/#sources" } },
      perp_cvd_1h:           { label: "perp CVD 1h",     layman: "On the leveraged futures market, over the last hour, was there more aggressive buying or selling? Positive = buying pressure has been winning. Negative = selling pressure. Tracks the net 'who's been pressing the trigger' over the last 60 minutes.",
                               source: { label: "Coinglass", url: "/#sources" } },
      spot_cvd_1h:           { label: "spot CVD 1h",     layman: "Same idea as perp CVD but on the regular (non-leveraged) market. Positive means people have been net-buying with real spot money in the last hour — a stronger signal than futures alone because there's no leverage behind it.",
                               source: { label: "Coinglass", url: "/#sources" } },
      aggregate_cvd_1h:      { label: "aggregate CVD 1h",layman: "Net buying-vs-selling across every major exchange combined over the last hour. Cuts through the noise of looking at just one venue — if this is strongly positive, real money is net-buying globally, not just on one exchange.",
                               source: { label: "Coinglass", url: "/#sources" } },
      bid_depth_05pct:       { label: "bid depth",       layman: "How much BTC is waiting on the BUY side in a half-percent window right under the current price. If price drops a tiny bit, this is how much demand is ready to catch it. Big number = strong cushion below; small number = air pocket, any dip falls through easily.",
                               source: { label: "Binance depth", url: "/#sources" } },
      ask_depth_05pct:       { label: "ask depth",       layman: "How much BTC is waiting on the SELL side in a half-percent window right above the current price. If price tries to go up, this is the supply it has to chew through. Big number = hard to break up (stiff resistance); small number = a small nudge can launch it.",
                               source: { label: "Binance depth", url: "/#sources" } },
      rr_25d_30d:            { label: "RR 25d/30d",      layman: "Among traders making 30-day bets, are more of them paying up for UP-bets or DOWN-bets? Positive = they're willing to pay more for upside insurance/speculation (bullish lean over the month). Negative = more fear, paying for downside protection (bearish lean).",
                               source: { label: "Deribit", url: "/#sources" } },
      iv_30d_atm:            { label: "IV 30d",          layman: "A single number that says 'how big a move does the market think BTC will make over the next 30 days'. Higher = traders expect wild swings (buckle up). Lower = traders expect a calm/quiet month. Think of it as the market's nervousness level.",
                               source: { label: "Deribit", url: "/#sources" } },
      spot_whale_buy_btc:    { label: "whale buy",       layman: "When a single trade of 5+ BTC hits the regular spot market as a BUY, that's usually a big player (fund, institution, long-time holder) taking a position. This shows how much of that showed up in the last 5 minutes. Sudden spike = a big player is stepping in on the buy side — often marks the start of a move up.",
                               source: { label: "Coinglass", url: "/#sources" } },
      spot_whale_sell_btc:   { label: "whale sell",      layman: "Mirror of whale buy — one big player dumped 5+ BTC in one click. This is how much of that happened in the last 5 minutes. Big number = a serious holder is offloading, often a warning that they know something or want out before price drops further.",
                               source: { label: "Coinglass", url: "/#sources" } },
      aggregate_funding_rate:{ label: "agg funding",     layman: "Same as funding_rate but averaged across every major exchange — catches the overall 'who's crowded in' picture without being fooled by one venue being different. Positive = UP-bettors paying globally, crowded long. Negative = DOWN-bettors paying globally, crowded short.",
                               source: { label: "Coinglass", url: "/#sources" } },
      aggregate_liquidations_usd:{label:"liquidations",  layman: "When leveraged traders get on the wrong side of a move, their position gets auto-closed (forced exit). This is the total $-value of those forced exits across all exchanges in the last 5 minutes. Big spikes here = a stop-cascade where forced selling (or buying) is fueling the move further.",
                               source: { label: "Coinglass", url: "/#sources" } },
      oi_velocity_pct:       { label: "OI velocity",     layman: "How fast money is flowing INTO or OUT OF leveraged bets right now. Rising = fresh traders committing new money — trend has real fuel. Falling = traders closing out — the current move might be running on fumes.",
                               source: { label: "Coinglass", url: "/#sources" } },
      stoch_k:           { label: "Stoch %K",      layman: "A 0-to-100 gauge of where price is sitting in its recent 5-minute range. Above 80 = price is pinned near the top of the range (overbought, breather likely). Below 20 = pinned near the bottom (oversold, bounce likely). It moves faster than RSI so it tags extremes more often.",
                           source: { label: "TradingView", url: "/#sources" } },
      macd_histogram:    { label: "MACD hist",     layman: "Shows whether short-term momentum is pulling away from longer-term momentum or coming back together. Positive and rising = momentum building UP. Negative and rising (less negative) = down-momentum fading, possible reversal. Crossing zero = momentum is flipping sides.",
                           source: { label: "TradingView", url: "/#sources" } },
      adx:               { label: "ADX",           layman: "How strong the current trend is, regardless of direction. Above 25 = real trend is happening (don't fade it). Below 20 = no trend, price chopping around (range-trade or stand aside). It doesn't tell you UP or DOWN, just whether the move has conviction.",
                           source: { label: "TradingView", url: "/#sources" } },
      ema_5_13_diff:     { label: "EMA 5/13 Δ",    layman: "How far the very-short-term average has pulled away from the slightly-longer one. Positive = short-term is above long-term (bullish slope). Negative = the opposite. Big absolute number = strong trend; near zero = no momentum, lines about to cross.",
                           source: { label: "TradingView", url: "/#sources" } },
      vwap_ref:          { label: "VWAP",          layman: "The volume-weighted average price for the current session — what the average buyer/seller has paid this session. Price above VWAP = buyers in control on average. Below = sellers. Distance from VWAP shows how stretched the current move is.",
                           source: { label: "Binance spot", url: "/#sources" } },
      mark_premium_pct:  { label: "mark prem",     layman: "How much the leveraged perpetual price is above (or below) the spot index price. Positive = perp is paying a premium over spot (bullish lean). Negative = perp is trading at a discount (bearish lean). Tracks short-term futures vs spot tension.",
                           source: { label: "Coinglass", url: "/#sources" } },
      dvol_pct:          { label: "DVOL",          layman: "Deribit's option-implied volatility index — what options traders are pricing in for moves over the next 30 days. Higher = options expect bigger swings (fear or anticipation). Lower = options expect calm. Sudden spikes here often precede big price moves.",
                           source: { label: "Deribit", url: "/#sources" } },
      btc_dominance_pct: { label: "BTC dom",       layman: "BTC's share of the entire crypto market cap. Rising = money rotating OUT of altcoins INTO BTC (often bullish for BTC, bearish for alts). Falling = money flowing into alts (alt-season). Above ~58% historically signals strong BTC preference.",
                           source: { label: "CoinGecko", url: "/#sources" } },
      fear_greed:        { label: "Fear & Greed",  layman: "A 0-100 sentiment gauge. Below 25 = traders are scared (often a contrarian buy signal). Above 75 = traders are greedy (often a contrarian sell signal). Mid-range = balanced. Daily macro indicator, not for 5-minute scalping but useful as background context.",
                           source: { label: "Alternative.me", url: "/#sources" } },
      mempool_fee:       { label: "mempool fee",   layman: "How crowded the Bitcoin network is right now (sat/vB to confirm fast). Low (1-5) = quiet network, no on-chain panic. High (50+) = lots of urgent transactions waiting, often coincides with market stress. Background context, not a direct trade signal.",
                           source: { label: "Mempool.space", url: "/#sources" } },
      kraken_premium_pct: { label: "Kraken prem",  layman: "How much higher (or lower) Kraken's price is vs OKX. Positive = EU/US regulated buyers paying up over global average (often institutional accumulation). Negative = the opposite. Big spread = regional dislocation, often bullish or bearish depending on direction.",
                           source: { label: "Kraken", url: "/#sources" } },
      top_long_short_ratio: { label: "top L/S",    layman: "Among the TOP traders on Binance futures (top 20% by account size), how many are betting UP vs DOWN. Different from retail L/S — these are the bigger, more informed accounts. When top traders heavily lean one way, it's a stronger signal than the crowd.",
                           source: { label: "Coinglass", url: "/#sources" } },
      spot_perp_cvd_div:  { label: "spot/perp CVD div", layman: "Difference between aggressive buying-vs-selling on regular spot markets vs leveraged perps. Positive = real spot money is buying while perps lag (durable rally). Negative = perp speculation is leading without spot confirmation (often a fakeout that reverses).",
                           source: { label: "Coinglass", url: "/#sources" } },
      put_call_ratio:    { label: "P/C vol",       layman: "Volume of DOWN-bet options divided by UP-bet options. Above 1.0 = traders are buying more downside protection (bearish lean or hedging). Below 1.0 = more upside speculation (bullish lean). Extreme readings are often contrarian — too much fear/greed marks turning points.",
                           source: { label: "Deribit", url: "/#sources" } },
    };
    // Text → signal-family map (mirrors server-side _TEXT_SIGNAL_FAMILIES in
    // trader_summary.py). When a bullet has conditions:[] but its text names
    // a known signal, we synthesize an info-only pill from the first matched
    // family member so the trader still sees live value + source link
    // instead of a "? UNKNOWN" badge.
    const TEXT_FAMILY_RULES = [
      // Plural-tolerant: "whale" / "whales" — bullets often say "Spot whales sold 0.68 BTC".
      { rx: /\bwhales?\b/i,                      metrics: ["spot_whale_buy_btc", "spot_whale_sell_btc"] },
      // Catches "taker buy", "taker sell", "taker buy and sell volumes", "taker aggression".
      { rx: /\btaker\s+(?:buy|sell|aggression|aggressor)/i, metrics: ["taker_ratio", "taker_buy_volume", "taker_sell_volume", "taker_volume"] },
      // Catches "taker volume(s)", "taker flow", "taker ratio".
      { rx: /\btaker\s+(?:volumes?|flow|ratio)\b/i, metrics: ["taker_ratio", "taker_buy_volume", "taker_sell_volume", "taker_volume"] },
      { rx: /\bBSR\b/i,                          metrics: ["taker_ratio"] },
      { rx: /\b(?:bid|ask)\s+(?:imbalance|depth|wall|side|book)\b/i, metrics: ["bid_imbalance", "ask_imbalance", "bid_depth_05pct", "ask_depth_05pct"] },
      // Plain "order book" mentions (without bid/ask qualifier) and "X BTC bids vs Y BTC asks" patterns.
      { rx: /\border\s+book\b|\bBTC\s+(?:bids?|asks?)\b/i, metrics: ["bid_imbalance", "ask_imbalance", "bid_depth_05pct", "ask_depth_05pct"] },
      { rx: /\bfunding\b/i,                      metrics: ["funding_rate", "aggregate_funding_rate"] },
      { rx: /\bOI\s+velocity\b|\bOI\s+(?:flips?|change|turns?|rising|falling|accelerat\w*)\b/i,
        metrics: ["oi_velocity_pct"] },
      { rx: /\bopen interest\b|\bOI\b/i,         metrics: ["open_interest", "oi_velocity_pct"] },
      { rx: /\bliquidations?\b/i,                metrics: ["aggregate_liquidations_usd"] },
      { rx: /\bRSI\b/i,                          metrics: ["rsi"] },
      { rx: /\blong\/short\b|\bL\/S\s*ratio\b/i, metrics: ["long_short_ratio"] },
      { rx: /\btop\s+(?:trader|account)s?\s+(?:L\/S|long\/short|position)\b/i, metrics: ["top_long_short_ratio"] },
      { rx: /\bCVD\b/i,                          metrics: ["aggregate_cvd_1h", "perp_cvd_1h", "spot_cvd_1h"] },
      { rx: /\bspot[\/-]perp\s+(?:CVD\s+)?(?:divergence|div)\b/i, metrics: ["spot_perp_cvd_div"] },
      { rx: /\bbasis\b/i,                        metrics: ["basis_pct"] },
      { rx: /\b(?:IV|implied\s+vol|skew|risk[- ]reversal)\b/i, metrics: ["iv_30d_atm", "rr_25d_30d"] },
      // Technical indicators (live values come from strategies[]).
      { rx: /\bstoch(?:astic)?\b|%\s*K\b|%\s*D\b/i, metrics: ["stoch_k"] },
      { rx: /\bMACD\b/i,                         metrics: ["macd_histogram"] },
      { rx: /\bADX\b/i,                          metrics: ["adx"] },
      { rx: /\bEMA\s*\d+|ema_cross|EMA\s*cross/i, metrics: ["ema_5_13_diff"] },
      { rx: /\bVWAP\b/i,                         metrics: ["vwap_ref"] },
      // Dashboard signals (live values come from dashboard_signals[]).
      { rx: /\bmark\s+premium\b/i,               metrics: ["mark_premium_pct"] },
      { rx: /\bDVOL\b/i,                         metrics: ["dvol_pct"] },
      { rx: /\bBTC\s+dominance\b|\bdominance\b/i, metrics: ["btc_dominance_pct"] },
      { rx: /\bfear\s*[\&]\s*greed\b|\bF\&G\b/i, metrics: ["fear_greed"] },
      { rx: /\bmempool\b/i,                      metrics: ["mempool_fee"] },
      { rx: /\bKraken\s+premium\b/i,             metrics: ["kraken_premium_pct"] },
      { rx: /\bput[\/-]call\b/i,                 metrics: ["put_call_ratio"] },
    ];
    const inferMetricsFromText = (text) => {
      if (!text) return [];
      const found = new Set();
      for (const { rx, metrics } of TEXT_FAMILY_RULES) {
        if (rx.test(text)) {
          for (const m of metrics) {
            if (metric(m) != null) { found.add(m); break; } // first live one per family
          }
        }
      }
      return [...found];
    };
    const ConditionPill = ({ cond, stableMet }) => {
      const live = metric(cond.metric);
      const heuristic = !!cond.heuristic;
      const infoOnly  = !!cond.__infoOnly;
      const thresholdStr = live ? live.f(cond.value) : `${cond.value}${cond.unit||""}`;
      const rawMet = (live && !infoOnly) ? opCheck[cond.op](live.v, cond.value) : null;
      const isMet = infoOnly ? null
                  : (stableMet !== undefined && stableMet !== null) ? stableMet : rawMet;
      const ok = isMet === true;
      const bad = isMet === false;
      // Heuristic thresholds: green/red still indicates met/unmet, but the
      // border is dashed to signal "rule of thumb, not a live-anchored level".
      // Info-only pills (no threshold, text-inferred): neutral grey always.
      const borderC = infoOnly ? C.borderSoft
                    : ok ? C.green : bad ? C.red : C.borderSoft;
      const bgC     = infoOnly ? "#F5F5F4"
                    : ok ? C.greenBg : bad ? C.redBg : "#F5F5F4";
      const fgC     = infoOnly ? C.textSec
                    : ok ? "#166534" : bad ? "#991B1B" : C.textSec;
      const icon = infoOnly     ? "ⓘ"
                 : !live        ? "ⓘ"
                 : cond.op === ">" || cond.op === ">=" ? "▲"
                 : cond.op === "<" || cond.op === "<=" ? "▼"
                 : "●";
      const meta    = METRIC_META[cond.metric] || {};
      const labelText = meta.label || cond.metric.replace(/_/g," ");
      const borderStyle = (heuristic && !infoOnly) ? "dashed" : "solid";
      // Detect degenerate conditions against live value: ">X" when live is
      // already 1.5x past X, or "<X" when live is already 0.67x below — the
      // condition is trivially true forever so the pill is misleading. We
      // still render it, but visually de-emphasise and annotate the tooltip
      // so the trader instantly sees "this threshold is meaningless".
      const degenerateLive = (() => {
        // Flag only EXTREME threshold/live mismatches (live >3x past threshold
        // on > side, or live <1/3 of threshold on < side). Normal observation
        // bullets describing current state have a threshold AT the regime
        // boundary (e.g. BSR < 1.0 when live is 0.58) — we don't want to
        // flag those as degenerate. The 3x band catches things like
        // "OI > 34,635 BTC" when live OI is 96,832 BTC (2.8x past) —
        // actually that's exactly the case that prompted this, so 2.5x:
        if (!live || infoOnly || heuristic) return false;
        const v = cond.value;
        if (!Number.isFinite(v) || v === 0) return false;
        if (cond.op === ">" || cond.op === ">=") {
          return live.v >= v * 2.5 && live.v > v;
        }
        if (cond.op === "<" || cond.op === "<=") {
          return live.v <= v * 0.4 && live.v < v;
        }
        return false;
      })();
      const pillTitle = [
        meta.layman || null,
        degenerateLive ? "⚠ Live value is already far past this threshold — the trigger is trivially true and tells you nothing about future moves." : null,
        heuristic ? "Rule-of-thumb threshold from DeepSeek — not live-anchored." : null,
      ].filter(Boolean).join("\n\n");
      return (
        <span title={pillTitle}
              style={{ display:"inline-flex", alignItems:"center", gap:7,
            fontSize:13, fontWeight:700, padding:"4px 10px", borderRadius:5,
            background: bgC, color: fgC,
            border:`1px ${degenerateLive ? "dashed" : borderStyle} ${degenerateLive ? C.muted : borderC}`,
            opacity: degenerateLive ? 0.7 : 1 }}>
          <span style={{ fontSize:15, fontWeight:900 }}>{icon}</span>
          {infoOnly ? (
            <span>{labelText} · live reading</span>
          ) : (
            <span>{labelText} {cond.op}{" "}
              <strong style={{ fontSize:17, color:C.text }}>{thresholdStr}</strong>
              {heuristic && (
                <span title="Rule-of-thumb threshold from DeepSeek — not a live-anchored level"
                      style={{ marginLeft:5, fontSize:9, fontWeight:900, letterSpacing:0.6,
                        color:C.muted, background:"#FFFFFF",
                        border:`1px dashed ${C.borderSoft}`, borderRadius:3,
                        padding:"0 4px", textTransform:"uppercase" }}>
                  rule of thumb
                </span>
              )}
              {degenerateLive && (
                <span title="Live value already far past this threshold — trigger is trivially met"
                      style={{ marginLeft:5, fontSize:9, fontWeight:900, letterSpacing:0.6,
                        color:C.muted, background:"#FFFFFF",
                        border:`1px dashed ${C.muted}`, borderRadius:3,
                        padding:"0 4px", textTransform:"uppercase" }}>
                  already past
                </span>
              )}
            </span>
          )}
          {live ? (
            <span style={{ color:C.muted, fontWeight:600 }}>{infoOnly ? "" : "· "}now{" "}
              <strong style={{ color:C.text, fontWeight:900, fontSize:17 }}>{live.f(live.v)}</strong>
            </span>
          ) : (
            <span style={{ color:C.muted, fontWeight:600, fontStyle:"italic" }}>· source unavailable</span>
          )}
          {meta.source && (
            (() => {
              const url = meta.source.url || "";
              const isInternal = url.startsWith("/#") || url.startsWith("#");
              return (
                <a href={url}
                   title={`Jump to ${meta.source.label} in the Sources tab`}
                   {...(isInternal ? {} : { target:"_blank", rel:"noopener noreferrer" })}
                   style={{ color:C.muted, textDecoration:"underline",
                     textDecorationColor:C.borderSoft, textUnderlineOffset:2,
                     fontSize:10, fontWeight:700, marginLeft:4, letterSpacing:0.3 }}
                   onClick={(e)=>e.stopPropagation()}>↗ {meta.source.label}</a>
              );
            })()
          )}
        </span>
      );
    };
    const stableMet = (condKey, rawMet) => {
      const now = Date.now();
      const ref = hysteresisRef.current;
      let t = ref[condKey];
      if (!t) {
        t = { committed: rawMet, raw: rawMet, changeAt: now };
        ref[condKey] = t;
        return rawMet;
      }
      if (rawMet !== t.raw) {
        t.raw = rawMet;
        t.changeAt = now;
      }
      if (rawMet !== t.committed && (now - t.changeAt) >= HYSTERESIS_MS) {
        t.committed = rawMet;
      }
      return t.committed;
    };
    // Auto-extract real (machine-checkable) conditions from bullet prose
    // when Venice forgot to emit them. Common failure pattern observed:
    // "If price drops below $77,590 and the ask wall at 421.6 BTC holds"
    // — Venice ships the bullet with no conditions, so it falls through to
    // INFO and the trigger (price < $77,590, ask_depth >= 421.6 BTC) goes
    // untracked. This extractor reads the prose for explicit thresholds and
    // promotes them to first-class conditions so the bullet behaves like
    // any other actionable trigger.
    const extractCondsFromText = (text) => {
      if (!text) return [];
      const out = [];
      // Helper to canonicalize $-prices and BTC volumes
      const num = (s) => {
        const v = parseFloat(String(s).replace(/[$,]/g, ""));
        return Number.isFinite(v) ? v : null;
      };
      // PRICE — "If price [drops|breaks|falls] below $X" → price < X
      const reBelow = /\bprice\b[^.\n]*?\b(?:drops?|breaks?|falls?|moves?)\s+below\s+\$?([\d,]+(?:\.\d+)?)/gi;
      let m;
      while ((m = reBelow.exec(text)) !== null) {
        const v = num(m[1]); if (v !== null) out.push({ metric: "price", op: "<", value: v, unit: "USD" });
      }
      // PRICE — "If price [holds|stays] above $X" → price > X
      const reAbove = /\bprice\b[^.\n]*?\b(?:holds?|stays?|breaks?|moves?|reaches?)\s+above\s+\$?([\d,]+(?:\.\d+)?)/gi;
      while ((m = reAbove.exec(text)) !== null) {
        const v = num(m[1]); if (v !== null) out.push({ metric: "price", op: ">", value: v, unit: "USD" });
      }
      // PRICE — "price can't break $X" / "price fails to break $X" → price < X
      const reCantBreak = /\bprice\b[^.\n]*?\b(?:can't|cannot|can\s*not|fails?\s+to)\s+break\s+\$?([\d,]+(?:\.\d+)?)/gi;
      while ((m = reCantBreak.exec(text)) !== null) {
        const v = num(m[1]); if (v !== null) out.push({ metric: "price", op: "<", value: v, unit: "USD" });
      }
      // ASK WALL — "ask wall at X BTC holds" / "ask wall ... doesn't fill"
      const reAskWall = /\bask\s+(?:wall|stack|side)[^.\n]*?\bat\s+([\d,]+(?:\.\d+)?)\s*BTC\b/gi;
      while ((m = reAskWall.exec(text)) !== null) {
        const v = num(m[1]); if (v !== null) out.push({ metric: "ask_depth_05pct", op: ">=", value: v, unit: "BTC" });
      }
      // BID WALL — same shape on the buy side
      const reBidWall = /\bbid\s+(?:wall|stack|side)[^.\n]*?\bat\s+([\d,]+(?:\.\d+)?)\s*BTC\b/gi;
      while ((m = reBidWall.exec(text)) !== null) {
        const v = num(m[1]); if (v !== null) out.push({ metric: "bid_depth_05pct", op: ">=", value: v, unit: "BTC" });
      }
      // De-dupe: prefer the first occurrence of each metric+op
      const seen = new Set(); const dedup = [];
      for (const c of out) {
        const k = `${c.metric}|${c.op}`;
        if (seen.has(k)) continue;
        seen.add(k); dedup.push(c);
      }
      return dedup;
    };

    const evalBullet = (b) => {
      const realConds = Array.isArray(b.conditions) ? b.conditions : [];
      // If the bullet text contains explicit thresholds (e.g. "price drops
      // below $X", "ask wall at Y BTC holds") that Venice forgot to emit
      // as conditions, recover them here. These become FIRST-CLASS
      // conditions (not info-only) so the bullet's met/unmet evaluation
      // and ACTIONABLE bucketing both work properly.
      const textConds = extractCondsFromText(`${b.text || ""} ${b.if_met || ""}`);
      // Merge: keep Venice's original conditions, add text-extracted ones
      // that aren't already covered (by metric+op).
      const haveMetricOp = new Set(realConds.map(c => `${c.metric}|${c.op}`));
      const recovered = textConds.filter(c => !haveMetricOp.has(`${c.metric}|${c.op}`));
      const mergedConds = [...realConds, ...recovered.map(c => ({ ...c, __recovered: true }))];

      // If still no real conditions but text names a signal family, fall
      // through to the existing info-only pill synthesis (live readings
      // for context).
      let effectiveConds = mergedConds;
      let inferred = false;
      if (mergedConds.length === 0) {
        const combined = `${b.text || ""} ${b.if_met || ""}`;
        const inferredMetrics = inferMetricsFromText(combined);
        if (inferredMetrics.length > 0) {
          effectiveConds = inferredMetrics.map((m) => ({
            metric: m, op: "==", value: 0, unit: "", __infoOnly: true,
          }));
          inferred = true;
        }
      }
      const results = effectiveConds.map((c) => {
        if (c.__infoOnly) return { raw: null, stable: null };
        const live = metric(c.metric);
        const raw  = live ? opCheck[c.op](live.v, c.value) : null;
        if (raw === null) return { raw: null, stable: null };
        const key  = `${b.text || ""}|${c.metric}|${c.op}|${c.value}`;
        return { raw, stable: stableMet(key, raw) };
      });
      // Real conditions = whatever Venice emitted PLUS anything we
      // recovered from the prose. A bullet whose only conditions are
      // text-recovered (e.g. "price drops below $X") still gets full
      // ACTIONABLE/WAITING bucketing, which is the whole point.
      const hasRealConds = mergedConds.length > 0;
      const allMet = hasRealConds && results.every(r => r.stable === true);
      // Narrative-only bullets (no machine-checkable threshold) are no longer
      // falsely marked "actionable · live now". They show as info context.
      const actionable = hasRealConds && allMet;
      return { ...b,
        conditions: effectiveConds,
        __allMet: allMet, __hasConds: hasRealConds,
        __inferred: inferred, __actionable: actionable,
        __condResults: results };
    };
    const Bullet = ({ tone, text, conditions, if_met, heuristic_text, __allMet, __hasConds, __actionable, __inferred, __condResults }) => {
      const fired     = __allMet;
      const active    = __actionable;
      const phrase         = `${if_met || ""} ${text || ""}`.toLowerCase();
      const DIRECTIONAL_RE = /\b(long|short|buy|sell|enter|exit|rally|drop|breakout|breakdown|upside|downside|bullish|bearish)\b/;
      const NEUTRAL_RE     = /\b(indecision|no breakout|no breakdown|no break|not breaking|fails to break|range-?bound|consolidation|consolidating|stand aside|sidelines?|chop(?:py)?|neutral)\b/;
      const directional    = DIRECTIONAL_RE.test(phrase) && !NEUTRAL_RE.test(phrase);
      const firedBull = active && tone === "bullish" && directional;
      const firedBear = active && tone === "bearish" && directional;
      const narrative = !__hasConds;  // no machine-checkable thresholds at all
      let bg, border, leftBar, msgColor, actionLabel, actionIcon;
      if (firedBull)       { bg = "#ECFDF5"; border = C.green;        leftBar = C.green;  msgColor = "#15803D"; actionLabel = "BUY";   actionIcon = "▲"; }
      else if (firedBear)  { bg = "#FEF2F2"; border = C.red;          leftBar = C.red;    msgColor = "#B91C1C"; actionLabel = "SELL";  actionIcon = "▼"; }
      else if (narrative)  { bg = "#F5F5F4"; border = C.borderSoft;   leftBar = C.muted;  msgColor = "#57534E"; actionLabel = "INFO";  actionIcon = "ⓘ"; }
      else if (active)     { bg = "#F5F5F4"; border = C.borderSoft;   leftBar = C.muted;  msgColor = "#57534E"; actionLabel = "PAUSE"; actionIcon = "⏸"; }
      else                 { bg = "#FAFAF9"; border = C.borderSoft;   leftBar = C.muted;  msgColor = C.muted;   actionLabel = null;    actionIcon = "⏸"; }
      // Source lookup (direct from a condition's metric, or inferred from
      // bullet prose for narrative-only bullets that name a known family).
      const conds = conditions || [];
      let sourced = conds.map(c => METRIC_META[c.metric]).find(m => m && m.source);
      let inferredSrc = false;
      if (!sourced) {
        for (const { rx, metrics } of TEXT_FAMILY_RULES) {
          if (rx.test(text || "")) {
            for (const m of metrics) {
              const meta = METRIC_META[m];
              if (meta && meta.source) { sourced = meta; inferredSrc = true; break; }
            }
            if (sourced) break;
          }
        }
      }
      // Should we render the if_met consequence line? Only on fired bullets,
      // and skip when the text already carries the implication via an arrow
      // or em-dash of its own.
      const showIfMet = (() => {
        if (!if_met || !fired) return false;
        const textHasArrow = /→|—/.test(text || "");
        const textLongEnough = (text || "").length >= 60;
        return !(textHasArrow && textLongEnough);
      })();
      const hasRightColumn = (conds && conds.length > 0) || sourced || !sourced;
      return (
        <div style={{ display:"flex", gap:12, alignItems:"stretch",
          padding:"10px 14px", borderRadius:6,
          background: bg,
          borderLeft: `4px solid ${leftBar}`,
          border: `1px solid ${border}`,
          transition: "background-color 400ms ease, border-color 400ms ease, border-left-color 400ms ease" }}>
          {/* LEFT COLUMN — action chip + bullet narrative + if_met consequence */}
          <div style={{ flex:"1 1 0", minWidth:0, display:"flex", flexDirection:"column", gap:6 }}>
            <div style={{ display:"flex", gap:10, alignItems:"flex-start" }}>
              {actionLabel ? (
                <span style={{ display:"inline-flex", alignItems:"center", gap:4,
                  fontSize:12, fontWeight:900, color:msgColor,
                  background:"#FFFFFF", border:`2px solid ${border}`,
                  borderRadius:4, padding:"3px 9px", letterSpacing:1.5,
                  flexShrink:0, lineHeight:1.2, minWidth:64, justifyContent:"center" }}>
                  <span style={{ fontSize:13 }}>{actionIcon}</span>
                  {actionLabel}
                </span>
              ) : (
                <span style={{ fontSize:14, color:C.muted, opacity:0.6, flexShrink:0, paddingTop:3, minWidth:64, textAlign:"center" }}>
                  ⏸
                </span>
              )}
              <span style={{ flex:1, minWidth:0, overflowWrap:"anywhere" }}>
                <BullBearText text={text} size={16} baseColor={C.text} />
                {heuristic_text && (
                  <span title="Rule of thumb — bullet contains a threshold number that wasn't directly cited from this bar's live data (e.g. a regime boundary like 'BSR < 0.9' or '$1.2M liquidations'). The reasoning is preserved; treat the specific number as a heuristic, not a live anchor."
                        style={{ display:"inline-flex", alignItems:"center", marginLeft:6,
                          fontSize:9, fontWeight:900, color:C.muted, letterSpacing:0.6,
                          padding:"1px 5px", border:`1px dashed ${C.borderSoft}`,
                          borderRadius:3, background:"#FFFFFF",
                          textTransform:"uppercase", verticalAlign:"middle" }}>
                    rule of thumb
                  </span>
                )}
              </span>
            </div>
            {showIfMet && (
              <div style={{ marginLeft:28, lineHeight:1.4,
                display:"flex", flexWrap:"wrap", alignItems:"baseline", gap:6 }}>
                <span style={{ fontSize:14, fontWeight:800, color:C.muted, letterSpacing:0.2 }}>→</span>
                <span style={{ fontSize:15, fontWeight:700, color: msgColor, overflowWrap:"anywhere" }}>
                  {if_met}
                </span>
              </div>
            )}
          </div>

          {/* RIGHT COLUMN — live values + source, contained on the right so
              the eye can sweep [narrative ← | → live data] at a glance. */}
          {hasRightColumn && (
            <div style={{ flex:"0 0 auto", width:260, maxWidth:"42%",
              display:"flex", flexDirection:"column", gap:6,
              paddingLeft:10, borderLeft:`1px solid ${C.borderSoft}` }}>
              {__inferred && (
                <span title="The bullet text mentions this signal without attaching a formal threshold. The NUMBER below is the live reading pulled straight from the backend feed — NOT made up. We just added the pill so you can see the current value at a glance without digging."
                      style={{ fontSize:9, fontWeight:800, color:C.muted, letterSpacing:1,
                        padding:"2px 7px", border:`1px dashed ${C.borderSoft}`,
                        borderRadius:3, background:"#FFFFFF", alignSelf:"flex-start",
                        textTransform:"uppercase" }}>
                  live reading (no threshold)
                </span>
              )}
              {(conds && conds.length > 0) && conds.map((c, i) => (
                <div key={i} style={{ display:"flex", alignItems:"flex-start" }}>
                  <ConditionPill cond={c}
                    stableMet={(__condResults && __condResults[i]) ? __condResults[i].stable : undefined} />
                </div>
              ))}
              {sourced ? (() => {
                const url = sourced.source.url || "";
                const isInternal = url.startsWith("/#") || url.startsWith("#");
                return (
                <a href={url}
                   {...(isInternal ? {} : { target:"_blank", rel:"noopener noreferrer" })}
                   title={inferredSrc
                     ? `Inferred source: ${sourced.source.label} (bullet prose names this family) — jump to the Sources tab to see live readings.`
                     : `Jump to ${sourced.source.label} in the Sources tab`}
                   onClick={(e)=>e.stopPropagation()}
                   style={{ color:"#B91C1C", textDecoration:"none",
                     fontSize:9, fontWeight:800, letterSpacing:1,
                     padding:"3px 8px", alignSelf:"flex-start",
                     border: inferredSrc ? "1px dashed #DC2626" : "1px solid #DC2626",
                     borderRadius:3, background:"#FFFFFF", lineHeight:1.3, whiteSpace:"nowrap" }}>
                  ↗ SOURCE
                </a>
                );
              })() : (
                <span title="No verifiable source mapped for this claim — flag for audit"
                      style={{ color:C.muted, fontSize:9, fontWeight:700, letterSpacing:1,
                        padding:"3px 8px", border:`1px dashed ${C.muted}`, alignSelf:"flex-start",
                        borderRadius:3, background:"transparent",
                        lineHeight:1.3, whiteSpace:"nowrap", fontStyle:"italic" }}>
                  ? UNKNOWN
                </span>
              )}
            </div>
          )}
        </div>
      );
    };
    // Treat UNAVAILABLE like ERROR — in both cases DeepSeek didn't produce a
    // valid directional call, so the briefing + SignalRow shouldn't render
    // as if they did. Previously only ERROR was gated.
    const _dsBad = (s) => s === "ERROR" || s === "UNAVAILABLE";
    const briefingReady = traderSummary && pendingDeepseekReady && activeDeepseekPred && !_dsBad(activeDeepseekPred.signal);
    return (
      <div style={{ width:"100%", minWidth:360, display:"flex", flexDirection:"column", gap:8 }}>
        {briefingReady && (
          <div style={{ ...card, flexShrink:0, padding:"14px 16px",
            background:"#FAFAF9", border:`2px solid ${C.borderSoft}` }}>
            <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:10 }}>
              <span style={{ fontSize:12, fontWeight:900, color:C.text,
                letterSpacing:1.2, textTransform:"uppercase" }}>⚡ Trader Briefing</span>
              <span style={{ fontSize:10, color:C.muted, fontStyle:"italic" }}>
                ~30s read · decision-ready
              </span>
            </div>
            {(() => {
              // Split the edge into takeaway + supporting detail. Rendered at
              // ONE consistent font size (15px) with a small vertical gap
              // between them — user feedback was that mixing 17px + 13px looked
              // like jarring headline/caption, not a coherent story. Keep the
              // hierarchy cue via font-weight instead (takeaway bolder, detail
              // regular) so the eye can still jump to the first line in a
              // scan, without the size whiplash.
              //
              // Split rule: the EARLIEST of (first em-dash, first en-dash,
              // first sentence-end) — pick whichever break happens first so
              // the takeaway is a single punchy idea, not two sentences run
              // together.
              const raw = (traderSummary.edge || "").trim();
              const cands = [];
              const em   = raw.indexOf(" — ");
              const dash = raw.indexOf(" – ");
              if (em   >= 20) cands.push({ at: em,   skip: 3 });
              if (dash >= 20) cands.push({ at: dash, skip: 3 });
              const sent = raw.match(/[.!?]\s+[A-Z0-9$]/);
              if (sent && sent.index >= 20) cands.push({ at: sent.index + 1, skip: 1 });
              cands.sort((a,b) => a.at - b.at);
              const first = cands[0];
              let takeaway, detail;
              if (first && first.at < raw.length - 4) {
                takeaway = raw.slice(0, first.at).trim().replace(/[.!?]$/, "");
                detail   = raw.slice(first.at + first.skip).trim();
              } else {
                takeaway = raw; detail = "";
              }
              const hasMore = (traderSummary.watch?.length || traderSummary.actions?.length);
              // width:100% + overflow-wrap:anywhere makes the edge text hug
                // the card's inner width and wrap at any point — prevents the
                // "rice bounced off $77,484..." / "$77" clipping seen when the
                // narrator ran a long sentence into a narrow container.
              const edgeStyle = {
                width: "100%", maxWidth: "100%",
                overflowWrap: "anywhere", wordBreak: "break-word",
                fontSize: 17, color: C.text, lineHeight: 1.5,
              };
              return (
                <div style={{ marginBottom: hasMore ? 12 : 0 }}>
                  <div style={{ ...edgeStyle, fontWeight: 700 }}>
                    <BullBearText text={takeaway} size={17} baseColor={C.text} />
                  </div>
                  {detail && (
                    <div style={{ ...edgeStyle, marginTop: 8, fontWeight: 400 }}>
                      <BullBearText text={detail} size={17} baseColor={C.text} />
                    </div>
                  )}
                </div>
              );
            })()}
            {(() => {
              const all = [
                ...(traderSummary.watch   || []).map(b => ({ ...b, __src: "w" })),
                ...(traderSummary.actions || []).map(b => ({ ...b, __src: "a" })),
              ].map(evalBullet);
              const actionable = all.filter(b => b.__actionable);
              // Info bullets: no machine-checkable threshold at all (either
              // pure narrative or only inferred info-pills). These used to be
              // lumped into "Actionable · live now" — misleading since there's
              // nothing firing. Split into a dedicated context section.
              const info       = all.filter(b => !b.__actionable && !b.__hasConds);
              // Bullets whose conditions haven't fired are intentionally
              // excluded from the briefing (user directive: waiting-for-
              // conditions is noise). If we need them later for diagnostics,
              // they're still in the raw Venice output.
              return (<>
                {actionable.length > 0 && (
                  <div style={{ marginTop:10 }}>
                    <div style={{ fontSize:11, fontWeight:800, color:"#15803D", letterSpacing:1.2,
                      textTransform:"uppercase", marginBottom:6 }}>Actionable · {actionable.length} live now</div>
                    <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
                      {actionable.map((b, i) => <Bullet key={`act${b.__src}${i}`} {...b} />)}
                    </div>
                  </div>
                )}
                {info.length > 0 && (
                  <div style={{ marginTop:12 }}>
                    <button onClick={() => setInfoOpen(v => !v)}
                      style={{ width:"100%", display:"flex", alignItems:"center",
                        justifyContent:"space-between", gap:8,
                        background:"none", border:"none", padding:"4px 0",
                        cursor:"pointer", fontFamily:"inherit" }}>
                      <span style={{ fontSize:11, fontWeight:700, color:C.muted,
                        letterSpacing:1.2, textTransform:"uppercase" }}>
                        {infoOpen ? "▾" : "▸"} Info · live context · {info.length}
                      </span>
                      <span style={{ fontSize:9, color:C.muted, fontStyle:"italic" }}>
                        {infoOpen ? "hide" : "show"}
                      </span>
                    </button>
                    {infoOpen && (
                      <div style={{ display:"flex", flexDirection:"column", gap:6, marginTop:6 }}>
                        {info.map((b, i) => <Bullet key={`info${b.__src}${i}`} {...b} />)}
                      </div>
                    )}
                  </div>
                )}
                {/* WAITING FOR CONDITIONS section removed per user directive
                    — unfired conditions are noise. Only ACTIONABLE (live-fired)
                    and INFO (current-state narrative) bullets remain visible. */}
              </>);
            })()}
          </div>
        )}
        {!briefingReady && (
          <div style={{ ...card, flexShrink:0, padding:"12px 14px",
            display:"flex", alignItems:"center", justifyContent:"space-between", gap:10, flexWrap:"wrap" }}>
            <span style={{ fontSize:14, lineHeight:1.4 }}>
              {pendingDeepseekReady && activeDeepseekPred?.signal==="ERROR" ? (
                <span style={{ color:C.red, fontWeight:700 }}>⚠️ Analysis error this bar — detail in History tab</span>
              ) : pendingDeepseekReady && activeDeepseekPred ? (
                <span style={{ color:C.textSec, fontStyle:"italic" }}>⟳ Preparing trader briefing…</span>
              ) : (
                <span style={{ color:C.amber }}>⟳ Analyzing new bar — briefing in ~30–60s</span>
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
        )}
      </div>
    );
  // `price` intentionally omitted — it ticks every ~1s and putting it in
  // deps rebuilds the entire briefingJSX tree on every tick, defeating
  // the memoization and re-mounting Bullet / ConditionPill components.
  // `winStartPrice` changes only at bar boundaries so it stays.
  }, [traderSummary, pendingDeepseekReady, activeDeepseekPred, backendSnap,
      winStartPrice, tk, ob, oif, ls, strategies, infoOpen, barCloseUTC]);

  const strats = STRATEGY_META.filter(m=>strategies[m.key]).map(m=>({...m,...strategies[m.key]}));

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

            {/* RIGHT: Trader briefing + DeepSeek header — 50% of screen,
                with a subtle zoom:0.85 to scale everything down ~15% since
                user noted the text was feeling big at native size. The
                2-column Bullet layout + 50% width absorbs the scale cleanly
                (pills don't re-wrap). */}
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
                function SignalRow({ sig, conf, confStr }) {
                  const up      = sig === "UP";
                  const neutral = sig === "NEUTRAL";
                  const clr     = neutral ? C.amber : up ? C.green : C.red;
                  // Use the real confidence number even for NEUTRAL — NEUTRAL
                  // at 58% is a meaningful "abstain-with-some-information"
                  // call, NOT "0%". Prior hard-coded "0%" was causing the
                  // header to briefly flash "NEUTRAL 0%" during state
                  // transitions, which looked like a bug to the trader.
                  const pct     = confStr ?? (conf != null ? conf.toFixed(1)+"%" : null);
                  const barW    = conf != null ? conf : (up ? 65 : neutral ? 50 : 35);
                  return (<>
                    <div style={{ display:"flex", alignItems:"baseline", gap:8 }}>
                      <span style={{ fontSize:26, fontWeight:900, color:clr, lineHeight:1 }}>
                        {neutral ? "— NEUTRAL" : up ? "▲ UP" : "▼ DOWN"}
                      </span>
                      {pct && <span style={{ fontSize:20, fontWeight:900, color:C.text }}>{pct}</span>}
                    </div>
                    <div style={{ height:3, background:C.borderSoft, borderRadius:2, margin:"3px 0" }}>
                      <div style={{ width:`${barW}%`, height:"100%", borderRadius:2, background:clr }} />
                    </div>
                  </>);
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
                const noAccData = !deepseekAcc?.total;
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
                        : c2sig ? <SignalRow sig={c2sig} conf={c2conf} />
                        : <div style={{ fontSize:11, color:C.muted }}>Analyzing…</div>}
                    </div>

                    {/* BIG ROW — accuracy % + wins/losses INLINE WITH countdown + tabs
                        All at matching size so the trader's eye takes everything in at once. */}
                    <div style={{ borderTop:`1px solid ${C.borderSoft}`, marginTop:8, paddingTop:6 }} />
                    <div style={{ display:"flex", alignItems:"baseline", justifyContent:"space-between", gap:14, flexWrap:"wrap" }}>
                      {/* LEFT — DeepSeek accuracy % + counts */}
                      <div style={{ display:"flex", alignItems:"baseline", gap:10 }}>
                        {!noAccData ? (
                          <>
                            <span style={{ fontSize:11, fontWeight:700, color:C.muted, letterSpacing:1.2, textTransform:"uppercase" }}>Accuracy</span>
                            <span style={{ fontSize:28, fontWeight:900, color:accPct>=50?C.green:C.red, letterSpacing:1, lineHeight:1 }}>{accPct.toFixed(1)}%</span>
                            <span style={{ fontSize:12, fontWeight:800, color:C.green }}>{wins}W</span>
                            <span style={{ fontSize:12, color:C.muted }}>/</span>
                            <span style={{ fontSize:12, fontWeight:800, color:C.red }}>{losses}L</span>
                            <span style={{ fontSize:12, color:C.muted }}>/</span>
                            <span style={{ fontSize:12, fontWeight:800, color:C.muted }}>{neutral}N</span>
                          </>
                        ) : (
                          <span style={{ fontSize:12, color:C.muted }}>No historical data yet</span>
                        )}
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
                    {/* Accuracy progress bar */}
                    {!noAccData && (
                      <div style={{ display:"flex", height:5, borderRadius:3, overflow:"hidden", marginTop:4, background:C.borderSoft }}>
                        {wBarPct > 0 && <div style={{ width:`${wBarPct}%`, background:C.green }} />}
                        {lBarPct > 0 && <div style={{ width:`${lBarPct}%`, background:C.red }} />}
                        {nBarPct > 0 && <div style={{ width:`${nBarPct}%`, background:C.muted }} />}
                      </div>
                    )}
                  </div>
                );
              })()}

              {/* TRADER BRIEFING + STATUS STRIP — wrapped in ErrorBoundary so a
                  render exception (bad Venice output, missing metric, etc.) doesn't
                  crash the whole live tab. Trader keeps seeing chart + DeepSeek card. */}
              <ErrorBoundary key="briefing">
              {briefingJSX}
              </ErrorBoundary>

              {/* Historical Pattern block removed — the raw pattern data is fed into Venice
                  and surfaces in the trader briefing above as Watch/Actions bullets when relevant.
                  The full historical_analysis string is still computed, stored in Postgres, and
                  viewable in the History tab for operators. */}

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
