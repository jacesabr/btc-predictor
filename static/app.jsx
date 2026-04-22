/* @jsxRuntime classic */
// BTC Oracle Predictor — integrated dashboard + microstructure

const { useState, useEffect, useRef, useCallback } = React;

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
  { key: "polymarket",   name: "Crowd",        color: "#3730A3" },
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
  polymarket:   { short:"Real-money crowd prediction markets aggregate directional bias.", how:"High percentage alignment from participants with money on the line is statistically reliable directional pressure." },
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
        autosize:true, symbol:"BINANCE:BTCUSDT", interval:"1",
        timezone:"Etc/UTC", theme:"light", style:"1", locale:"en",
        toolbar_bg:C.bg, enable_publishing:false, hide_side_toolbar:false,
        allow_symbol_change:false, save_image:false, hide_volume:false,
        withdateranges:true, container_id:id,
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
function MicroSummary({ ob, tk, ls, lq, oif, cz, ca, fg, mp, cg, dots, caDivPct, caSig, collapsed, onToggle, noCard }) {
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
        {[["ob","OB"],["tk","TF"],["ls","LS"],["lq","LQ"],["oif","OI"],["cz","CZ"],["ca","CA"],["fg","FG"],["mp","MP"],["cg","GK"]].map(function(pair){
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

        {/* CoinAPI Cross-Exchange Price */}
        <div style={tileStyle(caSig)}>
          <div style={{ fontSize:9, color:"#1D4ED8", fontWeight:700 }}>X-Price</div>
          <div style={{ fontSize:8, color:C.muted }}>CoinAPI</div>
          {ca && ca.rate ? (<>
            <div style={{ fontSize:10, fontWeight:800, color:C.text }}>{"$"+ca.rate.toLocaleString("en-US",{maximumFractionDigits:0})}</div>
            {caDivPct!=null ? <div style={{ fontSize:8, color:Math.abs(caDivPct)>0.05?C.amber:C.muted }}>{"Δ"+(caDivPct>=0?"+":"")+caDivPct.toFixed(3)+"%"}</div> : null}
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

  return (
    <div>
      {/* Price block */}
      <div style={{ display:"flex", alignItems:"baseline", gap:10, marginBottom:10, flexWrap:"wrap" }}>
        <div>
          <div style={{ fontSize:9, fontWeight:700, color:C.muted, textTransform:"uppercase" }}>Open</div>
          <div style={{ fontSize:20, fontWeight:900, color:C.text }}>${row.start_price?.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
        </div>
        {row.end_price != null ? (<>
          <span style={{ fontSize:16, color:C.muted }}>→</span>
          <div>
            <div style={{ fontSize:9, fontWeight:700, color:C.muted, textTransform:"uppercase" }}>Close</div>
            <div style={{ fontSize:20, fontWeight:900, color:row.end_price>=row.start_price?C.green:C.red }}>
              ${row.end_price?.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})}
            </div>
          </div>
          <span style={{ fontSize:13, fontWeight:800, color:row.end_price>=row.start_price?C.green:C.red, alignSelf:"flex-end", paddingBottom:2 }}>
            {row.end_price>=row.start_price?"+":""}{((row.end_price-row.start_price)/row.start_price*100).toFixed(3)}%
          </span>
        </>) : <span style={{ fontSize:12, color:C.muted, alignSelf:"flex-end", paddingBottom:4 }}>→ PENDING</span>}
        <div style={{ marginLeft:"auto", textAlign:"right" }}>
          <div style={{ fontSize:20, fontWeight:900, color:isUp?C.green:isNeutral?C.amber:C.red }}>{isUp?"▲ UP":isNeutral?"— NEUTRAL":"▼ DOWN"}</div>
          <div style={{ fontSize:15, fontWeight:900, color:C.text }}>{row.confidence ?? "—"}%</div>
        </div>
      </div>

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

function EnsembleTab({ weights, setWeights, ob, ls, tk, oif, lq, fg, mp, ca, cz, cg, dots, price, allAccuracy, allAccuracyErr, onRefreshAccuracy }) {
  const caDivPct = (ca?.rate && price) ? ((price - ca.rate) / ca.rate * 100) : null;
  const caSig = caDivPct != null
    ? (Math.abs(caDivPct) > 0.05 ? (caDivPct > 0 ? "BEARISH_ARBI" : "BULLISH_ARBI") : "NEUTRAL")
    : null;

  // Build micro accuracy lookup for inline display: dash key → {accuracy, correct, total}
  const microAcc = {};
  (allAccuracy?.microstructure || []).forEach(r => {
    // strip "dash:" prefix to match micro row keys
    const shortKey = r.key.replace(/^dash:/, "");
    microAcc[shortKey] = r;
  });

  const microRows = [
    { key:"order_book",  uiKey:"ob",  name:"Order Book",   dot:dots.ob,  signal:ob?.sig,
      kv: ob ? [["Bid",`${ob.bv?.toFixed(0)} BTC`],["Imb",`${ob.imb>=0?"+":""}${ob.imb?.toFixed(1)}%`],["Ask",`${ob.av?.toFixed(0)} BTC`]] : [] },
    { key:"long_short",  uiKey:"ls",  name:"Long/Short",   dot:dots.ls,  signal:ls?.rSig,
      kv: ls ? [["L/S",ls.lsr?.toFixed(3)],["Retail",`${ls.retailLong?.toFixed(0)}%L`],["Smart",`${ls.smartLong?.toFixed(0)}%L`],["Div",`${ls.div>=0?"+":""}${ls.div?.toFixed(1)}%`]] : [] },
    { key:"taker_flow",  uiKey:"tk",  name:"Taker Flow",   dot:dots.tk,  signal:tk?.sig,
      kv: tk ? [["BSR",tk.bsr?.toFixed(4)],["Buy",`${tk.bv?.toFixed(0)} BTC`],["Sell",`${tk.sv?.toFixed(0)} BTC`],["Trend",tk.trend]] : [] },
    { key:"oi_funding",  uiKey:"oif", name:"OI + Funding", dot:dots.oif, signal:oif?.frSig,
      kv: oif ? [["OI",`${oif.oi?.toFixed(0)} BTC`],["FR",`${(oif.fr*100)?.toFixed(4)}%`],["Prem",`${oif.premium?.toFixed(4)}%`],["Next",oif.nextFund]] : [] },
    { key:"liquidations",uiKey:"lq",  name:"Liquidations", dot:dots.lq,  signal:lq?.sig,
      kv: lq ? [["Total",lq.total],["Long",`${lq.longCount} ($${(lq.lvol||0).toLocaleString(undefined,{maximumFractionDigits:0})})`],["Short",`${lq.shortCount} ($${(lq.svol||0).toLocaleString(undefined,{maximumFractionDigits:0})})`]] : [] },
    { key:"fear_greed",  uiKey:"fg",  name:"Fear & Greed", dot:dots.fg,  signal:fg?.sig,
      kv: fg ? [["Index",`${fg.value} — ${fg.label}`],["Prev",fg.prev],["Δ",`${fg.delta>=0?"+":""}${fg.delta}`]] : [] },
    { key:"mempool",     uiKey:"mp",  name:"Mempool",      dot:dots.mp,  signal:mp?.sig,
      kv: mp ? [["Fast",`${mp.fastest} sat/vB`],["Half",`${mp.halfHour} sat/vB`],["Pending",mp.count?.toLocaleString()]] : [] },
    { key:"coinalyze",   uiKey:"cz",  name:"Coinalyze",    dot:dots.cz,  signal:cz?.sig,
      kv: cz ? [["X-ex FR",`${(cz.fr*100)?.toFixed(4)}%`]] : [] },
    { key:"coinapi",     uiKey:"ca",  name:"CoinAPI",      dot:dots.ca,  signal:caSig,
      kv: ca?.rate ? [["Agg rate",`$${ca.rate?.toLocaleString()}`],["Div",caDivPct!=null?`${caDivPct>=0?"+":""}${caDivPct.toFixed(3)}%`:"—"]] : [] },
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
          <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:10 }}>
            <div style={{ ...label, fontSize:10 }}>Prediction Accuracy — All Sources</div>
            <button
              onClick={() => {
                if (!confirm("Reset all win/loss scores? Historical bars are kept but scores restart from now.")) return;
                fetch("/reset-scores",{method:"POST"})
                  .then(()=>fetch("/weights/update",{method:"POST"}))
                  .then(()=>Promise.all([
                    fetch("/weights").then(r=>r.json()).then(setWeights),
                    fetch("/accuracy/all?n=200").then(r=>r.json()),
                  ]));
              }}
              style={{ fontSize:9, padding:"2px 8px", borderRadius:3,
                border:`1px solid ${C.border}`, background:C.surface,
                color:C.textSec, cursor:"pointer", fontFamily:"inherit", letterSpacing:1 }}>
              RECALIBRATE
            </button>
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
  ["order_book",        "Order Book",         "Binance spot · depth-20"],
  ["long_short",        "Long/Short Ratio",   "Binance Futures · 5m"],
  ["taker_flow",        "Taker Flow",         "Binance Futures aggressor"],
  ["oi_funding",        "OI + Funding",       "Binance Futures"],
  ["liquidations",      "Liquidations",       "OKX · last 5 min"],
  ["bybit_liquidations","Cross-ex Liqs",      "OKX isolated-margin"],
  ["fear_greed",        "Fear & Greed",       "Alternative.me · daily"],
  ["mempool",           "Mempool",            "mempool.space"],
  ["coingecko",         "CoinGecko",          "24h market data"],
  ["btc_dominance",     "BTC Dominance",      "CoinGecko global"],
  ["deribit_dvol",      "DVOL",               "Deribit implied vol"],
  ["kraken_premium",    "Kraken Premium",     "Kraken vs OKX spread"],
  ["oi_velocity",       "OI Velocity",        "Binance OI hist · 30m"],
  ["spot_whale_flow",   "Spot Whale Flow",    "Kraken trades · 5m"],
  ["okx_funding",       "OKX Funding",        "OKX funding rate"],
  ["top_position_ratio","Top Trader Ratio",   "Binance top traders"],
  ["funding_trend",     "Funding Trend",      "Binance 6-period avg"],
  ["coinalyze",         "Coinalyze",          "Cross-ex funding"],
];

function microSignalKey(key, d) {
  if (!d) return null;
  if (key === "long_short") return d.retail_signal_contrarian || d.signal;
  if (key === "oi_funding") return d.funding_signal || d.signal;
  return d.signal || null;
}

function microKV(key, d) {
  if (!d) return [];
  if (key === "order_book")         return [["Bid",`${d.bid_vol_btc?.toFixed(0)} BTC`],["Imb",`${d.imbalance_pct>=0?"+":""}${d.imbalance_pct?.toFixed(1)}%`],["Ask",`${d.ask_vol_btc?.toFixed(0)} BTC`]];
  if (key === "long_short")         return [["L/S",d.retail_lsr?.toFixed(3)],["Retail",`${d.retail_long_pct?.toFixed(0)}%L`],["Smart",`${d.smart_money_long_pct?.toFixed(0)}%L`],["Δ",`${(d.smart_vs_retail_div_pct>=0?"+":"")}${d.smart_vs_retail_div_pct?.toFixed(1)}%`]];
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
  const pm = snap.polymarket || {};
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

        {/* Ensemble + Polymarket */}
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
          {pm.is_live && (
            <div style={{ ...card, flex:1 }}>
              <div style={{ ...label, marginBottom:4 }}>Polymarket</div>
              <div style={{ fontSize:14, fontWeight:900, color:C.green }}>{((pm.yes_price||0)*100).toFixed(1)}% UP</div>
              <div style={{ height:3, background:C.borderSoft, borderRadius:2, margin:"4px 0" }}>
                <div style={{ width:`${(pm.yes_price||0)*100}%`, height:"100%", background:C.green }} />
                <div style={{ width:`${(1-(pm.yes_price||0))*100}%`, height:"100%", background:C.red, marginTop:-3 }} />
              </div>
              <div style={{ fontSize:9, color:C.muted }}>Odds: 1:{pm.market_odds?.toFixed(3)}</div>
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

function SourceHistoryTab({ sourceHistory, selectedSource, setSelectedSource }) {
  const record = sourceHistory[selectedSource] || null;
  const ds = record?.dashboard_signals_snapshot || {};
  const strats = record?.strategy_snapshot || {};

  function fmtTime(ts) {
    if (!ts) return "—";
    const d = new Date(ts * 1000);
    return String(d.getUTCHours()).padStart(2,"0") + ":" +
           String(d.getUTCMinutes()).padStart(2,"0") + " UTC";
  }

  return (
    <div style={{ display:"flex", gap:8, height:"100%", minHeight:0 }}>

      {/* Left column — window selector */}
      <div style={{ ...card, flex:"0 0 200px", display:"flex", flexDirection:"column", minHeight:0 }}>
        <div style={{ ...label, marginBottom:8, flexShrink:0 }}>Windows</div>
        <div style={{ flex:1, overflowY:"auto" }}>
          {sourceHistory.length === 0 && (
            <div style={{ color:C.muted, fontSize:10, padding:8 }}>
              No source history yet — data is stored from the next DeepSeek bar.
            </div>
          )}
          {sourceHistory.map((rec, i) => {
            const hasSig  = rec.dashboard_signals_snapshot && Object.keys(rec.dashboard_signals_snapshot).length > 0;
            const correct = rec.correct;
            const active  = i === selectedSource;
            return (
              <div key={i} onClick={() => setSelectedSource(i)}
                style={{ padding:"7px 10px", cursor:"pointer", borderRadius:4, marginBottom:2,
                  background: active ? C.amberBg : "transparent",
                  border: `1px solid ${active ? C.amberBorder : "transparent"}` }}>
                <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between" }}>
                  <span style={{ fontSize:10, fontWeight:700, color: active ? C.amber : C.text }}>
                    {fmtTime(rec.window_start)}
                  </span>
                  {correct != null && (
                    <span style={{ fontSize:9, fontWeight:700,
                      color: correct ? C.green : C.red }}>
                      {correct ? "✓" : "✕"}
                    </span>
                  )}
                </div>
                <div style={{ display:"flex", alignItems:"center", gap:4, marginTop:2 }}>
                  <span style={{ fontSize:10, fontWeight:700,
                    color: rec.signal==="UP" ? C.green : C.red }}>
                    {rec.signal==="UP" ? "▲" : "▼"} {rec.confidence}%
                  </span>
                  {!hasSig && (
                    <span style={{ fontSize:8, color:C.muted, fontStyle:"italic" }}>no snapshot</span>
                  )}
                </div>
                {rec.start_price && (
                  <div style={{ fontSize:9, color:C.textSec }}>
                    ${rec.start_price.toFixed(0)}
                    {rec.end_price ? ` → $${rec.end_price.toFixed(0)}` : ""}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Right panel — source detail */}
      <div style={{ flex:1, display:"flex", flexDirection:"column", gap:8, minWidth:0, overflowY:"auto", paddingBottom:8 }}>

        {!record ? (
          <div style={{ ...card, flex:1, display:"flex", alignItems:"center", justifyContent:"center",
            color:C.muted, fontSize:11 }}>
            Select a window from the left to inspect its source data.
          </div>
        ) : (
          <>
            {/* Header */}
            <div style={{ ...card, flexShrink:0, display:"flex", gap:16, alignItems:"center", flexWrap:"wrap" }}>
              <div>
                <div style={{ ...label, marginBottom:2 }}>Window</div>
                <div style={{ fontSize:13, fontWeight:900, color:C.text }}>
                  {fmtTime(record.window_start)}
                  <span style={{ fontSize:10, fontWeight:400, color:C.textSec, marginLeft:6 }}>
                    → {fmtTime(record.window_end)}
                  </span>
                </div>
              </div>
              <div>
                <div style={{ ...label, marginBottom:2 }}>DeepSeek Signal</div>
                <div style={{ fontSize:13, fontWeight:900,
                  color: record.signal==="UP" ? C.green : C.red }}>
                  {record.signal==="UP" ? "▲ UP" : "▼ DOWN"} · {record.confidence}%
                </div>
              </div>
              {record.actual_direction && (
                <div>
                  <div style={{ ...label, marginBottom:2 }}>Outcome</div>
                  <div style={{ fontSize:13, fontWeight:900,
                    color: record.correct ? C.green : C.red }}>
                    {record.correct ? "✓ WIN" : "✕ LOSS"} · actual {record.actual_direction}
                  </div>
                </div>
              )}
              <div>
                <div style={{ ...label, marginBottom:2 }}>Latency</div>
                <div style={{ fontSize:12, fontWeight:700, color:C.textSec }}>
                  {record.latency_ms ? `${(record.latency_ms/1000).toFixed(1)}s` : "—"}
                </div>
              </div>
            </div>

            {/* DeepSeek interpretation of the data */}
            <div style={{ ...card, flexShrink:0 }}>
              <div style={{ ...label, marginBottom:6 }}>How DeepSeek Used the Data</div>
              {record.data_received && (
                <div style={{ marginBottom:8 }}>
                  <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1,
                    textTransform:"uppercase", marginBottom:3 }}>Data Received (self-reported)</div>
                  <div style={{ fontSize:10, color:C.textSec, lineHeight:1.5 }}>{record.data_received}</div>
                </div>
              )}
              {record.reasoning && (
                <div style={{ marginBottom:8 }}>
                  <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1,
                    textTransform:"uppercase", marginBottom:3 }}>Reasoning</div>
                  <div style={{ fontSize:10, color:C.text, lineHeight:1.6,
                    borderLeft:`3px solid ${record.signal==="UP"?C.greenBorder:C.redBorder}`,
                    paddingLeft:8, background:record.signal==="UP"?C.greenBg:C.redBg,
                    padding:"6px 8px", borderRadius:4 }}>{record.reasoning}</div>
                </div>
              )}
              {record.narrative && (
                <div style={{ marginBottom:8 }}>
                  <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1,
                    textTransform:"uppercase", marginBottom:3 }}>Extended Narrative</div>
                  <div style={{ fontSize:10, color:C.textSec, lineHeight:1.55 }}>{record.narrative}</div>
                </div>
              )}
              {record.data_requests && record.data_requests.toUpperCase() !== "NONE" && record.data_requests.trim() !== "" && (
                <div>
                  <div style={{ fontSize:9, fontWeight:700, color:C.amber, letterSpacing:1,
                    textTransform:"uppercase", marginBottom:3 }}>⚠ Data Gaps Flagged</div>
                  <div style={{ fontSize:10, color:C.amber, lineHeight:1.5 }}>{record.data_requests}</div>
                </div>
              )}
              {record.free_observation && (
                <div style={{ marginTop:8 }}>
                  <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1,
                    textTransform:"uppercase", marginBottom:3 }}>Free Observation</div>
                  <div style={{ fontSize:10, color:C.textSec, lineHeight:1.5 }}>{record.free_observation}</div>
                </div>
              )}
            </div>

            {/* Microstructure source cards */}
            <div style={{ display:"flex", alignItems:"center", gap:8, flexShrink:0 }}>
              <div style={{ ...label }}>Microstructure Snapshot · embedding inputs</div>
              {(() => { const n = SOURCE_DEFS.filter(d=>ds[d.key]).length; const tot = SOURCE_DEFS.length; return (
                <span style={{ fontSize:9, fontWeight:700, padding:"1px 6px", borderRadius:3,
                  background:n>=14?C.greenBg:n>=8?C.amberBg:C.redBg,
                  color:n>=14?C.green:n>=8?C.amber:C.red,
                  border:`1px solid ${n>=14?C.greenBorder:n>=8?C.amberBorder:C.redBorder}` }}>
                  {n}/{tot} sources
                </span>
              ); })()}
            </div>
            <div style={{ display:"grid", gridTemplateColumns:"repeat(2,1fr)", gap:6, flexShrink:0 }}>
              {SOURCE_DEFS.map(def => (
                <SourceCard key={def.key} def={def} data={ds[def.key] || null} />
              ))}
            </div>

            {/* Strategy snapshot */}
            {Object.keys(strats).length > 0 && (
              <>
                <div style={{ ...label, flexShrink:0 }}>Strategy Votes (as fed to DeepSeek)</div>
                <div style={{ ...card, flexShrink:0 }}>
                  <table style={{ width:"100%", borderCollapse:"collapse", fontSize:10 }}>
                    <thead>
                      <tr style={{ borderBottom:`2px solid ${C.border}` }}>
                        {["Strategy","Signal","Conf","Reasoning"].map(h => (
                          <th key={h} style={{ textAlign:"left", padding:"4px 8px", fontSize:9,
                            letterSpacing:1, textTransform:"uppercase", color:C.muted }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(strats).map(([key, v]) => {
                        const m = STRATEGY_META.find(x => x.key === key);
                        const name = m ? m.name : key;
                        const sig = v?.signal || "—";
                        const conf = v?.confidence != null ? `${(v.confidence*100).toFixed(0)}%` : "—";
                        const reason = v?.reasoning || "";
                        return (
                          <tr key={key} style={{ borderBottom:`1px solid ${C.borderSoft}` }}>
                            <td style={{ ...td, color: m?.color || C.text, fontWeight:700 }}>{name}</td>
                            <td style={{ ...td, fontWeight:700,
                              color: sig==="UP" ? C.green : sig==="DOWN" ? C.red : C.amber }}>
                              {sig==="UP"?"▲ UP":sig==="DOWN"?"▼ DN":sig}
                            </td>
                            <td style={td}>{conf}</td>
                            <td style={{ ...td, color:C.textSec, fontSize:9, maxWidth:260,
                              whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis" }}>
                              {reason}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </>
        )}
      </div>
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
function EmbeddingAuditTab({ embeddingAuditLog }) {
  const [expandedAudit, setExpandedAudit] = React.useState(null);

  if (!embeddingAuditLog || embeddingAuditLog.length === 0) {
    return (
      <div style={{ padding:24, textAlign:"center", color:C.muted }}>
        <div style={{ fontSize:12, marginBottom:16 }}>No embedding audits yet. First audit fires ~4 hours after startup.</div>
        <button onClick={()=>fetch("/api/embedding-audit/run", {method:"POST"}).then(()=>console.log("Audit triggered"))}
          style={{ background:C.amberBg, color:C.amber, border:`1px solid ${C.amberBorder}`,
            padding:"6px 14px", borderRadius:4, cursor:"pointer", fontSize:11, fontFamily:"inherit", fontWeight:600 }}>
          Run Audit Now
        </button>
      </div>
    );
  }

  return (
    <div style={{ display:"flex", flexDirection:"column", height:"100%", overflow:"hidden" }}>
      {/* Header */}
      <div style={{ ...card, flexShrink:0, padding:"12px 16px", display:"flex", justifyContent:"space-between", alignItems:"center" }}>
        <div>
          <div style={label}>Embedding Audit Log</div>
          <div style={{ fontSize:10, color:C.muted, marginTop:2 }}>
            Every 4 hours, DeepSeek reasoner audits the embedding pipeline. Issues, patterns, suggestions below.
          </div>
        </div>
        <button onClick={()=>fetch("/api/embedding-audit/run", {method:"POST"}).then(()=>console.log("Audit triggered"))}
          style={{ background:C.amberBg, color:C.amber, border:`1px solid ${C.amberBorder}`,
            padding:"6px 12px", borderRadius:4, cursor:"pointer", fontSize:10, fontFamily:"inherit", fontWeight:600 }}>
          Run Now
        </button>
      </div>

      {/* Audit list */}
      <div style={{ flex:1, overflowY:"auto", padding:"12px 0" }}>
        {embeddingAuditLog.map((audit, idx) => {
          const isExpanded = expandedAudit === idx;
          const stats = audit.stats || {};
          return (
            <div key={idx} style={{ borderBottom:`1px solid ${C.border}`, padding:"0 16px" }}>
              {/* Summary row */}
              <div onClick={()=>setExpandedAudit(isExpanded ? null : idx)}
                style={{ padding:"10px 0", cursor:"pointer", display:"flex", gap:12, alignItems:"center", justifyContent:"space-between" }}>
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ fontWeight:600, fontSize:11, color:
                    audit.audit_signal === "GOOD" ? C.green :
                    audit.audit_signal === "NEEDS_IMPROVEMENT" ? C.amber :
                    audit.audit_signal === "CRITICAL" ? C.red : C.text }}>
                    {audit.timestamp_str}  —  {audit.audit_signal || "UNKNOWN"}
                  </div>
                  <div style={{ fontSize:10, color:C.textSec, marginTop:3, lineHeight:1.4 }}>
                    {audit.summary || "No summary"}
                  </div>
                  <div style={{ fontSize:9, color:C.muted, marginTop:4, display:"flex", gap:16 }}>
                    <span>Coverage: {stats.coverage_pct}%</span>
                    <span>HA Acc: {stats.ha_accuracy ? `${(stats.ha_accuracy * 100).toFixed(1)}%` : "N/A"}</span>
                    <span>Elapsed: {audit.elapsed_s}s</span>
                  </div>
                </div>
                <div style={{ color:C.muted, fontSize:14, flexShrink:0 }}>
                  {isExpanded ? "▼" : "▶"}
                </div>
              </div>

              {/* Expanded details */}
              {isExpanded && (
                <div style={{ paddingBottom:12, borderTop:`1px solid ${C.borderSoft}`, marginTop:8, paddingTop:12, fontSize:10, lineHeight:1.6 }}>
                  {/* Issues */}
                  {audit.issues && audit.issues.length > 0 && (
                    <div style={{ marginBottom:12 }}>
                      <div style={{ fontWeight:600, color:C.red, marginBottom:6 }}>⚠ Issues Found: {audit.issues.length}</div>
                      {audit.issues.map((issue, i) => (
                        <div key={i} style={{ background:C.surfaceAlt, padding:"8px", borderRadius:3, marginBottom:6, borderLeft:`3px solid ${C.red}`, whiteSpace:"pre-wrap", wordBreak:"break-word" }}>
                          {issue}
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Suggestions */}
                  {audit.suggestions && audit.suggestions.length > 0 && (
                    <div style={{ marginBottom:12 }}>
                      <div style={{ fontWeight:600, color:C.amber, marginBottom:6 }}>💡 Suggestions: {audit.suggestions.length}</div>
                      {audit.suggestions.map((sugg, i) => (
                        <div key={i} style={{ background:C.surfaceAlt, padding:"8px", borderRadius:3, marginBottom:6, borderLeft:`3px solid ${C.amber}`, whiteSpace:"pre-wrap", wordBreak:"break-word" }}>
                          {sugg}
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Full analysis (collapsible) */}
                  {audit.full_analysis && (
                    <div>
                      <details style={{ marginBottom:8 }}>
                        <summary style={{ cursor:"pointer", fontWeight:600, color:C.textSec, marginBottom:6 }}>
                          Full Analysis (DeepSeek Reasoning)
                        </summary>
                        <div style={{ background:C.surfaceAlt, padding:"10px", borderRadius:3, marginTop:6, whiteSpace:"pre-wrap", wordBreak:"break-word", maxHeight:"300px", overflowY:"auto", fontSize:9 }}>
                          {audit.full_analysis.substring(0, 3000)}{audit.full_analysis.length > 3000 ? "... (truncated)" : ""}
                        </div>
                      </details>
                    </div>
                  )}

                  {/* Stats summary */}
                  <div style={{ background:C.surfaceAlt, padding:"8px", borderRadius:3, marginTop:8, fontSize:9, display:"grid", gridTemplateColumns:"1fr 1fr", gap:6 }}>
                    <div>Total Bars: {stats.total_bars}</div>
                    <div>Embedded: {stats.embedded_bars}</div>
                    <div>HA Correct: {stats.ha_correct}/{stats.ha_total}</div>
                    <div>Sim Min/Max: {stats.sim_stats?.min}/{stats.sim_stats?.max}</div>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Errors Tab ────────────────────────────────────────────────
function ErrorsTab({ errors }) {
  const [expanded, setExpanded] = React.useState(null);
  if (!errors || errors.length === 0) return (
    <div style={{ padding:24, color:C.muted, fontSize:12, textAlign:"center" }}>
      No errors recorded this session. ERROR/UNAVAILABLE bars will appear here.
    </div>
  );
  return (
    <div style={{ overflow:"auto", height:"100%", padding:"6px 0" }}>
      <div style={{ fontSize:10, color:C.muted, letterSpacing:1, marginBottom:8, paddingLeft:4 }}>
        {errors.length} ERROR/UNAVAILABLE BAR{errors.length!==1?"S":""} — not embedded, excluded from history
      </div>
      {errors.map((e, i) => {
        const isOpen = expanded === i;
        const isErr  = e.signal === "ERROR";
        return (
          <div key={i} style={{ ...card, marginBottom:6, borderLeft:`3px solid ${isErr?C.red:C.amber}` }}>
            <div style={{ display:"flex", alignItems:"center", gap:10, cursor:"pointer" }}
                 onClick={() => setExpanded(isOpen ? null : i)}>
              <span style={{ fontSize:9, fontWeight:700, color:isErr?C.red:C.amber, letterSpacing:1 }}>
                {e.signal}
              </span>
              <span style={{ fontSize:10, color:C.muted }}>{e.bar_time}</span>
              <span style={{ fontSize:10, color:C.muted }}>Bar #{e.bar_num}</span>
              <span style={{ marginLeft:"auto", fontSize:9, color:C.muted }}>{isOpen?"▲":"▼"}</span>
            </div>
            {isOpen && (
              <div style={{ marginTop:8, borderTop:`1px solid ${C.border}`, paddingTop:8 }}>
                {e.reasoning && (
                  <div style={{ marginBottom:6 }}>
                    <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:3 }}>REASONING</div>
                    <pre style={{ fontSize:10, color:C.text, whiteSpace:"pre-wrap", margin:0 }}>{e.reasoning}</pre>
                  </div>
                )}
                {e.raw_response && (
                  <div>
                    <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, marginBottom:3 }}>RAW RESPONSE</div>
                    <pre style={{ fontSize:10, color:C.red, whiteSpace:"pre-wrap", margin:0, maxHeight:300, overflow:"auto",
                      background:"#1a0a0a", padding:8, borderRadius:4 }}>{e.raw_response}</pre>
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
  const [ensemble,       setEnsemble]       = useState(null);
  const [ensemblePred,   setEnsemblePred]   = useState(null);
  const [deepseekPred,   setDeepseekPred]   = useState(null);
  const [deepseekAcc,    setDeepseekAcc]    = useState(null);
  const [agreeAcc,       setAgreeAcc]       = useState(null);
  const [deepseekLog,    setDeepseekLog]    = useState([]);
  const [backtest,       setBacktest]       = useState(null);
  const [preds,          setPreds]          = useState([]);
  const [weights,        setWeights]        = useState({});
  const [bestIndicator,  setBestIndicator]  = useState(null);
  const [microOpen,      setMicroOpen]      = useState(false);
  const [indicatorsOpen, setIndicatorsOpen] = useState(false);

  const [polymarket,            setPolymarket]            = useState(null);
  const [specialistAt,          setSpecialistAt]          = useState(null);
  const [pendingDeepseekReady,  setPendingDeepseekReady]  = useState(false);
  const [pendingDeepseekPred,   setPendingDeepseekPred]   = useState(null);
  const [historicalAnalysis,    setHistoricalAnalysis]    = useState("");
  const [historicalContext,     setHistoricalContext]     = useState("");
  const [serviceUnavailable,    setServiceUnavailable]    = useState(false);
  const [serviceUnavailReason,  setServiceUnavailReason]  = useState("");
  const [binanceExpert,         setBinanceExpert]         = useState(null);
  const [tab,                   setTab]                   = useState("live");
  const [errorLog,              setErrorLog]              = useState([]);
  const [backendSnap,    setBackendSnap]    = useState(null);
  const [sourceHistory,  setSourceHistory]  = useState([]);
  const [selectedSource, setSelectedSource] = useState(0);
  const [allAccuracy,    setAllAccuracy]    = useState(null);
  const [allAccuracyErr, setAllAccuracyErr] = useState(false);
  const [embeddingAuditLog, setEmbeddingAuditLog] = useState([]);
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
  const [ca,  setCa]  = useState(null);  // CoinAPI (via proxy)
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
        if (d.prediction) {
          const p=d.prediction;
          setEnsemble({ signal:p.signal, confidence:p.confidence, bullish:p.bullish_count, bearish:p.bearish_count, source:p.source });
        }
        if (d.ensemble_prediction) {
          const ep=d.ensemble_prediction;
          setEnsemblePred({ signal:ep.signal, confidence:ep.confidence, bullish:ep.bullish_count, bearish:ep.bearish_count, upProb:ep.up_probability });
        }
        if (d.deepseek_prediction)                    setDeepseekPred(d.deepseek_prediction);
        if (d.pending_deepseek_prediction)            setPendingDeepseekPred(d.pending_deepseek_prediction);
        if (d.agree_accuracy)                         setAgreeAcc(d.agree_accuracy);
        if (d.polymarket)                             setPolymarket(d.polymarket);
        if (d.specialist_completed_at)                setSpecialistAt(d.specialist_completed_at);
        if (d.pending_deepseek_ready !== undefined)   setPendingDeepseekReady(d.pending_deepseek_ready);
        if (d.bar_historical_analysis !== undefined)  setHistoricalAnalysis(d.bar_historical_analysis || "");
        if (d.bar_historical_context !== undefined)   setHistoricalContext(d.bar_historical_context || "");
        if (d.service_unavailable !== undefined)      setServiceUnavailable(!!d.service_unavailable);
        if (d.service_unavailable_reason !== undefined) setServiceUnavailReason(d.service_unavailable_reason || "");
        if (d.bar_binance_expert && d.bar_binance_expert.signal) setBinanceExpert(d.bar_binance_expert);
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
        if (d.specialist_completed_at)     setSpecialistAt(d.specialist_completed_at);
        if (d.bar_historical_analysis !== undefined) setHistoricalAnalysis(d.bar_historical_analysis || "");
        if (d.bar_historical_context !== undefined)  setHistoricalContext(d.bar_historical_context || "");
        if (d.service_unavailable !== undefined)     setServiceUnavailable(!!d.service_unavailable);
        if (d.service_unavailable_reason !== undefined) setServiceUnavailReason(d.service_unavailable_reason || "");
      } catch(_) {}
    }
    pollDS();
    const id = setInterval(pollDS, 2000);
    return () => clearInterval(id);
  }, []);

  // ── REST polling (30s) ────────────────────────────────────────
  useEffect(() => {
    const safe = (url, setter) =>
      fetch(url).then(r => r.ok ? r.json() : null).then(d => { if (d != null) setter(d); }).catch(()=>{});
    function poll() {
      safe("/backtest",                setBacktest);
      safe("/predictions/recent?n=500", setPreds);
      safe("/weights",                 setWeights);
      safe("/deepseek/accuracy",       setDeepseekAcc);
      safe("/history/all",             setDeepseekLog);
      safe("/accuracy/agree",          setAgreeAcc);
      safe("/best-indicator",          setBestIndicator);
    }
    poll();
    const id = setInterval(poll, 30000);
    return () => clearInterval(id);
  }, []);

  // ── Backend snapshot — fetch on tab switch + new DS window ───
  useEffect(() => {
    if (tab !== "backend") return;
    fetch("/backend").then(r=>r.json()).then(setBackendSnap).catch(()=>{});
  }, [tab]);

  // ── Error log — fetch on tab switch ──────────────────────────
  useEffect(() => {
    if (tab !== "errors") return;
    fetch("/errors").then(r=>r.json()).then(d=>setErrorLog(d.errors||[])).catch(()=>{});
  }, [tab]);

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
  useEffect(() => {
    if (tab !== "ensemble" && tab !== "history") return;
    fetchAllAccuracy();
  }, [tab, fetchAllAccuracy]);
  useEffect(() => {
    if (tab !== "ensemble" && tab !== "history") return;
    const id = setInterval(fetchAllAccuracy, 20000);
    return () => clearInterval(id);
  }, [tab, fetchAllAccuracy]);

  // ── Embedding audit — fetch on tab switch ──────────────────────
  useEffect(() => {
    if (tab !== "embed_audit") return;
    fetch("/api/embedding-audit").then(r=>r.json()).then(d=>setEmbeddingAuditLog(d.audit_log||[])).catch(()=>{});
  }, [tab]);

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
    try {
      const d = await fetch("https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=3").then(r=>r.json());
      const latest = d[d.length-1]||{};
      const bsr = parseFloat(latest.buySellRatio||1);
      const bv  = parseFloat(latest.buyVol||0);
      const sv  = parseFloat(latest.sellVol||0);
      const sig = bsr>1.12?"BULLISH":bsr<0.90?"BEARISH":"NEUTRAL";
      // 3-bar trend
      let trend="MIXED";
      if (d.length>=3) {
        const r=[...d].map(x=>parseFloat(x.buySellRatio||1));
        if (r[2]>r[1]&&r[1]>r[0]) trend="ACC↑";
        else if (r[2]<r[1]&&r[1]<r[0]) trend="ACC↓";
      }
      setTk({ bsr, bv, sv, sig, trend });
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

  const fetchCA = useCallback(async () => {
    try {
      const d = await fetch("/api/proxy/coinapi").then(r=>r.json());
      if (d.error) throw new Error(d.error);
      setCa({ rate:d.rate });
      dot("ca","live");
    } catch { dot("ca","err"); }
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
    fetchOB(); fetchLS(); fetchTaker(); fetchOIF(); fetchLQ(); fetchFG(); fetchMP(); fetchCA(); fetchCZ(); fetchGecko();
    const ids = [
      setInterval(fetchOB,    5000),
      setInterval(fetchLS,   15000),
      setInterval(fetchTaker,15000),
      setInterval(fetchOIF,  15000),
      setInterval(fetchLQ,   30000),
      setInterval(fetchFG,  300000),
      setInterval(fetchMP,   60000),
      setInterval(fetchCA,   60000),
      setInterval(fetchCZ,   60000),
      setInterval(fetchGecko,60000),
    ];
    return () => ids.forEach(clearInterval);
  }, [fetchOB,fetchLS,fetchTaker,fetchOIF,fetchLQ,fetchFG,fetchMP,fetchCA,fetchCZ,fetchGecko]);

  // ── Derived values ────────────────────────────────────────────
  const priceDelta = (price&&winStartPrice) ? price-winStartPrice : 0;
  const pricePct   = winStartPrice ? priceDelta/winStartPrice*100 : 0;
  const polyLive   = polymarket?.is_live===true && polymarket?.market_odds>0;
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
  const activeDeepseekPred = (pendingDeepseekReady && pendingDeepseekPred) ? pendingDeepseekPred : deepseekPred;
  // Apply 70% threshold: Combined Indicators only trades when ≥70% confident; below = NEUTRAL (no trade)
  const effectiveEnsSig = ensemblePred
    ? (ensemblePred.signal !== "NEUTRAL" && ensemblePred.confidence * 100 < 70 ? "NEUTRAL" : ensemblePred.signal)
    : null;
  // aiAgree only when we have a live (pending) prediction — stale results must not show as current
  const aiAgree = (pendingDeepseekReady && activeDeepseekPred && ensemblePred && activeDeepseekPred.signal!=="ERROR")
                    ? activeDeepseekPred.signal===effectiveEnsSig : null;
  const strats = STRATEGY_META.filter(m=>strategies[m.key]).map(m=>({...m,...strategies[m.key]}));

  // Cross-exchange divergence (for microstructure display)
  const caDivPct = (ca?.rate&&winStartPrice) ? ((winStartPrice-ca.rate)/ca.rate*100) : null;
  const caSig    = caDivPct!=null ? (Math.abs(caDivPct)>0.05 ? (caDivPct>0?"BEARISH_ARBI":"BULLISH_ARBI") : "NEUTRAL") : null;

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

      {/* ── TABS ── */}
      <div style={{ display:"flex", borderBottom:`1px solid ${C.border}`, padding:"0 14px",
        flexShrink:0, background:C.surface }}>
        {/* Countdown integrated into tab bar */}
        <div style={{ display:"flex", alignItems:"center", gap:8, marginRight:16, paddingRight:14,
          borderRight:`1px solid ${C.border}` }}>
          <div style={{ textAlign:"center" }}>
            <div style={{ fontSize:28, fontWeight:900, color:timeLeft<60?C.red:timeLeft<120?C.amber:C.green,
              letterSpacing:2, fontVariantNumeric:"tabular-nums", lineHeight:1 }}>{mins}:{secs}</div>
            <div style={{ fontSize:10, color:C.muted, letterSpacing:1, marginTop:2 }}>bar closes</div>
          </div>
          {price && (
            <div style={{ textAlign:"center" }}>
              <div style={{ fontSize:8, color:C.muted, letterSpacing:1, textTransform:"uppercase" }}>BTC/USD</div>
              <div style={{ fontSize:16, fontWeight:900, color:C.text }}>${price.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
              {winStartPrice && (
                <div style={{ fontSize:9, fontWeight:700, color:priceDelta>=0?C.green:C.red }}>
                  {priceDelta>=0?"+":""}{pricePct.toFixed(3)}%
                </div>
              )}
            </div>
          )}
        </div>
        {[["live","LIVE"],["history","HISTORY"],["audit","AUDIT"],["ensemble","ENSEMBLE"],["sources","SOURCES"],["embed_audit","EMBED AUDIT"],["binance_test","BINANCE TEST"],["errors","ERRORS"]].map(([t,label])=>(
          <button key={t} onClick={()=>setTab(t)} style={{
            background:"none", border:"none",
            borderBottom:tab===t?`2px solid ${C.amber}`:"2px solid transparent",
            color:tab===t?C.amber:C.muted, fontWeight:tab===t?700:400,
            padding:"5px 18px", cursor:"pointer",
            fontSize:11, fontFamily:"inherit", letterSpacing:2 }}>{label}</button>
        ))}
        <div style={{ marginLeft:"auto", display:"flex", alignItems:"center", gap:8, paddingRight:4 }}>
          <button onClick={function(){
            if (window._forcingPredict) return;
            window._forcingPredict = true;
            var btn = document.getElementById("force-predict-btn");
            if (btn) { btn.textContent = "Calculating…"; btn.style.opacity = "0.6"; btn.style.cursor = "default"; }
            fetch("/force-predict",{method:"POST"}).then(function(r){ return r.json(); }).then(function(d){
              window._forcingPredict = false;
              if (btn) { btn.textContent = "Force Predict"; btn.style.opacity = "1"; btn.style.cursor = "pointer"; }
              if (d.status!=="ok") { alert("Force predict: "+(d.detail||"unknown error")); }
            }).catch(function(e){
              window._forcingPredict = false;
              if (btn) { btn.textContent = "Force Predict"; btn.style.opacity = "1"; btn.style.cursor = "pointer"; }
              alert("Force predict failed: "+e);
            });
          }} id="force-predict-btn" style={{
            background:C.amberBg, color:C.amber, border:"1px solid "+C.amberBorder,
            padding:"3px 10px", borderRadius:4, cursor:"pointer",
            fontSize:9, fontFamily:"inherit", fontWeight:700, letterSpacing:0.5 }}>
            Force Predict
          </button>
          <div style={{ width:7, height:7, borderRadius:"50%", background:connected?C.green:C.amber,
            boxShadow:connected?`0 0 5px ${C.green}66`:"none" }} />
          <span style={{ fontSize:9, color:C.muted }}>{connected?"live":"reconnecting"}</span>
        </div>
      </div>

      {/* ── BODY ── */}
      <div style={{ flex:1, overflow:"hidden", padding:"6px 10px", background:C.bg }}>

        {/* ══ DASHBOARD ══ */}
        {tab==="live" && (
          <div style={{ display:"flex", gap:6, height:"100%" }}>

            {/* LEFT: Chart (full height) */}
            <div style={{ flex:"0 0 46%", minWidth:0, display:"flex", flexDirection:"column" }}>
              <div style={{ ...card, flex:1, display:"flex", flexDirection:"column", minHeight:0 }}>
                <div style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1.5,
                  textTransform:"uppercase", marginBottom:4, flexShrink:0 }}>
                  BTC/USD · 1m · Binance via TradingView
                </div>
                <div style={{ flex:1, minHeight:0 }}><PriceChart /></div>
              </div>
            </div>

            {/* RIGHT: Predictions + DeepSeek + EV + Strategies */}
            <div style={{ flex:"0 0 54%", minWidth:0, display:"flex", flexDirection:"column", gap:5, overflowY:"auto", zoom:0.87 }}>

              {/* ① PREDICTION BAR — 4 columns, uniform layout */}
              {(() => {
                // Shared accuracy footer — identical across all 4 columns
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
                // Shared signal row — identical across all 4 columns
                function SignalRow({ sig, conf, confStr }) {
                  const up      = sig === "UP";
                  const neutral = sig === "NEUTRAL";
                  const clr     = neutral ? C.amber : up ? C.green : C.red;
                  // Neutral = no trade, 0 position — show 0% as placeholder
                  const pct     = neutral ? "0%" : (confStr ?? (conf != null ? conf.toFixed(1)+"%" : null));
                  const barW    = neutral ? 0 : (conf ?? (up ? 65 : 35));
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
                const colStyle = (border) => ({ ...(border==="right"?{borderRight:`1px solid ${C.borderSoft}`,paddingRight:12}:{borderLeft:`1px solid ${C.borderSoft}`,paddingLeft:12}) });
                const colTitle = { fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1.5, textTransform:"uppercase", marginBottom:6 };

                // Best indicator signal resolver (defined once, shared)
                const getBestSig = (name) => {
                  if (!name) return null;
                  if (name.startsWith("strat:")) return strategies[name.slice(6)]?.signal;
                  if (name.startsWith("dash:")) {
                    const key = name.slice(5);
                    const d = backendSnap?.snapshot?.dashboard_signals?.[key];
                    const raw = (microSignalKey(key, d) || d?.signal || "").toUpperCase();
                    return raw.includes("BULLISH") ? "UP" : raw.includes("BEARISH") ? "DOWN" : (raw==="UP"||raw==="DOWN") ? raw : null;
                  }
                  if (name === "deepseek") return activeDeepseekPred?.signal;
                  if (name === "ensemble") return ensemblePred?.signal || ensemble?.signal;
                  return null;
                };

                // ── Pre-compute all 4 column values before rendering ──────────────
                // Col 1 — Ensemble
                const c1sig  = effectiveEnsSig;
                const c1conf = c1sig==="NEUTRAL" ? 0 : ensemblePred?.confidence*100;
                const c1meta = ensemblePred ? { votes:`${ensemblePred.bullish}↑ ${ensemblePred.bearish}↓`, rawConf: effectiveEnsSig==="NEUTRAL"&&ensemblePred.signal!=="NEUTRAL" ? (ensemblePred.confidence*100).toFixed(1) : null } : null;

                // Col 2 — DeepSeek
                const dsLive = pendingDeepseekReady && activeDeepseekPred && activeDeepseekPred.signal!=="ERROR";
                const dsErr  = pendingDeepseekReady && activeDeepseekPred?.signal==="ERROR";
                const dsPrev = !dsLive && !dsErr && deepseekPred && deepseekPred.signal!=="ERROR";
                const c2src  = dsLive ? activeDeepseekPred : dsPrev ? deepseekPred : null;
                const c2sig  = c2src?.signal || null;
                const c2conf = c2src?.confidence ?? 0;
                const c2meta = c2src ? { label:`#${c2src.window_count} · ${c2src.latency_ms}ms`, prev:dsPrev, aiReq: c2src.data_requests&&c2src.data_requests.toUpperCase()!=="NONE"&&c2src.data_requests.trim()!=="" } : null;

                // Col 3 — Consensus
                const c3dsSig   = c2sig;
                const c3ensSig  = effectiveEnsSig;
                const c3agreed  = aiAgree!==null ? (aiAgree && c3ensSig!=="NEUTRAL") : (dsPrev && deepseekPred?.signal===c3ensSig && c3ensSig!=="NEUTRAL");
                const c3sig     = c3agreed ? c3ensSig : "NEUTRAL";
                const c3conf    = c3agreed ? c1conf : 0;
                const c3hasData = aiAgree!==null || (dsPrev && ensemblePred);
                const c3isPrev  = aiAgree===null && dsPrev;

                // Col 4 — Best Indicator
                let c4sig = null, c4conf = 50, c4display = "", c4split = false, c4acc = 0, c4ref = null, c4ready = false;
                if (bestIndicator) {
                  const ranked    = bestIndicator.ranked || [];
                  const qualified = ranked.filter(r => (r.directional ?? r.total) > 0);
                  if (qualified.length) {
                    const top = qualified[0];
                    c4sig     = getBestSig(top.name) || null;
                    c4conf    = 50;
                    c4display = top.name.replace(/^strat:|^spec:|^dash:/,"").replace(/_/g," ").replace(/\b\w/g,c=>c.toUpperCase());
                    c4acc     = (top.accuracy ?? 0) * 100;
                    c4ref     = top;
                    c4ready   = true;
                  }
                }

                // ── Shared cell style helpers ─────────────────────────────────────
                const cR  = { borderRight:`1px solid ${C.borderSoft}`, paddingRight:12 };
                const cM  = { borderRight:`1px solid ${C.borderSoft}`, padding:"0 12px" };
                const cL  = { paddingLeft:12 };
                // Each "row" in the flat grid: title / signal / meta / accuracy
                // meta row has a fixed minHeight so all columns stay level
                const metaRow = { height:26, display:"flex", alignItems:"center", gap:6, flexWrap:"nowrap", overflow:"hidden" };
                const badge = (ok) => ({ fontSize:9, fontWeight:800, padding:"1px 5px", borderRadius:3,
                  color:ok?C.green:C.amber, background:ok?C.greenBg:C.amberBg,
                  border:`1px solid ${ok?C.greenBorder:C.amberBorder}` });

                return (
                  <div style={{ ...card, flexShrink:0, padding:"8px 12px" }}>
                    <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr 1fr", gridTemplateRows:"auto auto auto auto", gap:0 }}>

                      {/* ══ ROW 1 — Titles ══ */}
                      <div style={{ ...cR, paddingBottom:4 }}><div style={colTitle}>Combined Indicators</div></div>
                      <div style={{ ...cM, paddingBottom:4 }}><div style={colTitle}>DeepSeek AI Analysis</div></div>
                      <div style={{ ...cM, paddingBottom:4 }}><div style={colTitle}>Consensus</div></div>
                      <div style={{ ...cL, paddingBottom:4 }}><div style={colTitle}>Best Indicator</div></div>

                      {/* ══ ROW 2 — Signals ══ */}
                      <div style={cR}>
                        {ensemblePred ? <SignalRow sig={c1sig} conf={c1conf} /> : <div style={{ fontSize:11, color:C.muted }}>Warming up…</div>}
                      </div>
                      <div style={cM}>
                        {dsErr ? <div style={{ fontSize:11, color:C.red }}>{activeDeepseekPred.reasoning||"API error"}</div>
                          : c2sig ? <SignalRow sig={c2sig} conf={c2conf} />
                          : <div style={{ fontSize:11, color:C.muted }}>Analyzing…</div>}
                      </div>
                      <div style={cM}>
                        {c3hasData ? <SignalRow sig={c3sig} conf={c3conf} /> : <div style={{ fontSize:11, color:C.muted }}>Waiting…</div>}
                      </div>
                      <div style={cL}>
                        {c4ready ? <SignalRow sig={c4sig||"NEUTRAL"} conf={c4sig&&!c4split?c4conf:0} confStr={c4split?"SPLIT":undefined} />
                          : <div style={{ fontSize:10, color:C.muted }}>No historical data{bestIndicator && bestIndicator.pattern_record_count >= 0 ? <span style={{marginLeft:4,opacity:0.6}}>[{bestIndicator.pattern_record_count} rec]</span> : null}</div>}
                      </div>

                      {/* ══ ROW 3 — Metadata (fixed minHeight keeps all cols level) ══ */}
                      <div style={{ ...cR, ...metaRow }}>
                        {c1meta && (<>
                          <span style={{ fontSize:9, color:C.muted }}>{c1meta.votes}</span>
                          {c1meta.rawConf && <span style={{ fontSize:9, fontWeight:700, color:C.amber }}>({c1meta.rawConf}% raw)</span>}
                          {aiAgree!==null && <span style={badge(aiAgree)}>{aiAgree?"✓ agree":"⚠ split"}</span>}
                        </>)}
                      </div>
                      <div style={{ ...cM, ...metaRow }}>
                        {c2meta && (<>
                          <span style={{ fontSize:9, color:C.muted }}>{c2meta.label}</span>
                          {c2meta.prev && <span style={{ fontSize:9, color:C.muted, fontStyle:"italic" }}>prev bar</span>}
                          {c2meta.aiReq && <span style={{ fontSize:9, fontWeight:700, padding:"1px 5px", borderRadius:3, color:C.amber, background:C.amberBg, border:`1px solid ${C.amberBorder}` }}>⚡ AI req</span>}
                        </>)}
                      </div>
                      <div style={{ ...cM, ...metaRow }}>
                        {c3hasData && (<>
                          {!c3agreed && c3dsSig && (<>
                            <span style={{ fontSize:10, fontWeight:900, color:c3ensSig==="UP"?C.green:c3ensSig==="NEUTRAL"?C.amber:C.red }}>{c3ensSig==="UP"?"▲":c3ensSig==="NEUTRAL"?"—":"▼"} Ind</span>
                            <span style={{ fontSize:10, fontWeight:900, color:c3dsSig==="UP"?C.green:c3dsSig==="NEUTRAL"?C.amber:C.red }}>{c3dsSig==="UP"?"▲":c3dsSig==="NEUTRAL"?"—":"▼"} AI</span>
                          </>)}
                          <span style={badge(c3agreed)}>{c3agreed?"✓ agree":"⚠ split"}</span>
                          {c3isPrev && <span style={{ fontSize:9, color:C.muted, fontStyle:"italic" }}>prev bar</span>}
                        </>)}
                      </div>
                      <div style={{ ...cL, ...metaRow }}>
                        {c4ready && (<>
                          <span style={{ fontSize:9, color:C.indigo, fontWeight:700 }}>{c4display}</span>
                          {c4split && <span style={badge(false)}>⚠ split</span>}
                        </>)}
                      </div>

                      {/* ══ ROW 4 — Accuracy ══ */}
                      <div style={cR}>
                        <AccuracyRow lbl="All-time accuracy" pct={allTimeAccuracy} wins={allTimeCorrect}
                          losses={allTimeTotal-allTimeCorrect} total={allTimeTotal+allTimeNeutral} noData={allTimeTotal===0} />
                      </div>
                      <div style={cM}>
                        <AccuracyRow lbl="DeepSeek accuracy" pct={deepseekAcc?.accuracy*100??0} wins={deepseekAcc?.correct??0}
                          losses={(deepseekAcc?.directional??deepseekAcc?.total??0)-(deepseekAcc?.correct??0)}
                          total={(deepseekAcc?.directional??0)+(deepseekAcc?.neutrals??0)} noData={!deepseekAcc?.total} />
                      </div>
                      <div style={cM}>
                        <AccuracyRow lbl="When both agreed" pct={agreeAcc?.accuracy_agree*100??0} wins={agreeAcc?.correct_agree??0}
                          losses={(agreeAcc?.total_agree??0)-(agreeAcc?.correct_agree??0)} noData={!agreeAcc?.total_agree} />
                      </div>
                      <div style={cL}>
                        {c4ready && c4ref
                          ? <AccuracyRow lbl="Accuracy (all-time)" pct={c4acc} wins={c4ref.wins??c4ref.correct??0}
                              losses={(c4ref.total??0)-(c4ref.wins??c4ref.correct??0)} total={c4ref.total} noData={false} />
                          : <div />}
                      </div>

                    </div>
                  </div>
                );
              })()}

              {/* ② DEEPSEEK ANALYSIS */}
              <div style={{ ...card, flexShrink:0 }}>
                {/* Header row: label + timing info */}
                <div style={{ display:"flex", alignItems:"flex-start", justifyContent:"space-between", marginBottom:10, flexWrap:"wrap", gap:6 }}>
                  <span style={label}>DeepSeek Analysis</span>
                  <div style={{ display:"flex", flexWrap:"wrap", alignItems:"center", gap:10, justifyContent:"flex-end" }}>
                    {pendingDeepseekReady && (
                      <span style={{ fontSize:12, fontWeight:700, color:"#15803D",
                        background:"#F0FDF4", border:`1px solid #86EFAC`,
                        borderRadius:5, padding:"2px 8px" }}>
                        ● LIVE — current bar
                      </span>
                    )}
                    {pendingDeepseekReady && activeDeepseekPred && activeDeepseekPred.signal!=="ERROR" && (<>
                      {activeDeepseekPred.latency_ms && (
                        <span style={{ fontSize:12, color:C.muted }}>
                          Calculated in <strong style={{ color:C.textSec }}>{(activeDeepseekPred.latency_ms/1000).toFixed(1)}s</strong>
                        </span>
                      )}
                    </>)}
                    {barCloseUTC && (
                      <span style={{ fontSize:14, fontWeight:900,
                        color:timeLeft<60?C.red:timeLeft<120?C.amber:"#15803D",
                        background:timeLeft<60?"#FFF1F2":timeLeft<120?C.amberBg:"#F0FDF4",
                        border:`1px solid ${timeLeft<60?C.redBorder:timeLeft<120?C.amberBorder:"#86EFAC"}`,
                        borderRadius:5, padding:"2px 10px" }}>
                        Closes: {barCloseUTC}
                      </span>
                    )}
                  </div>
                </div>
                {/* Status banner */}
                {pendingDeepseekReady ? (
                  <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:12,
                    padding:"8px 12px", borderRadius:6,
                    background:"#F0FDF4", border:"2px solid #86EFAC" }}>
                    <span style={{ fontSize:16, lineHeight:1, flexShrink:0 }}>●</span>
                    <span style={{ fontSize:13, fontWeight:900, color:"#15803D", letterSpacing:0.3, lineHeight:1.4 }}>
                      Live analysis — current bar · closes {barCloseUTC}
                    </span>
                  </div>
                ) : (
                  <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:12,
                    padding:"8px 12px", borderRadius:6,
                    background:C.amberBg, border:`1px solid ${C.amberBorder}` }}>
                    <span style={{ fontSize:16, lineHeight:1, flexShrink:0 }}>⟳</span>
                    <span style={{ fontSize:13, fontWeight:700, color:C.amber, lineHeight:1.4 }}>
                      Analyzing new bar — result appears when DeepSeek completes
                    </span>
                  </div>
                )}
                {/* Analysis content — only shown when a live (pending) result is ready */}
                {pendingDeepseekReady && activeDeepseekPred && activeDeepseekPred.signal!=="ERROR" ? (<>
                  {(activeDeepseekPred.data_received||(activeDeepseekPred.data_requests&&activeDeepseekPred.data_requests.toUpperCase()!=="NONE")) && (
                    <div style={{ display:"flex", gap:6, flexWrap:"wrap", marginBottom:8 }}>
                      {activeDeepseekPred.data_received && (
                        <div style={{ flex:1, minWidth:150, background:C.blueBg, border:`1px solid ${C.blueBorder}`,
                          borderRadius:4, padding:"3px 7px" }}>
                          <div style={{ fontSize:8, fontWeight:700, color:C.blue, letterSpacing:1, textTransform:"uppercase", marginBottom:2 }}>Data confirmed</div>
                          <div style={{ fontSize:10, color:"#1E40AF" }}>{activeDeepseekPred.data_received}</div>
                        </div>
                      )}
                      {activeDeepseekPred.data_requests&&activeDeepseekPred.data_requests.toUpperCase()!=="NONE" && (
                        <div style={{ flex:1, minWidth:150, background:C.amberBg, border:`1px solid ${C.amberBorder}`,
                          borderRadius:4, padding:"3px 7px" }}>
                          <div style={{ fontSize:8, fontWeight:700, color:C.amber, letterSpacing:1, textTransform:"uppercase", marginBottom:2 }}>AI requested</div>
                          <div style={{ fontSize:10, color:C.amber }}>{activeDeepseekPred.data_requests}</div>
                        </div>
                      )}
                    </div>
                  )}
                  {activeDeepseekPred.reasoning
                    ? activeDeepseekPred.reasoning.split("\n").filter(Boolean).map((line,i,arr)=>(
                        <div key={i} style={{ display:"flex", gap:10, marginBottom:i<arr.length-1?12:0, alignItems:"flex-start",
                          padding:"8px 10px", borderRadius:6,
                          background:i%2===0?C.surface:"#FAFAF9",
                          border:`1px solid ${C.borderSoft}` }}>
                          <span style={{ fontSize:13, fontWeight:900, color:C.amber, minWidth:20, paddingTop:1, flexShrink:0, lineHeight:1.6 }}>{i+1}.</span>
                          <span style={{ fontSize:13, color:C.textSec, lineHeight:1.75 }}>
                            <BoldAnalysis text={line} color={C.text} />
                          </span>
                        </div>
                      ))
                    : <div style={{ color:C.amber, fontSize:12 }}>No reasoning — check DeepSeek tab</div>
                  }
                  {activeDeepseekPred.narrative && (
                    <div style={{ background:C.blueBg, border:`1px solid ${C.blueBorder}`,
                      borderLeft:`3px solid ${C.blue}`, borderRadius:6, padding:"8px 12px", marginTop:12 }}>
                      <div style={{ fontSize:9, fontWeight:700, color:C.blue, textTransform:"uppercase",
                        letterSpacing:1, marginBottom:4 }}>Price Narrative</div>
                      <div style={{ fontSize:13, color:"#1E40AF", lineHeight:1.75, fontStyle:"italic" }}>
                        {activeDeepseekPred.narrative}
                      </div>
                    </div>
                  )}
                  {activeDeepseekPred.free_observation && (
                    <div style={{ background:C.amberBg, border:`1px solid ${C.amberBorder}`,
                      borderLeft:`3px solid ${C.amber}`, borderRadius:6, padding:"8px 12px", marginTop:8 }}>
                      <div style={{ fontSize:9, fontWeight:700, color:C.amber, textTransform:"uppercase",
                        letterSpacing:1, marginBottom:4 }}>AI Free Observation</div>
                      <div style={{ fontSize:13, color:C.amber, lineHeight:1.75 }}>
                        {activeDeepseekPred.free_observation}
                      </div>
                    </div>
                  )}
                </>) : pendingDeepseekReady && activeDeepseekPred?.signal==="ERROR" ? (
                  <div style={{ color:C.red, fontSize:11, textAlign:"center", padding:"4px 0" }}>
                    {activeDeepseekPred.reasoning || "API error"}
                  </div>
                ) : (
                  <div style={{ textAlign:"center", padding:"20px 0 12px", color:C.muted }}>
                    <div style={{ fontSize:11 }}>Previous bar resolved · results in History tab</div>
                    <div style={{ fontSize:11, marginTop:4, color:C.muted }}>New prediction will appear here once DeepSeek finishes</div>
                  </div>
                )}
              </div>

              {/* HISTORICAL LEAN — 1-line summary only */}
              {historicalAnalysis && (() => {
               // Strip markdown bold markers before matching
                const cleanText = historicalAnalysis.replace(/\*\*/g, '');
                const pos  = cleanText.match(/POSITION:\s*(\w+)/i)?.[1]?.toUpperCase();
                const conf = cleanText.match(/CONFIDENCE:\s*([\d]+)%/i)?.[1];
                const lean = cleanText.match(/LEAN:\s*(.+)/i)?.[1]?.trim();
                if (!lean) return null;
                const posColor  = pos==="UP" ? C.green : pos==="DOWN" ? C.red : C.amber;
                const posBg     = pos==="UP" ? C.greenBg : pos==="DOWN" ? C.redBg : C.amberBg;
                const posBorder = pos==="UP" ? C.greenBorder : pos==="DOWN" ? C.redBorder : C.amberBorder;
                return (
                  <div style={{ ...card, flexShrink:0, borderLeft:`3px solid ${posColor}`, padding:"8px 12px",
                    display:"flex", alignItems:"center", gap:10 }}>
                    <span style={{ fontSize:9, fontWeight:700, color:C.muted, letterSpacing:1, textTransform:"uppercase", flexShrink:0 }}>Pattern</span>
                    {pos && (
                      <span style={{ fontSize:9, fontWeight:800, padding:"1px 6px", borderRadius:3,
                        color:posColor, background:posBg, border:`1px solid ${posBorder}`, flexShrink:0 }}>
                        {pos==="UP"?"▲ UP":pos==="DOWN"?"▼ DOWN":"—"}{conf?` ${conf}%`:""}
                      </span>
                    )}
                    <span style={{ fontSize:11, color:C.textSec, lineHeight:1.5 }}>
                      <BoldAnalysis text={lean} color={C.text} />
                    </span>
                  </div>
                );
              })()}

              {/* ③+④ COMBINED DETAIL DROPDOWN */}
              <div style={{ ...card, flexShrink:0 }}>
                <button onClick={()=>setIndicatorsOpen(o=>!o)} style={{
                  display:"flex", justifyContent:"space-between", alignItems:"center",
                  width:"100%", background:"none", border:"none", cursor:"pointer", padding:0, color:"inherit", fontFamily:"inherit" }}>
                  <span style={{ fontSize:10, fontWeight:800, color:C.muted, letterSpacing:1.5, textTransform:"uppercase" }}>
                    {indicatorsOpen ? "▲  Collapse detailed information" : "▼  Click to expand for detailed information"}
                  </span>
                  <span style={{ fontSize:11, fontWeight:700, color:C.textSec }}>
                    Microstructure · {strats.length}/{STRATEGY_META.length} strategies active
                  </span>
                </button>
                {indicatorsOpen && <>
                  {/* Market Microstructure */}
                  <div style={{ marginTop:10, paddingTop:10, borderTop:`1px solid ${C.borderSoft}` }}>
                    <div style={{ ...label, marginBottom:6 }}>Market Microstructure · live</div>
                    <MicroSummary ob={ob} tk={tk} ls={ls} lq={lq} oif={oif} cz={cz} ca={ca} fg={fg} mp={mp} cg={cg}
                      dots={dots} caDivPct={caDivPct} caSig={caSig}
                      collapsed={false} onToggle={null} noCard={true} />
                  </div>
                  {/* Strategy Indicators */}
                  <div style={{ marginTop:10, paddingTop:10, borderTop:`1px solid ${C.borderSoft}` }}>
                    <div style={{ ...label, marginBottom:8 }}>Strategy Indicators</div>
                    <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:8 }}>
                  {STRATEGY_META.map(m=>{
                    const s=strategies[m.key];
                    const obos=s?.value?getStratOBOS(m.key,s.value):null;
                    if (m.key==="ema_cross") {
                      const fastSig=s?.signal, tlp=s?.htf_signal, slowSig=tlp&&tlp!=="N/A"?tlp:null;
                      const emaAgree=fastSig&&slowSig?fastSig===slowSig:null;
                      const descFast=STRATEGY_DESC["ema_cross"];
                      const descSlow=STRATEGY_DESC["ema_slow"];
                      return [
                        <div key="ema_fast" style={{ padding:"12px 10px", borderRadius:8, textAlign:"center",
                          background:fastSig==="UP"?C.greenBg:fastSig==="DOWN"?C.redBg:C.bg,
                          border:`1px solid ${fastSig==="UP"?C.greenBorder:fastSig==="DOWN"?C.redBorder:C.borderSoft}`,
                          opacity:!s?0.55:1, display:"flex", flexDirection:"column", gap:2 }}>
                          <div style={{ fontSize:13, color:m.color, fontWeight:800 }}>EMA Fast</div>
                          <div style={{ fontSize:10, color:C.muted, fontWeight:600 }}>5 / 13</div>
                          <div style={{ fontSize:28, fontWeight:900, color:fastSig==="UP"?C.green:fastSig==="DOWN"?C.red:C.muted, lineHeight:1, marginTop:2 }}>{fastSig==="UP"?"▲":fastSig==="DOWN"?"▼":"—"}</div>
                          <div style={{ fontSize:16, color:C.textSec, fontWeight:800 }}>{s?.confidence!=null?(s.confidence*100).toFixed(0)+"%":""}</div>
                          {emaAgree!=null&&<div style={{ fontSize:10,fontWeight:800,color:emaAgree?"#15803d":"#b45309",marginTop:1}}>{emaAgree?"✓ HTF aligned":"⚠ HTF split"}</div>}
                          {s?.reasoning&&<div title={s.reasoning} style={{ fontSize:9, color:C.muted, marginTop:4,
                            lineHeight:1.4, textAlign:"left", fontStyle:"italic", padding:"4px 6px 3px",
                            borderTop:`1px solid ${fastSig==="UP"?C.greenBorder:fastSig==="DOWN"?C.redBorder:C.borderSoft}`,
                            overflow:"hidden", display:"-webkit-box", WebkitLineClamp:2, WebkitBoxOrient:"vertical" }}>
                            {s.reasoning}
                          </div>}
                          {descFast&&<div style={{ marginTop:5, paddingTop:5, borderTop:`1px dashed ${C.borderSoft}`, textAlign:"left" }}>
                            <div style={{ fontSize:9, color:m.color, fontWeight:700, marginBottom:2 }}>{descFast.short}</div>
                            <div style={{ fontSize:9, color:C.textSec, lineHeight:1.45 }}>{descFast.how}</div>
                          </div>}
                        </div>,
                        <div key="ema_slow" style={{ padding:"12px 10px", borderRadius:8, textAlign:"center",
                          background:slowSig==="UP"?C.greenBg:slowSig==="DOWN"?C.redBg:C.bg,
                          border:`1px solid ${slowSig==="UP"?C.greenBorder:slowSig==="DOWN"?C.redBorder:C.borderSoft}`,
                          opacity:!s||!slowSig?0.55:1, display:"flex", flexDirection:"column", gap:2 }}>
                          <div style={{ fontSize:13, color:"#1D4ED8", fontWeight:800 }}>EMA Slow</div>
                          <div style={{ fontSize:10, color:C.muted, fontWeight:600 }}>21 / 55</div>
                          <div style={{ fontSize:28, fontWeight:900, color:slowSig==="UP"?C.green:slowSig==="DOWN"?C.red:C.muted, lineHeight:1, marginTop:2 }}>{slowSig==="UP"?"▲":slowSig==="DOWN"?"▼":"—"}</div>
                          <div style={{ fontSize:16, color:C.textSec, fontWeight:800 }}>{s?.slow_confidence!=null?((s.slow_confidence??s?.confidence??0)*100).toFixed(0)+"%":""}</div>
                          {emaAgree!=null&&<div style={{ fontSize:10,fontWeight:800,color:emaAgree?"#15803d":"#b45309",marginTop:1}}>{emaAgree?"✓ aligned":"⚠ split"}</div>}
                          {descSlow&&<div style={{ marginTop:5, paddingTop:5, borderTop:`1px dashed ${C.borderSoft}`, textAlign:"left" }}>
                            <div style={{ fontSize:9, color:"#1D4ED8", fontWeight:700, marginBottom:2 }}>{descSlow.short}</div>
                            <div style={{ fontSize:9, color:C.textSec, lineHeight:1.45 }}>{descSlow.how}</div>
                          </div>}
                        </div>
                      ];
                    }
                    const desc = STRATEGY_DESC[m.key];
                    if (!s) return (
                      <div key={m.key} style={{ padding:"12px 10px", borderRadius:8, background:C.bg,
                        border:`1px solid ${C.borderSoft}`, display:"flex", flexDirection:"column", gap:2, opacity:0.55 }}>
                        <div style={{ fontSize:13, color:C.muted, fontWeight:800, textAlign:"center" }}>{m.name}</div>
                        <div style={{ fontSize:10, color:C.muted, marginTop:4, textAlign:"center" }}>waiting for data…</div>
                        {desc&&<div style={{ marginTop:"auto", paddingTop:6, borderTop:`1px dashed ${C.borderSoft}` }}>
                          <div style={{ fontSize:9, color:m.color, fontWeight:700, marginBottom:2 }}>{desc.short}</div>
                          <div style={{ fontSize:9, color:C.textSec, lineHeight:1.45 }}>{desc.how}</div>
                        </div>}
                      </div>
                    );
                    return (
                      <div key={m.key} style={{ padding:"12px 10px", borderRadius:8,
                        background:s.signal==="UP"?C.greenBg:C.redBg,
                        border:`1px solid ${s.signal==="UP"?C.greenBorder:C.redBorder}`,
                        display:"flex", flexDirection:"column", gap:2 }}>
                        {/* Header: name */}
                        <div style={{ fontSize:13, color:m.color, fontWeight:800, textAlign:"center" }}>{m.name}</div>
                        {/* Big arrow */}
                        <div style={{ fontSize:30, fontWeight:900, color:s.signal==="UP"?C.green:s.signal==="NEUTRAL"?C.amber:C.red, lineHeight:1, textAlign:"center", marginTop:2 }}>
                          {s.signal==="UP"?"▲":s.signal==="NEUTRAL"?"—":"▼"}
                        </div>
                        {/* Confidence */}
                        <div style={{ fontSize:17, color:C.textSec, textAlign:"center", fontWeight:800 }}>{(s.confidence*100).toFixed(0)}%</div>
                        {/* Raw value + OBOS */}
                        {s.value&&s.value!=="N/A"&&<div style={{ fontSize:12, color:C.text, textAlign:"center", fontWeight:700, marginTop:1 }}>{s.value}</div>}
                        {obos&&<div style={{ fontSize:10, fontWeight:800, color:obos.color, background:obos.bg,
                          border:`2px solid ${obos.border}`, borderRadius:5, padding:"2px 6px", marginTop:2, textAlign:"center", alignSelf:"center" }}>{obos.label}</div>}
                        {/* Live reasoning */}
                        {s.reasoning&&<div title={s.reasoning} style={{ fontSize:9, color:C.muted, marginTop:4,
                          lineHeight:1.4, fontStyle:"italic", padding:"4px 6px 3px",
                          borderTop:`1px solid ${s.signal==="UP"?C.greenBorder:C.redBorder}`,
                          overflow:"hidden", display:"-webkit-box", WebkitLineClamp:3, WebkitBoxOrient:"vertical" }}>
                          {s.reasoning}
                        </div>}
                        {/* Static description */}
                        {desc&&<div style={{ marginTop:5, paddingTop:5, borderTop:`1px dashed ${s.signal==="UP"?C.greenBorder:C.redBorder}` }}>
                          <div style={{ fontSize:9, color:m.color, fontWeight:700, marginBottom:2 }}>{desc.short}</div>
                          <div style={{ fontSize:9, color:C.textSec, lineHeight:1.45 }}>{desc.how}</div>
                        </div>}
                      </div>
                    );
                  })}
                    </div>
                  </div>
                </>}
              </div>
            </div>
          </div>
        )}

        {/* ══ HISTORY TAB ══ */}
        {tab==="history" && (
          <ErrorBoundary key="history-tab">
          <div style={{ height:"100%", overflowY:"auto", display:"flex", flexDirection:"column", gap:8, paddingBottom:8 }}>

            {/* Accuracy summary row */}
            <div style={{ ...card, flexShrink:0, padding:"6px 0" }}>
              {/* Top row: 5 core metrics */}
              <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr 1fr 1fr", gap:0 }}>
                {[
                  ["Math Ensemble",
                    allTimeTotal>0?`${allTimeAccuracy.toFixed(1)}%`:"—",
                    allTimeTotal>0?`${allTimeCorrect}W · ${allTimeTotal-allTimeCorrect}L · ${allTimeNeutral}N`:"no data",
                    allTimeTotal>0?(allTimeAccuracy>=50?C.green:C.red):C.muted],
                  ["DeepSeek AI",
                    deepseekAcc?.total>0?`${(deepseekAcc.accuracy*100).toFixed(1)}%`:"—",
                    deepseekAcc?.total>0?`${deepseekAcc.correct}W · ${deepseekAcc.total-deepseekAcc.correct}L · ${deepseekAcc.neutrals??0}N`:"no data",
                    deepseekAcc?.total>0?(deepseekAcc.accuracy>=0.5?C.green:C.red):C.muted],
                  ["Agree Only",
                    agreeAcc?.total_agree>0?`${(agreeAcc.accuracy_agree*100).toFixed(1)}%`:"—",
                    agreeAcc?.total_agree>0?`${agreeAcc.correct_agree}W · ${agreeAcc.total_agree-agreeAcc.correct_agree}L`:"needs both to agree",
                    agreeAcc?.total_agree>0?(agreeAcc.accuracy_agree>=0.5?C.green:C.red):C.muted],
                  ["Agree Rate",
                    agreeAcc?.total_agree>0 && allTimeTotal>0
                      ? `${(agreeAcc.total_agree/Math.max(allTimeTotal,deepseekAcc?.total||1)*100).toFixed(0)}%`
                      : "—",
                    agreeAcc?.total_agree>0
                      ? `${agreeAcc.total_agree} bars both agreed`
                      : "needs both to fire",
                    C.textSec],
                  ["System", connected?"Live":"Offline", `${strats.length}/${STRATEGY_META.length} strategies`, connected?C.green:C.amber],
                ].map(([name,big,sub,col],i,arr)=>(
                  <div key={name} style={{ padding:"4px 12px", borderRight:i<arr.length-1?`1px solid ${C.borderSoft}`:"none" }}>
                    <div style={{ fontSize:9, color:C.muted, letterSpacing:1, textTransform:"uppercase", marginBottom:2 }}>{name}</div>
                    <div style={{ fontSize:18, fontWeight:900, color:col, lineHeight:1 }}>{big}</div>
                    <div style={{ fontSize:9, color:C.textSec, marginTop:2 }}>{sub}</div>
                  </div>
                ))}
              </div>
              {/* Strategy accuracy pills — show when allAccuracy is loaded */}
              {allAccuracy && (() => {
                const cats = [
                  { key:"strategies", label:"Strategies" },
                  { key:"specialists", label:"Specialists" },
                  { key:"microstructure", label:"Micro" },
                  { key:"ai", label:"AI" },
                ];
                const allRows = [];
                cats.forEach(({ key, label: catLabel }) => {
                  (allAccuracy[key] || []).forEach(r => {
                    if (r.total >= 3) allRows.push({ ...r, cat: catLabel });
                  });
                });
                if (!allRows.length) return null;
                allRows.sort((a,b) => b.accuracy - a.accuracy);
                return (
                  <div style={{ borderTop:`1px solid ${C.borderSoft}`, marginTop:6, paddingTop:5, paddingLeft:12, paddingRight:12 }}>
                    <div style={{ fontSize:8, color:C.muted, fontWeight:700, letterSpacing:1.2, textTransform:"uppercase", marginBottom:4 }}>
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

            {/* Full-width bar history list */}
            <div style={{ flex:1, minHeight:0 }}>
              <DeepSeekAuditTab
                deepseekLog={deepseekLog} deepseekAcc={deepseekAcc}
                deepseekPred={deepseekPred} ensembleAccuracy={allTimeAccuracy}
                totalPreds={allTimeTotal} correctPreds={allTimeCorrect} agreeAcc={agreeAcc}
              />
            </div>

          </div>
          </ErrorBoundary>
        )}

        {/* ══ AUDIT TAB ══ */}
        {tab==="audit" && (
          <HistoricalAnalysisAuditTab deepseekLog={deepseekLog} />
        )}

        {/* ══ SOURCES TAB ══ */}
        {tab==="sources" && (
          <SourceHistoryTab
            sourceHistory={sourceHistory}
            selectedSource={selectedSource}
            setSelectedSource={setSelectedSource}
          />
        )}

        {tab==="embed_audit" && (
          <EmbeddingAuditTab embeddingAuditLog={embeddingAuditLog} />
        )}

        {/* ══ OLD TABS (kept for reference, hidden) ══ */}
        {tab==="microstructure_old" && (
          <div style={{ height:"100%", overflowY:"auto", display:"flex", flexDirection:"column", gap:8, paddingBottom:8 }}>

            {/* Header bar */}
            <div style={{ ...card, flexShrink:0, padding:"8px 12px", display:"flex", gap:16, alignItems:"center", flexWrap:"wrap" }}>
              <div>
                <div style={label}>Live Market Microstructure</div>
                <div style={{ fontSize:10, color:C.muted, marginTop:1 }}>
                  All signals refresh live. DeepSeek receives a fresh snapshot at each bar open.
                  Performance is only recorded after the bar closes — no data leakage.
                </div>
              </div>
              <div style={{ marginLeft:"auto", display:"flex", gap:10, flexWrap:"wrap" }}>
                {[["ob","Order Book"],["tk","Taker Flow"],["ls","L/S Ratio"],["lq","Liquidations"],
                  ["oif","OI/Fund"],["cz","Coinalyze"],["ca","CoinAPI"],["fg","Fear&Greed"],
                  ["mp","Mempool"],["cg","CoinGecko"]].map(([k,n])=>{
                  const d=dots[k];
                  return <span key={k} style={{ display:"flex", alignItems:"center", gap:4, fontSize:9, color:C.muted }}>
                    <span style={{ width:6, height:6, borderRadius:"50%", display:"inline-block",
                      background:d==="live"?C.green:d==="err"?C.red:C.amber }} />
                    {n}
                  </span>;
                })}
              </div>
            </div>

            {/* Grid of signal cards */}
            <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))", gap:8 }}>

              {/* ORDER BOOK */}
              <MicroCard title="Order Book Imbalance" source="Binance spot · depth 20"
                sig={ob?.sig} dot={dots.ob||"pend"}>
                {ob ? (<>
                  <div style={{ display:"flex", gap:6 }}>
                    {[["Bids",ob.bv.toFixed(1)+" BTC",C.green],["Imbalance",`${ob.imb>0?"+":""}${ob.imb.toFixed(2)}%`,ob.imb>5?C.green:ob.imb<-5?C.red:C.amber],["Asks",ob.av.toFixed(1)+" BTC",C.red]].map(([l,v,c])=>(
                      <div key={l} style={{ flex:1, textAlign:"center", background:C.bg, borderRadius:4, padding:"4px 0" }}>
                        <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase" }}>{l}</div>
                        <div style={{ fontSize:13, fontWeight:800, color:c }}>{v}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{ display:"flex", gap:0, height:5, borderRadius:3, overflow:"hidden", marginTop:2 }}>
                    <div style={{ width:`${ob.bv/(ob.bv+ob.av)*100}%`, background:C.green }} />
                    <div style={{ flex:1, background:C.red }} />
                  </div>
                  <div style={{ fontSize:9, color:C.muted, marginTop:2 }}>
                    {ob.imb>5?"Strong bid wall — passive buyers defending at market price."
                     :ob.imb<-5?"Ask-heavy — sell wall capping immediate upside."
                     :"Balanced book — no strong directional edge from depth alone."}
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

              {/* TAKER FLOW */}
              <MicroCard title="Taker Buy/Sell Flow" source="Binance Futures · 5m aggressor"
                sig={tk?.sig} dot={dots.tk||"pend"}>
                {tk ? (<>
                  <div style={{ display:"flex", gap:6 }}>
                    {[["Buy vol",tk.bv.toFixed(0)+" BTC",C.green],["BSR",tk.bsr.toFixed(4),tk.bsr>1.12?C.green:tk.bsr<0.9?C.red:C.amber],["Sell vol",tk.sv.toFixed(0)+" BTC",C.red]].map(([l,v,c])=>(
                      <div key={l} style={{ flex:1, textAlign:"center", background:C.bg, borderRadius:4, padding:"4px 0" }}>
                        <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase" }}>{l}</div>
                        <div style={{ fontSize:13, fontWeight:800, color:c }}>{v}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{ fontSize:9, color:C.textSec, marginTop:2 }}>
                    3-bar trend: <strong style={{ color:tk.trend==="ACC↑"?C.green:tk.trend==="ACC↓"?C.red:C.amber }}>{tk.trend}</strong>
                  </div>
                  <div style={{ fontSize:9, color:C.muted, marginTop:2 }}>
                    {tk.bsr>1.12?"Aggressive buyers crossing the ask — real conviction, paying premium to go long."
                     :tk.bsr<0.9?"Aggressive sellers hitting the bid — bearish execution pressure confirmed."
                     :"Balanced flow — chop/reversal environment; defer to order book."}
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

              {/* LONG / SHORT */}
              <MicroCard title="Long / Short Ratio" source="Binance Futures · retail + top 20%"
                sig={ls?.rSig} dot={dots.ls||"pend"}>
                {ls ? (<>
                  <div style={{ fontSize:9, color:C.muted, textTransform:"uppercase", fontWeight:700, marginBottom:2 }}>Retail</div>
                  <div style={{ display:"flex", gap:0, height:5, borderRadius:3, overflow:"hidden", marginBottom:2 }}>
                    <div style={{ width:`${ls.retailLong}%`, background:C.green }} />
                    <div style={{ flex:1, background:C.red }} />
                  </div>
                  <div style={{ display:"flex", justifyContent:"space-between", fontSize:9, marginBottom:4 }}>
                    <span style={{ color:C.green, fontWeight:700 }}>L {ls.retailLong.toFixed(1)}%</span>
                    <span style={{ color:C.muted }}>L/S {ls.lsr.toFixed(3)}</span>
                    <span style={{ color:C.red, fontWeight:700 }}>S {ls.retailShort.toFixed(1)}%</span>
                  </div>
                  <div style={{ fontSize:9, color:C.muted, textTransform:"uppercase", fontWeight:700, marginBottom:2 }}>
                    Smart Money (top 20%) · <span style={{ color:sigColors(ls.sSig).color }}>{ls.sSig}</span>
                  </div>
                  <div style={{ display:"flex", gap:0, height:5, borderRadius:3, overflow:"hidden", marginBottom:2 }}>
                    <div style={{ width:`${ls.smartLong}%`, background:C.green }} />
                    <div style={{ flex:1, background:C.red }} />
                  </div>
                  <div style={{ display:"flex", justifyContent:"space-between", fontSize:9 }}>
                    <span style={{ color:C.green, fontWeight:700 }}>L {ls.smartLong.toFixed(1)}%</span>
                    <span style={{ color:Math.abs(ls.div)>10?C.amber:C.muted, fontWeight:700 }}>
                      Δ {ls.div>0?"+":""}{ls.div.toFixed(1)}%
                    </span>
                    <span style={{ color:C.red, fontWeight:700 }}>S {ls.smartShort.toFixed(1)}%</span>
                  </div>
                  <div style={{ fontSize:9, color:C.muted, marginTop:3 }}>
                    {Math.abs(ls.div)>10
                      ? `Smart money ${ls.smartLong>ls.retailLong?"more long":"more short"} than retail by ${Math.abs(ls.div).toFixed(1)}% — follow smart money.`
                      : "Retail and smart money aligned — high-conviction directional bias."}
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

              {/* LIQUIDATIONS */}
              <MicroCard title="Recent Liquidations" source="Binance Futures · last 10 force orders"
                sig={lq?.sig} dot={dots.lq||"pend"}>
                {lq ? (<>
                  {lq.total===0 ? (
                    <div style={{ fontSize:11, color:C.muted }}>No recent force orders — stable market.</div>
                  ) : (<>
                    <div style={{ display:"flex", gap:6 }}>
                      {[["Long liqs",lq.longCount+" ("+fmtK(lq.lvol)+")",C.red],["Short liqs",lq.shortCount+" ("+fmtK(lq.svol)+")",C.green]].map(([l,v,c])=>(
                        <div key={l} style={{ flex:1, textAlign:"center", background:C.bg, borderRadius:4, padding:"4px 4px" }}>
                          <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase" }}>{l}</div>
                          <div style={{ fontSize:12, fontWeight:800, color:c }}>{v}</div>
                        </div>
                      ))}
                    </div>
                    <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>
                      {lq.lvol>lq.svol*1.5?"Long cascade — forced sellers driving price down. Bearish momentum."
                       :lq.svol>lq.lvol*1.5?"Short squeeze — forced buyers creating upside spike risk."
                       :"Mixed — both sides liquidated. No directional cascade."}
                    </div>
                  </>)}
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

              {/* OI + FUNDING */}
              <MicroCard title="Open Interest + Funding" source="Binance Futures perpetual"
                sig={oif?.frSig} dot={dots.oif||"pend"}>
                {oif ? (<>
                  <div style={{ display:"flex", gap:6 }}>
                    {[["Open Int",oif.oi.toLocaleString("en-US",{maximumFractionDigits:0})+" BTC",C.text],
                      ["Funding 8h",`${oif.fr>=0?"+":""}${(oif.fr*100).toFixed(5)}%`,oif.frSig==="BEARISH"?C.red:oif.frSig==="BULLISH"?C.green:C.amber],
                      ["Mk premium",`${oif.premium>=0?"+":""}${oif.premium.toFixed(4)}%`,oif.pSig==="BEARISH"?C.red:oif.pSig==="BULLISH"?C.green:C.amber]].map(([l,v,c])=>(
                      <div key={l} style={{ flex:1, textAlign:"center", background:C.bg, borderRadius:4, padding:"4px 2px" }}>
                        <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase", lineHeight:1.2 }}>{l}</div>
                        <div style={{ fontSize:11, fontWeight:800, color:c }}>{v}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>
                    Next funding: {oif.nextFund} · {oif.fr>0.0006?"High positive — longs at cascade risk.":oif.fr<0?"Negative — shorts paying longs. Upward bias.":"Near-zero — leverage positioning balanced."}
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

              {/* COINALYZE — cross-exchange funding */}
              <MicroCard title="Cross-Exchange Funding" source="Coinalyze · BTCUSDT aggregate perp"
                sig={cz?.sig} dot={dots.cz||"pend"}>
                {cz ? (<>
                  <div style={{ fontSize:22, fontWeight:900,
                    color:cz.sig==="BEARISH"?C.red:cz.sig==="BULLISH"?C.green:C.amber }}>
                    {cz.fr>=0?"+":""}{(cz.fr*100).toFixed(5)}%
                  </div>
                  <div style={{ fontSize:9, color:C.muted }}>8h aggregate funding across all venues</div>
                  {oif && (
                    <div style={{ fontSize:9, color:C.textSec, marginTop:3 }}>
                      vs Binance: {(oif.fr*100).toFixed(5)}% · Δ {((cz.fr-oif.fr)*100).toFixed(6)}%
                    </div>
                  )}
                  <div style={{ fontSize:9, color:C.muted, marginTop:3 }}>
                    {cz.fr>0.0005?"Aggregate longs paying shorts — systemic leverage bias long. Cascade risk elevated."
                     :cz.fr<0?"Aggregate shorts paying longs — market incentivises short covering."
                     :"Cross-exchange funding balanced — no systemic leverage extreme."}
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading via proxy…</div>}
              </MicroCard>

              {/* COINAPI — cross-exchange price */}
              <MicroCard title="Cross-Exchange Price" source="CoinAPI · 350+ exchanges weighted avg"
                sig={caSig} dot={dots.ca||"pend"}>
                {ca ? (<>
                  <div style={{ fontSize:22, fontWeight:900, color:C.text }}>${ca.rate.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}</div>
                  <div style={{ fontSize:9, color:C.muted }}>Weighted average across 350+ exchanges</div>
                  {caDivPct!=null && (<>
                    <div style={{ display:"flex", justifyContent:"space-between", marginTop:4, fontSize:9 }}>
                      <span style={{ color:C.muted }}>Binance: ${(winStartPrice||price||0).toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}</span>
                      <span style={{ color:Math.abs(caDivPct)>0.05?C.amber:C.muted, fontWeight:700 }}>
                        Δ {caDivPct>=0?"+":""}{caDivPct.toFixed(4)}%
                      </span>
                    </div>
                    <div style={{ fontSize:9, color:C.muted, marginTop:3 }}>
                      {Math.abs(caDivPct)>0.05
                        ? caDivPct>0?"Binance at PREMIUM — arbitrage selling into Binance expected."
                                   :"Binance at DISCOUNT — arbitrage buying into Binance expected."
                        : "No significant cross-exchange divergence — arb pressure neutral."}
                    </div>
                  </>)}
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading via proxy…</div>}
              </MicroCard>

              {/* FEAR & GREED */}
              <MicroCard title="Fear &amp; Greed Index" source="Alternative.me · updates daily"
                sig={fg?.sig} dot={dots.fg||"pend"}>
                {fg ? (<>
                  <div style={{ display:"flex", gap:12, alignItems:"center" }}>
                    <div style={{ fontSize:36, fontWeight:900,
                      color:fg.sig==="BULLISH_CONTRARIAN"?C.green:fg.sig==="BEARISH_CONTRARIAN"?C.red:C.amber }}>
                      {fg.value}
                    </div>
                    <div>
                      <div style={{ fontSize:14, fontWeight:700 }}>{fg.label}</div>
                      <div style={{ fontSize:9, color:C.muted }}>prev: {fg.prev} (Δ{fg.delta>=0?"+":""}{fg.delta})</div>
                    </div>
                  </div>
                  <div style={{ height:5, background:C.borderSoft, borderRadius:3, overflow:"hidden", margin:"4px 0" }}>
                    <div style={{ width:`${fg.value}%`, height:"100%", borderRadius:3,
                      background:`linear-gradient(to right, ${C.green}, ${C.amber} 50%, ${C.red})` }} />
                  </div>
                  <div style={{ display:"flex", justifyContent:"space-between", fontSize:8, color:C.muted, marginBottom:3 }}>
                    <span>Extreme Fear (0)</span><span>Neutral (50)</span><span>Extreme Greed (100)</span>
                  </div>
                  <div style={{ fontSize:9, color:C.muted }}>
                    {fg.value<30?"Extreme fear — contrarian LONG signal. Retail capitulation, smart money accumulation."
                     :fg.value>75?"Extreme greed — contrarian SHORT lean. Smart money fades retail euphoria."
                     :"Neutral zone — no extreme contrarian edge. Weight technical signals more heavily."}
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

              {/* MEMPOOL */}
              <MicroCard title="Bitcoin Mempool" source="Mempool.space · on-chain fee pressure"
                sig={mp?.sig} dot={dots.mp||"pend"}>
                {mp ? (<>
                  <div style={{ display:"flex", gap:6 }}>
                    {[["Fastest",mp.fastest+" sat/vB",mp.sig==="BEARISH"?C.red:mp.sig==="BULLISH"?C.green:C.amber],
                      ["30 min",mp.halfHour+" sat/vB",C.text],["1 hour",mp.hour+" sat/vB",C.text]].map(([l,v,c])=>(
                      <div key={l} style={{ flex:1, textAlign:"center", background:C.bg, borderRadius:4, padding:"4px 2px" }}>
                        <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase" }}>{l}</div>
                        <div style={{ fontSize:12, fontWeight:800, color:c }}>{v}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>
                    {mp.count.toLocaleString()} pending · {mp.size.toFixed(2)} MB · {mp.fastest>50?"High urgency — network congested/panic exits. Bearish stress."
                     :mp.fastest<10?"Calm network — minimal on-chain urgency. Background positive."
                     :"Normal conditions — no on-chain stress signal."}
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

              {/* COINGECKO */}
              <MicroCard title="Market Overview" source="CoinGecko · macro context"
                sig={cg?.ch>=0?"BULLISH":"BEARISH"} dot={dots.cg||"pend"}>
                {cg ? (<>
                  <div style={{ display:"flex", gap:6 }}>
                    {[["Mkt cap",fmtK(cg.mcap),C.text],["24h vol",fmtK(cg.vol),C.text],
                      ["24h Δ",`${cg.ch>=0?"+":""}${cg.ch.toFixed(2)}%`,cg.ch>=0?C.green:C.red],
                      ["Vol/MCap",cg.vm.toFixed(2)+"%",cg.vm>5?C.amber:C.text]].map(([l,v,c])=>(
                      <div key={l} style={{ flex:1, textAlign:"center", background:C.bg, borderRadius:4, padding:"4px 2px" }}>
                        <div style={{ fontSize:8, color:C.muted, textTransform:"uppercase", lineHeight:1.2 }}>{l}</div>
                        <div style={{ fontSize:11, fontWeight:800, color:c }}>{v}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{ fontSize:9, color:C.muted, marginTop:4 }}>
                    Macro momentum {cg.ch>=0?"bullish":"bearish"} · vol/mcap {cg.vm.toFixed(2)}% ({cg.vm>5?"elevated activity":"normal activity"}).
                  </div>
                </>) : <div style={{ fontSize:10, color:C.muted }}>Loading…</div>}
              </MicroCard>

            </div>
          </div>
        )}

        {/* ══ DEEPSEEK TAB ══ */}
        {tab==="deepseek" && (
          <DeepSeekAuditTab
            deepseekLog={deepseekLog} deepseekAcc={deepseekAcc}
            deepseekPred={deepseekPred} ensembleAccuracy={allTimeAccuracy}
            totalPreds={allTimeTotal} correctPreds={allTimeCorrect} agreeAcc={agreeAcc}
          />
        )}

        {/* ══ ENSEMBLE TAB ══ */}
        {tab==="ensemble" && (
          <EnsembleTab
            weights={weights} setWeights={setWeights}
            ob={ob} ls={ls} tk={tk} oif={oif} lq={lq}
            fg={fg} mp={mp} ca={ca} cz={cz} cg={cg}
            dots={dots} price={price}
            allAccuracy={allAccuracy}
            allAccuracyErr={allAccuracyErr}
            onRefreshAccuracy={fetchAllAccuracy}
          />
        )}

        {/* ══ BACKEND TAB ══ */}
        {tab==="backend" && (
          <BackendTab backendSnap={backendSnap} deepseekPred={deepseekPred} />
        )}

        {/* ══ STRATEGIES TAB ══ */}
        {tab==="strategies" && (
          <div style={{ display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:6, height:"100%", overflowY:"auto" }}>
            {STRATEGY_META.map(m=>{
              const s=strategies[m.key], w=weights[m.key];
              if (!s) return null;
              return (
                <div key={m.key} style={{ ...card, borderLeft:`3px solid ${m.color}` }}>
                  <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                    <span style={{ fontSize:12, fontWeight:700, color:m.color }}>{m.name}</span>
                    <span style={{ fontSize:13, fontWeight:800, padding:"2px 10px", borderRadius:4,
                      background:s.signal==="UP"?C.greenBg:C.redBg,
                      color:s.signal==="UP"?C.green:C.red,
                      border:`1px solid ${s.signal==="UP"?C.greenBorder:C.redBorder}` }}>{s.signal}</span>
                  </div>
                  <div style={{ fontSize:10, color:C.textSec, marginTop:4, lineHeight:1.5 }}>{s.reasoning||"—"}</div>
                  <div style={{ display:"flex", gap:12, marginTop:5, fontSize:10 }}>
                    <span style={{ color:C.muted }}>Conf: <strong style={{ color:C.text }}>{(s.confidence*100).toFixed(1)}%</strong></span>
                    {s.value&&<span style={{ color:C.muted }}>Val: <strong style={{ color:C.text }}>{s.value}</strong></span>}
                    {w!=null&&<span style={{ color:C.muted }}>Wt: <strong style={{ color:C.amber }}>{w.toFixed(2)}x</strong></span>}
                  </div>
                  <div style={{ height:3, background:C.borderSoft, borderRadius:2, overflow:"hidden", marginTop:6 }}>
                    <div style={{ width:`${s.confidence*100}%`, height:"100%", background:m.color, opacity:0.7 }} />
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* ══ BACKTEST TAB ══ */}
        {tab==="backtest" && (
          <div style={{ ...card, height:"100%", display:"flex", flexDirection:"column" }}>
            <div style={{ ...label, flexShrink:0 }}>Prediction Log</div>
            <div style={{ flex:1, overflow:"auto", marginTop:6 }}>
              {preds.length===0
                ? <div style={{ color:C.muted, fontSize:11, padding:30, textAlign:"center" }}>Resolved windows appear here</div>
                : (
                  <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11 }}>
                    <thead>
                      <tr style={{ borderBottom:`2px solid ${C.border}` }}>
                        {["Time","Signal","Conf","Open","Close","Δ","PM Odds","EV","Result"].map(h=>(
                          <th key={h} style={{ textAlign:"left", padding:"5px 8px", fontSize:9, letterSpacing:1,
                            textTransform:"uppercase", color:C.muted, position:"sticky", top:0, background:C.surface }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {preds.map((p,i)=>{
                        const delta=(p.end_price||0)-p.start_price;
                        return (
                          <tr key={i} style={{ borderBottom:`1px solid ${C.borderSoft}` }}>
                            <td style={td}>{new Date(p.window_start*1000).toLocaleTimeString()}</td>
                            <td style={{ ...td, color:p.signal==="UP"?C.green:C.red, fontWeight:700 }}>{p.signal}</td>
                            <td style={td}>{(p.confidence*100).toFixed(0)}%</td>
                            <td style={td}>${p.start_price.toFixed(0)}</td>
                            <td style={td}>{p.end_price!=null?`$${p.end_price.toFixed(0)}`:"—"}</td>
                            <td style={{ ...td, color:delta>=0?C.green:C.red }}>{delta>=0?"+":""}{delta.toFixed(1)}</td>
                            <td style={{ ...td, color:C.amber }}>{p.market_odds!=null?`1:${p.market_odds.toFixed(2)}`:"—"}</td>
                            <td style={{ ...td, color:p.ev>0?C.green:p.ev<0?C.red:C.muted }}>{p.ev!=null?(p.ev>0?"+":"")+p.ev.toFixed(3):"—"}</td>
                            <td style={td}>
                              <span style={{ padding:"2px 7px", borderRadius:4, fontSize:9, fontWeight:700,
                                background:p.correct?C.greenBg:C.redBg,
                                border:`1px solid ${p.correct?C.greenBorder:C.redBorder}`,
                                color:p.correct?C.green:C.red }}>{p.correct?"✓ WIN":"✕ LOSS"}</span>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                )}
            </div>
          </div>
        )}

        {/* ══ BINANCE TEST TAB ══ */}
        {tab==="binance_test" && (
          <ErrorBoundary key="binance-test-tab">
            <BinanceTestTab binanceExpert={binanceExpert} />
          </ErrorBoundary>
        )}

        {/* ══ ERRORS TAB ══ */}
        {tab==="errors" && (
          <ErrorBoundary key="errors-tab">
            <ErrorsTab errors={errorLog} />
          </ErrorBoundary>
        )}

        {/* ══ SETTINGS TAB ══ */}
        {tab==="settings" && (
          <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:6, height:"100%", overflow:"auto" }}>
            <div style={card}>
              <div style={label}>Strategy Weights</div>
              <div style={{ marginTop:8 }}>
                {STRATEGY_META.map(m=>{
                  const w=weights[m.key];
                  if (w==null) return null;
                  return (
                    <div key={m.key} style={{ display:"flex", alignItems:"center", gap:8, marginBottom:6 }}>
                      <span style={{ fontSize:10, color:m.color, minWidth:52, fontWeight:700 }}>{m.name}</span>
                      <div style={{ flex:1, height:4, background:C.borderSoft, borderRadius:2, overflow:"hidden" }}>
                        <div style={{ width:`${Math.min(w/3,1)*100}%`, height:"100%", background:m.color }} />
                      </div>
                      <span style={{ fontSize:10, color:C.textSec, minWidth:32, fontWeight:600 }}>{w.toFixed(2)}x</span>
                    </div>
                  );
                })}
                {Object.keys(weights).length===0&&<div style={{ color:C.muted, fontSize:10 }}>Need ≥10 resolved predictions to calibrate</div>}
                <button onClick={()=>fetch("/weights/update",{method:"POST"}).then(()=>fetch("/weights").then(r=>r.json()).then(setWeights))}
                  style={{ marginTop:8, background:C.amberBg, color:C.amber, border:`1px solid ${C.amberBorder}`,
                    padding:"5px 14px", borderRadius:5, cursor:"pointer", fontSize:10, fontFamily:"inherit", fontWeight:600 }}>
                  Recalculate Weights
                </button>
              </div>
            </div>
            <div style={card}>
              <div style={label}>System Status</div>
              <div style={{ marginTop:8, fontSize:11, lineHeight:1.9 }}>
                <Row label="WS feed"       val={connected?"Live":"Reconnecting…"} c={connected?C.green:C.amber} />
                <Row label="Data source"   val={dataSource} />
                <Row label="Strategies"    val={`${strats.length}/${STRATEGY_META.length} active`} />
                <Row label="Ensemble"      val={totalPreds>0?`${totalPreds} resolved · ${accuracy.toFixed(1)}%`:"warming up"} c={accuracy>=50?C.green:C.textSec} />
                <Row label="DeepSeek"      val={deepseekAcc?.total>0?`${deepseekAcc.total} resolved · ${(deepseekAcc.accuracy*100).toFixed(1)}% · ${deepseekAcc.neutrals??0}N`:"warming up"} c={deepseekAcc?.accuracy>=0.5?C.green:C.textSec} />
                <Row label="Agree only"    val={agreeAcc?.total_agree>0?`${agreeAcc.total_agree} · ${(agreeAcc.accuracy_agree*100).toFixed(1)}%`:"no windows"} c={agreeAcc?.accuracy_agree>=0.5?C.green:C.textSec} />
                <Row label="Polymarket"    val={polyLive?`LIVE · UP=${(polymarket.yes_price*100).toFixed(1)}% · 1:${polymarket.market_odds.toFixed(3)}`:"no market"} c={polyLive?C.green:C.muted} />
                <div style={{ borderTop:`1px solid ${C.borderSoft}`, margin:"6px 0 4px" }} />
                <div style={{ ...label, marginBottom:4 }}>Microstructure API status</div>
                {[["Order book",dots.ob],["Taker flow",dots.tk],["L/S ratio",dots.ls],["Liquidations",dots.lq],
                  ["OI+Funding",dots.oif],["Coinalyze",dots.cz],["CoinAPI",dots.ca],
                  ["Fear&Greed",dots.fg],["Mempool",dots.mp],["CoinGecko",dots.cg]].map(([n,d])=>(
                  <Row key={n} label={n} val={d==="live"?"●  live":d==="err"?"✕  error":"○  pending"} c={d==="live"?C.green:d==="err"?C.red:C.muted} />
                ))}
              </div>
            </div>
          </div>
        )}

      </div>

      <div style={{ textAlign:"center", fontSize:8, color:C.muted, letterSpacing:2, padding:"3px 0",
        flexShrink:0, borderTop:`1px solid ${C.borderSoft}`, background:C.surface }}>
        BTC ORACLE · NOT FINANCIAL ADVICE · performance recorded at bar close only
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

// ── Binance Test Tab ──────────────────────────────────────────
const BINANCE_ENDPOINTS = [
  { id:"spot_price",       label:"Spot Price",            url:"https://api.binance.com/api/v3/ticker/price",                                        params:{symbol:"BTCUSDT"},                          desc:"Current BTCUSDT spot price" },
  { id:"ticker_24hr",      label:"24hr Ticker",           url:"https://api.binance.com/api/v3/ticker/24hr",                                         params:{symbol:"BTCUSDT"},                          desc:"24h volume, price change, statistics" },
  { id:"order_book",       label:"Order Book Depth",      url:"https://api.binance.com/api/v3/depth",                                               params:{symbol:"BTCUSDT",limit:20},                 desc:"Top 20 bids & asks" },
  { id:"klines_1m",        label:"1m Klines (OHLCV)",     url:"https://api.binance.com/api/v3/klines",                                              params:{symbol:"BTCUSDT",interval:"1m",limit:10},   desc:"Last 10 × 1-min candles" },
  { id:"ls_global",        label:"Global L/S Ratio",      url:"https://fapi.binance.com/futures/data/globalLongShortAccountRatio",                  params:{symbol:"BTCUSDT",period:"5m",limit:1},      desc:"Retail long/short account ratio" },
  { id:"ls_top_acct",      label:"Top Trader L/S Acct",   url:"https://fapi.binance.com/futures/data/topLongShortAccountRatio",                     params:{symbol:"BTCUSDT",period:"5m",limit:1},      desc:"Smart money account positioning" },
  { id:"ls_top_pos",       label:"Top Trader L/S Pos",    url:"https://fapi.binance.com/futures/data/topLongShortPositionRatio",                    params:{symbol:"BTCUSDT",period:"5m",limit:1},      desc:"Top traders notional long/short" },
  { id:"taker_flow",       label:"Taker Buy/Sell Ratio",  url:"https://fapi.binance.com/futures/data/takerlongshortRatio",                          params:{symbol:"BTCUSDT",period:"5m",limit:3},      desc:"Aggressive buyer vs seller flow" },
  { id:"open_interest",    label:"Open Interest",         url:"https://fapi.binance.com/fapi/v1/openInterest",                                      params:{symbol:"BTCUSDT"},                          desc:"Total BTC futures open interest" },
  { id:"premium_index",    label:"Premium Index",         url:"https://fapi.binance.com/fapi/v1/premiumIndex",                                      params:{symbol:"BTCUSDT"},                          desc:"Mark price, index price, funding rate" },
  { id:"oi_history",       label:"OI History",            url:"https://fapi.binance.com/futures/data/openInterestHist",                             params:{symbol:"BTCUSDT",period:"5m",limit:6},      desc:"OI change over last 30 min" },
  { id:"funding_history",  label:"Funding Rate History",  url:"https://fapi.binance.com/fapi/v1/fundingRate",                                       params:{symbol:"BTCUSDT",limit:6},                  desc:"Last 6 funding rates" },
];

function BinanceTestTab({ binanceExpert }) {
  const [results, setResults] = React.useState({});
  const [loading, setLoading] = React.useState(false);
  const [lastRun, setLastRun] = React.useState(null);

  const buildUrl = (url, params) => {
    const qs = Object.entries(params).map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join("&");
    return `${url}?${qs}`;
  };

  const summarise = (data) => {
    if (Array.isArray(data)) {
      const first = data[0];
      return { _type:`array[${data.length}]`, ...(typeof first==="object"&&first?first:{}) };
    }
    return data;
  };

  const runAll = React.useCallback(async () => {
    setLoading(true);
    setResults({});
    const fresh = {};
    await Promise.all(BINANCE_ENDPOINTS.map(async (ep) => {
      const t0 = Date.now();
      try {
        const r = await fetch(buildUrl(ep.url, ep.params));
        const ms = Date.now() - t0;
        const data = await r.json();
        fresh[ep.id] = { ok: r.ok, status: r.status, ms, data: summarise(data) };
      } catch(e) {
        fresh[ep.id] = { ok: false, status: "ERR", ms: Date.now()-t0, error: String(e) };
      }
    }));
    setResults(fresh);
    setLastRun(new Date().toLocaleTimeString());
    setLoading(false);
  }, []);

  React.useEffect(() => { runAll(); }, []);

  const passed = Object.values(results).filter(r=>r.ok).length;
  const total  = Object.keys(results).length;

  return (
    <div style={{ height:"100%", overflowY:"auto", display:"flex", flexDirection:"column", gap:6, paddingBottom:8 }}>
      {/* Header */}
      <div style={{ ...card, flexShrink:0, display:"flex", alignItems:"center", gap:12 }}>
        <div>
          <div style={{ fontSize:13, fontWeight:800, color:C.text }}>Binance API Connectivity Test</div>
          <div style={{ fontSize:9, color:C.muted, marginTop:2 }}>
            {total>0 ? `${passed}/${total} endpoints reachable` : "Press Run to test"}{lastRun?` · last run ${lastRun}`:""}
          </div>
        </div>
        <button onClick={runAll} disabled={loading} style={{
          marginLeft:"auto", background:C.amberBg, color:C.amber, border:`1px solid ${C.amberBorder}`,
          padding:"5px 16px", borderRadius:5, cursor:loading?"default":"pointer",
          fontSize:10, fontFamily:"inherit", fontWeight:700, letterSpacing:0.5, opacity:loading?0.6:1 }}>
          {loading ? "Testing…" : "Run All"}
        </button>
        {total>0 && (
          <div style={{ fontSize:11, fontWeight:700,
            color: passed===total ? C.green : passed===0 ? C.red : C.amber }}>
            {passed===total ? "✓ All OK" : passed===0 ? "✗ All Failed" : `${passed}/${total} OK`}
          </div>
        )}
      </div>

      {/* Latest Binance AI Analysis */}
      {binanceExpert && (
        <div style={{ ...card, flexShrink:0, borderLeft:`3px solid ${
          binanceExpert.signal==="UP" ? C.green : binanceExpert.signal==="DOWN" ? C.red : C.amber}` }}>
          <div style={{ display:"flex", alignItems:"center", gap:10, marginBottom:6 }}>
            <div style={{ fontSize:11, fontWeight:800, color:C.text }}>Latest Binance Expert Analysis</div>
            <div style={{ marginLeft:"auto", display:"flex", alignItems:"center", gap:6 }}>
              <div style={{ fontSize:12, fontWeight:900,
                color: binanceExpert.signal==="UP" ? C.green : binanceExpert.signal==="DOWN" ? C.red : C.amber }}>
                {binanceExpert.signal==="UP" ? "▲ ABOVE" : binanceExpert.signal==="DOWN" ? "▼ BELOW" : "◆ NEUTRAL"}
              </div>
              <div style={{ fontSize:10, fontWeight:700, color:C.textSec,
                background: binanceExpert.signal==="UP" ? C.greenBg : binanceExpert.signal==="DOWN" ? C.redBg : C.amberBg,
                border:`1px solid ${binanceExpert.signal==="UP" ? C.greenBorder : binanceExpert.signal==="DOWN" ? C.redBorder : C.amberBorder}`,
                borderRadius:4, padding:"2px 8px" }}>
                {binanceExpert.confidence}% confidence
              </div>
            </div>
          </div>
          {binanceExpert.analysis && (
            <div style={{ fontSize:10, color:C.textSec, lineHeight:1.5, marginBottom:4 }}>
              <span style={{ fontWeight:700, color:C.muted, fontSize:9, letterSpacing:1, textTransform:"uppercase" }}>Analysis · </span>
              {binanceExpert.analysis}
            </div>
          )}
          {binanceExpert.reasoning && (
            <div style={{ fontSize:10, color:C.textSec, lineHeight:1.5 }}>
              <span style={{ fontWeight:700, color:C.muted, fontSize:9, letterSpacing:1, textTransform:"uppercase" }}>Reasoning · </span>
              {binanceExpert.reasoning}
            </div>
          )}
        </div>
      )}
      {!binanceExpert && (
        <div style={{ ...card, flexShrink:0, background:C.amberBg, border:`1px solid ${C.amberBorder}` }}>
          <div style={{ fontSize:10, color:C.amber, fontWeight:700 }}>Binance Expert Analysis · Waiting for next bar...</div>
          <div style={{ fontSize:9, color:C.muted, marginTop:2 }}>The AI analysis of Binance microstructure data will appear here after the next 5-minute bar opens.</div>
        </div>
      )}

      {/* Grid of endpoint cards */}
      <div style={{ display:"grid", gridTemplateColumns:"repeat(3,1fr)", gap:6 }}>
        {BINANCE_ENDPOINTS.map(ep => {
          const r = results[ep.id];
          const statusColor = !r ? C.muted : r.ok ? C.green : C.red;
          const bgColor     = !r ? C.surface : r.ok ? C.greenBg : C.redBg;
          const borderColor = !r ? C.border  : r.ok ? C.greenBorder : C.redBorder;

          // pick a few key fields to show prominently
          const highlights = [];
          if (r?.ok && r.data) {
            const d = r.data;
            if (d.price)                highlights.push(["Price",  `$${parseFloat(d.price).toLocaleString()}`]);
            if (d.markPrice)            highlights.push(["Mark",   `$${parseFloat(d.markPrice).toLocaleString()}`]);
            if (d.openInterest)         highlights.push(["OI",     parseFloat(d.openInterest).toLocaleString()+" BTC"]);
            if (d.longShortRatio)       highlights.push(["L/S",    parseFloat(d.longShortRatio).toFixed(4)]);
            if (d.longAccount)          highlights.push(["Long",   `${(parseFloat(d.longAccount)*100).toFixed(1)}%`]);
            if (d.buySellRatio)         highlights.push(["BSR",    parseFloat(d.buySellRatio).toFixed(4)]);
            if (d.lastFundingRate!==undefined) highlights.push(["Fund%", (parseFloat(d.lastFundingRate||d.fundingRate||0)*100).toFixed(4)+"%"]);
            if (d.fundingRate!==undefined && !d.lastFundingRate) highlights.push(["Fund%", (parseFloat(d.fundingRate)*100).toFixed(4)+"%"]);
            if (d.priceChangePercent)   highlights.push(["24hΔ",   `${parseFloat(d.priceChangePercent).toFixed(2)}%`]);
            if (d.weightedAvgPrice)     highlights.push(["VWAP",   `$${parseFloat(d.weightedAvgPrice).toLocaleString()}`]);
            if (d.sumOpenInterest)      highlights.push(["OI",     parseFloat(d.sumOpenInterest).toLocaleString()+" BTC"]);
            if (d._type)                highlights.push(["Items",  d._type]);
          }

          return (
            <div key={ep.id} style={{ background:bgColor, border:`1px solid ${borderColor}`, borderRadius:8, padding:"10px 12px" }}>
              {/* Title row */}
              <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", marginBottom:4 }}>
                <div>
                  <div style={{ fontSize:11, fontWeight:700, color:C.text }}>{ep.label}</div>
                  <div style={{ fontSize:8, color:C.muted, marginTop:1 }}>{ep.desc}</div>
                </div>
                <div style={{ textAlign:"right", flexShrink:0, marginLeft:6 }}>
                  <div style={{ fontSize:11, fontWeight:800, color:statusColor }}>
                    {!r ? "—" : r.ok ? `✓ ${r.status}` : `✗ ${r.status}`}
                  </div>
                  {r?.ms!=null && <div style={{ fontSize:8, color:C.muted }}>{r.ms}ms</div>}
                </div>
              </div>

              {/* Highlights */}
              {highlights.length>0 && (
                <div style={{ display:"flex", flexWrap:"wrap", gap:4, marginBottom:4 }}>
                  {highlights.slice(0,4).map(([k,v])=>(
                    <div key={k} style={{ background:C.surface, borderRadius:4, padding:"2px 6px", fontSize:9 }}>
                      <span style={{ color:C.muted }}>{k} </span>
                      <span style={{ fontWeight:700, color:C.text }}>{v}</span>
                    </div>
                  ))}
                </div>
              )}

              {/* Raw JSON preview */}
              {r && (
                <details style={{ marginTop:2 }}>
                  <summary style={{ fontSize:8, color:C.muted, cursor:"pointer", userSelect:"none" }}>raw response</summary>
                  <pre style={{ fontSize:8, color:C.textSec, marginTop:4, whiteSpace:"pre-wrap", wordBreak:"break-all",
                    background:C.bg, padding:4, borderRadius:4, maxHeight:120, overflow:"auto" }}>
                    {r.error || JSON.stringify(r.data, null, 2)}
                  </pre>
                </details>
              )}

              {/* Loading shimmer */}
              {loading && !r && (
                <div style={{ fontSize:9, color:C.muted }}>Testing…</div>
              )}
            </div>
          );
        })}
      </div>
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
