"""
=============================================================
  FVG Scanner — OKX USDT Perpetual Swaps
  Streamlit UI  |  4H / 1D / 1W  |  Lookback 5/10/15
=============================================================
  Install : pip install streamlit requests
  Run     : streamlit run fvg_scanner.py

  Deploy  : Push to GitHub → share.streamlit.io
  No-sleep: Add to UptimeRobot (uptimerobot.com)
            Monitor Type: HTTP(s)
            URL: your Streamlit app URL
            Interval: 5 minutes
            → Pings every 5 min, keeps app awake 24/7
=============================================================

  BULLISH FVG (all 6 must be true):
    C2.open  < C1.close  — C2 opens inside C1 body
    C2.low   > C1.low    — C2 bottom above C1
    C2.high  > C1.high   — C2 top above C1
    C3.low   > C2.low    — C3 bottom above C2
    C3.high  > C2.high   — C3 top above C2
    C1.high  < C3.low    — gap zone = C1.high → C3.low

  BEARISH FVG (all 6 must be true):
    C2.open  > C1.close  — C2 opens inside C1 body
    C2.high  < C1.high   — C2 top below C1
    C2.low   < C1.low    — C2 bottom below C1
    C3.high  < C2.high   — C3 top below C2
    C3.low   < C2.low    — C3 bottom below C2
    C1.low   > C3.high   — gap zone = C3.high → C1.low
=============================================================
"""

import streamlit as st
import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────
BASE_URL   = "https://www.okx.com"
BATCH_SIZE = 8
DELAY_SEC  = 0.2
TIMEFRAMES = ["4h", "1d", "1w"]
LOOKBACKS  = [5, 10, 15]
OKX_TF     = {"4h": "4H", "1d": "1D", "1w": "1W"}

st.set_page_config(
    page_title="FVG Scanner",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── CSS ───────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
html,body,[class*="css"],[class*="st-"]{font-family:'JetBrains Mono',monospace!important;}
#MainMenu,footer,header{visibility:hidden;}
.block-container{padding-top:1rem!important;max-width:1260px;}

.pg-title{font-size:22px;font-weight:700;color:#fff;letter-spacing:3px;font-family:'JetBrains Mono',monospace;}
.pg-sub{font-size:11px;color:#8090b0;letter-spacing:.5px;margin-bottom:10px;font-family:'JetBrains Mono',monospace;}

.badge{display:inline-block;font-size:10px;font-weight:700;padding:2px 9px;border-radius:4px;letter-spacing:1px;font-family:'JetBrains Mono',monospace;}
.b-purple{color:#c49fff;background:#1e0d40;border:1px solid #6030c0;}
.b-blue  {color:#80b0ff;background:#0d1a40;border:1px solid #2050c0;}
.b-green {color:#40d4c0;background:#041814;border:1px solid #108060;}
.b-red   {color:#ff8080;background:#280808;border:1px solid #a02020;}
.b-okx   {color:#ffb840;background:#201008;border:1px solid #805010;}

.sec-lbl{font-size:11px;font-weight:700;color:#9090b8;letter-spacing:3px;margin-bottom:6px;margin-top:4px;font-family:'JetBrains Mono',monospace;text-transform:uppercase;}

/* Stat boxes */
.stat-box{background:#0d1424;border:1px solid #253050;border-radius:8px;padding:11px 8px;text-align:center;}
.stat-val{font-size:26px;font-weight:700;line-height:1.1;font-family:'JetBrains Mono',monospace;}
.stat-lbl{font-size:10px;font-weight:700;color:#9090b8;letter-spacing:2px;margin-top:5px;font-family:'JetBrains Mono',monospace;}
.c-w {color:#e8eef8;}
.c-g {color:#40d4c0;}
.c-r {color:#ff8080;}
.c-a {color:#ffb840;}
.c-b {color:#80b0ff;}

/* Results table */
.t-head{
    display:grid;
    grid-template-columns:1.2fr 120px 68px 120px 75px 110px 105px;
    padding:10px 16px;
    background:#090e1e;
    border:1px solid #253050;
    border-bottom:2px solid #3050a0;
    border-radius:8px 8px 0 0;
    gap:8px;
}
.t-head span{font-size:11px;font-weight:700;color:#b0c4e8;letter-spacing:2px;font-family:'JetBrains Mono',monospace;}
.t-body{border:1px solid #253050;border-top:none;border-radius:0 0 8px 8px;overflow:hidden;max-height:460px;overflow-y:auto;}
.t-row{
    display:grid;
    grid-template-columns:1.2fr 120px 68px 120px 75px 110px 105px;
    align-items:center;
    padding:10px 16px;
    border-bottom:1px solid #111e34;
    background:#0b1020;
    gap:8px;
    animation:fadeIn .3s ease;
}
@keyframes fadeIn{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
.t-row:nth-child(even){background:#0d1428;}
.t-row:hover{background:#14203c;}
.t-row:last-child{border-bottom:none;}
.sym{font-size:14px;font-weight:700;color:#e8eef8;font-family:'JetBrains Mono',monospace;}
.cell{font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace;}
.cell-dim{color:#6080a8;}
.cell-g  {color:#40d4c0;}
.cell-r  {color:#ff8080;}
.cell-w  {color:#e8eef8;}
.cell-a  {color:#ffb840;}
.t-empty{padding:36px;text-align:center;color:#4060a0;font-size:13px;font-weight:600;background:#0b1020;font-family:'JetBrains Mono',monospace;}
.t-foot{padding:8px 16px;background:#080c18;border:1px solid #253050;border-top:1px solid #1a2840;border-radius:0 0 8px 8px;font-size:11px;font-weight:600;color:#6080a8;font-family:'JetBrains Mono',monospace;display:flex;justify-content:space-between;}

/* Log */
.log-wrap{background:#070b14;border:1px solid #253050;border-radius:8px;overflow:hidden;}
.log-hdr{padding:8px 14px;background:#090e1e;border-bottom:1px solid #253050;font-size:11px;font-weight:700;color:#9090b8;letter-spacing:2px;font-family:'JetBrains Mono',monospace;display:flex;justify-content:space-between;}
.log-body{padding:8px 14px;font-size:11px;line-height:2;max-height:130px;overflow-y:auto;font-family:'JetBrains Mono',monospace;}
.log-dim {color:#4a6080;}
.log-g   {color:#40d4c0;}
.log-r   {color:#ff8080;}
.log-err {color:#ff7070;}
.log-done{color:#80b0ff;}
.log-warn{color:#ffb840;}

.prog-label{font-size:12px;font-weight:600;color:#9090b8;font-family:'JetBrains Mono',monospace;margin-bottom:4px;}
.prog-sym{font-size:11px;color:#506080;margin-top:3px;font-family:'JetBrains Mono',monospace;}

div.stButton>button{font-family:'JetBrains Mono',monospace!important;font-weight:700!important;font-size:13px!important;letter-spacing:1px!important;}
div.stRadio label{font-family:'JetBrains Mono',monospace!important;font-size:12px!important;font-weight:600!important;color:#a0b4d8!important;}
.stProgress>div>div>div>div{background:linear-gradient(90deg,#1a5cff,#40d4c0)!important;}
hr{border-color:#253050!important;margin:10px 0!important;}
::-webkit-scrollbar{width:3px;}
::-webkit-scrollbar-track{background:#070b14;}
::-webkit-scrollbar-thumb{background:#253050;border-radius:2px;}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# FVG DETECTION
# ══════════════════════════════════════════════════════════
def detect_fvg(candles, lookback):
    """
    Scan last `lookback` completed candles for FVG patterns.
    Candle tuple: (open, high, low, close, volume, timestamp)

    Bullish FVG — 6 conditions:
      C2.open  < C1.close  (C2 opens inside C1 body)
      C2.low   > C1.low    (C2 bottom above C1)
      C2.high  > C1.high   (C2 top above C1)
      C3.low   > C2.low    (C3 bottom above C2)
      C3.high  > C2.high   (C3 top above C2)
      C1.high  < C3.low    (THE GAP zone)

    Bearish FVG — 6 conditions:
      C2.open  > C1.close  (C2 opens inside C1 body)
      C2.high  < C1.high   (C2 top below C1)
      C2.low   < C1.low    (C2 bottom below C1)
      C3.high  < C2.high   (C3 top below C2)
      C3.low   < C2.low    (C3 bottom below C2)
      C1.low   > C3.high   (THE GAP zone)
    """
    window = candles[-(lookback + 2):] if len(candles) > lookback + 2 else candles
    n      = len(window)
    found  = []

    for i in range(n - 2):
        c1, c2, c3 = window[i], window[i+1], window[i+2]
        o1, h1, l1, cl1 = c1[0], c1[1], c1[2], c1[3]
        o2, h2, l2, cl2 = c2[0], c2[1], c2[2], c2[3]
        o3, h3, l3, cl3 = c3[0], c3[1], c3[2], c3[3]

        fvg_type = gap_low = gap_high = None

        # ── Bullish FVG ──
        if (o2 < cl1
                and l2 > l1 and h2 > h1
                and l3 > l2 and h3 > h2
                and h1 < l3):
            fvg_type = "BULLISH"
            gap_low  = h1    # C1 high = bottom of gap
            gap_high = l3    # C3 low  = top of gap

        # ── Bearish FVG ──
        elif (o2 > cl1
                and h2 < h1 and l2 < l1
                and h3 < h2 and l3 < l2
                and l1 > h3):
            fvg_type = "BEARISH"
            gap_low  = h3    # C3 high = bottom of gap
            gap_high = l1    # C1 low  = top of gap

        if fvg_type is None:
            continue

        gap_size = round((gap_high - gap_low) / gap_low * 100, 4) if gap_low else 0
        candles_ago = (n - 1) - (i + 2)

        found.append({
            "fvg_type":   fvg_type,
            "gap_low":    round(gap_low,  6),
            "gap_high":   round(gap_high, 6),
            "gap_size":   gap_size,
            "c1_high":    round(h1, 6),
            "c1_low":     round(l1, 6),
            "c3_high":    round(h3, 6),
            "c3_low":     round(l3, 6),
            "candles_ago": candles_ago,
            "timestamp":  c3[5] if len(c3) > 5 else None,
        })

    # Newest first
    found.sort(key=lambda x: -(x["timestamp"] or 0))
    return found


# ══════════════════════════════════════════════════════════
# OKX API  (accessible in India — no geo-block)
# ══════════════════════════════════════════════════════════
def fetch_symbols():
    r = requests.get(
        f"{BASE_URL}/api/v5/public/instruments",
        params={"instType": "SWAP"},
        timeout=10
    )
    r.raise_for_status()
    return sorted([
        s["instId"] for s in r.json()["data"]
        if s["settleCcy"] == "USDT"
        and s["state"] == "live"
        and s["instId"].endswith("-USDT-SWAP")
    ])

def fetch_klines(symbol, interval, limit):
    bar = OKX_TF.get(interval, "4H")
    params = {"instId": symbol, "bar": bar, "limit": str(limit + 3)}
    r = requests.get(
        f"{BASE_URL}/api/v5/market/candles",
        params=params, timeout=10
    )
    if r.status_code == 429:
        time.sleep(3)
        r = requests.get(
            f"{BASE_URL}/api/v5/market/candles",
            params=params, timeout=10
        )
    r.raise_for_status()
    raw = list(reversed(r.json()["data"]))
    # Drop current incomplete candle (confirm == "0")
    if raw and raw[-1][8] == "0":
        raw = raw[:-1]
    # OKX format: [ts, open, high, low, close, vol, ...]
    return [
        (float(c[1]), float(c[2]), float(c[3]),
         float(c[4]), float(c[5]), int(c[0]))
        for c in raw
    ]


# ══════════════════════════════════════════════════════════
# RENDER HELPERS
# ══════════════════════════════════════════════════════════
def fmt_date(ts_ms):
    if not ts_ms: return "—"
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")

def fmt_ago(n):
    if n == 0: return "latest"
    return f"{n}C ago"

def render_table(results):
    if not results:
        return '<div class="t-body"><div class="t-empty">No FVGs detected yet — press ▶ SCAN NOW</div></div>'
    html = ""
    for r in results:
        is_bull    = r["fvg_type"] == "BULLISH"
        type_badge = (
            '<span class="badge b-green">▲ Bullish FVG</span>'
            if is_bull else
            '<span class="badge b-red">▼ Bearish FVG</span>'
        )
        size_cls = "cell-g" if is_bull else "cell-r"
        inv_lbl  = "C3.low" if is_bull else "C3.high"
        inv_val  = r["c3_low"] if is_bull else r["c3_high"]
        html += f"""<div class="t-row">
            <span class="sym">{r['symbol']}</span>
            {type_badge}
            <span class="cell cell-dim">{r['tf'].upper()}</span>
            <span class="cell cell-dim">{fmt_date(r['timestamp'])}</span>
            <span class="cell {size_cls}">{r['gap_size']:.4f}%</span>
            <span class="cell cell-w">{r['gap_low']:,.4f} – {r['gap_high']:,.4f}</span>
            <span class="cell cell-a">{inv_lbl}: {inv_val:,.4f}</span>
        </div>"""
    return f'<div class="t-body">{html}</div>'

def render_log(log_lines):
    if not log_lines:
        return '<div class="log-dim" style="opacity:.4;">Scan log will appear here...</div>'
    return "".join(f'<div class="log-{lv}">{msg}</div>' for msg, lv in log_lines[-50:])


# ══════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════
for k, v in [
    ("symbol_list", []), ("results", []),
    ("log_lines", []), ("scanning", False), ("scan_done", False),
]:
    if k not in st.session_state:
        st.session_state[k] = v


# ══════════════════════════════════════════════════════════
# PAGE LAYOUT
# ══════════════════════════════════════════════════════════
st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
  <span style="font-size:14px;color:#40d4c0;">◆</span>
  <span class="pg-title">FVG SCANNER</span>
  <span class="badge b-okx">OKX</span>
  <span class="badge b-blue">USDT SWAP</span>
  <span class="badge b-purple">4H · 1D · 1W</span>
</div>
<div class="pg-sub">
  Bullish: C2.open&lt;C1.close · C2 engulfs C1 up · C3 engulfs C2 up · C1.high&lt;C3.low (gap) &nbsp;|&nbsp;
  Bearish: reverse · All 6 conditions required
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ── Load symbols ──
if not st.session_state.symbol_list:
    with st.spinner("Loading OKX symbols..."):
        try:
            st.session_state.symbol_list = fetch_symbols()
        except Exception as e:
            st.error(f"Failed to load symbols: {e}")
            st.stop()

# ── Controls ──
c1, c2, c3 = st.columns([2, 2, 1])
with c1:
    st.markdown('<div class="sec-lbl">Timeframe</div>', unsafe_allow_html=True)
    tf = st.radio("tf", TIMEFRAMES, horizontal=True,
                  label_visibility="collapsed",
                  disabled=st.session_state.scanning)
with c2:
    st.markdown('<div class="sec-lbl">Lookback (completed candles)</div>', unsafe_allow_html=True)
    lb = st.radio("lb", LOOKBACKS, horizontal=True,
                  label_visibility="collapsed",
                  disabled=st.session_state.scanning)
with c3:
    st.markdown('<div class="sec-lbl">&nbsp;</div>', unsafe_allow_html=True)
    scan_btn = st.button(
        "■  STOP" if st.session_state.scanning else "▶  SCAN NOW",
        use_container_width=True, type="primary"
    )

st.markdown("---")

# ── Stat boxes ──
sb1, sb2, sb3, sb4, sb5, sb6 = st.columns(6)
stat_scanned = sb1.empty(); stat_total  = sb2.empty()
stat_bull    = sb3.empty(); stat_bear   = sb4.empty()
stat_hits    = sb5.empty(); stat_eta    = sb6.empty()

def render_stats(scanned=0, total=0, bull=0, bear=0, hits=0, eta="—"):
    stat_scanned.markdown(f'<div class="stat-box"><div class="stat-val c-w">{scanned}</div><div class="stat-lbl">SCANNED</div></div>',    unsafe_allow_html=True)
    stat_total.markdown(  f'<div class="stat-box"><div class="stat-val c-w">{total or "—"}</div><div class="stat-lbl">TOTAL</div></div>', unsafe_allow_html=True)
    stat_bull.markdown(   f'<div class="stat-box"><div class="stat-val c-g">{bull}</div><div class="stat-lbl">BULL FVG</div></div>',       unsafe_allow_html=True)
    stat_bear.markdown(   f'<div class="stat-box"><div class="stat-val c-r">{bear}</div><div class="stat-lbl">BEAR FVG</div></div>',       unsafe_allow_html=True)
    stat_hits.markdown(   f'<div class="stat-box"><div class="stat-val c-a">{hits}</div><div class="stat-lbl">TOTAL HITS</div></div>',     unsafe_allow_html=True)
    stat_eta.markdown(    f'<div class="stat-box"><div class="stat-val c-b">{eta}</div><div class="stat-lbl">ETA (SEC)</div></div>',       unsafe_allow_html=True)

render_stats()

# ── Progress ──
prog_label_el = st.empty()
prog_bar_el   = st.empty()
prog_sym_el   = st.empty()

# ── Results table ──
st.markdown('<div class="sec-lbl" style="margin-top:14px;">Detected FVGs — results appear live as found</div>', unsafe_allow_html=True)

t_head_el = st.empty()
t_body_el = st.empty()
t_foot_el = st.empty()

t_head_el.markdown("""<div class="t-head">
  <span>Symbol</span>
  <span>Type</span>
  <span>TF</span>
  <span>Formed At (UTC)</span>
  <span>Gap Size</span>
  <span>Gap Zone</span>
  <span>C3 Level</span>
</div>""", unsafe_allow_html=True)

t_body_el.markdown(
    '<div class="t-body"><div class="t-empty">Select timeframe &amp; lookback then press ▶ SCAN NOW</div></div>',
    unsafe_allow_html=True
)

# ── Activity log ──
st.markdown('<div class="sec-lbl" style="margin-top:14px;">Activity Log</div>', unsafe_allow_html=True)
log_el = st.empty()
log_el.markdown("""<div class="log-wrap">
  <div class="log-hdr"><span>SCAN LOG</span><span>0 events</span></div>
  <div class="log-body"><div class="log-dim" style="opacity:.4;">Press ▶ SCAN NOW to start...</div></div>
</div>""", unsafe_allow_html=True)

# ── Legend ──
st.markdown("""
<div style="display:flex;flex-wrap:wrap;gap:14px;margin-top:8px;
     font-size:11px;font-weight:600;color:#4060a0;font-family:'JetBrains Mono',monospace;">
  <span><span style="color:#40d4c0;">▲ Bullish</span> — 3 rising candles, gap between C1.high &amp; C3.low</span>
  <span><span style="color:#ff8080;">▼ Bearish</span> — 3 falling candles, gap between C3.high &amp; C1.low</span>
  <span><span style="color:#ffb840;">C3 Level</span> — price that invalidates the FVG if breached</span>
  <span style="margin-left:auto;">8 symbols/batch · 200ms delay · OKX public API · India accessible</span>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ── Keep-alive tip ──
st.markdown("""
<div style="background:#090e1e;border:1px solid #1e2d4a;border-radius:8px;padding:12px 16px;font-size:11px;font-family:'JetBrains Mono',monospace;">
  <span style="color:#ffb840;font-weight:700;">⏰ PREVENT STREAMLIT SLEEP</span>
  <span style="color:#6080a8;"> — Free tier sleeps after 15 min inactivity.</span>
  <br>
  <span style="color:#4a6080;">1. Go to </span><span style="color:#80b0ff;">uptimerobot.com</span>
  <span style="color:#4a6080;"> → Sign up free → New Monitor</span><br>
  <span style="color:#4a6080;">2. Type: </span><span style="color:#e8eef8;">HTTP(s)</span>
  <span style="color:#4a6080;"> · URL: </span><span style="color:#e8eef8;">your Streamlit app URL</span>
  <span style="color:#4a6080;"> · Interval: </span><span style="color:#e8eef8;">5 minutes</span>
  <span style="color:#4a6080;"> → Create Monitor</span><br>
  <span style="color:#40d4c0;">✓ App stays awake 24/7 at $0 cost</span>
</div>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════
def update_table(results):
    t_body_el.markdown(render_table(results), unsafe_allow_html=True)
    n    = len(results)
    bull = sum(1 for r in results if r["fvg_type"] == "BULLISH")
    bear = sum(1 for r in results if r["fvg_type"] == "BEARISH")
    t_foot_el.markdown(
        f'<div class="t-foot">'
        f'<span>{n} FVG{"s" if n!=1 else ""} found'
        f' &nbsp;·&nbsp; {bull} bullish &nbsp;·&nbsp; {bear} bearish</span>'
        f'<span>{tf.upper()} &nbsp;·&nbsp; lookback {lb} candles</span>'
        f'</div>',
        unsafe_allow_html=True
    )

def update_log(log_lines):
    log_el.markdown(f"""<div class="log-wrap">
      <div class="log-hdr"><span>SCAN LOG</span><span>{len(log_lines)} events</span></div>
      <div class="log-body">{render_log(log_lines)}</div>
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# SCAN EXECUTION
# ══════════════════════════════════════════════════════════
if scan_btn and not st.session_state.scanning:
    st.session_state.results   = []
    st.session_state.log_lines = []
    st.session_state.scanning  = True
    st.session_state.scan_done = False

    results    = []
    log_lines  = []
    _cnt       = [0]
    scan_start = time.time()

    def log(msg, level="dim"):
        log_lines.append((msg, level))
        _cnt[0] += 1
        update_log(log_lines)

    symbols = st.session_state.symbol_list
    total   = len(symbols)

    log(f"⏳  Starting scan · {total} symbols · {tf.upper()} · lookback {lb}", "dim")
    render_stats(0, total, 0, 0, 0, "—")
    prog_label_el.markdown(
        '<div class="prog-label">Scanning all OKX USDT perpetuals for FVGs...</div>',
        unsafe_allow_html=True
    )
    bar     = prog_bar_el.progress(0)
    scanned = 0
    bull_ct = 0
    bear_ct = 0

    t_body_el.markdown(
        '<div class="t-body"><div class="t-empty">Scanning — FVGs appear here instantly as found...</div></div>',
        unsafe_allow_html=True
    )

    try:
        for i in range(0, total, BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as ex:
                futures = {ex.submit(fetch_klines, s, tf, lb): s for s in batch}
                for fut in as_completed(futures):
                    sym_name = futures[fut]
                    try:
                        candles = fut.result()
                        fvgs    = detect_fvg(candles, lb)
                        for fvg in fvgs:
                            fvg["symbol"] = sym_name
                            fvg["tf"]     = tf
                            results.append(fvg)
                            is_bull = fvg["fvg_type"] == "BULLISH"
                            if is_bull: bull_ct += 1
                            else:       bear_ct += 1
                            arrow = "▲" if is_bull else "▼"
                            log(
                                f"{arrow}  {sym_name}"
                                f"  {fvg['gap_size']:.4f}%"
                                f"  zone: {fvg['gap_low']:,.4f}–{fvg['gap_high']:,.4f}"
                                f"  [{fmt_date(fvg['timestamp'])}]",
                                "g" if is_bull else "r"
                            )
                            update_table(results)
                    except Exception as e:
                        if "429" in str(e):
                            log("⚠️  Rate limit — pausing 3s...", "warn")
                            time.sleep(3)

                    scanned += 1
                    elapsed  = time.time() - scan_start
                    rate     = scanned / elapsed if elapsed > 0 else 1
                    eta      = max(0, int((total - scanned) / rate))
                    bar.progress(int(scanned / total * 100))
                    render_stats(scanned, total, bull_ct, bear_ct, len(results), eta)
                    prog_sym_el.markdown(
                        f'<div class="prog-sym">Scanning: <b style="color:#8090b8;">{sym_name}</b>'
                        f' &nbsp;·&nbsp; ETA {eta}s</div>',
                        unsafe_allow_html=True
                    )

            if i + BATCH_SIZE < total:
                time.sleep(DELAY_SEC)

        elapsed = round(time.time() - scan_start, 1)
        bar.progress(100)
        render_stats(total, total, bull_ct, bear_ct, len(results), 0)
        prog_label_el.markdown(
            f'<div class="prog-label" style="color:#40d4c0;">'
            f'✅  Complete · {len(results)} FVG(s) found · {elapsed}s</div>',
            unsafe_allow_html=True
        )
        prog_sym_el.empty()
        log(f"🏁  Scan done · {len(results)} FVG(s) · {bull_ct}▲ {bear_ct}▼ · {elapsed}s", "done")

        if not results:
            t_body_el.markdown(
                '<div class="t-body"><div class="t-empty">'
                'No FVGs found. Try a wider lookback or different timeframe.'
                '</div></div>', unsafe_allow_html=True
            )

    except Exception as e:
        import traceback
        log(f"❌  Error: {e}", "err")
        log(traceback.format_exc(), "err")

    st.session_state.scanning  = False
    st.session_state.scan_done = True
    st.session_state.results   = results
    st.session_state.log_lines = log_lines

# ── Restore on rerun ──
elif st.session_state.scan_done and st.session_state.results:
    r    = st.session_state.results
    bull = sum(1 for x in r if x["fvg_type"] == "BULLISH")
    bear = sum(1 for x in r if x["fvg_type"] == "BEARISH")
    update_table(r)
    update_log(st.session_state.log_lines)
    render_stats(len(r), len(st.session_state.symbol_list), bull, bear, len(r), 0)
