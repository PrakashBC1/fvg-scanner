"""
=============================================================
  FVG Pullback Scanner — OKX USDT Perpetual Swaps
  Streamlit UI  |  Fast + IP-Safe
=============================================================
  Install : pip install streamlit requests
  Run     : streamlit run fvg_new_scanner.py
=============================================================

  FVG LOGIC (4-condition):
    Bullish:
      ① C2.low  > C1.low   (C2 engulfs C1 below)
      ② C2.high > C1.high  (C2 engulfs C1 above)
      ③ C2.close < C3.open (gap up into C3)
      ④ C1.high < C3.low   (gap zone = C1.high → C3.low)

    Bearish:
      ① C2.high < C1.high  (C2 engulfs C1 above)
      ② C2.low  < C1.low   (C2 engulfs C1 below)
      ③ C2.close > C3.open (gap down into C3)
      ④ C1.low  > C3.high  (gap zone = C3.high → C1.low)

  PULLBACK CONFIRMED:
    Bullish : candle after C3 enters gap zone, C3.high not breached
    Bearish : candle after C3 enters gap zone, C3.low  not breached
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
TIMEFRAMES = ["1h", "4h", "1d", "1w"]
LOOKBACKS  = [5, 10, 15]
OKX_INTERVAL = {"1h": "1H", "4h": "4H", "1d": "1D", "1w": "1W"}

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
.block-container{padding-top:1rem!important;max-width:1280px;}
.pg-title{font-size:22px;font-weight:700;color:#fff;letter-spacing:3px;font-family:'JetBrains Mono',monospace;}
.pg-sub{font-size:11px;color:#8090b0;letter-spacing:.5px;margin-bottom:10px;font-family:'JetBrains Mono',monospace;}
.badge{display:inline-block;font-size:10px;font-weight:700;padding:2px 9px;border-radius:4px;letter-spacing:1px;font-family:'JetBrains Mono',monospace;}
.b-purple{color:#c49fff;background:#1e0d40;border:1px solid #6030c0;}
.b-blue  {color:#80b0ff;background:#0d1a40;border:1px solid #2050c0;}
.b-green {color:#40d4c0;background:#041814;border:1px solid #108060;}
.b-red   {color:#ff8080;background:#280808;border:1px solid #a02020;}
.b-amber {color:#ffb840;background:#201008;border:1px solid #805010;}
.b-new   {color:#40ff90;background:#052010;border:1px solid #20a050;}
.sec-lbl{font-size:11px;font-weight:700;color:#9090b8;letter-spacing:3px;margin-bottom:6px;margin-top:4px;font-family:'JetBrains Mono',monospace;text-transform:uppercase;}
.stat-box{background:#0d1424;border:1px solid #253050;border-radius:8px;padding:11px 8px;text-align:center;}
.stat-val{font-size:26px;font-weight:700;line-height:1.1;font-family:'JetBrains Mono',monospace;}
.stat-lbl{font-size:10px;font-weight:700;color:#9090b8;letter-spacing:2px;margin-top:5px;font-family:'JetBrains Mono',monospace;}
.c-w{color:#e8eef8;}.c-g{color:#40d4c0;}.c-r{color:#ff8080;}.c-a{color:#ffb840;}.c-b{color:#80b0ff;}.c-nw{color:#40ff90;}
.t-head{display:grid;grid-template-columns:1.1fr 120px 65px 115px 70px 90px 100px;padding:10px 16px;background:#090e1e;border:1px solid #253050;border-bottom:2px solid #3050a0;border-radius:8px 8px 0 0;gap:8px;}
.t-head span{font-size:11px;font-weight:700;color:#b0c4e8;letter-spacing:2px;font-family:'JetBrains Mono',monospace;}
.t-body{border:1px solid #253050;border-top:none;border-radius:0 0 8px 8px;overflow:hidden;max-height:460px;overflow-y:auto;}
.t-row{display:grid;grid-template-columns:1.1fr 120px 65px 115px 70px 90px 100px;align-items:center;padding:10px 16px;border-bottom:1px solid #111e34;background:#0b1020;gap:8px;animation:fadeIn .3s ease;}
@keyframes fadeIn{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
.t-row:nth-child(even){background:#0d1428;}
.t-row:hover{background:#14203c;}
.t-row:last-child{border-bottom:none;}
.sym{font-size:14px;font-weight:700;color:#e8eef8;font-family:'JetBrains Mono',monospace;}
.cell{font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace;}
.cell-dim{color:#6080a8;}.cell-g{color:#40d4c0;}.cell-r{color:#ff8080;}.cell-w{color:#e8eef8;}.cell-a{color:#ffb840;}.cell-nw{color:#40ff90;}
.t-empty{padding:36px;text-align:center;color:#4060a0;font-size:13px;font-weight:600;background:#0b1020;font-family:'JetBrains Mono',monospace;}
.t-foot{padding:8px 16px;background:#080c18;border:1px solid #253050;border-top:1px solid #1a2840;border-radius:0 0 8px 8px;font-size:11px;font-weight:600;color:#6080a8;font-family:'JetBrains Mono',monospace;display:flex;justify-content:space-between;}
.log-wrap{background:#070b14;border:1px solid #253050;border-radius:8px;overflow:hidden;}
.log-hdr{padding:8px 14px;background:#090e1e;border-bottom:1px solid #253050;font-size:11px;font-weight:700;color:#9090b8;letter-spacing:2px;font-family:'JetBrains Mono',monospace;display:flex;justify-content:space-between;}
.log-body{padding:8px 14px;font-size:11px;line-height:2;max-height:130px;overflow-y:auto;font-family:'JetBrains Mono',monospace;}
.log-dim{color:#4a6080;}.log-ok{color:#40d4c0;}.log-err{color:#ff7070;}.log-done{color:#80b0ff;}.log-warn{color:#ffb840;}.log-new{color:#40ff90;}
.prog-label{font-size:12px;font-weight:600;color:#9090b8;font-family:'JetBrains Mono',monospace;margin-bottom:4px;}
.prog-sym{font-size:11px;color:#506080;margin-top:3px;font-family:'JetBrains Mono',monospace;}
div.stButton>button{font-family:'JetBrains Mono',monospace!important;font-weight:700!important;font-size:13px!important;letter-spacing:1px!important;}
div.stRadio label{font-family:'JetBrains Mono',monospace!important;font-size:12px!important;font-weight:600!important;color:#a0b4d8!important;}
.stProgress>div>div>div>div{background:linear-gradient(90deg,#20a050,#1a5cff)!important;}
hr{border-color:#253050!important;margin:10px 0!important;}
::-webkit-scrollbar{width:3px;}
::-webkit-scrollbar-track{background:#070b14;}
::-webkit-scrollbar-thumb{background:#253050;border-radius:2px;}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# FVG DETECTION
# ══════════════════════════════════════════════════════════
def find_fvgs(candles, lookback):
    """
    4-condition FVG with pullback confirmation.
    Candle tuple: (open, high, low, close, volume, timestamp)
    """
    window = candles[-(lookback + 6):] if len(candles) > lookback + 6 else candles
    n      = len(window)
    found  = []

    for i in range(n - 2):
        c1, c2, c3 = window[i], window[i+1], window[i+2]
        o1, h1, l1, cl1 = c1[0], c1[1], c1[2], c1[3]
        o2, h2, l2, cl2 = c2[0], c2[1], c2[2], c2[3]
        o3, h3, l3, cl3 = c3[0], c3[1], c3[2], c3[3]

        fvg_type = gap_low = gap_high = None

        # ── Bullish FVG ──
        # C2.open < C1.close  — C2 opens inside C1 body
        # C2.low  > C1.low    — C2 bottom above C1 bottom
        # C2.high > C1.high   — C2 top above C1 top
        # C3.low  > C2.low    — C3 bottom above C2 bottom
        # C3.high > C2.high   — C3 top above C2 top
        # C1.high < C3.low    — THE GAP zone
        if (o2 < cl1 and
                l2 > l1 and h2 > h1 and
                l3 > l2 and h3 > h2 and
                h1 < l3):
            fvg_type = "BULLISH"
            gap_low, gap_high = h1, l3

        # ── Bearish FVG ──
        # C2.open > C1.close  — C2 opens inside C1 body
        # C2.high < C1.high   — C2 top below C1 top
        # C2.low  < C1.low    — C2 bottom below C1 bottom
        # C3.high < C2.high   — C3 top below C2 top
        # C3.low  < C2.low    — C3 bottom below C2 bottom
        # C1.low  > C3.high   — THE GAP zone
        elif (o2 > cl1 and
                h2 < h1 and l2 < l1 and
                h3 < h2 and l3 < l2 and
                l1 > h3):
            fvg_type = "BEARISH"
            gap_low, gap_high = h3, l1

        if fvg_type is None:
            continue

        gap_size      = round((gap_high - gap_low) / gap_low * 100, 4) if gap_low else 0
        candles_after = window[i+3:]
        if not candles_after:
            continue

        # ── Pullback + validity check ──
        pullback_occurred = False
        invalidated       = False
        pullback_candle   = None

        for j, cf in enumerate(candles_after):
            hf, lf = cf[1], cf[2]
            if fvg_type == "BULLISH":
                if hf > h3:
                    invalidated = True; break
                if lf <= gap_high and not pullback_occurred:
                    pullback_occurred = True
                    pullback_candle   = j + 1
            else:
                if lf < l3:
                    invalidated = True; break
                if hf >= gap_low and not pullback_occurred:
                    pullback_occurred = True
                    pullback_candle   = j + 1

        if not pullback_occurred or invalidated:
            continue

        found.append({
            "fvg_type":       fvg_type,
            "gap_low":        round(gap_low,  6),
            "gap_high":       round(gap_high, 6),
            "gap_size":       gap_size,
            "pullback_after": pullback_candle,
            "c3_high":        round(h3, 6),
            "c3_low":         round(l3, 6),
            "candles_ago":    (n - 1) - (i + 2),
            "timestamp":      c3[5] if len(c3) > 5 else None,
        })

    found.sort(key=lambda x: -(x["timestamp"] or 0))
    return found


# ══════════════════════════════════════════════════════════
# OKX FETCHERS  (works in India — no geo-block)
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
    okx_bar = OKX_INTERVAL.get(interval, "1H")
    params  = {"instId": symbol, "bar": okx_bar, "limit": str(limit + 6)}
    r = requests.get(f"{BASE_URL}/api/v5/market/candles", params=params, timeout=10)
    if r.status_code == 429:
        time.sleep(3)
        r = requests.get(f"{BASE_URL}/api/v5/market/candles", params=params, timeout=10)
    r.raise_for_status()
    raw = list(reversed(r.json()["data"]))
    # drop current incomplete candle (confirm == "0")
    if raw and raw[-1][8] == "0":
        raw = raw[:-1]
    # OKX format: [ts, open, high, low, close, vol, ...]
    return [(float(c[1]), float(c[2]), float(c[3]),
             float(c[4]), float(c[5]), int(c[0])) for c in raw]


# ══════════════════════════════════════════════════════════
# RENDER HELPERS
# ══════════════════════════════════════════════════════════
def fmt_date(ts_ms):
    if not ts_ms: return "—"
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")

def render_table(results):
    if not results:
        return '<div class="t-body"><div class="t-empty">No setups found yet — press ▶ SCAN NOW</div></div>'
    html = ""
    for r in results:
        is_bull    = r["fvg_type"] == "BULLISH"
        type_badge = ('<span class="badge b-green">▲ Bullish</span>'
                      if is_bull else '<span class="badge b-red">▼ Bearish</span>')
        size_cls   = "cell-g" if is_bull else "cell-r"
        inv_lbl    = "C3.high" if is_bull else "C3.low"
        inv_val    = r["c3_high"] if is_bull else r["c3_low"]
        ago        = r["candles_ago"]
        ago_str    = "just formed" if ago == 0 else f"{ago}C ago"
        html += f"""<div class="t-row">
            <span class="sym">{r['symbol']}</span>
            {type_badge}
            <span class="cell cell-dim">{fmt_date(r['timestamp'])}</span>
            <span class="cell cell-w">{r['gap_low']:,.4f}–{r['gap_high']:,.4f}</span>
            <span class="cell {size_cls}">{r['gap_size']:.4f}%</span>
            <span class="cell cell-nw">+{r['pullback_after']}C</span>
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
  <span style="font-size:14px;color:#20a050;">◆</span>
  <span class="pg-title">FVG PULLBACK SCANNER</span>
  <span class="badge b-purple">OKX</span>
  <span class="badge b-blue">USDT SWAP</span>
  <span class="badge b-new">PULLBACK CONFIRMED</span>
</div>
<div class="pg-sub">
  Bullish: C2.open&lt;C1.close · C2.low&gt;C1.low · C2.high&gt;C1.high · C3.low&gt;C2.low · C3.high&gt;C2.high · C1.high&lt;C3.low &nbsp;|&nbsp;
  Bearish: reverse
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# Load symbols once
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
                  label_visibility="collapsed", disabled=st.session_state.scanning)
with c2:
    st.markdown('<div class="sec-lbl">Lookback (completed candles)</div>', unsafe_allow_html=True)
    lb = st.radio("lb", LOOKBACKS, horizontal=True,
                  label_visibility="collapsed", disabled=st.session_state.scanning)
with c3:
    st.markdown('<div class="sec-lbl">&nbsp;</div>', unsafe_allow_html=True)
    scan_clicked = st.button(
        "■  STOP" if st.session_state.scanning else "▶  SCAN NOW",
        use_container_width=True, type="primary"
    )

st.markdown("---")

# ── Stat boxes ──
sb1,sb2,sb3,sb4,sb5,sb6 = st.columns(6)
stat_scanned = sb1.empty(); stat_total = sb2.empty()
stat_bull    = sb3.empty(); stat_bear  = sb4.empty()
stat_setups  = sb5.empty(); stat_eta   = sb6.empty()

def render_stats(scanned=0, total=0, bull=0, bear=0, setups=0, eta="—"):
    stat_scanned.markdown(f'<div class="stat-box"><div class="stat-val c-w">{scanned}</div><div class="stat-lbl">SCANNED</div></div>', unsafe_allow_html=True)
    stat_total.markdown(  f'<div class="stat-box"><div class="stat-val c-w">{total or "—"}</div><div class="stat-lbl">TOTAL</div></div>', unsafe_allow_html=True)
    stat_bull.markdown(   f'<div class="stat-box"><div class="stat-val c-g">{bull}</div><div class="stat-lbl">BULL SETUPS</div></div>', unsafe_allow_html=True)
    stat_bear.markdown(   f'<div class="stat-box"><div class="stat-val c-r">{bear}</div><div class="stat-lbl">BEAR SETUPS</div></div>', unsafe_allow_html=True)
    stat_setups.markdown( f'<div class="stat-box"><div class="stat-val c-nw">{setups}</div><div class="stat-lbl">TOTAL SETUPS</div></div>', unsafe_allow_html=True)
    stat_eta.markdown(    f'<div class="stat-box"><div class="stat-val c-b">{eta}</div><div class="stat-lbl">ETA (SEC)</div></div>', unsafe_allow_html=True)

render_stats()

prog_label_el = st.empty()
prog_bar_el   = st.empty()
prog_sym_el   = st.empty()

st.markdown('<div class="sec-lbl" style="margin-top:14px;">Detected Setups — Live Results</div>', unsafe_allow_html=True)
t_head_el = st.empty()
t_body_el = st.empty()
t_foot_el = st.empty()

t_head_el.markdown("""<div class="t-head">
  <span>Symbol</span><span>Type</span><span>TF</span>
  <span>Gap Zone</span><span>Gap %</span>
  <span>Pullback</span><span>Invalidation</span>
</div>""", unsafe_allow_html=True)

t_body_el.markdown(
    '<div class="t-body"><div class="t-empty">Select timeframe & lookback then press ▶ SCAN NOW</div></div>',
    unsafe_allow_html=True
)

st.markdown('<div class="sec-lbl" style="margin-top:14px;">Activity Log</div>', unsafe_allow_html=True)
log_el = st.empty()
log_el.markdown("""<div class="log-wrap">
  <div class="log-hdr"><span>SCAN LOG</span><span>0 events</span></div>
  <div class="log-body"><div class="log-dim" style="opacity:.4;">Waiting to start...</div></div>
</div>""", unsafe_allow_html=True)

st.markdown("""
<div style="display:flex;flex-wrap:wrap;gap:14px;margin-top:8px;
     font-size:11px;font-weight:600;color:#4060a0;font-family:'JetBrains Mono',monospace;">
  <span><span style="color:#40d4c0;">▲ Bullish</span> — C2 engulfs C1 + gap into C3 + pullback into zone, C3.high intact</span>
  <span><span style="color:#ff8080;">▼ Bearish</span> — C2 engulfs C1 + gap into C3 + pullback into zone, C3.low intact</span>
  <span style="margin-left:auto;">8 symbols/batch · 200ms delay · OKX public API</span>
</div>
""", unsafe_allow_html=True)


# ── Helpers ──
def update_table(results):
    t_body_el.markdown(render_table(results), unsafe_allow_html=True)
    n    = len(results)
    bull = sum(1 for r in results if r["fvg_type"] == "BULLISH")
    bear = sum(1 for r in results if r["fvg_type"] == "BEARISH")
    t_foot_el.markdown(
        f'<div class="t-foot"><span>{n} setup{"s" if n!=1 else ""}'
        f' · {bull} bullish · {bear} bearish</span>'
        f'<span>{tf.upper()} · lookback {lb}</span></div>',
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
if scan_clicked and not st.session_state.scanning:
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

    log(f"⏳  Scanning {total} symbols · {tf.upper()} · lookback {lb}", "dim")
    render_stats(0, total, 0, 0, 0, "—")
    prog_label_el.markdown(
        '<div class="prog-label">Scanning OKX USDT perpetual swaps...</div>',
        unsafe_allow_html=True
    )
    bar     = prog_bar_el.progress(0)
    scanned = 0
    bull_ct = 0
    bear_ct = 0

    try:
        for i in range(0, total, BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as ex:
                futures = {ex.submit(fetch_klines, s, tf, lb): s for s in batch}
                for fut in as_completed(futures):
                    sym_name = futures[fut]
                    try:
                        candles = fut.result()
                        fvgs    = find_fvgs(candles, lb)
                        for fvg in fvgs:
                            fvg["symbol"] = sym_name
                            fvg["tf"]     = tf
                            results.append(fvg)
                            is_bull = fvg["fvg_type"] == "BULLISH"
                            if is_bull: bull_ct += 1
                            else:       bear_ct += 1
                            arrow = "▲" if is_bull else "▼"
                            log(
                                f"{arrow}  {sym_name}  {fvg['gap_size']:.4f}%"
                                f"  [+{fvg['pullback_after']}C pullback]"
                                f"  {fvg['gap_low']:,.4f}–{fvg['gap_high']:,.4f}",
                                "new"
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
                        f'<div class="prog-sym">Now: <b style="color:#8090b8;">{sym_name}</b>'
                        f' &nbsp;·&nbsp; ETA: {eta}s</div>',
                        unsafe_allow_html=True
                    )
            if i + BATCH_SIZE < total:
                time.sleep(DELAY_SEC)

        elapsed = round(time.time() - scan_start, 1)
        bar.progress(100)
        render_stats(total, total, bull_ct, bear_ct, len(results), 0)
        prog_label_el.markdown(
            f'<div class="prog-label" style="color:#40ff90;">'
            f'✅  Complete · {len(results)} setup(s) · {elapsed}s</div>',
            unsafe_allow_html=True
        )
        prog_sym_el.empty()
        log(f"🏁  Done · {len(results)} setup(s) · {bull_ct}▲ {bear_ct}▼ · {elapsed}s", "done")
        if not results:
            t_body_el.markdown(
                '<div class="t-body"><div class="t-empty">'
                'No setups found. Try wider lookback or different timeframe.'
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

elif st.session_state.scan_done and st.session_state.results:
    r    = st.session_state.results
    bull = sum(1 for x in r if x["fvg_type"] == "BULLISH")
    bear = sum(1 for x in r if x["fvg_type"] == "BEARISH")
    update_table(r)
    update_log(st.session_state.log_lines)
    render_stats(len(r), len(st.session_state.symbol_list), bull, bear, len(r), 0)
