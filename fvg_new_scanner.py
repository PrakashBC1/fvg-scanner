"""
=============================================================
  BINANCE USDT-M FUTURES — FVG Pullback Scanner
  Auto-schedules 5 min after each candle close
=============================================================
  Install : pip install streamlit requests
  Run     : streamlit run fvg_new_scanner.py
=============================================================

  SCAN SCHEDULE (5 min after candle close):
    1H  → every hour    at HH:05
    4H  → every 4 hours at 00:05, 04:05, 08:05, 12:05, 16:05, 20:05
    1D  → every day     at 00:05 UTC

  FVG LOGIC:
    Bullish : C2.close < C3.open  AND  C1.high < C3.low
    Bearish : C2.close > C3.open  AND  C1.low  > C3.high

  PULLBACK CONFIRMED:
    Bullish : price retraced into gap zone, C3.high not breached
    Bearish : price retraced into gap zone, C3.low  not breached

  DEDUPLICATION:
    Each scan cycle produces its own table.
    Results already seen in a previous table are filtered out
    — only fresh findings are shown in the latest table.
    All tables visible for 1 full day (24h), then cleared.
=============================================================
"""

import streamlit as st
import requests
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────
BASE_URL   = "https://api.binance.com/api/v3"
BATCH_SIZE = 8
DELAY_SEC  = 0.2
LOOKBACKS  = [5, 10, 15]

# Candle close times (UTC hours) + 5 min offset
TF_SCHEDULES = {
    "1h":  {"interval_h": 1,  "label": "Every hour (HH:05 UTC)"},
    "4h":  {"interval_h": 4,  "label": "Every 4h (00:05 04:05 08:05 12:05 16:05 20:05 UTC)"},
    "1d":  {"interval_h": 24, "label": "Every day (00:05 UTC)"},
}

RESULTS_TTL_HOURS = 24   # keep scan history for 24 hours

st.set_page_config(
    page_title="FVG Auto Scanner",
    page_icon="📡",
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
.b-gray  {color:#9090b0;background:#141420;border:1px solid #303050;}

.sec-lbl{font-size:11px;font-weight:700;color:#9090b8;letter-spacing:3px;margin-bottom:6px;margin-top:4px;font-family:'JetBrains Mono',monospace;text-transform:uppercase;}

/* Stat boxes */
.stat-box{background:#0d1424;border:1px solid #253050;border-radius:8px;padding:11px 8px;text-align:center;}
.stat-val{font-size:22px;font-weight:700;line-height:1.1;font-family:'JetBrains Mono',monospace;}
.stat-lbl{font-size:9px;font-weight:700;color:#9090b8;letter-spacing:2px;margin-top:4px;font-family:'JetBrains Mono',monospace;}
.c-w{color:#e8eef8;}.c-g{color:#40d4c0;}.c-r{color:#ff8080;}.c-a{color:#ffb840;}.c-b{color:#80b0ff;}.c-nw{color:#40ff90;}

/* Next scan card */
.next-card{background:#090e1e;border:1px solid #253050;border-radius:8px;padding:12px 16px;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;}
.next-label{font-size:11px;font-weight:700;color:#9090b8;letter-spacing:2px;font-family:'JetBrains Mono',monospace;}
.next-time{font-size:20px;font-weight:700;color:#40ff90;font-family:'JetBrains Mono',monospace;}
.next-sub{font-size:10px;color:#4060a0;font-family:'JetBrains Mono',monospace;}
.scanning-pill{font-size:11px;font-weight:700;color:#40ff90;background:#052010;border:1px solid #20a050;padding:4px 12px;border-radius:4px;animation:blink 1s infinite;font-family:'JetBrains Mono',monospace;}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.4}}

/* Scan table section */
.scan-section{margin-bottom:18px;}
.scan-hdr{display:flex;align-items:center;gap:10px;padding:10px 16px;background:#090e1e;border:1px solid #253050;border-bottom:2px solid #3050a0;border-radius:8px 8px 0 0;flex-wrap:wrap;}
.scan-hdr-title{font-size:13px;font-weight:700;color:#e8eef8;font-family:'JetBrains Mono',monospace;}
.scan-hdr-time{font-size:11px;color:#6080a8;font-family:'JetBrains Mono',monospace;}
.scan-hdr-count{font-size:11px;font-weight:700;font-family:'JetBrains Mono',monospace;}
.scan-hdr-fresh{color:#40ff90;}.scan-hdr-dupe{color:#6080a8;}

/* Results table */
.t-head{display:grid;grid-template-columns:1.2fr 125px 110px 75px 110px 110px;padding:8px 16px;background:#060911;border-left:1px solid #253050;border-right:1px solid #253050;gap:8px;}
.t-head span{font-size:10px;font-weight:700;color:#8090b0;letter-spacing:2px;font-family:'JetBrains Mono',monospace;}
.t-body{border:1px solid #253050;border-top:none;border-radius:0 0 8px 8px;overflow:hidden;}
.t-row{display:grid;grid-template-columns:1.2fr 125px 110px 75px 110px 110px;align-items:center;padding:9px 16px;border-bottom:1px solid #111e34;background:#0b1020;gap:8px;animation:fadeIn .3s ease;}
@keyframes fadeIn{from{opacity:0;transform:translateY(-3px)}to{opacity:1;transform:translateY(0)}}
.t-row:nth-child(even){background:#0d1428;}
.t-row:hover{background:#14203c;}
.t-row:last-child{border-bottom:none;}
.sym{font-size:14px;font-weight:700;color:#e8eef8;font-family:'JetBrains Mono',monospace;}
.cell{font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace;}
.cell-dim{color:#6080a8;}.cell-g{color:#40d4c0;}.cell-r{color:#ff8080;}.cell-w{color:#e8eef8;}.cell-a{color:#ffb840;}.cell-nw{color:#40ff90;}
.t-empty{padding:28px;text-align:center;color:#4060a0;font-size:12px;font-weight:600;background:#0b1020;font-family:'JetBrains Mono',monospace;}

/* Log */
.log-wrap{background:#070b14;border:1px solid #253050;border-radius:8px;overflow:hidden;}
.log-hdr{padding:7px 14px;background:#090e1e;border-bottom:1px solid #253050;font-size:11px;font-weight:700;color:#9090b8;letter-spacing:2px;font-family:'JetBrains Mono',monospace;display:flex;justify-content:space-between;}
.log-body{padding:8px 14px;font-size:11px;line-height:2;max-height:120px;overflow-y:auto;font-family:'JetBrains Mono',monospace;}
.log-dim{color:#4a6080;}.log-ok{color:#40d4c0;}.log-err{color:#ff7070;}.log-done{color:#80b0ff;}.log-warn{color:#ffb840;}.log-new{color:#40ff90;}

.prog-label{font-size:12px;font-weight:600;color:#9090b8;font-family:'JetBrains Mono',monospace;margin-bottom:4px;}
.prog-sym{font-size:11px;color:#506080;margin-top:2px;font-family:'JetBrains Mono',monospace;}

div.stButton>button{font-family:'JetBrains Mono',monospace!important;font-weight:700!important;font-size:12px!important;letter-spacing:1px!important;}
div.stRadio label{font-family:'JetBrains Mono',monospace!important;font-size:12px!important;font-weight:600!important;color:#a0b4d8!important;}
.stProgress>div>div>div>div{background:linear-gradient(90deg,#20a050,#1a5cff)!important;}
hr{border-color:#253050!important;margin:10px 0!important;}
::-webkit-scrollbar{width:3px;}
::-webkit-scrollbar-track{background:#070b14;}
::-webkit-scrollbar-thumb{background:#253050;border-radius:2px;}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# SCHEDULE HELPERS
# ══════════════════════════════════════════════════════════
def next_scan_time(tf):
    """
    Returns the next UTC datetime when a scan should fire
    (5 minutes after each candle close).
    """
    now = datetime.now(timezone.utc)
    interval_h = TF_SCHEDULES[tf]["interval_h"]

    if interval_h >= 24:
        # Daily: next 00:05 UTC
        target = now.replace(hour=0, minute=5, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target

    # Hourly / 4-hourly
    # Find how many intervals have elapsed since midnight
    minutes_since_midnight = now.hour * 60 + now.minute
    interval_mins = interval_h * 60
    intervals_elapsed = minutes_since_midnight // interval_mins
    # Next interval start (in hours from midnight)
    next_interval_start_h = (intervals_elapsed + 1) * interval_h
    scan_minute = 5   # 5 min after close

    target = now.replace(
        hour=next_interval_start_h % 24,
        minute=scan_minute,
        second=0, microsecond=0
    )
    # If next interval crosses midnight
    if next_interval_start_h >= 24:
        target += timedelta(days=1)
    if target <= now:
        target += timedelta(hours=interval_h)
    return target


def seconds_until(dt):
    now = datetime.now(timezone.utc)
    diff = (dt - now).total_seconds()
    return max(0, diff)


def fmt_countdown(secs):
    secs = int(secs)
    if secs <= 0:
        return "00:00:00"
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def is_scan_due(tf, last_scan_ts):
    """Returns True if a scan should fire now (5 min after candle close)."""
    now = datetime.now(timezone.utc)
    interval_h = TF_SCHEDULES[tf]["interval_h"]
    interval_m = interval_h * 60

    # How many minutes since midnight UTC
    mins_since_midnight = now.hour * 60 + now.minute

    # Are we in the 5-min window after a candle close?
    # Candles close at 0, interval_m, 2*interval_m, ... minutes since midnight
    remainder = mins_since_midnight % interval_m
    in_window = (remainder == 5) or (remainder == 6)   # 5-6 min window

    if not in_window:
        return False

    # Don't fire twice in the same window
    if last_scan_ts:
        mins_since_last = (now - last_scan_ts).total_seconds() / 60
        if mins_since_last < interval_m - 1:
            return False

    return True


# ══════════════════════════════════════════════════════════
# FVG DETECTION
# ══════════════════════════════════════════════════════════
def find_fvg_pullbacks(candles, lookback):
    window = candles[-(lookback + 6):] if len(candles) > lookback + 6 else candles
    n      = len(window)
    found  = []

    for i in range(n - 2):
        c1, c2, c3 = window[i], window[i+1], window[i+2]
        o1,h1,l1,cl1 = c1[0],c1[1],c1[2],c1[3]
        o2,h2,l2,cl2 = c2[0],c2[1],c2[2],c2[3]
        o3,h3,l3,cl3 = c3[0],c3[1],c3[2],c3[3]

        fvg_type = gap_low = gap_high = None

        if cl2 < o3 and h1 < l3:
            fvg_type = "BULLISH"
            gap_low, gap_high = h1, l3
        elif cl2 > o3 and l1 > h3:
            fvg_type = "BEARISH"
            gap_low, gap_high = h3, l1

        if fvg_type is None:
            continue

        gap_size      = round((gap_high - gap_low) / gap_low * 100, 4) if gap_low else 0
        candles_after = window[i+3:]
        if not candles_after:
            continue

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
            "fvg_type":      fvg_type,
            "gap_low":       round(gap_low,  6),
            "gap_high":      round(gap_high, 6),
            "gap_size":      gap_size,
            "pullback_after": pullback_candle,
            "c3_high":       round(h3, 6),
            "c3_low":        round(l3, 6),
            "timestamp":     c3[5] if len(c3) > 5 else None,
        })

    found.sort(key=lambda x: -(x["timestamp"] or 0))
    return found


# ══════════════════════════════════════════════════════════
# BINANCE FETCHERS
# ══════════════════════════════════════════════════════════
def fetch_futures_symbols():
    r = requests.get(f"{BASE_URL}/exchangeInfo", timeout=10)
    r.raise_for_status()
    return sorted([
        s["symbol"] for s in r.json()["symbols"]
        if s["status"] == "TRADING"
        and s["quoteAsset"] == "USDT" and s.get("isSpotTradingAllowed", True)
    ])

def fetch_klines(symbol, interval, limit):
    r = requests.get(
        f"{BASE_URL}/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit + 6},
        timeout=10
    )
    if r.status_code == 429:
        time.sleep(3)
        r = requests.get(
            f"{BASE_URL}/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit + 6},
            timeout=10
        )
    r.raise_for_status()
    raw = r.json()[:-1]
    return [(float(c[1]),float(c[2]),float(c[3]),float(c[4]),float(c[5]),int(c[0])) for c in raw]


# ══════════════════════════════════════════════════════════
# DEDUPLICATION
# ══════════════════════════════════════════════════════════
def make_key(r):
    """Unique key for a result — symbol + type + gap zone."""
    return f"{r['symbol']}|{r['fvg_type']}|{r['gap_low']}|{r['gap_high']}"

def deduplicate(new_results, all_previous_results):
    """
    Return only results NOT seen in any previous scan table.
    """
    seen = set()
    for prev_list in all_previous_results:
        for r in prev_list:
            seen.add(make_key(r))
    fresh = [r for r in new_results if make_key(r) not in seen]
    return fresh


# ══════════════════════════════════════════════════════════
# RENDER HELPERS
# ══════════════════════════════════════════════════════════
def fmt_date(ts_ms):
    if not ts_ms: return "—"
    return datetime.utcfromtimestamp(ts_ms/1000).strftime("%Y-%m-%d %H:%M")

def render_table_html(results):
    if not results:
        return '<div class="t-body"><div class="t-empty">No setups detected in this scan</div></div>'
    html = ""
    for r in results:
        is_bull = r["fvg_type"] == "BULLISH"
        type_badge = (
            '<span class="badge b-green">▲ Bullish</span>'
            if is_bull else
            '<span class="badge b-red">▼ Bearish</span>'
        )
        size_cls = "cell-g" if is_bull else "cell-r"
        inv      = f"{r['c3_high']:,.4f}" if is_bull else f"{r['c3_low']:,.4f}"
        inv_lbl  = "C3.high" if is_bull else "C3.low"
        html += f"""<div class="t-row">
            <span class="sym">{r['symbol']}</span>
            {type_badge}
            <span class="cell cell-dim">{fmt_date(r['timestamp'])}</span>
            <span class="cell {size_cls}">{r['gap_size']:.4f}%</span>
            <span class="cell cell-w">{r['gap_low']:,.4f}–{r['gap_high']:,.4f}</span>
            <span class="cell cell-a">{inv_lbl}: {inv}</span>
        </div>"""
    return f'<div class="t-body">{html}</div>'

def render_log(log_lines):
    if not log_lines:
        return '<div class="log-dim" style="opacity:.4;">Waiting for next candle close...</div>'
    return "".join(f'<div class="log-{lv}">{msg}</div>' for msg, lv in log_lines[-50:])


# ══════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════
for k, v in [
    ("symbol_list",   []),
    ("scan_history",  []),   # list of {ts, tf, lb, results, fresh_results}
    ("last_scan_ts",  None),
    ("scanning",      False),
    ("log_lines",     []),
    ("scan_count",    0),
]:
    if k not in st.session_state:
        st.session_state[k] = v


# ══════════════════════════════════════════════════════════
# PAGE LAYOUT
# ══════════════════════════════════════════════════════════
st.markdown("""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
  <span style="font-size:14px;color:#20a050;">◆</span>
  <span class="pg-title">FVG AUTO SCANNER</span>
  <span class="badge b-purple">SPOT USDT</span>
  <span class="badge b-blue">BINANCE</span>
  <span class="badge b-new">AUTO-SCHEDULED</span>
</div>
<div class="pg-sub">
  Fires 5 min after each candle close · 1H=hourly · 4H=every 4h · 1D=daily ·
  Results kept 24h · Each scan table shows only fresh findings
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# Load symbols once
if not st.session_state.symbol_list:
    with st.spinner("Loading symbol list..."):
        try:
            st.session_state.symbol_list = fetch_futures_symbols()
        except Exception as e:
            st.error(f"Failed to load symbols: {e}")
            st.stop()

# ── Controls ──
c1, c2, c3, c4 = st.columns([1.5, 1.5, 1.5, 1])
with c1:
    st.markdown('<div class="sec-lbl">Timeframe</div>', unsafe_allow_html=True)
    tf = st.radio("tf", ["1h","4h","1d"], horizontal=True,
                  label_visibility="collapsed",
                  disabled=st.session_state.scanning)
with c2:
    st.markdown('<div class="sec-lbl">Lookback (candles)</div>', unsafe_allow_html=True)
    lb = st.radio("lb", LOOKBACKS, horizontal=True,
                  label_visibility="collapsed",
                  disabled=st.session_state.scanning)
with c3:
    st.markdown('<div class="sec-lbl">Mode</div>', unsafe_allow_html=True)
    mode = st.radio("mode", ["Auto-schedule", "Manual"], horizontal=True,
                    label_visibility="collapsed",
                    disabled=st.session_state.scanning)
with c4:
    st.markdown('<div class="sec-lbl">&nbsp;</div>', unsafe_allow_html=True)
    manual_scan = st.button(
        "▶  SCAN NOW",
        use_container_width=True,
        type="primary",
        disabled=st.session_state.scanning
    )

st.markdown("---")

# ── Stat boxes ──
sb1,sb2,sb3,sb4,sb5,sb6 = st.columns(6)
stat_scans    = sb1.empty()
stat_total    = sb2.empty()
stat_bull     = sb3.empty()
stat_bear     = sb4.empty()
stat_fresh    = sb5.empty()
stat_next     = sb6.empty()

def render_stat_boxes(scans=0, total=0, bull=0, bear=0, fresh=0, next_str="—"):
    stat_scans.markdown( f'<div class="stat-box"><div class="stat-val c-w">{scans}</div><div class="stat-lbl">SCANS TODAY</div></div>', unsafe_allow_html=True)
    stat_total.markdown( f'<div class="stat-box"><div class="stat-val c-w">{total}</div><div class="stat-lbl">TOTAL HITS</div></div>',  unsafe_allow_html=True)
    stat_bull.markdown(  f'<div class="stat-box"><div class="stat-val c-g">{bull}</div><div class="stat-lbl">BULL SETUPS</div></div>',  unsafe_allow_html=True)
    stat_bear.markdown(  f'<div class="stat-box"><div class="stat-val c-r">{bear}</div><div class="stat-lbl">BEAR SETUPS</div></div>',  unsafe_allow_html=True)
    stat_fresh.markdown( f'<div class="stat-box"><div class="stat-val c-nw">{fresh}</div><div class="stat-lbl">FRESH ONLY</div></div>', unsafe_allow_html=True)
    stat_next.markdown(  f'<div class="stat-box"><div class="stat-val c-b" style="font-size:16px;">{next_str}</div><div class="stat-lbl">NEXT SCAN</div></div>', unsafe_allow_html=True)

render_stat_boxes()

# ── Progress ──
prog_label_el = st.empty()
prog_bar_el   = st.empty()
prog_sym_el   = st.empty()

# ── Next scan countdown ──
next_scan_el  = st.empty()

# ── Results section (one card per scan) ──
st.markdown('<div class="sec-lbl" style="margin-top:14px;">Scan History — Today\'s Results</div>', unsafe_allow_html=True)
results_el = st.empty()

# ── Log ──
st.markdown('<div class="sec-lbl" style="margin-top:14px;">Activity Log</div>', unsafe_allow_html=True)
log_el = st.empty()
log_el.markdown("""<div class="log-wrap">
  <div class="log-hdr"><span>SCAN LOG</span><span>0 events</span></div>
  <div class="log-body"><div class="log-dim" style="opacity:.4;">Waiting for next candle close...</div></div>
</div>""", unsafe_allow_html=True)

st.markdown("""
<div style="display:flex;flex-wrap:wrap;gap:14px;margin-top:8px;
     font-size:11px;font-weight:600;color:#4060a0;font-family:'JetBrains Mono',monospace;">
  <span><span style="color:#40d4c0;">▲ Bullish</span> — pullback into gap, C3.high intact</span>
  <span><span style="color:#ff8080;">▼ Bearish</span> — pullback into gap, C3.low intact</span>
  <span><span style="color:#40ff90;">◆ Fresh</span> — not seen in any previous scan today</span>
  <span style="margin-left:auto;">8/batch · 200ms delay · IP-safe</span>
</div>
""", unsafe_allow_html=True)


# ── Update helpers ──
def update_log(log_lines):
    log_el.markdown(f"""<div class="log-wrap">
      <div class="log-hdr"><span>SCAN LOG</span><span>{len(log_lines)} events</span></div>
      <div class="log-body">{render_log(log_lines)}</div>
    </div>""", unsafe_allow_html=True)

def refresh_results_display():
    """Rebuild the full results panel from scan_history."""
    history = st.session_state.scan_history
    if not history:
        results_el.markdown(
            '<div style="color:#4060a0;font-size:12px;font-family:JetBrains Mono,monospace;padding:8px 0;">'
            'No scans yet. Scanner will fire 5 min after each candle close.</div>',
            unsafe_allow_html=True
        )
        return

    html = ""
    for idx, scan in enumerate(reversed(history)):
        scan_n     = len(history) - idx
        fresh      = scan["fresh_results"]
        all_res    = scan["results"]
        n_fresh    = len(fresh)
        n_dupe     = len(all_res) - n_fresh
        scan_time  = scan["ts"].strftime("%Y-%m-%d %H:%M UTC")
        bull_f     = sum(1 for r in fresh if r["fvg_type"] == "BULLISH")
        bear_f     = sum(1 for r in fresh if r["fvg_type"] == "BEARISH")

        fresh_lbl = f'<span class="scan-hdr-count scan-hdr-fresh">◆ {n_fresh} fresh</span>'
        dupe_lbl  = (f'<span class="scan-hdr-count scan-hdr-dupe"> &nbsp;·&nbsp; {n_dupe} repeated (hidden)</span>'
                     if n_dupe > 0 else "")

        html += f"""<div class="scan-section">
          <div class="scan-hdr">
            <span class="scan-hdr-title">Scan #{scan_n} &nbsp;·&nbsp; {scan["tf"].upper()} &nbsp;·&nbsp; Lookback {scan["lb"]}</span>
            <span class="scan-hdr-time">{scan_time}</span>
            {fresh_lbl}{dupe_lbl}
            <span style="margin-left:auto;font-size:10px;color:#6080a8;font-family:JetBrains Mono,monospace;">
              ▲{bull_f} bull &nbsp; ▼{bear_f} bear
            </span>
          </div>
          <div class="t-head">
            <span>Symbol</span><span>Type</span>
            <span>Detected At</span><span>Gap Size</span>
            <span>Gap Zone</span><span>Invalidation</span>
          </div>
          {render_table_html(fresh)}
        </div>"""

    results_el.markdown(html, unsafe_allow_html=True)

def refresh_stats():
    history = st.session_state.scan_history
    total   = sum(len(s["fresh_results"]) for s in history)
    bull    = sum(sum(1 for r in s["fresh_results"] if r["fvg_type"]=="BULLISH") for s in history)
    bear    = sum(sum(1 for r in s["fresh_results"] if r["fvg_type"]=="BEARISH") for s in history)
    fresh   = total
    nxt     = next_scan_time(tf)
    nxt_str = nxt.strftime("%H:%M")
    render_stat_boxes(len(history), total, bull, bear, fresh, nxt_str)


# ══════════════════════════════════════════════════════════
# CORE SCAN FUNCTION
# ══════════════════════════════════════════════════════════
def run_scan(tf_val, lb_val, log_lines):
    """Run one full scan. Returns list of all results."""
    symbols    = st.session_state.symbol_list
    total      = len(symbols)
    results    = []
    scan_start = time.time()
    _cnt       = [len(log_lines)]

    def log(msg, level="dim"):
        log_lines.append((msg, level))
        _cnt[0] += 1
        update_log(log_lines)

    log(f"🔄  Scan #{st.session_state.scan_count+1} · {tf_val.upper()} · lookback {lb_val} · {total} symbols", "done")
    prog_label_el.markdown(
        '<div class="prog-label">Scanning all Spot USDT pairs...</div>',
        unsafe_allow_html=True
    )
    bar     = prog_bar_el.progress(0)
    scanned = 0
    bull_ct = 0
    bear_ct = 0

    for i in range(0, total, BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as ex:
            futures = {ex.submit(fetch_klines, s, tf_val, lb_val): s for s in batch}
            for fut in as_completed(futures):
                sym_name = futures[fut]
                try:
                    candles = fut.result()
                    fvgs    = find_fvg_pullbacks(candles, lb_val)
                    for fvg in fvgs:
                        fvg["symbol"] = sym_name
                        fvg["tf"]     = tf_val
                        results.append(fvg)
                        is_bull = fvg["fvg_type"] == "BULLISH"
                        if is_bull: bull_ct += 1
                        else:       bear_ct += 1
                        arrow = "▲" if is_bull else "▼"
                        log(
                            f"{arrow}  {sym_name}  {fvg['gap_size']:.4f}%  "
                            f"[+{fvg['pullback_after']}C pullback]  "
                            f"{fvg['gap_low']:,.4f}–{fvg['gap_high']:,.4f}",
                            "new"
                        )
                except Exception as e:
                    if "429" in str(e):
                        log("⚠️  Rate limit — pausing 3s...", "warn")
                        time.sleep(3)

                scanned += 1
                elapsed  = time.time() - scan_start
                rate     = scanned / elapsed if elapsed > 0 else 1
                eta      = max(0, int((total - scanned) / rate))
                bar.progress(int(scanned / total * 100))
                prog_sym_el.markdown(
                    f'<div class="prog-sym">Now: <b style="color:#8090b8;">{sym_name}</b>'
                    f' &nbsp;·&nbsp; ETA: {eta}s</div>',
                    unsafe_allow_html=True
                )

        if i + BATCH_SIZE < total:
            time.sleep(DELAY_SEC)

    elapsed = round(time.time() - scan_start, 1)
    bar.progress(100)
    prog_sym_el.empty()
    prog_label_el.markdown(
        f'<div class="prog-label" style="color:#40ff90;">'
        f'✅  Scan complete · {len(results)} setup(s) · {elapsed}s</div>',
        unsafe_allow_html=True
    )
    log(f"🏁  Done · {len(results)} setup(s) · {bull_ct}↑ {bear_ct}↓ · {elapsed}s", "done")
    return results


def do_scan():
    """Execute scan, deduplicate, store in history, refresh display."""
    st.session_state.scanning = True
    log_lines = list(st.session_state.log_lines)

    # Purge scan history older than 24h
    cutoff = datetime.now(timezone.utc) - timedelta(hours=RESULTS_TTL_HOURS)
    st.session_state.scan_history = [
        s for s in st.session_state.scan_history
        if s["ts"] > cutoff
    ]

    all_results = run_scan(tf, lb, log_lines)

    # Deduplicate against all previous scans today
    prev_results = [s["results"] for s in st.session_state.scan_history]
    fresh = deduplicate(all_results, prev_results)

    # Store this scan
    st.session_state.scan_history.append({
        "ts":            datetime.now(timezone.utc),
        "tf":            tf,
        "lb":            lb,
        "results":       all_results,
        "fresh_results": fresh,
    })
    st.session_state.scan_count    += 1
    st.session_state.last_scan_ts   = datetime.now(timezone.utc)
    st.session_state.log_lines      = log_lines
    st.session_state.scanning       = False

    refresh_results_display()
    refresh_stats()


# ══════════════════════════════════════════════════════════
# MAIN EXECUTION
# ══════════════════════════════════════════════════════════

# Always restore display
refresh_results_display()
refresh_stats()

# Manual scan button
if manual_scan and not st.session_state.scanning:
    do_scan()

# Auto-schedule mode
elif mode == "Auto-schedule" and not st.session_state.scanning:
    nxt  = next_scan_time(tf)
    secs = seconds_until(nxt)

    next_scan_el.markdown(f"""
    <div class="next-card">
      <div>
        <div class="next-label">NEXT SCHEDULED SCAN</div>
        <div class="next-time" id="countdown">{fmt_countdown(secs)}</div>
        <div class="next-sub">{nxt.strftime("%Y-%m-%d %H:%M UTC")} &nbsp;·&nbsp; {TF_SCHEDULES[tf]["label"]}</div>
      </div>
      <div style="text-align:right;">
        <div class="next-label">TIMEFRAME</div>
        <div style="font-size:18px;font-weight:700;color:#e8eef8;font-family:'JetBrains Mono',monospace;">{tf.upper()}</div>
        <div class="next-sub">lookback {lb} candles</div>
      </div>
    </div>""", unsafe_allow_html=True)

    # Check if scan is due right now
    if is_scan_due(tf, st.session_state.last_scan_ts):
        do_scan()
        time.sleep(60)   # avoid double-firing
        st.rerun()
    else:
        # Sleep 30s then rerun to update countdown
        time.sleep(30)
        st.rerun()
