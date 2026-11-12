"""Visual language: dark real-time-serving console.

Mirrors the Lakehouse//RT demo app's serving views — near-black canvas, card
surfaces, mono numerics, and a fixed accent per metric (green p50, amber p95,
cyan throughput). Lakehouse//RT is red-accented, the comparison warehouse blue,
so the two engines stay visually distinct wherever they appear side by side.
"""

C = {
    "bg": "#0B0E14",
    "card": "#131822",
    "card2": "#0F141C",
    "line": "#1E2733",
    "line2": "#2A3644",
    "ink": "#E6EDF6",
    "muted": "#8B98A9",
    "dim": "#5A6675",
    "p50": "#3FE0A0",     # green   — median latency
    "p95": "#FFC745",     # amber   — tail latency
    "qps": "#5ADBFF",     # cyan    — throughput
    "rt": "#FF5C5C",      # red     — Lakehouse//RT
    "dw": "#4A9EFF",      # blue    — comparison warehouse
    "good": "#3FE0A0",
    "bad": "#FF5C5C",
}

MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace"

CSS = f"""
<style>
:root {{
  --bg:{C['bg']}; --card:{C['card']}; --card2:{C['card2']}; --line:{C['line']};
  --line2:{C['line2']}; --ink:{C['ink']}; --muted:{C['muted']}; --dim:{C['dim']};
  --p50:{C['p50']}; --p95:{C['p95']}; --qps:{C['qps']}; --rt:{C['rt']}; --dw:{C['dw']};
  --mono:{MONO};
}}
.stApp {{ background: var(--bg); }}
.block-container {{ padding-top: .8rem !important; padding-bottom: 5rem !important; max-width: 1680px; }}
header[data-testid="stHeader"] {{ background: transparent; height: 0; }}
#MainMenu, footer {{ visibility: hidden; }}
h1,h2,h3,h4 {{ color: var(--ink) !important; }}

/* ---- eyebrow / page head ---------------------------------------------- */
.rk-eyebrow {{
  font-size:.7rem; letter-spacing:.19em; text-transform:uppercase;
  color:var(--muted); font-weight:700; margin:.1rem 0 .9rem;
}}

/* ---- nav pill buttons -------------------------------------------------- */
div[data-testid="stHorizontalBlock"] .stButton > button {{
  border-radius:999px; font-size:.79rem; font-weight:640; padding:.38rem .5rem; width:100%;
}}
.stButton > button[kind="secondary"] {{
  background:var(--card) !important; color:var(--muted) !important;
  border:1px solid var(--line2) !important;
}}
.stButton > button[kind="secondary"]:hover {{ color:var(--ink) !important; border-color:var(--dim) !important; }}
.stButton > button[kind="primary"] {{
  background:var(--p50) !important; color:#04150E !important; border:1px solid var(--p50) !important;
  font-weight:700 !important;
}}
.stButton > button[kind="primary"]:hover {{ filter:brightness(1.08); }}

/* ---- window frame (macOS chrome) -------------------------------------- */
.rk-win {{ background:var(--card2); border:1px solid var(--line2); border-radius:12px; overflow:hidden; }}
.rk-win.rt {{ border-color:rgba(255,92,92,.55); box-shadow:0 0 0 1px rgba(255,92,92,.18), 0 0 34px rgba(255,92,92,.10); }}
.rk-win.dw {{ border-color:rgba(74,158,255,.40); box-shadow:0 0 0 1px rgba(74,158,255,.14); }}
.rk-bar {{
  display:flex; align-items:center; gap:.5rem; padding:.6rem .8rem;
  background:#0C1119; border-bottom:1px solid var(--line);
}}
.rk-dots {{ display:flex; gap:.32rem; margin-right:.35rem; }}
.rk-dot {{ width:10px; height:10px; border-radius:50%; }}
.rk-name {{ font-size:.83rem; font-weight:680; color:var(--ink); }}
.rk-name .accent {{ color:var(--rt); }}
.rk-state {{
  margin-left:auto; font-size:.62rem; letter-spacing:.13em; font-weight:750;
  padding:.16rem .48rem; border-radius:4px; background:#161D28; color:var(--dim);
}}
.rk-state.done {{ background:rgba(63,224,160,.13); color:var(--p50); }}
.rk-state.run {{ background:rgba(255,199,69,.13); color:var(--p95); }}
.rk-state.err {{ background:rgba(255,92,92,.13); color:var(--rt); }}

/* ---- end-to-end latency strip ---------------------------------------- */
.rk-e2e {{
  display:flex; align-items:center; gap:.55rem; margin:.7rem .75rem;
  padding:.55rem .75rem; background:var(--card); border:1px solid var(--line);
  border-radius:8px;
}}
.rk-e2e .lbl {{ font-size:.63rem; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); font-weight:700; }}
.rk-e2e .val {{ margin-left:auto; font-family:var(--mono); font-size:1.15rem; font-weight:700; }}

/* ---- KPI tiles inside a window --------------------------------------- */
.rk-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:.7rem; padding:0 .8rem .85rem; }}
.rk-tile {{
  background:var(--card); border:1px solid var(--line); border-radius:10px;
  padding:.7rem .8rem .55rem; min-height:186px; display:flex; flex-direction:column;
}}
.rk-tile .t {{
  font-size:.66rem; letter-spacing:.11em; text-transform:uppercase; color:#AEBACA;
  font-weight:700; display:flex; justify-content:space-between; align-items:center;
}}
.rk-tile .t .arw {{ color:var(--dim); font-size:.75rem }}
.rk-tile .s {{ font-size:.63rem; color:var(--dim); margin-bottom:.35rem; }}
.rk-tile .v {{ font-family:var(--mono); font-size:1.55rem; font-weight:700; line-height:1.1; }}
.rk-tile .v.pending {{ color:#39424F; }}
.rk-tile .ms {{ font-family:var(--mono); font-size:.6rem; color:var(--dim); margin-top:.1rem; }}
.rk-tile .chart {{ margin-top:auto; padding-top:.5rem; }}

/* dashed placeholder frame shown before any engine is revealed */
.rk-stage-empty {{
  border:1px dashed var(--line2); border-radius:12px; min-height:420px;
  display:flex; flex-direction:column; align-items:center; justify-content:center; gap:.3rem;
}}
.rk-stage-empty .h {{ font-size:.95rem; font-weight:700; color:var(--ink); }}
.rk-stage-empty .p {{ font-size:.82rem; color:var(--muted); }}

/* ---- HUD (floating metrics panel) ------------------------------------ */
.rk-hud {{
  background:rgba(13,18,26,.97); border:1px solid var(--line2); border-radius:12px;
  box-shadow:0 18px 50px rgba(0,0,0,.55); padding:0; margin:.4rem 0 0;
}}
.rk-hud .row {{ display:grid; grid-template-columns:1fr 1fr 1fr; }}
.rk-hud .cell {{ padding:.85rem 1.1rem; border-right:1px solid var(--line); }}
.rk-hud .cell:last-child {{ border-right:none; }}
.rk-tagline {{ display:flex; align-items:center; gap:.45rem; }}
.rk-badge {{
  font-family:var(--mono); font-size:.6rem; font-weight:800; padding:.14rem .42rem;
  border-radius:4px; letter-spacing:.06em;
}}
.rk-hud .cap {{ font-size:.64rem; letter-spacing:.13em; text-transform:uppercase; color:var(--muted); font-weight:700; }}
.rk-hud .sub {{ font-size:.66rem; color:var(--dim); margin:.12rem 0 .3rem; }}
.rk-hud .big {{ font-family:var(--mono); font-size:2.05rem; font-weight:700; letter-spacing:-.01em; }}
.rk-hud .users {{ padding:.8rem 1.1rem; border-top:1px solid var(--line); }}
.rk-hud .row2 {{ display:grid; grid-template-columns:1fr 1fr 1fr; border-top:1px solid var(--line); }}
.rk-hud .row2 .cell {{ padding:.6rem 1.1rem; }}
.rk-hud .row2 .big {{ font-size:1.15rem; font-weight:600; }}
.rk-hud .pend {{ color:var(--dim) !important; font-size:1.15rem !important; }}

/* ---- session grid (many small dashboards) ---------------------------- */
.rk-sessions {{
  display:grid; grid-template-columns:repeat(auto-fill,minmax(132px,1fr));
  gap:.5rem; margin:.2rem 0 .6rem;
}}
.rk-mini {{
  background:var(--card2); border:1px solid var(--line); border-radius:7px;
  padding:.3rem .34rem; opacity:.92;
}}
.rk-mini .h {{ display:flex; align-items:center; gap:.2rem; margin-bottom:.16rem; }}
.rk-mini .h i {{ width:5px; height:5px; border-radius:50%; background:var(--rt); display:inline-block; }}
.rk-mini .h span {{ font-size:.5rem; color:var(--dim); font-family:var(--mono); }}

div[data-testid="stMetric"] {{ background:var(--card); border:1px solid var(--line); border-radius:9px; padding:.6rem .7rem; }}
div[data-testid="stMetricValue"] {{ font-family:var(--mono); color:var(--ink) !important; }}
div[data-testid="stMetricLabel"] {{ color:var(--muted) !important; font-size:.72rem !important; }}
div[data-testid="stDataFrame"] {{ border:1px solid var(--line); border-radius:9px; }}
hr {{ border-color:var(--line) !important; }}
.stSlider label, .stCheckbox label, .stSelectbox label {{ color:var(--muted) !important; font-size:.78rem !important; }}

/* ---- architecture layers (dark) -------------------------------------- */
.rk-layer {{
  background:var(--card); border:1px solid var(--line); border-left:3px solid var(--qps);
  border-radius:10px; padding:.85rem 1.05rem; margin-bottom:.6rem;
}}
.rk-layer .rk-step {{ font-size:.62rem; letter-spacing:.13em; text-transform:uppercase; color:var(--qps); font-weight:750; }}
.rk-layer h4 {{ margin:.15rem 0 .1rem; font-size:1rem; font-weight:700; color:var(--ink); }}
.rk-layer .rk-tag {{ color:var(--p95); font-size:.8rem; font-weight:620; margin-bottom:.35rem; }}
.rk-layer p {{ margin:0 0 .45rem; color:var(--muted); font-size:.83rem; line-height:1.55; }}
.rk-chips {{ display:flex; gap:.3rem; flex-wrap:wrap; }}
.rk-chip {{
  font-size:.69rem; font-weight:620; padding:.18rem .5rem; border-radius:5px;
  background:#18202C; color:var(--muted); border:1px solid var(--line2);
}}
.rk-chip.accent {{ background:rgba(90,219,255,.10); color:var(--qps); border-color:rgba(90,219,255,.25); }}
/* A chip that links to the deployed asset. Underlined only on hover, so a row of
   chips still reads as chips rather than a wall of links. */
a.rk-chip {{ text-decoration:none; display:inline-block; cursor:pointer; }}
a.rk-chip::after {{ content:" \\2197"; opacity:.5; font-weight:500; }}
a.rk-chip:hover {{ border-color:var(--qps); color:var(--qps); text-decoration:underline; }}
.rk-card {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:1rem 1.1rem; margin-bottom:.8rem; }}
.rk-card h4 {{ margin:0 0 .3rem; font-size:.94rem; color:var(--ink); }}
.rk-card p {{ margin:0; color:var(--muted); font-size:.84rem; line-height:1.5; }}
</style>
"""

# Tab order. Architecture leads so the setup is framed before any numbers, and
# it is also the default view (see main()).
NAV_ITEMS = [
    ("architecture", "Architecture"),
    ("headtohead", "Head to head"),
    ("scale", "Scaled load"),
]
