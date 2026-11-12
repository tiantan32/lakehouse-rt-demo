"""HTML builders for the head-to-head view: window frames and KPI tiles."""

from __future__ import annotations

import random
from demo_config import days_before
from theme import C


# --------------------------------------------------------------------------- #
# The six dashboard widgets. Each is one query, so a "dashboard load" is six
# statements — which is what makes the end-to-end number meaningful.
# --------------------------------------------------------------------------- #
def widget_specs(fact: str, monthly: str, anchor) -> list[dict]:
    """`anchor` is the last date in the data, so "last 90 days" never goes empty."""
    since = days_before(anchor, 90)
    return [
        {
            "key": "net_loss",
            "title": "Net chargeback loss",
            "sub": "last 90 days",
            "sql": f"SELECT SUM(net_loss_amount) AS v FROM {fact} "
                   f"WHERE chargeback_filing_date >= DATE '{since}'",
            "fmt": "usd",
        },
        {
            "key": "disputes",
            "title": "Disputes filed",
            "sub": "last 90 days",
            "sql": f"SELECT COUNT(*) AS v FROM {fact} "
                   f"WHERE chargeback_filing_date >= DATE '{since}'",
            "fmt": "int",
        },
        {
            "key": "at_risk",
            "title": "At deadline risk",
            "sub": "open, 5 days or less",
            "sql": f"SELECT COUNT(*) AS v FROM {fact} WHERE NOT is_closed AND is_deadline_at_risk",
            "fmt": "int",
        },
        {
            "key": "recovered",
            "title": "Recovered",
            "sub": "won representments",
            "sql": f"SELECT SUM(recovery_amount) AS v FROM {fact} WHERE is_won",
            "fmt": "usd",
        },
        {
            "key": "win_rate",
            "title": "Win rate",
            "sub": "closed cases",
            "sql": f"SELECT TRY_DIVIDE(SUM(CASE WHEN is_won THEN 1 ELSE 0 END), "
                   f"SUM(CASE WHEN is_closed THEN 1 ELSE 0 END)) AS v FROM {fact}",
            "fmt": "pct",
        },
        {
            "key": "days",
            "title": "Avg days to resolve",
            "sub": "closed cases",
            "sql": f"SELECT AVG(days_open) AS v FROM {fact} WHERE is_closed",
            "fmt": "num",
        },
    ]


def fmt_value(fmt: str, v) -> str:
    if v is None or v == "":
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if fmt == "usd":
        return f"${f:,.0f}" if abs(f) >= 1000 else f"${f:,.2f}"
    if fmt == "int":
        return f"{int(f):,}"
    if fmt == "pct":
        return f"{f:.1%}"
    return f"{f:,.1f}"


# --------------------------------------------------------------------------- #
_UID = [0]


def _uid() -> str:
    _UID[0] += 1
    return f"g{_UID[0]}"


def area_line(seed: int, w: int = 360, h: int = 62, color: str | None = None) -> str:
    """Line with a gradient fill underneath. Grey and flat-toned when idle."""
    r = random.Random(seed)
    n = 34
    pts = []
    v = r.uniform(0.35, 0.7)
    for _ in range(n):                     # random walk reads more like a metric
        v = min(0.95, max(0.12, v + r.uniform(-0.13, 0.13)))
        pts.append(v)
    step = w / (n - 1)
    poly = " ".join(f"{i*step:.1f},{h - p*h:.1f}" for i, p in enumerate(pts))
    col = color or C["dim"]
    gid = _uid()
    fill = (f'<defs><linearGradient id="{gid}" x1="0" y1="0" x2="0" y2="1">'
            f'<stop offset="0%" stop-color="{col}" stop-opacity="0.42"/>'
            f'<stop offset="100%" stop-color="{col}" stop-opacity="0"/></linearGradient></defs>'
            f'<path d="M0,{h} L {poly} L {w},{h} Z" fill="url(#{gid})"/>') if color else ""
    return (f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" preserveAspectRatio="none">'
            f'{fill}<polyline points="{poly}" fill="none" stroke="{col}" stroke-width="1.7" '
            f'stroke-linejoin="round" stroke-linecap="round"/></svg>')


def dense_bars(seed: int, w: int = 360, h: int = 62, color: str | None = None) -> str:
    """Dense thin bar strip, matching the demo app's transaction-count tiles."""
    r = random.Random(seed + 977)
    n = 34
    gap = w / n
    bw = gap * 0.55
    col = color or C["dim"]
    v = r.uniform(0.4, 0.7)
    bars = []
    for i in range(n):
        v = min(0.98, max(0.22, v + r.uniform(-0.1, 0.11)))
        bh = max(4.0, v * h)
        bars.append(f'<rect x="{i*gap:.1f}" y="{h-bh:.1f}" width="{bw:.1f}" height="{bh:.1f}" '
                    f'rx="1" fill="{col}"/>')
    return (f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" preserveAspectRatio="none">'
            f'{"".join(bars)}</svg>')


def fmt_latency(ms: float | None) -> str:
    if ms is None:
        return "—"
    return f"{ms/1000:.2f} s" if ms >= 1000 else f"{ms:,.0f} ms"


def window(name: str, engine: str, state: str, e2e_ms: float | None,
           tiles_html: str) -> str:
    """macOS-style window frame wrapping the six KPI tiles."""
    accent = C["rt"] if engine == "rt" else C["dw"]
    label = 'Lakehouse<span class="accent">//RT</span>' if engine == "rt" else name
    if state == "DONE":
        badge = (f'<span class="rk-state done" style="color:{accent};'
                 f'border:1px solid {accent}55;background:{accent}14">&#10003; DONE</span>')
    elif state == "RUNNING":
        badge = '<span class="rk-state run">RUNNING</span>'
    elif state == "WARMING":
        badge = '<span class="rk-state run">WARM-UP</span>'
    elif state == "ERROR":
        badge = '<span class="rk-state err">ERROR</span>'
    else:
        badge = '<span class="rk-state">IDLE</span>'
    # ONE quantity in this row, start to finish: CLIENT wall-clock end-to-end.
    #
    # It ticks up while the dashboard loads and lands on the final client total,
    # monotonically, freezing on its own last tick. It is never replaced with a
    # different measurement, because a headline that grows and then shrinks is
    # indistinguishable from a broken counter.
    #
    # Server-side engine duration is a DIFFERENT quantity -- it cannot tick at all
    # (query history settles ~2 s after a statement) -- so it is reported outside
    # this row, as its own labelled metric below the panels.
    if state == "WARMING":
        sub = f' <span style="color:{C["dim"]};font-weight:600"> · warm-up pass, not measured</span>'
        val_html = f'<span class="val" style="color:{C["dim"]}">&mdash;</span>'
    elif state == "RUNNING":
        sub = f' <span style="color:{C["dim"]};font-weight:600"> · loading</span>'
        val_html = f'<span class="val" style="color:{accent};opacity:.8">{fmt_latency(e2e_ms)}</span>'
    elif state == "DONE":
        sub = f' <span style="color:{C["dim"]};font-weight:600"> · client round-trip</span>'
        val_html = f'<span class="val" style="color:{accent}">{fmt_latency(e2e_ms)}</span>'
    else:
        sub = ""
        val_html = f'<span class="val" style="color:{C["dim"]}">&mdash;</span>'
    lat_col = accent if e2e_ms is not None else C["dim"]

    return (
        f'<div class="rk-win {engine}">'
        f'  <div class="rk-bar">'
        f'    <span class="rk-dots"><i class="rk-dot" style="background:#FF5F57"></i>'
        f'      <i class="rk-dot" style="background:#FEBC2E"></i>'
        f'      <i class="rk-dot" style="background:#28C840"></i></span>'
        f'    <span class="rk-dot" style="background:{accent}"></span>'
        f'    <span class="rk-name">{label}</span>{badge}'
        f'  </div>'
        f'  <div class="rk-e2e"><span class="lbl">&#9201; End-to-end latency{sub}</span>'
        f'    {val_html}</div>'
        f'  <div class="rk-grid">{tiles_html}</div>'
        f'</div>'
    )


def tile(spec: dict, value: str | None, ms: float | None, seed: int,
         engine: str) -> str:
    accent = C["rt"] if engine == "rt" else C["dw"]
    col = accent if value else None
    chart = area_line(seed, color=col) if seed % 2 == 0 else dense_bars(seed, color=col)
    if value:
        v = f'<div class="v" style="color:{accent}">{value}</div>'
    else:
        v = '<div class="v pending">—</div>'
    # Same clock as the headline: client ms, set once when the widget lands.
    m = (f'<div class="ms">{fmt_latency(ms)}</div>' if ms is not None
         else '<div class="ms">&nbsp;</div>')
    return (
        f'<div class="rk-tile"><div class="t">{spec["title"]} <span class="arw">&#8599;</span></div>'
        f'<div class="s">{spec["sub"]}</div>{v}{m}'
        f'<div class="chart">{chart}</div></div>'
    )
