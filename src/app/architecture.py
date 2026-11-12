"""Architecture view: the Lakehouse RT Demo as deployed.

Rendered as a dark, animated SVG flow: rounded stage cards joined by thin
connectors with a dot travelling along each one. The motion uses SVG
`<animateMotion><mpath/></animateMotion>` bound to the same `<path>` the
connector draws, so the dot can never drift off the line.

Nothing here is aspirational: every stage is a deployed artifact in the bundle,
and every number is labelled with where and when it was measured.
"""

from __future__ import annotations

import os

from demo_config import CATALOG, DBSQL_WAREHOUSE_ID, RT_WAREHOUSE_ID, SCHEMA
from theme import C, MONO

NODE_FILL = "#141A24"
NODE_EDGE = "#2A3644"
# Connectors read as structure, not decoration, so they are brighter and thicker
# than the node borders; dimmer wires disappeared against the card fill.
WIRE = "#8290A3"
WIRE_W = 2.2

# COLOUR = ASSET TYPE. One meaning per colour, used for every node and for the
# travelling dot on every connector.
TYPES = {
    "external":  ("#FFFFFF", "Job / client"),
    "gold":      ("#D4AF37", "Unity Catalog table"),
    "warehouse": ("#4A9EFF", "DBSQL Serverless"),
    "rt":        ("#FF5C5C", "Lakehouse//RT"),
    "consumer":  ("#5ADBFF", "App view"),
    "measured":  ("#FFC745", "Measurement / result"),
}


def T(key: str) -> str:
    """Colour for an asset type. KeyError here is a bug, not a fallback case."""
    return TYPES[key][0]


def legend_html() -> str:
    """Colour key, rendered above the diagram so the palette is self-documenting."""
    items = "".join(
        f'<span class="rk-chip" style="border-color:{c}33">'
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:2px;'
        f'background:{c};margin-right:.35rem;vertical-align:middle"></span>{label}</span>'
        for c, label in TYPES.values()
    )
    return f'<div class="rk-chips" style="margin:.1rem 0 .6rem">{items}</div>'


# Links to the deployed assets. Databricks Apps inject DATABRICKS_HOST, so the
# same code works in any workspace.
_HOST = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
if _HOST and not _HOST.startswith("http"):
    _HOST = "https://" + _HOST


def _tbl(name: str) -> str:
    return f"{_HOST}/explore/data/{CATALOG}/{SCHEMA}/{name}"


URLS = {
    "fact": _tbl("gold_dispute_case_fact"),
    "events": _tbl("gold_dispute_event_stream"),
    "monthly": _tbl("gold_merchant_dispute_monthly"),
    "schema": f"{_HOST}/explore/data/{CATALOG}/{SCHEMA}",
    "rt_warehouse": f"{_HOST}/sql/warehouses/{RT_WAREHOUSE_ID}",
    "dbsql_warehouse": f"{_HOST}/sql/warehouses/{DBSQL_WAREHOUSE_ID}",
    "rt_monitoring": f"{_HOST}/sql/warehouses/{RT_WAREHOUSE_ID}/monitoring",
}


def _node(x, y, w, h, title, sub="", accent=None, r=10):
    edge = accent or NODE_EDGE
    glow = (f'<rect x="{x-1}" y="{y-1}" width="{w+2}" height="{h+2}" rx="{r+1}" fill="none" '
            f'stroke="{accent}" stroke-width="1" opacity="0.22"/>') if accent else ""
    ty = y + (h / 2 - 3 if sub else h / 2 + 4)
    out = (
        f'{glow}<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{NODE_FILL}" '
        f'stroke="{edge}" stroke-width="{1.9 if accent else 1.3}"/>'
        f'<text x="{x + w/2}" y="{ty}" text-anchor="middle" font-size="12.5" font-weight="680" '
        f'fill="{C["ink"]}">{title}</text>'
    )
    if sub:
        out += (f'<text x="{x + w/2}" y="{y + h/2 + 13}" text-anchor="middle" font-size="10" '
                f'font-family="{MONO}" fill="{C["dim"]}">{sub}</text>')
    return out


def _wire(pid, d, color=None, dur="2.4s", delay="0s", width=None):
    """A connector plus a dot that rides the exact same path."""
    col = color or C["rt"]
    width = WIRE_W if width is None else width
    return (
        f'<path id="{pid}" d="{d}" fill="none" stroke="{WIRE}" stroke-width="{width}"/>'
        f'<circle r="3.1" fill="{col}" class="fdot" style="animation-delay:{delay}">'
        f'<animateMotion dur="{dur}" begin="{delay}" repeatCount="indefinite">'
        f'<mpath href="#{pid}"/></animateMotion></circle>'
    )


def _hwire(pid, x1, x2, y, color=None, **kw):
    """Straight horizontal connector. Used for every within-band hop."""
    return _wire(pid, f"M {x1} {y} L {x2} {y}", color, **kw)


def _vwire(pid, x, y1, y2, color=None, **kw):
    """Straight vertical connector. Used when two nodes are centre-aligned."""
    return _wire(pid, f"M {x} {y1} L {x} {y2}", color, **kw)


def _drop(pid, x1, y1, x2, y2, ymid, color=None, **kw):
    """Orthogonal drop: down, across at `ymid`, then down again.

    Only right angles, never a bezier. Several drops that share the same `ymid`
    overlap on that segment and read as one horizontal rail, which is what keeps
    a one-to-many fan legible instead of a bundle of crossing curves.
    """
    return _wire(pid, f"M {x1} {y1} L {x1} {ymid} L {x2} {ymid} L {x2} {y2}", color, **kw)


def _fork(pid, x1, y1, x2, y2, xmid, color=None, **kw):
    """Orthogonal fork: across, down at `xmid`, then across again."""
    return _wire(pid, f"M {x1} {y1} L {xmid} {y1} L {xmid} {y2} L {x2} {y2}", color, **kw)


def _route(pid, pts, color=None, **kw):
    """Explicit orthogonal polyline through `pts`. Every segment axis-aligned."""
    for (ax, ay), (bx, by) in zip(pts, pts[1:]):
        assert ax == bx or ay == by, f"{pid}: diagonal segment {(ax,ay)}->{(bx,by)}"
    return _wire(pid, "M " + " L ".join(f"{x} {y}" for x, y in pts), color, **kw)


def _elbow(pid, x1, y1, x2, y2, color=None, **kw):
    """Single right angle: horizontal to `x2`, then vertical to `y2`.

    Used to carry gold down to the metric view *around* the DBSQL warehouse bar —
    a straight vertical would run through it.
    """
    return _wire(pid, f"M {x1} {y1} L {x2} {y1} L {x2} {y2}", color, **kw)


def _band(y, h, label, color):
    return (
        f'<rect x="10" y="{y}" width="1380" height="{h}" rx="12" fill="{color}" opacity="0.045"/>'
        f'<rect x="10" y="{y}" width="1380" height="{h}" rx="12" fill="none" stroke="{color}" '
        f'stroke-opacity="0.16"/>'
        f'<text x="26" y="{y + 18}" font-size="9.5" font-weight="800" letter-spacing="1.6" '
        f'font-family="{MONO}" fill="{color}">{label}</text>'
    )


def diagram_svg() -> str:
    s = [
        '<svg viewBox="0 0 1400 790" width="100%" xmlns="http://www.w3.org/2000/svg" '
        f'style="max-width:100%;height:auto;font-family:system-ui,-apple-system,sans-serif;'
        f'background:{C["card2"]};border-radius:10px">',
        '<style>'
        '@keyframes fdotfade{0%{opacity:0}14%{opacity:1}86%{opacity:1}100%{opacity:0}}'
        '.fdot{animation:fdotfade 2.4s linear infinite}'
        '</style>',
    ]

    # Vertical rhythm. Bands never overlap and every node sits inside its band.
    #
    #   band 1  16-148   job + tables 72-128, rail y 56
    #   band 2 160-300   rail y 184, engines 214-270
    #   band 3 312-452   rail y 330, app views 366-422
    #   band 4 464-600   nodes 506-562
    #   band 5 612-748   nodes 654-710
    #
    # A dot takes the colour of the node it flows INTO, so colour encodes direction.

    # ---- Band 1: data ---------------------------------------------------- #
    s.append(_band(16, 132, "01 · DATA — synthetic end-state tables, no pipeline",
                   T("gold")))
    s.append(_node(34, 72, 230, 56, "Data job", "serverless · runs once",
                   accent=T("external")))
    tables = (
        (330, 330, "gold_dispute_case_fact", "18M rows · clustered merchant, date"),
        (690, 330, "gold_dispute_event_stream", "18M rows · clustered merchant"),
        (1050, 316, "gold_merchant_dispute_monthly", "~560K rows · merchant x month"),
    )
    for i, (x, w, title, sub) in enumerate(tables):
        s.append(_node(x, 72, w, 56, title, sub, accent=T("gold")))
        cx = x + w // 2
        s.append(_route(f"d{i}", [(264, 100), (296, 100), (296, 56), (cx, 56), (cx, 72)],
                        T("gold"), dur="2.4s", delay=f"{i * 0.3:.1f}s"))

    # ---- Band 2: engines ------------------------------------------------- #
    s.append(_band(160, 140, "02 · ENGINES — same tables, same SQL, two warehouses",
                   T("rt")))
    for i, (x, w, *_rest) in enumerate(tables):
        s.append(_vwire(f"t{i}", x + w // 2, 128, 184, T("measured"), dur="1.4s",
                        delay=f"{i * 0.2:.1f}s"))
    s.append(f'<path d="M 357 184 L 1208 184" fill="none" stroke="{WIRE}" '
             f'stroke-width="{WIRE_W}"/>')
    s.append(_node(34, 214, 646, 56, "DBSQL Serverless warehouse",
                   "the comparison engine · result cache disabled", accent=T("warehouse")))
    s.append(_node(720, 214, 646, 56, "Lakehouse//RT warehouse",
                   "SELECT only · autoscale adds executors (3 DBU/h each)", accent=T("rt")))
    s.append(_vwire("e1", 357, 184, 214, T("warehouse"), dur="1.4s", delay="0.1s"))
    s.append(_vwire("e2", 1043, 184, 214, T("rt"), dur="1.4s", delay="0.3s"))

    # ---- Band 3: the app ------------------------------------------------- #
    s.append(_band(312, 140, "03 · APP — one Databricks App, Statement Execution API",
                   T("consumer")))
    s.append(_node(34, 366, 646, 56, "Head to head",
                   "6 widgets · both engines in the same instant", accent=T("consumer")))
    s.append(_node(720, 366, 646, 56, "Scaled load",
                   "N users x 4 widgets / 10 s · open loop · RT only", accent=T("consumer")))
    s.append(_vwire("a1", 357, 366, 270, T("warehouse"), dur="1.6s", delay="0s"))
    s.append(_route("a2", [(560, 366), (560, 330), (900, 330), (900, 270)],
                    T("rt"), dur="2.0s", delay="0.3s"))
    s.append(_vwire("a3", 1043, 366, 270, T("rt"), dur="1.2s", delay="0.1s"))

    # ---- Band 4: measurement --------------------------------------------- #
    s.append(_band(464, 136, "04 · MEASURE — client clock and engine clock, never mixed",
                   T("measured")))
    meas = (
        ("client wall clock", "send to answer, in the app"),
        ("query history API", "is_final only · read 3 s behind"),
        ("engine p50 / p95", "warehouse time, no network"),
        ("QPS", "completions / trailing 15 s"),
    )
    for i, (title, sub) in enumerate(meas):
        x = 34 + i * 336
        s.append(_node(x, 506, 318, 56, title, sub,
                       accent=T("external") if i == 0 else T("measured")))
        if i:
            s.append(_hwire(f"m{i}", x - 18, x, 534, T("measured"), dur="1.4s",
                            delay=f"{i * 0.2:.1f}s"))

    # ---- Band 5: results ------------------------------------------------- #
    # Measured 2026-11-12, Small Lakehouse//RT warehouse, autoscale up to 96 DBU/h.
    # If the ladder or refresh in app.py changes, change these labels too.
    s.append(_band(612, 136, "05 · RESULT — Small Lakehouse//RT warehouse, measured 2026-11-12",
                   T("measured")))
    res = (
        ("1,000 users = 400 q/s", "engine p50 22-26 ms · 0 errors", T("measured")),
        ("2,000 users = 800 q/s", "engine p50 ~1.2 s · ~2% HTTP 503", T("rt")),
        ("not executor CPU", "~5 cores busy at 800 q/s", T("measured")),
        ("scale out", "more warehouses, not more DBUs", T("consumer")),
    )
    for i, (title, sub, col) in enumerate(res):
        x = 34 + i * 336
        s.append(_node(x, 654, 318, 56, title, sub, accent=col))
        if i:
            s.append(_hwire(f"r{i}", x - 18, x, 682, col, dur="1.4s",
                            delay=f"{i * 0.2:.1f}s"))

    s.append(f'<text x="700" y="774" text-anchor="middle" font-size="10" font-family="{MONO}" '
             f'fill="{C["dim"]}">every stage is a deployed artifact in this bundle · '
             f'Lakehouse//RT appears only on read paths because it is SELECT-only</text>')
    s.append("</svg>")
    return "".join(s)


LAYERS = [
    {
        "step": "01 · Data",
        "title": "Three synthetic gold tables, written once",
        "tag": "The end state of a dispute pipeline, without the pipeline.",
        "body": (
            "One serverless job generates corporate-card dispute data directly in its final "
            "shape: an 18M-row case fact, an 18M-row event stream and a merchant x month "
            "rollup, across 40,000 merchants and about 13 months of filings. All three are "
            "managed Delta, liquid-clustered on merchant_id and OPTIMIZEd, so a per-merchant "
            "lookup prunes to a few files. The data ends on the day it was generated, and the "
            "app anchors every time window on that day."
        ),
        "chips": [("gold_dispute_case_fact", URLS["fact"]),
                  ("gold_dispute_event_stream", URLS["events"]),
                  ("gold_merchant_dispute_monthly", URLS["monthly"]),
                  "managed Delta · liquid clustering", "~450 MB fact"],
    },
    {
        "step": "02 · Engines",
        "title": "Two warehouses, one copy of the data",
        "tag": "Nothing is extracted or copied for a serving tier.",
        "body": (
            "DBSQL Serverless and Lakehouse//RT both read the same Unity Catalog tables with "
            "the same SQL. Lakehouse//RT is SELECT-only and built for many small concurrent "
            "queries. Its autoscaling adds executors, 3 DBU/h each, between the size floor "
            "and the autoscale max (a Small with multiplier 8 runs 4 to 32 executors, 12 to "
            "96 DBU/h)."
        ),
        "chips": [("Lakehouse//RT warehouse", URLS["rt_warehouse"]),
                  ("DBSQL Serverless warehouse", URLS["dbsql_warehouse"]),
                  ("RT monitoring", URLS["rt_monitoring"]),
                  "same SQL, both engines"],
    },
    {
        "step": "03 · Head to head",
        "title": "One dashboard, both engines, same instant",
        "tag": "Measured, not claimed: identical values, different speed.",
        "body": (
            "Six KPI widgets, each its own query, issued concurrently the way a dashboard "
            "does, on both warehouses at once through persistent sessions. The headline is "
            "client wall clock and ticks live; engine time from query history is reported "
            "separately below the panels. The app checks that every widget returned the "
            "same value on both engines."
        ),
        "chips": ["6 widgets", "result cache off", "disk cache warm", "values compared"],
    },
    {
        "step": "04 · Scaled load",
        "title": "Thousands of live dashboards on one RT warehouse",
        "tag": "Throughput reported as an arrival rate, not a drain rate.",
        "body": (
            "Each simulated user is one dashboard with 4 merchant-scoped widgets, refreshed "
            "every 10 s, with refreshes spread evenly across the interval. Queries go out on "
            "a wall clock and never wait for earlier ones, so the offered rate is fixed by "
            "the user count (users x 4 / 10 s) and a slow warehouse shows up as latency. "
            "One asyncio loop drives 14 HTTP/2 connections; queries beyond 1,200 in flight "
            "are counted as dropped, which flags the app itself as the limit."
        ),
        "chips": ["1 to 3,000 users", "400 q/s at 1,000 users", "open loop",
                  "14 HTTP/2 connections", "drops = app limit"],
    },
    {
        "step": "05 · Result",
        "title": "Where one Small warehouse stops scaling",
        "tag": "The limit is per warehouse, not CPU and not the DBU cap.",
        "body": (
            "On a Small Lakehouse//RT warehouse, 1,000 users (400 q/s) held engine p50 at "
            "22 to 26 ms with no errors. At 2,000 users (800 q/s) engine p50 rose to about "
            "1.2 s with about 2% HTTP 503, while executors did only ~6 ms of work per query "
            "(~5 cores busy). Resizing to Medium improved this only partly. For light "
            "lookups at high q/s, the lever is more warehouses, not a higher DBU max."
        ),
        "chips": ["knee between 400 and 800 q/s", "executors mostly idle",
                  "Medium: partial", "scale out across warehouses"],
    },
]


# Deep dives, opened on demand: how the load is made, how each number is
# captured, and the traps that produced wrong numbers along the way.
DEEP_DIVES = [
    {
        "step": "Deep dive · 01",
        "title": "How load is generated",
        "tagline": "An arrival rate on a clock, from a pool of connections.",
        "caps": [
            {
                "name": "Open loop",
                "headline": "Queries leave on schedule, whatever the warehouse does.",
                "pitch": (
                    "A closed loop (send, wait, send) slows down with the warehouse and "
                    "reports how fast a queue drains. An open loop holds the offered rate "
                    "fixed, so saturation shows up as latency and errors."
                ),
                "bullets": [
                    "Offered q/s = users x 4 widgets / 10 s refresh. 1,000 users = 400 q/s.",
                    "Each user's refresh has its own phase offset; a scheduler fires due "
                    "refreshes every 50 ms.",
                    "Merchants spread over all 40,000 (100001 + user x 7919 mod 40000), so "
                    "users do not share rows.",
                    "Every statement carries a unique /* nocache */ comment and returns "
                    "INLINE JSON with a 30 s wait.",
                ],
                "doc": "https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial",
            },
            {
                "name": "Connections",
                "headline": "One HTTP/2 connection caps you at 100 in flight.",
                "pitch": (
                    "One asyncio loop is far faster than threads here, but a single HTTP/2 "
                    "connection allows 100 concurrent streams. With one connection the app "
                    "achieved 504 of 1,200 offered q/s; the warehouse was not the limit."
                ),
                "bullets": [
                    "14 single-connection clients, round-robin, ~90 streams each.",
                    "A broken connection is replaced, not retried forever.",
                    "Connections are opened and warmed before the clock starts.",
                    "perf/rt_scale_job.py runs the same workload from a serverless job, to "
                    "tell app limits from warehouse limits.",
                ],
                "doc": "https://www.python-httpx.org/http2/",
            },
        ],
    },
    {
        "step": "Deep dive · 02",
        "title": "How throughput and latency are captured",
        "tagline": "Two clocks, each labelled, never swapped.",
        "caps": [
            {
                "name": "Throughput",
                "headline": "Successful completions over a trailing window.",
                "pitch": (
                    "QPS is successful queries in the last 15 s (1.5 x refresh) divided by "
                    "the observed span. Offered q/s, in flight, errors and drops sit next to "
                    "it, so a gap between offered and achieved is visible."
                ),
                "bullets": [
                    "Hold each step at least 3 minutes; the warehouse needs time to scale.",
                    "Stop freezes the last run's numbers on screen until the next Start.",
                    "DBU/h comes from the warehouse monitoring page or system.billing.usage "
                    "(billing lags a few hours).",
                ],
                "doc": "https://docs.databricks.com/aws/en/compute/sql-warehouse/monitor",
            },
            {
                "name": "Latency",
                "headline": "Client time and engine time answer different questions.",
                "pitch": (
                    "Client time is what a dashboard user waits: network, front door, any "
                    "app queue and the engine. Engine time comes from the query history API "
                    "and is what the warehouse itself spent."
                ),
                "bullets": [
                    "History is polled every second on its own connection.",
                    "Only rows with is_final and FINISHED count; the newest rows are "
                    "provisional at high q/s, so the window ends 3 s in the past.",
                    "The gap between client and engine time is time outside the engine.",
                    "Cores busy is q/s x task time from history metrics; it separates "
                    "engine CPU from queueing.",
                ],
                "doc": "https://docs.databricks.com/api/workspace/queryhistory/list",
            },
        ],
    },
]
