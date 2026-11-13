"""Lakehouse RT Demo: a Databricks App comparing DBSQL Serverless and Lakehouse//RT.

Three views, selected with the `view` query parameter:

  ?view=architecture  (default)  What is deployed and how the numbers are measured.

  ?view=headtohead               The SAME six-widget dashboard loaded side by side,
                                 left on DBSQL Serverless, right on Lakehouse//RT,
                                 each with its own measured latency. Same managed
                                 Delta tables, same SQL, two engines.

  ?view=scale                    Lakehouse//RT under load. N simulated dashboard
                                 users, open loop, with live QPS and engine
                                 p50/p95 from query history.

The data is three synthetic gold tables written by the bundle's data job
(src/data/generate_tables.py). There is no pipeline: the tables are the end state.
"""

from __future__ import annotations

import contextlib
import logging
import os
import time
import uuid
from concurrent.futures import wait

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

import architecture
import demo_config
from demo_config import (CATALOG, DBSQL_WAREHOUSE_ID, EVENTS, FACT, MONTHLY,
                         RT_WAREHOUSE_ID, SCHEMA)
from kernel_driver import KernelPool
import panels
import rt_scale
import warehouse_control as control
from theme import C, CSS, MONO as MONO_FONT, NAV_ITEMS

# Usage tracking with dbdemos-tracker (optional). Counts page views of this demo
# so its owners can show usage. The library sends a SHA-256 hash of the viewer's
# email only for @databricks.com users; anyone else is an anonymous view. Set
# DBDEMOS_TRACKER_DISABLED=1 to turn it off. It never blocks or breaks a page.
DEMO_ID = "lakehouse-rt-demo"
Tracker = None
if os.getenv("DBDEMOS_TRACKER_DISABLED") != "1":
    try:
        from dbdemos_tracker import Tracker
    except Exception as exc:  # noqa: BLE001 - tracking is optional
        logging.warning("dbdemos-tracker unavailable, usage tracking off: %s", exc)


# Scaled load: the ladder the slider exposes. Offered rate = sessions x 4 widgets
# / refresh. At 10 s, 1,000 users offer 400 q/s, the measured healthy point on a
# Small warehouse (engine p50 22-26 ms). 2,000 and 3,000 users (800 and 1,200
# q/s) go past the per-warehouse knee; compare against perf/rt_scale_job.py,
# same workload from a separate job, to tell app limits from warehouse limits.
SCALE_STEPS = [1, 10, 100, 500, 1000, 2000, 3000]
SCALE_REFRESH_S = float(os.getenv("RT_SCALE_REFRESH_S", "10"))

st.set_page_config(page_title="Lakehouse RT Demo", page_icon="⚡", layout="wide",
                   initial_sidebar_state="collapsed")

# --------------------------------------------------------------------------- #
# Widget callbacks.
#
# Every button uses `on_click` rather than its return value, and navigation never
# calls st.rerun(). A Streamlit button returns True for exactly one rerun after a
# click, but st.rerun() can cut a run short before another button's pending True
# has been consumed -- that state then fires on the NEXT render. That is what made
# "Run dashboard" jump to Scaled load: a nav click from an earlier interaction was
# still buffered. on_click callbacks are applied before the script body runs and
# are consumed exactly once, so no stale click can survive.
# --------------------------------------------------------------------------- #
def _nav_to(key: str) -> None:
    control.touch_activity("nav")
    st.query_params["view"] = key


def _h2h_start() -> None:
    control.touch_activity("headtohead")
    st.session_state.pop("h2h", None)
    st.session_state["h2h_run"] = True
    # Run whatever is on stage; do NOT force both engines into view. Forcing it
    # meant selecting one dashboard and pressing Run silently did nothing useful.
    if st.session_state.get("h2h_shown", 0) < 1:
        st.session_state["h2h_shown"] = 1


def _h2h_show_next() -> None:
    st.session_state["h2h_shown"] = min(2, st.session_state.get("h2h_shown", 0) + 1)


def _h2h_reset() -> None:
    st.session_state.pop("h2h", None)
    st.session_state.pop("h2h_run", None)
    st.session_state["h2h_shown"] = 0


def eyebrow(text: str) -> None:
    st.markdown(f'<div class="rk-eyebrow">{text}</div>', unsafe_allow_html=True)


# show_spinner=False: the default cache spinner inserts an element on first call,
# which remounts components.html iframes rendered later in the same run.
@st.cache_resource(show_spinner=False)
def client() -> WorkspaceClient:
    # In Databricks Apps the service principal's credentials arrive via env vars,
    # so the default constructor authenticates with no arguments.
    return WorkspaceClient()


@st.cache_resource(show_spinner=False)
def _workspace_id() -> int:
    return client().get_workspace_id()


class _CachedWorkspace:
    """Gives the tracker the workspace id once per process, not one API call per session."""

    @staticmethod
    def get_workspace_id() -> int:
        return _workspace_id()


def setup_tracking() -> None:
    """Register the usage tracker once per browser session. Never raises."""
    if Tracker is None or st.session_state.get("_tracker_off"):
        return
    try:
        if Tracker.setup_streamlit_tracker(DEMO_ID, workspace_client=_CachedWorkspace) is None:
            st.session_state["_tracker_off"] = True     # do not retry on every rerun
    except Exception as exc:  # noqa: BLE001 - tracking is optional
        logging.warning("usage tracker setup failed: %s", exc)
        st.session_state["_tracker_off"] = True


def track_page(path: str) -> None:
    """One view per page entry. Reruns inside a page (button clicks) do not count again.

    Fire-and-forget: the library posts from a daemon thread with a 2 s timeout.
    """
    if (Tracker is None or st.session_state.get("_tracker_off")
            or st.session_state.get("_tracked_page") == path):
        return
    try:
        Tracker.track_streamlit_event(path)
        st.session_state["_tracked_page"] = path
    except Exception as exc:  # noqa: BLE001 - tracking is optional
        logging.warning("usage tracking failed: %s", exc)


@st.cache_resource(show_spinner=False)
def anchor_date():
    """Last date in the synthetic data. Time-windowed widgets filter from here."""
    # No warehouse id: read the table property only, so a page view never
    # starts a warehouse. Missing property means today.
    return demo_config.resolve_anchor(client())


@st.cache_resource(show_spinner=False)
def query_pool(warehouse_id: str, size: int,
               disable_result_cache: bool = False) -> KernelPool:
    """Persistent optimized SQL sessions, shared across Streamlit reruns."""
    return KernelPool(warehouse_id, size, disable_result_cache=disable_result_cache)


def live_pool(warehouse_id: str, size: int,
              disable_result_cache: bool = False) -> KernelPool:
    """`query_pool`, but never hand back a pool whose OAuth token has expired.

    The pool is cached for the life of the app while its bearer token lives about
    an hour, so a demo run later in the day would otherwise fail on every
    statement with an auth error that reads like a warehouse fault. On expiry the
    stale connections are closed before the cache is dropped, so the sockets and
    worker threads go away instead of leaking until restart.
    """
    pool = query_pool(warehouse_id, size, disable_result_cache)
    if not pool.is_stale():
        return pool
    try:
        pool.close()
    except Exception:  # noqa: BLE001 - a failed close must not block the rebuild
        pass
    query_pool.clear()
    return query_pool(warehouse_id, size, disable_result_cache)


def cache_bust(sql: str) -> str:
    """Make the statement text unique so the DBSQL result cache cannot serve it.

    Do NOT use `SET use_cached_result = false; SELECT ...` -- the Statement
    Execution API takes ONE statement, so that form fails outright with
    INVALID_SET_SYNTAX (it failed 1280/1280 statements here). It would also be
    rejected on Reyden, which is SELECT-only. The result cache keys on query
    text, so a unique comment forces a miss while leaving the plan identical.
    """
    return f"/* nocache={uuid.uuid4().hex} */ {sql}"


def run_sql_full(sql: str, warehouse_id: str, timeout_s: int = 120):
    """Execute and return (DataFrame, statement_id).

    The statement_id matters: the Statement Execution API response carries NO
    timing, so server-side duration has to be looked up in query history
    afterwards (see `server_durations`).

    `format` and `disposition` are pinned deliberately. A PRO warehouse defaults to
    JSON_ARRAY and populates `result.data_array`, but a Lakehouse//RT warehouse
    defaults to ARROW_STREAM, returning `result.attachment` (base64 Arrow IPC) and
    leaving `data_array` as None. Reading only `data_array` therefore yields ZERO
    ROWS against Reyden while the statement reports SUCCEEDED.
    """
    w = client()
    resp = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
    )
    deadline = time.time() + timeout_s
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            raise TimeoutError(f"statement exceeded {timeout_s}s")
        time.sleep(0.4)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        detail = resp.status.error.message if (resp.status and resp.status.error) else "unknown error"
        raise RuntimeError(detail)

    cols = [c.name for c in resp.manifest.schema.columns] if resp.manifest and resp.manifest.schema else []
    rows = (resp.result.data_array or []) if resp.result else []

    # If a warehouse ever ignores the JSON_ARRAY request, fail loudly rather than
    # returning an empty frame that reads as "no data".
    if not rows and resp.result is not None and getattr(resp.result, "attachment", None):
        fmt = getattr(resp.manifest, "format", "unknown") if resp.manifest else "unknown"
        raise RuntimeError(
            f"warehouse returned {fmt} instead of JSON_ARRAY, so no rows were read. "
            "This is the Reyden ARROW_STREAM default; pin format=Format.JSON_ARRAY."
        )
    return pd.DataFrame(rows, columns=cols), resp.statement_id


def run_sql(sql: str, warehouse_id: str, timeout_s: int = 120) -> pd.DataFrame:
    return run_sql_full(sql, warehouse_id, timeout_s)[0]


def timed_sql(sql: str, warehouse_id: str, nocache: bool = True):
    """Return (client_ms, DataFrame, error_or_None, statement_id)."""
    stmt = cache_bust(sql) if nocache else sql
    start = time.perf_counter()
    try:
        df, sid = run_sql_full(stmt, warehouse_id, timeout_s=90)
    except Exception as e:  # noqa: BLE001 - surfaced in the UI
        return (time.perf_counter() - start) * 1000.0, None, str(e), None
    return (time.perf_counter() - start) * 1000.0, df, None, sid


# --------------------------------------------------------------------------- #
# Warm gate. Nothing is measured until every warehouse involved is RUNNING.
#
# Both warehouses auto-stop when idle (RT 5 min, DBSQL 10 min), so STOPPED is the
# normal state between demo runs. A stopped serverless warehouse costs tens of
# seconds to start. The discarded warm-up pass keeps the NUMBERS honest either
# way, but without this gate the UI simply hangs with no explanation, and a
# single warm-up burst against a just-started warehouse may not have filled the
# disk cache yet.
# --------------------------------------------------------------------------- #
def ensure_warm(warehouse_ids: list[str], slot=None,
                timeout_s: float = 300.0) -> tuple[bool, dict]:
    """Start any stopped warehouse and block until all are RUNNING.

    Returns (all_running, {display_name: state}). Reports progress into `slot`
    so a 40 s start-up looks like progress rather than a frozen app.
    """
    from databricks.sdk.service.sql import State
    w = client()
    ids = [i for i in dict.fromkeys(warehouse_ids) if i]
    if not ids:
        return False, {}
    names, need = {}, []
    for i in ids:
        try:
            g = w.warehouses.get(id=i)
        except Exception as e:  # noqa: BLE001
            return False, {i: f"unreadable: {e}"}
        names[i] = g.name or i
        if g.state != State.RUNNING:
            need.append(i)
    if not need:
        return True, {names[i]: "RUNNING" for i in ids}

    for i in need:
        try:
            w.warehouses.start(id=i)
        except Exception:  # noqa: BLE001
            pass                      # already STARTING is fine
    deadline = time.monotonic() + timeout_s
    while True:
        cur = {}
        for i in ids:
            try:
                cur[i] = w.warehouses.get(id=i).state
            except Exception:  # noqa: BLE001
                cur[i] = None
        if all(v == State.RUNNING for v in cur.values()):
            return True, {names[i]: "RUNNING" for i in ids}
        if time.monotonic() > deadline:
            return False, {names[i]: (cur[i].value if cur[i] else "?") for i in ids}
        if slot is not None:
            slot.info("Starting warehouses, so nothing is measured cold — "
                      + " · ".join(f"**{names[i]}**: {cur[i].value if cur[i] else '?'}"
                                   for i in ids))
        time.sleep(3.0)


def server_durations(statement_ids: list[str], warehouse_ids: list[str],
                     attempts: int = 6, budget_s: float = 12.0,
                     since_ms: int | None = None) -> dict[str, float]:
    """Look up SERVER-SIDE duration (ms) per statement from query history.

    Why this exists: a client round-trip includes HTTPS to the control plane, SDK
    overhead and JSON parsing. Comparing engines on client time therefore measures
    partly network; comparing on server duration measures the engine.

    History publishes each record twice: first at ~0.3 s with is_final=false and a
    PROVISIONAL duration (measured up to 23% high), then settled at ~1.6-2.4 s. Only
    settled records are accepted.

    PAGES. An earlier version issued a single max_results=100 request, so a 1,000
    statement burst could only ever resolve 100 of them and the reported percentiles
    came from a 10% sample. Now it follows next_page_token until the wanted ids are
    found, bounded by a wall-clock budget so a huge burst cannot hang the UI.

    `since_ms` narrows history to the burst window. Measured on 250 statements:
    without it, 250/250 in 3.0 s; with it, 250/250 in 0.2 s -- because unfiltered
    pages are mostly unrelated queries. This is what made the engine-time block
    appear seconds after the run finished.
    """
    want = set(i for i in statement_ids if i)
    out: dict[str, float] = {}
    if not want:
        return out
    w = client()
    deadline = time.perf_counter() + budget_s
    for _ in range(attempts):
        for wid in warehouse_ids:
            token, pages = None, 0
            while pages < 25 and time.perf_counter() < deadline:
                q = {"max_results": 100, "filter_by.warehouse_ids": wid}
                if since_ms is not None:
                    q["filter_by.query_start_time_range.start_time_ms"] = since_ms
                if token:
                    q["page_token"] = token
                try:
                    resp = w.api_client.do("GET", "/api/2.0/sql/history/queries", query=q)
                except Exception:  # noqa: BLE001
                    break
                for r in (resp.get("res") or []):
                    if (r.get("query_id") in want and r.get("is_final") is True
                            and r.get("duration") is not None):
                        out[r["query_id"]] = float(r["duration"])
                token = resp.get("next_page_token")
                pages += 1
                if not token or not (want - set(out)):
                    break
        if len(out) >= len(want) or time.perf_counter() >= deadline:
            break
        time.sleep(0.7)
    return out


def permission_hint(msg: str) -> None:
    if "PERMISSION_DENIED" in msg or "does not have" in msg:
        st.warning(
            "This is a **permissions** problem, not missing data. The app runs as its own "
            "service principal, which needs Unity Catalog read access granted separately "
            "from the warehouse permissions. The data job grants it; rerun "
            "`databricks bundle run generate_data`, or:\n\n"
            "```sql\n"
            f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `<app-sp-id>`;\n"
            f"GRANT USE SCHEMA  ON SCHEMA  {CATALOG}.{SCHEMA} TO `<app-sp-id>`;\n"
            f"GRANT SELECT      ON SCHEMA  {CATALOG}.{SCHEMA} TO `<app-sp-id>`;\n"
            "```"
        )
    elif "TABLE_OR_VIEW_NOT_FOUND" in msg or "cannot be found" in msg:
        st.info("The tables do not exist yet. Run the data job: "
                "`databricks bundle run generate_data`.")


# --------------------------------------------------------------------------- #
# Head to head: two warehouses, one dashboard
# --------------------------------------------------------------------------- #
def _tiles_for(specs, vals: dict, kind: str) -> str:
    return "".join(
        panels.tile(sp, vals.get(sp["key"], (None, None))[0],
                    vals.get(sp["key"], (None, None))[1], i, kind)
        for i, sp in enumerate(specs)
    )


def _load_both_live(engines, slots, specs, warmup: bool = False) -> dict:
    """Load every engine's dashboard simultaneously, ticking the client clock live.

    One clock in the window: client wall-clock, which ticks up and lands on the
    final total. Engine time (server-side duration from query history) is fetched
    afterwards and reported as its own metric BELOW the panels -- never swapped into
    the headline, because a number that grows then shrinks reads as broken.
    """
    names = {k: n for n, k, _ in engines}
    vals = {k: {} for _, k, _ in engines}
    sids = {k: {} for _, k, _ in engines}
    errs = {k: None for _, k, _ in engines}
    remaining = {k: len(specs) for _, k, _ in engines}
    done_ms = {}
    # shown_ms is THE number on screen for each engine. It only ever moves
    # forward, and the value frozen at DONE is literally its last tick -- so the
    # final headline is a number the viewer watched the counter reach.
    shown_ms = {k: 0.0 for _, k, _ in engines}
    painted_done: set[str] = set()
    last_tick_s = -1.0

    futures = {}
    pools = {
        kind: live_pool(
            wid,
            len(specs),
            disable_result_cache=(kind == "dw"),
        )
        for _, kind, wid in engines
    }
    # Use one freshly cache-busted statement per widget across both engines so
    # the SQL text is byte-for-byte identical. DBSQL also has result caching
    # explicitly disabled on its persistent sessions; the comment is retained
    # as defense in depth and keeps the RT side on a unique statement too.
    statements = {sp["key"]: cache_bust(sp["sql"]) for sp in specs}
    t0 = time.perf_counter()
    burst_ms = int(time.time() * 1000) - 2000
    for _name, kind, _wid in engines:
        for sp in specs:
            future = pools[kind].submit(statements[sp["key"]], include_frame=True)
            futures[future] = (kind, sp)
    pending = set(futures)
    while pending:
        finished, pending = wait(pending, timeout=0.05)
        for fut in finished:
            kind, sp = futures[fut]
            result = fut.result()
            ms, df, e, sid = (
                result.client_ms,
                result.frame,
                result.error,
                result.statement_id,
            )
            if e:
                errs[kind] = errs[kind] or e
                vals[kind][sp["key"]] = (None, None)
            else:
                raw = df.iloc[0]["v"] if (df is not None and not df.empty) else None
                vals[kind][sp["key"]] = (panels.fmt_value(sp["fmt"], raw), ms)
                sids[kind][sp["key"]] = sid
            remaining[kind] -= 1
            if remaining[kind] == 0 and kind not in done_ms:
                shown_ms[kind] = max(shown_ms[kind],
                                     (time.perf_counter() - t0) * 1000.0)
                done_ms[kind] = shown_ms[kind]      # freeze == last tick

        # A warm-up is invisible. Replacing both complete SVG dashboards during
        # two discarded passes caused most of the visible blinking.
        if not warmup:
            elapsed_s = time.perf_counter() - t0
            now = elapsed_s * 1000.0
            tick_due = elapsed_s - last_tick_s >= 0.10
            for _, kind, _ in engines:
                finished_k = remaining[kind] == 0
                if finished_k:
                    if kind in painted_done:
                        continue
                    state, val = ("ERROR" if errs[kind] else "DONE"), done_ms[kind]
                    painted_done.add(kind)
                else:
                    if not tick_due:
                        continue
                    shown_ms[kind] = max(shown_ms[kind], now)   # never goes back
                    state, val = "RUNNING", shown_ms[kind]
                slots[kind].markdown(
                    panels.window(names[kind], kind, state, val,
                                  _tiles_for(specs, vals[kind], kind)),
                    unsafe_allow_html=True)
            if tick_due:
                last_tick_s = elapsed_s

    fb = (time.perf_counter() - t0) * 1000.0
    if warmup:
        return {}

    srv = server_durations([x for k in sids for x in sids[k].values()],
                           [w for _, _, w in engines], since_ms=burst_ms)
    return {k: {"vals": vals[k], "client_ms": done_ms.get(k, fb), "err": errs[k],
                "engine_ms": (max([srv[x] for x in sids[k].values() if x in srv])
                              if any(x in srv for x in sids[k].values()) else None)}
            for _, k, _ in engines}


def view_headtohead() -> None:
    track_page("/headtohead")
    specs = panels.widget_specs(FACT, MONTHLY, anchor_date())
    engines = [("DBSQL Serverless", "dw", DBSQL_WAREHOUSE_ID)]
    if RT_WAREHOUSE_ID:
        engines.append(("Lakehouse//RT", "rt", RT_WAREHOUSE_ID))

    shown = st.session_state.get("h2h_shown", 0)
    has_run = "h2h" in st.session_state

    head = st.columns([4, 1.2, 1.4, 0.9])
    head[0].markdown('<div class="rk-eyebrow">Real-time serving · head to head</div>',
                     unsafe_allow_html=True)
    if shown < len(engines):
        head[1].button(f"Show dashboard ({shown}/{len(engines)})", key="h2h_show_btn",
                       use_container_width=True, on_click=_h2h_show_next)
    else:
        head[1].button("Reset", key="h2h_reset_btn", use_container_width=True,
                       on_click=_h2h_reset)
    head[2].button("Run again" if has_run else "Run Dashboard", key="h2h_run_btn",
                   type="primary", use_container_width=True, on_click=_h2h_start,
                   disabled=shown < 1)
    head[3].button("Home", key="h2h_home_btn", use_container_width=True,
                   on_click=_nav_to, args=("architecture",))

    if shown == 0:
        st.markdown(
            '<div class="rk-stage-empty"><div class="h">2 engines, one workload</div>'
            '<div class="p">Press "Show dashboard" to introduce each engine, left to right.'
            '</div></div>', unsafe_allow_html=True)
        return

    warm_slot = st.empty()          # warm-gate status, above the panels
    cols = st.columns(len(engines))
    slots = {kind: cols[i].empty() for i, (_, kind, _) in enumerate(engines) if i < shown}

    cached = st.session_state.get("h2h", {})
    for name, kind, wid in engines[:shown]:
        got = cached.get(kind)
        if got:
            slots[kind].markdown(
                panels.window(name, kind, "ERROR" if got["err"] else "DONE",
                              got["client_ms"], _tiles_for(specs, got["vals"], kind)),
                unsafe_allow_html=True)
        else:
            slots[kind].markdown(
                panels.window(name, kind, "IDLE", None, _tiles_for(specs, {}, kind)),
                unsafe_allow_html=True)
    if st.session_state.pop("h2h_run", False):
        live = [(n, k, w) for n, k, w in engines[:shown] if w]
        prewarmed = _prewarmed()
        if not prewarmed:
            for name, kind, wid in live:
                slots[kind].markdown(
                    panels.window(name, kind, "WARMING", None, _tiles_for(specs, {}, kind)),
                    unsafe_allow_html=True)
        # Gate: every warehouse RUNNING before anything is measured.
        ok, states = ensure_warm([w for _, _, w in live], slot=warm_slot)
        if not ok:
            warm_slot.error(
                "Not all warehouses reached RUNNING, so the comparison would measure "
                "start-up rather than the engines: "
                + " · ".join(f"{k}: {v}" for k, v in states.items())
                + ". Press Pre-warm and try again.")
            return
        # Pre-warm runs these exact dashboard queries on both warehouses. Without
        # it (warehouses started by this click, or a local run), two discarded
        # passes fill the disk cache so start-up is never measured.
        if not prewarmed:
            for _ in range(2):
                _load_both_live(live, slots, specs, warmup=True)
        results = _load_both_live(live, slots, specs)
        st.session_state["h2h"] = results
        cached = results


    # ---- engine time reported here, as its own quantity ------------------ #
    if cached and any(v.get("engine_ms") for v in cached.values()):
        st.markdown("")
        st.markdown('<div class="rk-eyebrow">Engine time · server-side duration from query '
                    'history</div>', unsafe_allow_html=True)
        m = st.columns(len(cached) + 1)
        for i, (name, kind, _) in enumerate(engines[:shown]):
            got = cached.get(kind) or {}
            m[i].metric(name, panels.fmt_latency(got.get("engine_ms")),
                        help="What the warehouse itself spent, accepted only once query "
                             "history reports is_final. The panel headline above is client "
                             "round-trip, which additionally carries the ~60-70 ms of HTTPS "
                             "and SDK overhead that both engines pay equally.")
        if len(cached) == 2 and all(cached[k].get("engine_ms") for k in cached):
            dw, rt = cached["dw"]["engine_ms"], cached["rt"]["engine_ms"]
            faster = "Lakehouse//RT" if rt < dw else "DBSQL Serverless"
            m[-1].metric(f"{faster} faster by", f"{max(dw, rt) / min(dw, rt):.1f}x")

    if len(cached) == 2 and all(not v["err"] for v in cached.values()) and shown == 2:
        same = all(cached["dw"]["vals"].get(sp["key"], (None,))[0]
                   == cached["rt"]["vals"].get(sp["key"], (None,))[0] for sp in specs)
        if same:
            st.success(
                "Every widget returned the same value on both engines. One logical copy of data, "
                "two serving engines — the only thing that changed is how fast it came back.")
        else:
            st.error("Widget values differ between engines. Both panels must read the same table.")

    st.caption(
        "The same six-widget dashboard over the same managed Delta gold table, loaded on both "
        "engines. Each widget is its own query, issued concurrently the way a dashboard does, and "
        "both engines start in the same instant through persistent kernel-backed sessions. The "
        "deployed app pre-starts and keeps both warehouses warm before this button is pressed."
    )

    errs = [v["err"] for v in cached.values() if v and v.get("err")]
    if errs:
        st.error(errs[0])
        permission_hint(errs[0])

    with st.expander("Measurement notes"):
        st.markdown(
            """
| | |
|---|---|
| **Panel headline** | Client wall-clock end-to-end, measured inside the app container over persistent `use_kernel=True` sessions. Counts up only during the measured pass, never decreases, and freezes on its own last tick. |
| **Engine time** | Server-side duration from query history, below the panels, accepted only once `is_final` is true. Lower than the headline because it excludes the network and SDK time both engines pay equally. |
| **Result cache** | Disabled on every persistent DBSQL session with `use_cached_result=false`. Both engines also receive the same byte-for-byte SQL with a fresh `/* nocache=<uuid> */` comment. Lakehouse//RT has no result cache. |
| **Disk cache** | Warm on both engines. Pre-warm runs these exact queries on both warehouses; without it, two discarded passes run first. |
| **Warehouses** | Size both the same (for example both Small), so the comparison is not a sizing artifact. |
| **Layout** | Liquid-clustered on `merchant_id, chargeback_filing_date`, with OPTIMIZE run. Clustering is only metadata until OPTIMIZE runs. |

**Where the large gaps live.** One dashboard is one user. Concurrency is what separates
the engines; see the Scaled load view.
            """
        )


def _scale_widgets() -> list:
    return rt_scale.widget_specs(FACT, EVENTS, MONTHLY, anchor_date())


@st.cache_resource(show_spinner=False)
def scale_driver(warehouse_id: str):
    """One driver per app process.

    Cached on warehouse_id ONLY. The previous view cached connection pools by
    (warehouse_id, size), so sweeping 16/32/64/128 left four live pools with
    their threads and connections alive until the app restarted.
    """
    return rt_scale.ScaleDriver(warehouse_id, _scale_widgets())


def _scale_config(selected: int) -> dict:
    return {
        "widgets": rt_scale.widget_meta(_scale_widgets()),
        "scales": SCALE_STEPS,
        "widget_count": len(_scale_widgets()),
        "warehouse_id": RT_WAREHOUSE_ID,
        "schema": f"{CATALOG}.{SCHEMA}",
        "default_refresh_s": SCALE_REFRESH_S,
        "sessions_selected": selected,
    }


# --------------------------------------------------------------------------- #
# Scaled load: N concurrent dashboard sessions against Lakehouse//RT
#
# The view is the ORIGINAL frontend, unchanged: served as a Streamlit static
# file and embedded as a same-origin iframe so its own JavaScript runs. Only the
# transport differs -- the driver publishes snapshots to static/rt_state.json and
# the page polls it, because Streamlit owns the single port and exposes no custom
# endpoints. The iframe src never changes, so Streamlit never re-renders it and
# the wall is never rebuilt.
#
# Controls live INSIDE the iframe and click the hidden marker buttons rendered
# below. Streamlit's component sandbox allows same-origin but NOT top-level
# navigation, so the earlier query-param approach silently did nothing and Start
# looked dead. Clicking a real Streamlit button runs its on_click callback and
# reruns; the component's HTML is constant, so that rerun does not rebuild the
# wall.
# --------------------------------------------------------------------------- #
def _rt_set(n: int, run: bool) -> None:
    if run:
        control.touch_activity("scale")
    st.session_state["rt_n"] = n
    st.session_state["rt_run"] = run


def view_scale() -> None:
    track_page("/scale")
    if not RT_WAREHOUSE_ID:
        st.error("RT_WAREHOUSE_ID is not set. Add the Lakehouse//RT warehouse as an app resource.")
        return

    pre = rt_scale.preflight()
    if not pre["ok"]:
        st.error(
            f"Scaled load is unavailable - httpx: {pre['httpx']} / "
            f"static dir: {pre['static']} / auth: {pre['auth']}"
        )
        return

    driver = scale_driver(RT_WAREHOUSE_ID)

    # A fresh visit starts from scratch: a new browser session (page reload) or
    # arriving from another view resets the selection, stops any run and drops
    # the frozen result. Reruns inside the view (Start, Stop, user count) keep
    # it, so a finished run stays on screen until the viewer leaves.
    if not st.session_state.get("_rt_in_scale"):
        st.session_state["_rt_in_scale"] = True
        st.session_state["rt_n"] = SCALE_STEPS[0]
        st.session_state["rt_run"] = False
        driver.ui_config = _scale_config(SCALE_STEPS[0])
        driver.reset()

    # Hidden control surface. The iframe finds these by their exact label text and
    # clicks them; fitToParent() in the page injects CSS into this document to
    # hide them, so nothing shows here.
    for n in SCALE_STEPS:
        st.button(f"rtc:start:{n}", key=f"rtc_start_{n}",
                  on_click=_rt_set, args=(n, True))
    st.button("rtc:stop", key="rtc_stop", on_click=_rt_set,
              args=(st.session_state.get("rt_n", SCALE_STEPS[0]), False))

    want_n = int(st.session_state.get("rt_n", SCALE_STEPS[0]))
    if want_n not in SCALE_STEPS:
        want_n = SCALE_STEPS[0]
    want_run = bool(st.session_state.get("rt_run", False))
    # Also honour query params, so a direct link still works -- but only ONCE per
    # browser session. Re-reading `run=1` on every rerun overrode the Stop button:
    # Stop set rt_run=False, and the next rerun turned it straight back on.
    if not st.session_state.get("_rt_qp_applied"):
        st.session_state["_rt_qp_applied"] = True
        with contextlib.suppress(Exception):
            if "n" in st.query_params:
                q = int(st.query_params["n"])
                if q in SCALE_STEPS:
                    want_n = q
                    st.session_state["rt_n"] = q
            if "run" in st.query_params:
                want_run = str(st.query_params["run"]) == "1"
                st.session_state["rt_run"] = want_run
                # Drop it from the URL so a page reload does not restart the load.
                del st.query_params["run"]

    driver.ui_config = _scale_config(want_n)

    # Emit the page FIRST, before any slow work. ensure_warm() (a warehouse API
    # call) and driver.start() used to run before this line, so every Start /
    # Stop rerun left the iframe mid-rerun; the page remounted, flashed its
    # built-in defaults ("1 user", ticks 1/10/100/500), then caught up.
    # The string is constant across reruns, so Streamlit keeps the same iframe.
    components.html(rt_scale.frontend_html(), height=1400, scrolling=False)

    if want_run and not (driver.running and driver.sessions == want_n):
        # Gate on RUNNING before measuring, the same way head to head does. After
        # Pre-warm this is a no-op. Otherwise it starts RT here: without the gate,
        # warehouse start-up lands inside the first cycle's client latency and the
        # headline reads as an engine problem.
        ok, states = ensure_warm([RT_WAREHOUSE_ID])
        if not ok:
            st.error(
                "Lakehouse//RT did not reach RUNNING, so this would measure "
                "start-up rather than serving latency: "
                + " / ".join(f"{k}: {v}" for k, v in states.items())
                + ". Press Pre-warm and try again."
            )
            return
        driver.start(want_n, SCALE_REFRESH_S)
    elif not want_run and driver.running:
        driver.stop()

    if not driver.running:
        # Idle: publish the last run's frozen result (if any) so Stop keeps the
        # numbers on screen; the empty framework only before the first run.
        with contextlib.suppress(Exception):
            rt_scale.publish_idle(driver.ui_config, driver.last_result)

    # Note on the components.html call above: injected as srcdoc, NOT loaded by
    # iframe src, because Streamlit's static handler serves .html as text/plain.
    # The page polls /app/static/rt_state.json for its own state and resizes
    # itself, so the height is only a pre-fit starting value.


# --------------------------------------------------------------------------- #
# Architecture
# --------------------------------------------------------------------------- #
def view_architecture() -> None:
    track_page("/architecture")
    eyebrow("Architecture")
    st.title("Lakehouse RT Demo")
    st.caption(
        "One copy of the data in Unity Catalog, two SQL engines reading it. Every box below "
        "is a deployed artifact in the lakehouse-rt-demo bundle."
    )

    # Colour key first: the palette encodes asset type, so the diagram is only
    # readable once the reader knows what each colour means.
    st.markdown(architecture.legend_html(), unsafe_allow_html=True)
    st.markdown(
        f'<div class="rk-card" style="padding:.7rem">{architecture.diagram_svg()}</div>',
        unsafe_allow_html=True,
    )

    st.markdown("")

    def _chip(c, first: bool) -> str:
        """A chip, rendered as a link when the entry carries a URL.

        Entries are either a plain label or a (label, url) pair pointing at the
        deployed asset, so the page doubles as a jump-off point into the workspace.
        """
        cls = "rk-chip accent" if first else "rk-chip"
        if isinstance(c, (tuple, list)):
            label, url = c
            return (f'<a class="{cls}" href="{url}" target="_blank" '
                    f'rel="noopener">{label}</a>')
        return f'<span class="{cls}">{c}</span>'

    for layer in architecture.LAYERS:
        chips = "".join(
            _chip(c, i == 0) for i, c in enumerate(layer["chips"])
        )
        st.markdown(
            f'<div class="rk-layer"><div class="rk-step">{layer["step"]}</div>'
            f'<h4>{layer["title"]}</h4><div class="rk-tag">{layer["tag"]}</div>'
            f'<p>{layer["body"]}</p><div class="rk-chips">{chips}</div></div>',
            unsafe_allow_html=True,
        )

    # Deep dives: same order as the bands above, one section per theme. Collapsed by
    # default so the page still reads top-to-bottom in a demo, expandable when a
    # customer engineer wants the specifics behind a band.
    st.markdown("---")
    st.subheader("Section by section")
    st.caption(
        "Each section expands the corresponding band above. Every claim names a deployed "
        "artifact or a measured number rather than a product capability."
    )

    for dive in architecture.DEEP_DIVES:
        st.markdown(
            f'<div class="rk-layer" style="margin-bottom:.35rem">'
            f'<div class="rk-step">{dive["step"]}</div>'
            f'<h4>{dive["title"]}</h4>'
            f'<div class="rk-tag">{dive["tagline"]}</div></div>',
            unsafe_allow_html=True,
        )
        for cap in dive["caps"]:
            with st.expander(f'{cap["name"]} — {cap["headline"]}'):
                st.markdown(
                    f'<div class="rk-tagline">{cap["pitch"]}</div>', unsafe_allow_html=True
                )
                st.markdown("\n".join(f"- {b}" for b in cap["bullets"]))
                st.markdown(
                    f'<a href="{cap["doc"]}" target="_blank" rel="noopener" '
                    f'class="rk-chip accent" style="text-decoration:none">documentation ↗</a>',
                    unsafe_allow_html=True,
                )


# --------------------------------------------------------------------------- #
# Warehouse controls, on every page.
#
# The buttons only queue a command; the watchdog in warehouse_keeper.py starts,
# warms and stops the warehouses, and turns them off after the idle limit. The
# bar is a fragment that refreshes itself every few seconds, so its reruns and
# button clicks never rerun the page (and never remount the Scaled load wall).
# --------------------------------------------------------------------------- #
_PHASES = {
    "warm": ("Warm", C["p50"]),
    "running": ("Running", C["p50"]),
    "starting": ("Starting", C["p95"]),
    "warming": ("Warming cache", C["p95"]),
    "stopping": ("Stopping", C["p95"]),
    "partial": ("Partly on", C["p95"]),
    "off": ("Off", C["dim"]),
    "unknown": ("Unknown", C["dim"]),
}


def _prewarmed() -> bool:
    s = control.read_status()
    return bool(s and s.get("warm"))


def _wh_warm() -> None:
    control.touch_activity("prewarm")
    st.session_state["_wh_pending"] = control.send_command("warm")


def _wh_off() -> None:
    # A live scaled-load run would start RT again on its next statement.
    st.session_state["rt_run"] = False
    if RT_WAREHOUSE_ID:
        with contextlib.suppress(Exception):
            scale_driver(RT_WAREHOUSE_ID).reset()
    st.session_state["_wh_pending"] = control.send_command("stop")


@st.fragment(run_every=4)
def warehouse_bar() -> None:
    s = control.read_status()
    pending = st.session_state.get("_wh_pending")
    if pending and s and s.get("handled") == pending:
        st.session_state.pop("_wh_pending", None)
        pending = None
    info, warm_col, off_col = st.columns([3.4, 1.2, 1.2], gap="small")

    if s is None:
        info.markdown(
            f'<div style="font-size:.74rem;color:{C["dim"]};padding-top:.45rem">'
            "Warehouse watchdog is not running here, so the controls are off.</div>",
            unsafe_allow_html=True)
        warm_col.button("Pre-warm", key="wh_warm", disabled=True, use_container_width=True)
        off_col.button("Turn off", key="wh_off", disabled=True, use_container_width=True)
        return

    phase = s.get("phase", "unknown")
    label, col = _PHASES.get(phase, _PHASES["unknown"])
    chips = " ".join(
        f'<span style="color:{C["p50"] if w["state"] == "RUNNING" else C["muted"]}">'
        f'{"RT" if w["id"] == RT_WAREHOUSE_ID else "DBSQL"} {w["state"].lower()}</span>'
        for w in s.get("warehouses", []))
    if pending:
        detail = "request sent, waiting for the watchdog"
    elif phase in ("warm", "running", "partial"):
        left = (s.get("idle_stop_s") or 0) - (s.get("idle_for_s") or 0)
        detail = (f"auto-off in {max(1, round(left / 60))} min if idle" if left > 0
                  else "auto-off check due")
        if s.get("kept_on"):
            detail = "left on: someone else is using it"
    elif phase == "off":
        detail = "press Pre-warm about a minute before demoing"
    else:
        detail = s.get("note") or ""
    if str(s.get("note", "")).startswith("pre-warm failed"):
        detail = s["note"][:120]
    info.markdown(
        f'<div style="font-size:.74rem;line-height:1.25;padding-top:.15rem">'
        f'<span style="color:{C["muted"]};letter-spacing:.12em;font-weight:700">WAREHOUSES</span> '
        f'<span style="color:{col};font-weight:700">&#9679; {label}</span><br>'
        f'<span style="font-family:{MONO_FONT}">{chips}</span> '
        f'<span style="color:{C["dim"]}">&middot; {detail}</span></div>',
        unsafe_allow_html=True)

    busy = bool(pending) or phase in ("starting", "warming", "stopping")
    mins = int((s.get("idle_stop_s") or 0) // 60)
    warm_col.button(
        "Pre-warm", key="wh_warm", on_click=_wh_warm, use_container_width=True,
        disabled=busy or phase == "warm",
        help=f"Start both warehouses and fill their disk cache (about a minute). They "
             f"stay warm while the demo is used and turn off after {mins} min idle.")
    off_col.button(
        "Turn off", key="wh_off", on_click=_wh_off, use_container_width=True,
        disabled=busy or phase == "off",
        help="Stop both warehouses now. This also ends anyone else's queries on them.")


# --------------------------------------------------------------------------- #
def main() -> None:
    setup_tracking()
    st.markdown(CSS, unsafe_allow_html=True)

    valid = [k for k, _ in NAV_ITEMS]
    view = st.query_params.get("view", "architecture")
    if view not in valid:
        view = "architecture"

    # Navigation uses real buttons. HTML anchors (`<a href="?view=...">`) render
    # inside st.markdown but do not reliably navigate in Streamlit, which left the
    # pills looking clickable and doing nothing.
    nav_cols = st.columns([1, 1, 1, 0.3, 5.2], gap="small")
    for col, (key, label) in zip(nav_cols, NAV_ITEMS):
        col.button(label, key=f"nav_{key}", use_container_width=True,
                   type="primary" if key == view else "secondary",
                   on_click=_nav_to, args=(key,), disabled=(key == view))
    with nav_cols[4]:
        warehouse_bar()
    st.markdown("")

    if not DBSQL_WAREHOUSE_ID and not RT_WAREHOUSE_ID:
        st.error("No warehouse configured. Add them as app resources.")
        return

    # Leaving Scaled load stops its run and forgets the result (see view_scale).
    if view != "scale" and st.session_state.pop("_rt_in_scale", False) and RT_WAREHOUSE_ID:
        with contextlib.suppress(Exception):
            scale_driver(RT_WAREHOUSE_ID).reset()

    if view == "headtohead":
        view_headtohead()
    elif view == "scale":
        view_scale()
    else:
        view_architecture()


if __name__ == "__main__":
    main()
