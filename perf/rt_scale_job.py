"""Scaled-load step test for the Lakehouse//RT warehouse, run as a serverless job.

Same workload as the app's Scaled load tab (src/app/rt_scale.py): every simulated
user refreshes the same 4 widget queries for its own merchant every REFRESH_S
seconds, refreshes spread evenly across the interval (open loop, arrival rate).
Offered q/s = users x 4 / REFRESH_S.

The load is split across several OS processes, each with its own asyncio loop and
HTTP/2 clients, so one Python process is not the throughput ceiling.

Parameters (positional, all optional):
  1  path to the deployed src/app directory (imports rt_scale from there)
  2  warehouse id
  3  steps, "users:seconds,users:seconds,..."
  4  worker processes
  5  refresh seconds
  6  catalog
  7  schema

Client-side numbers are printed here. Engine-side numbers (inside the warehouse)
come from query history, read separately for the same time window.
"""
from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
import os
import sys
import time
import uuid

ARGS = sys.argv[1:]
APP_DIR = ARGS[0]
WAREHOUSE = ARGS[1]
STEPS = [(int(a), int(b)) for a, b in
         (s.split(":") for s in (ARGS[2] if len(ARGS) > 2 else "1000:60,2000:180,3000:180").split(","))]
PROCS = int(ARGS[3]) if len(ARGS) > 3 else 8
REFRESH_S = float(ARGS[4]) if len(ARGS) > 4 else 10.0
os.environ["DEMO_CATALOG"] = ARGS[5] if len(ARGS) > 5 else "main"
os.environ["DEMO_SCHEMA"] = ARGS[6] if len(ARGS) > 6 else "lakehouse_rt_demo"
INFLIGHT_CAP = 2000          # per process; beyond this a request is counted as dropped
CLIENTS_PER_PROC = 4         # HTTP/2 connections per process (100 streams each)

sys.path.insert(0, APP_DIR)
import demo_config  # noqa: E402
import rt_scale  # noqa: E402  same widget SQL and merchant mapping as the app
from databricks.sdk import WorkspaceClient  # noqa: E402

ANCHOR = demo_config.resolve_anchor(WorkspaceClient(), WAREHOUSE)
WIDGETS = rt_scale.widget_specs(demo_config.FACT, demo_config.EVENTS,
                                demo_config.MONTHLY, ANCHOR)


def worker(k: int, token: str, host: str, t0: float, out_q) -> None:
    import httpx

    async def main():
        hdr = {"Authorization": f"Bearer {token}"}
        clients = [httpx.AsyncClient(base_url=host, http2=True,
                                     limits=httpx.Limits(max_connections=8,
                                                         max_keepalive_connections=8),
                                     timeout=httpx.Timeout(60.0, connect=15.0))
                   for _ in range(CLIENTS_PER_PROC)]
        # Warm each connection before the clock starts, so TLS is not measured.
        await asyncio.gather(*(c.post(rt_scale.STATEMENT_PATH, headers=hdr, json={
            "statement": f"/* warm={uuid.uuid4().hex} */ SELECT 1",
            "warehouse_id": WAREHOUSE, "format": "JSON_ARRAY",
            "disposition": "INLINE", "wait_timeout": "30s"}) for c in clients),
            return_exceptions=True)

        rec = []          # (send_ts, latency_ms, outcome)
        state = {"inflight": 0, "max_inflight": 0, "dropped": 0, "i": 0}
        tasks = set()

        async def one(m: int, w: dict):
            c = clients[state["i"] % len(clients)]
            state["i"] += 1
            sent = time.time()
            outcome = "ok"
            try:
                r = await c.post(rt_scale.STATEMENT_PATH, headers=hdr, json={
                    "statement": f"/* nocache={uuid.uuid4().hex} */ {w['sql'](m)}",
                    "warehouse_id": WAREHOUSE, "format": "JSON_ARRAY",
                    "disposition": "INLINE", "wait_timeout": "30s"})
                if r.status_code != 200:
                    outcome = f"http_{r.status_code}"
                else:
                    st = (r.json().get("status") or {})
                    s = st.get("state")
                    if s != "SUCCEEDED":
                        err = (st.get("error") or {}).get("error_code") or ""
                        outcome = f"{s}:{err}"[:60]
            except Exception as exc:  # noqa: BLE001
                outcome = f"exc_{type(exc).__name__}"
            finally:
                state["inflight"] -= 1
            rec.append((sent, (time.time() - sent) * 1000.0, outcome))

        start = t0
        for users, dur in STEPS:
            mine = [s for s in range(users) if s % PROCS == k]
            events = []
            cycles = int(dur // REFRESH_S) + 1
            for c in range(cycles):
                for s in mine:
                    t = start + (s / users) * REFRESH_S + c * REFRESH_S
                    if t < start + dur:
                        events.append((t, s))
            events.sort()
            for t, s in events:
                d = t - time.time()
                if d > 0:
                    await asyncio.sleep(d)
                m = rt_scale.merchant_for_session(s)
                for w in WIDGETS:
                    if state["inflight"] >= INFLIGHT_CAP:
                        state["dropped"] += 1
                        continue
                    state["inflight"] += 1
                    state["max_inflight"] = max(state["max_inflight"], state["inflight"])
                    task = asyncio.create_task(one(m, w))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
            start += dur
        if tasks:
            await asyncio.wait(tasks, timeout=90)
        for c in clients:
            await c.aclose()
        return rec, state

    rec, state = asyncio.run(main())
    out_q.put((k, rec, state["max_inflight"], state["dropped"]))


def pct(v, p):
    return round(v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))], 1) if v else None


def run() -> None:
    from databricks.sdk.core import Config

    cfg = Config()
    host = cfg.host.rstrip("/")
    host = host if host.startswith("http") else "https://" + host
    token = cfg.authenticate()["Authorization"].split(" ", 1)[1]
    print(f"cpu_count={os.cpu_count()} procs={PROCS} refresh_s={REFRESH_S} "
          f"steps={STEPS} warehouse={WAREHOUSE} anchor={ANCHOR}", flush=True)

    ctx = mp.get_context("fork")
    q = ctx.Queue()
    t0 = time.time() + 20.0
    procs = [ctx.Process(target=worker, args=(k, token, host, t0, q)) for k in range(PROCS)]
    for p in procs:
        p.start()
    results = [q.get() for _ in procs]
    for p in procs:
        p.join(timeout=30)

    rec = [r for _, rr, _, _ in results for r in rr]
    max_inflight = sum(x[2] for x in results)
    dropped = sum(x[3] for x in results)

    print(f"\nwindow_utc_start={time.strftime('%H:%M:%S', time.gmtime(t0))} "
          f"total_sent={len(rec)} dropped={dropped} sum_max_inflight={max_inflight}")
    # Per 10 s bucket, by send time.
    print("\nbucket_utc  users offered_qps achieved_ok_qps errors client_p50 client_p95 client_p99")
    start = t0
    summary = []
    for users, dur in STEPS:
        offered = users * len(WIDGETS) / REFRESH_S
        step_rec = [r for r in rec if start <= r[0] < start + dur]
        for b in range(int(dur // 10)):
            lo = start + b * 10
            br = [r for r in step_rec if lo <= r[0] < lo + 10]
            ok = sorted(r[1] for r in br if r[2] == "ok")
            print(f"{time.strftime('%H:%M:%S', time.gmtime(lo))}  {users:5d} {offered:8.0f} "
                  f"{len(ok)/10:10.1f} {len(br)-len(ok):6d} {pct(ok,50)} {pct(ok,95)} {pct(ok,99)}")
        # Steady state = drop the first 30 s of each step.
        ss = [r for r in step_rec if r[0] >= start + 30]
        ok = sorted(r[1] for r in ss if r[2] == "ok")
        errs = {}
        for r in ss:
            if r[2] != "ok":
                errs[r[2]] = errs.get(r[2], 0) + 1
        span = max(dur - 30, 1)
        summary.append({
            "users": users, "offered_qps": offered,
            "steady_window_utc": [time.strftime('%H:%M:%S', time.gmtime(start + 30)),
                                  time.strftime('%H:%M:%S', time.gmtime(start + dur))],
            "achieved_ok_qps": round(len(ok) / span, 1),
            "sent": len(ss), "errors": sum(errs.values()), "error_kinds": errs,
            "client_ms": {"p50": pct(ok, 50), "p95": pct(ok, 95), "p99": pct(ok, 99)},
        })
        start += dur
    print("\nSUMMARY " + json.dumps({"dropped": dropped, "steps": summary}))


run()
