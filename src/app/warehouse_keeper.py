"""Supervise Streamlit and watch the demo's two SQL warehouses.

Nothing here keeps compute alive on its own. The warehouses run only after
someone presses "Pre-warm" in the app or starts a measurement, and this
watchdog stops them once the demo has been idle for
WAREHOUSE_IDLE_STOP_MINUTES (default 20).

Every LOOP_S seconds the watchdog:
  1. Runs the latest button press from the app: warm (start both warehouses,
     wait for RUNNING, fill the disk cache, prove the kernel SQL path) or stop.
  2. Polls warehouse state with a control-plane GET. That is not a query, so
     it never counts as warehouse activity or keeps a warehouse awake.
  3. While the demo is in use (a click within the idle window, or a scaled-load
     run), touches RUNNING warehouses every KEEPALIVE_S so their own auto-stop
     does not fire mid-demo. It never touches a stopped warehouse, because a
     statement would start it.
  4. Once idle past the limit, stops each RUNNING warehouse, unless someone
     other than this app ran a query on it within the window: these can be
     shared warehouses, and their own auto-stop handles that case.

"In use" means a button press in the app (navigation, Run, Start, Pre-warm)
recorded in warehouse_control's activity file. Opening a page does not count.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
import warehouse_control as control
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState, State


from demo_config import DBSQL_WAREHOUSE_ID, FACT, MONTHLY, RT_WAREHOUSE_ID, resolve_anchor

WAREHOUSE_IDS = tuple(dict.fromkeys(w for w in (DBSQL_WAREHOUSE_ID, RT_WAREHOUSE_ID) if w))
KEEPALIVE_SECONDS = max(60, int(os.getenv("WAREHOUSE_KEEPALIVE_SECONDS", "180")))
IDLE_STOP_SECONDS = max(60, int(float(os.getenv("WAREHOUSE_IDLE_STOP_MINUTES", "20")) * 60))
LOOP_SECONDS = 5
POLL_SECONDS = 20
# The app's own queries show up in query history under its service principal's
# application id, which Databricks Apps inject as DATABRICKS_CLIENT_ID.
APP_PRINCIPAL = os.getenv("DATABRICKS_CLIENT_ID", "")


def log(msg: str) -> None:
    print(f"warehouse watchdog: {msg}", flush=True)


def warm_queries(client: WorkspaceClient) -> list[str]:
    """The head-to-head widget SQL, byte-for-byte, plus one per-merchant lookup."""
    import panels

    anchor = resolve_anchor(client, WAREHOUSE_IDS[0])
    return [sp["sql"] for sp in panels.widget_specs(FACT, MONTHLY, anchor)] + [
        f"SELECT dispute_case_id FROM {FACT} "
        "WHERE merchant_id = 100001 AND NOT is_closed "
        "ORDER BY days_to_deadline LIMIT 50",
    ]


def wait_for_statement(client: WorkspaceClient, response, timeout_s: float = 120) -> None:
    deadline = time.monotonic() + timeout_s
    while response.status and response.status.state in {
        StatementState.PENDING,
        StatementState.RUNNING,
    }:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"statement {response.statement_id} exceeded {timeout_s}s")
        time.sleep(0.25)
        response = client.statement_execution.get_statement(response.statement_id)
    if not response.status or response.status.state != StatementState.SUCCEEDED:
        detail = (
            response.status.error.message
            if response.status and response.status.error
            else "unknown error"
        )
        raise RuntimeError(detail)


def start_warehouses(client: WorkspaceClient, timeout_s: float = 300) -> None:
    for warehouse_id in WAREHOUSE_IDS:
        warehouse = client.warehouses.get(id=warehouse_id)
        if warehouse.state != State.RUNNING:
            try:
                client.warehouses.start(id=warehouse_id)
            except Exception:
                pass

    deadline = time.monotonic() + timeout_s
    while WAREHOUSE_IDS:
        states = {
            warehouse_id: client.warehouses.get(id=warehouse_id).state
            for warehouse_id in WAREHOUSE_IDS
        }
        if all(state == State.RUNNING for state in states.values()):
            return
        if time.monotonic() >= deadline:
            shown = {key: value.value for key, value in states.items()}
            raise TimeoutError(f"warehouses did not reach RUNNING: {shown}")
        time.sleep(2)


def warm_one(client: WorkspaceClient, warehouse_id: str, statements: list[str]) -> None:
    # Two passes match the click-time warm-up, but run before Streamlit accepts
    # traffic so they add no delay to the demo interaction.
    for _ in range(2):
        for statement in statements:
            response = client.statement_execution.execute_statement(
                statement=f"/* prewarm={uuid.uuid4().hex} */ {statement}",
                warehouse_id=warehouse_id,
                wait_timeout="30s",
                format=Format.JSON_ARRAY,
                disposition=Disposition.INLINE,
            )
            wait_for_statement(client, response)


def warm_one_statement(client: WorkspaceClient, warehouse_id: str) -> None:
    response = client.statement_execution.execute_statement(
        statement="SELECT 1",
        warehouse_id=warehouse_id,
        wait_timeout="30s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
    )
    wait_for_statement(client, response)


def kernel_auth_preflight() -> None:
    """Prove the kernel SQL path can authenticate, before anyone clicks Start.

    The scaled-load view builds its connection pool on the Start click, so an auth
    failure there is invisible until a demo is already underway: the platform
    reports RUNNING and HTTP 200 either way, and the traceback only appears in the
    app log. One 1-connection pool plus `SELECT 1` is enough to exercise the exact
    token path the pool uses.

    Never fatal. Every other view works without the kernel pool, so a failure here
    is logged loudly and the app still serves.
    """
    rt = os.getenv("RT_WAREHOUSE_ID", "")
    if not rt:
        print("warehouse keeper: kernel preflight skipped, no RT_WAREHOUSE_ID",
              flush=True)
        return
    # Open a few sessions rather than one. Pool construction fans connections out
    # through a capped opener, and the original failure surfaced inside that
    # concurrent open path, so a 1-connection check would miss it. Kept small so
    # app startup stays fast; the token is minted once regardless of pool size,
    # which is the property that makes a 128-session pool safe.
    probe_size = 4
    try:
        from kernel_driver import KernelPool

        started = time.monotonic()
        pool = KernelPool(rt, probe_size)
        opened_ms = (time.monotonic() - started) * 1000.0
        try:
            results = [f.result() for f in
                       [pool.submit("SELECT 1") for _ in range(probe_size)]]
            failed = [r.error for r in results if r.error]
            if failed:
                print(f"warehouse keeper: kernel preflight QUERY FAILED "
                      f"({len(failed)}/{probe_size}): {failed[0]}", flush=True)
            else:
                slowest = max(r.client_ms for r in results)
                print(f"warehouse keeper: kernel preflight ok, {probe_size} "
                      f"sessions opened in {opened_ms:.0f} ms, {probe_size} "
                      f"concurrent SELECT 1 all succeeded "
                      f"(slowest {slowest:.0f} ms)", flush=True)
        finally:
            pool.close()
    except Exception as exc:  # noqa: BLE001
        print(f"warehouse keeper: kernel preflight FAILED: "
              f"{type(exc).__name__}: {exc}", flush=True)


class Watchdog:
    """State machine for the two warehouses. One instance, one thread."""

    def __init__(self, client: WorkspaceClient):
        self.w = client
        self.boot = time.time()
        self.last_stop = self.boot      # keepalive needs activity AFTER this
        self.keeper_activity = 0.0      # set when a pre-warm finishes
        self.states: dict[str, tuple[str, str]] = {}
        self.warm = False               # disk cache filled since the last start
        self.phase = "unknown"
        self.note = ""
        self.kept_on: list[str] = []    # names skipped at idle stop (others using them)
        self.last_poll = self.last_touch = self.next_idle_check = 0.0
        # Never replay a click from before a restart.
        self.handled = (control.read_command() or {}).get("id")

    # ---- inputs -------------------------------------------------------- #
    def activity_ts(self) -> float:
        ts = max(control.read_activity_ts(), self.keeper_activity)
        return time.time() if control.scale_running() else ts

    def running(self) -> list[str]:
        return [wid for wid, (_, st) in self.states.items() if st == "RUNNING"]

    def poll(self) -> None:
        for wid in WAREHOUSE_IDS:
            name = self.states.get(wid, (wid, ""))[0]
            try:
                g = self.w.warehouses.get(id=wid)
                self.states[wid] = (g.name or wid, g.state.value if g.state else "?")
            except Exception as exc:  # noqa: BLE001
                self.states[wid] = (name, "?")
                log(f"could not read {wid}: {exc}")
        if len(self.running()) < len(WAREHOUSE_IDS):
            self.warm = False
        self.last_poll = time.time()
        self.phase = self._derive_phase()

    def _derive_phase(self) -> str:
        st = [v for _, v in self.states.values()]
        if not st or "?" in st:
            return "unknown"
        if all(v == "RUNNING" for v in st):
            return "warm" if self.warm else "running"
        if "STARTING" in st:
            return "starting"
        if "STOPPING" in st:
            return "stopping"
        if all(v in ("STOPPED", "DELETED") for v in st):
            return "off"
        return "partial"

    def others_using(self, wid: str, since_s: float) -> bool:
        """True if anyone but this app ran a query on `wid` since `since_s`."""
        try:
            resp = self.w.api_client.do("GET", "/api/2.0/sql/history/queries", query={
                "max_results": 100, "filter_by.warehouse_ids": wid,
                "filter_by.query_start_time_range.start_time_ms": int(since_s * 1000)})
        except Exception as exc:  # noqa: BLE001 - unknown means do not stop
            log(f"query history unreadable for {wid}, keeping it on: {exc}")
            return True
        return any(q.get("user_name") not in (APP_PRINCIPAL, None)
                   for q in (resp.get("res") or []))

    # ---- actions ------------------------------------------------------- #
    def do_warm(self) -> None:
        self.phase, self.note = "starting", "starting warehouses"
        self.publish()
        try:
            start_warehouses(self.w)
            self.phase, self.note = "warming", "filling the disk cache"
            self.publish()
            statements = warm_queries(self.w)
            with ThreadPoolExecutor(max_workers=len(WAREHOUSE_IDS)) as ex:
                for f in [ex.submit(warm_one, self.w, wid, statements)
                          for wid in WAREHOUSE_IDS]:
                    f.result()
            kernel_auth_preflight()
            self.poll()
            self.warm = len(self.running()) == len(WAREHOUSE_IDS)
            self.phase = self._derive_phase()
            self.note = "pre-warmed"
            self.keeper_activity = self.last_touch = time.time()
            log(f"pre-warmed {len(WAREHOUSE_IDS)} warehouses")
        except Exception as exc:  # noqa: BLE001 - surfaced in the app
            self.note = f"pre-warm failed: {exc}"
            log(self.note)
            self.poll()

    def do_stop(self, wids: list[str], reason: str) -> None:
        self.phase, self.note = "stopping", reason
        self.publish()
        for wid in wids:
            try:
                self.w.warehouses.stop(id=wid)
            except Exception as exc:  # noqa: BLE001
                log(f"stop failed for {wid}: {exc}")
        self.warm = False
        self.last_stop = time.time()
        log(f"stopped {len(wids)} warehouse(s): {reason}")
        self.poll()

    # ---- loop ---------------------------------------------------------- #
    def tick(self) -> None:
        cmd = control.read_command()
        if cmd and cmd.get("id") != self.handled:
            self.handled = cmd["id"]
            if cmd.get("cmd") == "warm":
                self.do_warm()
            elif cmd.get("cmd") == "stop":
                self.do_stop(list(WAREHOUSE_IDS), "turned off from the app")

        now = time.time()
        if now - self.last_poll >= POLL_SECONDS:
            self.poll()

        act = self.activity_ts()
        in_use = act > self.last_stop and now - act < IDLE_STOP_SECONDS
        idle_for = now - max(act, self.boot)

        if in_use and self.running() and now - self.last_touch >= KEEPALIVE_SECONDS:
            self.poll()                      # fresh state: never touch a stopped one
            for wid in self.running():
                try:
                    warm_one_statement(self.w, wid)
                except Exception as exc:  # noqa: BLE001
                    log(f"keepalive failed for {wid}: {exc}")
            self.last_touch = now
        elif (not in_use and idle_for >= IDLE_STOP_SECONDS and self.running()
              and now >= self.next_idle_check):
            self.next_idle_check = now + 60
            since = now - IDLE_STOP_SECONDS
            stop, keep = [], []
            for wid in self.running():
                (keep if self.others_using(wid, since) else stop).append(wid)
            self.kept_on = [self.states[w][0] for w in keep]
            if keep:
                log(f"idle, but someone else used {len(keep)} warehouse(s); leaving them on")
            if stop:
                self.do_stop(stop, f"idle for {IDLE_STOP_SECONDS // 60} min")
        self.publish(act, idle_for)

    def publish(self, act: float | None = None, idle_for: float | None = None) -> None:
        with contextlib.suppress(Exception):
            control.write_status({
                "ts": time.time(), "phase": self.phase, "note": self.note,
                "warm": self.warm, "handled": self.handled,
                "warehouses": [{"id": wid, "name": n, "state": st}
                               for wid, (n, st) in self.states.items()],
                "idle_for_s": idle_for, "idle_stop_s": IDLE_STOP_SECONDS,
                "in_use": bool(act and act > self.last_stop
                               and time.time() - act < IDLE_STOP_SECONDS),
                "kept_on": self.kept_on,
            })

    def run(self, stop_event: threading.Event) -> None:
        log(f"watching {len(WAREHOUSE_IDS)} warehouses, idle stop after "
            f"{IDLE_STOP_SECONDS // 60} min, keepalive every {KEEPALIVE_SECONDS} s while in use")
        while True:
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001 - the loop must survive
                log(f"tick failed: {exc}")
            if stop_event.wait(LOOP_SECONDS):
                return


def serve() -> int:
    """Start the watchdog, then supervise Streamlit. Nothing is warmed at boot."""
    stop_event = threading.Event()
    if WAREHOUSE_IDS:
        watchdog = Watchdog(WorkspaceClient())
        threading.Thread(target=watchdog.run, args=(stop_event,),
                         name="warehouse-watchdog", daemon=True).start()

    port = os.getenv("DATABRICKS_APP_PORT", "8000")
    streamlit = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "app.py",
            "--server.port",
            port,
            "--server.address",
            "0.0.0.0",
        ]
    )

    def stop(_signum, _frame) -> None:
        stop_event.set()
        if streamlit.poll() is None:
            streamlit.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        return streamlit.wait()
    finally:
        stop_event.set()
        if streamlit.poll() is None:
            streamlit.terminate()
            try:
                streamlit.wait(timeout=10)
            except subprocess.TimeoutExpired:
                streamlit.kill()
                streamlit.wait()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()
    if not args.serve:
        parser.error("choose --serve")
    raise SystemExit(serve())


if __name__ == "__main__":
    main()
