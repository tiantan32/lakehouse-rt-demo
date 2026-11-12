"""Pre-start and keep the demo's SQL warehouses warm.

This supervisor is launched before Streamlit, so the first browser interaction
does not pay warehouse startup or disk-cache fill. It then runs a lightweight
keepalive thread below the warehouses' auto-stop intervals.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState, State


from demo_config import DBSQL_WAREHOUSE_ID, FACT, MONTHLY, RT_WAREHOUSE_ID, resolve_anchor

WAREHOUSE_IDS = tuple(dict.fromkeys(w for w in (DBSQL_WAREHOUSE_ID, RT_WAREHOUSE_ID) if w))
KEEPALIVE_SECONDS = max(60, int(os.getenv("WAREHOUSE_KEEPALIVE_SECONDS", "180")))


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


def warm_all() -> None:
    if not WAREHOUSE_IDS:
        raise RuntimeError("no DBSQL_WAREHOUSE_ID or RT_WAREHOUSE_ID configured")
    client = WorkspaceClient()
    start_warehouses(client)
    statements = warm_queries(client)
    with ThreadPoolExecutor(max_workers=len(WAREHOUSE_IDS)) as executor:
        futures = [
            executor.submit(warm_one, client, warehouse_id, statements)
            for warehouse_id in WAREHOUSE_IDS
        ]
        for future in futures:
            future.result()
    print(f"warehouse keeper: warmed {len(WAREHOUSE_IDS)} warehouses", flush=True)


def touch_all() -> None:
    """Keep compute alive without competing with the measured dashboard queries."""
    client = WorkspaceClient()
    start_warehouses(client)
    with ThreadPoolExecutor(max_workers=len(WAREHOUSE_IDS)) as executor:
        futures = [
            executor.submit(warm_one_statement, warehouse_id)
            for warehouse_id in WAREHOUSE_IDS
        ]
        for future in futures:
            future.result()


def warm_one_statement(warehouse_id: str) -> None:
    client = WorkspaceClient()
    response = client.statement_execution.execute_statement(
        statement="SELECT 1",
        warehouse_id=warehouse_id,
        wait_timeout="30s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
    )
    wait_for_statement(client, response)


def keepalive_loop(stop_event: threading.Event) -> None:
    while not stop_event.wait(KEEPALIVE_SECONDS):
        try:
            touch_all()
            print(f"warehouse keeper: touched {len(WAREHOUSE_IDS)} warehouses", flush=True)
        except Exception as exc:
            print(f"warehouse keeper: refresh failed: {exc}", flush=True)


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


def serve() -> int:
    """Warm first, then supervise Streamlit and the keepalive thread.

    Warm-up is never fatal: before the data job has run the tables do not exist,
    and the app must still serve so it can say so.
    """
    try:
        warm_all()
    except Exception as exc:  # noqa: BLE001
        print(f"warehouse keeper: warm-up FAILED, serving anyway: {exc}", flush=True)
    kernel_auth_preflight()
    stop_event = threading.Event()
    keeper = threading.Thread(
        target=keepalive_loop,
        args=(stop_event,),
        name="warehouse-keepalive",
        daemon=True,
    )
    keeper.start()

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
