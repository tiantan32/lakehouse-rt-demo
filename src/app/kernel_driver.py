"""Persistent kernel-backed SQL driver for Lakehouse RT demo workloads.

Lakehouse RT's optimized Python path is the Databricks SQL Connector with
``use_kernel=True``. A bounded pool is intentional: simulated browser sessions
may number in the thousands, but creating one Python thread and one TLS/OAuth
session per browser makes the Streamlit process the load-test bottleneck.
"""

from __future__ import annotations

import concurrent.futures
import queue
import random
import time
from dataclasses import dataclass
from typing import Callable, Sequence

import pandas as pd


@dataclass(frozen=True)
class QueryResult:
    client_ms: float
    frame: pd.DataFrame | None
    error: str | None
    statement_id: str | None


ProgressCallback = Callable[[int, int, Sequence[QueryResult], float], None]


class KernelPool:
    """A persistent set of one-in-flight-query SQL sessions."""

    def __init__(self, warehouse_id: str, size: int,
                 disable_result_cache: bool = False):
        from databricks import sql
        from databricks.sdk.core import Config

        self.warehouse_id = warehouse_id
        self.size = max(1, int(size))
        self.disable_result_cache = bool(disable_result_cache)
        self._sql = sql
        self._cfg = Config()
        self._token_expires_at = 0.0
        self._host = self._cfg.host.removeprefix("https://").removeprefix(
            "http://"
        ).rstrip("/")
        self._auth_kwargs = self._kernel_auth_kwargs()
        self._available: queue.Queue = queue.Queue()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.size,
            thread_name_prefix=f"sql-kernel-{warehouse_id[-6:]}",
        )

        # Open sessions once, but cap the TLS/OAuth connection fan-out. At 256
        # measured sessions, opening every socket in the same instant can make
        # the app -- rather than RT -- the bottleneck before the test even starts.
        # Connection setup remains outside measured query latency.
        opener = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(self.size, 8),
            thread_name_prefix=f"sql-open-{warehouse_id[-6:]}",
        )
        futures = [opener.submit(self._connect_with_retry) for _ in range(self.size)]
        opened = []
        try:
            first_error = None
            for future in futures:
                try:
                    connection = future.result()
                    opened.append(connection)
                    self._available.put(connection)
                except Exception as exc:
                    first_error = first_error or exc
            if first_error is not None:
                raise first_error
        except Exception:
            for connection in opened:
                try:
                    connection.close()
                except Exception:
                    pass
            self._executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            opener.shutdown(wait=True, cancel_futures=True)

    def _kernel_auth_kwargs(self) -> dict:
        """Hand every connection ONE pre-minted bearer token.

        Do NOT pass `oauth_client_id`/`oauth_client_secret` here. The Rust kernel
        runs its own client-credentials grant once PER CONNECTION, so building a
        128-connection pool fires 128 grants in a burst. In the Databricks Apps
        container that fails:

            databricks_sql_kernel.KernelError: could not client credentials grant:
              M2M token exchange failed: Failed to parse server response

        Every attempt in `_connect_with_retry` hits the same wall, so the pool
        constructor raises and the whole scaled-load view dies on the Start click.
        The app still reports RUNNING and HTTP 200, so this is visible only in
        `databricks apps logs` as an uncaught exception.

        Minting once here replaces N grants with 1: the burst cannot throttle
        itself, and pool construction gets faster as a side effect. The plain
        `/oidc/v1/token` POST is verified working in the same container where the
        kernel's own exchange fails.
        """
        token, expires_at = self._mint_token()
        self._token_expires_at = expires_at
        return {"access_token": token}

    def _mint_token(self) -> tuple[str, float]:
        """Return (bearer token, unix expiry).

        Each failure mode is checked on its own rather than trusting a 200 and
        calling `.json()`: an unparseable body is exactly what the kernel chokes
        on, so this must not repeat that mistake with a bare parse.
        """
        host = (self._cfg.host or "").rstrip("/")
        if host and not host.startswith("http"):
            host = "https://" + host

        # Databricks Apps inject OAuth M2M credentials.
        if self._cfg.client_id and self._cfg.client_secret and host:
            import requests

            try:
                resp = requests.post(
                    f"{host}/oidc/v1/token",
                    auth=(self._cfg.client_id, self._cfg.client_secret),
                    data={"grant_type": "client_credentials", "scope": "all-apis"},
                    timeout=30,
                )
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"M2M token request failed: {exc}") from exc
            if resp.status_code != 200:
                raise RuntimeError(
                    f"M2M token endpoint returned HTTP {resp.status_code}: "
                    f"{resp.text[:200]}"
                )
            try:
                body = resp.json() or {}
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"M2M token response was not JSON: {resp.text[:200]}"
                ) from exc
            token = body.get("access_token")
            if not token:
                raise RuntimeError("M2M token response carried no access_token")
            # Refresh slightly early; a token that expires mid-demo looks like a
            # warehouse failure rather than an auth one.
            return token, time.time() + float(body.get("expires_in", 3600))

        # Local/notebook OAuth profiles expose a short-lived bearer token. The SDK
        # does not report its lifetime, so assume a conservative 45 minutes.
        headers = self._cfg.authenticate()
        auth = headers.get("Authorization") or headers.get("authorization")
        if not auth or not auth.lower().startswith("bearer "):
            raise RuntimeError("Databricks authentication did not provide a bearer token")
        return auth.split(" ", 1)[1], time.time() + 2700.0

    def is_stale(self, skew_s: float = 300.0) -> bool:
        """True when the pool's token is at or near expiry.

        The pool is cached for the app's lifetime while the token lives ~1 hour, so
        a demo run more than an hour after app start would otherwise fail on every
        statement with an auth error that reads like a warehouse problem.
        """
        expires_at = getattr(self, "_token_expires_at", 0.0)
        return bool(expires_at) and time.time() >= (expires_at - skew_s)

    def _connect(self):
        return self._sql.connect(
            server_hostname=self._host,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            use_kernel=True,
            session_configuration=(
                {"use_cached_result": "false"}
                if self.disable_result_cache else None
            ),
            **self._auth_kwargs,
        )

    def _connect_with_retry(self):
        """Absorb transient M2M throttling while establishing a large pool."""
        for attempt in range(6):
            try:
                return self._connect()
            except Exception:
                if attempt == 5:
                    raise
                time.sleep(min(4.0, 0.25 * (2**attempt)) + random.uniform(0.0, 0.2))

    def execute(self, statement: str, include_frame: bool = False) -> QueryResult:
        connection = self._available.get()
        cursor = None
        started = time.perf_counter()
        try:
            cursor = connection.cursor()
            cursor.execute(statement)
            rows = cursor.fetchall()
            columns = [item[0] for item in (cursor.description or [])]
            frame = pd.DataFrame(rows, columns=columns) if include_frame else None
            return QueryResult(
                client_ms=(time.perf_counter() - started) * 1000.0,
                frame=frame,
                error=None,
                statement_id=cursor.query_id,
            )
        except Exception as exc:  # every failed query is surfaced in the UI
            statement_id = getattr(cursor, "query_id", None) if cursor else None
            return QueryResult(
                client_ms=(time.perf_counter() - started) * 1000.0,
                frame=None,
                error=str(exc),
                statement_id=statement_id,
            )
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            self._available.put(connection)

    def submit(self, statement: str, include_frame: bool = False):
        return self._executor.submit(self.execute, statement, include_frame)

    def run(
        self,
        warmup_statements: Sequence[str],
        measured_statements: Sequence[str],
        *,
        on_progress: ProgressCallback | None = None,
        progress_interval_s: float = 0.35,
    ) -> tuple[list[QueryResult], float]:
        if warmup_statements:
            concurrent.futures.wait(
                [self.submit(statement) for statement in warmup_statements]
            )

        started = time.perf_counter()
        pending = {self.submit(statement) for statement in measured_statements}
        results: list[QueryResult] = []
        last_paint = 0.0
        total = len(pending)
        while pending:
            finished, pending = concurrent.futures.wait(
                pending,
                timeout=progress_interval_s,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            results.extend(future.result() for future in finished)
            elapsed = time.perf_counter() - started
            if on_progress and (
                not pending or elapsed - last_paint >= progress_interval_s
            ):
                on_progress(len(results), total, results, elapsed)
                last_paint = elapsed
        return results, time.perf_counter() - started

    def close(self) -> None:
        connections = []
        while True:
            try:
                connections.append(self._available.get_nowait())
            except queue.Empty:
                break
        for connection in connections:
            try:
                connection.close()
            except Exception:
                pass
        self._executor.shutdown(wait=False, cancel_futures=True)
