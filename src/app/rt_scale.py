"""Lakehouse//RT scaled-load view: open-loop driver + CSS-only dashboard wall.

Load model. Every simulated user is one dashboard of 4 widget queries scoped to
its own merchant, refreshed every refresh_s seconds. Refreshes are spread evenly
across the interval and launched on a wall clock (open loop), so the offered
rate is fixed by the user count: N x 4 / refresh_s. A slow warehouse shows up as
rising latency, never as a quietly lower arrival rate.

Why asyncio and not threads: OS threads got SLOWER as they widened (213 q/s at
32 threads down to 50 q/s at 128), because the GIL plus per-thread TLS costs more
than the queries do. One asyncio loop over a pool of HTTP/2 connections reaches
the warehouse limit instead of its own.

Why a connection pool: one HTTP/2 connection carries at most 100 concurrent
streams, so a single client caps the app at 100 requests in flight and the app,
not the warehouse, becomes the bottleneck (measured: 504 of 1,200 q/s achieved
at 3,000 users with one connection).

Reference points measured 2026-11-12 on a Small Lakehouse//RT warehouse with
autoscale up to 96 DBU/h (engine time from query history):

    400 q/s  ->  engine p50 22-26 ms, 0 errors                    healthy
    800 q/s  ->  ~782 achieved, engine p50 ~1.2 s, ~2% HTTP 503   past the knee

Throughput for light lookups is bounded per warehouse (one driver per
warehouse), not by executor CPU or the DBU cap.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import pathlib
import tempfile
import threading
import time
import uuid
from collections import deque

# The frontend is served as a Streamlit static file and polls this JSON. Streamlit
# owns the single port and exposes no custom endpoints, so a file is the only
# same-origin channel available to it.
STATIC_DIR = pathlib.Path(__file__).parent / "static"
STATE_PATH = STATIC_DIR / "rt_state.json"
FRONTEND_PATH = STATIC_DIR / "rt_scale.html"
PUBLISH_INTERVAL_S = 0.25


def frontend_html() -> str:
    """The scale view's page, injected via components.html as srcdoc.

    NOT served by iframe src: Streamlit's static handler returns every non-image
    file as `content-type: text/plain` (it will not host HTML/JS on the app
    origin), so the browser rendered the page as literal source text. srcdoc
    executes normally. The string is constant, so Streamlit never re-renders the
    component and the wall is never rebuilt — the page fetches its own state.
    """
    return FRONTEND_PATH.read_text()


def publish_state(payload: dict) -> None:
    """Write the state file atomically.

    Rename, not write-in-place: the frontend polls 4x/s and would otherwise read
    a half-written file and throw on JSON.parse.
    """
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(STATIC_DIR), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        # mkstemp creates 0600; the static file server must be able to read it.
        os.chmod(tmp, 0o644)
        os.replace(tmp, STATE_PATH)
    except Exception:
        with contextlib.suppress(Exception):
            os.unlink(tmp)
        raise


def static_writable() -> str | None:
    """Return None if the state file can be written, else why not.

    The app's source tree is downloaded into the container and is normally
    writable, but that is not guaranteed, and a read-only static dir would break
    the feed with no visible cause.
    """
    try:
        publish_state({"probe": True})
        return None
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {exc}"

# Distinct result sets published to the UI per widget. Cards map onto slots by
# index. Publishing a single series per widget made every card render the same
# data and flip in unison, which reads as fake.
PAINT_SLOTS = 32
PAINT_ROWS = 16
SLICE_S = 0.05
STATEMENT_PATH = "/api/2.0/sql/statements"
HISTORY_PATH = "/api/2.0/sql/history/queries"
# How far behind "now" the engine-time sampler reads, so it sees final records.
HISTORY_SETTLE_MS = 3000

from demo_config import MERCHANT_BASE, MERCHANT_COUNT, days_before  # noqa: E402


# --------------------------------------------------------------------------- #
# Widget set. Four statements per dashboard refresh, each scoped to one merchant
# so liquid clustering prunes to roughly one file.
# --------------------------------------------------------------------------- #
def widget_specs(fact: str, events: str, monthly: str, anchor) -> list[dict]:
    """`anchor` is the last date in the data (demo_config.resolve_anchor)."""
    d89 = days_before(anchor, 89)
    d103 = days_before(anchor, 103)
    return [
        {
            "key": "spend_movement",
            "label": "SPEND MOVEMENT",
            "caption": "Live transaction activity",
            "kind": "bar",
            "unit": "usd",
            "sql": lambda m: (
                f"SELECT date_format(chargeback_filing_date,'MM-dd') AS bucket, "
                f"CAST(SUM(chargeback_amount) AS DOUBLE) AS value "
                f"FROM {fact} WHERE merchant_id = {m} "
                f"AND chargeback_filing_date >= DATE '{d89}' "
                f"GROUP BY bucket ORDER BY bucket"
            ),
        },
        {
            "key": "transaction_volume",
            "label": "TRANSACTION VOLUME",
            "caption": "Transactions processed by the decision stream",
            "kind": "bar",
            "unit": "count",
            "sql": lambda m: (
                f"SELECT date_format(event_ts,'MM-dd') AS bucket, "
                f"CAST(COUNT(*) AS DOUBLE) AS value "
                f"FROM {events} WHERE merchant_id = {m} "
                f"AND event_ts >= TIMESTAMP '{d89}' "
                f"GROUP BY bucket ORDER BY bucket"
            ),
        },
        {
            "key": "auto_review_rate",
            "label": "AUTO-REVIEW RATE",
            "caption": "Share reviewed without finance intervention",
            "kind": "bar",
            "unit": "pct",
            "sql": lambda m: (
                f"SELECT filing_year_month AS bucket, "
                f"CAST(representment_rate AS DOUBLE) AS value "
                f"FROM {monthly} WHERE merchant_id = {m} ORDER BY bucket"
            ),
        },
        {
            "key": "policy_exceptions",
            "label": "POLICY EXCEPTIONS",
            "caption": "Flagged transactions requiring review",
            "kind": "line",
            "unit": "count",
            "sql": lambda m: (
                f"SELECT date_format(chargeback_filing_date,'MM-dd') AS bucket, "
                f"CAST(SUM(CASE WHEN is_deadline_at_risk THEN 1 ELSE 0 END) AS DOUBLE) "
                f"AS value FROM {fact} WHERE merchant_id = {m} "
                f"AND chargeback_filing_date >= DATE '{d103}' "
                f"GROUP BY bucket ORDER BY bucket"
            ),
        },
    ]


def merchant_for_session(i: int) -> int:
    return MERCHANT_BASE + (i * 7919) % MERCHANT_COUNT


def widget_meta(widgets: list[dict]) -> list[dict]:
    """Frontend-facing widget fields. Drops the `sql` callables, which are not
    JSON-serialisable."""
    return [{k: w[k] for k in ("key", "label", "caption", "kind", "unit")}
            for w in widgets]


def publish_idle(config: dict, last: dict | None = None) -> None:
    """State for a stopped app: config present, metrics empty or the last run.

    With `last` (the final snapshot of the run that was just stopped), the
    frontend keeps showing that run's P50/P95/QPS and wall, marked as stopped,
    instead of blanking everything the moment Stop is pressed.
    """
    if last:
        frozen = dict(last)
        frozen.update(running=False, stopped=True, inflight=0)
        publish_state({"stamp": time.time(), "config": config, "metrics": frozen})
        return
    publish_state({
        "stamp": time.time(),
        "config": config,
        "metrics": {"running": False, "sessions": config.get("sessions_selected", 1),
                    "widgets": config.get("widget_count", 4),
                    "refresh_s": config.get("default_refresh_s", 20.0),
                    "offered_qps": 0.0, "qps": 0.0, "completed": 0,
                    "errors": 0, "dropped": 0, "inflight": 0,
                    "engine_samples": 0, "paint": {}},
    })


# --------------------------------------------------------------------------- #
# Auth. Carried over from the previous open-loop driver, which was reverted for
# UI reasons while this fix was kept: the Rust SQL kernel fired one OAuth grant
# per connection and a 128-connection pool overwhelmed the token endpoint. Mint
# ONCE and reuse, and treat every M2M failure mode explicitly rather than
# trusting a 200 (an unparseable body is exactly how "Failed to parse server
# response" surfaced).
# --------------------------------------------------------------------------- #
def bearer_token() -> tuple[str, str, str]:
    """Return (token, host, source). Falls back to the SDK for local profiles."""
    from databricks.sdk.core import Config

    cfg = Config()
    host = (cfg.host or "").rstrip("/")
    if host and not host.startswith("http"):
        host = "https://" + host
    if not host:
        raise RuntimeError("no Databricks host resolved for the scale driver")

    note = "m2m unavailable (no client_id/secret)"
    cid, secret = cfg.client_id, cfg.client_secret
    if cid and secret:
        import requests

        try:
            resp = requests.post(
                f"{host}/oidc/v1/token",
                auth=(cid, secret),
                data={"grant_type": "client_credentials", "scope": "all-apis"},
                timeout=30,
            )
        except Exception as exc:  # noqa: BLE001
            note = f"m2m request failed: {type(exc).__name__}"
        else:
            if resp.status_code != 200:
                note = f"m2m HTTP {resp.status_code}: {resp.text[:120]}"
            else:
                try:
                    token = (resp.json() or {}).get("access_token")
                except Exception as exc:  # noqa: BLE001
                    note = f"m2m response was not JSON: {type(exc).__name__}"
                else:
                    if token:
                        return token, host, "m2m"
                    note = "m2m response carried no access_token"

    headers = cfg.authenticate()
    auth = headers.get("Authorization") or headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise RuntimeError(f"no bearer token available for the scale driver ({note})")
    return auth.split(" ", 1)[1], host, f"sdk ({note})"


def preflight() -> dict:
    """Verify the driver's dependencies before a demo, without running load.

    Both hard dependencies otherwise fail late: httpx is imported inside the run
    loop and the token is minted on first dispatch, so either would surface as a
    dead Start button in front of an audience.
    """
    out: dict = {"ok": False, "httpx": None, "auth": None, "host": None,
                 "static": None}
    try:
        import httpx

        out["httpx"] = getattr(httpx, "__version__", "present")
    except Exception as exc:  # noqa: BLE001
        out["httpx"] = f"MISSING: {type(exc).__name__}: {exc}"
        return out
    problem = static_writable()
    out["static"] = problem or "writable"
    if problem:
        return out
    try:
        token, host, source = bearer_token()
        out["auth"], out["host"], out["ok"] = source, host, bool(token)
    except Exception as exc:  # noqa: BLE001
        out["auth"] = f"FAILED: {type(exc).__name__}: {exc}"
    return out


class _ClientPool:
    """Round-robin over several single-connection httpx clients.

    Exposes `.post` like an AsyncClient, so call sites do not change. A client
    whose HTTP/2 connection fails at the protocol level (for example after a
    server GOAWAY: `LocalProtocolError`) is replaced, so one dead connection
    does not keep failing every Nth request.
    """

    def __init__(self, factory, n: int):
        self._factory = factory
        self._clients = [factory() for _ in range(n)]
        self._i = 0

    def __len__(self) -> int:
        return len(self._clients)

    async def post(self, *args, **kwargs):
        import httpx

        k = self._i % len(self._clients)
        self._i += 1
        client = self._clients[k]
        try:
            return await client.post(*args, **kwargs)
        except (httpx.LocalProtocolError, httpx.RemoteProtocolError):
            if self._clients[k] is client:
                self._clients[k] = self._factory()
                with contextlib.suppress(Exception):
                    await client.aclose()
            raise

    async def aclose(self) -> None:
        for c in self._clients:
            with contextlib.suppress(Exception):
                await c.aclose()


def _pct(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    idx = min(len(vals) - 1, max(0, int(round((p / 100.0) * (len(vals) - 1)))))
    return round(vals[idx], 1)


# --------------------------------------------------------------------------- #
class ScaleDriver:
    """Continuous open-loop run. Poll `snapshot()` from Streamlit; never block.

    Runs its own event loop on a daemon thread because Streamlit owns the main
    thread and reruns constantly. State is guarded by a lock and only ever
    copied out.
    """

    def __init__(self, warehouse_id: str, widgets: list[dict],
                 max_inflight: int = 1200):
        self.warehouse_id = warehouse_id
        self.widgets = widgets
        self.max_inflight = max_inflight

        self.sessions = 0
        self.refresh_s = 20.0
        self.running = False
        # Final snapshot of the last stopped run, shown until the next Start.
        self.last_result: dict | None = None
        # Set by the view; echoed into the published state so the frontend can
        # configure itself (scale ladder, widget labels, warehouse id).
        self.ui_config: dict = {}

        self._lock = threading.Lock()
        self._samples: deque = deque()      # (monotonic, client_ms)
        self._reset_at = time.monotonic()
        self._window_s = 30.0
        self._completed = 0
        self._errors = 0
        self._dropped = 0
        self._inflight = 0
        self._last_error: str | None = None
        self._engine: deque = deque()       # (monotonic, duration_ms)
        self._engine_seen: set = set()
        self._history_error: str | None = None
        self._auth_source: str | None = None
        # widget key -> {slot: {"m": merchant, "r": rows}}
        self._paint: dict[str, dict] = {w["key"]: {} for w in widgets}

        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------ #
    def start(self, sessions: int, refresh_s: float) -> None:
        self.stop()
        self.last_result = None      # a new run replaces the frozen result
        self.sessions = max(1, int(sessions))
        self.refresh_s = max(1.0, float(refresh_s))
        with self._lock:
            self._samples.clear()
            self._engine.clear()
            self._engine_seen.clear()
            self._completed = self._errors = self._dropped = 0
            self._last_error = self._history_error = None
            self._reset_at = time.monotonic()
            # The window must span a full refresh cycle, else low session counts
            # report no samples at all (1 session emits only 4 per cycle).
            self._window_s = max(5.0, self.refresh_s * 1.5)
            for slots in self._paint.values():
                slots.clear()
        self._stop.clear()
        self.running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True,
                                        name="rt-scale-driver")
        self._thread.start()

    def stop(self) -> None:
        was_running = self.running
        # Freeze the run's final numbers BEFORE tearing down, so Stop keeps the
        # result on screen (marked "stopped") instead of blanking P50/P95/QPS.
        final = self.snapshot() if was_running else None
        self._stop.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=6.0)
        self._thread = None
        self.running = False
        if final is not None:
            self.last_result = final
        with self._lock:
            for slots in self._paint.values():
                slots.clear()
            self._inflight = 0
        # The publisher task died with the thread, so emit a final stopped state
        # here, carrying the frozen result.
        with contextlib.suppress(Exception):
            if self.ui_config:
                publish_idle(self.ui_config, self.last_result)

    def reset(self) -> None:
        """Back to a blank page: stop any run and forget the frozen result.

        The frozen result exists so Stop leaves the finished run on screen. It is
        dropped when the viewer leaves the view or reloads the page, so the next
        visit starts from scratch instead of showing an old run.
        """
        self.stop()
        self.last_result = None
        with contextlib.suppress(Exception):
            if self.ui_config:
                publish_idle(self.ui_config, None)

    # ------------------------------------------------------------------ #
    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self._last_error = f"{type(exc).__name__}: {exc}"[:240]
        finally:
            with contextlib.suppress(Exception):
                loop.close()

    async def _main(self) -> None:
        import httpx

        token, host, source = bearer_token()
        with self._lock:
            self._auth_source = source

        limits = httpx.Limits(max_connections=self.max_inflight,
                              max_keepalive_connections=self.max_inflight,
                              keepalive_expiry=120.0)
        # HTTP/2 multiplexes the Statement Execution API calls over a few
        # connections (MAX_CONCURRENT_STREAMS=100). On HTTP/1.1 one process
        # collapses between 32 and 64 in flight, and 400 q/s needs ~35.
        try:
            import h2  # noqa: F401
            use_h2 = True
        except ImportError:
            use_h2 = False
        # ONE HTTP/2 connection caps the app at 100 concurrent requests (the
        # server's MAX_CONCURRENT_STREAMS); httpx queues the rest on that
        # connection instead of opening another. Measured 2026-11-12 at 3,000
        # users: 1,199 in flight, 504 of 1,200 q/s achieved, client p50 2.35 s
        # while engine p50 was 32 ms -- the app, not the warehouse, was the limit.
        # A pool of single-connection clients gives ~90 streams each.
        n_conn = max(4, -(-self.max_inflight // 90)) if use_h2 else 1
        pool = _ClientPool(lambda: httpx.AsyncClient(
            base_url=host, http2=use_h2,
            limits=(httpx.Limits(max_connections=1, max_keepalive_connections=1)
                    if use_h2 else limits),
            timeout=httpx.Timeout(60.0, connect=15.0)), n_conn)
        # The engine sampler gets its OWN connection. Sharing the statement
        # connection starved it behind the load and blanked the P50/P95 tiles.
        sampler_client = httpx.AsyncClient(base_url=host,
                                           timeout=httpx.Timeout(30.0, connect=15.0))
        print(f"scale driver: sessions={self.sessions} refresh_s={self.refresh_s} "
              f"http2={use_h2} connections={n_conn}", flush=True)
        try:
            hdr = {"Authorization": f"Bearer {token}"}
            await self._warm(pool, hdr)
            sampler = asyncio.create_task(self._sample_engine(sampler_client, hdr))
            publisher = asyncio.create_task(self._publish_loop())
            try:
                await self._schedule(pool, hdr)
            finally:
                for t in (sampler, publisher):
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t
        finally:
            await pool.aclose()
            await sampler_client.aclose()

    async def _publish_loop(self) -> None:
        """Write the state file for the frontend to poll."""
        while not self._stop.is_set():
            with contextlib.suppress(Exception):
                publish_state({"stamp": time.time(),
                               "config": self.ui_config,
                               "metrics": self.snapshot()})
            await asyncio.sleep(PUBLISH_INTERVAL_S)

    async def _warm(self, client, hdr) -> None:
        """Pre-open connections. Each statement is one HTTPS round trip (RT
        refuses Thrift), so without this the first cycle measures TLS setup."""
        want = int(min(self.max_inflight,
                       max(4, self.sessions * len(self.widgets) / self.refresh_s * 0.15)))
        # At least one ping per pooled connection, so every connection is open
        # (TLS done) before the measured run starts.
        want = max(want, len(client) if hasattr(client, "__len__") else 0)

        async def ping():
            with contextlib.suppress(Exception):
                await client.post(STATEMENT_PATH, headers=hdr, json={
                    "statement": f"/* warm={uuid.uuid4().hex} */ SELECT 1",
                    "warehouse_id": self.warehouse_id,
                    "format": "JSON_ARRAY", "disposition": "INLINE",
                    "wait_timeout": "30s"})

        await asyncio.gather(*(ping() for _ in range(want)))

    async def _schedule(self, client, hdr) -> None:
        """Wall-clock arrival scheduler with per-session phase offsets."""
        slices = max(1, int(round(self.refresh_s / SLICE_S)))
        pending: set = set()
        t0 = time.monotonic()
        i = 0
        while not self._stop.is_set():
            lo = (self.sessions * (i % slices)) // slices
            hi = (self.sessions * ((i % slices) + 1)) // slices
            for s in range(lo, hi):
                m = merchant_for_session(s)
                for w in range(len(self.widgets)):
                    with self._lock:
                        if self._inflight >= self.max_inflight:
                            self._dropped += 1
                            continue
                        self._inflight += 1
                    t = asyncio.create_task(self._one(client, hdr, s, m, w))
                    pending.add(t)
                    t.add_done_callback(pending.discard)
            i += 1
            delay = (t0 + i * SLICE_S) - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            else:
                # Fell behind: re-anchor so the rate degrades instead of bursting.
                t0 = time.monotonic() - i * SLICE_S
        for t in list(pending):
            t.cancel()

    async def _one(self, client, hdr, session_i: int, merchant: int, w_i: int) -> None:
        spec = self.widgets[w_i]
        # Unique comment defeats the DBSQL result cache: the API takes exactly one
        # statement, so `SET use_cached_result` is not available, and SET is
        # unsupported on RT regardless.
        stmt = f"/* nocache={uuid.uuid4().hex} s={session_i} */ {spec['sql'](merchant)}"
        t_start = time.perf_counter()
        try:
            r = await client.post(STATEMENT_PATH, headers=hdr, json={
                "statement": stmt, "warehouse_id": self.warehouse_id,
                # Pinned: RT defaults to ARROW_STREAM, and a client reading only
                # data_array then gets zero rows while state == SUCCEEDED.
                "format": "JSON_ARRAY", "disposition": "INLINE",
                "wait_timeout": "30s", "on_wait_timeout": "CANCEL"})
            elapsed = (time.perf_counter() - t_start) * 1000.0
            if r.status_code != 200:
                self._record_error(f"HTTP {r.status_code}: {r.text[:160]}")
                return
            body = r.json()
            state = (body.get("status") or {}).get("state")
            if state != "SUCCEEDED":
                msg = ((body.get("status") or {}).get("error") or {}).get("message", state)
                self._record_error(f"{state}: {msg}")
                return
            result = body.get("result") or {}
            rows = result.get("data_array")
            if not rows and result.get("attachment"):
                self._record_error("Arrow attachment returned despite JSON_ARRAY")
                return
            with self._lock:
                self._samples.append((time.monotonic(), elapsed))
                self._completed += 1
                if rows:
                    self._paint[spec["key"]][session_i % PAINT_SLOTS] = {
                        "m": merchant, "r": rows[-PAINT_ROWS:]}
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._record_error(f"{type(exc).__name__}: {exc}")
        finally:
            with self._lock:
                self._inflight -= 1

    def _record_error(self, msg: str) -> None:
        with self._lock:
            self._errors += 1
            self._last_error = msg[:240]

    async def _sample_engine(self, client, hdr) -> None:
        """Sample engine duration from query history once a second.

        History is the only source of engine time: the Statement API response
        carries no timing. On RT the `metrics` object is empty, so `duration` is
        all there is, and it covers execution ONLY (a statement is not registered
        until admitted), which is why client and engine are reported separately.
        """
        while not self._stop.is_set():
            try:
                # End the window HISTORY_SETTLE_MS in the past. History returns the
                # newest 100 rows first; at 400 q/s those span ~0.25 s and are still
                # provisional (is_final=false), so a window ending "now" kept 0-2 of
                # 100 and the P50/P95 tiles stayed blank. Measured 2026-11-12: rows
                # ending >= 2 s ago were 100/100 final.
                now_ms = int(time.time() * 1000) - HISTORY_SETTLE_MS
                body = {"filter_by": {
                    "warehouse_ids": [self.warehouse_id],
                    "query_start_time_range": {
                        "start_time_ms": now_ms - int(self._window_s * 1000),
                        "end_time_ms": now_ms}},
                    "max_results": 100, "include_metrics": False}
                r = await client.request("GET", HISTORY_PATH, headers=hdr, json=body)
                if r.status_code == 405:
                    r = await client.post(HISTORY_PATH, headers=hdr, json=body)
                if r.status_code != 200:
                    with self._lock:
                        self._history_error = f"history HTTP {r.status_code}"
                else:
                    rows = r.json().get("res") or []
                    # Stride across the page: history pages at 100 rows newest
                    # first, so a contiguous tail oversamples the most saturated
                    # instant and inflates the reported p95.
                    keep = rows if len(rows) <= 60 else rows[:: max(1, len(rows) // 60)]
                    with self._lock:
                        for row in keep:
                            # Gate on is_final: the first record is provisional
                            # and reads up to 23% high.
                            if not row.get("is_final") or row.get("status") != "FINISHED":
                                continue
                            qid = row.get("query_id") or row.get("statement_id")
                            if qid and qid in self._engine_seen:
                                continue
                            if qid:
                                self._engine_seen.add(qid)
                            if row.get("duration") is not None:
                                self._engine.append((time.monotonic(),
                                                     float(row["duration"])))
                        if len(self._engine_seen) > 20000:
                            self._engine_seen.clear()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._history_error = f"{type(exc).__name__}: {exc}"[:160]
            await asyncio.sleep(1.0)

    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            cutoff = now - self._window_s
            while self._samples and self._samples[0][0] < cutoff:
                self._samples.popleft()
            while self._engine and self._engine[0][0] < cutoff:
                self._engine.popleft()
            cli = sorted(v for _, v in self._samples)
            eng = sorted(v for _, v in self._engine)
            span = min(self._window_s, max(now - self._reset_at, 1e-9))
            paint = {k: [v.get(i) for i in range(PAINT_SLOTS)]
                     for k, v in self._paint.items()}
            merchants = len({e["m"] for v in self._paint.values() for e in v.values()})
            out = {
                "running": self.running,
                "sessions": self.sessions,
                "refresh_s": self.refresh_s,
                "widgets": len(self.widgets),
                "offered_qps": round(self.sessions * len(self.widgets)
                                     / self.refresh_s, 1),
                # Divide by the span actually observed, capped at the window.
                # Dividing by the nominal window before it fills understated the
                # rate ~10x and made it look like it was climbing on its own.
                "qps": round(len(cli) / span, 1) if cli and span >= 0.5 else 0.0,
                "window_span_s": round(span, 1),
                "window_full": span >= self._window_s - 0.05,
                "client_p50": _pct(cli, 50), "client_p95": _pct(cli, 95),
                "engine_p50": _pct(eng, 50), "engine_p95": _pct(eng, 95),
                "engine_samples": len(eng),
                "completed": self._completed, "errors": self._errors,
                "dropped": self._dropped, "inflight": self._inflight,
                "last_error": self._last_error,
                "history_error": self._history_error,
                "auth": self._auth_source,
                "merchants_shown": merchants,
                "paint": paint if self.running else {},
            }
        return out
