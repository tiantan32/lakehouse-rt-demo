"""Shared state between the app's warehouse buttons and the watchdog.

The Streamlit app and the watchdog (warehouse_keeper.py) are separate processes
in the same container, so they talk through three small JSON files:

  command.json   app -> watchdog   the latest button press: warm | stop
  activity.json  app -> watchdog   when someone last used the demo
  status.json    watchdog -> app   warehouse states, phase, idle countdown

Writes are atomic (temp file + rename), so a reader never sees half a file.
"""

from __future__ import annotations

import contextlib
import json
import os
import pathlib
import tempfile
import time
import uuid

CONTROL_DIR = pathlib.Path(os.getenv("DEMO_CONTROL_DIR")
                           or pathlib.Path(tempfile.gettempdir()) / "lakehouse-rt-demo")
COMMAND = CONTROL_DIR / "command.json"
ACTIVITY = CONTROL_DIR / "activity.json"
STATUS = CONTROL_DIR / "status.json"
# Written by the scaled-load driver 4x a second while a run is live.
SCALE_STATE = pathlib.Path(__file__).parent / "static" / "rt_state.json"

# The watchdog publishes every few seconds; older than this means it is not running.
STATUS_STALE_S = 30.0


def _write(path: pathlib.Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp, path)
    except Exception:
        with contextlib.suppress(Exception):
            os.unlink(tmp)
        raise


def _read(path: pathlib.Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except Exception:  # noqa: BLE001 - missing or mid-replace: treat as absent
        return None


# ---- app side -------------------------------------------------------------- #
def send_command(cmd: str) -> str:
    """Queue a button press for the watchdog. Returns the command id."""
    cid = uuid.uuid4().hex
    _write(COMMAND, {"id": cid, "cmd": cmd, "ts": time.time()})
    return cid


def touch_activity(reason: str) -> None:
    """Record that someone is using the demo. Never raises."""
    with contextlib.suppress(Exception):
        _write(ACTIVITY, {"ts": time.time(), "reason": reason})


def read_status() -> dict | None:
    """The watchdog's latest status, or None when it is absent or stale."""
    s = _read(STATUS)
    if not s or time.time() - float(s.get("ts", 0)) > STATUS_STALE_S:
        return None
    return s


# ---- watchdog side --------------------------------------------------------- #
def read_command() -> dict | None:
    return _read(COMMAND)


def read_activity_ts() -> float:
    a = _read(ACTIVITY)
    return float(a.get("ts", 0)) if a else 0.0


def scale_running(fresh_s: float = 15.0) -> bool:
    """True while a scaled-load run is live (its state file is fresh and running)."""
    s = _read(SCALE_STATE)
    if not s:
        return False
    metrics = s.get("metrics") or {}
    return bool(metrics.get("running")) and time.time() - float(s.get("stamp", 0)) < fresh_s


def write_status(payload: dict) -> None:
    _write(STATUS, payload)
