"""Shared settings for the app, the warehouse keeper and the load-test job.

Catalog, schema and warehouse ids arrive as environment variables set by the
bundle (resources/app.yml), so nothing here is workspace-specific.

ANCHOR DATE. The synthetic tables end on the day they were generated, and every
time-windowed widget filters relative to that day rather than to
current_date(). Otherwise a demo run weeks after generation would show empty
"last 90 days" tiles. The data job stores it as a table property.
"""

from __future__ import annotations

import datetime as _dt
import os

CATALOG = os.getenv("DEMO_CATALOG", "main")
SCHEMA = os.getenv("DEMO_SCHEMA", "lakehouse_rt_demo")
DBSQL_WAREHOUSE_ID = os.getenv("DBSQL_WAREHOUSE_ID", "")
RT_WAREHOUSE_ID = os.getenv("RT_WAREHOUSE_ID", "")


def table(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


FACT = table("gold_dispute_case_fact")
EVENTS = table("gold_dispute_event_stream")
MONTHLY = table("gold_merchant_dispute_monthly")

# Must match src/data/generate_tables.py: merchant ids are contiguous.
MERCHANT_BASE = 100001
MERCHANT_COUNT = 40000


ANCHOR_PROPERTY = "demo.anchor_date"


def resolve_anchor(client, warehouse_id: str | None = None) -> _dt.date:
    """Last filing date in the data. Falls back to today on any failure.

    Read from the fact table's `demo.anchor_date` property through the Unity
    Catalog API, which needs no warehouse, so opening a page never starts one.
    Only when the property is missing and `warehouse_id` is given does it fall
    back to querying MAX(chargeback_filing_date).
    """
    try:
        t = client.tables.get(full_name=f"{CATALOG}.{SCHEMA}.gold_dispute_case_fact")
        v = (t.properties or {}).get(ANCHOR_PROPERTY)
        if v:
            return _dt.date.fromisoformat(v)
    except Exception:  # noqa: BLE001
        pass
    if warehouse_id:
        from databricks.sdk.service.sql import Disposition, Format

        try:
            r = client.statement_execution.execute_statement(
                statement=f"SELECT CAST(MAX(chargeback_filing_date) AS STRING) FROM {FACT}",
                warehouse_id=warehouse_id, wait_timeout="50s",
                format=Format.JSON_ARRAY, disposition=Disposition.INLINE)
            v = r.result.data_array[0][0] if (r.result and r.result.data_array) else None
            if v:
                return _dt.date.fromisoformat(v)
        except Exception:  # noqa: BLE001
            pass
    return _dt.date.today()


def days_before(anchor: _dt.date, days: int) -> str:
    """ISO date string `days` before the anchor, for inlining as a SQL literal."""
    return (anchor - _dt.timedelta(days=days)).isoformat()
