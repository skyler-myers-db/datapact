"""Dashboard creation helpers for DataPact.

This module provides a thin abstraction for creating/publishing dashboards.
In this refactor we keep the complex layout logic in the client for now, but
expose a seam for future extraction.
"""

from __future__ import annotations

from databricks.sdk import WorkspaceClient


def publish_dashboard(
    w: WorkspaceClient,
    *,
    dashboard_id: str,
    warehouse_id: str,
    embed_credentials: bool = True,
) -> None:
    w.lakeview.publish(
        dashboard_id=dashboard_id,
        embed_credentials=embed_credentials,
        warehouse_id=warehouse_id,
    )
