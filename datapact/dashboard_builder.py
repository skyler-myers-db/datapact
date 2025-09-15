"""Dashboard creation helpers for DataPact.

This module provides a thin abstraction for creating/publishing dashboards.
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
    """
    Publish a Lakeview dashboard in Databricks workspace.

    This function publishes a dashboard using the Databricks Lakeview API, making it
    available for viewing and sharing within the workspace.

    Args:
        w (WorkspaceClient): The Databricks workspace client instance used to
            interact with the Databricks API.
        dashboard_id (str): The unique identifier of the dashboard to publish.
        warehouse_id (str): The ID of the SQL warehouse to use for executing
            queries in the dashboard.
        embed_credentials (bool, optional): Whether to embed credentials in the
            published dashboard. When True, viewers don't need additional
            authentication to view the dashboard. Defaults to True.

    Returns:
        None

    Raises:
        DatabricksError: If the dashboard publishing fails due to API errors,
            invalid dashboard_id, or insufficient permissions.

    Example:
        >>> from databricks.sdk import WorkspaceClient
        >>> w = WorkspaceClient()
        >>> publish_dashboard(
        ...     w,
        ...     dashboard_id="dashboard-123",
        ...     warehouse_id="warehouse-456",
        ...     embed_credentials=False
        ... )
    """
    w.lakeview.publish(
        dashboard_id=dashboard_id,
        embed_credentials=embed_credentials,
        warehouse_id=warehouse_id,
    )
