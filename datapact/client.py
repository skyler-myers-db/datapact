"""
The core client for interacting with the Databricks API.

This module contains the DataPactClient class. It orchestrates the entire
validation process by dynamically generating pure SQL validation scripts based
on a user's configuration. It then creates and runs a multi-task Databricks Job
where each task executes one of the generated SQL scripts on a specified
Serverless SQL Warehouse. Finally, it can create a results dashboard.
"""

import json
import os
import textwrap
import time
from datetime import datetime, timedelta
from typing import Any, Final, TypedDict, cast

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import sql as sql_service
from databricks.sdk.service import workspace
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.service.jobs import RunLifeCycleState, Task
from jinja2 import Environment, PackageLoader
from loguru import logger

from .config import DataPactConfig, ValidationTask
from .job_orchestrator import (
    add_dashboard_refresh_task,
    add_genie_room_task,
    build_tasks,
    ensure_job,
    run_and_wait,
)
from .sql_generator import render_aggregate_sql, render_validation_sql
from .sql_utils import (
    escape_sql_identifier,
    escape_sql_string,
    format_fully_qualified_name,
    parse_fully_qualified_name,
    validate_job_name,
)

__all__ = ["DataPactClient", "resolve_warehouse_name"]

TERMINAL_STATES: list[RunLifeCycleState] = [
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
]
DEFAULT_CATALOG: Final[str] = "datapact"
DEFAULT_SCHEMA: Final[str] = "results"
DEFAULT_TABLE: Final[str] = "run_history"


class WidgetPosition(TypedDict):
    x: int
    y: int
    width: int
    height: int


class WidgetDefinitionBase(TypedDict):
    ds_name: str
    type: str
    title: str
    pos: WidgetPosition


class WidgetDefinition(WidgetDefinitionBase, total=False):
    value_col: str
    x_field: str
    y_field: str | None
    y_display: str
    y_agg: str | None
    show_targets: bool
    x_display: str


class DatasetDefinitionBase(TypedDict):
    name: str
    queryLines: list[str]


class DatasetDefinition(DatasetDefinitionBase, total=False):
    displayName: str
    filters: list[dict[str, Any]]


class DashboardPageBase(TypedDict):
    name: str
    displayName: str
    layout: list[dict[str, Any]]


class DashboardPage(DashboardPageBase, total=False):
    pageType: str
    filters: list[dict[str, Any]]


class DashboardPayloadBase(TypedDict):
    datasets: list[DatasetDefinition]
    pages: list[DashboardPage]


class DashboardPayload(DashboardPayloadBase, total=False):
    uiSettings: dict[str, Any]


def resolve_warehouse_name(
    workspace_client: WorkspaceClient,
    *,
    explicit_name: str | None = None,
) -> str:
    """Resolve the SQL warehouse name using CLI args, env vars, or Databricks config."""

    warehouse_name = explicit_name or os.getenv("DATAPACT_WAREHOUSE")
    if warehouse_name:
        return warehouse_name

    config = getattr(workspace_client, "config", None)
    config_value = getattr(config, "datapact_warehouse", None) if config else None
    if config_value:
        return config_value

    raise ValueError(
        "A warehouse must be provided via the --warehouse flag, the DATAPACT_WAREHOUSE "
        "environment variable, or a 'datapact_warehouse' key in your Databricks config profile."
    )


class DataPactClient:
    """
    A high-level client for orchestrating SQL-based data validation workflows on Databricks.
    This class automates the setup, execution, and monitoring of validation jobs, as well as
    the creation of results dashboards.

    Features:
    - Initializes Databricks workspace credentials and sets up user-specific workspace paths.
    - Provides robust SQL execution with polling and error handling.
    - Ensures required infrastructure (catalog, schema, results table) exists.
    - Dynamically generates SQL scripts for various validation types (count, row hash, null checks, aggregations).
    - Uploads SQL scripts and dashboard notebooks to the Databricks workspace.
    - Creates and publishes executive-ready Lakeview dashboards for validation results.
    - Orchestrates Databricks jobs with multiple validation and aggregation tasks.
    - Monitors job execution, logs progress, and raises errors on failure or timeout.
    - Supports flexible configuration for validation tasks and dashboard customization.

    Args:
        profile (str): The Databricks CLI profile to use for authentication (default: "DEFAULT").

    Raises:
        ValueError: If required infrastructure or resources cannot be set up or identified.
        TimeoutError: If SQL execution or job runs exceed allotted time.
        RuntimeError: If job runs finish with a failure state or cannot be determined.

    Typical usage example:
        client = DataPactClient(profile="DEFAULT")
        client.run_validation(config, job_name="My Validation Job", warehouse_name="My Warehouse")
    """

    def __init__(
        self: "DataPactClient",
        profile: str = "DEFAULT",
        workspace_client: WorkspaceClient | None = None,
    ) -> None:
        """
        Initializes the client with Databricks workspace credentials and sets up the user's datapact root directory.

        Args:
            profile (str, optional): The Databricks CLI profile to use for authentication. Defaults to "DEFAULT".

        Attributes:
            w (WorkspaceClient): The Databricks workspace client instance.
            user_name (str | None): The username of the current Databricks user.
            root_path (str): The root directory path for datapact in the user's workspace.

        Side Effects:
            Creates the datapact root directory in the user's Databricks workspace if it does not already exist.
        """
        workspace_ctor = WorkspaceClient

        # Detect Databricks runtime (serverless or classic)
        in_dbr_runtime = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

        workspace_is_mock = workspace_client is None and getattr(
            workspace_ctor, "__module__", ""
        ).startswith("unittest.mock")

        if workspace_client is not None:
            # Caller handed us an already authenticated client
            self.w: WorkspaceClient = workspace_client
        elif in_dbr_runtime:
            # Inside a Databricks notebook / job: use runtime native auth
            self.w = workspace_ctor()
        elif workspace_is_mock:
            logger.debug("WorkspaceClient patched; using mock instance for initialization.")
            self.w = workspace_ctor()
        else:
            # Local / external: fall back to profile / env / .databrickscfg
            logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
            try:
                from databricks.sdk.config import Config

                config = Config(profile=profile, http_timeout_seconds=1200)  # 20 minute timeout
                self.w = workspace_ctor(config=config)
            except ValueError:
                # Fallback for tests or environments without profile configured
                self.w = workspace_ctor(profile=profile)
        self.user_name: str | None = self.w.current_user.me().user_name
        self.root_path: str = f"/Users/{self.user_name}/datapact"
        self.w.workspace.mkdirs(self.root_path)
        # Cache for Jinja2 Environment to avoid re-instantiation per render
        self._env: Environment | None = None

    def _jinja_env(self: "DataPactClient") -> Environment:
        """Return a cached Jinja2 environment configured for SQL template rendering."""
        env = getattr(self, "_env", None)
        if env is None:
            env = Environment(
                loader=PackageLoader("datapact", "templates"),
                autoescape=False,
                trim_blocks=True,
                lstrip_blocks=True,
                extensions=["jinja2.ext.do"],
            )
            # Cache on self for subsequent calls (works even if __init__ was bypassed)
            self._env = env
        return env

    def _execute_sql(
        self: "DataPactClient",
        sql: str,
        warehouse_id: str,
    ) -> None:
        """
        A robust, synchronous helper function to execute a SQL statement using a polling loop.
        This is used for setting up infrastructure before the main job runs.

        Args:
            sql: The SQL string to execute.
            warehouse_id: The ID of the SQL warehouse to run the statement on.

        Raises:
            Exception: If the SQL statement fails to execute.
            TimeoutError: If the execution takes longer than the defined timeout.
        """
        try:
            resp = self.w.statement_execution.execute_statement(
                statement=sql, warehouse_id=warehouse_id, wait_timeout="0s"
            )
            statement_id = resp.statement_id
            if statement_id is None:
                raise ValueError("Statement ID is None. Cannot poll statement status.")
            timeout = timedelta(minutes=5)
            deadline = datetime.now() + timeout
            while datetime.now() < deadline:
                status = self.w.statement_execution.get_statement(statement_id)
                if status.status is None:
                    raise RuntimeError(
                        "Statement status is None. Cannot determine execution state."
                    )
                current_state = status.status.state
                if current_state == sql_service.StatementState.SUCCEEDED:
                    return
                if current_state in [
                    sql_service.StatementState.FAILED,
                    sql_service.StatementState.CANCELED,
                    sql_service.StatementState.CLOSED,
                ]:
                    raise RuntimeError(
                        f"SQL execution failed: {status.status.error.message if status.status.error else 'Unknown'}"
                    )
                time.sleep(5)
            raise TimeoutError("SQL statement timed out.")
        except Exception as e:
            logger.critical(f"Failed to execute SQL: {sql}")
            raise e

    def _setup_default_infrastructure(
        self: "DataPactClient",
        warehouse_id: str,
    ) -> None:
        """Creates the default catalog and schema if they do not already exist."""
        logger.info(
            f"Ensuring default infrastructure ('{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}') exists..."
        )
        self._execute_sql(f"CREATE CATALOG IF NOT EXISTS `{DEFAULT_CATALOG}`", warehouse_id)
        self._execute_sql(
            f"GRANT USAGE ON CATALOG `{DEFAULT_CATALOG}` TO `{self.user_name}`;",
            warehouse_id,
        )
        self._execute_sql(
            f"CREATE SCHEMA IF NOT EXISTS `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`",
            warehouse_id,
        )
        logger.success("Default infrastructure is ready.")

    def _ensure_results_table_exists(
        self: "DataPactClient",
        results_table_fqn: str,
        warehouse_id: str,
    ) -> None:
        """Ensures the results Delta table exists, creating it if necessary."""
        logger.info(f"Ensuring results table '{results_table_fqn}' exists...")
        ddl = textwrap.dedent(
            f"""CREATE TABLE IF NOT EXISTS {results_table_fqn} (
            task_key STRING, status STRING, run_id BIGINT, job_id BIGINT, job_name STRING,
            job_start_ts TIMESTAMP,
            validation_begin_ts TIMESTAMP,
            validation_complete_ts TIMESTAMP,
            source_catalog STRING, source_schema STRING, source_table STRING,
            target_catalog STRING, target_schema STRING, target_table STRING,
            business_domain STRING, business_owner STRING, business_priority STRING,
            expected_sla_hours DOUBLE, estimated_impact_usd DOUBLE,
            result_payload VARIANT) USING DELTA"""
        )
        self._execute_sql(ddl, warehouse_id)
        logger.success(f"Results table '{results_table_fqn}' is ready.")

    def _bootstrap_exec_summary_tables(
        self: "DataPactClient",
        warehouse_id: str,
        results_table_fqn: str,
    ) -> None:
        """Create the executive summary tables up-front so Lakeview queries succeed."""

        statement_api = getattr(self.w, "statement_execution", None)
        if statement_api is None:
            logger.debug(
                "Skipping executive summary table bootstrap; statement execution client not available."
            )
            return

        try:
            catalog, schema, _ = parse_fully_qualified_name(results_table_fqn)
        except ValueError:
            catalog, schema = DEFAULT_CATALOG, DEFAULT_SCHEMA

        base_path = f"{escape_sql_identifier(catalog)}.{escape_sql_identifier(schema)}"

        logger.debug(
            f"Ensuring executive summary tables exist in {base_path} using warehouse {warehouse_id}."
        )

        table_definitions: dict[str, str] = {
            "exec_run_summary": """
                run_id BIGINT,
                job_id BIGINT,
                job_name STRING,
                total_tasks BIGINT,
                failure_count BIGINT,
                success_count BIGINT,
                success_rate_percent DOUBLE,
                data_quality_score DOUBLE,
                critical_failures BIGINT,
                potential_impact_usd DOUBLE,
                realized_impact_usd DOUBLE,
                avg_expected_sla_hours DOUBLE,
                failed_task_keys ARRAY<STRING>,
                generated_at TIMESTAMP
            """,
            "exec_domain_breakdown": """
                run_id BIGINT,
                job_name STRING,
                business_domain STRING,
                total_validations BIGINT,
                failed_validations BIGINT,
                success_rate_percent DOUBLE,
                avg_expected_sla_hours DOUBLE,
                potential_impact_usd DOUBLE,
                realized_impact_usd DOUBLE,
                last_failure_ts TIMESTAMP,
                generated_at TIMESTAMP
            """,
            "exec_owner_breakdown": """
                run_id BIGINT,
                job_name STRING,
                business_owner STRING,
                total_validations BIGINT,
                failed_validations BIGINT,
                success_rate_percent DOUBLE,
                avg_expected_sla_hours DOUBLE,
                potential_impact_usd DOUBLE,
                realized_impact_usd DOUBLE,
                last_failure_ts TIMESTAMP,
                generated_at TIMESTAMP
            """,
            "exec_priority_breakdown": """
                run_id BIGINT,
                job_name STRING,
                business_priority STRING,
                total_validations BIGINT,
                failed_validations BIGINT,
                success_rate_percent DOUBLE,
                potential_impact_usd DOUBLE,
                realized_impact_usd DOUBLE,
                last_failure_ts TIMESTAMP,
                generated_at TIMESTAMP
            """,
        }

        for table_name, columns in table_definitions.items():
            ddl = textwrap.dedent(
                f"""
                CREATE TABLE IF NOT EXISTS {base_path}.`{table_name}` (
                    {textwrap.dedent(columns).strip()}
                ) USING DELTA
                """
            ).strip()
            self._execute_sql(ddl, warehouse_id)

        logger.success("Executive summary tables are ready for dashboard queries.")

    def _generate_validation_sql(
        self: "DataPactClient",
        config: ValidationTask,
        results_table: str,
        job_name: str,
    ) -> str:
        """Render validation SQL for a single task via the SQL generator module."""
        return render_validation_sql(self._jinja_env(), config, results_table, job_name)

    def _generate_genie_room_sql(
        self: "DataPactClient",
        results_table: str,
        job_name: str,
    ) -> str:
        """Generate SQL script for creating curated datasets for Genie room.

        This creates Delta tables of the validation results optimized for
        natural language querying through Databricks AI/BI Genie.
        """
        # Safely escape SQL values to prevent injection
        safe_job_name = escape_sql_string(validate_job_name(job_name))

        # Extract catalog and schema from results table to put genie tables in same location
        try:
            genie_catalog_raw, genie_schema_raw, _ = parse_fully_qualified_name(results_table)
        except ValueError:
            genie_catalog_raw, genie_schema_raw = DEFAULT_CATALOG, DEFAULT_SCHEMA

        genie_catalog = escape_sql_identifier(genie_catalog_raw)
        genie_schema = escape_sql_identifier(genie_schema_raw)

        return f"""-- DataPact Genie Room Data Preparation
-- Creates curated datasets for natural language analysis in Databricks AI/BI Genie
-- After this runs, create a Genie space in the UI using these tables

-- 1. Current validation status summary
CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.genie_current_status AS
SELECT
    task_key as validation_name,
    CASE status
        WHEN 'SUCCESS' THEN 'Passed'
        WHEN 'FAILURE' THEN 'Failed'
        ELSE status
    END as validation_status,
    get_json_object(to_json(result_payload), '$.source_catalog') || '.' ||
    get_json_object(to_json(result_payload), '$.source_schema') || '.' ||
    get_json_object(to_json(result_payload), '$.source_table') as source_table,
    get_json_object(to_json(result_payload), '$.target_catalog') || '.' ||
    get_json_object(to_json(result_payload), '$.target_schema') || '.' ||
    get_json_object(to_json(result_payload), '$.target_table') as target_table,
    validation_begin_ts as last_validated,
    CASE
        WHEN get_json_object(to_json(result_payload), '$.count_validation.status') = 'FAIL' THEN 'Row count mismatch'
        WHEN get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'FAIL' THEN 'Data integrity issue'
        WHEN to_json(result_payload) LIKE '%null_validation%FAIL%' THEN 'Missing required data'
        WHEN to_json(result_payload) LIKE '%uniqueness_validation%FAIL%' THEN 'Duplicate records found'
        WHEN to_json(result_payload) LIKE '%agg_validation%FAIL%' THEN 'Business rule violation'
        WHEN status = 'SUCCESS' THEN 'All checks passed'
        ELSE 'Unknown issue'
    END as issue_type,
    get_json_object(to_json(result_payload), '$.count_validation.source_count') as source_row_count,
    get_json_object(to_json(result_payload), '$.count_validation.target_count') as target_row_count,
    run_id,
    job_name
FROM (
    SELECT
      *,
      row_number() over (partition by run_id, task_key order by validation_begin_ts desc) as rn
    FROM
      {results_table}
    WHERE job_name = {safe_job_name}
  )
WHERE rn = 1;

-- 2. Data quality metrics by table
CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.genie_table_quality AS
SELECT
    CONCAT(source_schema, '.', source_table) as table_name,
    COUNT(*) as total_validations,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as passed_validations,
    SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failed_validations,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_score,
    MAX(validation_begin_ts) as last_checked
FROM (
    SELECT
      *,
      row_number() over (partition by run_id, task_key order by validation_begin_ts desc) as rn
    FROM
      {results_table}
    WHERE job_name = {safe_job_name}
  )
WHERE rn = 1
GROUP BY 1;

-- 3. Issue details for failed validations
CREATE OR REPLACE TABLE {genie_catalog}.{genie_schema}.genie_issues AS
SELECT
    task_key as validation_name,
    get_json_object(to_json(result_payload), '$.source_table') as table_name,
    CASE
        WHEN get_json_object(to_json(result_payload), '$.count_validation.status') = 'FAIL' THEN
            CONCAT('Expected ', get_json_object(to_json(result_payload), '$.count_validation.source_count'),
                   ' rows but found ', get_json_object(to_json(result_payload), '$.count_validation.target_count'))
        WHEN get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'FAIL' THEN
            CONCAT('Data integrity check failed for ',
                   get_json_object(to_json(result_payload), '$.row_hash_validation.failed_count'), ' records')
        ELSE 'Validation failed - check details'
    END as issue_description,
    validation_begin_ts as detected_at,
    'High' as severity
FROM (
    SELECT
      *,
      row_number() over (partition by run_id, task_key order by validation_begin_ts desc) as rn
    FROM
      {results_table}
    WHERE job_name = {safe_job_name}
  )
WHERE rn = 1
AND status = 'FAILURE';

-- Display instructions for setting up Genie space
SELECT '🚀 GENIE ROOM SETUP INSTRUCTIONS' as title,
'Your data quality datasets have been created! To enable natural language analysis:

1. Go to Databricks AI/BI → Create → Genie Space
2. Add these tables as data sources:
   • genie_current_status - Current validation results
   • genie_table_quality - Quality scores by table
   • genie_issues - Detailed issue tracking
3. Set the space description: "Data quality validation results for ' || {safe_job_name} || '"
4. Add sample questions:
   • What tables have data quality issues?
   • Show me all failed validations
   • Which tables have the lowest quality scores?
   • What are the most common types of validation failures?
   • How many records are affected by data integrity issues?
5. Save and share the Genie space with your team

Once created, users can ask questions in natural language to analyze data quality!' as instructions;
"""

    def _generate_dashboard_notebook_content(self: "DataPactClient") -> str:
        """Generates the Python code for the dashboard-creation notebook."""
        return textwrap.dedent(
            """
            import textwrap
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service import sql as sql_service
            from loguru import logger

            # Get parameters from the job task
            dbutils.widgets.text("job_name", "", "Job Name")
            dbutils.widgets.text("results_table_fqn", "", "Results Table FQN")
            dbutils.widgets.text("warehouse_id", "", "Warehouse ID")

            job_name = dbutils.widgets.get("job_name")
            results_table_fqn = dbutils.widgets.get("results_table_fqn")
            warehouse_id = dbutils.widgets.get("warehouse_id")

            logger.info(f"Starting dashboard creation for job: {job_name}")
            logger.info(f"Results table: {results_table_fqn}")
            logger.info(f"Warehouse ID: {warehouse_id}")

            w = WorkspaceClient() # Authenticates automatically inside Databricks

            dashboard_name = f"DataPact Results: {job_name}"

            # Clean up old dashboard and associated queries
            for d in w.dashboards.list(q=dashboard_name):
                if d.display_name == dashboard_name:
                    logger.warning(f"Deleting existing dashboard (ID: {d.dashboard_id}) to recreate.")
                    w.dashboards.delete(d.dashboard_id)
                    if d.widgets:
                        for widget in d.widgets:
                            if widget.visualization and widget.visualization.query:
                                try: w.queries.delete(widget.visualization.query.query_id)
                                except Exception: pass

            queries = {
                "run_summary": f"SELECT status, COUNT(1) as task_count FROM {results_table_fqn} WHERE run_id = (SELECT MAX(run_id) FROM {results_table_fqn} WHERE job_name = '{job_name}') GROUP BY status",
                "failure_rate_over_time": f"SELECT to_date(validation_begin_ts) as run_date, COUNT(CASE WHEN status = 'FAILURE' THEN 1 END) * 100.0 / COUNT(1) as failure_rate_percent FROM {results_table_fqn} WHERE job_name = '{job_name}' GROUP BY 1 ORDER BY 1",
                "top_failing_tasks": f"SELECT task_key, COUNT(1) as failure_count FROM {results_table_fqn} WHERE status = 'FAILURE' AND job_name = '{job_name}' GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
                "raw_history": f"SELECT task_key, status, run_id, job_id, job_name, validation_begin_ts, result_payload FROM {results_table_fqn} WHERE job_name = '{job_name}' ORDER BY validation_begin_ts DESC, task_key"
            }

            widgets = []
            for name, sql in queries.items():
                query_obj = w.queries.create(name=f"DataPact-{job_name}-{name}", data_source_id=warehouse_id, query=sql)
                viz_options, viz_type = {}, "TABLE"
                if name == "run_summary": viz_type = "COUNTER"; viz_options = {"counterColName": "task_count"}
                elif name == "failure_rate_over_time": viz_type = "CHART"; viz_options = {"globalSeriesType": "line"}
                elif name == "top_failing_tasks": viz_type = "CHART"; viz_options = {"globalSeriesType": "bar"}
                viz = w.visualizations.create(query_id=query_obj.id, type=viz_type, name=f"Viz-{name}", options=viz_options)
                widgets.append(sql_service.WidgetCreate(visualization_id=viz.id))

            dashboard = w.dashboards.create(name=dashboard_name, warehouse_id=warehouse_id, widgets=widgets)
            dashboard_url = f"{w.config.host}/sql/dashboards/{dashboard.id}"
            logger.success(f"✅ Dashboard is ready! View it here: {dashboard_url}")
            dbutils.notebook.exit(dashboard_url)
        """
        )

    def _upload_sql_scripts(
        self: "DataPactClient",
        config: DataPactConfig,
        results_table: str,
        job_name: str,
    ) -> dict[str, str]:
        """Generate and upload all SQL files required for the job.

        This renders each validation task SQL from the Jinja templates and uploads
        them to the Databricks workspace. It also renders the aggregate results SQL
        from the dedicated template to avoid duplication and keep behavior consistent.
        """
        logger.info("Generating and uploading job assets...")
        asset_paths: dict[str, str] = {}
        job_assets_path: str = f"{self.root_path}/job_assets/{job_name}"
        self.w.workspace.mkdirs(job_assets_path)

        env = self._jinja_env()

        for task_config in config.validations:
            task_key: str = task_config.task_key
            sql_script: str = self._generate_validation_sql(
                task_config,
                results_table,
                job_name,
            )
            script_path: str = f"{job_assets_path}/{task_key}.sql"
            self.w.workspace.upload(
                path=script_path,
                content=sql_script.encode("utf-8"),
                overwrite=True,
                format=workspace.ImportFormat.RAW,
            )
            asset_paths[task_key] = script_path

        agg_script_path: str = f"{job_assets_path}/aggregate_results.sql"
        # Render from the shared Jinja template to keep content in sync with tests/assets
        agg_sql_script: str = render_aggregate_sql(env, results_table)
        self.w.workspace.upload(
            path=agg_script_path,
            content=agg_sql_script.encode("utf-8"),
            overwrite=True,
            format=workspace.ImportFormat.RAW,
        )
        asset_paths["aggregate_results"] = agg_script_path

        # Generate and upload Genie room setup SQL
        genie_script_path: str = f"{job_assets_path}/setup_genie_datasets.sql"
        genie_sql_script: str = self._generate_genie_room_sql(results_table, job_name)
        self.w.workspace.upload(
            path=genie_script_path,
            content=genie_sql_script.encode("utf-8"),
            overwrite=True,
            format=workspace.ImportFormat.RAW,
        )
        asset_paths["setup_genie_datasets"] = genie_script_path

        logger.success("All SQL scripts uploaded successfully.")
        return asset_paths

    def ensure_dashboard_exists(
        self: "DataPactClient",
        job_name: str,
        results_table_fqn: str,
        warehouse_id: str,
    ) -> str:
        """
        Create or recreate a Databricks Lakeview dashboard for DataPact job results.

        This method generates a comprehensive dashboard with multiple visualizations including
        KPI counters, success rate metrics, failure analysis charts, and detailed history tables.
        The dashboard is designed to provide executive-ready insights into data validation job performance.

        Args:
            job_name (str): Name of the DataPact job to create the dashboard for. Used for
                filtering data and generating the dashboard display name.
            results_table_fqn (str): Fully qualified name of the table containing job results
                data (e.g., "catalog.schema.table_name").
            warehouse_id (str): Databricks warehouse ID to use for running dashboard queries
                and publishing the dashboard.

        Returns:
            str: The dashboard_id of the created draft dashboard, which can be used for
                further operations like publishing or sharing.

        Raises:
            RuntimeError: If dashboard creation fails or returns a None dashboard_id.
            NotFound: May be raised during cleanup of existing dashboard files.

        Note:
            - If a dashboard with the same name already exists, it will be deleted and recreated
            - The dashboard includes 7 widgets: KPI counters, donut chart, line chart, bar chart, and table
            - The dashboard is automatically published with embedded credentials
            - Dashboard display name format: "DataPact_Results_{sanitized_job_name}"
            - Dashboard file is stored at: "{root_path}/dashboards/{display_name}.lvdash.json"

        Dashboard Components:
            - Total Tasks Executed (counter)
            - Failed Tasks (counter)
            - Success Rate (percentage counter)
            - Run Summary (donut chart)
            - Failure Rate Over Time (line chart)
            - Top Failing Tasks (bar chart)
            - Detailed Run History (table)
        """
        display_name: str = f"DataPact_Results_{job_name.replace(' ', '_').replace(':', '')}"
        parent_path: str = f"{self.root_path}/dashboards"
        draft_path: str = f"{parent_path}/{display_name}.lvdash.json"
        self.w.workspace.mkdirs(parent_path)

        # Ensure supporting executive summary tables exist before the dashboard is created
        if os.getenv("DATAPACT_SKIP_EXEC_SUMMARY_BOOTSTRAP") == "1":
            logger.debug("Skipping executive summary bootstrap via environment override.")
        else:
            self._bootstrap_exec_summary_tables(warehouse_id, results_table_fqn)

        try:
            self.w.workspace.get_status(draft_path)
            logger.warning(
                f"Found existing dashboard file at {draft_path}. Deleting to recreate with the correct format."
            )
            self.w.workspace.delete(path=draft_path, recursive=True)
            time.sleep(2)
        except NotFound:
            # File doesn't exist - this is expected for new dashboards
            logger.info("Dashboard file does not yet exist – will create")
        except (PermissionError, OSError) as e:
            # Log specific filesystem/permission errors but continue with dashboard creation
            logger.warning(f"Error accessing dashboard file: {e}. Proceeding with creation.")
        except Exception as exc:  # pragma: no cover - safeguard for unexpected SDK responses
            logger.warning(
                f"Unexpected error checking dashboard file: {exc}. Proceeding with creation."
            )

        try:
            catalog, schema, _ = parse_fully_qualified_name(results_table_fqn)
        except ValueError:
            catalog, schema = DEFAULT_CATALOG, DEFAULT_SCHEMA

        def fq(name: str) -> str:
            return (
                f"{escape_sql_identifier(catalog)}."
                f"{escape_sql_identifier(schema)}."
                f"{escape_sql_identifier(name)}"
            )

        run_summary_table = fq("exec_run_summary")
        domain_breakdown_table = fq("exec_domain_breakdown")
        owner_breakdown_table = fq("exec_owner_breakdown")
        priority_breakdown_table = fq("exec_priority_breakdown")

        def q(sql):
            # Safely escape job name to prevent SQL injection
            safe_job_name = escape_sql_string(validate_job_name(job_name))
            return sql.format(
                table=results_table_fqn,
                job=safe_job_name,
                run_summary=run_summary_table,
                domain_breakdown=domain_breakdown_table,
                owner_breakdown=owner_breakdown_table,
                priority_breakdown=priority_breakdown_table,
            )

        # Define all datasets needed for the dashboard
        datasets: list[DatasetDefinition] = [
            {
                "name": "ds_kpi",
                "displayName": "Executive KPI Dashboard",
                "queryLines": [
                    q(
                        """WITH latest_summary AS (
                          SELECT *
                          FROM {run_summary}
                          WHERE job_name = {job}
                          ORDER BY generated_at DESC
                          LIMIT 1
                        ),
                        scoped AS (
                          SELECT *
                          FROM {table}
                          WHERE job_name = {job}
                            AND job_start_ts = (
                              SELECT MAX(job_start_ts)
                              FROM {table}
                              WHERE job_name = {job}
                            )
                        ),
                        metrics AS (
                          SELECT
                            COUNT(*) AS total_tasks,
                            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS passed_tasks,
                            SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) AS failed_tasks,
                            ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_percent,
                            ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS data_quality_score
                          FROM scoped
                        )
                        SELECT
                          metrics.total_tasks,
                          metrics.passed_tasks,
                          metrics.failed_tasks,
                          metrics.success_rate_percent,
                          metrics.data_quality_score,
                          latest_summary.critical_failures,
                          latest_summary.potential_impact_usd,
                          latest_summary.realized_impact_usd,
                          latest_summary.avg_expected_sla_hours,
                          metrics.total_tasks AS tables_validated
                        FROM
                          metrics
                          CROSS JOIN latest_summary;"""
                    )
                ],
            },
            {
                "name": "ds_summary",
                "displayName": "Validation Status Overview",
                "queryLines": [
                    q(
                        "SELECT "
                        "  CASE status "
                        "    WHEN 'SUCCESS' THEN 'Passed' "
                        "    WHEN 'FAILURE' THEN 'Failed' "
                        "    ELSE status "
                        "  END as status, "
                        "  COUNT(*) as task_count "
                        "FROM "
                        "  {table} "
                        "WHERE "
                        "  job_name = {job} "
                        "  AND job_start_ts = ( "
                        "    SELECT "
                        "      MAX(job_start_ts) "
                        "    FROM "
                        "      {table} "
                        "    WHERE "
                        "      job_name = {job} "
                        "  ) "
                        "GROUP BY "
                        "  status; "
                    )
                ],
            },
            {
                "name": "ds_failure_rate",
                "displayName": "Data Quality Trend Analysis",
                "queryLines": [
                    q(
                        "SELECT date(validation_begin_ts) as run_date, "
                        "ROUND(COUNT(IF(status='FAILURE',1,NULL))*100.0/COUNT(*), 2) as failure_rate, "
                        "ROUND(COUNT(IF(status='SUCCESS',1,NULL))*100.0/COUNT(*), 2) as success_rate, "
                        "COUNT(*) as validations_run "
                        "FROM {table} WHERE job_name={job} GROUP BY 1 ORDER BY 1 DESC LIMIT 30"
                    )
                ],
            },
            {
                "name": "ds_top_failures",
                "displayName": "Top Failing Tasks",
                "queryLines": [
                    q(
                        "SELECT task_key, COUNT(*) as failure_count FROM {table} WHERE status='FAILURE' AND job_name={job} GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
                    )
                ],
            },
            {
                "name": "ds_failures_by_type",
                "displayName": "Issue Classification & Impact Analysis",
                "queryLines": [
                    q(
                        "WITH failure_details AS (\n"
                        "  SELECT task_key, result_payload\n"
                        "  FROM (\n"
                        "    SELECT\n"
                        "    *,\n"
                        "    row_number() over (partition by run_id, task_key order by job_start_ts desc) AS rn\n"
                        "    FROM\n"
                        "       {table}\n"
                        "    WHERE\n"
                        "       job_name = {job}\n"
                        ")\n"
                        "  WHERE rn = 1\n"
                        "    AND status = 'FAILURE'\n"
                        ")\n"
                        "SELECT validation_type, COUNT(DISTINCT task_key) as failure_count FROM (\n"
                        "  SELECT task_key, 'Row Count Mismatch' AS validation_type\n"
                        "  FROM failure_details\n"
                        "  WHERE get_json_object(to_json(result_payload), '$.count_validation.status') = 'FAIL'\n"
                        "  UNION ALL\n"
                        "  SELECT task_key, 'Data Integrity Issue' AS validation_type\n"
                        "  FROM failure_details\n"
                        "  WHERE get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'FAIL'\n"
                        "  UNION ALL\n"
                        "  SELECT task_key, 'Data Completeness' AS validation_type\n"
                        "  FROM failure_details\n"
                        "  WHERE to_json(result_payload) LIKE '%null_validation_%status%FAIL%'\n"
                        "  UNION ALL\n"
                        "  SELECT task_key, 'Duplicate Records' AS validation_type\n"
                        "  FROM failure_details\n"
                        "  WHERE to_json(result_payload) LIKE '%uniqueness_validation_%status%FAIL%'\n"
                        "  UNION ALL\n"
                        "  SELECT task_key, 'Business Rule Violation' AS validation_type\n"
                        "  FROM failure_details\n"
                        "  WHERE to_json(result_payload) LIKE '%agg_validation_%status%FAIL%'\n"
                        "  UNION ALL\n"
                        "  SELECT task_key, 'Custom SQL Mismatch' AS validation_type\n"
                        "  FROM failure_details\n"
                        "  WHERE to_json(result_payload) LIKE '%custom_sql_validation_%status%FAIL%'\n"
                        ") t\n"
                        "GROUP BY validation_type\n"
                        "ORDER BY failure_count DESC;"
                    )
                ],
            },
            {
                "name": "ds_history",
                "displayName": "Detailed Run History",
                "queryLines": [
                    q(
                        "SELECT "
                        "  task_key, "
                        "  status, "
                        "  job_start_ts, "
                        "  trim(CAST(result_payload:applied_filter AS STRING)) AS applied_filter, "
                        "  result_payload:applied_filter IS NOT NULL AS is_filtered, "
                        "  CAST(result_payload:configured_primary_keys AS STRING) AS configured_primary_keys, "
                        "  to_json(result_payload) as payload_json, "
                        "  run_id, "
                        "  job_name, "
                        "  business_priority, "
                        "  business_domain, "
                        "  business_owner "
                        "FROM "
                        "  {table} "
                        "WHERE "
                        "  job_name = {job} "
                        "ORDER BY "
                        "  job_start_ts DESC, "
                        "  task_key; "
                    )
                ],
            },
            {
                "name": "ds_latest_run_details",
                "displayName": "All Run Details",
                "queryLines": [
                    q(
                        "SELECT "
                        "  task_key, "
                        "  CASE status "
                        "    WHEN 'SUCCESS' THEN '✅ PASSED' "
                        "    WHEN 'FAILURE' THEN '❌ FAILED' "
                        "    ELSE status "
                        "  END as status, "
                        "  CONCAT(source_catalog, '.', source_schema, '.', source_table) as source_table, "
                        "  CONCAT(target_catalog, '.', target_schema, '.', target_table) as target_table, "
                        "  DATE_FORMAT(job_start_ts, 'yyyy-MM-dd HH:mm:ss') as job_start_ts, "
                        "  to_json(result_payload) as result_payload, "
                        "  run_id, "
                        "  job_name "
                        "FROM "
                        "  {table} "
                        "WHERE "
                        "  job_name = {job} "
                        "  AND job_start_ts = ( "
                        "    SELECT "
                        "      MAX(job_start_ts) "
                        "    FROM "
                        "      {table} "
                        "    WHERE "
                        "      job_name = {job} "
                        "  ) "
                        "ORDER BY "
                        "  CASE status "
                        "    WHEN 'FAILURE' THEN 0 "
                        "    ELSE 1 "
                        "  END, "
                        "  task_key; "
                    )
                ],
            },
            {
                "name": "ds_success_trend",
                "displayName": "Success Rate Over Time",
                "queryLines": [
                    q(
                        "SELECT date(job_start_ts) as run_date, COUNT(IF(status='SUCCESS',1,NULL))*100/COUNT(*) as success_rate FROM {table} WHERE job_name={job} GROUP BY 1 ORDER BY 1"
                    )
                ],
            },
            {
                "name": "ds_business_impact",
                "displayName": "Business Impact Assessment",
                "queryLines": [
                    q(
                        """WITH latest_run_ts AS (
                              SELECT
                                MAX(generated_at) AS generated_at
                              FROM
                                {domain_breakdown}
                              WHERE
                                job_name = {job}
                            ),
                            latest_runs AS (
                              SELECT
                                run_id
                              FROM
                                {run_summary}
                              WHERE
                                job_name = {job}
                              ORDER BY
                                generated_at DESC
                              LIMIT 1
                            ),
                            bus_impact AS (
                              SELECT
                                business_domain,
                                SUM(total_validations) AS total_validations,
                                SUM(failed_validations) AS failed_validations,
                                FIRST(success_rate_percent) AS success_rate_percent,
                                FIRST(success_rate_percent::DOUBLE) || '%' AS quality_score,
                                '$' || FORMAT_NUMBER(SUM(potential_impact_usd), 2) AS potential_impact_usd,
                                '$' || FORMAT_NUMBER(SUM(realized_impact_usd), 2) AS realized_impact_usd,
                                AVG(avg_expected_sla_hours) AS avg_expected_sla_hours,
                                COALESCE(DATE_FORMAT(MAX(last_failure_ts), 'yyyy-MM-dd HH:mm'), 'No failures') as last_issue
                              FROM
                                {domain_breakdown}
                              WHERE
                                job_name = {job}
                                AND generated_at = (SELECT generated_at FROM latest_run_ts)
                                AND run_id IN (SELECT run_id FROM latest_runs)
                              GROUP BY
                                business_domain
                            )
                            SELECT
                              business_domain,
                              total_validations,
                              failed_validations,
                              CASE
                                WHEN failed_validations = 0 THEN '100.00%'
                                ELSE ROUND((1 - failed_validations / total_validations) * 100, 2) || '%'
                              END AS quality_score,
                              potential_impact_usd,
                              realized_impact_usd,
                              avg_expected_sla_hours,
                              CASE
                            WHEN failed_validations = 0 THEN '🟢 Excellent'
                            WHEN success_rate_percent >= 95 THEN '🟡 Good'
                            WHEN success_rate_percent >= 90 THEN '🟠 Fair'
                            ELSE '🔴 Needs Attention'
                              END as health_status,
                              CASE
                                WHEN avg_expected_sla_hours IS NULL THEN 'Unknown SLA'
                                WHEN avg_expected_sla_hours <= 4 THEN 'Lightning Response (<=4h)'
                                WHEN avg_expected_sla_hours <= 12 THEN 'Business Hours (<=12h)'
                                WHEN avg_expected_sla_hours <= 24 THEN 'Standard (<=24h)'
                                ELSE 'Backlog Risk (>24h)'
                              END AS sla_profile,
                              last_issue
                            FROM
                              bus_impact
                            ORDER BY
                              failed_validations DESC,
                              total_validations DESC;"""
                    )
                ],
            },
            {
                "name": "ds_owner_accountability",
                "displayName": "Owner Accountability Overview",
                "queryLines": [
                    q(
                        """WITH latest_run_ts AS (
                              SELECT
                                MAX(generated_at) AS generated_at
                              FROM
                                {owner_breakdown}
                              WHERE
                                job_name = {job}
                            ),
                            latest_runs AS (
                              SELECT
                                run_id
                              FROM
                                {run_summary}
                              WHERE
                                job_name = {job}
                              ORDER BY
                                generated_at DESC
                              LIMIT 1
                            ),
                            owner_stats AS (
                              SELECT
                                business_owner,
                                SUM(total_validations) AS total_validations,
                                SUM(failed_validations) AS failed_validations,
                                '$' || FORMAT_NUMBER(SUM(potential_impact_usd), 2) AS potential_impact_usd,
                                '$' || FORMAT_NUMBER(SUM(realized_impact_usd), 2) AS realized_impact_usd,
                                AVG(avg_expected_sla_hours) As avg_expected_sla_hours,
                                COALESCE(DATE_FORMAT(MAX(last_failure_ts), 'yyyy-MM-dd HH:mm'), 'No failures') AS last_issue
                              FROM
                                {owner_breakdown}
                              WHERE
                                job_name = {job}
                                AND generated_at = (SELECT generated_at FROM latest_run_ts)
                                AND run_id IN (SELECT run_id FROM latest_runs)
                              GROUP BY
                                business_owner
                            )
                            SELECT
                              business_owner,
                              total_validations,
                              failed_validations,
                              CASE
                                WHEN failed_validations = 0 THEN '100.00%'
                                ELSE ROUND((1 - failed_validations / total_validations) * 100, 2) || '%'
                              END AS success_rate_percent,
                              potential_impact_usd,
                              realized_impact_usd,
                              avg_expected_sla_hours,
                              last_issue
                            FROM
                              owner_stats
                            ORDER BY
                              failed_validations DESC,
                              total_validations DESC;"""
                    )
                ],
            },
            {
                "name": "ds_priority_profile",
                "displayName": "Priority Risk Profile",
                "queryLines": [
                    q(
                        """WITH latest_run_ts AS (
                              SELECT
                                MAX(generated_at) AS generated_at
                              FROM
                                {priority_breakdown}
                              WHERE
                                job_name = {job}
                            ),
                            latest_runs AS (
                              SELECT
                                run_id
                              FROM
                                {run_summary}
                              WHERE
                                job_name = {job}
                              ORDER BY
                                generated_at DESC
                              LIMIT 1
                            )
                            SELECT
                              business_priority,
                              total_validations,
                              failed_validations,
                              success_rate_percent,
                              potential_impact_usd,
                              realized_impact_usd,
                              COALESCE(DATE_FORMAT(last_failure_ts, 'yyyy-MM-dd HH:mm'), 'No failures') as last_issue
                            FROM
                              {priority_breakdown}
                            WHERE
                              job_name = {job}
                              AND generated_at = (SELECT generated_at FROM latest_run_ts)
                              AND run_id IN (SELECT run_id FROM latest_runs)
                            ORDER BY
                              failed_validations DESC,
                              potential_impact_usd DESC;"""
                    )
                ],
            },
            {
                "name": "ds_exploded_checks",
                "displayName": "Detailed View of All Checks",
                "queryLines": [
                    q(
                        "WITH latest_run AS ( "
                        "  SELECT MAX(job_start_ts) AS job_start_ts "
                        "  FROM {table} "
                        "  WHERE job_name = {job} "
                        "), "
                        "base_data AS ( "
                        "  SELECT "
                        "    task_key, "
                        "    status, "
                        "    result_payload, "
                        "    run_id, "
                        "    job_name "
                        "  FROM "
                        "    ( "
                        "      SELECT "
                        "        task_key, "
                        "        status, "
                        "        result_payload, "
                        "        run_id, "
                        "        job_name, "
                        "        row_number() over ( "
                        "          partition by task_key "
                        "          order by job_start_ts desc, run_id desc "
                        "        ) AS rn "
                        "      FROM "
                        "        {table} "
                        "      WHERE "
                        "        job_name = {job} "
                        "        AND job_start_ts = (SELECT job_start_ts FROM latest_run) "
                        "    ) "
                        "  WHERE "
                        "    rn = 1 "
                        "), "
                        "expanded_checks AS ( "
                        "  SELECT "
                        "    task_key, "
                        "    'Count Check' as check_type, "
                        "    get_json_object( "
                        "      to_json(result_payload), "
                        "      '$.count_validation.status' "
                        "    ) as check_status, "
                        "    CONCAT( "
                        "      'Source: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.count_validation.source_count' "
                        "      ), "
                        "      ' | Target: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.count_validation.target_count' "
                        "      ), "
                        "      ' | Diff: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.count_validation.relative_diff_percent' "
                        "      ), "
                        "      ' | Tolerance: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.count_validation.tolerance_percent' "
                        "      ) "
                        "    ) as details "
                        "  FROM "
                        "    base_data "
                        "  WHERE "
                        "    get_json_object(to_json(result_payload), '$.count_validation') IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT "
                        "    task_key, "
                        "    'Row Hash Check' as check_type, "
                        "    get_json_object( "
                        "      to_json(result_payload), "
                        "      '$.row_hash_validation.status' "
                        "    ) as check_status, "
                        "    CONCAT( "
                        "      'Compared: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.row_hash_validation.compared_rows' "
                        "      ), "
                        "      ' rows | Mismatches: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.row_hash_validation.mismatch_count' "
                        "      ), "
                        "      ' | Diff: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.row_hash_validation.mismatch_percent' "
                        "      ), "
                        "      ' | Tolerance: ', "
                        "      get_json_object( "
                        "        to_json(result_payload), "
                        "        '$.row_hash_validation.tolerance_percent' "
                        "      ) "
                        "    ) as details "
                        "  FROM "
                        "    base_data "
                        "  WHERE "
                        "    get_json_object(to_json(result_payload), '$.row_hash_validation') IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT "
                        "    task_key, "
                        "    CONCAT('Null Check: ', null_field) as check_type, "
                        "    get_json_object(null_json, '$.status') as check_status, "
                        "    CONCAT( "
                        "      'Source nulls: ', "
                        "      get_json_object(null_json, '$.source_nulls'), "
                        "      ' | Target nulls: ', "
                        "      get_json_object(null_json, '$.target_nulls'), "
                        "      ' | Diff: ', "
                        "      get_json_object(null_json, '$.relative_diff_percent'), "
                        "      ' | Tolerance: ', "
                        "      get_json_object(null_json, '$.tolerance_percent') "
                        "    ) as details "
                        "  FROM "
                        "    ( "
                        "      SELECT "
                        "        task_key, "
                        "        result_payload, "
                        "        regexp_extract(key, 'null_validation_(.*)', 1) as null_field, "
                        "        value as null_json "
                        "      FROM "
                        "        base_data LATERAL VIEW explode( "
                        "          from_json(to_json(result_payload), 'map<string,string>') "
                        "        ) t AS key, "
                        "        value "
                        "      WHERE key LIKE 'null_validation_%' "
                        "    ) null_data "
                        "  WHERE "
                        "    null_field IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT "
                        "    task_key, "
                        "    CONCAT('Uniqueness Check: ', unique_field) as check_type, "
                        "    get_json_object(unique_json, '$.status') as check_status, "
                        "    CONCAT( "
                        "      'Source duplicates: ', "
                        "      COALESCE( "
                        "        get_json_object(unique_json, '$.source_duplicates'), "
                        "        '0' "
                        "      ), "
                        "      ' | Target duplicates: ', "
                        "      COALESCE( "
                        "        get_json_object(unique_json, '$.target_duplicates'), "
                        "        '0' "
                        "      ), "
                        "      ' | Tolerance: ', "
                        "      get_json_object(unique_json, '$.tolerance_percent') "
                        "    ) as details "
                        "  FROM "
                        "    ( "
                        "      SELECT "
                        "        task_key, "
                        "        result_payload, "
                        "        regexp_extract(key, 'uniqueness_validation_(.*)', 1) as unique_field, "
                        "        value as unique_json "
                        "      FROM "
                        "        base_data LATERAL VIEW explode( "
                        "          from_json(to_json(result_payload), 'map<string,string>') "
                        "        ) t AS key, "
                        "        value "
                        "      WHERE key LIKE 'uniqueness_validation_%' "
                        "    ) unique_data "
                        "  WHERE "
                        "    unique_field IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT "
                        "    task_key, "
                        "    CONCAT('Aggregation Check: ', agg_field) as check_type, "
                        "    get_json_object(agg_json, '$.status') as check_status, "
                        "    CONCAT( "
                        "      'Source: ', "
                        "      get_json_object(agg_json, '$.source_value'), "
                        "      ' | Target: ', "
                        "      get_json_object(agg_json, '$.target_value'), "
                        "      ' | Diff: ', "
                        "      get_json_object(agg_json, '$.relative_diff_percent'), "
                        "      ' | Tolerance: ', "
                        "      get_json_object(agg_json, '$.tolerance_percent') "
                        "    ) as details "
                        "  FROM "
                        "    ( "
                        "      SELECT "
                        "        task_key, "
                        "        result_payload, "
                        "        regexp_extract(key, 'agg_validation_(.*)', 1) as agg_field, "
                        "        value as agg_json "
                        "      FROM "
                        "        base_data LATERAL VIEW explode( "
                        "          from_json(to_json(result_payload), 'map<string,string>') "
                        "        ) t AS key, "
                        "        value "
                        "      WHERE key LIKE 'agg_validation_%' "
                        "    ) agg_data "
                        "  WHERE "
                        "    agg_field IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT "
                        "    task_key, "
                        "    CONCAT('Custom SQL: ', custom_field) as check_type, "
                        "    get_json_object(custom_json, '$.status') as check_status, "
                        "    CONCAT( "
                        "      'Source rows: ', "
                        "      COALESCE(get_json_object(custom_json, '$.source_row_count'), '0'), "
                        "      ' | Target rows: ', "
                        "      COALESCE(get_json_object(custom_json, '$.target_row_count'), '0'), "
                        "      ' | Rows missing in target: ', "
                        "      COALESCE(get_json_object(custom_json, '$.rows_missing_in_target'), '0'), "
                        "      ' | Rows missing in source: ', "
                        "      COALESCE(get_json_object(custom_json, '$.rows_missing_in_source'), '0'), "
                        "      CASE "
                        "        WHEN get_json_object(custom_json, '$.sample_missing_in_target') IS NOT NULL "
                        "             AND get_json_object(custom_json, '$.sample_missing_in_target') <> 'null' "
                        "        THEN CONCAT(' | Example present only in source: ', get_json_object(custom_json, '$.sample_missing_in_target')) "
                        "        ELSE '' "
                        "      END, "
                        "      CASE "
                        "        WHEN get_json_object(custom_json, '$.sample_missing_in_source') IS NOT NULL "
                        "             AND get_json_object(custom_json, '$.sample_missing_in_source') <> 'null' "
                        "        THEN CONCAT(' | Example present only in target: ', get_json_object(custom_json, '$.sample_missing_in_source')) "
                        "        ELSE '' "
                        "      END "
                        "    ) as details "
                        "  FROM "
                        "    ( "
                        "      SELECT "
                        "        task_key, "
                        "        result_payload, "
                        "        regexp_extract(key, 'custom_sql_validation_(.*)', 1) as custom_field, "
                        "        value as custom_json "
                        "      FROM "
                        "        base_data LATERAL VIEW explode( "
                        "          from_json(to_json(result_payload), 'map<string,string>') "
                        "        ) t AS key, "
                        "        value "
                        "      WHERE key LIKE 'custom_sql_validation_%' "
                        "    ) custom_data "
                        "  WHERE "
                        "    custom_field IS NOT NULL "
                        "), "
                        "duped AS ( "
                        "  SELECT "
                        "    task_key as validation_name, "
                        "    check_type, "
                        "    CASE "
                        "      check_status "
                        "      WHEN 'PASS' THEN '✅ PASS' "
                        "      WHEN 'FAIL' THEN '❌ FAIL' "
                        "      ELSE '⚠️ ' || COALESCE(check_status, 'UNKNOWN') "
                        "    END as status, "
                        "    details "
                        "  FROM "
                        "    expanded_checks "
                        ") "
                        "SELECT "
                        "  DISTINCT * "
                        "FROM "
                        "  duped "
                        "ORDER BY "
                        "  validation_name, "
                        "  check_type; "
                    )
                ],
            },
            {
                "name": "ds_validation_details",
                "displayName": "Validation Results with Check Status",
                "queryLines": [
                    q(
                        """WITH latest_run AS (
                          SELECT
                            MAX(job_start_ts) AS job_start_ts
                          FROM
                            {table}
                          WHERE
                            job_name = {job}
                        ),
                        ranked AS (
                          SELECT
                            *,
                            row_number() over (partition by task_key order by job_start_ts desc, run_id desc) AS rn
                          FROM
                            {table}
                          WHERE
                            job_name = {job}
                        )
                        SELECT
                          task_key as validation_name,
                          CASE status
                            WHEN 'SUCCESS' THEN '✅'
                            WHEN 'FAILURE' THEN '❌'
                            ELSE '❓'
                          END as overall_status,
                          CASE
                            WHEN
                              get_json_object(to_json(result_payload), '$.count_validation.status') = 'PASS'
                            THEN
                              '✅ Count'
                            WHEN
                              get_json_object(to_json(result_payload), '$.count_validation.status') = 'FAIL'
                            THEN
                              '❌ Count'
                            ELSE '➖'
                          END as count_check,
                          CASE
                            WHEN
                              get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'PASS'
                            THEN
                              '✅ Hash'
                            WHEN
                              get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'FAIL'
                            THEN
                              '❌ Hash'
                            ELSE '➖'
                          END as hash_check,
                          CASE
                            WHEN
                              to_json(result_payload) LIKE '%null_validation%'
                              AND to_json(result_payload) NOT LIKE '%null_validation%"status":"FAIL"%'
                            THEN
                              '✅ Nulls'
                            WHEN to_json(result_payload) LIKE '%null_validation%FAIL%' THEN '❌ Nulls'
                            ELSE '➖'
                          END as null_check,
                          CASE
                            WHEN
                              to_json(result_payload) LIKE '%uniqueness_validation%'
                              AND to_json(result_payload) NOT LIKE '%uniqueness_validation%"status":"FAIL"%'
                            THEN
                              '✅ Unique'
                            WHEN to_json(result_payload) LIKE '%uniqueness_validation%FAIL%' THEN '❌ Unique'
                            ELSE '➖'
                          END as unique_check,
                          CASE WHEN to_json(result_payload) LIKE '%agg_validation%' AND to_json(result_payload) NOT LIKE '%agg_validation%"status":"FAIL"%' THEN '✅ Aggs'
                          -- CASE WHEN to_json(result_payload) LIKE '%agg_validation%' AND to_json(result_payload) NOT LIKE '%agg_validation%FAIL%'
                               WHEN to_json(result_payload) LIKE '%agg_validation%' AND to_json(result_payload) NOT LIKE '%agg_validation%FAIL%' THEN '✅ Aggs'
                               WHEN to_json(result_payload) LIKE '%agg_validation%"status":"FAIL"%' THEN '❌ Aggs'
                               WHEN to_json(result_payload) LIKE '%agg_validation%FAIL%' THEN '❌ Aggs'
                               ELSE '➖'
                          END as agg_check,
                          CASE WHEN to_json(result_payload) LIKE '%custom_sql_validation%' AND to_json(result_payload) NOT LIKE '%custom_sql_validation%"status":"FAIL"%' THEN '✅ Custom SQL'
                          -- CASE WHEN to_json(result_payload) LIKE '%custom_sql_validation%' AND to_json(result_payload) NOT LIKE '%custom_sql_validation%FAIL%'
                               WHEN to_json(result_payload) LIKE '%custom_sql_validation%' AND to_json(result_payload) NOT LIKE '%custom_sql_validation%FAIL%' THEN '✅ Custom SQL'
                               WHEN to_json(result_payload) LIKE '%custom_sql_validation%"status":"FAIL"%' THEN '❌ Custom SQL'
                               WHEN to_json(result_payload) LIKE '%custom_sql_validation%FAIL%' THEN '❌ Custom SQL'
                               ELSE '➖'
                          END as custom_sql_check,
                          COALESCE(business_priority, 'UNSPECIFIED') AS business_priority,
                          COALESCE(business_domain, 'Unspecified') AS business_domain,
                          COALESCE(business_owner, 'Unassigned') AS business_owner,
                          ROUND(expected_sla_hours, 2) AS expected_sla_hours,
                          '$' || FORMAT_NUMBER(ROUND(estimated_impact_usd, 2), 2) AS estimated_impact_usd,
                          trim(CAST(result_payload:applied_filter AS STRING)) AS applied_filter,
                          CAST(result_payload:configured_primary_keys AS STRING) AS configured_primary_keys,
                          CONCAT_WS('.', source_catalog, source_schema, source_table) AS source_table,
                          CONCAT_WS('.', target_catalog, target_schema, target_table) AS target_table
                        FROM
                          ranked
                        WHERE
                          rn = 1
                          AND job_start_ts = (SELECT job_start_ts FROM latest_run)
                        ORDER BY
                          status DESC,
                          validation_name;"""
                    )
                ],
            },
            {
                "name": "ds_performance_metrics",
                "displayName": "Task Performance Metrics",
                "queryLines": [
                    q(
                        "SELECT task_key, "
                        "FORMAT_NUMBER(AVG(CAST(unix_timestamp(validation_complete_ts) - unix_timestamp(validation_begin_ts) AS DOUBLE)), 2) as avg_runtime_seconds, "
                        "FORMAT_NUMBER(MIN(CAST(unix_timestamp(validation_complete_ts) - unix_timestamp(validation_begin_ts) AS BIGINT)), 0) as min_runtime_seconds, "
                        "FORMAT_NUMBER(MAX(CAST(unix_timestamp(validation_complete_ts) - unix_timestamp(validation_begin_ts) AS BIGINT)), 0) as max_runtime_seconds, "
                        "FORMAT_NUMBER(COUNT(*), 0) as total_runs, "
                        "FORMAT_NUMBER(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END), 0) as successful_runs, "
                        "ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) || '%' as success_rate "
                        "FROM {table} "
                        "WHERE job_name = {job} "
                        "AND validation_begin_ts IS NOT NULL AND validation_complete_ts IS NOT NULL "
                        "GROUP BY task_key "
                        "ORDER BY avg_runtime_seconds DESC"
                    )
                ],
            },
            {
                "name": "ds_job_performance",
                "displayName": "Job Run Performance",
                "queryLines": [
                    q(
                        "SELECT run_id, "
                        "MIN(job_start_ts) as job_start, "
                        "MAX(validation_complete_ts) as job_end, "
                        "CAST(unix_timestamp(MAX(validation_complete_ts)) - unix_timestamp(MIN(validation_begin_ts)) AS BIGINT) as total_runtime_seconds, "
                        "COUNT(DISTINCT task_key) as tasks_run, "
                        "SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_tasks, "
                        "SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failed_tasks "
                        "FROM {table} "
                        "WHERE job_name = {job} "
                        "AND validation_begin_ts IS NOT NULL AND validation_complete_ts IS NOT NULL "
                        "GROUP BY run_id "
                        "ORDER BY job_start DESC "
                        "LIMIT 30"
                    )
                ],
            },
            {
                "name": "ds_parallel_efficiency",
                "displayName": "Parallelism & Throughput",
                "queryLines": [
                    q(
                        "WITH base AS (\n"
                        "  SELECT\n"
                        "    run_id,\n"
                        "    task_key,\n"
                        "    validation_begin_ts,\n"
                        "    validation_complete_ts,\n"
                        "    CAST(unix_timestamp(validation_complete_ts) - unix_timestamp(validation_begin_ts) AS DOUBLE) AS task_runtime_seconds\n"
                        "  FROM {table}\n"
                        "  WHERE job_name = {job}\n"
                        "    AND validation_begin_ts IS NOT NULL\n"
                        "    AND validation_complete_ts IS NOT NULL\n"
                        "),\n"
                        "run_windows AS (\n"
                        "  SELECT\n"
                        "    run_id,\n"
                        "    MIN(validation_begin_ts) AS run_start,\n"
                        "    MAX(validation_complete_ts) AS run_end,\n"
                        "    CAST(unix_timestamp(MAX(validation_complete_ts)) - unix_timestamp(MIN(validation_begin_ts)) AS DOUBLE) AS runtime_seconds,\n"
                        "    COUNT(*) AS total_tasks\n"
                        "  FROM base\n"
                        "  GROUP BY run_id\n"
                        "),\n"
                        "concurrency AS (\n"
                        "  SELECT\n"
                        "    a.run_id,\n"
                        "    a.task_key,\n"
                        "    COUNT(*) AS concurrent_tasks\n"
                        "  FROM base a\n"
                        "  JOIN base b\n"
                        "    ON a.run_id = b.run_id\n"
                        "   AND a.validation_begin_ts <= b.validation_complete_ts\n"
                        "   AND b.validation_begin_ts <= a.validation_complete_ts\n"
                        "  GROUP BY a.run_id, a.task_key\n"
                        "),\n"
                        "aggregated AS (\n"
                        "  SELECT\n"
                        "    r.run_id,\n"
                        "    r.run_start,\n"
                        "    r.runtime_seconds,\n"
                        "    r.total_tasks,\n"
                        "    AVG(b.task_runtime_seconds) AS avg_task_duration_seconds,\n"
                        "    percentile_approx(b.task_runtime_seconds, 0.95) AS p95_task_duration_seconds,\n"
                        "    MAX(c.concurrent_tasks) AS peak_parallelism\n"
                        "  FROM run_windows r\n"
                        "  JOIN base b ON r.run_id = b.run_id\n"
                        "  JOIN concurrency c ON c.run_id = b.run_id AND c.task_key = b.task_key\n"
                        "  GROUP BY r.run_id, r.run_start, r.runtime_seconds, r.total_tasks\n"
                        ")\n"
                        "SELECT\n"
                        "  run_id,\n"
                        "  run_start,\n"
                        "  runtime_seconds,\n"
                        "  total_tasks,\n"
                        "  ROUND(total_tasks / NULLIF(runtime_seconds / 60.0, 0), 2) AS tasks_per_minute,\n"
                        "  ROUND(avg_task_duration_seconds, 2) AS avg_task_duration_seconds,\n"
                        "  ROUND(p95_task_duration_seconds, 2) AS p95_task_duration_seconds,\n"
                        "  peak_parallelism,\n"
                        "  DENSE_RANK() OVER (ORDER BY run_start DESC) AS recency_rank\n"
                        "FROM aggregated\n"
                        "ORDER BY run_start DESC\n"
                        "LIMIT 60"
                    )
                ],
            },
            {
                "name": "ds_parallel_kpi",
                "displayName": "Parallelism KPI Snapshot",
                "queryLines": [
                    q(
                        "WITH base AS (\n"
                        "  SELECT\n"
                        "    run_id,\n"
                        "    task_key,\n"
                        "    validation_begin_ts,\n"
                        "    validation_complete_ts,\n"
                        "    CAST(unix_timestamp(validation_complete_ts) - unix_timestamp(validation_begin_ts) AS DOUBLE) AS task_runtime_seconds\n"
                        "  FROM {table}\n"
                        "  WHERE job_name = {job}\n"
                        "    AND validation_begin_ts IS NOT NULL\n"
                        "    AND validation_complete_ts IS NOT NULL\n"
                        "),\n"
                        "run_windows AS (\n"
                        "  SELECT\n"
                        "    run_id,\n"
                        "    MIN(validation_begin_ts) AS run_start,\n"
                        "    MAX(validation_complete_ts) AS run_end,\n"
                        "    CAST(unix_timestamp(MAX(validation_complete_ts)) - unix_timestamp(MIN(validation_begin_ts)) AS DOUBLE) AS runtime_seconds,\n"
                        "    COUNT(*) AS total_tasks\n"
                        "  FROM base\n"
                        "  GROUP BY run_id\n"
                        "),\n"
                        "concurrency AS (\n"
                        "  SELECT\n"
                        "    a.run_id,\n"
                        "    a.task_key,\n"
                        "    COUNT(*) AS concurrent_tasks\n"
                        "  FROM base a\n"
                        "  JOIN base b\n"
                        "    ON a.run_id = b.run_id\n"
                        "   AND a.validation_begin_ts <= b.validation_complete_ts\n"
                        "   AND b.validation_begin_ts <= a.validation_complete_ts\n"
                        "  GROUP BY a.run_id, a.task_key\n"
                        "),\n"
                        "aggregated AS (\n"
                        "  SELECT\n"
                        "    r.run_id,\n"
                        "    r.run_start,\n"
                        "    r.runtime_seconds,\n"
                        "    r.total_tasks,\n"
                        "    ROUND(r.total_tasks / NULLIF(r.runtime_seconds / 60.0, 0), 2) AS tasks_per_minute,\n"
                        "    ROUND(AVG(b.task_runtime_seconds), 2) AS avg_task_duration_seconds,\n"
                        "    ROUND(percentile_approx(b.task_runtime_seconds, 0.95), 2) AS p95_task_duration_seconds,\n"
                        "    MAX(c.concurrent_tasks) AS peak_parallelism,\n"
                        "    DENSE_RANK() OVER (ORDER BY r.run_start DESC) AS recency_rank\n"
                        "  FROM run_windows r\n"
                        "  JOIN base b ON r.run_id = b.run_id\n"
                        "  JOIN concurrency c ON c.run_id = b.run_id AND c.task_key = b.task_key\n"
                        "  GROUP BY r.run_id, r.run_start, r.runtime_seconds, r.total_tasks\n"
                        ")\n"
                        "SELECT\n"
                        "  run_id,\n"
                        "  run_start,\n"
                        "  runtime_seconds,\n"
                        "  total_tasks,\n"
                        "  tasks_per_minute,\n"
                        "  avg_task_duration_seconds,\n"
                        "  p95_task_duration_seconds,\n"
                        "  peak_parallelism\n"
                        "FROM aggregated\n"
                        "WHERE recency_rank = 1\n"
                        "ORDER BY run_start DESC\n"
                        "LIMIT 1"
                    )
                ],
            },
            {
                "name": "ds_runtime_trend",
                "displayName": "Runtime Trend",
                "queryLines": [
                    q(
                        "SELECT MIN(validation_begin_ts)::DATE as run_date, "
                        "AVG(CAST(unix_timestamp(validation_complete_ts) - unix_timestamp(validation_begin_ts) AS DOUBLE)) as avg_runtime_seconds, "
                        "COUNT(DISTINCT run_id) as num_runs "
                        "FROM {table} "
                        "WHERE job_name = {job} "
                        "AND validation_begin_ts IS NOT NULL AND validation_complete_ts IS NOT NULL "
                        "GROUP BY validation_begin_ts::DATE "
                        "ORDER BY run_date DESC "
                        "LIMIT 30"
                    )
                ],
            },
            {
                "name": "ds_cost_history",
                "displayName": "Job Cost History",
                "queryLines": [
                    q(
                        "WITH runs AS (\n"
                        "  SELECT\n"
                        "    j.job_id,\n"
                        "    r.run_id,\n"
                        "    MIN(r.period_start_time) AS run_start_time,\n"
                        "    MAX(r.period_end_time) AS run_end_time\n"
                        "  FROM\n"
                        "    system.lakeflow.job_run_timeline r\n"
                        "      JOIN system.lakeflow.jobs j\n"
                        "        ON r.workspace_id = j.workspace_id\n"
                        "        AND r.job_id = j.job_id\n"
                        "  WHERE\n"
                        "    j.name = {job}\n"
                        "    AND r.period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS\n"
                        "  GROUP BY\n"
                        "    j.job_id,\n"
                        "    r.run_id\n"
                        "),\n"
                        "-- All statements for those runs (only present if the task used a SQL Warehouse)\n"
                        "run_stmt AS (\n"
                        "  SELECT\n"
                        "    q.query_source.job_info.job_run_id AS run_id,\n"
                        "    q.compute.warehouse_id AS warehouse_id,\n"
                        "    date_trunc('hour', q.end_time) AS hour_ts,\n"
                        "    SUM(q.total_duration_ms) AS run_ms_in_hour\n"
                        "  FROM\n"
                        "    system.query.history q\n"
                        "  WHERE\n"
                        "    q.query_source.job_info.job_run_id IN (\n"
                        "      SELECT\n"
                        "        run_id\n"
                        "      FROM\n"
                        "        runs\n"
                        "    )\n"
                        "  GROUP BY\n"
                        "    ALL\n"
                        "),\n"
                        "-- Total statement time per warehouse-hour (denominator for apportioning)\n"
                        "wh_hour_totals AS (\n"
                        "  SELECT\n"
                        "    q.compute.warehouse_id AS warehouse_id,\n"
                        "    date_trunc('hour', q.end_time) AS hour_ts,\n"
                        "    SUM(q.total_duration_ms) AS wh_ms_in_hour\n"
                        "  FROM\n"
                        "    system.query.history q\n"
                        "  WHERE\n"
                        "    (q.compute.warehouse_id, date_trunc('hour', q.end_time)) IN (\n"
                        "      SELECT\n"
                        "        warehouse_id,\n"
                        "        hour_ts\n"
                        "      FROM\n"
                        "        run_stmt\n"
                        "    )\n"
                        "  GROUP BY\n"
                        "    ALL\n"
                        "),\n"
                        "-- Dollar cost per warehouse-hour from system.billing.usage × list_prices\n"
                        "wh_hour_cost AS (\n"
                        "  SELECT\n"
                        "    u.usage_metadata.warehouse_id AS warehouse_id,\n"
                        "    date_trunc('hour', u.usage_end_time) AS hour_ts,\n"
                        "    SUM(u.usage_quantity * lp.pricing.effective_list.default) AS usd_in_hour\n"
                        "  FROM\n"
                        "    system.billing.usage u\n"
                        "      JOIN system.billing.list_prices lp\n"
                        "        ON u.sku_name = lp.sku_name\n"
                        "        AND u.cloud = lp.cloud\n"
                        "        AND u.usage_end_time >= lp.price_start_time\n"
                        "        AND (\n"
                        "          lp.price_end_time IS NULL\n"
                        "          OR u.usage_end_time < lp.price_end_time\n"
                        "        )\n"
                        "  WHERE\n"
                        "    u.usage_metadata.warehouse_id IN (\n"
                        "      SELECT DISTINCT\n"
                        "        warehouse_id\n"
                        "      FROM\n"
                        "        run_stmt\n"
                        "    )\n"
                        "  GROUP BY\n"
                        "    ALL\n"
                        ")\n"
                        "SELECT\n"
                        "  {job} AS job_name,\n"
                        "  r.run_id,\n"
                        "  MIN(r.run_start_time) AS run_start_time,\n"
                        "  MAX(r.run_end_time) AS run_end_time,\n"
                        "  SUM(wh.usd_in_hour * (rs.run_ms_in_hour / NULLIF(wt.wh_ms_in_hour, 0))) AS estimated_run_cost_usd\n"
                        "FROM\n"
                        "  runs r\n"
                        "    JOIN run_stmt rs\n"
                        "      ON r.run_id = rs.run_id\n"
                        "    JOIN wh_hour_totals wt\n"
                        "      ON rs.warehouse_id = wt.warehouse_id\n"
                        "      AND rs.hour_ts = wt.hour_ts\n"
                        "    JOIN wh_hour_cost wh\n"
                        "      ON rs.warehouse_id = wh.warehouse_id\n"
                        "      AND rs.hour_ts = wh.hour_ts\n"
                        "GROUP BY\n"
                        "  r.run_id\n"
                        "ORDER BY\n"
                        "  run_start_time;"
                    )
                ],
            },
        ]

        widget_definitions: list[WidgetDefinition] = [
            {
                "ds_name": "ds_kpi",
                "type": "SUCCESS_RATE_COUNTER",
                "title": "Data Quality Score",
                "pos": {"x": 0, "y": 0, "width": 2, "height": 3},
                "value_col": "data_quality_score",
            },
            {
                "ds_name": "ds_kpi",
                "type": "COUNTER",
                "title": "Critical Issues",
                "pos": {"x": 2, "y": 0, "width": 1, "height": 3},
                "value_col": "failed_tasks",
            },
            {
                "ds_name": "ds_kpi",
                "type": "COUNTER",
                "title": "Total Validations",
                "pos": {"x": 3, "y": 0, "width": 1, "height": 3},
                "value_col": "tables_validated",
            },
            {
                "ds_name": "ds_parallel_kpi",
                "type": "COUNTER",
                "title": "Peak Parallelism",
                "pos": {"x": 4, "y": 0, "width": 1, "height": 3},
                "value_col": "peak_parallelism",
            },
            {
                "ds_name": "ds_parallel_kpi",
                "type": "COUNTER",
                "title": "Throughput (tasks/min)",
                "pos": {"x": 5, "y": 0, "width": 1, "height": 3},
                "value_col": "tasks_per_minute",
            },
            {
                "ds_name": "ds_summary",
                "type": "DONUT",
                "title": "Validation Status Distribution",
                "pos": {"x": 0, "y": 3, "width": 3, "height": 6},
            },
            {
                "ds_name": "ds_failure_rate",
                "type": "LINE",
                "title": "Quality Trend (30 Days)",
                "pos": {"x": 3, "y": 3, "width": 3, "height": 6},
                "show_targets": True,  # Add reference lines for targets
            },
            {
                "ds_name": "ds_failures_by_type",
                "type": "BAR",
                "title": "Issue Classification",
                "pos": {"x": 0, "y": 9, "width": 6, "height": 5},
                "x_field": "validation_type",
                "y_field": "failure_count",
                "y_agg": None,  # Already aggregated in query
            },
            {
                "ds_name": "ds_validation_details",
                "type": "TABLE",
                "title": "Validation Results with Check Details",
                "pos": {"x": 0, "y": 15, "width": 6, "height": 8},
            },
            {
                "ds_name": "ds_business_impact",
                "type": "TABLE",
                "title": "Business Domain Quality Summary",
                "pos": {"x": 0, "y": 23, "width": 3, "height": 5},
            },
            {
                "ds_name": "ds_owner_accountability",
                "type": "TABLE",
                "title": "Owner Accountability",
                "pos": {"x": 3, "y": 23, "width": 3, "height": 5},
            },
            {
                "ds_name": "ds_exploded_checks",
                "type": "TABLE",
                "title": "Check Details",
                "pos": {"x": 0, "y": 33, "width": 6, "height": 9},
            },
            {
                "ds_name": "ds_top_failures",
                "type": "BAR",
                "title": "Top Failing Validations",
                "pos": {"x": 0, "y": 28, "width": 3, "height": 5},
                "x_field": "task_key",
                "y_field": "failure_count",
                "y_agg": None,  # Already aggregated in query
            },
            {
                "ds_name": "ds_priority_profile",
                "type": "BAR",
                "title": "Priority Risk Profile",
                "pos": {"x": 3, "y": 28, "width": 3, "height": 5},
                "x_field": "business_priority",
                "y_field": "failed_validations",
                "y_agg": None,
                "y_display": "Failures",
            },
        ]

        layout_widgets: list[dict[str, Any]] = []
        dashboard_filters: list[dict[str, Any]] = [
            {
                "widget": {
                    "name": "3b28acc4",
                    "queries": [
                        {
                            "name": "ds_validation_details_overall_status",
                            "query": {
                                "datasetName": "ds_validation_details",
                                "fields": [
                                    {
                                        "name": "overall_status",
                                        "expression": "`overall_status`",
                                    },
                                    {
                                        "name": "overall_status_associativity",
                                        "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                    },
                                ],
                                "disaggregated": False,
                            },
                        }
                    ],
                    "spec": {
                        "version": 2,
                        "widgetType": "filter-single-select",
                        "encodings": {
                            "fields": [
                                {
                                    "fieldName": "overall_status",
                                    "displayName": "overall_status",
                                    "queryName": "ds_validation_details_overall_status",
                                }
                            ]
                        },
                        "frame": {"showTitle": True, "title": "Test Status"},
                    },
                },
                "position": {"x": 0, "y": 14, "width": 2, "height": 1},
            },
            {
                "widget": {
                    "name": "4c785157",
                    "queries": [
                        {
                            "name": "filter_business_priority_1",
                            "query": {
                                "datasetName": "ds_validation_details",
                                "fields": [
                                    {
                                        "name": "business_priority",
                                        "expression": "`business_priority`",
                                    },
                                    {
                                        "name": "business_priority_associativity",
                                        "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                    },
                                ],
                                "disaggregated": False,
                            },
                        }
                    ],
                    "spec": {
                        "version": 2,
                        "widgetType": "filter-multi-select",
                        "encodings": {
                            "fields": [
                                {
                                    "fieldName": "business_priority",
                                    "displayName": "business_priority",
                                    "queryName": "filter_business_priority_1",
                                }
                            ]
                        },
                        "frame": {"showTitle": True, "title": "Business Priority"},
                    },
                },
                "position": {"x": 2, "y": 14, "width": 2, "height": 1},
            },
            {
                "widget": {
                    "name": "b82f2304",
                    "queries": [
                        {
                            "name": "filter_business_owner_1",
                            "query": {
                                "datasetName": "ds_validation_details",
                                "fields": [
                                    {
                                        "name": "business_owner",
                                        "expression": "`business_owner`",
                                    },
                                    {
                                        "name": "business_owner_associativity",
                                        "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                    },
                                ],
                                "disaggregated": False,
                            },
                        }
                    ],
                    "spec": {
                        "version": 2,
                        "widgetType": "filter-multi-select",
                        "encodings": {
                            "fields": [
                                {
                                    "fieldName": "business_owner",
                                    "displayName": "business_owner",
                                    "queryName": "filter_business_owner_1",
                                }
                            ]
                        },
                        "frame": {"showTitle": True, "title": "Data Owner"},
                    },
                },
                "position": {"x": 4, "y": 14, "width": 2, "height": 1},
            },
            {
                "widget": {
                    "name": "56ba318f",
                    "queries": [
                        {
                            "name": "ds_exploded_checks_status",
                            "query": {
                                "datasetName": "ds_exploded_checks",
                                "fields": [
                                    {"name": "status", "expression": "`status`"},
                                    {
                                        "name": "status_associativity",
                                        "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                    },
                                ],
                                "disaggregated": False,
                            },
                        }
                    ],
                    "spec": {
                        "version": 2,
                        "widgetType": "filter-single-select",
                        "encodings": {
                            "fields": [
                                {
                                    "fieldName": "status",
                                    "displayName": "status",
                                    "queryName": "ds_exploded_checks_status",
                                }
                            ]
                        },
                        "frame": {"showTitle": True, "title": "Validation Status"},
                    },
                },
                "position": {"x": 0, "y": 33, "width": 6, "height": 1},
            },
        ]
        main_page_filter_metadata: list[dict[str, Any]] = [
            {
                "name": "job_name",
                "field": "job_name",
                "dataset": "ds_validation_details",
                "displayName": "Job Name",
                "allowMultipleValues": False,
            },
            {
                "name": "run_id",
                "field": "run_id",
                "dataset": "ds_validation_details",
                "displayName": "Run ID",
                "allowMultipleValues": False,
            },
            {
                "name": "status_filter",
                "field": "overall_status",
                "dataset": "ds_validation_details",
                "displayName": "Validation Status",
                "allowMultipleValues": True,
                "defaultValues": ["SUCCESS", "FAILURE"],
            },
            {
                "name": "task_key_filter_main",
                "field": "validation_name",
                "dataset": "ds_validation_details",
                "displayName": "Validation Name",
                "allowMultipleValues": True,
            },
            {
                "name": "time_range",
                "field": "job_start_ts",
                "dataset": "ds_validation_details",
                "displayName": "Time Range",
                "type": "date_range",
            },
            {
                "name": "business_priority_filter",
                "field": "business_priority",
                "dataset": "ds_validation_details",
                "displayName": "Business Priority",
                "allowMultipleValues": True,
            },
        ]

        details_page_filter_metadata: list[dict[str, Any]] = [
            {
                "name": "status_filter_details",
                "field": "status",
                "dataset": "ds_history",
                "displayName": "Validation Status",
                "allowMultipleValues": True,
            },
            {
                "name": "task_key_filter",
                "field": "task_key",
                "dataset": "ds_history",
                "displayName": "Task Key",
                "allowMultipleValues": True,
            },
            {
                "name": "job_name_details",
                "field": "job_name",
                "dataset": "ds_history",
                "displayName": "Job Name",
                "allowMultipleValues": False,
            },
            {
                "name": "run_id_details",
                "field": "run_id",
                "dataset": "ds_history",
                "displayName": "Run ID",
                "allowMultipleValues": False,
            },
            {
                "name": "time_range_details",
                "field": "job_start_ts",
                "dataset": "ds_history",
                "displayName": "Time Range",
                "type": "date_range",
            },
        ]

        for i, w_def in enumerate(widget_definitions):
            spec: dict[str, Any] = {}
            query_fields: list[dict[str, str]] = []
            encodings: dict[str, Any] = {}

            if w_def["type"] == "COUNTER":
                # Safely obtain the optional 'value_col' key from the TypedDict
                value_col = w_def.get("value_col")
                if not isinstance(value_col, str):
                    # Fallback to a safe default name when not provided
                    value_col = "value_col"

                query_fields = [
                    {
                        "name": value_col,
                        "expression": f"`{value_col}`",
                    }
                ]
                encodings = {"value": {"fieldName": value_col}}

                # Add conditional formatting for counters
                if w_def.get("title") == "Critical Issues":
                    encodings["value"] = {
                        "fieldName": "failed_tasks",
                        "format": {
                            "type": "number",
                            "conditionalFormats": [
                                {
                                    "condition": {"type": "equals", "value": 0},
                                    "textColor": "#FAD6FF",
                                    "backgroundColor": "#2B1030",
                                },
                                {
                                    "condition": {"type": "greaterThan", "value": 0},
                                    "textColor": "#FF6EC7",
                                    "backgroundColor": "#360B3C",
                                },
                            ],
                        },
                        "displayName": "failed_tasks",
                        "style": {
                            "rules": [
                                {
                                    "condition": {
                                        "operator": ">",
                                        "operand": {
                                            "type": "data-value",
                                            "value": "-999999",
                                        },
                                    },
                                    "color": "#FF6EC7",
                                }
                            ]
                        },
                    }
                elif w_def.get("title") == "Peak Parallelism":
                    encodings["value"]["format"] = {
                        "type": "number",
                        "conditionalFormats": [
                            {
                                "condition": {
                                    "type": "greaterThanOrEquals",
                                    "value": 16,
                                },
                                "textColor": "#22D3EE",
                                "backgroundColor": "#051B2C",
                            },
                            {
                                "condition": {
                                    "type": "between",
                                    "min": 8,
                                    "max": 15.99,
                                },
                                "textColor": "#FACC15",
                                "backgroundColor": "#2B1A05",
                            },
                            {
                                "condition": {"type": "lessThan", "value": 8},
                                "textColor": "#F87171",
                                "backgroundColor": "#2C0A0A",
                            },
                        ],
                    }
                    encodings["value"]["style"] = {
                        "rules": [
                            {
                                "condition": {
                                    "operator": ">",
                                    "operand": {
                                        "type": "data-value",
                                        "value": "-999999",
                                    },
                                },
                                "color": "#FF6EC7",
                            }
                        ]
                    }
                elif w_def.get("title") == "Throughput (tasks/min)":
                    encodings["value"]["format"] = {
                        "type": "number",
                        "decimalPlaces": {"type": "max", "places": 2},
                        "conditionalFormats": [
                            {
                                "condition": {
                                    "type": "greaterThanOrEquals",
                                    "value": 12,
                                },
                                "textColor": "#34D399",
                                "backgroundColor": "#0A2D1F",
                            },
                            {
                                "condition": {
                                    "type": "between",
                                    "min": 8,
                                    "max": 11.99,
                                },
                                "textColor": "#FBBF24",
                                "backgroundColor": "#32200A",
                            },
                            {
                                "condition": {"type": "lessThan", "value": 8},
                                "textColor": "#F87171",
                                "backgroundColor": "#2C0A0A",
                            },
                        ],
                    }
                    encodings["value"]["style"] = {
                        "rules": [
                            {
                                "condition": {
                                    "operator": ">",
                                    "operand": {
                                        "type": "data-value",
                                        "value": "-999999",
                                    },
                                },
                                "color": "#FF6EC7",
                            }
                        ]
                    }
                elif w_def.get("title") == "Total Validations":
                    encodings["value"]["format"] = {
                        "type": "number",
                        "conditionalFormats": [
                            {
                                "condition": {
                                    "type": "greaterThan",
                                    "value": 100,
                                },
                                "textColor": "#00A972",
                                "backgroundColor": "#E8F5E9",
                            },
                            {
                                "condition": {
                                    "type": "lessThanOrEquals",
                                    "value": 100,
                                },
                                "textColor": "#FF9800",
                                "backgroundColor": "#FFF7ED",
                            },
                        ],
                    }
                    encodings["value"]["style"] = {
                        "rules": [
                            {
                                "condition": {
                                    "operator": ">",
                                    "operand": {
                                        "type": "data-value",
                                        "value": "-999999",
                                    },
                                },
                                "color": "#FF6EC7",
                            }
                        ]
                    }

                spec = {
                    "version": 3,
                    "widgetType": "counter",
                    "encodings": encodings,
                }
            elif w_def["type"] == "SUCCESS_RATE_COUNTER":
                # value_col is optional in the WidgetDefinition TypedDict; use .get() and validate
                value_col = w_def.get("value_col")
                if not isinstance(value_col, str):
                    # fallback to a safe default when not provided
                    value_col = "value_col"

                query_fields = [
                    {
                        "name": value_col,
                        "expression": f"`{value_col}`",
                    }
                ]
                spec = {
                    "version": 2,
                    "widgetType": "counter",
                    "encodings": {
                        "value": {
                            "fieldName": value_col,
                            "format": {
                                "type": "number-percent",
                                "decimalPlaces": {"type": "max", "places": 2},
                                "conditionalFormats": [
                                    {
                                        "condition": {
                                            "type": "greaterThanOrEquals",
                                            "value": 0.99,
                                        },
                                        "textColor": "#FF85E1",
                                        "backgroundColor": "#311338",
                                    },
                                    {
                                        "condition": {
                                            "type": "between",
                                            "min": 0.95,
                                            "max": 0.9899,
                                        },
                                        "textColor": "#FF6EC7",
                                        "backgroundColor": "#2A0F30",
                                    },
                                    {
                                        "condition": {
                                            "type": "lessThan",
                                            "value": 0.95,
                                        },
                                        "textColor": "#F749C2",
                                        "backgroundColor": "#2C0A2C",
                                    },
                                ],
                            },
                            "style": {
                                "rules": [
                                    {
                                        "condition": {
                                            "operator": ">",
                                            "operand": {
                                                "type": "data-value",
                                                "value": "-999999",
                                            },
                                        },
                                        "color": "#FF6EC7",
                                    },
                                ]
                            },
                        }
                    },
                }

            elif w_def["type"] == "DONUT":
                query_fields = [
                    {"name": "sum(task_count)", "expression": "SUM(`task_count`)"},
                    {"name": "status", "expression": "`status`"},
                ]
                spec = {
                    "version": 3,
                    "widgetType": "pie",
                    "encodings": {
                        "angle": {
                            "fieldName": "sum(task_count)",
                            "scale": {"type": "quantitative"},
                            "displayName": "Proportion of Passed vs. Failed Validations",
                        },
                        "color": {
                            "fieldName": "status",
                            "scale": {
                                "type": "categorical",
                                "mappings": [
                                    {
                                        "value": "Passed",
                                        "color": "#00A972",  # Green for success
                                    },
                                    {
                                        "value": "Failed",
                                        "color": "#E92828",  # Red for failure
                                    },
                                ],
                            },
                            "displayName": "Status",
                        },
                        "label": {"show": True},
                    },
                }

            elif w_def["type"] == "LINE":
                query_fields = [
                    {"name": "run_date", "expression": "`run_date`"},
                    {"name": "avg(failure_rate)", "expression": "AVG(`failure_rate`)"},
                ]
                spec = {
                    "version": 3,
                    "widgetType": "line",
                    "encodings": {
                        "x": {
                            "fieldName": "run_date",
                            "scale": {"type": "temporal"},
                            "displayName": "Date",
                        },
                        "y": {
                            "fieldName": "avg(failure_rate)",
                            "scale": {"type": "quantitative"},
                            "displayName": "Failure Rate (%)",
                        },
                    },
                }
                # Add reference lines for targets if requested
                if w_def.get("show_targets"):
                    spec["referenceLines"] = [
                        {
                            "value": 5,
                            "label": "Target (5%)",
                            "color": "#00A972",
                            "style": "dashed",
                        },
                        {
                            "value": 10,
                            "label": "Warning (10%)",
                            "color": "#FF9800",
                            "style": "dashed",
                        },
                    ]

            elif w_def["type"] == "BAR":
                x_field = cast(str, w_def.get("x_field", "task_key"))
                raw_y_field = w_def.get("y_field")
                y_field = raw_y_field if raw_y_field else "failure_count"
                y_agg = w_def.get("y_agg")
                if y_agg is None and "y_agg" not in w_def:
                    y_agg = "SUM"

                if y_agg:
                    y_agg = y_agg.upper()
                    y_alias = f"{y_agg.lower()}({y_field})"
                    query_fields = [
                        {"name": x_field, "expression": f"`{x_field}`"},
                        {"name": y_alias, "expression": f"{y_agg}(`{y_field}`)"},
                    ]
                else:
                    # Field is already aggregated in the query
                    y_alias = y_field
                    query_fields = [
                        {"name": x_field, "expression": f"`{x_field}`"},
                        {"name": y_field, "expression": f"`{y_field}`"},
                    ]

                # Use custom display names for aggregated fields
                default_display = "Failures" if "failure" in y_field.lower() else "Count"
                y_display_name = w_def.get("y_display", default_display)

                encodings = {
                    "x": {
                        "fieldName": x_field,
                        "scale": {"type": "categorical"},
                        "displayName": w_def.get("x_display", "Validation"),
                    },
                    "y": {
                        "fieldName": y_alias,
                        "scale": {"type": "quantitative"},
                        "displayName": y_display_name,
                    },
                }

                if w_def.get("title") == "Issue Classification":
                    encodings["color"] = {
                        "fieldName": "failure_count",
                        "scale": {
                            "type": "quantitative",
                            "reverse": False,
                            "colorRamp": {"mode": "scheme", "scheme": "orangered"},
                        },
                        "displayName": "failure_count",
                    }
                elif w_def.get("title") == "Top Failing Validations":
                    encodings["color"] = {
                        "fieldName": "failure_count",
                        "scale": {
                            "type": "quantitative",
                            "reverse": True,
                            "colorRamp": {"mode": "scheme", "scheme": "magma"},
                        },
                        "displayName": "failure_count",
                    }

                spec = {
                    "version": 3,
                    "widgetType": "bar",
                    "encodings": encodings,
                }
                # Best-effort drill-through: clicking a bar navigates to details page filtered by task_key
                if w_def.get("ds_name") == "ds_top_failures":
                    spec["interactions"] = [
                        {
                            "type": "drillthrough",
                            "targetPage": "details_page",
                            "filters": [
                                {
                                    "dataset": "ds_latest_run_details",
                                    "targetField": "task_key",
                                    "sourceField": x_field,
                                }
                            ],
                        }
                    ]

            elif w_def["type"] == "TABLE":
                # Different table widgets need different columns
                if w_def["ds_name"] == "ds_business_impact":
                    columns = [
                        "business_domain",
                        "total_validations",
                        "failed_validations",
                        "quality_score",
                        "potential_impact_usd",
                        "realized_impact_usd",
                        "avg_expected_sla_hours",
                        "health_status",
                        "sla_profile",
                        "last_issue",
                    ]
                    display_names = [
                        "Business Domain",
                        "Total Validations",
                        "Failures",
                        "Quality Score",
                        "Potential Financial Impact",
                        "Realized Financial Impact",
                        "Avg SLA (hrs)",
                        "Health Status",
                        "SLA Profile",
                        "Last Issue Timestamp",
                    ]
                elif w_def["ds_name"] == "ds_validation_details":
                    columns = [
                        "validation_name",
                        "overall_status",
                        "count_check",
                        "hash_check",
                        "null_check",
                        "unique_check",
                        "agg_check",
                        "custom_sql_check",
                        "business_priority",
                        "business_domain",
                        "business_owner",
                        "expected_sla_hours",
                        "estimated_impact_usd",
                        "applied_filter",
                        "configured_primary_keys",
                        "source_table",
                        "target_table",
                    ]
                    display_names = [
                        "Validation",
                        "Status",
                        "Count",
                        "Hash",
                        "Nulls",
                        "Unique",
                        "Aggs",
                        "Custom SQL",
                        "Priority",
                        "Domain",
                        "Owner",
                        "SLA (hrs)",
                        "Financial Impact",
                        "Applied Filter",
                        "Primary Keys",
                        "Source Table",
                        "Target Table",
                    ]
                elif w_def["ds_name"] == "ds_owner_accountability":
                    columns = [
                        "business_owner",
                        "total_validations",
                        "failed_validations",
                        "success_rate_percent",
                        "potential_impact_usd",
                        "realized_impact_usd",
                        "avg_expected_sla_hours",
                        "last_issue",
                    ]
                    display_names = [
                        "Owner",
                        "Validations",
                        "Failures",
                        "Success Rate",
                        "Potential Financial Impact",
                        "Realized Financial Impact",
                        "Avg SLA (hrs)",
                        "Last Issue",
                    ]
                elif w_def["ds_name"] == "ds_exploded_checks":
                    columns = [
                        "validation_name",
                        "check_type",
                        "status",
                        "details",
                    ]
                    display_names = [
                        "Validation",
                        "Check Type",
                        "Status",
                        "Details",
                    ]
                elif w_def["ds_name"] == "ds_latest_run_details":
                    columns = [
                        "task_key",
                        "status",
                        "source_table",
                        "target_table",
                        "validation_begin_ts",
                        "result_payload",
                    ]
                    display_names = [
                        "Task Key",
                        "Status",
                        "Source Table",
                        "Target Table",
                        "Job Complete Timestamp",
                        "Result Payload",
                    ]
                else:
                    # Default columns for other table widgets
                    columns = [
                        "task_key",
                        "status",
                        "validation_begin_ts",
                        "payload_json",
                        "run_id",
                        "job_name",
                    ]
                    display_names = [
                        "Task Key",
                        "Status",
                        "Job Complete Timestamp",
                        "Result Payload",
                        "Run ID",
                        "Job Name",
                    ]

                query_fields = [{"name": c, "expression": f"`{c}`"} for c in columns]

                # Build column encodings with conditional formatting
                column_encodings = []
                for col, display in zip(columns, display_names, strict=False):
                    col_encoding: dict[str, Any] = {
                        "fieldName": col,
                        "displayName": display,
                    }

                    # Add conditional formatting for status columns
                    if col in ["overall_status", "status", "health_status"]:
                        col_encoding["cellFormat"] = {
                            "conditionalFormats": [
                                {
                                    "condition": {"type": "equals", "value": "✅"},
                                    "textColor": "#00A972",
                                    "backgroundColor": "#E8F5E9",
                                },
                                {
                                    "condition": {"type": "equals", "value": "❌"},
                                    "textColor": "#FF3621",
                                    "backgroundColor": "#FFEBEE",
                                },
                                {
                                    "condition": {
                                        "type": "contains",
                                        "value": "SUCCESS",
                                    },
                                    "textColor": "#00A972",
                                    "backgroundColor": "#E8F5E9",
                                },
                                {
                                    "condition": {
                                        "type": "contains",
                                        "value": "FAILURE",
                                    },
                                    "textColor": "#FF3621",
                                    "backgroundColor": "#FFEBEE",
                                },
                                {
                                    "condition": {"type": "contains", "value": "🟢"},
                                    "textColor": "#00A972",
                                },
                                {
                                    "condition": {"type": "contains", "value": "🟡"},
                                    "textColor": "#FF9800",
                                },
                                {
                                    "condition": {"type": "contains", "value": "🟠"},
                                    "textColor": "#FF6B00",
                                },
                                {
                                    "condition": {"type": "contains", "value": "🔴"},
                                    "textColor": "#FF3621",
                                },
                            ]
                        }
                    # Format quality score column
                    elif col == "quality_score":
                        col_encoding["format"] = {
                            "type": "number",
                            "decimalPlaces": {"type": "max", "places": 2},
                            "conditionalFormats": [
                                {
                                    "condition": {
                                        "type": "greaterThanOrEquals",
                                        "value": 99,
                                    },
                                    "textColor": "#00A972",
                                },
                                {
                                    "condition": {
                                        "type": "between",
                                        "min": 95,
                                        "max": 99,
                                    },
                                    "textColor": "#FF9800",
                                },
                                {
                                    "condition": {"type": "lessThan", "value": 95},
                                    "textColor": "#FF3621",
                                },
                            ],
                        }
                    # Format failure counts
                    elif col in ["failures", "failure_count"]:
                        col_encoding["format"] = {
                            "type": "number",
                            "conditionalFormats": [
                                {
                                    "condition": {"type": "equals", "value": 0},
                                    "textColor": "#00A972",
                                },
                                {
                                    "condition": {"type": "greaterThan", "value": 0},
                                    "textColor": "#FF3621",
                                },
                            ],
                        }

                    column_encodings.append(col_encoding)

                spec = {
                    "version": 3,
                    "widgetType": "table",
                    "encodings": {"columns": column_encodings},
                }

            spec["frame"] = {"title": w_def["title"], "showTitle": True}
            layout_widgets.append(
                {
                    "widget": {
                        "name": f"w_{i}",
                        "queries": [
                            {
                                "name": "main_query",
                                "query": {
                                    "datasetName": w_def["ds_name"],
                                    "fields": query_fields,
                                    "disaggregated": False,
                                },
                            }
                        ],
                        "spec": spec,
                    },
                    "position": w_def["pos"],
                }
            )

        layout_widgets.extend(dashboard_filters)

        dashboard_payload: DashboardPayload = {
            "datasets": datasets,
            "pages": [
                {
                    "name": "main_page",
                    "displayName": "Executive Data Quality Dashboard",
                    "layout": layout_widgets,
                    "pageType": "PAGE_TYPE_CANVAS",
                    "filters": main_page_filter_metadata,
                },
                {
                    "name": "details_page",
                    "displayName": "Historical Validation Runs",
                    "layout": [
                        {
                            "widget": {
                                "name": "details_table",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_history",
                                            "fields": [
                                                {
                                                    "name": "task_key",
                                                    "expression": "`task_key`",
                                                },
                                                {
                                                    "name": "status",
                                                    "expression": "`status`",
                                                },
                                                {
                                                    "name": "job_start_ts",
                                                    "expression": "`job_start_ts`",
                                                },
                                                {
                                                    "name": "payload_json",
                                                    "expression": "`payload_json`",
                                                },
                                                {
                                                    "name": "run_id",
                                                    "expression": "`run_id`",
                                                },
                                                {
                                                    "name": "job_name",
                                                    "expression": "`job_name`",
                                                },
                                                {
                                                    "name": "is_filtered",
                                                    "expression": "`is_filtered`",
                                                },
                                                {
                                                    "name": "applied_filter",
                                                    "expression": "`applied_filter`",
                                                },
                                                {
                                                    "name": "configured_primary_keys",
                                                    "expression": "`configured_primary_keys`",
                                                },
                                                {
                                                    "name": "business_priority",
                                                    "expression": "`business_priority`",
                                                },
                                                {
                                                    "name": "business_domain",
                                                    "expression": "`business_domain`",
                                                },
                                                {
                                                    "name": "business_owner",
                                                    "expression": "`business_owner`",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "table",
                                    "encodings": {
                                        "columns": [
                                            {
                                                "fieldName": "task_key",
                                                "displayName": "Task Key",
                                            },
                                            {
                                                "fieldName": "status",
                                                "displayName": "Status",
                                            },
                                            {
                                                "fieldName": "job_start_ts",
                                                "displayName": "Job Start Timestamp",
                                            },
                                            {
                                                "fieldName": "is_filtered",
                                                "displayName": "Filter Applied?",
                                            },
                                            {
                                                "fieldName": "applied_filter",
                                                "displayName": "Applied Filter",
                                            },
                                            {
                                                "fieldName": "configured_primary_keys",
                                                "displayName": "Primary Keys",
                                            },
                                            {
                                                "fieldName": "payload_json",
                                                "displayName": "Result Payload",
                                            },
                                            {
                                                "fieldName": "run_id",
                                                "displayName": "Run ID",
                                            },
                                            {
                                                "fieldName": "job_name",
                                                "displayName": "Job Name",
                                            },
                                            {
                                                "fieldName": "business_priority",
                                                "displayName": "Business Priority",
                                            },
                                            {
                                                "fieldName": "business_domain",
                                                "displayName": "Business Domain",
                                            },
                                            {
                                                "fieldName": "business_owner",
                                                "displayName": "Business Owner",
                                            },
                                        ]
                                    },
                                    "frame": {
                                        "title": "All Run Details",
                                        "showTitle": True,
                                    },
                                },
                            },
                            "position": {"x": 0, "y": 0, "width": 6, "height": 18},
                        },
                        {
                            "widget": {
                                "name": "success_trend",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_success_trend",
                                            "fields": [
                                                {
                                                    "name": "run_date",
                                                    "expression": "`run_date`",
                                                },
                                                {
                                                    "name": "avg(success_rate)",
                                                    "expression": "AVG(`success_rate`)",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "line",
                                    "encodings": {
                                        "x": {
                                            "fieldName": "run_date",
                                            "scale": {"type": "temporal"},
                                            "displayName": "Date",
                                        },
                                        "y": {
                                            "fieldName": "avg(success_rate)",
                                            "scale": {"type": "quantitative"},
                                            "displayName": "Success Rate",
                                        },
                                    },
                                    "frame": {
                                        "title": "Success Rate Over Time",
                                        "showTitle": True,
                                    },
                                },
                            },
                            "position": {"x": 0, "y": 18, "width": 6, "height": 9},
                        },
                        {
                            "widget": {
                                "name": "f56055e3",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_cost_history",
                                            "fields": [
                                                {
                                                    "name": "run_end_time",
                                                    "expression": "`run_end_time`",
                                                },
                                                {
                                                    "name": "job_name",
                                                    "expression": "`job_name`",
                                                },
                                                {
                                                    "name": "run_start_time",
                                                    "expression": "`run_start_time`",
                                                },
                                                {
                                                    "name": "estimated_run_cost_usd",
                                                    "expression": "`estimated_run_cost_usd`",
                                                },
                                            ],
                                            "disaggregated": True,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "scatter",
                                    "encodings": {
                                        "x": {
                                            "fieldName": "run_start_time",
                                            "scale": {"type": "temporal"},
                                            "displayName": "Job Start Time",
                                        },
                                        "y": {
                                            "fieldName": "estimated_run_cost_usd",
                                            "scale": {"type": "quantitative"},
                                            "displayName": "Job Cost ($)",
                                        },
                                        "extra": [
                                            {
                                                "fieldName": "run_end_time",
                                                "displayName": "Job End Time",
                                            },
                                            {
                                                "fieldName": "job_name",
                                                "displayName": "Job Name",
                                            },
                                        ],
                                    },
                                    "frame": {
                                        "showTitle": True,
                                        "title": "Job Cost Over Time",
                                        "showDescription": True,
                                        "description": "How much money each job run cost over time",
                                    },
                                    "mark": {"size": 1},
                                },
                            },
                            "position": {"x": 0, "y": 28, "width": 6, "height": 12},
                        },
                        {
                            "widget": {
                                "name": "a87e3364",
                                "queries": [
                                    {
                                        "name": "filter_task_key",
                                        "query": {
                                            "datasetName": "ds_history",
                                            "fields": [
                                                {
                                                    "name": "task_key",
                                                    "expression": "`task_key`",
                                                },
                                                {
                                                    "name": "task_key_associativity",
                                                    "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "filter-multi-select",
                                    "encodings": {
                                        "fields": [
                                            {
                                                "fieldName": "task_key",
                                                "displayName": "task_key",
                                                "queryName": "filter_task_key",
                                            }
                                        ]
                                    },
                                    "frame": {"showTitle": True, "title": "Task Key"},
                                },
                            },
                            "position": {"x": 0, "y": 0, "width": 2, "height": 1},
                        },
                        {
                            "widget": {
                                "name": "ed13b7d0",
                                "queries": [
                                    {
                                        "name": "filter_business_priority_2",
                                        "query": {
                                            "datasetName": "ds_history",
                                            "fields": [
                                                {
                                                    "name": "business_priority",
                                                    "expression": "`business_priority`",
                                                },
                                                {
                                                    "name": "business_priority_associativity",
                                                    "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "filter-multi-select",
                                    "encodings": {
                                        "fields": [
                                            {
                                                "fieldName": "business_priority",
                                                "displayName": "business_priority",
                                                "queryName": "filter_business_priority_2",
                                            }
                                        ]
                                    },
                                    "frame": {
                                        "showTitle": True,
                                        "title": "Business Priority",
                                    },
                                },
                            },
                            "position": {"x": 2, "y": 0, "width": 2, "height": 1},
                        },
                        {
                            "widget": {
                                "name": "fc07e7f7",
                                "queries": [
                                    {
                                        "name": "filter_business_domain_2",
                                        "query": {
                                            "datasetName": "ds_history",
                                            "fields": [
                                                {
                                                    "name": "business_domain",
                                                    "expression": "`business_domain`",
                                                },
                                                {
                                                    "name": "business_domain_associativity",
                                                    "expression": "COUNT_IF(`associative_filter_predicate_group`)",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 2,
                                    "widgetType": "filter-multi-select",
                                    "encodings": {
                                        "fields": [
                                            {
                                                "fieldName": "business_domain",
                                                "displayName": "business_domain",
                                                "queryName": "filter_business_domain_2",
                                            }
                                        ]
                                    },
                                    "frame": {
                                        "showTitle": True,
                                        "title": "Business Domain",
                                    },
                                },
                            },
                            "position": {"x": 4, "y": 0, "width": 2, "height": 1},
                        },
                    ],
                    "pageType": "PAGE_TYPE_CANVAS",
                    "filters": details_page_filter_metadata,
                },
                {
                    "name": "performance_page",
                    "displayName": "Performance Metrics",
                    "layout": [
                        {
                            "widget": {
                                "name": "job_performance_table",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_job_performance",
                                            "fields": [
                                                {
                                                    "name": "run_id",
                                                    "expression": "`run_id`",
                                                },
                                                {
                                                    "name": "job_start",
                                                    "expression": "`job_start`",
                                                },
                                                {
                                                    "name": "job_end",
                                                    "expression": "`job_end`",
                                                },
                                                {
                                                    "name": "total_runtime_seconds",
                                                    "expression": "`total_runtime_seconds`",
                                                },
                                                {
                                                    "name": "tasks_run",
                                                    "expression": "`tasks_run`",
                                                },
                                                {
                                                    "name": "successful_tasks",
                                                    "expression": "`successful_tasks`",
                                                },
                                                {
                                                    "name": "failed_tasks",
                                                    "expression": "`failed_tasks`",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "table",
                                    "encodings": {
                                        "columns": [
                                            {
                                                "fieldName": "run_id",
                                                "displayName": "Run ID",
                                            },
                                            {
                                                "fieldName": "job_start",
                                                "displayName": "Start Time",
                                                "format": {"type": "datetime"},
                                            },
                                            {
                                                "fieldName": "job_end",
                                                "displayName": "End Time",
                                                "format": {"type": "datetime"},
                                            },
                                            {
                                                "fieldName": "total_runtime_seconds",
                                                "displayName": "Runtime (sec)",
                                                "format": {
                                                    "type": "number",
                                                    "decimalPlaces": {
                                                        "type": "max",
                                                        "places": 2,
                                                    },
                                                },
                                            },
                                            {
                                                "fieldName": "tasks_run",
                                                "displayName": "Total Tasks",
                                            },
                                            {
                                                "fieldName": "successful_tasks",
                                                "displayName": "Successful",
                                            },
                                            {
                                                "fieldName": "failed_tasks",
                                                "displayName": "Failed",
                                            },
                                        ]
                                    },
                                    "frame": {
                                        "title": "Job Execution History",
                                        "showTitle": True,
                                    },
                                },
                            },
                            "position": {"x": 0, "y": 0, "width": 6, "height": 8},
                        },
                        {
                            "widget": {
                                "name": "task_performance_table",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_performance_metrics",
                                            "fields": [
                                                {
                                                    "name": "task_key",
                                                    "expression": "`task_key`",
                                                },
                                                {
                                                    "name": "avg_runtime_seconds",
                                                    "expression": "`avg_runtime_seconds`",
                                                },
                                                {
                                                    "name": "min_runtime_seconds",
                                                    "expression": "`min_runtime_seconds`",
                                                },
                                                {
                                                    "name": "max_runtime_seconds",
                                                    "expression": "`max_runtime_seconds`",
                                                },
                                                {
                                                    "name": "total_runs",
                                                    "expression": "`total_runs`",
                                                },
                                                {
                                                    "name": "success_rate",
                                                    "expression": "`success_rate`",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "table",
                                    "encodings": {
                                        "columns": [
                                            {
                                                "fieldName": "task_key",
                                                "displayName": "Task",
                                            },
                                            {
                                                "fieldName": "avg_runtime_seconds",
                                                "displayName": "Avg Runtime (sec)",
                                                "format": {
                                                    "type": "number",
                                                    "decimalPlaces": {
                                                        "type": "max",
                                                        "places": 2,
                                                    },
                                                },
                                            },
                                            {
                                                "fieldName": "min_runtime_seconds",
                                                "displayName": "Min (sec)",
                                                "format": {
                                                    "type": "number",
                                                    "decimalPlaces": {
                                                        "type": "max",
                                                        "places": 2,
                                                    },
                                                },
                                            },
                                            {
                                                "fieldName": "max_runtime_seconds",
                                                "displayName": "Max (sec)",
                                                "format": {
                                                    "type": "number",
                                                    "decimalPlaces": {
                                                        "type": "max",
                                                        "places": 2,
                                                    },
                                                },
                                            },
                                            {
                                                "fieldName": "total_runs",
                                                "displayName": "Runs",
                                            },
                                            {
                                                "fieldName": "success_rate",
                                                "displayName": "Success Rate",
                                                "format": {
                                                    "type": "number",
                                                    "decimalPlaces": {
                                                        "type": "max",
                                                        "places": 2,
                                                    },
                                                },
                                            },
                                        ]
                                    },
                                    "frame": {
                                        "title": "Task Performance Statistics",
                                        "showTitle": True,
                                    },
                                },
                            },
                            "position": {"x": 0, "y": 8, "width": 6, "height": 10},
                        },
                        {
                            "widget": {
                                "name": "parallel_throughput_chart",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_parallel_efficiency",
                                            "fields": [
                                                {
                                                    "name": "run_start",
                                                    "expression": "`run_start`",
                                                },
                                                {
                                                    "name": "tasks_per_minute",
                                                    "expression": "`tasks_per_minute`",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "line",
                                    "encodings": {
                                        "x": {
                                            "fieldName": "run_start",
                                            "scale": {"type": "temporal"},
                                            "displayName": "Run Start",
                                        },
                                        "y": {
                                            "fieldName": "tasks_per_minute",
                                            "scale": {"type": "quantitative"},
                                            "displayName": "Tasks per Minute",
                                        },
                                    },
                                    "frame": {
                                        "title": "Hyper-Scale Throughput",
                                        "showTitle": True,
                                    },
                                },
                            },
                            "position": {"x": 0, "y": 18, "width": 3, "height": 8},
                        },
                        {
                            "widget": {
                                "name": "runtime_trend_chart",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_runtime_trend",
                                            "fields": [
                                                {
                                                    "name": "run_date",
                                                    "expression": "`run_date`",
                                                },
                                                {
                                                    "name": "avg_runtime_seconds",
                                                    "expression": "`avg_runtime_seconds`",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "line",
                                    "encodings": {
                                        "x": {
                                            "fieldName": "run_date",
                                            "scale": {"type": "temporal"},
                                            "displayName": "Date",
                                        },
                                        "y": {
                                            "fieldName": "avg_runtime_seconds",
                                            "scale": {"type": "quantitative"},
                                            "displayName": "Avg Runtime (seconds)",
                                        },
                                    },
                                    "frame": {
                                        "title": "Runtime Trend (30 Days)",
                                        "showTitle": True,
                                    },
                                },
                            },
                            "position": {"x": 3, "y": 18, "width": 3, "height": 8},
                        },
                        {
                            "widget": {
                                "name": "peak_parallelism_chart",
                                "queries": [
                                    {
                                        "name": "main_query",
                                        "query": {
                                            "datasetName": "ds_parallel_efficiency",
                                            "fields": [
                                                {
                                                    "name": "run_id",
                                                    "expression": "`run_id`",
                                                },
                                                {
                                                    "name": "peak_parallelism",
                                                    "expression": "`peak_parallelism`",
                                                },
                                            ],
                                            "disaggregated": False,
                                        },
                                    }
                                ],
                                "spec": {
                                    "version": 3,
                                    "widgetType": "bar",
                                    "encodings": {
                                        "x": {
                                            "fieldName": "run_id",
                                            "scale": {"type": "categorical"},
                                            "displayName": "Run",
                                        },
                                        "y": {
                                            "fieldName": "peak_parallelism",
                                            "scale": {"type": "quantitative"},
                                            "displayName": "Count",
                                        },
                                        "color": {
                                            "fieldName": "peak_parallelism",
                                            "scale": {
                                                "type": "quantitative",
                                                "colorRamp": {
                                                    "mode": "scheme",
                                                    "scheme": "magma",
                                                },
                                            },
                                            "displayName": "peak_parallelism",
                                        },
                                    },
                                    "frame": {
                                        "title": "Peak Parallelism by Run",
                                        "showTitle": True,
                                    },
                                },
                            },
                            "position": {"x": 0, "y": 26, "width": 6, "height": 7},
                        },
                    ],
                    "pageType": "PAGE_TYPE_CANVAS",
                },
            ],
            "uiSettings": {
                "theme": {
                    "canvasBackgroundColor": {"light": "#1F1B2E", "dark": "#0C0B1D"},
                    "widgetBackgroundColor": {"light": "#2A2442", "dark": "#1B1833"},
                    "widgetBorderColor": {"light": "#5E5A80", "dark": "#3F3B60"},
                    "fontColor": {"light": "#FDFDFD", "dark": "#F5F5F5"},
                    "selectionColor": {"light": "#FF6EC7", "dark": "#FF6EC7"},
                    "visualizationColors": [
                        "#FF6EC7",
                        "#00FFF7",
                        "#BF00FF",
                        "#FF5F1F",
                        "#FFFF4D",
                        "#D9007F",
                        "#007FFF",
                    ],
                    "widgetHeaderAlignment": "LEFT",
                }
            },
        }

        draft: Dashboard = self.w.lakeview.create(
            Dashboard(
                display_name=display_name,
                parent_path=parent_path,
                warehouse_id=warehouse_id,
                serialized_dashboard=json.dumps(dashboard_payload),
            )
        )

        if not draft.dashboard_id:
            raise RuntimeError("Failed to create dashboard: dashboard_id is None")
        self.w.lakeview.publish(
            dashboard_id=draft.dashboard_id,
            embed_credentials=True,
            warehouse_id=warehouse_id,
        )
        logger.success(
            f"✅ Created dashboard: {self.w.config.host}/dashboardsv3/{draft.dashboard_id}/published"
        )
        return draft.dashboard_id

    def run_validation(
        self: "DataPactClient",
        config: DataPactConfig,
        job_name: str,
        warehouse_name: str,
        results_table: str | None = None,
    ) -> None:
        """
        Orchestrates the execution of a data validation workflow as a Databricks job.

        This method sets up the required SQL warehouse and results table, uploads SQL scripts,
        creates or updates a Databricks job with validation and aggregation tasks, and triggers
        the job run. It monitors the job execution, logs progress, and raises errors on failure
        or timeout.

        Args:
            config (dict[str, Any]): Configuration dictionary containing validation definitions and parameters.
            job_name (str): Name of the Databricks job to create or update.
            warehouse_name (str): Name of the SQL warehouse to use for executing tasks.
            results_table (str | None, optional): Fully qualified name of the results table. If None, a default table is used.

        Raises:
            ValueError: If required infrastructure (warehouse or job) cannot be set up or identified.
            TimeoutError: If the job run does not complete within the allotted time.
            RuntimeError: If the job run finishes with a failure state or if the run state cannot be determined.

        Side Effects:
            - Creates or updates Databricks jobs and dashboards.
            - Uploads SQL scripts to the workspace.
            - Logs job progress and results.
        """
        warehouse: sql_service.GetWarehouseResponse = self._ensure_sql_warehouse(warehouse_name)
        if results_table:
            try:
                catalog, schema, table = parse_fully_qualified_name(results_table)
            except ValueError as exc:
                raise ValueError(
                    "Results table must be provided as catalog.schema.table (three parts)."
                ) from exc
        else:
            catalog, schema, table = DEFAULT_CATALOG, DEFAULT_SCHEMA, DEFAULT_TABLE

        final_results_table = format_fully_qualified_name(catalog, schema, table)

        if not results_table:
            if warehouse.id is None:
                raise ValueError("SQL Warehouse ID is None. Cannot set up infrastructure.")
            self._setup_default_infrastructure(warehouse.id)
        if warehouse.id is None:
            raise ValueError("SQL Warehouse ID is None. Cannot ensure results table exists.")
        self._ensure_results_table_exists(final_results_table, warehouse.id)

        dashboard_id: str = self.ensure_dashboard_exists(
            job_name,
            final_results_table,
            warehouse.id,
        )

        asset_paths: dict[str, str] = self._upload_sql_scripts(
            config,
            final_results_table,
            job_name,
        )
        validation_task_keys: list[str] = [v.task_key for v in config.validations]
        sql_params: dict[str, str] = {
            "run_id": "{{job.run_id}}",
            "job_id": "{{job.id}}",
            "job_start_ts": "{{job.start_time.iso_datetime}}",
        }

        tasks: list[Task] = build_tasks(
            asset_paths=asset_paths,
            warehouse_id=warehouse.id,
            validation_task_keys=validation_task_keys,
            sql_params=sql_params,
        )
        add_dashboard_refresh_task(tasks, dashboard_id=dashboard_id, warehouse_id=warehouse.id)
        add_genie_room_task(
            tasks=tasks,
            asset_paths=asset_paths,
            warehouse_id=warehouse.id,
        )

        job_id: int = ensure_job(self.w, job_name=job_name, tasks=tasks, user_name=self.user_name)
        # Inject time providers so tests can patch datapact.client.datetime and time.sleep
        run_and_wait(
            self.w,
            job_id=job_id,
            tasks=tasks,
            timeout_hours=1,
            now_fn=datetime.now,
            sleep_fn=time.sleep,
        )

    def _ensure_sql_warehouse(
        self: "DataPactClient",
        name: str,
    ) -> sql_service.GetWarehouseResponse:
        """
        Finds a SQL warehouse by its name, ensures it is running, and returns its details.

        This method searches for a SQL warehouse with the specified name. If the warehouse is found but not running,
        it attempts to start it and waits for it to become available. If the warehouse cannot be found or started,
        an exception is raised.

        Args:
            name (str): The name of the SQL warehouse to locate and ensure is running.

        Returns:
            sql_service.GetWarehouseResponse: Details for the located (and running) SQL warehouse.

        Raises:
            ValueError: If the warehouse with the given name is not found, has no ID, or cannot be started.
        """
        logger.info(f"Looking for SQL Warehouse '{name}'...")
        warehouse: sql_service.EndpointInfo | None = next(
            (wh for wh in self.w.warehouses.list() if wh.name == name), None
        )

        if not warehouse:
            raise ValueError(f"SQL Warehouse '{name}' not found.")

        logger.info(f"Found warehouse '{name}' (ID: {warehouse.id}). State: {warehouse.state}")

        if warehouse.state not in [
            sql_service.State.RUNNING,
            sql_service.State.STARTING,
        ]:
            logger.info(f"Warehouse '{name}' is {warehouse.state}. Attempting to start...")
            if warehouse.id is None:
                raise ValueError(f"Warehouse '{name}' has no ID and cannot be started.")
            # Trigger start and poll the warehouse state until RUNNING or timeout
            self.w.warehouses.start(warehouse.id)
            logger.info(f"Waiting for warehouse '{name}' to reach RUNNING state...")
            deadline = datetime.now() + timedelta(minutes=10)
            while datetime.now() < deadline:
                try:
                    wh = self.w.warehouses.get(warehouse.id)
                except Exception:
                    wh = None
                state = getattr(wh, "state", None)
                if state == sql_service.State.RUNNING:
                    logger.success(f"Warehouse '{name}' started successfully.")
                    break
                if state in (
                    sql_service.State.DELETING,
                    sql_service.State.DELETED,
                ):
                    raise RuntimeError(f"Warehouse '{name}' failed to start. State: {state}")
                # Allow STOPPED/STOPPING/STARTING to continue polling
                time.sleep(5)
            else:
                raise TimeoutError(f"Timed out waiting for warehouse '{name}' to start.")

        if warehouse.id is None:
            raise ValueError(f"Warehouse '{name}' has no ID and cannot be retrieved.")
        return self.w.warehouses.get(warehouse.id)
