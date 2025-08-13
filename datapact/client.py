"""
The core client for interacting with the Databricks API.

This module contains the DataPactClient class. It orchestrates the entire
validation process by dynamically generating pure SQL validation scripts based
on a user's configuration. It then creates and runs a multi-task Databricks Job
where each task executes one of the generated SQL scripts on a specified
Serverless SQL Warehouse. Finally, it can create a results dashboard.
"""

import time
import json
import textwrap
from datetime import timedelta, datetime
from typing import Any, Final

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service, workspace
from databricks.sdk.service.jobs import (
    RunLifeCycleState,
    Task,
)
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.errors import NotFound
from loguru import logger
from jinja2 import Environment, PackageLoader
from .config import DataPactConfig, ValidationTask
from .sql_generator import render_validation_sql, render_aggregate_sql
from .sql_utils import escape_sql_string, validate_job_name
from .job_orchestrator import (
    build_tasks,
    add_dashboard_refresh_task,
    add_genie_room_task,
    ensure_job,
    run_and_wait,
)

TERMINAL_STATES: list[RunLifeCycleState] = [
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
]
DEFAULT_CATALOG: Final[str] = "datapact"
DEFAULT_SCHEMA: Final[str] = "results"
DEFAULT_TABLE: Final[str] = "run_history"


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
        logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
        # Configure for enterprise-scale operations with extended timeout
        try:
            from databricks.sdk.config import Config

            config = Config(
                profile=profile, http_timeout_seconds=1200
            )  # 20 minute timeout
            self.w: WorkspaceClient = WorkspaceClient(config=config)
        except ValueError:
            # Fallback for tests or environments without profile configured
            self.w: WorkspaceClient = WorkspaceClient(profile=profile)
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

    # Resource management helpers
    def close(self: "DataPactClient") -> None:
        """Release cached resources held by this client.

        Currently clears the cached Jinja2 Environment to avoid keeping template
        loader references alive for long-lived client instances.
        """
        self._env = None

    def __enter__(self: "DataPactClient") -> "DataPactClient":
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        # Do not suppress exceptions; returning False explicitly communicates this.
        return False

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
        self._execute_sql(
            f"CREATE CATALOG IF NOT EXISTS `{DEFAULT_CATALOG}`", warehouse_id
        )
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
            timestamp TIMESTAMP, result_payload VARIANT) USING DELTA"""
        )
        self._execute_sql(ddl, warehouse_id)
        logger.success(f"Results table '{results_table_fqn}' is ready.")

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

        This creates materialized views of the validation results optimized for
        natural language querying through Databricks AI/BI Genie.
        """
        # Safely escape SQL values to prevent injection
        safe_job_name = escape_sql_string(validate_job_name(job_name))

        return f"""-- DataPact Genie Room Data Preparation
-- Creates curated datasets for natural language analysis in Databricks AI/BI Genie
-- After this runs, create a Genie space in the UI using these tables

-- 1. Current validation status summary
CREATE OR REPLACE TABLE datapact_genie_current_status AS
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
    timestamp as last_validated,
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
FROM {results_table}
WHERE job_name = {safe_job_name}
AND run_id = (SELECT MAX(run_id) FROM {results_table} WHERE job_name = {safe_job_name});

-- 2. Data quality metrics by table
CREATE OR REPLACE TABLE datapact_genie_table_quality AS
SELECT
    CONCAT(get_json_object(to_json(result_payload), '$.source_schema'), '.',
           get_json_object(to_json(result_payload), '$.source_table')) as table_name,
    COUNT(*) as total_validations,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as passed_validations,
    SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failed_validations,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_score,
    MAX(timestamp) as last_checked
FROM {results_table}
WHERE job_name = {safe_job_name}
AND run_id = (SELECT MAX(run_id) FROM {results_table} WHERE job_name = {safe_job_name})
GROUP BY 1;

-- 3. Issue details for failed validations
CREATE OR REPLACE TABLE datapact_genie_issues AS
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
    timestamp as detected_at,
    'High' as severity
FROM {results_table}
WHERE job_name = {safe_job_name}
AND status = 'FAILURE'
AND run_id = (SELECT MAX(run_id) FROM {results_table} WHERE job_name = {safe_job_name});

-- Display instructions for setting up Genie space
SELECT '🚀 GENIE ROOM SETUP INSTRUCTIONS' as title,
'Your data quality datasets have been created! To enable natural language analysis:

1. Go to Databricks AI/BI → Create → Genie Space
2. Add these tables as data sources:
   • datapact_genie_current_status - Current validation results
   • datapact_genie_table_quality - Quality scores by table
   • datapact_genie_issues - Detailed issue tracking
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
                "failure_rate_over_time": f"SELECT to_date(timestamp) as run_date, COUNT(CASE WHEN status = 'FAILURE' THEN 1 END) * 100.0 / COUNT(1) as failure_rate_percent FROM {results_table_fqn} WHERE job_name = '{job_name}' GROUP BY 1 ORDER BY 1",
                "top_failing_tasks": f"SELECT task_key, COUNT(1) as failure_count FROM {results_table_fqn} WHERE status = 'FAILURE' AND job_name = '{job_name}' GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
                "raw_history": f"SELECT task_key, status, run_id, job_id, job_name, timestamp, result_payload FROM {results_table_fqn} WHERE job_name = '{job_name}' ORDER BY timestamp DESC, task_key"
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
        display_name: str = (
            f"DataPact_Results_{job_name.replace(' ', '_').replace(':', '')}"
        )
        parent_path: str = f"{self.root_path}/dashboards"
        draft_path: str = f"{parent_path}/{display_name}.lvdash.json"
        self.w.workspace.mkdirs(parent_path)

        try:
            self.w.workspace.get_status(draft_path)
            logger.warning(
                f"Found existing dashboard file at {draft_path}. Deleting to recreate with the correct format."
            )
            self.w.workspace.delete(path=draft_path, recursive=True)
            time.sleep(2)
        except (NotFound, Exception):
            # NotFound or any other exception means the file doesn't exist
            logger.info("Dashboard file does not yet exist – will create")

        def q(sql):
            # Safely escape job name to prevent SQL injection
            safe_job_name = escape_sql_string(validate_job_name(job_name))
            return sql.format(table=results_table_fqn, job=safe_job_name)

        # Define all datasets needed for the dashboard
        datasets: list[dict[str, Any]] = [
            {
                "name": "ds_kpi",
                "displayName": "Executive KPI Dashboard",
                "queryLines": [
                    q(
                        "WITH latest_run AS (SELECT task_key, status, "
                        "get_json_object(to_json(result_payload), '$.source_catalog') as source_catalog, "
                        "get_json_object(to_json(result_payload), '$.source_schema') as source_schema, "
                        "get_json_object(to_json(result_payload), '$.source_table') as source_table, "
                        "result_payload "
                        "FROM {table} WHERE run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name={job})) "
                        "SELECT COUNT(*) as total_tasks, "
                        "COUNT(IF(status = 'FAILURE', 1, NULL)) as failed_tasks, "
                        "ROUND(COUNT(IF(status = 'SUCCESS', 1, NULL)) * 100.0 / COUNT(*), 2) as success_rate_percent, "
                        "ROUND(COUNT(IF(status = 'SUCCESS', 1, NULL)) * 1.0 / COUNT(*), 4) as data_quality_score, "
                        "COUNT(*) as tables_validated "
                        "FROM latest_run"
                    )
                ],
            },
            {
                "name": "ds_summary",
                "displayName": "Validation Status Overview",
                "queryLines": [
                    q(
                        "SELECT CASE status "
                        "WHEN 'SUCCESS' THEN 'Passed' "
                        "WHEN 'FAILURE' THEN 'Failed' "
                        "ELSE status END as status, "
                        "COUNT(*) as task_count "
                        "FROM {table} WHERE run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name={job}) "
                        "GROUP BY status"
                    )
                ],
            },
            {
                "name": "ds_failure_rate",
                "displayName": "Data Quality Trend Analysis",
                "queryLines": [
                    q(
                        "SELECT date(timestamp) as run_date, "
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
                        "  FROM {table}\n"
                        "  WHERE job_name={job} AND run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name={job})\n"
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
                        ") t\n"
                        "GROUP BY validation_type\n"
                        "ORDER BY failure_count DESC"
                    )
                ],
            },
            {
                "name": "ds_history",
                "displayName": "Detailed Run History",
                "queryLines": [
                    q(
                        "SELECT task_key, status, timestamp, to_json(result_payload) as payload_json, run_id, job_name FROM {table} WHERE job_name={job} ORDER BY timestamp DESC, task_key"
                    )
                ],
            },
            {
                "name": "ds_latest_run_details",
                "displayName": "All Run Details",
                "queryLines": [
                    q(
                        "SELECT task_key, "
                        "CASE status WHEN 'SUCCESS' THEN '✅ PASSED' WHEN 'FAILURE' THEN '❌ FAILED' ELSE status END as status, "
                        "CONCAT(get_json_object(to_json(result_payload), '$.source_catalog'), '.', "
                        "       get_json_object(to_json(result_payload), '$.source_schema'), '.', "
                        "       get_json_object(to_json(result_payload), '$.source_table')) as source_table, "
                        "CONCAT(get_json_object(to_json(result_payload), '$.target_catalog'), '.', "
                        "       get_json_object(to_json(result_payload), '$.target_schema'), '.', "
                        "       get_json_object(to_json(result_payload), '$.target_table')) as target_table, "
                        "DATE_FORMAT(timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp, "
                        "to_json(result_payload) as result_payload, "
                        "run_id, job_name "
                        "FROM {table} WHERE run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name={job}) "
                        "ORDER BY CASE status WHEN 'FAILURE' THEN 0 ELSE 1 END, task_key"
                    )
                ],
            },
            {
                "name": "ds_success_trend",
                "displayName": "Success Rate Over Time",
                "queryLines": [
                    q(
                        "SELECT date(timestamp) as run_date, COUNT(IF(status='SUCCESS',1,NULL))*100/COUNT(*) as success_rate FROM {table} WHERE job_name={job} GROUP BY 1 ORDER BY 1"
                    )
                ],
            },
            {
                "name": "ds_business_impact",
                "displayName": "Business Impact Assessment",
                "queryLines": [
                    q(
                        "WITH impact_analysis AS ("
                        "SELECT COALESCE(get_json_object(to_json(result_payload), '$.source_schema'), 'Unknown Schema') as source_schema, "
                        "COUNT(*) as total_validations, "
                        "SUM(CASE WHEN status = 'FAILURE' THEN 1 ELSE 0 END) as failures, "
                        "ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_score, "
                        "MAX(CASE WHEN status = 'FAILURE' THEN timestamp END) as last_failure "
                        "FROM {table} "
                        "WHERE job_name = {job} "
                        "AND run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name = {job}) "
                        "GROUP BY COALESCE(get_json_object(to_json(result_payload), '$.source_schema'), 'Unknown Schema')) "
                        "SELECT source_schema as schema_name, "
                        "total_validations, failures, quality_score, "
                        "CASE WHEN failures = 0 THEN '🟢 Excellent' "
                        "WHEN quality_score >= 95 THEN '🟡 Good' "
                        "WHEN quality_score >= 90 THEN '🟠 Fair' "
                        "ELSE '🔴 Needs Attention' END as health_status, "
                        "COALESCE(DATE_FORMAT(last_failure, 'yyyy-MM-dd HH:mm'), 'No failures') as last_issue "
                        "FROM impact_analysis "
                        "ORDER BY failures DESC, quality_score ASC"
                    )
                ],
            },
            {
                "name": "ds_exploded_checks",
                "displayName": "Exploded View of All Validation Checks",
                "queryLines": [
                    q(
                        "WITH base_data AS ( "
                        "  SELECT task_key, status, result_payload, run_id, job_name "
                        "  FROM {table} "
                        "  WHERE job_name = {job} "
                        "  AND run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name = {job}) "
                        "), "
                        "expanded_checks AS ( "
                        "  SELECT task_key, 'Count Validation' as check_type, "
                        "    get_json_object(to_json(result_payload), '$.count_validation.status') as check_status, "
                        "    CONCAT('Source: ', get_json_object(to_json(result_payload), '$.count_validation.source_count'), "
                        "           ', Target: ', get_json_object(to_json(result_payload), '$.count_validation.target_count'), "
                        "           ', Diff: ', get_json_object(to_json(result_payload), '$.count_validation.relative_diff_percent')) as details "
                        "  FROM base_data "
                        "  WHERE get_json_object(to_json(result_payload), '$.count_validation') IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT task_key, 'Row Hash Validation' as check_type, "
                        "    get_json_object(to_json(result_payload), '$.row_hash_validation.status') as check_status, "
                        "    CONCAT('Compared: ', get_json_object(to_json(result_payload), '$.row_hash_validation.compared_rows'), "
                        "           ', Mismatches: ', get_json_object(to_json(result_payload), '$.row_hash_validation.mismatch_count'), "
                        "           ' (', get_json_object(to_json(result_payload), '$.row_hash_validation.mismatch_percent'), ')') as details "
                        "  FROM base_data "
                        "  WHERE get_json_object(to_json(result_payload), '$.row_hash_validation') IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT task_key, CONCAT('Null Check: ', col_name) as check_type, "
                        "    get_json_object(to_json(result_payload), CONCAT('$.null_validation_', col_name, '.status')) as check_status, "
                        "    CONCAT('Source nulls: ', get_json_object(to_json(result_payload), CONCAT('$.null_validation_', col_name, '.source_nulls')), "
                        "           ', Target nulls: ', get_json_object(to_json(result_payload), CONCAT('$.null_validation_', col_name, '.target_nulls'))) as details "
                        "  FROM base_data "
                        "  LATERAL VIEW explode(array('v1', 'v2', 'email', 'phone', 'price', 'stock_quantity')) t AS col_name "
                        "  WHERE get_json_object(to_json(result_payload), CONCAT('$.null_validation_', col_name)) IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT task_key, CONCAT('Uniqueness Check: ', col_name) as check_type, "
                        "    get_json_object(to_json(result_payload), CONCAT('$.uniqueness_validation_', col_name, '.status')) as check_status, "
                        "    CONCAT('Duplicates found: ', get_json_object(to_json(result_payload), CONCAT('$.uniqueness_validation_', col_name, '.duplicate_count'))) as details "
                        "  FROM base_data "
                        "  LATERAL VIEW explode(array('email', 'id', 'customer_id', 'order_id')) t AS col_name "
                        "  WHERE get_json_object(to_json(result_payload), CONCAT('$.uniqueness_validation_', col_name)) IS NOT NULL "
                        "  UNION ALL "
                        "  SELECT task_key, CONCAT('Aggregation Check: ', col_agg) as check_type, "
                        "    get_json_object(to_json(result_payload), CONCAT('$.agg_validation_', col_agg, '.status')) as check_status, "
                        "    CONCAT('Source: ', get_json_object(to_json(result_payload), CONCAT('$.agg_validation_', col_agg, '.source_value')), "
                        "           ', Target: ', get_json_object(to_json(result_payload), CONCAT('$.agg_validation_', col_agg, '.target_value')), "
                        "           ', Diff: ', get_json_object(to_json(result_payload), CONCAT('$.agg_validation_', col_agg, '.relative_diff_percent'))) as details "
                        "  FROM base_data "
                        "  LATERAL VIEW explode(array('v1_SUM', 'v1_AVG', 'v2_SUM', 'total_spent_SUM', 'revenue_SUM', 'amount_SUM')) t AS col_agg "
                        "  WHERE length(col_agg) > 0 AND get_json_object(to_json(result_payload), CONCAT('$.agg_validation_', col_agg)) IS NOT NULL "
                        ") "
                        "SELECT task_key as validation_name, "
                        "  check_type, "
                        "  CASE check_status "
                        "    WHEN 'PASS' THEN '✅ PASS' "
                        "    WHEN 'FAIL' THEN '❌ FAIL' "
                        "    ELSE '⚠️ ' || COALESCE(check_status, 'UNKNOWN') "
                        "  END as status, "
                        "  details "
                        "FROM expanded_checks "
                        "ORDER BY task_key, check_type"
                    )
                ],
            },
            {
                "name": "ds_validation_details",
                "displayName": "Detailed Validation Results with Individual Check Status",
                "queryLines": [
                    q(
                        "SELECT task_key as validation_name, "
                        "CASE status "
                        "  WHEN 'SUCCESS' THEN '✅' "
                        "  WHEN 'FAILURE' THEN '❌' "
                        "  ELSE '❓' END as overall_status, "
                        "CASE WHEN get_json_object(to_json(result_payload), '$.count_validation.status') = 'PASS' THEN '✅ Count' "
                        "     WHEN get_json_object(to_json(result_payload), '$.count_validation.status') = 'FAIL' THEN '❌ Count' "
                        "     ELSE '➖' END as count_check, "
                        "CASE WHEN get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'PASS' THEN '✅ Hash' "
                        "     WHEN get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'FAIL' THEN '❌ Hash' "
                        "     ELSE '➖' END as hash_check, "
                        "CASE WHEN to_json(result_payload) LIKE '%null_validation%' AND to_json(result_payload) NOT LIKE '%null_validation%FAIL%' THEN '✅ Nulls' "
                        "     WHEN to_json(result_payload) LIKE '%null_validation%FAIL%' THEN '❌ Nulls' "
                        "     ELSE '➖' END as null_check, "
                        "CASE WHEN to_json(result_payload) LIKE '%uniqueness_validation%' AND to_json(result_payload) NOT LIKE '%uniqueness_validation%FAIL%' THEN '✅ Unique' "
                        "     WHEN to_json(result_payload) LIKE '%uniqueness_validation%FAIL%' THEN '❌ Unique' "
                        "     ELSE '➖' END as unique_check, "
                        "CASE WHEN to_json(result_payload) LIKE '%agg_validation%' AND to_json(result_payload) NOT LIKE '%agg_validation%FAIL%' THEN '✅ Aggs' "
                        "     WHEN to_json(result_payload) LIKE '%agg_validation%FAIL%' THEN '❌ Aggs' "
                        "     ELSE '➖' END as agg_check, "
                        "CASE WHEN get_json_object(to_json(result_payload), '$.source_catalog') IS NOT NULL "
                        "     THEN CONCAT(get_json_object(to_json(result_payload), '$.source_catalog'), '.', "
                        "                 get_json_object(to_json(result_payload), '$.source_schema'), '.', "
                        "                 get_json_object(to_json(result_payload), '$.source_table')) "
                        "     ELSE 'N/A' END as source_table, "
                        "CASE WHEN get_json_object(to_json(result_payload), '$.target_catalog') IS NOT NULL "
                        "     THEN CONCAT(get_json_object(to_json(result_payload), '$.target_catalog'), '.', "
                        "                 get_json_object(to_json(result_payload), '$.target_schema'), '.', "
                        "                 get_json_object(to_json(result_payload), '$.target_table')) "
                        "     ELSE 'N/A' END as target_table "
                        "FROM {table} WHERE job_name = {job} "
                        "AND run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name = {job}) "
                        "ORDER BY status DESC, task_key"
                    )
                ],
            },
        ]

        widget_definitions: list[dict[str, Any]] = [
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
                "pos": {"x": 2, "y": 0, "width": 2, "height": 3},
                "value_col": "failed_tasks",
            },
            {
                "ds_name": "ds_kpi",
                "type": "COUNTER",
                "title": "Total Validations",
                "pos": {"x": 4, "y": 0, "width": 2, "height": 3},
                "value_col": "tables_validated",
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
            },
            {
                "ds_name": "ds_failures_by_type",
                "type": "BAR",
                "title": "Issue Classification",
                "pos": {"x": 0, "y": 9, "width": 6, "height": 5},
                "x_field": "validation_type",
                "y_field": "failure_count",
                "y_agg": "SUM",
            },
            {
                "ds_name": "ds_validation_details",
                "type": "TABLE",
                "title": "Validation Results with Check Details",
                "pos": {"x": 0, "y": 14, "width": 6, "height": 8},
            },
            {
                "ds_name": "ds_business_impact",
                "type": "TABLE",
                "title": "Source Schema Quality Summary",
                "pos": {"x": 0, "y": 22, "width": 6, "height": 5},
            },
            {
                "ds_name": "ds_exploded_checks",
                "type": "TABLE",
                "title": "Detailed Check-by-Check Results (Filterable)",
                "pos": {"x": 0, "y": 27, "width": 6, "height": 8},
            },
            {
                "ds_name": "ds_top_failures",
                "type": "BAR",
                "title": "Top Failing Validations",
                "pos": {"x": 0, "y": 27, "width": 6, "height": 5},
                "x_field": "task_key",
                "y_field": "failure_count",
                "y_agg": "SUM",
            },
        ]

        layout_widgets: list[dict[str, Any]] = []
        for i, w_def in enumerate(widget_definitions):
            spec, query_fields = {}, []

            if w_def["type"] == "COUNTER":
                query_fields = [
                    {
                        "name": w_def["value_col"],
                        "expression": f"`{w_def['value_col']}`",
                    }
                ]
                encodings = {"value": {"fieldName": w_def["value_col"]}}

                # Add color coding for critical issues
                if w_def.get("title") == "Critical Issues":
                    encodings["value"]["displayAs"] = "color"
                    encodings["value"]["colorRange"] = {
                        "start": {"value": 0, "color": "#00A972"},  # Green for 0
                        "end": {"value": 1, "color": "#FF3621"},  # Red for > 0
                    }

                spec = {
                    "version": 3,
                    "widgetType": "counter",
                    "encodings": encodings,
                }
            elif w_def["type"] == "SUCCESS_RATE_COUNTER":
                query_fields = [
                    {
                        "name": w_def["value_col"],
                        "expression": f"`{w_def['value_col']}`",
                    }
                ]
                spec = {
                    "version": 2,
                    "widgetType": "counter",
                    "encodings": {
                        "value": {
                            "fieldName": w_def["value_col"],
                            "format": {
                                "type": "number-percent",
                                "decimalPlaces": {"type": "max", "places": 2},
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

            elif w_def["type"] == "BAR":
                x_field = w_def.get("x_field", "task_key")
                y_field = w_def.get("y_field", "failure_count")
                y_agg = w_def.get("y_agg", "SUM").upper()
                y_alias = f"{y_agg.lower()}({y_field})"
                query_fields = [
                    {"name": x_field, "expression": f"`{x_field}`"},
                    {"name": y_alias, "expression": f"{y_agg}(`{y_field}`)"},
                ]
                spec = {
                    "version": 3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": x_field,
                            "scale": {"type": "categorical"},
                            "displayName": w_def.get("x_display", "Category"),
                        },
                        "y": {
                            "fieldName": y_alias,
                            "scale": {"type": "quantitative"},
                            "displayName": w_def.get("y_display", "Count"),
                        },
                    },
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
                        "schema_name",
                        "total_validations",
                        "failures",
                        "quality_score",
                        "health_status",
                        "last_issue",
                    ]
                    display_names = [
                        "Schema",
                        "Total Validations",
                        "Failures",
                        "Quality Score (%)",
                        "Health Status",
                        "Last Issue",
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
                        "Source",
                        "Target",
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
                        "timestamp",
                        "result_payload",
                    ]
                    display_names = [
                        "Task Key",
                        "Status",
                        "Source Table",
                        "Target Table",
                        "Timestamp",
                        "Result Payload",
                    ]
                else:
                    # Default columns for other table widgets
                    columns = [
                        "task_key",
                        "status",
                        "timestamp",
                        "payload_json",
                        "run_id",
                        "job_name",
                    ]
                    display_names = [
                        "Task Key",
                        "Status",
                        "Timestamp",
                        "Result Payload",
                        "Run ID",
                        "Job Name",
                    ]

                query_fields = [{"name": c, "expression": f"`{c}`"} for c in columns]
                spec = {
                    "version": 3,
                    "widgetType": "table",
                    "encodings": {
                        "columns": [
                            {"fieldName": col, "displayName": display}
                            for col, display in zip(columns, display_names)
                        ]
                    },
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

        dashboard_payload: dict[str, Any] = {
            "datasets": datasets,
            "pages": [
                {
                    "name": "main_page",
                    "displayName": "Executive Data Quality Dashboard",
                    "layout": layout_widgets,
                    "filters": [
                        {
                            "name": "job_name",
                            "dataset": "ds_history",
                            "field": "job_name",
                        },
                        {"name": "run_id", "dataset": "ds_history", "field": "run_id"},
                        {
                            "name": "status_filter",
                            "dataset": "ds_validation_details",
                            "field": "overall_status",
                            "displayName": "Filter by Status",
                            "allowMultipleValues": True,
                        },
                    ],
                    "pageType": "PAGE_TYPE_CANVAS",
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
                                                    "name": "timestamp",
                                                    "expression": "`timestamp`",
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
                                                "fieldName": "timestamp",
                                                "displayName": "Timestamp",
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
                                            "displayName": "Success Rate (%)",
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
                    ],
                    "filters": [
                        {
                            "name": "status_filter_details",
                            "dataset": "ds_history",
                            "field": "status",
                            "displayName": "Filter by Status",
                            "allowMultipleValues": True,
                        },
                        {
                            "name": "task_key_filter",
                            "dataset": "ds_history",
                            "field": "task_key",
                            "displayName": "Filter by Task",
                            "allowMultipleValues": True,
                        },
                        {
                            "name": "job_name",
                            "dataset": "ds_latest_run_details",
                            "field": "job_name",
                        },
                        {
                            "name": "run_id",
                            "dataset": "ds_latest_run_details",
                            "field": "run_id",
                        },
                    ],
                    "pageType": "PAGE_TYPE_CANVAS",
                },
            ],
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
        warehouse: sql_service.GetWarehouseResponse = self._ensure_sql_warehouse(
            warehouse_name
        )
        final_results_table = (
            f"`{results_table}`"
            if results_table
            else f"`{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.`{DEFAULT_TABLE}`"
        )
        if not results_table:
            if warehouse.id is None:
                raise ValueError(
                    "SQL Warehouse ID is None. Cannot set up infrastructure."
                )
            self._setup_default_infrastructure(warehouse.id)
        if warehouse.id is None:
            raise ValueError(
                "SQL Warehouse ID is None. Cannot ensure results table exists."
            )
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
        }

        tasks: list[Task] = build_tasks(
            asset_paths=asset_paths,
            warehouse_id=warehouse.id,
            validation_task_keys=validation_task_keys,
            sql_params=sql_params,
        )
        add_dashboard_refresh_task(
            tasks, dashboard_id=dashboard_id, warehouse_id=warehouse.id
        )
        add_genie_room_task(
            tasks=tasks,
            asset_paths=asset_paths,
            warehouse_id=warehouse.id,
        )

        job_id: int = ensure_job(
            self.w, job_name=job_name, tasks=tasks, user_name=self.user_name
        )
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

        logger.info(
            f"Found warehouse '{name}' (ID: {warehouse.id}). State: {warehouse.state}"
        )

        if warehouse.state not in [
            sql_service.State.RUNNING,
            sql_service.State.STARTING,
        ]:
            logger.info(
                f"Warehouse '{name}' is {warehouse.state}. Attempting to start..."
            )
            if warehouse.id is None:
                raise ValueError(f"Warehouse '{name}' has no ID and cannot be started.")
            self.w.warehouses.start(warehouse.id).result(timeout=timedelta(minutes=10))
            logger.success(f"Warehouse '{name}' started successfully.")

        if warehouse.id is None:
            raise ValueError(f"Warehouse '{name}' has no ID and cannot be retrieved.")
        return self.w.warehouses.get(warehouse.id)
