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
from databricks.sdk.service import jobs, sql as sql_service, workspace
from databricks.sdk.service.jobs import (
    RunLifeCycleState,
    Source,
    JobRunAs,
    JobSettings,
    Task,
    SqlTask,
    SqlTaskFile,
    TaskDependency,
    RunIf,
    DashboardTask,
)
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.errors import NotFound
from loguru import logger
from .config import DataPactConfig, ValidationTask

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
        self.w: WorkspaceClient = WorkspaceClient(profile=profile)
        self.user_name: str | None = self.w.current_user.me().user_name
        self.root_path: str = f"/Users/{self.user_name}/datapact"
        self.w.workspace.mkdirs(self.root_path)

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
        """
        Generates a complete, multi-statement SQL script for one validation task.

        Args:
            config: The config dictionary for a single validation task.
            results_table: The FQN of the results table to insert into.
            job_name: The name of the parent job, for logging.

        Returns:
            A string containing the complete, executable SQL for one validation task.
        """
        task_key: str = config.task_key
        source_fqn = f"`{config.source_catalog}`.`{config.source_schema}`.`{config.source_table}`"
        target_fqn = f"`{config.target_catalog}`.`{config.target_schema}`.`{config.target_table}`"
        ctes, payload_structs, overall_clauses = [], [], []

        if config.count_tolerance is not None:
            tolerance: float = config.count_tolerance
            ctes.append(
                f"count_metrics AS (SELECT (SELECT COUNT(1) FROM {source_fqn}) AS source_count, (SELECT COUNT(1) FROM {target_fqn}) AS target_count)"
            )
            check = f"COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= {tolerance}"
            payload_structs.append(
                textwrap.dedent(
                    f"""
            struct(
                FORMAT_NUMBER(source_count, '#,##0') AS source_count,
                FORMAT_NUMBER(target_count, '#,##0') AS target_count,
                FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
                FORMAT_STRING('%.2f%%', CAST({tolerance} * 100 AS DOUBLE)) AS tolerance_percent,
                CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status
            ) AS count_validation"""
                )
            )
            overall_clauses.append(check)

        if config.pk_row_hash_check and config.primary_keys:
            hash_pks: list[str] = config.primary_keys
            hash_threshold: float = config.pk_hash_threshold or 0.0
            hash_cols: list[str] | None = config.hash_columns
            hash_expr = f"md5(to_json(struct({', '.join([f'`{c}`' for c in hash_cols]) if hash_cols else '*'})))"
            pk_cols_str = ", ".join([f"`{pk}`" for pk in hash_pks])
            join_expr = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in hash_pks])
            ctes.append(
                f"row_hash_metrics AS (SELECT COUNT(1) AS total_compared_rows, COALESCE(SUM(CASE WHEN s.row_hash <> t.row_hash THEN 1 ELSE 0 END), 0) AS mismatch_count FROM (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {source_fqn}) s INNER JOIN (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {target_fqn}) t ON {join_expr})"
            )
            check = f"COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) <= {hash_threshold}"
            payload_structs.append(
                textwrap.dedent(
                    f"""
            struct(
                FORMAT_NUMBER(total_compared_rows, '#,##0') AS compared_rows,
                FORMAT_NUMBER(mismatch_count, '#,##0') AS mismatch_count,
                FORMAT_STRING('%.2f%%', CAST(COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) * 100 AS DOUBLE)) as mismatch_percent,
                FORMAT_STRING('%.2f%%', CAST({hash_threshold} * 100 AS DOUBLE)) AS threshold_percent,
                CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status
            ) AS row_hash_validation"""
                )
            )
            overall_clauses.append(check)

        if config.null_validation_columns and config.null_validation_threshold:
            null_threshold: float = config.null_validation_threshold
            null_pks: list[str] | None = config.primary_keys
            for col in config.null_validation_columns:
                cte_key: str = f"null_metrics_{col}"
                src_null_alias: str = f"source_nulls_{col}"
                tgt_null_alias: str = f"target_nulls_{col}"
                total_alias: str = f"total_compared_{col}"
                diff_calculation: str = ""

                if null_pks:
                    join_expr = " AND ".join(
                        [f"s.`{pk}` = t.`{pk}`" for pk in null_pks]
                    )
                    ctes.append(
                        f"""{cte_key} AS (SELECT
                        SUM(CASE WHEN s.`{col}` IS NULL THEN 1 ELSE 0 END) as {src_null_alias},
                        SUM(CASE WHEN t.`{col}` IS NULL THEN 1 ELSE 0 END) as {tgt_null_alias},
                        COUNT(1) as {total_alias}
                        FROM {source_fqn} s JOIN {target_fqn} t ON {join_expr})"""
                    )
                    check = f"COALESCE(ABS({src_null_alias} - {tgt_null_alias}) / NULLIF(CAST({total_alias} AS DOUBLE), 0), 0) <= {null_threshold}"
                    diff_calculation = f"COALESCE(ABS({src_null_alias} - {tgt_null_alias}) / NULLIF(CAST({total_alias} AS DOUBLE), 0), 0)"
                else:
                    ctes.append(
                        f"""{cte_key} AS (SELECT
                        (SELECT COUNT(1) FROM {source_fqn} WHERE `{col}` IS NULL) as {src_null_alias},
                        (SELECT COUNT(1) FROM {target_fqn} WHERE `{col}` IS NULL) as {tgt_null_alias})"""
                    )
                    check = f"CASE WHEN {src_null_alias} = 0 THEN {tgt_null_alias} = 0 ELSE COALESCE(ABS({tgt_null_alias} - {src_null_alias}) / NULLIF(CAST({src_null_alias} AS DOUBLE), 0), 0) <= {null_threshold} END"
                    diff_calculation = f"COALESCE(ABS({tgt_null_alias} - {src_null_alias}) / NULLIF(CAST({src_null_alias} AS DOUBLE), 0), 0)"

                payload_structs.append(
                    textwrap.dedent(
                        f"""
                struct(
                    FORMAT_NUMBER({src_null_alias}, '#,##0') AS source_nulls,
                    FORMAT_NUMBER({tgt_null_alias}, '#,##0') AS target_nulls,
                    FORMAT_STRING('%.2f%%', CAST({diff_calculation} * 100 AS DOUBLE)) as relative_diff_percent,
                    FORMAT_STRING('%.2f%%', CAST({null_threshold} * 100 AS DOUBLE)) AS threshold_percent,
                    CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status
                ) AS null_validation_{col}"""
                    )
                )
                overall_clauses.append(check)

        if config.agg_validations:
            for agg_config in config.agg_validations:
                col, validations = agg_config.column, agg_config.validations
                for val in validations:
                    agg, tolerance = val.agg.upper(), val.tolerance
                    src_val_alias, tgt_val_alias = (
                        f"source_value_{col}_{agg}",
                        f"target_value_{col}_{agg}",
                    )
                    ctes.append(
                        f"agg_metrics_{col}_{agg} AS (SELECT TRY_CAST((SELECT {agg}(`{col}`) FROM {source_fqn}) AS DECIMAL(38, 6)) AS {src_val_alias}, TRY_CAST((SELECT {agg}(`{col}`) FROM {target_fqn}) AS DECIMAL(38, 6)) AS {tgt_val_alias})"
                    )
                    check = f"COALESCE(ABS({src_val_alias} - {tgt_val_alias}) / NULLIF(ABS(CAST({src_val_alias} AS DOUBLE)), 0), 0) <= {tolerance}"
                    payload_structs.append(
                        textwrap.dedent(
                            f"""
                    struct(
                        FORMAT_NUMBER({src_val_alias}, '#,##0.00') as source_value,
                        FORMAT_NUMBER({tgt_val_alias}, '#,##0.00') as target_value,
                        FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS({src_val_alias} - {tgt_val_alias}) / NULLIF(ABS(CAST({src_val_alias} AS DOUBLE)), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
                        FORMAT_STRING('%.2f%%', CAST({tolerance} * 100 AS DOUBLE)) AS tolerance_percent,
                        CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status
                    ) AS agg_validation_{col}_{agg}"""
                        )
                    )
                    overall_clauses.append(check)

        if not payload_structs:
            view_creation_sql = f"CREATE OR REPLACE TEMP VIEW final_metrics_view AS SELECT true as overall_validation_passed, parse_json(to_json(struct('No validations configured for task {task_key}' as message))) as result_payload"
        else:
            view_creation_sql = "CREATE OR REPLACE TEMP VIEW final_metrics_view AS\n"
            if ctes:
                view_creation_sql += "WITH\n" + ",\n".join(ctes) + "\n"
            from_clause = (
                " CROSS JOIN ".join([cte.split(" AS ")[0].strip() for cte in ctes])
                if ctes
                else "(SELECT 1 AS placeholder)"
            )
            select_payload = f"parse_json(to_json(struct({', '.join(payload_structs)}))) as result_payload"
            select_status = f"{' AND '.join(overall_clauses) if overall_clauses else 'true'} AS overall_validation_passed"
            view_creation_sql += (
                f"SELECT {select_payload}, {select_status} FROM {from_clause};"
            )

        insert_sql = (
            f"INSERT INTO {results_table} (task_key, status, run_id, job_id, job_name, timestamp, result_payload) "
            f"SELECT '{task_key}', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END, "
            f":run_id, :job_id, '{job_name}', current_timestamp(), result_payload FROM final_metrics_view"
        )
        fail_sql = f"SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: {task_key}. Payload: \\n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false"
        pass_sql = "SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true"
        return ";\n\n".join([view_creation_sql, insert_sql, fail_sql, pass_sql])

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
                "raw_history": f"SELECT * FROM {results_table_fqn} WHERE job_name = '{job_name}' ORDER BY timestamp DESC, task_key"
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
        """Generates and uploads all SQL files and the dashboard notebook for the job."""
        logger.info("Generating and uploading job assets...")
        asset_paths: dict[str, str] = {}
        job_assets_path: str = f"{self.root_path}/job_assets/{job_name}"
        self.w.workspace.mkdirs(job_assets_path)

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
        agg_sql_script: str = textwrap.dedent(
            f"""
            WITH run_results AS (
                SELECT * FROM {results_table} WHERE run_id = :run_id
            ),
            agg_metrics AS (
                SELECT
                  (SELECT COUNT(1) FROM run_results WHERE status = 'FAILURE') AS failure_count,
                  (SELECT COLLECT_LIST(task_key) FROM run_results WHERE status = 'FAILURE') as failed_tasks
            )
            SELECT CASE
                WHEN (SELECT failure_count FROM agg_metrics) > 0
                THEN RAISE_ERROR(CONCAT('DataPact validation tasks failed: ', to_json((SELECT failed_tasks FROM agg_metrics))))
                ELSE 'All DataPact validations passed successfully!'
            END;
        """
        )
        self.w.workspace.upload(
            path=agg_script_path,
            content=agg_sql_script.encode("utf-8"),
            overwrite=True,
            format=workspace.ImportFormat.RAW,
        )
        asset_paths["aggregate_results"] = agg_script_path
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
        except NotFound:
            logger.info("Dashboard file does not yet exist – will create")

        def q(sql):
            return sql.format(table=results_table_fqn, job=job_name)

        # Define all datasets needed for the dashboard
        datasets: list[dict[str, Any]] = [
            {
                "name": "ds_kpi",
                "displayName": "KPI Metrics (Latest Run)",
                "queryLines": [
                    q(
                        "WITH latest_run AS (SELECT * FROM {table} WHERE run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name='{job}')) "
                        "SELECT COUNT(*) as total_tasks, COUNT(IF(status = 'FAILURE', 1, NULL)) as failed_tasks, "
                        "COUNT(IF(status = 'SUCCESS', 1, NULL)) * 1.0 / COUNT(*) as success_rate_percent FROM latest_run"  # Corrected: Removed * 100
                    )
                ],
            },
            {
                "name": "ds_summary",
                "displayName": "Run Summary",
                "queryLines": [
                    q(
                        "SELECT status, COUNT(*) as task_count FROM {table} WHERE run_id = (SELECT MAX(run_id) FROM {table} WHERE job_name='{job}') GROUP BY status"
                    )
                ],
            },
            {
                "name": "ds_failure_rate",
                "displayName": "Failure Rate Over Time",
                "queryLines": [
                    q(
                        "SELECT date(timestamp) as run_date, COUNT(IF(status='FAILURE',1,NULL))*100/COUNT(*) as failure_rate FROM {table} WHERE job_name='{job}' GROUP BY 1 ORDER BY 1"
                    )
                ],
            },
            {
                "name": "ds_top_failures",
                "displayName": "Top Failing Tasks",
                "queryLines": [
                    q(
                        "SELECT task_key, COUNT(*) as failure_count FROM {table} WHERE status='FAILURE' AND job_name='{job}' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
                    )
                ],
            },
            {
                "name": "ds_history",
                "displayName": "Detailed Run History",
                "queryLines": [
                    q(
                        "SELECT task_key, status, timestamp, to_json(result_payload) as payload_json FROM {table} WHERE job_name='{job}' ORDER BY timestamp DESC, task_key"
                    )
                ],
            },
        ]

        widget_definitions: list[dict[str, Any]] = [
            {
                "ds_name": "ds_kpi",
                "type": "COUNTER",
                "title": "Total Tasks Executed",
                "pos": {"x": 0, "y": 0, "width": 2, "height": 4},
                "value_col": "total_tasks",
            },
            {
                "ds_name": "ds_kpi",
                "type": "COUNTER",
                "title": "Failed Tasks",
                "pos": {"x": 4, "y": 0, "width": 2, "height": 4},
                "value_col": "failed_tasks",
            },
            {
                "ds_name": "ds_kpi",
                "type": "SUCCESS_RATE_COUNTER",
                "title": "Success Rate",
                "pos": {"x": 2, "y": 0, "width": 2, "height": 4},
                "value_col": "success_rate_percent",
            },
            {
                "ds_name": "ds_summary",
                "type": "DONUT",
                "title": "Run Summary",
                "pos": {"x": 0, "y": 4, "width": 3, "height": 8},
            },
            {
                "ds_name": "ds_failure_rate",
                "type": "LINE",
                "title": "Failure Rate Over Time",
                "pos": {"x": 3, "y": 4, "width": 3, "height": 8},
            },
            {
                "ds_name": "ds_top_failures",
                "type": "BAR",
                "title": "Top Failing Tasks",
                "pos": {"x": 0, "y": 12, "width": 6, "height": 8},
            },
            {
                "ds_name": "ds_history",
                "type": "TABLE",
                "title": "Detailed Run History",
                "pos": {"x": 0, "y": 20, "width": 6, "height": 7},
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
                spec = {
                    "version": 3,
                    "widgetType": "counter",
                    "encodings": {"value": {"fieldName": w_def["value_col"]}},
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
                                        "value": "SUCCESS",
                                        "color": {
                                            "themeColorType": "visualizationColors",
                                            "position": 3,
                                        },
                                    },
                                    {
                                        "value": "FAILURE",
                                        "color": {
                                            "themeColorType": "visualizationColors",
                                            "position": 4,
                                        },
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
                query_fields = [
                    {"name": "task_key", "expression": "`task_key`"},
                    {
                        "name": "sum(failure_count)",
                        "expression": "SUM(`failure_count`)",
                    },
                ]
                spec = {
                    "version": 3,
                    "widgetType": "bar",
                    "encodings": {
                        "x": {
                            "fieldName": "task_key",
                            "scale": {"type": "categorical"},
                            "displayName": "Failing Task",
                        },
                        "y": {
                            "fieldName": "sum(failure_count)",
                            "scale": {"type": "quantitative"},
                            "displayName": "Total Failures",
                        },
                    },
                }

            elif w_def["type"] == "TABLE":
                query_fields = [
                    {"name": c, "expression": f"`{c}`"}
                    for c in ["task_key", "status", "timestamp", "payload_json"]
                ]
                spec = {
                    "version": 3,
                    "widgetType": "table",
                    "encodings": {
                        "columns": [
                            {"fieldName": "task_key", "displayName": "Task Key"},
                            {"fieldName": "status", "displayName": "Status"},
                            {"fieldName": "timestamp", "displayName": "Timestamp"},
                            {
                                "fieldName": "payload_json",
                                "displayName": "Result Payload",
                            },
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
                    "displayName": "DataPact Validation Results",
                    "layout": layout_widgets,
                    "pageType": "PAGE_TYPE_CANVAS",
                }
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
        warehouse: sql_service.EndpointInfo = self._ensure_sql_warehouse(warehouse_name)
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

        tasks: list[Task] = [
            Task(
                task_key=tk,
                sql_task=SqlTask(
                    file=SqlTaskFile(path=asset_paths[tk], source=Source.WORKSPACE),
                    warehouse_id=warehouse.id,
                    parameters=sql_params,
                ),
            )
            for tk in validation_task_keys
        ]

        tasks.append(
            Task(
                task_key="aggregate_results",
                depends_on=[TaskDependency(task_key=tk) for tk in validation_task_keys],
                run_if=RunIf.ALL_DONE,
                sql_task=SqlTask(
                    file=SqlTaskFile(
                        path=asset_paths["aggregate_results"], source=Source.WORKSPACE
                    ),
                    warehouse_id=warehouse.id,
                    parameters=sql_params,
                ),
            )
        )

        tasks.append(
            Task(
                task_key="refresh_dashboard",
                depends_on=[TaskDependency(task_key="aggregate_results")],
                run_if=RunIf.ALL_DONE,
                dashboard_task=DashboardTask(
                    dashboard_id=dashboard_id,
                    warehouse_id=warehouse.id,
                ),
            )
        )

        runner_is_service_principal: bool = bool(
            self.user_name and len(self.user_name) == 36 and "@" not in self.user_name
        )
        logger.info(
            "Program is being run as a service principal"
            if runner_is_service_principal
            else "Program is not being run as a service principal"
        )

        job_settings: JobSettings = JobSettings(
            name=job_name,
            tasks=tasks,
            run_as=JobRunAs(
                service_principal_name=(
                    self.user_name if runner_is_service_principal else None
                ),
                user_name=self.user_name if not runner_is_service_principal else None,
            ),
        )

        existing_job = next(iter(self.w.jobs.list(name=job_name)), None)
        job_id: int | None = (
            existing_job.job_id
            if existing_job
            else self.w.jobs.create(
                name=job_settings.name,
                tasks=job_settings.tasks,
                run_as=job_settings.run_as,
            ).job_id
        )
        if existing_job and job_id is not None:
            self.w.jobs.reset(job_id=job_id, new_settings=job_settings)

        if job_id is None:
            raise ValueError("Job ID is None. Cannot launch job run.")
        logger.info(f"Launching job {job_id}...")
        run_info = self.w.jobs.run_now(job_id=job_id)
        run_metadata = self.w.jobs.get_run(run_info.run_id)
        logger.info(f"Run started! View progress here: {run_metadata.run_page_url}")

        timeout: timedelta = timedelta(hours=1)
        deadline: datetime = datetime.now() + timeout
        while datetime.now() < deadline:
            if run_metadata.run_id is None:
                raise ValueError("Run ID is None. Cannot poll job run status.")
            run = self.w.jobs.get_run(run_metadata.run_id)
            life_cycle_state = getattr(run.state, "life_cycle_state", None)
            if life_cycle_state is not None and life_cycle_state in TERMINAL_STATES:
                break
            finished_tasks = sum(
                1
                for t in (run.tasks or [])
                if getattr(getattr(t, "state", None), "life_cycle_state", None)
                in TERMINAL_STATES
            )
            logger.info(
                f"Job state: {life_cycle_state}. Tasks finished: {finished_tasks}/{len(tasks)}"
            )
            time.sleep(30)
        else:
            raise TimeoutError("Job run timed out.")

        if run.state is not None:
            logger.info(f"Run finished with state: {run.state.result_state}")

            if run.state.result_state == jobs.RunResultState.SUCCESS:
                logger.success("✅ DataPact job completed successfully.")
            else:
                error_message = (
                    run.state.state_message or "Job failed without a specific message."
                )
                logger.warning(
                    f"DataPact job finished with failing tasks. Final state: {run.state.result_state}. Reason: {error_message} View details at {run.run_page_url}"
                )
        else:
            logger.error("Run state is None. Unable to determine job result.")
            raise RuntimeError("Run state is None. Unable to determine job result.")

    def _ensure_sql_warehouse(
        self: "DataPactClient",
        name: str,
    ) -> sql_service.EndpointInfo:
        """
        Finds a SQL warehouse by its name, ensures it is running, and returns its details.

        This method searches for a SQL warehouse with the specified name. If the warehouse is found but not running,
        it attempts to start it and waits for it to become available. If the warehouse cannot be found or started,
        an exception is raised.

        Args:
            name (str): The name of the SQL warehouse to locate and ensure is running.

        Returns:
            sql_service.EndpointInfo: The information object for the located (and running) SQL warehouse.

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
        return warehouse
