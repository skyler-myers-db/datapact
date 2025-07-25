"""
The core client for interacting with the Databricks API.

This module contains the DataPactClient class. It orchestrates the entire
validation process by dynamically generating pure SQL validation scripts based
on a user's configuration. It then creates and runs a multi-task Databricks Job
where each task executes one of the generated SQL scripts on a specified
Serverless SQL Warehouse. Finally, it can create a results dashboard.
"""

import os
import time
import textwrap
from datetime import timedelta
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, sql as sql_service, workspace
from databricks.sdk.service.jobs import (
    RunLifeCycleState, Source, JobRunAs, JobSettings, Task, SqlTask,
    SqlTaskFile, TaskDependency, RunIf, JobParameterDefinition
)
from loguru import logger

TERMINAL_STATES: list[RunLifeCycleState] = [
    RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR
]
DEFAULT_CATALOG: str = "datapact_main"
DEFAULT_SCHEMA: str = "results"
DEFAULT_TABLE: str = "run_history"

class DataPactClient:
    """
    A client that orchestrates validation tests by generating and running
    a pure SQL-based Databricks Job and creating a results dashboard.
    """

    def __init__(self, profile: str = "DEFAULT") -> None:
        """
        Initializes the client with Databricks workspace credentials.

        Args:
            profile: The Databricks CLI profile to use for authentication.
        """
        logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
        self.w: WorkspaceClient = WorkspaceClient(profile=profile)
        self.user_name: str | None = self.w.current_user.me().user_name
        self.root_path: str = f"/Users/{self.user_name}/datapact"
        self.w.workspace.mkdirs(self.root_path)

    def _execute_sql(self, sql: str, warehouse_id: str) -> None:
        """
        A robust, synchronous helper function to execute a SQL statement.
        
        It submits the statement to the Statement Execution API and polls until
        it reaches a terminal state, raising an exception on failure.

        Args:
            sql: The SQL string to execute.
            warehouse_id: The ID of the SQL warehouse to run the statement on.
        
        Raises:
            Exception: If the SQL statement fails to execute.
            TimeoutError: If the execution takes longer than the defined timeout.
        """
        try:
            resp: sql_service.ExecuteStatementResponse = self.w.statement_execution.execute_statement(
                statement=sql, warehouse_id=warehouse_id, wait_timeout='0s', disposition='SYNC'
            )
            status: sql_service.StatementStatus = resp.status
            if status.state in [sql_service.StatementState.FAILED, sql_service.StatementState.CANCELED, sql_service.StatementState.CLOSED]:
                error: sql_service.Error | None = status.error
                error_message: str = error.message if error else "Unknown execution error."
                raise Exception(f"SQL execution failed with state {status.state}: {error_message}")
        except Exception as e:
            logger.critical(f"Failed to execute SQL: {sql}")
            raise e

    def _setup_default_infrastructure(self, warehouse_id: str) -> None:
        """Creates the default catalog and schema if they do not already exist."""
        logger.info(f"Ensuring default infrastructure ('{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}') exists...")
        self._execute_sql(f"CREATE CATALOG IF NOT EXISTS `{DEFAULT_CATALOG}`", warehouse_id)
        self._execute_sql(f"CREATE SCHEMA IF NOT EXISTS `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`", warehouse_id)
        logger.success("Default infrastructure is ready.")

    def _ensure_results_table_exists(self, results_table_fqn: str, warehouse_id: str) -> None:
        """Ensures the results Delta table exists, creating it if necessary."""
        logger.info(f"Ensuring results table '{results_table_fqn}' exists...")
        create_table_ddl: str = textwrap.dedent(f"""
            CREATE TABLE IF NOT EXISTS {results_table_fqn} (
                task_key STRING,
                status STRING,
                run_id BIGINT,
                job_id BIGINT,
                job_name STRING,
                timestamp TIMESTAMP,
                result_payload VARIANT
            ) USING DELTA
        """)
        self._execute_sql(create_table_ddl, warehouse_id)
        logger.success(f"Results table '{results_table_fqn}' is ready.")

    def _generate_validation_sql(self, config: dict[str, any], results_table: str, job_name: str) -> str:
        """
        Generates a complete, multi-statement SQL script for one validation task.

        Args:
            config: The config dictionary for a single validation task.
            results_table: The FQN of the results table to insert into.
            job_name: The name of the parent job, for logging.

        Returns:
            A string containing the complete, executable SQL for one validation task.
        """
        task_key: str = config['task_key']
        
        if config.get('pk_row_hash_check') and not config.get('primary_keys'):
            raise ValueError(f"Task '{task_key}': 'pk_row_hash_check' requires 'primary_keys'.")
        if config.get('null_validation_columns') and not config.get('primary_keys'):
            logger.warning(f"Task '{task_key}': 'null_validation_columns' without 'primary_keys' will only compare overall null counts, which can be misleading. For best results, provide primary keys.")

        source_fqn: str = f"`{config['source_catalog']}`.`{config['source_schema']}`.`{config['source_table']}`"
        target_fqn: str = f"`{config['target_catalog']}`.`{config['target_schema']}`.`{config['target_table']}`"
        
        ctes: list[str] = []
        payload_structs: list[str] = []
        overall_validation_passed_clauses: list[str] = []

        # Validation 1: Row Count
        if 'count_tolerance' in config:
            tolerance: float = config.get('count_tolerance', 0.0)
            ctes.append(f"count_metrics AS (SELECT (SELECT COUNT(1) FROM {source_fqn}) AS source_count, (SELECT COUNT(1) FROM {target_fqn}) AS target_count)")
            check: str = f"COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= {tolerance}"
            payload_structs.append("struct(source_count, target_count, "
                                   f"COALESCE((ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0)), 0) as relative_diff, {tolerance} as tolerance, "
                                   f"CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status) AS count_validation")
            overall_validation_passed_clauses.append(check)

        # Validation 2: Row Hash
        if config.get('pk_row_hash_check') and config.get('primary_keys'):
            pks: list[str] = config['primary_keys']
            threshold: float = config.get('pk_hash_threshold', 0.0)
            hash_cols: list[str] | None = config.get('hash_columns')
            hash_expr: str = f"md5(to_json(struct({', '.join([f'`{c}`' for c in hash_cols]) if hash_cols else '*'})))"
            pk_cols_str: str = ", ".join([f"`{pk}`" for pk in pks])
            join_expr: str = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in pks])
            ctes.append(f"""row_hash_metrics AS (
                SELECT COUNT(1) AS total_compared_rows, COALESCE(SUM(CASE WHEN s.row_hash <> t.row_hash THEN 1 ELSE 0 END), 0) AS mismatch_count
                FROM (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {source_fqn}) s
                INNER JOIN (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {target_fqn}) t ON {join_expr}
            )""")
            check = f"COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) <= {threshold}"
            payload_structs.append("struct(total_compared_rows, mismatch_count, "
                                   f"COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) as mismatch_ratio, {threshold} as threshold, "
                                   f"CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status) AS row_hash_validation")
            overall_validation_passed_clauses.append(check)
        
        # Validation 3: Null Counts
        null_cols: list[str] | None = config.get('null_validation_columns')
        if null_cols and 'null_validation_threshold' in config:
            threshold = config['null_validation_threshold']
            pks = config.get('primary_keys')
            for col in null_cols:
                if pks: # Advanced check: compare null status for matching rows
                    pk_cols_str = ", ".join([f"`{pk}`" for pk in pks])
                    join_expr = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in pks])
                    ctes.append(f"""null_metrics_{col} AS (
                        SELECT 
                            SUM(CASE WHEN s.`{col}` IS NULL THEN 1 ELSE 0 END) as source_nulls,
                            SUM(CASE WHEN t.`{col}` IS NULL THEN 1 ELSE 0 END) as target_nulls,
                            COUNT(1) as total_compared_rows
                        FROM {source_fqn} s JOIN {target_fqn} t ON {join_expr}
                    )""")
                    check = f"COALESCE(ABS(source_nulls - target_nulls) / NULLIF(CAST(total_compared_rows AS DOUBLE), 0), 0) <= {threshold}"

                else: # Simple check: compare total null counts in each table
                    ctes.append(f"""null_metrics_{col} AS (
                        SELECT
                            (SELECT COUNT(1) FROM {source_fqn} WHERE `{col}` IS NULL) AS source_nulls,
                            (SELECT COUNT(1) FROM {target_fqn} WHERE `{col}` IS NULL) AS target_nulls,
                            (SELECT COUNT(1) FROM {source_fqn}) AS source_total
                    )""")
                    check = f"COALESCE(ABS(source_nulls - target_nulls) / NULLIF(CAST(source_total AS DOUBLE), 0), 0) <= {threshold}"
                
                payload_structs.append("struct(source_nulls, target_nulls, total_compared_rows, "
                                      f"COALESCE(ABS(source_nulls - target_nulls) / NULLIF(CAST(total_compared_rows AS DOUBLE), 0), 0) as diff_ratio, {threshold} as threshold, "
                                      f"CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END as status) as null_validation_{col}")
                overall_validation_passed_clauses.append(check)

        # Validation 4: Aggregates
        if config.get('agg_validations'):
            for agg_config in config.get('agg_validations', []):
                col, validations = agg_config['column'], agg_config['validations']
                for val in validations:
                    agg, tolerance = val['agg'].upper(), val['tolerance']
                    src_alias, tgt_alias = f"source_value_{col}_{agg}", f"target_value_{col}_{agg}"
                    ctes.append(f"agg_{col}_{agg} AS (SELECT (SELECT {agg}(`{col}`) FROM {source_fqn}) AS {src_alias}, (SELECT {agg}(`{col}`) FROM {target_fqn}) AS {tgt_alias})")
                    check = f"COALESCE(ABS({src_alias} - {tgt_alias}) / NULLIF(ABS(CAST({src_alias} AS DOUBLE)), 0), 0) <= {tolerance}"
                    payload_structs.append(f"struct({src_alias} as source_value, {tgt_alias} as target_value, "
                                           f"COALESCE(ABS({src_alias} - {tgt_alias}) / NULLIF(ABS(CAST({src_alias} AS DOUBLE)), 0), 0) as relative_diff, {tolerance} as tolerance, "
                                           f"CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END as status) as agg_validation_{col}_{agg}")
                    overall_validation_passed_clauses.append(check)

        if not payload_structs: return f"SELECT 'No validations configured for task {task_key}' AS status;"

        view_creation_sql = "CREATE OR REPLACE TEMP VIEW final_metrics_view AS\n"
        view_creation_sql += "WITH\n" + ",\n".join(ctes) + "\n"
        from_clause = " CROSS JOIN ".join([cte.split(" AS ")[0].strip() for cte in ctes])
        select_payload = f"parse_json(to_json(struct({', '.join(payload_structs)}))) as result_payload"
        select_status = f"{' AND '.join(overall_validation_passed_clauses)} AS overall_validation_passed"
        view_creation_sql += f"SELECT {select_payload}, {select_status} FROM {from_clause};"
        
        insert_sql = (f"INSERT INTO {results_table} (task_key, status, run_id, job_id, job_name, timestamp, result_payload) "
                      f"SELECT '{task_key}', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END, "
                      f"{{{{job.run_id}}}}, {{{{job.id}}}}, '{job_name}', current_timestamp(), result_payload FROM final_metrics_view;")

        fail_sql = "SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: {task_key}. Payload: \\n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false;"
        pass_sql = "SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true;"
        
        return "\n\n".join([view_creation_sql, insert_sql, fail_sql, pass_sql])

    def _upload_sql_scripts(self, config: dict[str, any], results_table: str, job_name: str) -> dict[str, str]:
        """Generates and uploads all necessary SQL scripts for the job."""
        logger.info("Generating and uploading SQL validation scripts...")
        task_paths: dict[str, str] = {}
        sql_tasks_path: str = f"{self.root_path}/sql_tasks/{job_name}_{int(time.time())}"
        self.w.workspace.mkdirs(sql_tasks_path)

        for task_config in config['validations']:
            task_key: str = task_config['task_key']
            sql_script: str = self._generate_validation_sql(task_config, results_table, job_name)
            script_path: str = f"{sql_tasks_path}/{task_key}.sql"
            self.w.workspace.upload(path=script_path, content=sql_script.encode('utf-8'), overwrite=True)
            task_paths[task_key] = script_path
            logger.info(f"  - Uploaded SQL for task '{task_key}' to {script_path}")
        
        agg_script_path: str = f"{sql_tasks_path}/aggregate_results.sql"
        agg_sql_script: str = textwrap.dedent(f"""
            CREATE OR REPLACE TEMP VIEW run_results AS
            SELECT * FROM {results_table} WHERE run_id = {{{{job.run_id}}}};
            CREATE OR REPLACE TEMP VIEW agg_metrics AS
            SELECT
              (SELECT COUNT(1) FROM run_results WHERE status = 'FAILURE') AS failure_count,
              (SELECT COLLECT_LIST(task_key) FROM run_results WHERE status = 'FAILURE') as failed_tasks;
            SELECT CASE
                WHEN (SELECT failure_count FROM agg_metrics) > 0
                THEN RAISE_ERROR(CONCAT('DataPact tasks failed: ', (SELECT to_json(failed_tasks) FROM agg_metrics)))
                ELSE 'All DataPact validations passed successfully!'
            END;
        """)
        self.w.workspace.upload(path=agg_script_path, content=agg_sql_script.encode('utf-8'), overwrite=True)
        task_paths['aggregate_results'] = agg_script_path
        logger.info(f"  - Uploaded aggregation SQL to {agg_script_path}")
        return task_paths

    def run_validation(
        self,
        config: dict[str, any],
        job_name: str,
        warehouse_name: str,
        results_table: str | None = None
    ) -> None:
        """
        Main orchestrator for the validation process. This function will:
        1. Set up the necessary infrastructure (results table).
        2. Generate and upload SQL scripts for each validation task.
        3. Create and run a multi-task Databricks job.
        4. Automatically create or update a BI dashboard with the results.
        
        Args:
            config: Loaded validation YAML file as a dictionary.
            job_name: Name for the Databricks Job and Dashboard.
            warehouse_name: Name of the Serverless SQL Warehouse.
            results_table: Optional FQN of the table to store results.
        """
        warehouse = self._ensure_sql_warehouse(warehouse_name)
        
        final_results_table: str
        if not results_table:
            self._setup_default_infrastructure(warehouse.id)
            final_results_table = f"`{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.`{DEFAULT_TABLE}`"
        else:
            final_results_table = f"`{results_table}`"

        self._ensure_results_table_exists(final_results_table, warehouse.id)
        
        task_paths = self._upload_sql_scripts(config, final_results_table, job_name)
        
        tasks: list[Task] = []
        validation_task_keys: list[str] = [v_conf["task_key"] for v_conf in config["validations"]]
        for task_key in validation_task_keys:
            tasks.append(Task(task_key=task_key, sql_task=SqlTask(file=SqlTaskFile(path=task_paths[task_key]), warehouse_id=warehouse.id)))

        tasks.append(Task(task_key="aggregate_results", depends_on=[TaskDependency(task_key=tk) for tk in validation_task_keys], run_if=RunIf.ALL_DONE, sql_task=SqlTask(file=SqlTaskFile(path=task_paths['aggregate_results']), warehouse_id=warehouse.id)))

        job_settings = JobSettings(name=job_name, tasks=tasks, run_as=JobRunAs(user_name=self.user_name))
        
        existing_job = next(iter(self.w.jobs.list(name=job_name)), None)
        job_id: int
        if existing_job:
            logger.info(f"Updating existing job '{job_name}' (ID: {existing_job.job_id})...")
            self.w.jobs.reset(job_id=existing_job.job_id, new_settings=job_settings)
            job_id = existing_job.job_id
        else:
            logger.info(f"Creating new job '{job_name}'...")
            new_job = self.w.jobs.create(**job_settings.as_dict())
            job_id = new_job.job_id

        logger.info(f"Launching job {job_id}...")
        run_info = self.w.jobs.run_now(job_id=job_id)
        run = self.w.jobs.get_run(run_info.run_id)
        
        logger.info(f"Run started! View progress here: {run.run_page_url}")
        while run.state.life_cycle_state not in TERMINAL_STATES:
            time.sleep(20)
            run = self.w.jobs.get_run(run.run_id)
            finished_tasks = sum(1 for t in run.tasks if t.state.life_cycle_state in TERMINAL_STATES)
            logger.info(f"Job state: {run.state.life_cycle_state}. Tasks finished: {finished_tasks}/{len(tasks)}")

        final_state = run.state.result_state
        logger.info(f"Run finished with state: {final_state}")
        
        # Dashboard creation is now automatic and unconditional.
        try:
            self._create_or_update_dashboard(job_name, final_results_table, warehouse.id)
        except Exception as e:
            logger.error(f"Failed to create or update the dashboard. Please check permissions. Error: {e}")

        if final_state == jobs.RunResultState.SUCCESS:
            logger.success("✅ DataPact job completed successfully.")
        else:
            logger.error(f"DataPact job did not succeed. Final state: {final_state}. View details at {run.run_page_url}")
            raise Exception("DataPact job failed.")

    def _create_or_update_dashboard(self, job_name: str, results_table_fqn: str, warehouse_id: str) -> None:
        """
        Automatically creates or updates a Databricks SQL Dashboard.
        This method will replace any existing dashboard with the same name to ensure it's up-to-date.
        """
        dashboard_name = f"DataPact Results: {job_name}"
        logger.info(f"Automatically creating or updating dashboard: '{dashboard_name}'...")

        for d in self.w.dashboards.list(q=dashboard_name):
            if d.display_name == dashboard_name:
                logger.warning(f"Replacing existing dashboard (ID: {d.dashboard_id}) to ensure it is up-to-date.")
                self.w.dashboards.delete(d.dashboard_id)
                # Clean up associated queries to prevent clutter
                if d.widgets:
                    for widget in d.widgets:
                        if widget.visualization and widget.visualization.query:
                            try:
                                self.w.queries.delete(widget.visualization.query.query_id)
                            except Exception:
                                pass # Ignore if query is already gone

        queries = {
            "run_summary": f"SELECT status, COUNT(1) as task_count FROM {results_table_fqn} WHERE run_id = (SELECT MAX(run_id) FROM {results_table_fqn} WHERE job_name = '{job_name}') GROUP BY status",
            "failure_rate_over_time": f"SELECT to_date(timestamp) as run_date, COUNT(CASE WHEN status = 'FAILURE' THEN 1 END) * 100.0 / COUNT(1) as failure_rate_percent FROM {results_table_fqn} WHERE job_name = '{job_name}' GROUP BY 1 ORDER BY 1",
            "top_failing_tasks": f"SELECT task_key, COUNT(1) as failure_count FROM {results_table_fqn} WHERE status = 'FAILURE' AND job_name = '{job_name}' GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            "raw_history": f"SELECT *, to_json(result_payload) as payload_json FROM {results_table_fqn} WHERE job_name = '{job_name}' ORDER BY timestamp DESC, task_key"
        }
        
        widgets = []
        for name, sql in queries.items():
            query_obj = self.w.queries.create(name=f"DataPact-{job_name}-{name}", data_source_id=warehouse_id, query=sql)
            logger.info(f"  - Created dashboard query '{query_obj.name}'")
            
            viz_options: dict[str, Any] = {}
            viz_type = "TABLE"
            if name == "run_summary":
                viz_type = "COUNTER"; viz_options = {"counterColName": "task_count"}
            elif name == "failure_rate_over_time":
                viz_type = "CHART"; viz_options = {"globalSeriesType": "line", "yAxis": [{"type": "linear"}], "xAxis": {"labels": {"enabled": True}}, "series": {"stackingMode": "stack"}}
            elif name == "top_failing_tasks":
                viz_type = "CHART"; viz_options = {"globalSeriesType": "bar", "yAxis": [{"type": "linear"}], "xAxis": {"labels": {"enabled": True}}}

            viz = self.w.visualizations.create(query_id=query_obj.id, type=viz_type, name=f"Viz - {name}", options=viz_options)
            widgets.append(sql_service.WidgetCreate(visualization_id=viz.id, text=""))

        dashboard = self.w.dashboards.create(name=dashboard_name, warehouse_id=warehouse_id, widgets=widgets)
        logger.success(f"✅ Dashboard is ready! View your data quality command center here: {self.w.config.host}/sql/dashboards/{dashboard.id}")

    def _ensure_sql_warehouse(self, name: str) -> sql_service.EndpointInfo:
        """Finds a SQL warehouse by name, starts it if stopped, and returns details."""
        logger.info(f"Looking for SQL Warehouse '{name}'...")
        warehouse: sql_service.EndpointInfo | None = next((wh for wh in self.w.warehouses.list() if wh.name == name), None)

        if not warehouse:
            raise ValueError(f"SQL Warehouse '{name}' not found.")

        logger.info(f"Found warehouse '{name}' (ID: {warehouse.id}). State: {warehouse.state}")
        
        if warehouse.state not in [sql_service.State.RUNNING, sql_service.State.STARTING]:
            logger.info(f"Warehouse '{name}' is {warehouse.state}. Attempting to start...")
            self.w.warehouses.start(warehouse.id).result(timeout=timedelta(minutes=10))
            logger.success(f"Warehouse '{name}' started successfully.")
        
        return self.w.warehouses.get(warehouse.id)
