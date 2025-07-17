"""
The core client for interacting with the Databricks API.

This module contains the DataPactClient class. It orchestrates the entire
validation process by dynamically generating pure SQL validation scripts based
on a user's configuration. It then creates and runs a multi-task Databricks Job
where each task executes one of the generated SQL scripts on a specified
Serverless SQL Warehouse.
"""

import os
import time
import textwrap
from datetime import timedelta

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
    a pure SQL-based Databricks Job.
    """

    def __init__(self, profile: str = "DEFAULT") -> None:
        """
        Initializes the client with Databricks workspace credentials and
        determines the warehouse to use based on a defined hierarchy.

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
        it reaches a terminal state, raising an exception on failure. This is
        used for setting up infrastructure before the main job runs.

        Args:
            sql: The SQL string to execute.
            warehouse_id: The ID of the SQL warehouse to run the statement on.
        
        Raises:
            Exception: If the SQL statement fails to execute.
            TimeoutError: If the execution takes longer than the defined timeout.
        """
        resp: sql_service.ExecuteStatementResponse = self.w.statement_execution.execute_statement(
            statement=sql, warehouse_id=warehouse_id, wait_timeout='0s'
        )
        statement_id: str = resp.statement_id
        timeout_seconds: int = 120

        start_time: float = time.time()
        while time.time() - start_time < timeout_seconds:
            status: sql_service.StatementStatus = self.w.statement_execution.get_statement(statement_id=statement_id)
            current_state: sql_service.StatementState = status.status.state

            if current_state == sql_service.StatementState.SUCCEEDED:
                return
            
            if current_state in [sql_service.StatementState.FAILED, sql_service.StatementState.CANCELED, sql_service.StatementState.CLOSED]:
                error: sql_service.Error | None = status.status.error
                error_message: str = error.message if error else "Unknown execution error."
                raise Exception(f"SQL execution failed with state {current_state}: {error_message}")

            time.sleep(3)
        
        raise TimeoutError(f"SQL statement timed out after {timeout_seconds} seconds.")

    def _setup_default_infrastructure(self, warehouse_id: str) -> None:
        """
        Creates the default catalog and schema if they do not already exist.

        Args:
            warehouse_id: The ID of the SQL warehouse to use for creation.
        """
        logger.info(f"Ensuring default infrastructure ('{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}') exists...")
        self._execute_sql(f"CREATE CATALOG IF NOT EXISTS `{DEFAULT_CATALOG}`", warehouse_id)
        self._execute_sql(f"CREATE SCHEMA IF NOT EXISTS `{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`", warehouse_id)
        logger.success("Default infrastructure is ready.")

    def _ensure_results_table_exists(self, results_table_fqn: str, warehouse_id: str) -> None:
        """
        Ensures the results Delta table exists, creating it if necessary.
        This uses the modern VARIANT data type for storing the JSON payload.

        Args:
            results_table_fqn: The fully-qualified (catalog.schema.table) name for the results table.
            warehouse_id: The ID of the SQL warehouse to run the DDL on.
        """
        logger.info(f"Ensuring results table '{results_table_fqn}' exists...")
        create_table_ddl: str = textwrap.dedent(f"""
            CREATE TABLE IF NOT EXISTS {results_table_fqn} (
                task_key STRING,
                status STRING,
                run_id BIGINT,
                timestamp TIMESTAMP,
                result_payload VARIANT
            ) USING DELTA
        """)
        self._execute_sql(create_table_ddl, warehouse_id)
        logger.success(f"Results table '{results_table_fqn}' is ready.")

def _generate_validation_sql(self, config: dict[str, any], results_table: str) -> str:
        """
        Generates a complete, multi-statement SQL script for the validation task.

        Args:
            config: The configuration dictionary for a single validation task.
            results_table: The FQN of the results table to insert into.

        Returns:
            A string containing the complete, executable SQL for one validation task.
        """
        source_fqn: str = f"`{config['source_catalog']}`.`{config['source_schema']}`.`{config['source_table']}`"
        target_fqn: str = f"`{config['target_catalog']}`.`{config['target_schema']}`.`{config['target_table']}`"
        task_key: str = config['task_key']
        ctes: list[str] = []
        payload_structs: list[str] = []
        overall_validation_passed_clauses: list[str] = []

        if 'count_tolerance' in config:
            tolerance: float = config.get('count_tolerance', 0.0)
            ctes.append(f"count_metrics AS (SELECT (SELECT COUNT(1) FROM {source_fqn}) AS source_count, (SELECT COUNT(1) FROM {target_fqn}) AS target_count)")
            check: str = f"COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= {tolerance}"
            payload_structs.append(textwrap.dedent(f"""
            struct(
                source_count,
                target_count,
                FORMAT_STRING('%.2f%%', CAST(COALESCE((ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0)), 0) * 100 AS DOUBLE)) as relative_diff_percent,
                FORMAT_STRING('%.2f%%', CAST({tolerance} * 100 AS DOUBLE)) AS tolerance_percent,
                CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status
            ) AS count_validation
            """))
            overall_validation_passed_clauses.append(check)

        if config.get('pk_row_hash_check') and config.get('primary_keys'):
            primary_keys: list[str] = config.get('primary_keys', [])
            pk_hash_threshold: float = config.get('pk_hash_threshold', 0.0)
            hash_columns: list[str] | None = config.get('hash_columns')
            hash_expr: str = f"md5(to_json(struct({', '.join([f'`{c}`' for c in hash_columns]) if hash_columns else '*'})))"
            pk_cols_str: str = ", ".join([f"`{pk}`" for pk in primary_keys])
            join_expr: str = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in primary_keys])
            ctes.append(f"row_hash_metrics AS (SELECT COUNT(1) AS total_compared_rows, COALESCE(SUM(CASE WHEN s.row_hash <> t.row_hash THEN 1 ELSE 0 END), 0) AS mismatch_count FROM (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {source_fqn}) s INNER JOIN (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {target_fqn}) t ON {join_expr})")
            check: str = f"COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) <= {pk_hash_threshold}"
            payload_structs.append(textwrap.dedent(f"""
            struct(
                total_compared_rows AS compared_rows,
                mismatch_count,
                FORMAT_STRING('%.2f%%', CAST(COALESCE((mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)), 0) * 100 AS DOUBLE)) as mismatch_percent,
                FORMAT_STRING('%.2f%%', CAST({pk_hash_threshold} * 100 AS DOUBLE)) AS threshold_percent,
                CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status
            ) AS row_hash_validation
            """))
            overall_validation_passed_clauses.append(check)

        if config.get('agg_validations'):
            for agg_config in config.get('agg_validations', []):
                col, agg, tolerance = agg_config['column'], agg_config['validations'][0]['agg'], agg_config['validations'][0]['tolerance']
                src_val_alias, tgt_val_alias = f"source_value_{col}_{agg}", f"target_value_{col}_{agg}"
                ctes.append(f"agg_metrics_{col}_{agg} AS (SELECT TRY_CAST((SELECT {agg}(`{col}`) FROM {source_fqn}) AS DECIMAL(38, 6)) AS {src_val_alias}, TRY_CAST((SELECT {agg}(`{col}`) FROM {target_fqn}) AS DECIMAL(38, 6)) AS {tgt_val_alias})")
                check = f"COALESCE(ABS({src_val_alias} - {tgt_val_alias}) / NULLIF(ABS(CAST({src_val_alias} AS DOUBLE)), 0), 0) <= {tolerance}"
                payload_structs.append(textwrap.dedent(f"""
                struct(
                    {src_val_alias} AS source_value,
                    {tgt_val_alias} AS target_value,
                    FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS({src_val_alias} - {tgt_val_alias}) / NULLIF(ABS(CAST({src_val_alias} AS DOUBLE)), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,
                    FORMAT_STRING('%.2f%%', CAST({tolerance} * 100 AS DOUBLE)) AS tolerance_percent,
                    CASE WHEN {check} THEN 'PASS' ELSE 'FAIL' END AS status
                ) AS agg_validation_{col}_{agg}
                """))
                overall_validation_passed_clauses.append(check)

        sql_statements: list[str] = []
        view_creation_sql: str = f"CREATE OR REPLACE TEMP VIEW final_metrics_view AS\n"
        if ctes: view_creation_sql += "WITH\n" + ",\n".join(ctes) + "\n"
        from_clause: str = " CROSS JOIN ".join([cte.split(" AS ")[0].strip() for cte in ctes]) if ctes else "(SELECT 1 AS placeholder)"
        view_creation_sql += f"SELECT parse_json(to_json(struct({', '.join(payload_structs)}))) as result_payload, {' AND '.join(overall_validation_passed_clauses) if overall_validation_passed_clauses else 'true'} AS overall_validation_passed FROM {from_clause}"
        sql_statements.append(view_creation_sql)

        sql_statements.append(f"INSERT INTO {results_table} (task_key, status, run_id, timestamp, result_payload) SELECT '{task_key}', CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END, :run_id, current_timestamp(), result_payload FROM final_metrics_view")
        sql_statements.append(f"SELECT RAISE_ERROR(CONCAT('DataPact validation failed for task: {task_key}. Payload: \\n', to_json(result_payload, map('pretty', 'true')))) FROM final_metrics_view WHERE overall_validation_passed = false")
        sql_statements.append(f"SELECT to_json(result_payload, map('pretty', 'true')) AS result FROM final_metrics_view WHERE overall_validation_passed = true")
        
        return ";\n\n".join(sql_statements)
    def _upload_sql_scripts(self, config: dict[str, any], results_table: str) -> dict[str, str]:
        """
        Generates and uploads SQL scripts, including a more robust and informative aggregation script.
        """
        logger.info("Generating and uploading SQL validation scripts...")
        task_paths: dict[str, str] = {}
        sql_tasks_path: str = f"{self.root_path}/sql_tasks"
        self.w.workspace.mkdirs(sql_tasks_path)

        for task_config in config['validations']:
            task_key: str = task_config['task_key']
            sql_script: str = self._generate_validation_sql(task_config, results_table)
            script_path: str = f"{sql_tasks_path}/{task_key}.sql"
            self.w.workspace.upload(path=script_path, content=sql_script.encode('utf-8'), overwrite=True, format=workspace.ImportFormat.RAW)
            task_paths[task_key] = script_path
            logger.info(f"  - Uploaded SQL FILE for task '{task_key}' to {script_path}")
        
        agg_script_path: str = f"{sql_tasks_path}/aggregate_results.sql"
        agg_sql_script: str = textwrap.dedent(f"""
            CREATE OR REPLACE TEMP VIEW latest_run_id AS 
            SELECT run_id FROM {results_table} ORDER BY timestamp DESC LIMIT 1;

            CREATE OR REPLACE TEMP VIEW agg_metrics AS
            SELECT
              (SELECT COUNT(1) FROM {results_table} WHERE run_id = (SELECT run_id FROM latest_run_id) AND status = 'FAILURE') AS failure_count,
              (SELECT COUNT(1) FROM {results_table} WHERE run_id = (SELECT run_id FROM latest_run_id) AND status = 'SUCCESS') AS success_count,
              (SELECT COLLECT_LIST(task_key) FROM {results_table} WHERE run_id = (SELECT run_id FROM latest_run_id) AND status = 'FAILURE') as failed_tasks;

            SELECT CASE 
                WHEN (SELECT failure_count FROM agg_metrics) > 0 
                THEN RAISE_ERROR(CONCAT('The following DataPact tasks failed: ', (SELECT to_json(failed_tasks) FROM agg_metrics)))
                WHEN (SELECT success_count FROM agg_metrics) < CAST(:expected_successes AS INT)
                THEN RAISE_ERROR(CONCAT('Expected ', :expected_successes, ' successful tasks, but only ', (SELECT success_count FROM agg_metrics), ' reported success. Check for silent failures.'))
                ELSE 'All DataPact validations passed successfully!' 
            END;
        """)
        self.w.workspace.upload(path=agg_script_path, content=agg_sql_script.encode('utf-8'), overwrite=True, format=workspace.ImportFormat.RAW)
        task_paths['aggregate_results'] = agg_script_path
        logger.info(f"  - Uploaded informative aggregation SQL FILE to {agg_script_path}")
        return task_paths

    def run_validation(self, config: dict[str, any], job_name: str, warehouse_name: str | None = None, results_table: str | None = None) -> None:
        """
        The main orchestrator, now with an improved warehouse configuration hierarchy.
        """
        final_warehouse_name: str
        if warehouse_name:
            final_warehouse_name = warehouse_name
            logger.info(f"Using warehouse provided via --warehouse flag: '{final_warehouse_name}'")
        elif self.w.config.get('datapact_warehouse'):
            final_warehouse_name = self.w.config.get('datapact_warehouse')
            logger.info(f"Using warehouse from .databrickscfg [datapact_warehouse]: '{final_warehouse_name}'")
        elif os.getenv("DATAPACT_WAREHOUSE"):
            final_warehouse_name = os.getenv("DATAPACT_WAREHOUSE")
            logger.info(f"Using warehouse from DATAPACT_WAREHOUSE env var: '{final_warehouse_name}'")
        else:
            raise ValueError("No warehouse specified. Provide one via --warehouse flag, a 'datapact_warehouse' entry in .databrickscfg, or the DATAPACT_WAREHOUSE env var.")

        warehouse: sql_service.EndpointInfo = self._ensure_sql_warehouse(final_warehouse_name)
        
        final_results_table: str = results_table if results_table else f"`{DEFAULT_CATALOG}`.`{DEFAULT_SCHEMA}`.`{DEFAULT_TABLE}`"
        if not results_table:
            self._setup_default_infrastructure(warehouse.id)
            logger.info(f"No results table provided. Using default: {final_results_table}")

        self._ensure_results_table_exists(final_results_table, warehouse.id)
        
        task_paths: dict[str, str] = self._upload_sql_scripts(config, final_results_table)
        
        tasks: list[Task] = []
        validation_task_keys: list[str] = [v_conf["task_key"] for v_conf in config["validations"]]
        num_validation_tasks: int = len(validation_task_keys)

        for task_key in validation_task_keys:
            tasks.append(Task(task_key=task_key, sql_task=SqlTask(file=SqlTaskFile(path=task_paths[task_key], source=Source.WORKSPACE), warehouse_id=warehouse.id)))

        agg_task_parameters: dict[str, str] = {'expected_successes': str(num_validation_tasks)}
        tasks.append(Task(task_key="aggregate_results", depends_on=[TaskDependency(task_key=tk) for tk in validation_task_keys], run_if=RunIf.ALL_DONE, sql_task=SqlTask(file=SqlTaskFile(path=task_paths['aggregate_results'], source=Source.WORKSPACE), warehouse_id=warehouse.id, parameters=agg_task_parameters)))

        job_settings: JobSettings = JobSettings(name=job_name, tasks=tasks, run_as=JobRunAs(user_name=self.user_name), parameters=[JobParameterDefinition(name="run_id", default="{{job.run_id}}")])
        
        existing_job: jobs.Job | None = next(iter(self.w.jobs.list(name=job_name)), None)
        
        job_id: int
        if existing_job:
            logger.info(f"Updating existing job '{job_name}' (ID: {existing_job.job_id})...")
            self.w.jobs.reset(job_id=existing_job.job_id, new_settings=job_settings)
            job_id = existing_job.job_id
        else:
            logger.info(f"Creating new job '{job_name}'...")
            new_job: jobs.Job = self.w.jobs.create(name=job_settings.name, tasks=job_settings.tasks, run_as=job_settings.run_as, parameters=job_settings.parameters)
            job_id = new_job.job_id

        logger.info(f"Launching job {job_id}...")
        run_info: jobs.Run = self.w.jobs.run_now(job_id=job_id)
        run: jobs.Run = self.w.jobs.get_run(run_info.run_id)
        
        logger.info(f"Run started! View progress here: {run.run_page_url}")
        while run.state.life_cycle_state not in TERMINAL_STATES:
            time.sleep(20)
            run = self.w.jobs.get_run(run.run_id)
            finished_tasks: int = sum(1 for t in run.tasks if t.state.life_cycle_state in TERMINAL_STATES)
            logger.info(f"Job state: {run.state.life_cycle_state}. Tasks finished: {finished_tasks}/{len(tasks)}")

        final_state: jobs.RunResultState = run.state.result_state
        logger.info(f"Run finished with state: {final_state}")
        if final_state == jobs.RunResultState.SUCCESS:
            logger.success("✅ DataPact job completed successfully.")
        else:
            logger.error(f"DataPact job did not succeed. Final state: {final_state}. View details at {run.run_page_url}")
            raise Exception(f"DataPact job failed.")

    def _ensure_sql_warehouse(self, name: str) -> sql_service.EndpointInfo:
        """
        Finds a SQL warehouse by name, starts it if it's stopped, and returns its details.
        """
        logger.info(f"Looking for SQL Warehouse '{name}'...")
        warehouse: sql_service.EndpointInfo | None = None
        for wh in self.w.warehouses.list():
            if wh.name == name:
                warehouse = wh
                break

        if not warehouse:
            raise ValueError(f"SQL Warehouse '{name}' not found.")

        warehouse_id_to_check: str = warehouse.id
        logger.info(f"Found warehouse {warehouse_id_to_check}. State: {warehouse.state}")
        
        if warehouse.state not in [sql_service.State.RUNNING, sql_service.State.STARTING]:
            logger.info(f"Warehouse '{name}' is {warehouse.state}. Starting it...")
            self.w.warehouses.start(warehouse_id_to_check).result(timeout=timedelta(minutes=5))
            logger.success(f"Warehouse '{name}' started successfully.")
        
        return self.w.warehouses.get(warehouse_id_to_check)
