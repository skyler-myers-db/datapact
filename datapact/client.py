"""
The core client for interacting with the Databricks API.

This module contains the `DataPactClient` class. Its architecture is a
local-first orchestrator that dynamically generates pure SQL validation scripts.
It uploads these scripts as raw SOURCE files and creates a multi-task Databricks
Job where each task is a SQL Task of type 'File', running directly on a
specified Serverless SQL Warehouse. This is the definitive, correct architecture.
"""

import json
import time
from pathlib import Path
from datetime import timedelta
import textwrap

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, sql as sql_service, workspace
from databricks.sdk.service.jobs import RunLifeCycleState
from loguru import logger

TERMINAL_STATES: list[RunLifeCycleState] = [
    RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR
]

class DataPactClient:
    """
    A client that orchestrates validation tests by generating and running
    a pure SQL-based Databricks Job.
    """

    def __init__(self, profile: str = "DEFAULT") -> None:
        """Initializes the client with Databricks workspace credentials."""
        logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
        self.w = WorkspaceClient(profile=profile)
        self.user_name = self.w.current_user.me().user_name
        self.root_path = f"/Users/{self.user_name}/datapact"
        self.w.workspace.mkdirs(self.root_path)

    def _ensure_sql_warehouse(self, name: str) -> sql_service.EndpointInfo:
        """Ensures a Serverless SQL Warehouse exists and is running."""
        logger.info(f"Looking for SQL Warehouse '{name}'...")
        warehouse = None
        try:
            for wh in self.w.warehouses.list():
                if wh.name == name:
                    warehouse = wh
                    break
        except Exception as e:
            logger.error(f"An error occurred while trying to list warehouses: {e}")
            raise

        if not warehouse:
            raise ValueError(f"SQL Warehouse '{name}' not found. Please ensure it exists and you have permissions to view it.")

        logger.info(f"Found warehouse {warehouse.id}. State: {warehouse.state}")

        if warehouse.state not in [sql_service.State.RUNNING, sql_service.State.STARTING]:
            logger.info(f"Warehouse '{name}' is in state {warehouse.state}. Starting it...")
            self.w.warehouses.start(warehouse.id).result(timeout=timedelta(seconds=600))
            logger.success(f"Warehouse '{name}' started successfully.")
        
        return self.w.warehouses.get(warehouse.id)

    def _generate_validation_sql(self, config: dict[str, any], results_table: str | None) -> str:
        """
        Generates a complete, idempotent, and dynamic SQL validation script for a single task
        based on the provided configuration.
        """
        # --- FQNs and Basic Config ---
        source_fqn = f"`{config['source_catalog']}`.`{config['source_schema']}`.`{config['source_table']}`"
        target_fqn = f"`{config['target_catalog']}`.`{config['target_schema']}`.`{config['target_table']}`"
        task_key = config['task_key']
        ctes = []
        # --- SQL Generation ---
        final_checks = []
        metric_payload_parts = []
        
        # --- Overall Validation Logic ---
        overall_validation_passed_clauses = []
        
        # 1. Count Validation
        count_tolerance = config.get('count_tolerance')
        if count_tolerance is not None:
            ctes.append(textwrap.dedent(f"""
            count_metrics AS (
                SELECT
                    (SELECT COUNT(1) FROM {source_fqn}) AS source_count,
                    (SELECT COUNT(1) FROM {target_fqn}) AS target_count
            )
            """))
            metric_payload_parts.extend([
                "'source_count'", "source_count",
                "'target_count'", "target_count",
                "'count_relative_diff'", "ABS(target_count - source_count) / NULLIF(CAST(source_count AS DOUBLE), 0)"
            ])
            overall_validation_passed_clauses.append(
                f"(ABS(target_count - source_count) / NULLIF(CAST(source_count AS DOUBLE), 0)) <= {count_tolerance}"
            )
        
        # 2. Per-Row Hash Validation
        if config.get('pk_row_hash_check') and config.get('primary_keys'):
            primary_keys = config['primary_keys']
            pk_hash_threshold = config.get('pk_hash_threshold', 0.0)
            
            hash_columns = config.get('hash_columns')
            if hash_columns:
                hash_expr = f"md5(to_json(struct({', '.join([f'`{c}`' for c in hash_columns])})))"
            else:
                # This is the robust way to hash all columns without listing them
                hash_expr = "md5(to_json(struct(*)))"
                
            pk_cols_str = ", ".join([f"`{pk}`" for pk in primary_keys])
            join_expr = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in primary_keys])
            
            ctes.append(textwrap.dedent(f"""
            row_hash_metrics AS (
                SELECT
                    COUNT(1) AS total_compared_rows,
                    COALESCE(SUM(CASE WHEN s.row_hash <> t.row_hash THEN 1 ELSE 0 END), 0) AS mismatch_count
                FROM
                    (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {source_fqn}) s
                INNER JOIN
                    (SELECT {pk_cols_str}, {hash_expr} AS row_hash FROM {target_fqn}) t
                ON {join_expr}
            )
            """))
            metric_payload_parts.extend([
                "'total_compared_rows'", "total_compared_rows",
                "'mismatch_count'", "mismatch_count",
                "'mismatch_ratio'", "mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)"
            ])
            overall_validation_passed_clauses.append(
                f"(mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)) <= {pk_hash_threshold}"
            )
        # 3. Null Count Validation
        if config.get('null_validation_columns'):
            for col in config['null_validation_columns']:
                null_val_threshold = config.get('null_validation_threshold', 0.0)
                cte_key = f"null_metrics_{col}"
                ctes.append(textwrap.dedent(f"""
                {cte_key} AS (
                    SELECT
                        (SELECT COUNT(1) FROM {source_fqn} WHERE `{col}` IS NULL) AS source_nulls,
                        (SELECT COUNT(1) FROM {target_fqn} WHERE `{col}` IS NULL) AS target_nulls
                )
                """))
                metric_payload_parts.extend([
                    f"'source_nulls_{col}'", f"{cte_key}.source_nulls",
                    f"'target_nulls_{col}'", f"{cte_key}.target_nulls",
                    f"'null_relative_diff_{col}'", f"ABS({cte_key}.target_nulls - {cte_key}.source_nulls) / NULLIF(CAST({cte_key}.source_nulls AS DOUBLE), 0)"
                ])
                overall_validation_passed_clauses.append(
                    f"(ABS({cte_key}.target_nulls - {cte_key}.source_nulls) / NULLIF(CAST({cte_key}.source_nulls AS DOUBLE), 0)) <= {null_val_threshold}"
                )
        # 4. Aggregate Validations
        if config.get('agg_validations'):
            for agg_config in config.get('agg_validations', []):
                col = agg_config['column']
                for validation in agg_config['validations']:
                    agg = validation['agg']
                    tolerance = validation['tolerance']
                    cte_key = f"agg_metrics_{col}_{agg}"
                    ctes.append(textwrap.dedent(f"""
                    {cte_key} AS (
                        SELECT
                            TRY_CAST((SELECT {agg}(`{col}`) FROM {source_fqn}) AS DECIMAL(38, 6)) AS source_agg,
                            TRY_CAST((SELECT {agg}(`{col}`) FROM {target_fqn}) AS DECIMAL(38, 6)) AS target_agg
                    )
                    """))
                    metric_payload_parts.extend([
                        f"'source_agg_{col}_{agg}'", f"{cte_key}.source_agg",
                        f"'target_agg_{col}_{agg}'", f"{cte_key}.target_agg",
                        f"'agg_relative_diff_{col}_{agg}'", f"ABS({cte_key}.target_agg - {cte_key}.source_agg) / NULLIF(ABS(CAST({cte_key}.source_agg AS DOUBLE)), 0)"
                    ])
                    overall_validation_passed_clauses.append(
                        f"(ABS({cte_key}.target_agg - {cte_key}.source_agg) / NULLIF(ABS(CAST({cte_key}.source_agg AS DOUBLE)), 0)) <= {tolerance}"
                    )
        # --- Build the Final Query ---
        from_clause = "CROSS JOIN ".join([cte.split(" AS ")[0] for cte in ctes])
        final_sql = f"-- DataPact Validation for task: {task_key}\n"
        final_sql += f"-- Generated at: {time.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        final_sql += "WITH\n" + ", \n".join(ctes) + "\n" if ctes else ""
        final_sql += textwrap.dedent(f"""
        , final_metrics AS (
            SELECT
                map({', '.join(metric_payload_parts)}) as result_payload,
                {' AND '.join(overall_validation_passed_clauses) if overall_validation_passed_clauses else 'true'} AS overall_validation_passed
            FROM {from_clause}
        )
        """)
        if results_table:
            final_sql += textwrap.dedent(f"""
            INSERT INTO {results_table} (task_key, status, run_id, timestamp, result_payload)
            SELECT
                '{task_key}',
                CASE WHEN overall_validation_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
                :run_id,
                current_timestamp(),
                to_json(result_payload)
            FROM final_metrics;
            """)
        final_sql += textwrap.dedent(f"""
        SELECT
          CASE
            WHEN (SELECT overall_validation_passed FROM final_metrics)
            THEN 'Validation PASSED'
            ELSE RAISE_ERROR(CONCAT('DataPact validation failed for task: {task_key}. Payload: ', (SELECT to_json(result_payload) FROM final_metrics)))
          END;
        """)
        return final_sql
    
    def _upload_sql_scripts(self, config: dict[str, any], results_table: str | None) -> dict[str, str]:
        """Generates and uploads SQL scripts for all validation tasks."""
        logger.info("Generating and uploading SQL validation scripts...")
        task_paths: dict[str, str] = {}
        
        sql_tasks_path = f"{self.root_path}/sql_tasks"
        self.w.workspace.mkdirs(sql_tasks_path)
    
        for task_config in config['validations']:
            task_key = task_config['task_key']
            sql_script = self._generate_validation_sql(task_config, results_table)
            
            script_path = f"{sql_tasks_path}/{task_key}.sql"
            self.w.workspace.upload(
                path=script_path,
                content=sql_script.encode('utf-8'),
                overwrite=True,
                format=workspace.ImportFormat.RAW,
            )
            task_paths[task_key] = script_path
            logger.info(f"  - Uploaded SQL FILE for task '{task_key}' to {script_path}")
    
        if results_table:
            agg_script_path = f"{sql_tasks_path}/aggregate_results.sql"
            agg_sql_script = textwrap.dedent(f"""\
                -- DataPact Final Aggregation Task
                -- This task checks the results table for any failures from the current run.
                SELECT
                  CASE
                    WHEN (
                      SELECT COUNT(1)
                      FROM {results_table}
                      WHERE run_id = :run_id AND status = 'FAILURE'
                    ) > 0
                    THEN RAISE_ERROR('One or more DataPact validations failed. Check the results table for details.')
                    ELSE 'All DataPact validations passed successfully!'
                  END AS overall_status;
            """)
            self.w.workspace.upload(
                path=agg_script_path,
                content=agg_sql_script.encode('utf-8'),
                overwrite=True,
                format=workspace.ImportFormat.RAW,
            )
            task_paths['aggregate_results'] = agg_script_path
            logger.info(f"  - Uploaded aggregation SQL FILE to {agg_script_path}")
    
        return task_paths

    def run_validation(
            self,
            config: dict[str, any],
            job_name: str,
            warehouse_name: str,
            results_table: str | None = None,
        ) -> None:
            """Constructs, deploys, and runs the DataPact validation workflow."""
            warehouse = self._ensure_sql_warehouse(warehouse_name)
            task_paths = self._upload_sql_scripts(config, results_table)
    
            # Use the SDK's Job-related dataclasses
            from databricks.sdk.service.jobs import (
                JobParameter,
                JobSettings,
                RunAs,
                RunIf,
                SqlTask,
                Task,
                TaskDependency,
                FileSource
            )
    
            tasks: list[Task] = []
            validation_task_keys = [v_conf["task_key"] for v_conf in config["validations"]]
    
            # Build the list of tasks using the Task dataclass
            for task_key in validation_task_keys:
                tasks.append(
                    Task(
                        task_key=task_key,
                        sql_task=SqlTask(
                            file=FileSource(
                                path=task_paths[task_key],
                                source="WORKSPACE"
                            ),
                            warehouse_id=warehouse.id,
                        ),
                    )
                )
    
            # Build the aggregation task, if needed
            if results_table and 'aggregate_results' in task_paths:
                tasks.append(
                    Task(
                        task_key="aggregate_results",
                        depends_on=[TaskDependency(task_key=tk) for tk in validation_task_keys],
                        run_if=RunIf.ALL_DONE,
                        sql_task=SqlTask(
                            file=FileSource(
                                path=task_paths['aggregate_results'],
                                source="WORKSPACE"
                            ),
                            warehouse_id=warehouse.id,
                        ),
                    )
                )
    
            # Construct the entire job definition using the JobSettings dataclass
            job_settings = JobSettings(
                name=job_name,
                tasks=tasks,
                run_as=RunAs(user_name=self.user_name),
                parameters=[
                    JobParameter(name="run_id", default="{{job.run_id}}")
                ],
            )
    
            # Check for an existing job
            existing_job = None
            for j in self.w.jobs.list(name=job_name):
                existing_job = j
                break
            
            # Use the job_settings object for both creating and resetting the job
            if existing_job:
                logger.info(f"Updating existing job '{job_name}' (ID: {existing_job.job_id})...")
                self.w.jobs.reset(job_id=existing_job.job_id, new_settings=job_settings)
                job_id = existing_job.job_id
            else:
                logger.info(f"Creating new job '{job_name}'...")
                # The create method takes the attributes of the JobSettings object as keyword arguments
                new_job = self.w.jobs.create(
                    name=job_settings.name,
                    tasks=job_settings.tasks,
                    run_as=job_settings.run_as,
                    parameters=job_settings.parameters,
                )
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
            final_state_message = run.state.state_message
            logger.info(f"Run finished with state: {final_state}")
            if final_state == jobs.RunResultState.SUCCESS:
                logger.success("✅ DataPact job completed successfully.")
            else:
                logger.error(f"DataPact job did not succeed. Final state: {final_state}.")
                logger.error(f"Message: {final_state_message}")
                raise Exception(f"DataPact job failed. View details at {run.run_page_url}")
