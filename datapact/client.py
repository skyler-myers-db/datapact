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
        """Generates a complete, idempotent SQL validation script for a single task."""
        source_fqn = f"`{config['source_catalog']}`.`{config['source_schema']}`.`{config['source_table']}`"
        target_fqn = f"`{config['target_catalog']}`.`{config['target_schema']}`.`{config['target_table']}`"
        
        count_tolerance = config.get('count_tolerance', 0.0)
        
        sql = textwrap.dedent(f"""\
            -- DataPact Validation for task: {config['task_key']}
            -- Step 1: Declare a variable to hold the validation outcome.
            DECLARE validation_passed BOOLEAN;

            -- Step 2: Calculate all metrics and insert a detailed JSON payload into the history table.
            -- This step will ALWAYS run to ensure logging, even on failure.
            CREATE TABLE IF NOT EXISTS {results_table} (task_key STRING, status STRING, run_id BIGINT, timestamp TIMESTAMP, result_payload STRING);
            INSERT INTO {results_table} (task_key, status, run_id, timestamp, result_payload)
            WITH
              source_metrics AS (
                SELECT COUNT(1) AS count FROM {source_fqn}
              ),
              target_metrics AS (
                SELECT COUNT(1) AS count FROM {target_fqn}
              ),
              validation_checks AS (
                SELECT
                  (SELECT count FROM source_metrics) AS source_count,
                  (SELECT count FROM target_metrics) AS target_count,
                  -- Correctly handle division by zero
                  CASE
                    WHEN (SELECT count FROM source_metrics) = 0 THEN (SELECT count FROM target_metrics) = 0
                    ELSE (abs((SELECT count FROM target_metrics) - (SELECT count FROM source_metrics)) / CAST((SELECT count FROM source_metrics) AS DOUBLE)) <= {count_tolerance}
                  END AS count_check_passed
              )
            SELECT
              '{config['task_key']}',
              CASE WHEN v.count_check_passed THEN 'SUCCESS' ELSE 'FAILURE' END,
              :run_id,
              current_timestamp(),
              to_json(
                struct(
                  '{config['task_key']}' AS `task_key`,
                  v.source_count,
                  v.target_count,
                  v.count_check_passed,
                  (v.count_check_passed) AS overall_validation_passed -- In a full version, this would be AND of all checks
                )
              )
            FROM validation_checks v;

            -- Step 3: Set the variable based on the result that was just inserted.
            SET VAR validation_passed = (
                SELECT result_payload:overall_validation_passed
                FROM {results_table}
                WHERE run_id = :run_id AND task_key = '{config['task_key']}'
                ORDER BY timestamp DESC
                LIMIT 1
            );

            -- Step 4: After logging, use the variable to conditionally fail the task.
            SELECT
              CASE
                WHEN validation_passed THEN TRUE
                ELSE
                  RAISE_ERROR(
                    'One or more validations failed for task {config['task_key']}. Check history table for details.'
                  )
              END AS `Validation Passed`;
        """)
        return sql

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
                format=workspace.ImportFormat.RAW
            )
            task_paths[task_key] = script_path
            logger.info(f"  - Uploaded SQL FILE for task '{task_key}' to {script_path}")

        if results_table:
            agg_script_path = f"{sql_tasks_path}/aggregate_results.sql"
            agg_sql_script = textwrap.dedent(f"""\
                -- DataPact Aggregation Task
                -- This task verifies that all upstream tasks have successfully logged their results.
                SELECT
                  CASE
                    WHEN
                      (
                        SELECT
                          COUNT(*)
                        FROM
                          `{results_table}`
                        WHERE
                          run_id = :run_id
                          AND result_payload:overall_validation_passed = FALSE
                      ) = 0
                    THEN
                      'All validation tasks succeeded.'
                    ELSE
                        RAISE ERROR(
                          'Aggregation check failed: One or more validation tasks failed.'
                        )
                  END AS validation_status;
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

        tasks_list = []
        validation_task_keys = [v_conf["task_key"] for v_conf in config["validations"]]

        for task_key in validation_task_keys:
            tasks_list.append({
                "task_key": task_key,
                "sql_task": {
                    "file": {
                        "path": task_paths[task_key],
                        "source": "WORKSPACE"
                    },
                    "warehouse_id": warehouse.id,
                }
            })

        if results_table and 'aggregate_results' in task_paths:
            tasks_list.append({
                "task_key": "aggregate_results",
                "depends_on": [{"task_key": tk} for tk in validation_task_keys],
                "run_if": "ALL_DONE",
                "sql_task": {
                    "file": {
                        "path": task_paths['aggregate_results'],
                        "source": "WORKSPACE"
                    },
                    "warehouse_id": warehouse.id,
                }
            })

        job_settings_dict = {
            "name": job_name,
            "tasks": tasks_list,
            "run_as": {"user_name": self.user_name},
            "parameters": [
                {
                    "name": "run_id",
                    "default": "{{job.run_id}}",
                },
            ],
        }

        existing_job = None
        for j in self.w.jobs.list(name=job_name):
            existing_job = j
            break
        
        if existing_job:
            logger.info(f"Updating existing job '{job_name}' (ID: {existing_job.job_id})...")
            job_settings_obj = jobs.JobSettings.from_dict(job_settings_dict)
            self.w.jobs.reset(job_id=existing_job.job_id, new_settings=job_settings_obj)
            job_id = existing_job.job_id
        else:
            logger.info(f"Creating new job '{job_name}'...")
            new_job = self.w.jobs.create(**job_settings_dict)
            job_id = new_job.job_id

        logger.info(f"Launching job {job_id}...")
        run_info = self.w.jobs.run_now(job_id=job_id)
        run = self.w.jobs.get_run(run_info.run_id)
        
        logger.info(f"Run started! View progress here: {run.run_page_url}")
        while run.state.life_cycle_state not in TERMINAL_STATES:
            time.sleep(20)
            run = self.w.jobs.get_run(run.run_id)
            finished_tasks = sum(1 for t in run.tasks if t.state.life_cycle_state == RunLifeCycleState.TERMINATED)
            logger.info(f"Job state: {run.state.life_cycle_state}. Tasks finished: {finished_tasks}/{len(task_keys)}")

        final_state = run.state.result_state
        logger.info(f"Run finished with state: {final_state}")
        if final_state == jobs.RunResultState.SUCCESS:
            logger.success("✅ DataPact job completed successfully.")
        else:
            raise Exception(f"DataPact job did not succeed. Final state: {final_state}. View details at {run.run_page_url}")
