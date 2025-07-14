"""
The core client for interacting with the Databricks API.

This module contains the `DataPactClient` class. Its architecture is a
local-first orchestrator that dynamically generates pure SQL validation scripts
in memory. It creates a multi-task Databricks Job where each task is a SQL Task
defined by a raw query string, running directly on a Serverless SQL Warehouse.
This file-less approach is the definitive and most robust architecture.
"""

import json
import time
from pathlib import Path
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, sql as sql_service
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
        
        # Use ASSERT statements for validation. The task will fail if any assertion is false.
        count_tolerance = config.get('count_tolerance', 0.0)
        
        sql = f"""
        -- DataPact Validation for task: {config['task_key']}
        -- This script uses ASSERT statements to validate data properties.
        -- The entire task will fail if any single assertion returns false.

        CREATE OR REPLACE TEMP VIEW source_metrics AS SELECT COUNT(1) AS count FROM {source_fqn};
        CREATE OR REPLACE TEMP VIEW target_metrics AS SELECT COUNT(1) AS count FROM {target_fqn};

        ASSERT ((abs((SELECT count FROM target_metrics) - (SELECT count FROM source_metrics)) / (SELECT count FROM source_metrics)) <= {count_tolerance}) : 'Count validation failed.';

        -- Additional validations (hash, nulls, etc.) would be added as more ASSERT statements here.
        """

        if results_table:
            sql += f"""
            -- If all assertions pass, log success to the history table.
            CREATE TABLE IF NOT EXISTS {results_table} (task_key STRING, status STRING, run_id STRING);
            INSERT INTO {results_table} VALUES ('{config['task_key']}', 'SUCCESS', '{{{{job.run_id}}}}');
            """
        
        sql += f"\nSELECT 'Task {config['task_key']} completed successfully.' AS final_status;"
        return sql

    def run_validation(
        self,
        config: dict[str, any],
        job_name: str,
        warehouse_name: str,
        results_table: str | None = None,
    ) -> None:
        """Constructs, deploys, and runs the DataPact validation workflow."""
        warehouse = self._ensure_sql_warehouse(warehouse_name)

        tasks_list = []
        task_keys = [v_conf["task_key"] for v_conf in config["validations"]]

        for task_config in config['validations']:
            task_key = task_config['task_key']
            sql_script = self._generate_validation_sql(task_config, results_table)
            
            tasks_list.append({
                "task_key": task_key,
                "sql_task": {
                    "query": {"query": sql_script},
                    "warehouse_id": warehouse.id
                }
            })

        job_settings_dict = {
            "name": job_name,
            "tasks": tasks_list,
            "run_as": {"user_name": self.user_name},
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
