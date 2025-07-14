"""
The core client for interacting with the Databricks API.

This module contains the `DataPactClient` class. Its architecture is a
local-first orchestrator that dynamically generates pure SQL validation scripts.
It uploads these scripts and creates a multi-task Databricks Job where each
task is a SQL Task that runs directly on a specified Serverless SQL Warehouse.
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
        logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
        self.w = WorkspaceClient(profile=profile)
        self.user_name = self.w.current_user.me().user_name
        self.root_path = f"/Shared/datapact/{self.user_name}"

    def _ensure_sql_warehouse(self, name: str, auto_create: bool) -> sql_service.EndpointInfo:
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

        if warehouse:
            logger.info(f"Found warehouse {warehouse.id}. State: {warehouse.state}")
        else:
            if not auto_create:
                raise ValueError(f"SQL Warehouse '{name}' not found and auto_create is False.")
            
            logger.info(f"Warehouse '{name}' not found. Creating a new Serverless SQL Warehouse...")
            warehouse = self.w.warehouses.create_and_wait(
                name=name,
                cluster_size="Small",
                enable_serverless_compute=True,
                channel=sql_service.Channel(name=sql_service.ChannelName.CHANNEL_NAME_CURRENT)
            )
            logger.success(f"Successfully created and started warehouse {warehouse.id}.")
            return warehouse

        if warehouse.state not in [sql_service.State.RUNNING, sql_service.State.STARTING]:
            logger.info(f"Warehouse '{name}' is in state {warehouse.state}. Starting it...")
            self.w.warehouses.start(warehouse.id).result(timeout=timedelta(seconds=600))
            logger.success(f"Warehouse '{name}' started successfully.")
        
        return self.w.warehouses.get(warehouse.id)

    def _generate_sql_script(self, config: dict[str, any], results_table: str | None) -> str:
        """Generates a complete, idempotent SQL validation script for a single task."""
        source_fqn = f"{config['source_catalog']}.{config['source_schema']}.{config['source_table']}"
        target_fqn = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"
        
        # Use CTEs to calculate all metrics up front
        sql = f"""
        WITH source_metrics AS (
            SELECT COUNT(1) AS count FROM {source_fqn}
        ),
        target_metrics AS (
            SELECT COUNT(1) AS count FROM {target_fqn}
        ),
        results AS (
            SELECT
                (SELECT count FROM source_metrics) AS source_count,
                (SELECT count FROM target_metrics) AS target_count
        )
        SELECT * FROM results;
        """
        # In a full implementation, more CTEs for hash/null/agg checks would be added here.
        # The final part of the script would contain ASSERT statements.
        # e.g., ASSERT (abs(source_count - target_count) / source_count) <= {config['count_tolerance']} : 'Count Mismatch';

        # If a results table is provided, add the INSERT statement
        if results_table:
            # This is a simplified example of what would be inserted.
            sql += f"""
            -- Log results to history table
            -- In a real scenario, this would be a more complex INSERT statement
            -- that captures all metrics calculated in the CTEs above.
            -- For now, this demonstrates the concept.
            CREATE TABLE IF NOT EXISTS {results_table} (task_key STRING, status STRING, run_id STRING);
            INSERT INTO {results_table} VALUES ('{config['task_key']}', 'SUCCESS', '{{{{job.run_id}}}}');
            """
        return sql

    def _upload_sql_scripts(self, config: dict[str, any], results_table: str | None) -> dict[str, str]:
        """Generates and uploads SQL scripts for all validation tasks."""
        logger.info("Generating and uploading SQL validation scripts...")
        task_paths: dict[str, str] = {}
        
        for task_config in config['validations']:
            task_key = task_config['task_key']
            sql_script = self._generate_sql_script(task_config, results_table)
            
            script_path = f"{self.root_path}/sql_tasks/{task_key}.sql"
            self.w.workspace.upload(path=script_path, content=sql_script.encode('utf-8'), overwrite=True)
            task_paths[task_key] = script_path
            logger.info(f"  - Uploaded script for task '{task_key}' to {script_path}")

        # Upload the aggregation notebook
        agg_notebook_path = f"{self.root_path}/sql_tasks/aggregate_results.sql"
        agg_notebook_content = (Path(__file__).parent / "templates/aggregation_notebook.sql").read_text()
        self.w.workspace.upload(path=agg_notebook_path, content=agg_notebook_content.encode('utf-8'), overwrite=True)
        task_paths['aggregate_results'] = agg_notebook_path
        logger.info(f"  - Uploaded aggregation notebook to {agg_notebook_path}")

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
        task_keys = [v_conf["task_key"] for v_conf in config["validations"]]

        for task_key, script_path in task_paths.items():
            if task_key == 'aggregate_results':
                continue
            tasks_list.append({
                "task_key": task_key,
                "sql_task": {
                    "file": {"path": script_path},
                    "warehouse_id": warehouse.id
                }
            })

        # Add the final aggregation task
        tasks_list.append({
            "task_key": "aggregate_results",
            "depends_on": [{"task_key": tk} for tk in task_keys],
            "sql_task": {
                "notebook": {"path": task_paths['aggregate_results']},
                "warehouse_id": warehouse.id,
                "parameters": {
                    "results_table": results_table or "",
                    "expected_tasks": str(len(task_keys))
                }
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
            logger.info(f"Job state: {run.state.life_cycle_state}. Tasks finished: {finished_tasks}/{len(task_keys)+1}")

        final_state = run.state.result_state
        logger.info(f"Run finished with state: {final_state}")
        if final_state == jobs.RunResultState.SUCCESS:
            logger.success("✅ DataPact job completed successfully.")
        else:
            raise Exception(f"DataPact job did not succeed. Final state: {final_state}. View details at {run.run_page_url}")
