"""
The core client for interacting with the Databricks API.

This module contains the `DataPactClient` class, which encapsulates all the
programmatic logic for managing and executing DataPact workflows. It is the
engine room of the accelerator, responsible for:

1.  Authenticating with the Databricks workspace using the SDK.
2.  Uploading the necessary task notebooks to a user-specific path.
3.  Programmatically ensuring the specified Serverless SQL Warehouse exists and is running.
4.  Dynamically constructing a multi-task Databricks Job definition in memory based
    on the user's YAML configuration.
5.  Submitting the job to the Databricks API, either creating a new job or updating
    an existing one (idempotency).
6.  Monitoring the job run until it reaches a terminal state and reporting the outcome.
"""

import json
import time
from pathlib import Path
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, sql as sql_service
from databricks.sdk.service.jobs import RunLifeCycleState
from loguru import logger

# Define the states that indicate a job run has finished.
TERMINAL_STATES: list[RunLifeCycleState] = [
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR
]

class DataPactClient:
    """
    A client to programmatically manage and run DataPact validation workflows.
    """

    def __init__(self, profile: str = "DEFAULT") -> None:
        """Initializes the client with Databricks workspace credentials."""
        logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
        self.w = WorkspaceClient(profile=profile)
        self.root_path = f"/Shared/datapact/{self.w.current_user.me().user_name}"
        logger.info(f"Using workspace path: {self.root_path}")

    def _upload_notebooks(self) -> None:
        # ... (implementation is correct) ...

    def _ensure_sql_warehouse(self, name: str, auto_create: bool) -> sql_service.EndpointInfo:
        # ... (implementation is correct) ...

    def run_validation(
        self,
        config: dict[str, any],
        job_name: str,
        warehouse_name: str,
        create_warehouse: bool,
        results_table: str | None = None,
    ) -> None:
        """Constructs, deploys, and runs the DataPact validation workflow."""
        self._upload_notebooks()
        warehouse = self._ensure_sql_warehouse(warehouse_name, create_warehouse)

        tasks_list = []
        task_keys = [v_conf["task_key"] for v_conf in config["validations"]]

        for v_conf in config["validations"]:
            tasks_list.append({
                "task_key": v_conf["task_key"],
                "notebook_task": {
                    "notebook_path": f"{self.root_path}/validation_notebook.py",
                    "base_parameters": {
                        "config_json": json.dumps(v_conf),
                        "databricks_host": self.w.config.host,
                        "sql_warehouse_http_path": warehouse.odbc_params.path,
                    },
                },
            })

        tasks_list.append({
            "task_key": "aggregate_results",
            "depends_on": [{"task_key": tk} for tk in task_keys],
            "notebook_task": {
                "notebook_path": f"{self.root_path}/aggregation_notebook.py",
                "base_parameters": {
                    "upstream_task_keys": json.dumps(task_keys),
                    "results_table": results_table or "",
                    "run_id": "{{job.run_id}}",
                },
            },
        })

        # The 'libraries' key is NOT SUPPORTED for serverless notebook tasks
        job_settings_dict = {
            "name": job_name,
            "tasks": tasks_list,
            "run_as": {"user_name": self.w.current_user.me().user_name},
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
