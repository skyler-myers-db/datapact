import json
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs, compute
from loguru import logger

class DataPactClient:
    """
    A client to programmatically manage and run DataPact validation workflows on Databricks.
    """

    def __init__(self, profile: str = "DEFAULT"):
        logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
        self.w = WorkspaceClient(profile=profile)
        self.root_path = f"/Shared/datapact/{self.w.currentUser.me().workspace_user_name}"
        logger.info(f"Using workspace path: {self.root_path}")

    def _upload_notebooks(self) -> None:
        """Uploads the validation and aggregation notebooks to the Databricks workspace."""
        logger.info(f"Uploading notebooks to {self.root_path}...")
        templates_dir = Path(__file__).parent / "templates"
        self.w.workspace.mkdirs(self.root_path)
        for notebook_file in templates_dir.glob("*.py"):
            with open(notebook_file, "rb") as f:
                self.w.workspace.upload(
                    path=f"{self.root_path}/{notebook_file.name}",
                    content=f.read(),
                    overwrite=True,
                )
        logger.info("Notebooks uploaded successfully.")

    def _ensure_sql_warehouse(self, name: str, auto_create: bool) -> compute.EndpointInfo:
        """
        Ensures a Serverless SQL Warehouse with the given name exists and is running.

        Args:
            name: The name of the SQL Warehouse.
            auto_create: If True, creates the warehouse if it doesn't exist.

        Returns:
            The EndpointInfo object for the running warehouse.
        
        Raises:
            ValueError: If the warehouse doesn't exist and auto_create is False.
            TimeoutError: If the warehouse fails to start in time.
        """
        try:
            logger.info(f"Looking for SQL Warehouse '{name}'...")
            warehouse = self.w.warehouses.get_by_name(name)
            logger.info(f"Found warehouse {warehouse.id}. State: {warehouse.state}")
        except NotFound:
            if not auto_create:
                raise ValueError(f"SQL Warehouse '{name}' not found and auto_create is False.")
            logger.info(f"Warehouse '{name}' not found. Creating a new Serverless SQL Warehouse...")
            warehouse = self.w.warehouses.create_and_wait(
                name=name,
                cluster_size="Small",
                enable_serverless_compute=True,
                channel=compute.Channel(name=compute.ChannelName.CHANNEL_NAME_CURRENT)
            )
            logger.info(f"Successfully created and started warehouse {warehouse.id}.")
            return warehouse

        if warehouse.state not in [compute.State.RUNNING, compute.State.STARTING]:
            logger.info(f"Warehouse '{name}' is in state {warehouse.state}. Starting it...")
            self.w.warehouses.start(warehouse.id).result()
            logger.info(f"Warehouse '{name}' started successfully.")
        
        return self.w.warehouses.get(warehouse.id)

    def run_validation(
        self,
        config: dict[str, any],
        job_name: str,
        warehouse_name: str,
        create_warehouse: bool,
        results_table: str | None = None,
    ) -> None:
        """
        Constructs, deploys, and runs the DataPact validation workflow.
        """
        self._upload_notebooks()
        warehouse = self._ensure_sql_warehouse(warehouse_name, create_warehouse)

        validation_tasks: list[jobs.Task] = []
        task_keys = [v_conf["task_key"] for v_conf in config["validations"]]

        dbr_version = self.w.clusters.select_spark_version(long_term_support=True, serverless=True)
        logger.info(f"Using Serverless DBR version for orchestration: {dbr_version}")

        for v_conf in config["validations"]:
            validation_tasks.append(jobs.Task(
                task_key=v_conf["task_key"],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{self.root_path}/validation_notebook.py",
                    base_parameters={
                        "config_json": json.dumps(v_conf),
                        "databricks_host": self.w.config.host,
                        "sql_warehouse_http_path": warehouse.odbc_params.path,
                    },
                ),
            ))

        aggregation_task = jobs.Task(
            task_key="aggregate_results",
            depends_on=[jobs.TaskDependency(task_key=tk) for tk in task_keys],
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{self.root_path}/aggregation_notebook.py",
                base_parameters={
                    "upstream_task_keys": json.dumps(task_keys),
                    "results_table": results_table or "",
                    "run_id": "{{job.run_id}}",
                },
            ),
        )

        job_settings = jobs.JobSettings(
            name=job_name,
            tasks=validation_tasks + [aggregation_task],
            spark_version=dbr_version,
            run_as=jobs.JobRunAs(user_name=self.w.currentUser.me().user_name),
        )

        try:
            existing_job = self.w.jobs.get_by_name(job_name)
            logger.info(f"Updating existing job '{job_name}' (ID: {existing_job.job_id})...")
            self.w.jobs.reset(job_id=existing_job.job_id, new_settings=job_settings.as_dict())
            job_id = existing_job.job_id
        except NotFound:
            logger.info(f"Creating new job '{job_name}'...")
            new_job = self.w.jobs.create(**job_settings.as_dict())
            job_id = new_job.job_id

        logger.info(f"Launching job {job_id}...")
        run = self.w.jobs.run_now(job_id=job_id).result(timeout=3600)
        
        logger.info(f"Run started! View progress here: {run.run_page_url}")
        while not run.is_terminal:
            time.sleep(20)
            run = self.w.jobs.get_run(run.run_id)
            finished_tasks = sum(1 for t in run.tasks if t.state.life_cycle_state == jobs.RunLifeCycleState.TERMINATED)
            logger.info(f"Job state: {run.state.life_cycle_state}. Tasks finished: {finished_tasks}/{len(task_keys)+1}")

        final_state = run.state.result_state
        logger.info(f"Run finished with state: {final_state}")
        if final_state != jobs.RunResultState.SUCCESS:
            raise Exception(f"DataPact job failed with state {final_state}. View details at {run.run_page_url}")
        logger.success("DataPact job completed successfully.")
