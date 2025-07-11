# datapact/client.py
import json
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask

class DataPactClient:
    def __init__(self, profile: str = "DEFAULT"):
        self.w = WorkspaceClient(profile=profile)
        self.root_path = f"/Shared/datapact/{self.w.currentUser.me().workspace_user_name}"

    def _upload_notebooks(self):
        print(f"Uploading notebooks to {self.root_path}...")
        templates_dir = Path(__file__).parent / "templates"
        for notebook_file in templates_dir.glob("*.py"):
            with open(notebook_file, "rb") as f:
                self.w.workspace.upload(
                    path=f"{self.root_path}/{notebook_file.name}",
                    content=f.read(),
                    overwrite=True,
                )
    
    def _ensure_sql_warehouse(self, warehouse_name: str):
        # This is a simplified version. A robust implementation would check for existence first.
        print(f"Ensuring Serverless SQL Warehouse '{warehouse_name}' exists...")
        # In a real scenario, you'd use w.warehouses.get_by_name and create if not found.
        # For this example, we assume it exists and just get its details.
        warehouse = self.w.warehouses.get_by_name(warehouse_name)
        return warehouse.id

    def run_validation(self, config: dict, job_name: str, warehouse_name: str):
        self._upload_notebooks()
        sql_warehouse_id = self._ensure_sql_warehouse(warehouse_name)

        validation_tasks = []
        task_keys = []

        for v_conf in config["validations"]:
            task_key = v_conf["task_key"]
            task_keys.append(task_key)
            validation_tasks.append(
                Task(
                    task_key=task_key,
                    notebook_task=NotebookTask(
                        notebook_path=f"{self.root_path}/validation_notebook.py",
                        base_parameters={"config_json": json.dumps(v_conf)},
                    ),
                    # Each task runs on a small, ephemeral job cluster
                    # but connects to the SQL warehouse for queries.
                    job_cluster_key="datapact_cluster",
                )
            )

        aggregation_task = Task(
            task_key="aggregate_results",
            depends_on=[{"task_key": tk} for tk in task_keys],
            notebook_task=NotebookTask(
                notebook_path=f"{self.root_path}/aggregation_notebook.py",
                base_parameters={"upstream_task_keys": json.dumps(task_keys)},
            ),
            job_cluster_key="datapact_cluster",
        )

        # Find if the job already exists
        existing_job = self.w.jobs.get_by_name(job_name)
        
        job_definition = {
            "name": job_name,
            "tasks": validation_tasks + [aggregation_task],
            "job_clusters": [
                {
                    "job_cluster_key": "datapact_cluster",
                    "new_cluster": {
                        "spark_version": self.w.clusters.select_spark_version(long_term_support=True),
                        "node_type_id": self.w.clusters.select_node_type(local_disk=True),
                        "num_workers": 1,
                    },
                }
            ],
            "parameters": [
                {"name": "DATABRICKS_SERVER_HOSTNAME", "default": self.w.config.host},
                {"name": "DATABRICKS_HTTP_PATH", "default": self.w.warehouses.get_by_name(warehouse_name).odbc_params.path}
            ]
        }

        if existing_job:
            print(f"Updating existing job '{job_name}' (ID: {existing_job.job_id})...")
            self.w.jobs.reset(job_id=existing_job.job_id, new_settings=job_definition)
            job_id = existing_job.job_id
        else:
            print(f"Creating new job '{job_name}'...")
            new_job = self.w.jobs.create(**job_definition)
            job_id = new_job.job_id

        print(f"Launching job {job_id}...")
        run = self.w.jobs.run_now(job_id=job_id).result()
        
        print(f"Run started! View progress here: {run.run_page_url}")
        while not run.is_terminal:
            time.sleep(15)
            run = self.w.jobs.get_run(run.run_id)
            print(f"Job state: {run.state.life_cycle_state}")

        print(f"Run finished with state: {run.state.result_state}")
        if run.state.result_state == "FAILED":
            raise Exception(f"DataPact job failed. View details at {run.run_page_url}")
