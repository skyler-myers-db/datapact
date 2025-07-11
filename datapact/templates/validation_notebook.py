# datapact/templates/validation_notebook.py
import json
import os
from databricks import sql
from databricks.sdk.runtime import *

# --- 1. Get Parameters ---
# The job definition will pass the table specific config as a parameter.
config_str = dbutils.widgets.get("config_json")
params = json.loads(config_str)
task_key = params["task_key"]
print(f"--- Starting Validation for: {task_key} ---")

# --- 2. Connect to Serverless SQL Warehouse ---
# The notebook task will run on a Job Cluster, but it connects to the SQL Warehouse for queries.
def execute_query(query_string):
    # When running inside Databricks, we can authenticate automatically.
    with sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        # The token is automatically available to the notebook.
        token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query_string)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

# --- 3. The Validation Logic ---

# Example for count validation:
metrics = {"task_key": task_key}
validation_passed = True
source_fqn = f"{params['source_catalog']}.{params['source_schema']}.{params['source_table']}"
target_fqn = f"{params['target_catalog']}.{params['target_schema']}.{params['target_table']}"
source_count = execute_query(f"SELECT COUNT(*) as cnt FROM {source_fqn}")[0]['cnt']
target_count = execute_query(f"SELECT COUNT(*) as cnt FROM {target_fqn}")[0]['cnt']
# ... etc ...

# --- 4. Return Results via Task Values ---
metrics["overall_validation_passed"] = validation_passed
print(f"--- Final Summary for {task_key} ---")
print(json.dumps(metrics, indent=2))

# This makes the result available to the downstream aggregation task
dbutils.jobs.taskValues.set(key="summary", value=json.dumps(metrics))

if not validation_passed:
    raise Exception(f"Validation failed for {task_key}.")
