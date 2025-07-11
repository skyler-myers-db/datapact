# datapact/templates/aggregation_notebook.py
import json
from databricks.sdk.runtime import *

# Get the task keys of all the upstream validation tasks.
# The CLI will pass this as a parameter.
upstream_task_keys_str = dbutils.widgets.get("upstream_task_keys")
upstream_task_keys = json.loads(upstream_task_keys_str)

print(f"Aggregating results from {len(upstream_task_keys)} tasks.")

all_results = []
failed_tasks = []
successful_tasks = []

# Iterate through the upstream tasks and get their results
for task_key in upstream_task_keys:
    try:
        # Get the value that was set by the upstream task
        result_str = dbutils.jobs.taskValues.get(taskKey=task_key, key="summary")
        result = json.loads(result_str)
        all_results.append(result)
        if result.get("overall_validation_passed"):
            successful_tasks.append(task_key)
        else:
            failed_tasks.append(task_key)
    except Exception as e:
        print(f"Could not retrieve result for task '{task_key}': {e}")
        failed_tasks.append(task_key)

# --- Final Report ---
print("\n" + "="*50)
print("           DataPact - Final Validation Report")
print("="*50)
print(f"Total Tasks: {len(all_results)}")
print(f"✅ Successful: {len(successful_tasks)}")
print(f"❌ Failed: {len(failed_tasks)}")
print("-"*50)

if failed_tasks:
    print("\nFailed Task Details:")
    for task_key in failed_tasks:
        print(f"  - {task_key}")

# We can extend this to write the `all_results` list to a Delta table for historical tracking.
# spark.createDataFrame(all_results).write.mode("append").saveAsTable("validation_reports.datapact_runs")

if failed_tasks:
    raise Exception("One or more DataPact validations failed.")
else:
    print("\nAll DataPact validations passed successfully!")
