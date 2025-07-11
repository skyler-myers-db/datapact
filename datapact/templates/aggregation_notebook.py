import json
from typing import List, Dict, Any
from databricks.sdk.runtime import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, LongType, MapType
from loguru import logger

logger.add(lambda msg: print(msg, end=""), format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | - {message}")

# --- Parameters ---
dbutils.widgets.text("upstream_task_keys", "[]", "JSON list of upstream task keys")
dbutils.widgets.text("results_table", "", "Full 3-level name of the Delta table for results (e.g. catalog.schema.table)")
dbutils.widgets.text("run_id", "", "The Databricks Job Run ID")

upstream_task_keys_str: str = dbutils.widgets.get("upstream_task_keys")
results_table: str = dbutils.widgets.get("results_table")
run_id: str = dbutils.widgets.get("run_id")
upstream_task_keys: List[str] = json.loads(upstream_task_keys_str)

logger.info(f"Aggregating results from {len(upstream_task_keys)} tasks for Run ID: {run_id}.")

# --- Data Collection ---
all_results: List[Dict[str, Any]] = []
failed_tasks: List[str] = []

for task_key in upstream_task_keys:
    try:
        result_str = dbutils.jobs.taskValues.get(taskKey=task_key, key="summary")
        result = json.loads(result_str)
        all_results.append(result)
        if not result.get("overall_validation_passed", False):
            failed_tasks.append(task_key)
    except Exception as e:
        logger.error(f"Could not retrieve result for task '{task_key}': {e}")
        failed_tasks.append(task_key)
        all_results.append({"task_key": task_key, "overall_validation_passed": False, "error": str(e)})

# --- Final Report Logging ---
logger.info("\n" + "="*60)
logger.info("           DataPact - Final Validation Report")
logger.info("="*60)
logger.info(f"Total Tasks: {len(all_results)}")
logger.info(f"✅ Successful: {len(all_results) - len(failed_tasks)}")
logger.info(f"❌ Failed: {len(failed_tasks)}")
if failed_tasks:
    logger.error(f"Failed Task Keys: {', '.join(failed_tasks)}")
logger.info("="*60)

# --- Write to Delta Lake ---
if results_table:
    logger.info(f"Writing {len(all_results)} results to Delta table: {results_table}")
    try:
        spark = SparkSession.builder.appName("DataPact-Aggregation").getOrCreate()
        
        # Flatten the metrics for easier querying
        flat_results = []
        for result in all_results:
            base_info = {
                "run_id": run_id,
                "task_key": result.get("task_key"),
                "overall_validation_passed": result.get("overall_validation_passed"),
                "execution_timestamp_utc": spark.sql("SELECT current_timestamp()").first()[0]
            }
            # Add all other metrics from the result dict
            base_info.update(result)
            flat_results.append(base_info)

        results_df = spark.createDataFrame(flat_results)
        results_df.write.format("delta").mode("append").saveAsTable(results_table)
        logger.info("Successfully wrote results to Delta table.")
    except Exception as e:
        logger.critical(f"Failed to write results to Delta table {results_table}: {e}")
        # Do not fail the job for a reporting failure, but log it as critical.

if failed_tasks:
    raise Exception("One or more DataPact validations failed. Check the logs for details.")
else:
    logger.success("All DataPact validations passed successfully!")
