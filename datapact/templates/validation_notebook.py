"""
DataPact Validation Task Notebook.

IMPORTANT: This script is not intended to be run locally. It is uploaded to
Databricks and executed as a notebook task on Serverless Compute by a
Databricks Job.

This script represents the "brain" for a single table validation. It is
parameterized via job widgets (`config_json`, `databricks_host`, etc.).

Its responsibilities are:
1.  Parse the incoming JSON configuration for a specific table validation.
2.  Connect to the specified Serverless SQL Warehouse using the Databricks SQL Connector.
3.  Execute the full suite of validation logic (count, aggregates, hash, nulls)
    by dynamically building and sending SQL queries to the warehouse.
4.  Log the results of each check.
5.  Package a final JSON summary of all metrics.
6.  Pass this summary to the downstream aggregation task using `dbutils.jobs.taskValues.set`.
"""

# This command is executed by Databricks before any other code in this notebook.
%pip install databricks-sql-connector loguru

import json
import os
from typing import Any, Dict, List, Tuple, Optional

from databricks import sql
from databricks.sdk.runtime import *
from loguru import logger

# --- 1. Configuration & Setup ---
logger.add(lambda msg: print(msg, end=""), format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | - {message}")

# Define widgets to receive parameters from the orchestrator.
dbutils.widgets.text("config_json", "{}", "Table Validation Config JSON")
dbutils.widgets.text("databricks_host", "", "Databricks Host")
dbutils.widgets.text("sql_warehouse_http_path", "", "Databricks SQL Warehouse HTTP Path")

# Get parameters from widgets
config_str: str = dbutils.widgets.get("config_json")
databricks_host: str = dbutils.widgets.get("databricks_host")
sql_warehouse_http_path: str = dbutils.widgets.get("sql_warehouse_http_path")

if not all([config_str, databricks_host, sql_warehouse_http_path]):
    raise ValueError("One or more required parameters (config_json, databricks_host, sql_warehouse_http_path) are missing.")

params: Dict[str, Any] = json.loads(config_str)
task_key: str = params["task_key"]
logger.info(f"--- Starting Validation for Task: {task_key} ---")

# Extract and type-check all parameters
source_catalog: str = params["source_catalog"]
source_schema: str = params["source_schema"]
source_table: str = params["source_table"]
target_catalog: str = params["target_catalog"]
target_schema: str = params["target_schema"]
target_table: str = params["target_table"]
primary_keys: Optional[List[str]] = params.get("primary_keys")
count_tolerance: float = float(params.get("count_tolerance", 0.0))
pk_row_hash_check: bool = params.get("pk_row_hash_check", False)
pk_hash_threshold: Optional[float] = float(params["pk_hash_threshold"]) if params.get("pk_hash_threshold") is not None else None
hash_columns: Optional[List[str]] = params.get("hash_columns")
agg_validations: Optional[Dict[str, Any]] = params.get("agg_validations")
null_validation_threshold: Optional[float] = float(params["null_validation_threshold"]) if params.get("null_validation_threshold") is not None else None
null_validation_columns: Optional[List[str]] = params.get("null_validation_columns")

source_fqn = f"{source_catalog}.{source_schema}.{source_table}"
target_fqn = f"{target_catalog}.{target_schema}.{target_table}"
logger.info(f"Source: {source_fqn} | Target: {target_fqn}")

# --- 2. Database Connection Helper ---
def execute_query(query: str) -> List[Dict[str, Any]]:
    """Executes a SQL query on the configured Serverless Warehouse and returns results."""
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    try:
        with sql.connect(
            server_hostname=databricks_host,
            http_path=sql_warehouse_http_path,
            token=token,
        ) as connection:
            with connection.cursor() as cursor:
                logger.debug(f"Executing query: {query}")
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Query execution failed for query: {query}")
        logger.error(f"Error: {e}")
        raise

# --- 3. Validation Logic ---
validation_passed = True
metrics: Dict[str, Any] = {"task_key": task_key}

try:
    # === Count Validation ===
    logger.info("Starting Count Validation...")
    source_count = execute_query(f"SELECT COUNT(1) as cnt FROM {source_fqn}")[0]['cnt']
    target_count = execute_query(f"SELECT COUNT(1) as cnt FROM {target_fqn}")[0]['cnt']
    rel_diff = 0 if source_count == 0 else abs(target_count - source_count) / source_count
    
    metrics["source_count"] = source_count
    metrics["target_count"] = target_count
    metrics["count_relative_diff_percent"] = rel_diff * 100
    metrics["count_validation_passed"] = count_passed = rel_diff <= count_tolerance

    if not count_passed:
        validation_passed = False
        logger.error(f"❌ COUNT FAILED: Diff {rel_diff*100:.2f}% > Tolerance {count_tolerance*100:.2f}%")
    else:
        logger.success("✅ Count Validation Passed.")

    # === Aggregated Validations ===
    if agg_validations:
        logger.info("Starting Aggregated Validations...")
        for col, validations in agg_validations.items():
            for v in validations:
                agg_func = v["agg"]
                tolerance = float(v["tolerance"])
                
                source_agg_val = execute_query(f"SELECT {agg_func}({col}) as val FROM {source_fqn}")[0]['val']
                target_agg_val = execute_query(f"SELECT {agg_func}({col}) as val FROM {target_fqn}")[0]['val']

                if source_agg_val is None or target_agg_val is None:
                    agg_passed = source_agg_val is None and target_agg_val is None
                    rel_diff_agg = 0 if agg_passed else 1.0
                elif source_agg_val == 0:
                    rel_diff_agg = 0 if target_agg_val == 0 else 1.0
                else:
                    rel_diff_agg = abs(float(target_agg_val) - float(source_agg_val)) / abs(float(source_agg_val))
                
                agg_passed = rel_diff_agg <= tolerance
                metric_key = f"agg_{col}_{agg_func}"
                metrics[f"{metric_key}_source_val"] = source_agg_val
                metrics[f"{metric_key}_target_val"] = target_agg_val
                metrics[f"{metric_key}_relative_diff_percent"] = rel_diff_agg * 100
                metrics[f"{metric_key}_validation_passed"] = agg_passed

                if not agg_passed:
                    validation_passed = False
                    logger.error(f"❌ AGG FAILED ({col}/{agg_func}): Diff {rel_diff_agg*100:.2f}% > Tolerance {tolerance*100:.2f}%")
                else:
                    logger.success(f"✅ Agg Validation Passed for {col}/{agg_func}.")

    # === Per-Row Hash Comparison (Primary Key based) ===
    if pk_row_hash_check and primary_keys:
        logger.info("Starting Per-Row Hash Validation...")
        join_conditions = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in primary_keys])
        
        # Use all columns if hash_columns is not specified
        if not hash_columns:
            source_cols_query = f"SELECT * FROM {source_fqn} LIMIT 1"
            cols_result = execute_query(source_cols_query)
            if not cols_result:
                 logger.warning("Source table is empty, skipping row hash check.")
                 hash_columns_to_use = []
            else:
                 hash_columns_to_use = list(cols_result[0].keys())
        else:
            hash_columns_to_use = hash_columns

        if hash_columns_to_use:
            hash_expr = f"md5(to_json(struct({', '.join([f'`{c}`' for c in hash_columns_to_use])})))"
            
            mismatch_query = f"""
                SELECT
                    COUNT(1) AS total_compared_rows,
                    SUM(CASE WHEN s.row_hash <> t.row_hash THEN 1 ELSE 0 END) AS mismatch_count
                FROM
                    (SELECT {', '.join(primary_keys)}, {hash_expr} as row_hash FROM {source_fqn}) s
                INNER JOIN
                    (SELECT {', '.join(primary_keys)}, {hash_expr} as row_hash FROM {target_fqn}) t
                ON {join_conditions}
            """
            hash_results = execute_query(mismatch_query)[0]
            total_rows = hash_results["total_compared_rows"]
            mismatch_count = hash_results["mismatch_count"]
            
            ratio = 0 if total_rows == 0 else mismatch_count / float(total_rows)
            threshold = pk_hash_threshold if pk_hash_threshold is not None else 0.0
            hash_passed = ratio <= threshold

            metrics["per_row_hash_compared_rows"] = total_rows
            metrics["per_row_hash_mismatches"] = mismatch_count
            metrics["per_row_hash_mismatch_ratio_percent"] = ratio * 100
            metrics["per_row_hash_validation_passed"] = hash_passed

            if not hash_passed:
                validation_passed = False
                logger.error(f"❌ HASH FAILED: Mismatch ratio {ratio*100:.2f}% > Threshold {threshold*100:.2f}%")
            else:
                logger.success("✅ Per-Row Hash Validation Passed.")

    # === Null Count Validation ===
    if null_validation_threshold is not None and null_validation_columns:
        logger.info("Starting Null Count Validation...")
        # This implementation compares null counts on a per column basis
        for col in null_validation_columns:
            source_null_q: str = f"SELECT COUNT(1) as cnt FROM {source_fqn} WHERE `{col}` IS NULL"
            target_null_q: str = f"SELECT COUNT(1) as cnt FROM {target_fqn} WHERE `{col}` IS NULL"
            
            source_nulls: int = execute_query(source_null_q)[0]['cnt']
            target_nulls: int = execute_query(target_null_q)[0]['cnt']

            if source_nulls == 0:
                # If source has no nulls, the relative difference is effectively infinite
                # if the target has any nulls. It only passes if the target also has zero.
                null_rel_diff: float = 0.0 if target_nulls == 0 else float('inf')
            else:
                null_rel_diff: float = abs(target_nulls - source_nulls) / float(source_nulls)

            null_passed: bool = null_rel_diff <= null_validation_threshold

            metric_key = f"null_count_{col}"
            metrics[f"{metric_key}_source_nulls"] = source_nulls
            metrics[f"{metric_key}_target_nulls"] = target_nulls
            metrics[f"{metric_key}_relative_diff_percent"] = null_rel_diff * 100
            metrics[f"{metric_key}_validation_passed"] = null_passed

            if not null_passed:
                validation_passed: bool = False
                logger.error(f"❌ NULL COUNT FAILED ({col}): Diff {null_rel_diff*100:.2f}% > Threshold {null_validation_threshold*100:.2f}%")
            else:
                logger.success(f"✅ Null Count Validation Passed for {col}.")

except Exception as e:
    logger.critical(f"A critical error occurred during validation for task {task_key}: {e}")
    validation_passed = False
    metrics["error"] = str(e)

# --- 4. Finalize and Return Results ---
metrics["overall_validation_passed"] = validation_passed
logger.info(f"--- Final Summary for {task_key} ---")
logger.info(json.dumps(metrics, indent=2, default=str))

# This makes the result available to the downstream aggregation task
dbutils.jobs.taskValues.set(key="summary", value=json.dumps(metrics, default=str))

if not validation_passed:
    raise Exception(f"Validation failed for {task_key}. See logs for details.")
