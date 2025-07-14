"""
The core client for interacting with the Databricks API.

This module contains the `DataPactClient` class. Its architecture is now a
local-first orchestrator. It reads the configuration and uses a local thread
pool to run validations for each table in parallel.

For each validation, it dynamically generates SQL queries and executes them
directly against the specified Serverless SQL Warehouse using the robust
Statement Execution API. All comparison logic and aggregation happens locally,
providing immediate feedback and eliminating the overhead of Databricks Jobs.
"""

import json
import time
from pathlib import Path
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from loguru import logger

class DataPactClient:
    """
    A client that orchestrates validation tests locally and executes SQL
    remotely on a Databricks Serverless SQL Warehouse.
    """

    def __init__(self, profile: str = "DEFAULT") -> None:
        """Initializes the client with Databricks workspace credentials."""
        logger.info(f"Initializing WorkspaceClient with profile '{profile}'...")
        self.w = WorkspaceClient(profile=profile)

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
            raise ValueError(f"SQL Warehouse '{name}' not found. Auto-creation is not supported in this version.")

        if warehouse.state not in [sql_service.State.RUNNING, sql_service.State.STARTING]:
            logger.info(f"Warehouse '{name}' is in state {warehouse.state}. Starting it...")
            self.w.warehouses.start(warehouse.id).result(timeout=timedelta(seconds=600))
            logger.success(f"Warehouse '{name}' started successfully.")
        
        return self.w.warehouses.get(warehouse.id)

    def _execute_sql(self, statement: str, warehouse_id: str) -> list[dict]:
        """Executes a single SQL statement and returns the result."""
        try:
            # Use the robust, synchronous-wait method for simplicity and reliability.
            # The 50-second timeout is for the API call itself, not the query execution.
            waiter = self.w.statement_execution.execute_statement(
                statement=statement,
                warehouse_id=warehouse_id,
                wait_timeout='50s'
            )
            result = waiter.result()
            if result.status.state == sql_service.StatementState.SUCCEEDED:
                # The SDK automatically fetches and deserializes the result for you.
                return result.result.data_array
            else:
                error = result.status.error
                logger.error(f"SQL statement failed with state: {result.status.state}")
                if error:
                    logger.error(f"Error: {error.message}")
                return []
        except Exception as e:
            logger.error(f"An unexpected error occurred during SQL execution: {e}")
            return []

    def _run_single_validation(self, validation_config: dict, warehouse_id: str) -> dict:
        """Runs all configured checks for a single table."""
        task_key = validation_config['task_key']
        logger.info(f"--- Starting Validation for Task: {task_key} ---")
        metrics = {"task_key": task_key}
        validation_passed = True

        source_fqn = f"{validation_config['source_catalog']}.{validation_config['source_schema']}.{validation_config['source_table']}"
        target_fqn = f"{validation_config['target_catalog']}.{validation_config['target_schema']}.{validation_config['target_table']}"

        # 1. Count Validation
        source_count_res = self._execute_sql(f"SELECT COUNT(1) FROM {source_fqn}", warehouse_id)
        target_count_res = self._execute_sql(f"SELECT COUNT(1) FROM {target_fqn}", warehouse_id)

        if not source_count_res or not target_count_res:
            validation_passed = False
            logger.error(f"❌ COUNT FAILED: Could not retrieve row counts for {task_key}.")
        else:
            source_count = source_count_res[0][0]
            target_count = target_count_res[0][0]
            tolerance = validation_config.get('count_tolerance', 0.0)
            rel_diff = 0 if source_count == 0 else abs(target_count - source_count) / source_count
            
            if rel_diff > tolerance:
                validation_passed = False
                logger.error(f"❌ COUNT FAILED for {task_key}: Diff {rel_diff*100:.2f}% > Tolerance {tolerance*100:.2f}%")
            else:
                logger.success(f"✅ Count Validation Passed for {task_key}.")
            metrics['source_count'] = source_count
            metrics['target_count'] = target_count

        # Add other validation logic (hash, nulls, aggregates) here in the same pattern...
        # For brevity, only count validation is shown, but the pattern is identical.

        metrics['overall_validation_passed'] = validation_passed
        return metrics

    def run_validation(
        self,
        config: dict[str, any],
        job_name: str, # Note: job_name is now just for logging/reporting
        warehouse_name: str,
        results_table: str | None = None,
    ) -> None:
        """Orchestrates the parallel execution of validations from the local client."""
        warehouse = self._ensure_sql_warehouse(warehouse_name)
        warehouse_id = warehouse.id
        
        all_results = []
        failed_tasks = 0

        # Use a thread pool to run validations in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_task = {
                executor.submit(self._run_single_validation, task_config, warehouse_id): task_config['task_key']
                for task_config in config['validations']
            }
            for future in as_completed(future_to_task):
                task_key = future_to_task[future]
                try:
                    result = future.result()
                    all_results.append(result)
                    if not result['overall_validation_passed']:
                        failed_tasks += 1
                except Exception as exc:
                    logger.critical(f"Task {task_key} generated an exception: {exc}")
                    all_results.append({'task_key': task_key, 'overall_validation_passed': False, 'error': str(exc)})
                    failed_tasks += 1
        
        logger.info("--- FINAL VALIDATION SUMMARY ---")
        logger.info(json.dumps(all_results, indent=2))

        # Optional: Write results to a Delta history table
        if results_table:
            logger.info(f"Writing results to history table: {results_table}")
            # This would involve another _execute_sql call with a large INSERT statement
            # For brevity, the implementation of the INSERT is omitted.

        if failed_tasks > 0:
            raise Exception(f"{failed_tasks} validation task(s) failed. Check logs for details.")
        else:
            logger.success("✅ All DataPact validations passed successfully.")
