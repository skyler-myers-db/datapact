"""
A local utility script to set up the DataPact demo environment.

This script acts as a local client to prepare the necessary resources for
running the DataPact demo. It reads a pure SQL file (`setup.sql`) and
executes it as a single, multi-statement query against a specified
Serverless SQL Warehouse. It uses a robust asynchronous polling pattern
to handle the potentially long-running setup script.
"""

import argparse
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from loguru import logger

def get_warehouse_by_name(w: WorkspaceClient, name: str) -> sql_service.EndpointInfo | None:
    """Finds a SQL warehouse by name by listing all warehouses."""
    try:
        for wh in w.warehouses.list():
            if wh.name == name:
                return wh
    except Exception as e:
        logger.error(f"An error occurred while trying to list warehouses: {e}")
    return None

def run_demo_setup():
    """The main function to set up the DataPact demo environment."""
    parser = argparse.ArgumentParser(description="Set up the DataPact demo environment.")
    parser.add_argument("--warehouse", required=True, help="Name of the Serverless SQL Warehouse to use.")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile to use.")
    args = parser.parse_args()

    logger.info(f"Connecting to Databricks with profile '{args.profile}'...")
    w = WorkspaceClient(profile=args.profile)

    warehouse = get_warehouse_by_name(w, args.warehouse)
    if not warehouse:
        logger.critical(f"Failed to find warehouse '{args.warehouse}'. Please ensure it exists and you have permissions to view it.")
        return

    logger.info(f"Found warehouse '{args.warehouse}' (ID: {warehouse.id}).")

    sql_file_path = Path(__file__).parent / "setup.sql"
    logger.info(f"Reading setup script from: {sql_file_path}")
    with open(sql_file_path, 'r') as f:
        sql_script = f.read()

    logger.info("Submitting SQL script for asynchronous execution...")

    try:
        # Step 1: Execute the statement with wait_timeout='0s' to get the statement_id immediately.
        resp = w.statement_execution.execute_statement(
            statement=sql_script,
            warehouse_id=warehouse.id,
            wait_timeout='0s', # Makes the call non-blocking
        )
        statement_id = resp.statement_id
        logger.info(f"Successfully submitted statement with ID: {statement_id}")

        # Step 2: Poll for the result in a loop.
        timeout_seconds: int = 600  # 10 minutes
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status = w.statement_execution.get_statement(statement_id=statement_id)
            current_state = status.status.state
            logger.info(f"Polling statement status... Current state: {current_state}")

            if current_state == sql_service.StatementState.SUCCEEDED:
                logger.success("✅ SQL script executed successfully!")
                logger.success("✅ Demo environment setup complete!")
                logger.info("You can now run the demo validation with the following command:")
                
                run_command: str = (
                    "datapact run \\\n"
                    "  --config demo/demo_config.yml \\\n"
                    f"  --warehouse \"{args.warehouse}\" \\\n"
                    "  --job-name \"DataPact Demo Run\" \\\n"
                    f"  --profile {args.profile}"
                )
                logger.info("\n\n" + "="*50 + f"\n{run_command}\n" + "="*50 + "\n")
                return # Exit successfully

            if current_state in [sql_service.StatementState.FAILED, sql_service.StatementState.CANCELED, sql_service.StatementState.CLOSED]:
                error = status.status.error
                logger.critical(f"SQL script execution failed with terminal state: {current_state}")
                if error:
                    logger.critical(f"Error: {error.message}")
                return # Exit with failure

            time.sleep(10) # Wait 10 seconds between polls

        logger.critical(f"Timeout: Script execution did not complete within {timeout_seconds} seconds.")

    except Exception as e:
        logger.critical(f"An unexpected error occurred during statement execution: {e}")

if __name__ == "__main__":
    run_demo_setup()
