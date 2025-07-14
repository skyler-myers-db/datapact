"""
A local utility script to set up the DataPact demo environment.

This script acts as a local client to prepare the necessary resources for
running the DataPact demo. It reads a pure SQL file (`setup.sql`) and
executes it as a single, multi-statement query against a specified
Serverless SQL Warehouse. This approach is robust and aligns with the
DataPact architectural philosophy.
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

    logger.info("Submitting entire SQL script for execution...")

    try:
        # Use the robust method for executing a multi-statement SQL script
        waiter = w.statement_execution.execute_statement(
            statement=sql_script,
            warehouse_id=warehouse.id,
            wait_timeout='50s',
        )
        result = waiter.result()

        if result.status.state == sql_service.StatementState.SUCCEEDED:
            logger.success("✅ SQL script executed successfully!")
            logger.success("✅ Demo environment setup complete!")
            logger.info("You can now run the demo validation with the following command:")

            # Use loguru for all output, with formatting for clarity
            run_command = (
                "datapact run \\\n"
                "  --config demo/demo_config.yml \\\n"
                f"  --warehouse \"{args.warehouse}\" \\\n"
                "  --job-name \"DataPact Demo Run\" \\\n"
                f"  --profile {args.profile}"
            )

            logger.info("\n\n" + "="*50 + f"\n{run_command}\n" + "="*50 + "\n")
        else:
            # If the statement fails, provide the error details
            error = result.status.error
            logger.critical(f"SQL script execution failed with state: {result.status.state}")
            logger.critical(f"Error: {error.message}")

    except Exception as e:
        logger.critical(f"An error occurred during statement execution: {e}")

if __name__ == "__main__":
    run_demo_setup()
