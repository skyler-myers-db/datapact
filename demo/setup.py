"""
A local utility script to set up the DataPact demo environment.

This script acts as a local client to prepare the necessary resources for
running the DataPact demo. It reads a pure SQL file (`setup.sql`), parses it
into individual statements, and executes them sequentially against a specified
Serverless SQL Warehouse.

This one-statement-at-a-time, synchronous-wait approach is the most robust
method for executing a series of DDL commands, ensuring that each step
completes before the next one begins.
"""

import argparse
import time
import re
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

def run_demo_setup() -> None:
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
    logger.info(f"Reading and parsing setup script from: {sql_file_path}")
    with open(sql_file_path, 'r') as f:
        sql_script = f.read()

    # Robustly parse the SQL script into individual statements.
    sql_script = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.DOTALL)
    sql_script = re.sub(r'--.*', '', sql_script)
    sql_commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]

    logger.info(f"Found {len(sql_commands)} individual SQL statements to execute sequentially.")

    # Execute each statement one-by-one and wait for completion.
    for i, command in enumerate(sql_commands):
        logger.info(f"Executing statement {i+1}/{len(sql_commands)}...")
        logger.debug(f"SQL: {command}")
        try:
            resp = w.statement_execution.execute_statement(
                statement=command,
                warehouse_id=warehouse.id,
                wait_timeout='0s'  # Submit and poll.
            )
            statement_id = resp.statement_id

            timeout_seconds = 300  # 5-minute timeout per statement.
            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                status = w.statement_execution.get_statement(statement_id=statement_id)
                current_state = status.status.state

                if current_state == sql_service.StatementState.SUCCEEDED:
                    logger.success(f"Statement {i+1} succeeded.")
                    break

                if current_state in [sql_service.StatementState.FAILED, sql_service.StatementState.CANCELED, sql_service.StatementState.CLOSED]:
                    error = status.status.error
                    logger.critical(f"Statement {i+1} failed with state: {current_state}")
                    if error:
                        logger.critical(f"Error: {error.message}")
                    raise Exception(f"Setup script failed at statement {i+1}.")

                time.sleep(5)
            else:
                raise TimeoutError(f"Statement {i+1} timed out after {timeout_seconds} seconds.")

        except Exception as e:
            logger.critical(f"An error occurred during execution. Halting setup.")
            raise e

    # If the script reaches this point, all statements have succeeded.
    logger.success("✅ SQL script executed successfully!")
    logger.success("✅ Demo environment setup complete!")
    logger.info("You can now run the demo validation with the following command:")

    run_command = (
        "datapact run \\\n"
        "  --config demo/demo_config.yml \\\n"
        f"  --warehouse \"{args.warehouse}\" \\\n"
        "  --job-name \"DataPact Demo Run\" \\\n"
        "  --results-table \"datapact_demo_catalog.source_data.datapact_run_history\" \\\n"
        f"  --profile {args.profile}"
    )
    logger.info("\n\n" + "="*50 + f"\n{run_command}\n" + "="*50 + "\n")

if __name__ == "__main__":
    run_demo_setup()
