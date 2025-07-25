"""
A local utility script to set up the DataPact demo environment.

This script acts as a local client to prepare a comprehensive and realistic
demo environment. It executes a pure SQL file (`setup.sql`) to create millions
of rows across several tables, each designed to showcase a specific feature or
edge case that DataPact handles.
"""
import argparse
import os
import re
import time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from loguru import logger

def get_warehouse_by_name(w: WorkspaceClient, name: str) -> sql_service.EndpointInfo | None:
    """
    Finds a SQL warehouse by its display name.

    Args:
        w: An initialized Databricks WorkspaceClient.
        name: The name of the SQL warehouse to find.

    Returns:
        An EndpointInfo object if the warehouse is found, otherwise None.
    """
    try:
        for wh in w.warehouses.list():
            if wh.name == name:
                return wh
    except Exception as e:
        logger.error(f"An error occurred while trying to list warehouses: {e}")
    return None

def run_demo_setup() -> None:
    """The main function to orchestrate the demo environment setup."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description="Set up the DataPact demo environment.")
    parser.add_argument("--warehouse", help="Name of the Serverless SQL Warehouse. Overrides all other settings.")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile to use.")
    args: argparse.Namespace = parser.parse_args()

    profile_name: str = args.profile or os.getenv("DATABRICKS_PROFILE", "DEFAULT")
    
    logger.info(f"Connecting to Databricks with profile '{profile_name}'...")
    w: WorkspaceClient = WorkspaceClient(profile=profile_name)
    
    warehouse_name: str | None = args.warehouse
    if not warehouse_name:
        warehouse_name = os.getenv("DATAPACT_WAREHOUSE")
        if not warehouse_name:
            warehouse_name = w.config.config.get('datapact_warehouse')

    if not warehouse_name:
        raise ValueError(
            "A warehouse must be provided via the --warehouse flag, the DATAPACT_WAREHOUSE "
            "environment variable, or a 'datapact_warehouse' key in your Databricks config profile."
        )

    logger.info(f"Using warehouse: {warehouse_name}")
    warehouse: sql_service.EndpointInfo | None = get_warehouse_by_name(w, warehouse_name)
    if not warehouse:
        logger.critical(f"Failed to find warehouse '{warehouse_name}'. Please ensure it exists.")
        return
    logger.info(f"Found warehouse '{args.warehouse}' (ID: {warehouse.id}).")

    sql_file_path: Path = Path(__file__).parent / "setup.sql"
    logger.info(f"Reading and parsing setup script from: {sql_file_path}")
    with open(sql_file_path, 'r') as f:
        sql_script: str = f.read()

    sql_script = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.DOTALL)
    sql_script = re.sub(r'--.*', '', sql_script)
    sql_commands: list[str] = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]

    logger.info(f"Found {len(sql_commands)} individual SQL statements to execute sequentially.")

    for i, command in enumerate(sql_commands):
        logger.info(f"Executing statement {i+1}/{len(sql_commands)}...")
        logger.debug(f"SQL: {command}")
        try:
            resp: sql_service.ExecuteStatementResponse = w.statement_execution.execute_statement(
                statement=command,
                warehouse_id=warehouse.id,
                wait_timeout='0s'
            )
            statement_id: str = resp.statement_id
            timeout_seconds: int = 600
            start_time: float = time.time()
            while time.time() - start_time < timeout_seconds:
                status: sql_service.StatementStatus = w.statement_execution.get_statement(statement_id=statement_id)
                current_state: sql_service.StatementState = status.status.state
                if current_state == sql_service.StatementState.SUCCEEDED:
                    logger.success(f"Statement {i+1} succeeded.")
                    break
                if current_state in [sql_service.StatementState.FAILED, sql_service.StatementState.CANCELED, sql_service.StatementState.CLOSED]:
                    error: Optional[sql_service.Error] = status.status.error
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

    logger.success("✅ Comprehensive demo environment setup complete!")
    logger.info("\nYou have just set up a realistic, multi-faceted data environment. The upcoming validation run will showcase:")
    logger.info("  - Handling of large tables (millions of rows).")
    logger.info("  - A mix of PASSING and FAILING tasks.")
    logger.info("  - Advanced features like accepted thresholds and selective column hashing.")
    logger.info("  - Graceful handling of edge cases like empty tables and tables without primary keys.")
    logger.info("\nRun the demo validation with the following command (ensure your .databrickscfg has 'datapact_warehouse' set):")

    # Corrected CLI guidance
    run_command: str = (
        "datapact run \\\n"
        "  --config demo/demo_config.yml \\\n"
        "  --job-name \"DataPact Comprehensive Demo\" \\\n"
        "  --results-table \"datapact_demo_catalog.source_data.datapact_run_history\" \\\n"
        f"  --profile {args.profile}"
    )
    logger.info("\n\n" + "="*60 + f"\n{run_command}\n" + "="*60 + "\n")

if __name__ == "__main__":
    run_demo_setup()
