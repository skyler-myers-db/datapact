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
from pathlib import Path
from typing import Optional
from datetime import timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from loguru import logger

def get_warehouse_by_name(w: WorkspaceClient, name: str) -> Optional[sql_service.EndpointInfo]:
    """
    Finds a SQL warehouse by its display name.

    Args:
        w: An initialized Databricks WorkspaceClient.
        name: The name of the SQL warehouse to find.

    Returns:
        An EndpointInfo object if the warehouse is found, otherwise None.
    """
    try:
        return next((wh for wh in w.warehouses.list() if wh.name == name), None)
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
            if hasattr(w.config, 'datapact_warehouse'):
                warehouse_name = w.config.datapact_warehouse

    if not warehouse_name:
        raise ValueError(
            "A warehouse must be provided via the --warehouse flag, the DATAPACT_WAREHOUSE "
            "environment variable, or a 'datapact_warehouse' key in your Databricks config profile."
        )

    logger.info(f"Using warehouse: {warehouse_name}")
    warehouse: Optional[sql_service.EndpointInfo] = get_warehouse_by_name(w, warehouse_name)
    if not warehouse:
        logger.critical(f"Failed to find warehouse '{warehouse_name}'. Please ensure it exists.")
        return
    logger.info(f"Found warehouse '{warehouse_name}' (ID: {warehouse.id}).")

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
            w.statement_execution.execute_statement(
                statement=command,
                warehouse_id=warehouse.id,
                wait_timeout='0s' 
            ).result(timeout=timedelta(minutes=10))
            
            logger.success(f"Statement {i+1} succeeded.")
        except Exception as e:
            logger.critical(f"An error occurred during execution of statement {i+1}. Halting setup.")
            logger.critical(f"Failed SQL: {command}")
            raise e

    logger.success("✅ Comprehensive demo environment setup complete!")
    logger.info("\nYou have just set up a realistic, multi-faceted data environment. The upcoming validation run will showcase:")
    logger.info("  - Validation across a 12-table enterprise model (Sales, HR, Marketing, Finance).")
    logger.info("  - A mix of PASSING and FAILING tasks to demonstrate rich reporting.")
    logger.info("  - Advanced features like accepted thresholds and selective column hashing.")
    logger.info("  - Graceful handling of edge cases like empty tables and tables without primary keys.")
    logger.info("\nRun the demo validation with the following command (ensure your .databrickscfg has 'datapact_warehouse' set):")

    run_command: str = (
        "datapact run \\\n"
        "  --config demo/demo_config.yml \\\n"
        "  --job-name \"DataPact Enterprise Demo\" \\\n"
        f"  --profile {args.profile}"
    )
    logger.info("\n\n" + "="*60 + f"\n{run_command}\n" + "="*60 + "\n")

if __name__ == "__main__":
    run_demo_setup()
