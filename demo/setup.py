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
from typing import Optional
from datetime import timedelta, datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as sql_service
from loguru import logger


def get_warehouse_by_name(
    w: WorkspaceClient, name: str
) -> Optional[sql_service.EndpointInfo]:
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
    except (AttributeError, ConnectionError, TimeoutError) as e:
        logger.error(f"An error occurred while trying to list warehouses: {e}")
    return None


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for setting up the DataPact demo environment.

    Returns:
        argparse.Namespace: The parsed command-line arguments, including:
            --warehouse (str, optional): Name of the Serverless SQL Warehouse. Overrides all other settings.
            --profile (str, optional): Databricks CLI profile to use. Defaults to "DEFAULT".
    """
    parser = argparse.ArgumentParser(
        description="Set up the DataPact demo environment."
    )
    parser.add_argument(
        "--warehouse",
        help="Name of the Serverless SQL Warehouse. Overrides all other settings.",
    )
    parser.add_argument(
        "--profile", default="DEFAULT", help="Databricks CLI profile to use."
    )
    return parser.parse_args()


def resolve_warehouse_name(args, w: WorkspaceClient) -> str:
    """
    Resolves the warehouse name from command-line arguments, environment variables, or Databricks config.

    The function checks for the warehouse name in the following order:
    1. The `--warehouse` argument provided in `args`.
    2. The `DATAPACT_WAREHOUSE` environment variable.
    3. The `datapact_warehouse` attribute in the Databricks config profile (`w.config`).

    Args:
        args: An object with a `warehouse` attribute, typically parsed from command-line arguments.
        w (WorkspaceClient): A Databricks WorkspaceClient instance with a `config` attribute.

    Returns:
        str: The resolved warehouse name.

    Raises:
        ValueError: If the warehouse name cannot be determined from any of the sources.
    """
    warehouse_name = args.warehouse
    if not warehouse_name:
        warehouse_name = os.getenv("DATAPACT_WAREHOUSE")
        if not warehouse_name and hasattr(w.config, "datapact_warehouse"):
            warehouse_name = w.config.datapact_warehouse
    if not warehouse_name:
        raise ValueError(
            "A warehouse must be provided via the --warehouse flag, the DATAPACT_WAREHOUSE "
            "environment variable, or a 'datapact_warehouse' key in your Databricks config profile."
        )
    return warehouse_name


def read_sql_commands(sql_file_path: Path) -> list[str]:
    """
    Reads a SQL script file, removes comments, and splits it into individual SQL commands.

    Args:
        sql_file_path (Path): The path to the SQL script file.

    Returns:
        list[str]: A list of SQL command strings, with comments removed and whitespace trimmed.
    """
    logger.info(f"Reading and parsing setup script from: {sql_file_path}")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_script = f.read()
    sql_script = re.sub(r"/\*.*?\*/", "", sql_script, flags=re.DOTALL)
    sql_script = re.sub(r"--.*", "", sql_script)
    return [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]


def execute_sql_commands(
    w: WorkspaceClient, warehouse, sql_commands: list[str]
) -> None:
    """
    Executes a list of SQL commands sequentially on a specified warehouse using the provided WorkspaceClient.

    Each SQL command is executed as a separate statement. The function logs the progress and status of each statement,
    waiting up to 10 minutes for each to complete. If a statement fails, is canceled, closed, or times out, an exception is raised.
    Execution halts on the first failure.

    Args:
        w (WorkspaceClient): The client used to execute SQL statements.
        warehouse: The warehouse object containing the warehouse ID for statement execution.
        sql_commands (list[str]): A list of SQL command strings to execute sequentially.

    Raises:
        Exception: If a statement fails, is canceled, closed, or encounters an error during execution.
        TimeoutError: If a statement does not complete within the allotted timeout period.
    """
    logger.info(
        f"Found {len(sql_commands)} individual SQL statements to execute sequentially."
    )
    for i, command in enumerate(sql_commands):
        logger.info(f"Executing statement {i + 1}/{len(sql_commands)}...")
        logger.debug(f"SQL: {command}")
        try:
            resp = w.statement_execution.execute_statement(
                statement=command, warehouse_id=warehouse.id, wait_timeout="0s"
            )
            statement_id = resp.statement_id
            if not statement_id:
                raise RuntimeError(f"Statement {i + 1} failed to get a statement ID.")
            timeout = timedelta(minutes=10)
            deadline = datetime.now() + timeout
            while datetime.now() < deadline:
                status = w.statement_execution.get_statement(statement_id)
                if not status.status:
                    time.sleep(5)
                    continue
                current_state = status.status.state
                if current_state == sql_service.StatementState.SUCCEEDED:
                    logger.success(f"Statement {i + 1} succeeded.")
                    break
                if current_state in [
                    sql_service.StatementState.FAILED,
                    sql_service.StatementState.CANCELED,
                    sql_service.StatementState.CLOSED,
                ]:
                    error = status.status.error
                    error_message = (
                        error.message if error else "No error message provided."
                    )
                    raise RuntimeError(
                        f"Statement {i + 1} failed with state {current_state}. Reason: {error_message}"
                    )
                time.sleep(5)
            else:
                raise TimeoutError(
                    f"Statement {i + 1} timed out after {timeout.total_seconds()} seconds."
                )
        except Exception as e:
            logger.critical(
                f"An error occurred during execution of statement {i + 1}. Halting setup."
            )
            logger.critical(f"Failed SQL: {command}")
            raise e


def print_success_and_instructions(profile: str) -> None:
    """
    Print success message and detailed instructions for running the demo validation.

    This function outputs a comprehensive success message indicating that the demo
    environment setup is complete, along with detailed information about what the
    validation run will demonstrate. It also provides the exact command needed to
    run the demo validation.

    Args:
        profile (str): The profile name to be used in the datapact run command.
                      This should correspond to a valid profile in the user's
                      .databrickscfg file.

    Returns:
        None: This function only prints information and does not return any value.

    Example:
        >>> print_success_and_instructions("production")
        # Prints success message and command:
        # datapact run --config demo/demo_config.yml --job-name "DataPact Enterprise Demo" --profile production
    """
    logger.success("✅ Comprehensive demo environment setup complete!")
    logger.info(
        "\nYou have just set up a realistic, multi-faceted data environment. The upcoming validation run will showcase:"
    )
    logger.info(
        "  - Validation across a 12-table enterprise model (Sales, HR, Marketing, Finance)."
    )
    logger.info("  - A mix of PASSING and FAILING tasks to demonstrate rich reporting.")
    logger.info(
        "  - Advanced features like accepted thresholds and selective column hashing."
    )
    logger.info(
        "  - Graceful handling of edge cases like empty tables and tables without primary keys."
    )
    logger.info(
        "\nRun the demo validation with the following command (ensure your .databrickscfg has 'datapact_warehouse' set):"
    )
    run_command = (
        "datapact run \\\n"
        "  --config demo/demo_config.yml \\\n"
        '  --job-name "DataPact Enterprise Demo" \\\n'
        f"  --profile {profile}"
    )
    logger.info("\n\n" + "=" * 60 + f"\n{run_command}\n" + "=" * 60 + "\n")


def run_demo_setup() -> None:
    """
    Orchestrates the complete demo environment setup process.

    This function performs the following steps:
    1. Parses command line arguments to get configuration options
    2. Establishes connection to Databricks using the specified or default profile
    3. Resolves and validates the target warehouse for SQL execution
    4. Reads SQL setup commands from the setup.sql file
    5. Executes the SQL commands against the Databricks warehouse
    6. Displays success message and setup instructions

    The function uses the DATABRICKS_PROFILE environment variable as fallback
    if no profile is specified via command line arguments. It will terminate
    early if the specified warehouse cannot be found.

    Returns:
        None

    Raises:
        May raise exceptions from underlying Databricks API calls or file I/O
        operations if warehouse connection fails or SQL file cannot be read.
    """
    args = parse_args()
    profile_name = args.profile or os.getenv("DATABRICKS_PROFILE", "DEFAULT")
    logger.info(f"Connecting to Databricks with profile '{profile_name}'...")
    w = WorkspaceClient(profile=profile_name)
    warehouse_name = resolve_warehouse_name(args, w)
    logger.info(f"Using warehouse: {warehouse_name}")
    warehouse = get_warehouse_by_name(w, warehouse_name)
    if not warehouse:
        logger.critical(
            f"Failed to find warehouse '{warehouse_name}'. Please ensure it exists."
        )
        return
    logger.info(f"Found warehouse '{warehouse_name}' (ID: {warehouse.id}).")
    sql_file_path = Path(__file__).parent / "setup.sql"
    sql_commands = read_sql_commands(sql_file_path)
    execute_sql_commands(w, warehouse, sql_commands)
    print_success_and_instructions(args.profile)


if __name__ == "__main__":
    run_demo_setup()
