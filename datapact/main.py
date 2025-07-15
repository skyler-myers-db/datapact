"""
The main entry point for the DataPact Command-Line Interface (CLI).

This module is responsible for defining and parsing all command-line arguments
that the user provides. It uses Python's `argparse` library to create a
user-friendly interface for running validations.

Its primary role is to act as the bridge between the user's terminal and the
core application logic. It parses the user's inputs, instantiates the
`DataPactClient`, and then invokes the main `run_validation` method to
start the process.
"""

import argparse
import os
import yaml
from .client import DataPactClient
from loguru import logger

def main() -> None:
    """
    The main function that executes when the `datapact` command is run.
    """
    parser = argparse.ArgumentParser(
        description="DataPact: The enterprise-grade data validation accelerator for Databricks.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--config", required=True, help="Path to the validation_config.yml file.")
    parser.add_argument("--job-name", default="datapact-validation-run", help="Name of the Databricks job.")
    parser.add_argument(
        "--warehouse",
        help="Name of the Serverless SQL Warehouse. Can also be set via DATAPACT_WAREHOUSE env var."
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile. Can also be set via DATABRICKS_PROFILE env var."
    )
    parser.add_argument(
        "--results-table",
        help="Optional: A 3-level (catalog.schema.table) Delta table name to store results. If not provided, a default is created."
    )
    
    args = parser.parse_args()

    warehouse_name = args.warehouse or os.getenv("DATAPACT_WAREHOUSE")
    profile_name = args.profile or os.getenv("DATABRICKS_PROFILE", "DEFAULT")

    if not warehouse_name:
        raise ValueError("Warehouse must be provided via --warehouse flag or DATAPACT_WAREHOUSE environment variable.")

    if args.command == "run":
        logger.info(f"Loading configuration from {args.config}...")
        with open(args.config, 'r') as f:
            config: dict[str, any] = yaml.safe_load(f)
        
        try:
            client = DataPactClient(profile=profile_name)
            client.run_validation(
                config=config,
                job_name=args.job_name,
                warehouse_name=warehouse_name,
                results_table=args.results_table,
            )
        except Exception as e:
            logger.critical(f"An error occurred during the DataPact run: {e}")
            exit(1)

if __name__ == "__main__":
    main()
