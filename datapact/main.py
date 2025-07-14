"""
The main entry point for the DataPact Command-Line Interface (CLI).

This module is responsible for defining and parsing all command-line arguments
that the user provides. It uses Python's `argparse` library to create a
user-friendly interface for running validations.

Its primary role is to act as the bridge between the user's terminal and the
core application logic. It parses the user's inputs (like the config file path
and warehouse name), instantiates the `DataPactClient`, and then invokes the
main `run_validation` method to start the process.
"""

import argparse
import os
import yaml
from .client import DataPactClient
from loguru import logger

def main() -> None:
    """
    The main entry point for the DataPact Command-Line Interface (CLI).

    This function is responsible for parsing command-line arguments, loading the
    validation configuration, initializing the DataPactClient, and triggering
    the validation run. It acts as the primary user-facing interface for
    running DataPact from an external environment like a terminal or CI/CD pipeline.
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
    
    parser.add_argument("--create-warehouse", action="store_true", help="If specified, creates the SQL warehouse if it does not exist.")
    parser.add_argument("--results-table", help="Optional: FQN of a Delta table to store results.")
    
    args = parser.parse_args()

    warehouse_name = args.warehouse or os.getenv("DATAPACT_WAREHOUSE")
    profile_name = args.profile or os.getenv("DATABRICKS_PROFILE")

    if not warehouse_name:
        raise ValueError("Warehouse must be provided via --warehouse flag or DATAPACT_WAREHOUSE environment variable.")
    if not profile_name:
        # Default to 'DEFAULT' if nothing is provided, as the SDK does.
        profile_name = "DEFAULT"
        logger.info("No --profile or DATABRICKS_PROFILE found, using 'DEFAULT' profile.")

    if args.command == "run":
        logger.info(f"Loading configuration from {args.config}...")
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        
        try:
            client = DataPactClient(profile=profile_name)
            client.run_validation(
                config=config,
                job_name=args.job_name,
                warehouse_name=warehouse_name,
                create_warehouse=args.create_warehouse,
                results_table=args.results_table,
            )
        except Exception as e:
            logger.critical(f"An error occurred during the DataPact run: {e}")
            exit(1)

if __name__ == "__main__":
    main()
