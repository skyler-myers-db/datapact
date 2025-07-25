"""
The main entry point for the DataPact Command-Line Interface (CLI).
"""
import argparse
import os
import yaml
from .client import DataPactClient
from loguru import logger

def main() -> None:
    """The main function that executes when the `datapact` command is run."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="DataPact: The enterprise-grade data validation accelerator for Databricks.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("command", choices=["run"], help="The command to execute.")
    parser.add_argument("--config", required=True, help="Path to the validation_config.yml file.")
    parser.add_argument("--job-name", default="datapact-validation-run", help="Name for the created Databricks job and dashboard.")
    parser.add_argument("--warehouse", help="Name of the Serverless SQL Warehouse. Overrides all other settings.")
    parser.add_argument("--profile", help="Databricks CLI profile. Overrides DATABRICKS_PROFILE env var.")
    parser.add_argument("--results-table", help="Optional: A 3-level (catalog.schema.table) Delta table name to store results. If not provided, a default is created.")
    
    args: argparse.Namespace = parser.parse_args()

    profile_name: str = args.profile or os.getenv("DATABRICKS_PROFILE", "DEFAULT")
    
    client: DataPactClient = DataPactClient(profile=profile_name)

    warehouse_name: str | None = args.warehouse
    if not warehouse_name:
        warehouse_name = os.getenv("DATAPACT_WAREHOUSE")
        if not warehouse_name:
            if hasattr(client.w.config, 'datapact_warehouse'):
                warehouse_name = client.w.config.datapact_warehouse

    if not warehouse_name:
        raise ValueError(
            "A warehouse must be provided via the --warehouse flag, the DATAPACT_WAREHOUSE "
            "environment variable, or a 'datapact_warehouse' key in your Databricks config profile."
        )

    if args.command == "run":
        logger.info(f"Loading configuration from {args.config}...")
        with open(args.config, 'r') as f:
            validation_config: dict[str, any] = yaml.safe_load(f)
        
        try:
            client.run_validation(
                config=validation_config,
                job_name=args.job_name,
                warehouse_name=warehouse_name,
                results_table=args.results_table,
            )
        except Exception as e:
            logger.critical(f"A critical error occurred during the DataPact run: {e}", exc_info=True)
            exit(1)

if __name__ == "__main__":
    main()
