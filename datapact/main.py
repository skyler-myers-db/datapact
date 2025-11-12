"""
The main entry point for the DataPact Command-Line Interface (CLI).
"""

import argparse
import os
import sys
import textwrap
from pathlib import Path

import yaml  # type: ignore[import]
from loguru import logger
from pydantic import ValidationError

from .client import DataPactClient, resolve_warehouse_name
from .config import DataPactConfig

DEFAULT_CONFIG_TEMPLATE = (
    textwrap.dedent(
        """
        # DataPact starter configuration
        # Update the catalog/schema/table names to match your environment.
        validations:
          - task_key: "validate_sample_table"
            source_catalog: "source_catalog"
            source_schema: "bronze"
            source_table: "sample_table"
            target_catalog: "main"
            target_schema: "gold"
            target_table: "sample_table"
            primary_keys: ["id"]
            count_tolerance: 0.0
            pk_row_hash_check: true
            pk_hash_tolerance: 0.0
            null_validation_tolerance: 0.01
            null_validation_columns: ["status", "country"]
            agg_validations:
              - column: "revenue"
                validations:
                  - { agg: "SUM", tolerance: 0.01 }
          - task_key: "validate_recent_activity_slice"
            source_catalog: "source_catalog"
            source_schema: "bronze"
            source_table: "activity"
            target_catalog: "main"
            target_schema: "gold"
            target_table: "activity_curated"
            primary_keys: ["activity_id"]
            filter: |
              activity_date >= date_sub(current_date(), 7)
            count_tolerance: 0.02
            pk_row_hash_check: true
            pk_hash_tolerance: 0.01
            uniqueness_columns: ["user_id", "activity_date"]
            uniqueness_tolerance: 0.0
        """
    ).strip()
    + "\n"
)


def _load_config(config_path: str) -> DataPactConfig:
    with open(config_path, encoding="utf-8") as config_file:
        raw_config = yaml.safe_load(config_file)
    return DataPactConfig(**raw_config)


def _summarize_config(config: DataPactConfig) -> None:
    task_count = len(config.validations)
    logger.info(f"Detected {task_count} validation task(s) in this configuration.")
    for task in config.validations:
        source = f"{task.source_catalog}.{task.source_schema}.{task.source_table}"
        target = f"{task.target_catalog}.{task.target_schema}.{task.target_table}"
        logger.info(f" • {task.task_key}: {source} ➜ {target}")


def _scaffold_config(output_path: str, force: bool) -> None:
    target = Path(output_path).expanduser().resolve()
    if target.exists() and not force:
        logger.error(
            f"Cannot scaffold config: file already exists at {target}. Pass --force to overwrite."
        )
        sys.exit(1)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(DEFAULT_CONFIG_TEMPLATE, encoding="utf-8")
    logger.success(f"✨ Created starter config at {target}")


def main() -> None:
    """The main function that executes when the `datapact` command is run."""
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="DataPact: The enterprise-grade data validation accelerator for Databricks.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("command", choices=["run", "plan", "init"], help="The command to execute.")
    parser.add_argument(
        "--config", help="Path to the validation_config.yml file (plan/run commands)."
    )
    parser.add_argument(
        "--job-name",
        default="datapact-validation-run",
        help="Name for the created Databricks job and dashboard.",
    )
    parser.add_argument(
        "--warehouse",
        help="Name of the Serverless SQL Warehouse. Overrides all other settings.",
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile. Overrides DATABRICKS_PROFILE env var.",
    )
    parser.add_argument(
        "--results-table",
        help="Optional: A 3-level (catalog.schema.table) Delta table name to store results. If not provided, a default is created at `datapact.results.run_history`.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and show a plan without running Databricks jobs.",
    )
    parser.add_argument(
        "--output",
        default="datapact_config.yml",
        help="Target path for the `init` command (default: %(default)s).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite any existing file when using the `init` command.",
    )

    args: argparse.Namespace = parser.parse_args()

    if args.command == "init":
        _scaffold_config(args.output, args.force)
        return

    if not args.config:
        parser.error("--config is required for the run and plan commands.")

    client: DataPactClient | None = None
    warehouse_name: str | None = None
    if args.command == "run" and not args.dry_run:
        profile_name: str = args.profile or os.getenv("DATABRICKS_PROFILE", "DEFAULT")
        client = DataPactClient(profile=profile_name)
        warehouse_name = resolve_warehouse_name(
            workspace_client=client.w, explicit_name=args.warehouse
        )

    logger.info(f"Loading configuration from {args.config}...")
    try:
        validation_config: DataPactConfig = _load_config(args.config)
        logger.success("✅ Configuration is valid.")
    except FileNotFoundError:
        logger.critical(f"Configuration file not found at: {args.config}")
        sys.exit(1)
    except ValidationError as exc:
        logger.critical(
            f"Configuration file '{args.config}' is invalid. Please fix the following errors:"
        )
        logger.error(exc)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.critical(f"An error occurred while parsing the YAML config: {exc}")
        sys.exit(1)

    if args.command == "plan":
        _summarize_config(validation_config)
        return

    if args.dry_run:
        logger.info("Dry run requested – validations will not be executed.")
        _summarize_config(validation_config)
        return

    assert client is not None and warehouse_name is not None  # mypy safety
    client.run_validation(
        config=validation_config,
        job_name=args.job_name,
        warehouse_name=warehouse_name,
        results_table=args.results_table,
    )


if __name__ == "__main__":
    main()
