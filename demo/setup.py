import argparse
import time
from pathlib import Path
from databricks import sql
from loguru import logger

def run_demo_setup():
    """
    Sets up the DataPact demo environment in a Databricks workspace.

    This script acts as a local client to prepare the necessary resources for
    running the DataPact demo. It reads a pure SQL file (`setup.sql`) and
    executes it statement-by-statement against a specified Serverless SQL
    Warehouse. This approach ensures the demo setup is 100% SQL-based and
    does not require any Spark or notebook execution for the setup itself,
    perfectly aligning with the DataPact architectural philosophy.
    """
    parser = argparse.ArgumentParser(description="Set up the DataPact demo environment.")
    parser.add_argument("--warehouse", required=True, help="Name of the Serverless SQL Warehouse to use.")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile to use.")
    args = parser.parse_args()

    logger.info(f"Connecting to Databricks with profile '{args.profile}'...")
    # We use the SDK's sql.connect for direct SQL execution
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=args.profile)
    
    try:
        warehouse = w.warehouses.get_by_name(args.warehouse)
        logger.info(f"Found warehouse '{args.warehouse}' (ID: {warehouse.id}).")
    except Exception as e:
        logger.critical(f"Failed to find warehouse '{args.warehouse}'. Please ensure it exists. Error: {e}")
        return

    sql_file_path = Path(__file__).parent / "setup.sql"
    logger.info(f"Reading setup commands from: {sql_file_path}")
    with open(sql_file_path, 'r') as f:
        # Split SQL script into individual statements, filtering out empty lines and comments
        sql_commands = [
            cmd.strip() for cmd in f.read().split(';') 
            if cmd.strip() and not cmd.strip().startswith('--')
        ]

    logger.info(f"Found {len(sql_commands)} SQL commands to execute on warehouse '{args.warehouse}'.")

    try:
        with sql.connect(
            server_hostname=w.config.host,
            http_path=warehouse.odbc_params.path,
            token=w.config.token,
        ) as connection:
            with connection.cursor() as cursor:
                for i, command in enumerate(sql_commands):
                    logger.info(f"Executing command {i+1}/{len(sql_commands)}...")
                    logger.debug(command)
                    cursor.execute(command)
                    time.sleep(1) # Small delay to prevent overwhelming the API
        
        logger.success("✅ Demo environment setup complete!")
        logger.info("You can now run the demo validation with:")
        logger.info(f"datapact run --config demo/demo_config.yml --warehouse \"{args.warehouse}\" --profile {args.profile}")

    except Exception as e:
        logger.critical(f"An error occurred during SQL execution: {e}")

if __name__ == "__main__":
    run_demo_setup()
