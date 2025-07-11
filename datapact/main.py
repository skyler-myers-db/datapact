# datapact/main.py
import argparse
import yaml
from .client import DataPactClient

def main():
    parser = argparse.ArgumentParser(description="DataPact: The Databricks Data Validation Accelerator")
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--config", required=True, help="Path to the validation_config.yml file.")
    parser.add_argument("--job-name", default="datapact-validation-run", help="Name of the Databricks job to create.")
    parser.add_argument("--warehouse", required=True, help="Name of the Serverless SQL Warehouse to use.")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile to use for authentication.")
    
    args = parser.parse_args()

    if args.command == "run":
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        
        client = DataPactClient(profile=args.profile)
        client.run_validation(config, args.job_name, args.warehouse)

if __name__ == "__main__":
    main()
