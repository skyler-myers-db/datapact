# In a Databricks Notebook
# 1. Install the datapact .whl file to your cluster

import yaml
from datapact.client import DataPactClient

# Config can be read from a file in a Volume or defined in the notebook
config_yaml = """
validations:
  - task_key: "validate_dim_users"
    # ... rest of config
"""
config = yaml.safe_load(config_yaml)

# When running inside, the client authenticates automatically, no profile needed.
client = DataPactClient() 
client.run_validation(
    config=config, 
    job_name="In-Databricks Validation Run", 
    warehouse_name="My Serverless Warehouse"
)
