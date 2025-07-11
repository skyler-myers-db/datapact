# DataPact 🚀

**An enterprise grade, programmatic data validation accelerator for Databricks.**

DataPact ensures the integrity of your data by creating a "pact" between your source and target tables. It programmatically generates, runs, and reports on a suite of validation tests directly within your Databricks workspace, enabling reliable and scalable data quality assurance.

---

### Why DataPact?

*   **100% Programmatic:** Define your entire validation suite in a simple YAML file. Create, run, and manage tests from your local CLI or a CI/CD pipeline. No UI clicking required.
*   **Fully Serverless:** Built for efficiency. DataPact uses lightweight Serverless Notebooks for orchestration and powerful Serverless SQL Warehouses for query execution, minimizing cost and operational overhead.
*   **Source Agnostic:** Validate data from any source system that can be connected to Unity Catalog through federation (e.g., PostgreSQL, MySQL, Snowflake) against a Databricks target. Also perfect for Databricks-to-Databricks validation (e.g., Bronze vs. Silver).
*   **Scalable & Parallel:** Each table validation runs as a separate, parallel task in a Databricks Job, allowing you to test dozens or hundreds of tables concurrently.
*   **Rich Validations:** Go beyond simple row counts. DataPact supports aggregate comparisons (SUM, AVG), per-row hash validation, null count analysis, and more.
*   **Persistent Reporting:** Automatically log detailed validation results to a Delta table for historical analysis, auditing, and building data quality dashboards.

### Architecture Overview

DataPact operates on a simple but powerful three-layer model:

1.  **Remote Control (CLI):** The `datapact` command-line tool, running on your local machine or in a CI/CD pipeline. It reads your configuration and instructs the Databricks workspace.
2.  **Control Plane (Serverless Notebooks):** The CLI dynamically generates a multi-task Databricks Job. Each task is a Python script that runs on ephemeral, serverless compute. This layer orchestrates the tests and builds the necessary SQL queries.
3.  **Execution Engine (Serverless SQL Warehouse):** The Python control plane sends all data-intensive SQL queries to a specified Serverless SQL Warehouse. This is where the high-performance comparison of your source and target data occurs.

### Quickstart Tutorial

#### Prerequisites

1.  **Databricks Workspace:** A Databricks workspace with Unity Catalog enabled.
2.  **Federated Source:** Your source database (e.g., PostgreSQL, Snowflake, or another Databricks workspace) must be registered as a Federated Catalog in Unity Catalog.
3.  **Permissions:** You need permissions in Databricks to create and run jobs, and to create and use a SQL Warehouse.
4.  **Python >= 3.10:** A local Python environment.
5.  **Databricks CLI:** Authenticate your local machine with your workspace:
    ```bash
    pip install databricks-cli
    databricks configure --profile my-profile
    ```

#### 1. Installation

Install the DataPact package from your project root:

```bash
pip install .
```

#### 2. Create Your Configuration

Create a `validation_config.yml file`. This file defines every test you want to run.

```bash
datapact run \
  --config ./validation_config.yml \
  --warehouse "DataPact Warehouse" \
  --job-name "Daily Data Migration Validation" \
  --results-table "main.reporting.datapact_results" \
  --create-warehouse \
  --profile my-profile
```

DataPact will stream the logs to your terminal and provide a link to the live run in the Databricks UI.

## Configuration Details
Below are all available parameters for each task in validation_config.yml:
Parameter	Type	Required	Description
task_key	string	Yes	A unique identifier for the validation task.
source_catalog	string	Yes	The Unity Catalog name for your source system.
source_schema	string	Yes	The source schema name.
source_table	string	Yes	The source table name.
target_catalog	string	Yes	The target Unity Catalog name (e.g., main).
target_schema	string	Yes	The target schema name.
target_table	string	Yes	The target table name.
primary_keys	list[string]	No	List of primary key columns, required for hash checks.
count_tolerance	float	No	Allowed relative difference for row counts (e.g., 0.01 for 1%). Defaults to 0.0.
pk_row_hash_check	boolean	No	If true, performs a per-row hash comparison. Requires primary_keys.
pk_hash_threshold	float	No	Allowed ratio of mismatched hashes. Requires pk_row_hash_check. Defaults to 0.0.
hash_columns	list[string]	No	Specific columns to include in the row hash. If omitted, all columns are used.
null_validation_threshold	float	No	Allowed relative difference for null counts in a column.
null_validation_columns	list[string]	No	List of columns to perform null count validation on. Requires null_validation_threshold.
agg_validations	list[dict]	No	A list of aggregate validations to perform. See structure in examples.
Results & Reporting
If you provide the --results-table argument, DataPact will write a detailed summary of every validation task to the specified Delta table. This allows you to build dashboards in Databricks SQL to monitor data quality trends over time.
The table will contain the run ID, task key, timestamps, and all metrics collected during the validation.
Development

To contribute to DataPact, clone the repository and install the dependencies in editable mode:

```bash
git clone https://github.com/Entrada-AI/datapact.git
cd datapact
pip install -e .
```
