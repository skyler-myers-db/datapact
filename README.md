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

---

### Live Demo in 5 Minutes

See DataPact in action with a realistic, high volume dataset without using your own data.

#### Prerequisites

1.  **Databricks Workspace:** A Databricks workspace with Unity Catalog enabled.
2.  **Permissions:** Permissions to create catalogs, schemas, tables, and run jobs.
3.  **Python >= 3.10:** A local Python environment.
4.  **Databricks CLI:** Authenticate your local machine with your workspace:
    ```bash
    pip install databricks-cli
    databricks configure --profile my-profile
    ```

#### Step 1: Install DataPact

Clone this repository and install the package in editable mode.

```bash
git clone <your-repo-url>
cd datapact
pip install -e .
```

#### Step 2: Set Up the Demo Environment

Run the included setup script from your terminal. This will connect to your Databricks workspace and run a pure SQL script to create a new catalog (`datapact_demo_catalog`) with 10,000 users and 50,000 transactions, including several intentional discrepancies for DataPact to find.

```bash
python demo/setup.py --warehouse "Your Serverless Warehouse Name" --profile my-profile
```

#### Step 3: Run the Demo Validation

Execute DataPact using the pre-made demo configuration file. This run will intentionally show both passing (`transactions`) and failing (`users`) tests, giving you a real sense of the tool's output.

```bash
datapact run \
  --config demo/demo_config.yml \
  --warehouse "Your Serverless Warehouse Name" \
  --job-name "DataPact Demo Run" \
  --profile my-profile
```

 That's it! You will see the validation results streamed to your terminal.

 ---

### Using DataPact on Your Own Data

1.  **Create Your Config:** Create a `my_validations.yml` file. Use the `demo/demo_config.yml` as a template.
2.  **Run DataPact:**
    ```bash
    datapact run \
      --config my_validations.yml \
      --warehouse "Your Serverless Warehouse Name" \
      --job-name "My Production Validation" \
      --results-table "main.reporting.datapact_results" \
      --create-warehouse
    ```

### Configuration Details

Below are all available parameters for each task in your `validation_config.yml`:

| Parameter                 | Type         | Required | Description                                                                          |
|---------------------------|--------------|----------|--------------------------------------------------------------------------------------|
| `task_key`                | string       | Yes      | A unique identifier for the validation task.                                         |
| `source_catalog`          | string       | Yes      | The Unity Catalog name for your source system.                                       |
| `source_schema`           | string       | Yes      | The source schema name.                                                              |
| `source_table`            | string       | Yes      | The source table name.                                                               |
| `target_catalog`          | string       | Yes      | The target Unity Catalog name (e.g., `main`).                                        |
| `target_schema`           | string       | Yes      | The target schema name.                                                              |
| `target_table`            | string       | Yes      | The target table name.                                                               |
| `primary_keys`            | list[string] | No       | List of primary key columns, required for hash checks.                               |
| `count_tolerance`         | float        | No       | Allowed relative difference for row counts (e.g., `0.01` for 1%). Defaults to `0.0`. |
| `pk_row_hash_check`       | boolean      | No       | If `true`, performs a per-row hash comparison. Requires `primary_keys`.              |
| `pk_hash_threshold`       | float        | No       | Allowed ratio of mismatched hashes. Requires `pk_row_hash_check`. Defaults to `0.0`. |
| `hash_columns`            | list[string] | No       | Specific columns to include in the row hash. If omitted, all columns are used.       |
| `null_validation_threshold` | float        | No       | Allowed relative difference for null counts in a column.                             |
| `null_validation_columns` | list[string] | No       | List of columns to perform null count validation on. Requires `null_validation_threshold`. |
| `agg_validations`         | list[dict]   | No       | A list of aggregate validations to perform. See structure in examples.               |

---

### Results & Reporting

If you provide the `--results-table` argument, DataPact will write a detailed summary of every validation task to the specified Delta table. This allows you to build dashboards in Databricks SQL to monitor data quality trends over time.
