# DataPact 🚀

**An enterprise grade, programmatic data validation accelerator for Databricks.**

DataPact ensures the integrity of your data by creating a "pact" between your source and target tables. It programmatically generates, runs, and reports on a suite of validation tests directly within your Databricks workspace, enabling reliable and scalable data quality assurance.

---

### Why DataPact?

*   **Zero-Config Start:** Run validations without any setup. DataPact automatically creates a default catalog and table (`datapact_main.results.run_history`) to store your results history.
*   **100% Programmatic:** Define your entire validation suite in a simple YAML file. Create, run, and manage tests from your local CLI or a CI/CD pipeline. No UI clicking required.
*   **Fully Serverless:** Built for efficiency. DataPact uses a local client for orchestration and powerful Serverless SQL Warehouses for all query execution, minimizing cost and operational overhead.
*   **Source Agnostic:** Validate data from any source system connected to Unity Catalog through federation (e.g., PostgreSQL, MySQL, Snowflake) against a Databricks target. Perfect for Databricks-to-Databricks validation (e.g., Bronze vs. Silver).
*   **Scalable & Parallel:** Each table validation runs as a separate, parallel task in a Databricks Job, allowing you to test dozens or hundreds of tables concurrently.
*   **Rich Validations & Reporting:** Go beyond simple row counts. DataPact supports aggregate comparisons, per-row hash validation, and null count analysis. It logs detailed, structured `VARIANT` results to a Delta table for historical analysis, auditing, and building data quality dashboards.

---

### See DataPact in Action: The Comprehensive Demo

See DataPact's full potential with a realistic, large scale demo that showcases all advanced features and edge cases without using your own data.

#### Prerequisites

1.  **Databricks Workspace:** A Databricks workspace with Unity Catalog enabled.
2.  **Permissions:** Permissions to create catalogs, schemas, tables, and run jobs.
3.  **Python >= 3.10:** A local Python environment.
4.  **Databricks CLI:** [Install the Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) and configure it. For a seamless experience, we recommend adding a `datapact_warehouse` key to your profile in `~/.databrickscfg`:
    ```ini
    [my-profile]
    host = ...
    token = ...
    datapact_warehouse = "Your Serverless Warehouse Name"
    ```

#### Step 1: Install DataPact

Clone this repository and install the package in editable mode.
```bash
git clone git@github.com:skyler-myers-db/datapact.git
cd datapact
pip install -e .
```

#### Step 2: Set Up the Demo Environment

```bash
# The script uses the warehouse from your --profile argument
python demo/setup.py --profile my-profile
```

#### Step 3: Run the Validation

Execute DataPact using the pre-made comprehensive demo configuration. This run will showcase:

✅ Performance on millions of rows.
✅ A mix of PASSING and FAILING tasks.
✅ Advanced features like accepted thresholds (pk_hash_threshold) and performance tuning (hash_columns).
✅ Graceful handling of edge cases like empty tables and tables without primary keys.

```bash
# Assumes 'datapact_warehouse' is set in your profile
datapact run \
  --config demo/demo_config.yml \
  --job-name "DataPact Comprehensive Demo" \
  --profile my-profile
```

That's it! You will see the validation results streamed to your terminal, including beautifully formatted payloads for every task, and the final aggregation task will report exactly which validations failed.

 ---

### Using DataPact on Your Own Data

1.  **Create Your Config:** Create a `my_validations.yml` file.
2.  **Run DataPact:**

    To get started quickly without specifying a results table:
    
    ```bash
    datapact run \
      --config my_validations.yml \
      --warehouse "Your Serverless Warehouse Name"
    ```
    
    This will automatically create and log results to `datapact_main.results.run_history`.

    To specify a custom results table:
    
    ```bash
    datapact run \
      --config my_validations.yml \
      --warehouse "Your Serverless Warehouse Name" \
      --results-table "my_catalog.my_schema.my_history"
    ```

That's it! You will see the validation results streamed to your terminal, and a new history table will be created and populated in your Databricks workspace.

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
| `results-table` | string | No | FQN of the results table. If omitted, `datapact_main.results.run_history` is used. |

---

#### Warehouse Configuration (Required)

DataPact needs to know which Serverless SQL Warehouse to run on. It looks for the warehouse name in the following order:

1. --warehouse flag (Highest Priority):

```bash
datapact run --config ... --warehouse "my_cli_warehouse"`
```

2. `.databrickscfg` File: Add a `datapact_warehouse` key to your profile in `~/.databrickscfg`. This is the recommended way to set a project-wide default.

```bash
[my-profile]
host = ...
token = ...
datapact_warehouse = "my_default_warehouse"
```

3. Environment Variable (Lowest Priority):

```bash
export DATAPACT_WAREHOUSE="my_env_var_warehouse"`
```

### Results & Reporting

If you provide the `--results-table` argument, DataPact will write a detailed summary of every validation task to the specified Delta table. This allows you to build dashboards in Databricks SQL to monitor data quality trends over time.

#### Simplified Configuration with Environment Variables (Recommended)

For convenience, you can set your warehouse and profile as environment variables to avoid typing them in every command.

In your terminal (or add to `~/.bash_profile`, `~/.zshrc`):
```bash
export DATAPACT_WAREHOUSE="Your Serverless Warehouse Name"
export DATABRICKS_PROFILE="my-profile"
```

Now you can run DataPact with a much shorter command:

```bash
datapact run \
  --config my_validations.yml \
  --job-name "My Production Validation" \
  --results-table "main.reporting.datapact_results"
```
