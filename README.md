# DataPact 🚀

**An enterprise grade, programmatic data validation accelerator for Databricks.**

DataPact ensures the integrity of your data by creating a 'pact' between your source and target tables. It programmatically generates, runs, and reports on a suite of validation tests directly within your Databricks workspace, enabling reliable, scalable, and observable data quality assurance.

---

### Why DataPact?

*   **Built-in Analytics Dashboard:** Every run automatically creates or updates a rich Databricks SQL Dashboard, visualizing data quality trends, failure rates, and the most common issues. Get immediate, powerful insights with zero effort.
*   **Zero-Config Start:** Run validations instantly. DataPact automatically creates a default catalog and table to store your results history.
*   **100% Programmatic:** Define your entire validation suite in a simple `YAML` file. Create, run, and manage tests from your local CLI or a CI/CD pipeline. No UI clicking required.
*   **Fully Serverless:** Built for efficiency. DataPact uses powerful Serverless SQL Warehouses for all operations, minimizing cost and operational overhead.
*   **Rich Validations:** Go beyond simple row counts. DataPact supports aggregate comparisons (SUM, AVG, MAX), per-row hash validation, and multi-column null count analysis.
*   **Persistent, Queryable Reporting:** Automatically log detailed validation results to a Delta table. The results are stored in a `VARIANT` column, allowing for easy, powerful, and native querying of your data quality history in Databricks SQL.

---

### Configuration: The 3 Tiers

DataPact intelligently finds your SQL warehouse in the following order of precedence:

1.  **`--warehouse` Flag (Highest Priority):** A warehouse specified directly on the command line will always be used.

    ```bash
    datapact run --config ... --warehouse "my_cli_warehouse"
    ```
    
3.  **`DATAPACT_WAREHOUSE` Environment Variable:** If the flag is not present, DataPact will look for this environment variable.

    ```bash
    export DATAPACT_WAREHOUSE="my_env_var_warehouse"
    datapact run --config ...
    ```
    
5.  **`.databrickscfg` File (Recommended Default):** If neither of the above is found, DataPact looks for a `datapact_warehouse` key inside your active Databricks CLI profile (`~/.databrickscfg`). This is the recommended way to set your default warehouse.

    **Example `~/.databrickscfg` entry:**
    
    ```ini
    [my-profile]
    host = https://dbc-....cloud.databricks.com
    token = dap...
    datapact_warehouse = "Your Serverless Warehouse Name"
    ```

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
    host = https://dbc-....cloud.databricks.com
    token = dap...
    datapact_warehouse = "Your Serverless Warehouse Name"
    ```

#### Step 1: Clone the Repository

```bash
git clone https://github.com/skyler-myers-db/datapact.git
cd datapact
```

#### Step 2: Create and Activate a Virtual Environment

This creates a self-contained environment to avoid conflicts with other Python projects.

```bash
# For macOS/Linux
python -m venv .venv
source .venv/bin/activate

# For Windows
python -m venv .venv
.venv\Scripts\activate
```

#### Step 3: Install DataPact

Install the package and its dependencies in editable mode.

```bash
pip install -e .
```

#### Step 4: Set Up the Demo Environment

Run the included setup script from your terminal. This will connect to your Databricks workspace and create the demo data. The script will use the warehouse defined in your `.databrickscfg` or `DATAPACT_WAREHOUSE` environment variable if the `--warehouse` parameter is not provided.

```bash
python demo/setup.py --profile my-profile
```

Alternatively, you can provide the warehouse directly:

```bash
python demo/setup.py --warehouse "Your Serverless Warehouse Name" --profile your-profile
```

#### Step 5: Run the Demo and Create the Dashboard

Execute DataPact using the pre-made comprehensive demo configuration. This run will showcase:

* ✅ Performance on tables with millions of rows.
* ✅ A mix of intentionally PASSING and FAILING tasks.
* ✅ Advanced features like accepted change thresholds (pk_hash_threshold).
* ✅ Performance tuning with selective column hashing (hash_columns).
* ✅ Detailed aggregate validations (SUM, MAX).
* ✅ In-depth null-count validation (null_validation_columns).
* ✅ Graceful handling of edge cases like empty tables and tables without primary keys.

```bash
# Assumes 'datapact_warehouse' is set in your profile
datapact run \
  --config demo/demo_config.yml \
  --job-name "DataPact Comprehensive Demo" \
  --profile your-databricks-profile
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
      --results-table "main.reporting.datapact_results" \
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
