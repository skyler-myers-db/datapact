<br/>
<p align="center">
<img src="https://i.imgur.com/Y4L121k.png" alt="DataPact Animated Demo">
</p>
<br/>

# DataPact 🚀

**Stop letting silent data errors compromise your BI dashboards, ML models, and critical business decisions.**

DataPact is an enterprise-grade, programmatic data validation accelerator for Databricks. It ensures the integrity of your data pipelines by creating a declarative "pact" between your source and target tables. It programmatically generates, runs, and reports on a suite of validation tests directly within your Databricks workspace, enabling **reliable, scalable, and observable data quality assurance.**

---

### The Business Value: Why DataPact?

| Feature                      | Business Value                                                                                                                                                                                                  |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Automated Observability**  | Every run automatically creates or updates a rich Lakeview Dashboard. Get immediate, zero-maintenance insights into data quality trends, failure rates, and hotspots, turning raw data into business intelligence. |
| **Declarative & Auditable**  | Define your entire validation suite in a simple `YAML` file. This provides a human-readable, version-controllable audit trail of your data quality rules, perfect for governance and compliance.                  |
| **CI/CD Native**             | Built for modern data platforms. Seamlessly integrate DataPact into your CI/CD pipelines (GitHub Actions, Azure DevOps) to prevent bad data from ever reaching production.                                       |
| **Deep Data Forensics**      | Go beyond simple row counts. Perform per-row hash comparisons, multi-column null analysis, and aggregate checks (`SUM`, `AVG`) to pinpoint the exact cause of data corruption.                                      |
| **Efficient & Scalable**     | Built for performance and cost-efficiency. DataPact leverages the power of Databricks SQL Serverless, automatically scaling to handle billions of rows while minimizing operational overhead.                   |
| **Persistent Reporting**     | Automatically log detailed validation results to a Delta table. The results are stored in a `VARIANT` column, allowing for easy, powerful, and native querying of your data quality history.                  |

---

### Core Validation Suite

DataPact provides a rich suite of validations to cover the most critical data quality dimensions.

| Validation               | **Business Question It Answers**                                                              | **Example Configuration**                                                                                                                                |
| ------------------------ | --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Count Validation**     | _"Did we lose or gain a significant number of records during our ETL process?"_                | <pre lang="yaml">task_key: validate_users<br>...<br>count_tolerance: 0.01</pre>                                                                          |
| **Row Hash Check**       | _"Have any of our supposedly identical records been subtly corrupted or changed?"_             | <pre lang="yaml">task_key: validate_products<br>...<br>primary_keys: [product_id]<br> pk_row_hash_check: true<br> pk_hash_threshold: 0.05</pre>           |
| **Selective Hashing**    | _"How can we check for data integrity on critical columns while ignoring frequently changing ones like timestamps?"_ | <pre lang="yaml">task_key: validate_events<br>...<br>primary_keys: [event_id]<br> pk_row_hash_check: true<br> hash_columns: [user_id, event_type]</pre> |
| **Aggregate Validation** | _"Has the total revenue, average order value, or max transaction ID changed beyond an acceptable threshold?"_ | <pre lang="yaml">task_key: validate_finance<br>...<br>agg_validations:<br>  - column: "total_revenue"<br>    validations: [{agg: SUM, tolerance: 0.005}]</pre>   |
| **Null Count Validation**| _"Has a recent upstream change caused a spike in NULL values in our critical identifier or attribute columns?"_ | <pre lang="yaml">task_key: validate_customers<br>...<br> null_validation_threshold: 0.02<br> null_validation_columns: [email, country]</pre>       |
| **Uniqueness Validation**| _"Are key columns unique (e.g., no duplicate emails or IDs) within each side?"_ | <pre lang="yaml">task_key: validate_users<br>...<br> uniqueness_columns: [email]<br> uniqueness_threshold: 0.0</pre> |

---


### How It Works: Architecture

DataPact simplifies complex data validation into a clean, automated workflow within your existing Databricks environment.

```
+--------------+     +-------------------+     +---------------------+     +--------------------+
|   User CLI   | --> |  Databricks API   | --> |   Databricks Job    | --> |  SQL Warehouse     |
| (datapact run) |     (Submits Job)     |     |  (Multi-Task)       |     | (Executes SQL)     |
+--------------+     +-------------------+     +----------+----------+     +----------+---------+
                                                          |                     |
                                                          |                     |
+----------------------+     +------------------+     +------------------+     +--------------------+
|  Lakeview Dashboard  | <-- |  Dashboard Task  | <-- | Aggregation Task | <-- |  Results Delta Table |
|   (View Results)     |     |   (Refreshes)    |     | (Checks & Fails) |     |  (Stores History)  |
+----------------------+     +------------------+     +------------------+     +--------------------+
```

---

### See DataPact in Action: The Comprehensive Demo

See DataPact's full potential with a realistic, large-scale demo. This will create 12 tables with millions of rows and run a full validation suite, showcasing all advanced features.

#### Prerequisites

1.  **Databricks Workspace:** A Databricks workspace with Unity Catalog enabled.
2.  **Permissions:** Permissions to create catalogs, schemas, tables, and run jobs.
3.  **Python >= 3.13.5:** A local Python environment.
4.  **Databricks CLI:** [Install and configure the Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html). For a seamless experience, we recommend adding a `datapact_warehouse` key to your profile in `~/.databrickscfg`.

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

#### Step 2: Create and Activate a Virtual Environment (Python 3.13.5+)

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

#### Step 2: Set Up the Demo Environment

This script connects to your workspace and creates millions of rows of realistic data across 12 tables.
```bash
# This will use the warehouse defined in your ~/.databrickscfg
python demo/setup.py --profile your-profile
```

#### Step 3: Run the Demo & Create the Dashboard

Execute DataPact using the pre-made demo configuration. This command will create a new Databricks Job, run all 12 validation tasks, and generate your interactive results dashboard.


```bash
datapact run \
  --config demo/demo_config.yml \
  --job-name "DataPact Enterprise Demo" \
  --profile your-profile
```

**That's it!** A link to your new, populated dashboard will be printed in the console. You get:

* ✅ Performance on tables with millions of rows.
* ✅ A mix of intentionally PASSING and FAILING tasks.
* ✅ Advanced features like accepted change thresholds (pk_hash_threshold).
* ✅ Performance tuning with selective column hashing (hash_columns).
* ✅ Detailed aggregate validations (SUM, MAX).
* ✅ In-depth null-count validation (null_validation_columns).
* ✅ Optional uniqueness checks for key columns (uniqueness_columns, threshold).
* ✅ Graceful handling of edge cases like empty tables and tables without primary keys.

---

### Using DataPact on Your Own Data

1.  **Create Your Config:** Create a `my_validations.yml` file based on the examples and the parameter table below.
2.  **Run DataPact:**

    **Quickest Start** (uses defaults from `~/.databrickscfg`):
    ```bash
    datapact run --config my_validations.yml --job-name "My Data Validation"
    ```

    **Custom Configuration:**
    ```bash
    datapact run \
      --config my_validations.yml \
      --job-name "Production Finance Validation" \
      --warehouse "prod_serverless_wh" \
      --results-table "governance.reporting.datapact_history"
    ```
---

### Project Roadmap

We are committed to evolving DataPact into a comprehensive data governance platform.

-   **✅ V1.0: Headless Validation Engine.** Core programmatic validation engine.
-   **✅ V2.0: Automated Observability.** Auto-generated Lakeview Dashboards.
-   **🚀 V3.0: UI-Driven Configuration.** A Databricks App to allow non-engineers to create validation configs through a guided UI.
-   **🚀 V4.0: The Integrated Workbench.** A single, embedded app to configure, run, and view DataPact results without leaving the UI.
-   **🚀 V5.0: Proactive Governance.** A `datapact init` command to auto-scaffold tests for an entire schema, plus integrated Databricks SQL Alerting for real-time failure notifications.

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
| `uniqueness_columns`      | list[string] | No       | Columns that must be unique within source and within target.                         |
| `uniqueness_threshold`    | float        | No       | Allowed duplicate ratio (e.g., 0.0 = strict no duplicates).                          |
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

If you provide the `--results-table` argument, DataPact will write a detailed summary of every validation task to the specified Delta table. This allows you to build dashboards in Databricks SQL to monitor data quality trends over time. Otherwise, it will write to the default location: `datapact.results.run_history`.

The autogenerated Lakeview dashboard now includes basic filters for job_name and run_id on both pages. Use these to quickly narrow the view to a specific job or run.

#### Simplified Configuration with Environment Variables

For convenience, you can set your warehouse and profile as environment variables to avoid typing them in every command (more details above).

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
