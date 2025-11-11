<br/>
<p align="center">
<img src="https://i.imgur.com/H5HLT0P.png" alt="DataPact Animated Demo">
</p>
<br/>

# DataPact 🚀

**Executive-grade data validation for the Databricks Lakehouse.** Automate migration testing, keep production pipelines honest, and deliver a Lakeview dashboard that makes executives reach for their budget approvals.

---

## Highlights
- **Executive ROI intelligence** – every run persists curated Delta tables (`exec_run_summary`, `exec_domain_breakdown`, `exec_owner_breakdown`, `exec_priority_breakdown`) that feed impact counters, SLA heatmaps, and accountability views.
- **Declarative, auditable validation** – describe row-count, hash, null, aggregate, and uniqueness checks in YAML. Add business metadata (domain, owner, priority, SLA, impact) to drive storytelling.
- **One-click Lakeview experience** – DataPact publishes an interactive dashboard with ROI metrics, trend lines, remediation drill-downs, and check-level payloads. Genie datasets are ready for conversational analytics.
- **Databricks-first orchestration** – runs natively through Databricks Jobs and Serverless SQL; no clusters to babysit or dashboards to wire manually.
- **Enterprise mega-demo** – spin up 5M customers, 25M+ transactions, 12M streaming telemetry events, 15M digital sessions, an AI feature store, and multi-cloud FinOps telemetry to showcase Databricks-scale validation out of the box.

---

### The Business Value: Why DataPact?

| Feature                     | Business Value                                                                                                                                                                                                     |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Automated Observability** | Every run automatically creates or updates a rich Lakeview Dashboard. Get immediate, zero-maintenance insights into data quality trends, failure rates, and hotspots, turning raw data into business intelligence. |
| **Declarative & Auditable** | Define your entire validation suite in a simple `YAML` file. This provides a human-readable, version-controllable audit trail of your data quality rules, perfect for governance and compliance.                   |
| **CI/CD Native**            | Built for modern data platforms. Seamlessly integrate DataPact into your CI/CD pipelines (GitHub Actions, Azure DevOps) to prevent bad data from ever reaching production.                                         |
| **Deep Data Forensics**     | Go beyond simple row counts. Perform per-row hash comparisons, multi-column null analysis, and aggregate checks (`SUM`, `AVG`) to pinpoint the exact cause of data corruption.                                     |
| **Efficient & Scalable**    | Built for performance and cost-efficiency. DataPact leverages the power of Databricks SQL Serverless, automatically scaling to handle billions of rows while minimizing operational overhead.                      |
| **Persistent Reporting**    | Automatically log detailed validation results to a Delta table. The results are stored in a `VARIANT` column, allowing for easy, powerful, and native querying of your data quality history.                       |


### Core Validation Suite

DataPact provides a rich suite of validations to cover the most critical data quality dimensions.

### Demo highlights

- Multi-column uniqueness aliasing:
  - Config entry: `validate_users_email_country_uniqueness_FAIL` in `demo/demo_config.yml`.
  - What it does: Enforces uniqueness on the composite key `(email, country)` with a strict `uniqueness_tolerance: 0.0`.
  - Where to see it: In the results payload, look for `uniqueness_validation_email_country` within the latest run’s details table on the “Run Details” page. The “Failures by Validation Type” chart also includes a “uniqueness” bucket.

<<<<<<< Updated upstream
| Validation               | **Business Question It Answers**                                                              | **Example Configuration**                                                                                                                                |
| ------------------------ | --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Count Validation**     | _"Did we lose or gain a significant number of records during our ETL process?"_                | <pre lang="yaml">task_key: validate_users<br>...<br>count_tolerance: 0.01</pre>                                                                          |
| **Row Hash Check**       | _"Have any of our supposedly identical records been subtly corrupted or changed?"_             | <pre lang="yaml">task_key: validate_products<br>...<br>primary_keys: [product_id]<br> pk_row_hash_check: true<br> pk_hash_tolerance: 0.05</pre>           |
| **Selective Hashing**    | _"How can we check for data integrity on critical columns while ignoring frequently changing ones like timestamps?"_ | <pre lang="yaml">task_key: validate_events<br>...<br>primary_keys: [event_id]<br> pk_row_hash_check: true<br> hash_columns: [user_id, event_type]</pre> |
| **Aggregate Validation** | _"Has the total revenue, average order value, or max transaction ID changed beyond an acceptable tolerance?"_ | <pre lang="yaml">task_key: validate_finance<br>...<br>agg_validations:<br>  - column: "total_revenue"<br>    validations: [{agg: SUM, tolerance: 0.005}]</pre>   |
| **Null Count Validation**| _"Has a recent upstream change caused a spike in NULL values in our critical identifier or attribute columns?"_ | <pre lang="yaml">task_key: validate_customers<br>...<br> null_validation_tolerance: 0.02<br> null_validation_columns: [email, country]</pre>       |
| **Uniqueness Validation**| _"Are key columns unique (e.g., no duplicate emails or IDs) within each side?"_ | <pre lang="yaml">task_key: validate_users<br>...<br> uniqueness_columns: [email]<br> uniqueness_tolerance: 0.0</pre> |
||||||| Stash base
| Validation               | **Business Question It Answers**                                                              | **Example Configuration**                                                                                                                                |
| ------------------------ | --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Count Validation**     | _"Did we lose or gain a significant number of records during our ETL process?"_                | <pre lang="yaml">task_key: validate_users<br>...<br>count_tolerance: 0.01</pre>                                                                          |
| **Row Hash Check**       | _"Have any of our supposedly identical records been subtly corrupted or changed?"_             | <pre lang="yaml">task_key: validate_products<br>...<br>primary_keys: [product_id]<br> pk_row_hash_check: true<br> pk_hash_tolerance: 0.05</pre>           |
| **Selective Hashing**    | _"How can we check for data integrity on critical columns while ignoring frequently changing ones like timestamps?"_ | <pre lang="yaml">task_key: validate_events<br>...<br>primary_keys: [event_id]<br> pk_row_hash_check: true<br> hash_columns: [user_id, event_type]</pre> |
| **Aggregate Validation** | _"Has the total revenue, average order value, or max transaction ID changed beyond an acceptable tolerance?"_ | <pre lang="yaml">task_key: validate_finance<br>...<br>agg_validations:<br>  - column: "total_revenue"<br>    validations: [{agg: SUM, tolerance: 0.005}]</pre>   |
| **Null Count Validation**| _"Has a recent upstream change caused a spike in NULL values in our critical identifier or attribute columns?"_ | <pre lang="yaml">task_key: validate_customers<br>...<br> null_validation_tolerance: 0.02<br> null_validation_columns: [email, country]</pre>       |
| **Uniqueness Validation**| _"Are key columns unique (e.g., no duplicate emails or IDs) within each side?"_ | <pre lang="yaml">task_key: validate_users<br>...<br> uniqueness_columns: [email]<br> uniqueness_tolerance: 0.0</pre> |
| **Custom SQL Validation**| _"Can I codify bespoke reconciliation logic without wiring a new harness?"_ | <pre lang="yaml">task_key: validate_finance<br>...<br>custom_sql_tests:<br>  - name: "Revenue by Channel"<br>    sql: \|<br>      SELECT channel, SUM(net_revenue) AS total_revenue<br>      FROM {{ table_fqn }}<br>      GROUP BY channel</pre> |
=======
| Validation                | **Business Question It Answers**                                                                                     | **Example Configuration**                                                                                                                                                                                                                         |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Count Validation**      | _"Did we lose or gain a significant number of records during our ETL process?"_                                      | <pre lang="yaml">task_key: validate_users<br>...<br>count_tolerance: 0.01</pre>                                                                                                                                                                   |
| **Row Hash Check**        | _"Have any of our supposedly identical records been subtly corrupted or changed?"_                                   | <pre lang="yaml">task_key: validate_products<br>...<br>primary_keys: [product_id]<br> pk_row_hash_check: true<br> pk_hash_tolerance: 0.05</pre>                                                                                                   |
| **Selective Hashing**     | _"How can we check for data integrity on critical columns while ignoring frequently changing ones like timestamps?"_ | <pre lang="yaml">task_key: validate_events<br>...<br>primary_keys: [event_id]<br> pk_row_hash_check: true<br> hash_columns: [user_id, event_type]</pre>                                                                                           |
| **Aggregate Validation**  | _"Has the total revenue, average order value, or max transaction ID changed beyond an acceptable tolerance?"_        | <pre lang="yaml">task_key: validate_finance<br>...<br>agg_validations:<br>  - column: "total_revenue"<br>    validations: [{agg: SUM, tolerance: 0.005}]</pre>                                                                                    |
| **Null Count Validation** | _"Has a recent upstream change caused a spike in NULL values in our critical identifier or attribute columns?"_      | <pre lang="yaml">task_key: validate_customers<br>...<br> null_validation_tolerance: 0.02<br> null_validation_columns: [email, country]</pre>                                                                                                      |
| **Uniqueness Validation** | _"Are key columns unique (e.g., no duplicate emails or IDs) within each side?"_                                      | <pre lang="yaml">task_key: validate_users<br>...<br> uniqueness_columns: [email]<br> uniqueness_tolerance: 0.0</pre>                                                                                                                              |
| **Custom SQL Validation** | _"Can I codify bespoke reconciliation logic without wiring a new harness?"_                                          | <pre lang="yaml">task_key: validate_finance<br>...<br>custom_sql_tests:<br>  - name: "Revenue by Channel"<br>    sql: \|<br>      SELECT channel, SUM(net_revenue) AS total_revenue<br>      FROM {{ table_fqn }}<br>      GROUP BY channel</pre> |
>>>>>>> Stashed changes

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
* ✅ Advanced features like accepted change tolerance (pk_hash_tolerance).
* ✅ Performance tuning with selective column hashing (hash_columns).
* ✅ Detailed aggregate validations (SUM, MAX).
* ✅ In-depth null-count validation (null_validation_columns).
* ✅ Optional uniqueness checks for key columns (uniqueness_columns, tolerance).
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

<<<<<<< Updated upstream
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
| `pk_hash_tolerance`       | float        | No       | Allowed ratio of mismatched hashes. Requires `pk_row_hash_check`. Defaults to `0.0`. |
| `hash_columns`            | list[string] | No       | Specific columns to include in the row hash. If omitted, all columns are used.       |
| `null_validation_tolerance` | float        | No       | Allowed relative difference for null counts in a column.                             |
| `null_validation_columns` | list[string] | No       | List of columns to perform null count validation on. Requires `null_validation_tolerance`. |
| `agg_validations`         | list[dict]   | No       | A list of aggregate validations to perform. See structure in examples.               |
| `uniqueness_columns`      | list[string] | No       | Columns that must be unique within source and within target.                         |
| `uniqueness_tolerance`    | float        | No       | Allowed duplicate ratio (e.g., 0.0 = strict no duplicates).                          |
| `results-table` | string | No | FQN of the results table. If omitted, `datapact_main.results.run_history` is used. |
||||||| Stash base
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
| `filter`                  | string       | No       | Optional SQL predicate applied to all built-in validations (count/hash/null/agg/uniqueness). Custom SQL tests are unaffected. |
| `count_tolerance`         | float        | No       | Allowed relative difference for row counts (e.g., `0.01` for 1%). Defaults to `0.0`. |
| `pk_row_hash_check`       | boolean      | No       | If `true`, performs a per-row hash comparison. Requires `primary_keys`.              |
| `pk_hash_tolerance`       | float        | No       | Allowed ratio of mismatched hashes. Requires `pk_row_hash_check`. Defaults to `0.0`. |
| `hash_columns`            | list[string] | No       | Specific columns to include in the row hash. If omitted, all columns are used.       |
| `null_validation_tolerance` | float        | No       | Allowed relative difference for null counts in a column.                             |
| `null_validation_columns` | list[string] | No       | List of columns to perform null count validation on. Requires `null_validation_tolerance`. |
| `agg_validations`         | list[dict]   | No       | A list of aggregate validations to perform. See structure in examples.               |
| `uniqueness_columns`      | list[string] | No       | Columns that must be unique within source and within target.                         |
| `uniqueness_tolerance`    | float        | No       | Allowed duplicate ratio (e.g., 0.0 = strict no duplicates).                          |
| `results-table` | string | No | FQN of the results table. If omitted, `datapact_main.results.run_history` is used. |
=======
| Parameter                   | Type         | Required | Description                                                                                                                   |
| --------------------------- | ------------ | -------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `task_key`                  | string       | Yes      | A unique identifier for the validation task.                                                                                  |
| `source_catalog`            | string       | Yes      | The Unity Catalog name for your source system.                                                                                |
| `source_schema`             | string       | Yes      | The source schema name.                                                                                                       |
| `source_table`              | string       | Yes      | The source table name.                                                                                                        |
| `target_catalog`            | string       | Yes      | The target Unity Catalog name (e.g., `main`).                                                                                 |
| `target_schema`             | string       | Yes      | The target schema name.                                                                                                       |
| `target_table`              | string       | Yes      | The target table name.                                                                                                        |
| `primary_keys`              | list[string] | No       | List of primary key columns, required for hash checks.                                                                        |
| `filter`                    | string       | No       | Optional SQL predicate applied to all built-in validations (count/hash/null/agg/uniqueness). Custom SQL tests are unaffected. |
| `count_tolerance`           | float        | No       | Allowed relative difference for row counts (e.g., `0.01` for 1%). Defaults to `0.0`.                                          |
| `pk_row_hash_check`         | boolean      | No       | If `true`, performs a per-row hash comparison. Requires `primary_keys`.                                                       |
| `pk_hash_tolerance`         | float        | No       | Allowed ratio of mismatched hashes. Requires `pk_row_hash_check`. Defaults to `0.0`.                                          |
| `hash_columns`              | list[string] | No       | Specific columns to include in the row hash. If omitted, all columns are used.                                                |
| `null_validation_tolerance` | float        | No       | Allowed relative difference for null counts in a column.                                                                      |
| `null_validation_columns`   | list[string] | No       | List of columns to perform null count validation on. Requires `null_validation_tolerance`.                                    |
| `agg_validations`           | list[dict]   | No       | A list of aggregate validations to perform. See structure in examples.                                                        |
| `uniqueness_columns`        | list[string] | No       | Columns that must be unique within source and within target.                                                                  |
| `uniqueness_tolerance`      | float        | No       | Allowed duplicate ratio (e.g., 0.0 = strict no duplicates).                                                                   |
| `results-table`             | string       | No       | FQN of the results table. If omitted, `datapact_main.results.run_history` is used.                                            |
>>>>>>> Stashed changes

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

For quick context, every result payload also captures the comma-separated `configured_primary_keys`, and the Historical Validation Runs table on the dashboard shows that value in a dedicated “Primary Keys” column. You no longer need to reopen the YAML config to remember which PKs belong to which validation task.

Uniqueness payload aliasing: The result payload for uniqueness is stored under a field name that encodes the validated columns, e.g., `uniqueness_validation_email` for a single field or `uniqueness_validation_email_domain` for multiple fields. This avoids collisions when multiple uniqueness checks exist in one task.

#### How to read payloads

Each validation task writes a JSON payload under `result_payload` with named sections. Examples:

```json
{
  "count_validation": {
    "source_count": "10,000",
    "target_count": "9,830",
    "relative_diff_percent": "1.70%",
    "tolerance_percent": "1.00%",
    "status": "FAIL"
  },
  "null_validation_status": {
    "source_nulls": "0",
    "target_nulls": "200",
    "relative_diff_percent": "—",
    "tolerance_percent": "2.00%",
    "status": "FAIL"
  },
  "agg_validation_total_logins_SUM": {
    "source_value": "100.00",
    "target_value": "110.00",
    "relative_diff_percent": "10.00%",
    "tolerance_percent": "5.00%",
    "status": "FAIL"
  },
  "uniqueness_validation_email_country": {
    "source_duplicates": "0",
    "target_duplicates": "25",
    "source_dupe_percent": "0.00%",
    "target_dupe_percent": "0.25%",
    "tolerance_percent": "0.00%",
    "status": "FAIL"
  }
}
```

Notes:
- Aggregate aliases include the column + aggregate name, e.g., `agg_validation_total_logins_SUM`.
- Null validation aliases include the column validated, e.g., `null_validation_status`.
- Uniqueness aliases include the concatenated columns, e.g., `uniqueness_validation_email_country`.

#### Querying payloads (SQL snippets)

Use Databricks SQL to pull specific validation details from `result_payload`.

Example: latest run for a job with selected fields

```sql
WITH latest AS (
  SELECT MAX(run_id) AS run_id
  FROM datapact.results.run_history
  WHERE job_name = 'DataPact Enterprise Demo'
)
SELECT
  task_key,
  status,
  get_json_object(to_json(result_payload), '$.count_validation.relative_diff_percent') AS count_diff_pct,
  get_json_object(to_json(result_payload), '$.agg_validation_total_logins_SUM.status') AS agg_sum_status,
  get_json_object(to_json(result_payload), '$.null_validation_status.status') AS null_status,
  get_json_object(to_json(result_payload), '$.uniqueness_validation_email_country.status') AS uniqueness_status
FROM datapact.results.run_history r
JOIN latest ON r.run_id = latest.run_id
WHERE job_name = 'DataPact Enterprise Demo'
ORDER BY task_key;
```

Example: trend of uniqueness failures over time

```sql
SELECT
  date(timestamp) AS run_date,
  COUNT(CASE WHEN to_json(result_payload) LIKE '%"uniqueness_validation_"%"status":"FAIL"%' THEN 1 END) AS uniqueness_failures
FROM datapact.results.run_history
WHERE job_name = 'DataPact Enterprise Demo'
GROUP BY 1
ORDER BY 1;
```

Example: list tasks failing any aggregate (SUM) on `total_logins`

```sql
SELECT
  run_id,
  task_key,
  get_json_object(to_json(result_payload), '$.agg_validation_total_logins_SUM.status') AS status,
  get_json_object(to_json(result_payload), '$.agg_validation_total_logins_SUM.relative_diff_percent') AS diff_pct
FROM datapact.results.run_history
WHERE job_name = 'DataPact Enterprise Demo'
  AND get_json_object(to_json(result_payload), '$.agg_validation_total_logins_SUM.status') = 'FAIL'
ORDER BY run_id DESC, task_key;
```

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

---

## Demo Deep Dive
### 1. Data generation (`demo/setup.py`)
- 5M customers, 25M+ transactions, 12M real-time events, 15M clickstream sessions, hyperscale AI feature vectors, FinOps spend snapshots, and dozens of supporting reference tables.
- Intentional corruption patterns (PII masking failures, missing GL postings, telemetry packet loss, hazmat mislabels, bot inflation, etc.).
- Metadata curated to highlight ROI (domain, owner, priority, SLA, impact).

### 2. Validation run (`datapact run`)
Pipeline steps:
1. Results table (`datapact.results.run_history`) is created with the full metadata schema.
2. Validation SQL, aggregate SQL, and Genie setup SQL are uploaded to your workspace.
3. A Databricks Job executes each validation, writes results, and materialises the ROI tables.
4. A Lakeview dashboard is regenerated and published with embedded credentials.
5. Genie datasets are rebuilt (`genie_current_status`, `genie_table_quality`, `genie_issues`).

### Business metadata parameters (optional but powerful)
Every validation in `demo/demo_config.yml` can carry executive-facing context that flows end-to-end through the pipeline. These optional keys make the dashboards and ROI tables instantly actionable:

- `business_domain` – which pillar (Finance, Marketing, Supply Chain, etc.) owns the data.
- `business_owner` – the accountable leader; surfaced in the Owner Accountability table.
- `business_priority` – `CRITICAL`, `HIGH`, `MEDIUM`, or `LOW`; drives the Priority Risk profile.
- `expected_sla_hours` – target remediation window that powers SLA storytelling.
- `estimated_impact_usd` – potential dollar exposure if the check fails; rolls into ROI metrics.

Any combination of these fields can be omitted; when present, they enrich the serialized run history, the Genie-ready tables, and every Lakeview widget that references the executive summary tables.

### 3. Lakeview dashboard
Main page widgets:
- **Data Quality Score**, **Critical Issues**, **Total Validations**, **Peak Parallelism**, **Throughput (tasks/min)**
- **Validation Status Distribution**, **Issue Classification**, **Priority Risk Profile**, **Top Failing Validations**
- **Business Domain Quality Summary** (health status + SLA profile)
- **Owner Accountability** (impact-ranked table)
- **Validation Drill-down** with pass/fail badges, metadata, SLA, and impact per task

Additional pages provide detailed run history, exploded check payloads, and trends.

### 4. ROI tables ready for SQL
```sql
SELECT * FROM datapact.results.exec_run_summary ORDER BY run_id DESC LIMIT 5;
SELECT business_owner, potential_impact_usd FROM datapact.results.exec_owner_breakdown
WHERE run_id = (SELECT MAX(run_id) FROM datapact.results.exec_run_summary)
ORDER BY potential_impact_usd DESC;
```
Use these tables for alerts, custom BI, or compliance auditing.

### 5. Conversational analytics (optional)
Add the Genie tables to a Databricks AI/BI space and seed example prompts:
- “Which owners have the highest realised impact this week?”
- “Show critical domains with SLA at risk.”
- “List validations with potential impact above $250K.”

---

## Using DataPact on Your Data
1. **Author YAML validations** – use `demo/demo_config.yml` as a template and fill in business metadata.
2. **Run DataPact** via CLI or CI/CD.
3. **Share dashboards / query ROI tables** to operationalise insights.

### Validation schema (per task)
| Field                                                                               | Description                                   |
| ----------------------------------------------------------------------------------- | --------------------------------------------- |
| `task_key`                                                                          | Unique identifier                             |
| `source_catalog/schema/table`, `target_catalog/schema/table`                        | Required sources                              |
| `primary_keys`                                                                      | List of key columns for joins                 |
| `count_tolerance`                                                                   | Relative tolerance (0-1) for row counts       |
| `pk_row_hash_check`, `pk_hash_tolerance`, `hash_columns`                            | Row-level diffing options                     |
| `null_validation_tolerance`, `null_validation_columns`                              | Null drift checks                             |
| `agg_validations`                                                                   | Aggregate checks (`SUM`, `AVG`, `MIN`, `MAX`) |
| `uniqueness_columns`, `uniqueness_tolerance`                                        | Duplicate detection                           |
| `business_domain`, `business_owner`, `business_priority` (Critical/High/Medium/Low) | Executive metadata                            |
| `expected_sla_hours`, `estimated_impact_usd`                                        | SLA/impact instrumentation                    |

All metadata flows into dashboards, ROI tables, and Genie datasets.

### Results schema
- **Run history**: `datapact.results.run_history`
- **Derived tables**: `datapact.results.exec_run_summary`, `exec_domain_breakdown`, `exec_owner_breakdown`, `exec_priority_breakdown`
- **Genie tables**: `datapact.results.genie_current_status`, `genie_table_quality`, `genie_issues`

---

## Development
- Activate the virtual environment and install dev extras: `pip install -e .[dev]`
- Run tests (requires Python 3.13.5+): `pytest`
- `pre-commit` hooks optional for lint/format

---

## Roadmap
- **✅ Executive ROI instrumentation (this release)**
- **🚀 V3: UI-driven configuration**
- **🚀 V4: Integrated Lakeview workbench**
- **🚀 V5: Proactive governance & alerting**

Ideas, issues, or contributions are welcome! Reach out: `myers_skyler@icloud.com`

Happy validating! 🎯

### Current Limitations

* The primary hard limitation/requirement of DataPact is that, since all computations are fully run and executed on Databricks, both the source and target data need to be accessible via Unity Catalog. This means that, for data sources outside of Databricks, they need to be available to and configured with [Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/) (or, at least, synced to a Databricks UC table)
* DataPact is currently limited to the preconfigured testing scenarios (counts, hash checks for equality, aggregations, nulls, etc.). A future release will open it up to creating your own custom test cases (such as custom SQL)
