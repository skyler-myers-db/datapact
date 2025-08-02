# DataPact: Technology & Product Roadmap

This document outlines the strategic vision for DataPact, evolving it from a powerful command-line utility into a seamless, enterprise-grade data governance platform integrated directly within the Databricks ecosystem.

---

### **Version 1.0: The Headless Validation Engine (Completed)**

*   **Core Theme:** Foundational, automatable, and programmatic data validation.
*   **Executive Pitch:** "Version 1.0 establishes the core of DataPact as a robust, CI/CD-friendly engine. It allows our data teams to programmatically define and execute complex data quality checks, ensuring that every automated data pipeline is validated against a strict 'pact.' This is our automated safety net for data integrity."

*   **Key Features & Technical Implementation:**
    1.  **CLI-Driven Execution:** The system is anchored by a Python-based command-line interface (`datapact/main.py`) using the `argparse` library. This allows for easy integration into any script or automated workflow.
    2.  **Declarative YAML Configuration:** All validations are defined in a simple, human-readable `.yml` file. This decouples the validation logic from the code, allowing data stewards to define tests without being Python programmers.
    3.  **Dynamic SQL Generation:** The core engine (`DataPactClient._generate_validation_sql`) is the product's "secret sauce." It does not use static checks; instead, it dynamically constructs complex, pure-SQL scripts on the fly based on the YAML configuration. This includes:
        *   **Count Validation:** Generates SQL `COUNT(1)` subqueries for both source and target, comparing them within a given float tolerance.
        *   **Row Hash Validation:** For a given list of primary keys, it generates SQL that computes a checksum of all specified columns (`md5(to_json(struct(*)))`) for each row. It then performs an `INNER JOIN` on the primary keys to find hash mismatches, providing surgical precision on data corruption.
        *   **Aggregate & Null Validations:** Dynamically builds Common Table Expressions (CTEs) for each configured aggregate (`SUM`, `AVG`) or null check, ensuring all validation logic is executed in a single, efficient SQL statement per task.
    4.  **Databricks Job Orchestration:** The client programmatically creates and runs a multi-task Databricks Job.
        *   **Task Isolation:** Each validation task in the YAML becomes an independent `SqlTask` in the job, allowing for parallel execution and granular failure analysis.
        *   **Ephemeral SQL Scripts:** The dynamically generated SQL scripts are uploaded to a user-specific path in the Databricks Workspace (`/Users/{user}/datapact/job_assets/...`) for the job to execute, ensuring no permanent clutter.
    5.  **Centralized, Auditable Results:** All results are written to a user-defined Delta table (`run_history`), creating a permanent, queryable audit log of all data quality checks. The `result_payload` is stored as a `VARIANT`, allowing for easy JSON parsing in Databricks SQL.
    6.  **Fail-Fast Aggregation Task:** A final, dependent task (`aggregate_results`) runs only after all validation tasks are complete. It queries the results table for any record marked `'FAILURE'` for the current `run_id`. If failures are found, it uses the `RAISE_ERROR()` SQL function to explicitly fail the entire Databricks Job, providing a clear, unambiguous signal to CI/CD systems.

*   **Outcome:** A powerful, headless engine that provides robust, automatable data quality assurance.

---

### **Version 2.0: Automated Observability (Completed)**

*   **Core Theme:** Transforming raw data into actionable, visual insights with zero manual effort.
*   **Executive Pitch:** "DataPact no longer just tells us *if* a job failed; it now tells us *why*. Version 2.0 automatically generates and refreshes a rich analytics dashboard for every validation suite. This provides immediate, zero-maintenance observability into our data quality trends, hotspots, and historical performance, turning raw data into business intelligence."

*   **Key Features & Technical Implementation:**
    1.  **Dashboard-as-Code:** The dashboard is not created manually. The `DataPactClient.ensure_dashboard_exists` method programmatically defines the entire dashboard as a complex JSON structure.
    2.  **Idempotent Creation:** On each run, the client first checks if a dashboard with the target name exists. If so, it is deleted and recreated. This ensures that any updates to the dashboard's structure in the DataPact code are immediately reflected on the next run.
    3.  **Serialized Payload Generation:** The core of this feature is the dynamic construction of the `serialized_dashboard` JSON string. This process involves:
        *   **Defining Datasets:** Creating a list of JSON objects, where each object defines a `name` and the `queryLines` for a SQL query that will power one or more visualizations (e.g., the `ds_kpi` dataset).
        *   **Defining Layout & Widgets:** Building a `layout` list containing widget objects. Each widget definition is a JSON object that specifies:
            *   `position`: The exact `x`, `y`, `width`, and `height` coordinates on the dashboard's 12-unit grid system, ensuring a balanced and professional layout.
            *   `queries`: A link back to one of the defined `datasets`.
            *   `spec`: The most critical part. This defines the visualization type (`pie`, `line`, `counter`), the `encodings` that map data fields to visual properties (e.g., `x-axis`, `y-axis`, `color`), and the all-important `scale` property that tells the chart how to interpret the data (`quantitative`, `categorical`, `temporal`).
    4.  **Robust API Interaction:** The entire JSON payload is passed to the `w.lakeview.create(Dashboard(...))` constructor. This proven method, using keyword arguments like `display_name` and `parent_path`, correctly creates the dashboard object in the workspace.
    5.  **Dedicated Dashboard Refresh Task:** A final `DashboardTask` is added to the Databricks Job. It depends on the `aggregate_results` task and its sole purpose is to trigger a refresh of the Lakeview Dashboard, ensuring the visualizations always reflect the latest run data.

*   **Outcome:** A complete, end-to-end solution that not only validates data but also provides immediate, rich, and actionable insights with zero operational overhead.

---

### **Version 3.0: UI-Driven Configuration with Databricks Apps**

*   **Core Theme:** Democratizing data quality by lowering the technical barrier to entry.
*   **Executive Pitch:** "We are empowering our data stewards and analysts to take ownership of data quality. With the new DataPact App, non-engineers can now define comprehensive validation suites through a guided, point-and-click interface directly within Databricks, without ever writing a line of YAML."
*   **Key Features & Technical Implementation:**
    1.  **Develop a Databricks App:** Create a simple web application (using Streamlit or Flask) that can be run as a Databricks App.
    2.  **Interactive UI:** The app will guide the user through a wizard:
        *   **Connection:** Select a warehouse.
        *   **Table Selection:** Using `w.tables.list()`, populate dropdowns to allow users to select their source and target catalogs, schemas, and tables.
        *   **Column Introspection:** Once a table is selected, use `w.tables.get()` to fetch its schema and display column names in multi-select boxes for defining `primary_keys` and `hash_columns`.
        *   **Rule Configuration:** Provide intuitive UI elements (sliders for tolerance, checkboxes for checks) to configure the validation rules.
    3.  **YAML Generation:** A "Generate" button will take the state of the UI and convert it into a perfectly formatted YAML string, which is displayed in a text box for the user to copy.

*   **Outcome:** Drastically increased adoption of DataPact across the organization. Data quality becomes a shared responsibility, not just an engineering task.

---

### **Version 4.0: The Integrated DataPact Workbench**

*   **Core Theme:** Creating a seamless, "single pane of glass" experience for the entire data quality lifecycle.
*   **Executive Pitch:** "We've eliminated all context switching. The DataPact Workbench provides a fully integrated environment where our teams can define rules, execute validations, and analyze the interactive results dashboard, all within a single application inside Databricks. This maximizes efficiency and provides an unparalleled user experience."
*   **Key Features & Technical Implementation:**
    1.  **Embed the Dashboard:** Enhance the Databricks App from V3. Add a new tab or section that uses an `iframe` to embed the Lakeview Dashboard generated by DataPact. The dashboard URL will be dynamically constructed using the `job_name`.
    2.  **In-App Job Execution:** Add a "Run Validation" button to the app. This button will use the `databricks.sdk` to trigger the `datapact run` job via the `w.jobs.run_now()` method.
    3.  **Live Status Polling:** After triggering a run, the app will poll the `w.jobs.get_run()` endpoint to show the live status of the job directly in the UI (e.g., "Pending," "Running," "Success," "Failed").

*   **Outcome:** A frictionless, end-to-end workflow that solidifies DataPact as a fully-fledged platform, not just a CLI tool.

---

### **Version 5.0: Proactive Governance & Automated Alerting**

*   **Core Theme:** Shifting from on-demand validation to a proactive, automated data governance framework.
*   **Executive Pitch:** "DataPact now functions as our always-on data guardian. It automatically discovers and sets up monitoring for new datasets, and through its new real-time alerting capability, it notifies our teams of critical data failures the moment they happen. We no longer find problems; the problems find us, dramatically reducing our time-to-resolution."
*   **Key Features & Technical Implementation:**
    1.  **`datapact init` Command:** Implement the new CLI command.
        *   It will connect to Databricks and use `information_schema` queries to list all tables within a given schema.
        *   It will further query `information_schema.table_constraints` and `key_column_usage` to intelligently infer primary keys.
        *   It will then generate a complete `datapact_config.yml` file with a validation task for every table, providing instant, 100% test coverage for a schema.
    2.  **SQL Alert Integration:**
        *   The `aggregate_results.sql` script will be modified. In the failure path, in addition to `RAISE_ERROR`, it will `INSERT` a summary record (job_name, run_id, failed_tasks) into a new, dedicated table named `datapact_main.results.alert_history`.
        *   **Programmatic Alert Creation:** A new command, `datapact setup-alert`, will use `w.alerts.create()` to create a Databricks SQL Alert. This alert will be configured to query the `alert_history` table every 5 minutes.
        *   **Alert Destination:** The alert will be configured to use a user-provided Alert Destination (e.g., a Slack webhook URL or email address), ensuring immediate notification of failures to the responsible team.

*   **Outcome:** DataPact becomes a fully proactive, automated governance solution that minimizes risk, reduces manual toil, and provides an enterprise-grade safety net for all critical data.
