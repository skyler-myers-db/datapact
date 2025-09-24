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

## Quick Start
1. **Install prerequisites**
   ```bash
   python3.13 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -e .
   ```
2. **Verify Databricks CLI profile** with access to a Serverless SQL warehouse (set `--warehouse`, `DATAPACT_WAREHOUSE`, or `datapact_warehouse` in `~/.databrickscfg`).
3. **Seed the demo**
   ```bash
   python demo/setup.py --profile <PROFILE> --warehouse <SERVERLESS_WH>
   ```
4. **Run DataPact**
   ```bash
   datapact run \
     --config demo/demo_config.yml \
     --job-name "DataPact Enterprise Demo" \
     --profile <PROFILE> \
     --warehouse <SERVERLESS_WH>
   ```
5. **Open Lakeview** – copy the dashboard URL from the CLI output and explore the ROI dashboards.

---

## Prerequisites
- Python **3.13.5+**
- Databricks workspace with Lakeview & Serverless SQL access
- Databricks CLI authenticated with a PAT or AAD credentials
- Optional: Databricks AI/BI Genie for conversational analysis

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
| Field | Description |
| --- | --- |
| `task_key` | Unique identifier |
| `source_catalog/schema/table`, `target_catalog/schema/table` | Required sources |
| `primary_keys` | List of key columns for joins |
| `count_tolerance` | Relative tolerance (0-1) for row counts |
| `pk_row_hash_check`, `pk_hash_tolerance`, `hash_columns` | Row-level diffing options |
| `null_validation_tolerance`, `null_validation_columns` | Null drift checks |
| `agg_validations` | Aggregate checks (`SUM`, `AVG`, `MIN`, `MAX`) |
| `uniqueness_columns`, `uniqueness_tolerance` | Duplicate detection |
| `business_domain`, `business_owner`, `business_priority` (Critical/High/Medium/Low) | Executive metadata |
| `expected_sla_hours`, `estimated_impact_usd` | SLA/impact instrumentation |

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
