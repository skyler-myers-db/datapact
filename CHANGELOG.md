# In CHANGELOG.md

# Changelog

All notable changes to this project will be documented in this file.

## [2.5.0] - 2025-09-15

### ✨ Added (New Features)

- **Better Visualizations:** Much better visualizations with 3 tabs for different topics, including job run metrics.
- **New Dashboard Theme:** The dashboard has a new, custom, aesthetic theme that makes it stand out and easy to read.
- **Dashboard Filters:** Now you can filter some of the visualizations based on the test results.

### 🐛 Fixed

- Some of the visualiztion results were not coming in properly.
- The filter for the most recent job run was on `run_id`, which is not a reliable indicator of the latest run (it has been switched to `completed_at` combined with `run_id`).

## [2.0.0] - 2025-08-02

### ✨ Added (New Features)

- **Automated Observability Dashboard:** DataPact now automatically creates and refreshes a rich, interactive Databricks Lakeview Dashboard for every validation job.
- **KPI Header:** The dashboard includes a high-level summary of total tasks, failed tasks, and the overall success rate for at-a-glance insights.
- **Polished Visualizations:** Charts now include custom axis labels, strategic color-coding (red for failures, green for successes), and a balanced, professional layout.
- The `ensure_dashboard_exists` method was completely refactored to programmatically generate the dashboard payload, ensuring reliability and a polished end-user experience.

### 🐛 Fixed

- Resolved a series of critical API errors related to the creation of Lakeview dashboards via the SDK.
- Corrected dashboard layout issues that caused skewed and misaligned visualizations.

## [1.0.0] - 2025-05-20

### ✨ Added
- Initial release of the DataPact validation engine.
- Support for Count, Row Hash, Aggregate, and Null validations.
- CLI for running validations and orchestrating Databricks Jobs.
