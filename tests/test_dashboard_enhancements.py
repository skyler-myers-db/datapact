"""
Comprehensive tests for dashboard visualization enhancements and bug fixes.
This test module validates all dashboard-related fixes including:
- Status column accuracy in Validation Results
- Source/Target column null handling
- Aggregation validation status logic
- Schema column display in Business Impact
- UDTF alias handling in exploded checks
- Run details naming and filtering
- Result payload column display
"""

import json
import pytest
from unittest.mock import MagicMock, patch
from databricks.sdk.errors import NotFound


class TestDashboardVisualizationFixes:
    """Test suite for all dashboard visualization fixes."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock DataPactClient with necessary workspace methods."""
        with patch("datapact.client.WorkspaceClient"):
            from datapact.client import DataPactClient

            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.lakeview = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test/path"

            return client

    @pytest.fixture
    def sample_result_payload(self):
        """Sample result payload with all validation types."""
        return {
            "source_catalog": "test_catalog",
            "source_schema": "test_schema",
            "source_table": "test_table",
            "target_catalog": "target_catalog",
            "target_schema": "target_schema",
            "target_table": "target_table",
            "count_validation": {
                "source_count": "1,000,000",
                "target_count": "999,995",
                "relative_diff_percent": "0.0005%",
                "tolerance_percent": "1.00%",
                "status": "PASS",
            },
            "row_hash_validation": {
                "source_count": "1,000,000",
                "target_count": "999,995",
                "missing_count": "5",
                "relative_diff_percent": "0.0005%",
                "tolerance_percent": "0.00%",
                "status": "FAIL",
            },
            "null_validation_email": {
                "source_nulls": "10",
                "target_nulls": "10",
                "relative_diff_percent": "0.00%",
                "status": "PASS",
            },
            "uniqueness_validation_email": {
                "source_duplicates": "0",
                "target_duplicates": "5",
                "status": "FAIL",
            },
            "agg_validation_amount_SUM": {
                "source_value": "8,434,332,207.93",
                "target_value": "8,434,332,207.93",
                "relative_diff_percent": "0.00%",
                "status": "PASS",
            },
        }

    def _get_dashboard_json(self, mock_client):
        """Helper to setup mocks and get dashboard JSON."""
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results",
            warehouse_id="test_warehouse",
        )

        create_call = mock_client.w.lakeview.create.call_args
        dashboard_obj = create_call[0][0]
        return json.loads(dashboard_obj.serialized_dashboard)

    def test_validation_results_status_accuracy(
        self, mock_client, sample_result_payload
    ):
        """Test that Status column correctly reflects actual validation results."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Find the validation details dataset
        datasets = dashboard_json["datasets"]
        validation_details = next(
            d for d in datasets if d["name"] == "ds_validation_details"
        )

        # Verify the status logic uses simple CASE statement
        query = " ".join(validation_details["queryLines"])

        # Check for simplified status logic
        assert "CASE status" in query
        assert "WHEN 'SUCCESS' THEN '✅'" in query
        assert "WHEN 'FAILURE' THEN '❌'" in query

    def test_source_target_columns_null_handling(self, mock_client):
        """Test that Source and Target columns handle null values properly."""
        dashboard_json = self._get_dashboard_json(mock_client)

        datasets = dashboard_json["datasets"]
        validation_details = next(
            d for d in datasets if d["name"] == "ds_validation_details"
        )

        query = " ".join(validation_details["queryLines"])

        # Verify null handling for source and target tables
        assert (
            "CONCAT_WS('.', source_catalog, source_schema, source_table) AS source_table"
            in query
        )
        assert (
            "CONCAT_WS('.', target_catalog, target_schema, target_table) AS target_table"
            in query
        )

    def test_agg_validation_status_logic(self, mock_client):
        """Test aggregation validation status shows PASS when values match."""
        dashboard_json = self._get_dashboard_json(mock_client)

        datasets = dashboard_json["datasets"]
        validation_details = next(
            d for d in datasets if d["name"] == "ds_validation_details"
        )

        query = " ".join(validation_details["queryLines"])

        # Verify simplified agg validation check logic
        assert (
            """CASE WHEN to_json(result_payload) LIKE '%agg_validation%' AND to_json(result_payload) NOT LIKE '%agg_validation%"status":"FAIL"%' THEN '✅ Aggs'"""
            in query
        )
        assert (
            """WHEN to_json(result_payload) LIKE '%agg_validation%"status":"FAIL"%' THEN '❌ Aggs'"""
            in query
        )
        assert (
            """CASE WHEN to_json(result_payload) LIKE '%custom_sql_validation%' AND to_json(result_payload) NOT LIKE '%custom_sql_validation%"status":"FAIL"%' THEN '✅ Custom SQL'"""
            in query
        )
        assert (
            """WHEN to_json(result_payload) LIKE '%custom_sql_validation%"status":"FAIL"%' THEN '❌ Custom SQL'"""
            in query
        )

    def test_schema_column_display_not_unknown(self, mock_client):
        """Test Schema column shows actual schema instead of 'Unknown'."""
        dashboard_json = self._get_dashboard_json(mock_client)

        datasets = dashboard_json["datasets"]
        business_impact = next(d for d in datasets if d["name"] == "ds_business_impact")

        query = " ".join(business_impact["queryLines"])

        # Verify the dataset targets the latest run snapshot via timestamp filtering
        assert "WITH latest_run_ts AS" in query
        assert "AND run_id IN (SELECT run_id FROM latest_runs)" in query
        assert "business_domain" in query

    def test_history_dataset_surfaces_primary_keys(self, mock_client):
        """Historical dataset should expose configured primary keys for quick lookup."""
        dashboard_json = self._get_dashboard_json(mock_client)
        datasets = dashboard_json["datasets"]
        history_ds = next(d for d in datasets if d["name"] == "ds_history")
        query = " ".join(history_ds["queryLines"])
        assert "CAST(result_payload:configured_primary_keys AS STRING)" in query
        assert "trim(CAST(result_payload:applied_filter AS STRING))" in query

        details_page = next(
            p for p in dashboard_json["pages"] if p["name"] == "details_page"
        )
        details_widget = next(
            w for w in details_page["layout"] if w["widget"]["name"] == "details_table"
        )
        column_fields = [
            col["fieldName"]
            for col in details_widget["widget"]["spec"]["encodings"]["columns"]
        ]
        assert "configured_primary_keys" in column_fields

    def test_udtf_alias_fix_in_exploded_checks(self, mock_client):
        """Test UDTF alias mismatch error is fixed in exploded checks."""
        dashboard_json = self._get_dashboard_json(mock_client)

        datasets = dashboard_json["datasets"]
        exploded_checks = next(
            (d for d in datasets if d["name"] == "ds_exploded_checks"), None
        )

        if exploded_checks:
            query = " ".join(exploded_checks["queryLines"])

            # Should use dynamic extraction for aggregations with from_json
            assert "from_json(to_json(result_payload), 'map<string,string>')" in query
            assert "WHERE key LIKE 'agg_validation_%'" in query
            assert "regexp_extract(key, 'agg_validation_(.*)', 1)" in query
            assert "WHERE key LIKE 'custom_sql_validation_%'" in query
            assert "regexp_extract(key, 'custom_sql_validation_(.*)', 1)" in query

    def test_run_details_naming_reflects_all_runs(self, mock_client):
        """Test that 'Latest Run Details' is renamed to reflect it shows all runs."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Also check the widget title in the details page
        pages = dashboard_json["pages"]
        details_page = next(p for p in pages if p["name"] == "details_page")
        details_table = details_page["layout"][0]["widget"]
        assert details_table["spec"]["frame"]["title"] == "All Run Details"

    def test_status_filter_added_to_run_details(self, mock_client):
        """Test Status filter is added to run details visualization."""
        dashboard_json = self._get_dashboard_json(mock_client)

        pages = dashboard_json["pages"]

        # Check main page has status filter
        main_page = next(p for p in pages if p["name"] == "main_page")
        status_filters = [
            f
            for f in main_page.get("filters", [])
            if "status" in f.get("field", "").lower()
            or f.get("name", "") == "status_filter"
        ]
        assert len(status_filters) > 0

        # Check details page has status filter
        details_page = next(p for p in pages if p["name"] == "details_page")
        details_filters = details_page.get("filters", [])
        status_filter_details = next(
            (f for f in details_filters if f["name"] == "status_filter_details"), None
        )
        assert status_filter_details is not None
        assert status_filter_details["field"] == "status"
        assert status_filter_details["allowMultipleValues"] is True

    def test_result_payload_column_not_empty(self, mock_client):
        """Test Result Payload column is properly populated."""
        dashboard_json = self._get_dashboard_json(mock_client)

        datasets = dashboard_json["datasets"]

        # Check ds_latest_run_details dataset
        latest_run_details = next(
            d for d in datasets if d["name"] == "ds_latest_run_details"
        )
        query = " ".join(latest_run_details["queryLines"])
        assert "to_json(result_payload) as result_payload" in query

        # Check the details page widget
        pages = dashboard_json["pages"]
        details_page = next(p for p in pages if p["name"] == "details_page")
        details_table = details_page["layout"][0]["widget"]

        # Verify result_payload is in the columns (shown as payload_json in the widget)
        columns = details_table["spec"]["encodings"]["columns"]
        column_fields = [col["fieldName"] for col in columns]
        assert "payload_json" in column_fields


class TestExecutiveDashboardPerspective:
    """Tests from an executive stakeholder perspective."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock DataPactClient."""
        with patch("datapact.client.WorkspaceClient"):
            from datapact.client import DataPactClient

            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.lakeview = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test/path"

            return client

    def _get_dashboard_json(self, mock_client):
        """Helper to setup mocks and get dashboard JSON."""
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results",
            warehouse_id="test_warehouse",
        )

        create_call = mock_client.w.lakeview.create.call_args
        dashboard_obj = create_call[0][0]
        return json.loads(dashboard_obj.serialized_dashboard)

    def test_executive_kpis_prominently_displayed(self, mock_client):
        """Test that key executive KPIs are prominently displayed."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Check main page widgets
        main_page = dashboard_json["pages"][0]
        widgets_at_top = []

        for widget_config in main_page["layout"]:
            position = widget_config["position"]
            if position["y"] == 0:  # Top row
                widget = widget_config["widget"]
                title = widget["spec"]["frame"]["title"]
                widgets_at_top.append(title)

        # Executive KPIs should be at the top
        assert "Data Quality Score" in widgets_at_top
        assert "Critical Issues" in widgets_at_top
        assert "Total Validations" in widgets_at_top
        assert "Peak Parallelism" in widgets_at_top
        assert "Throughput (tasks/min)" in widgets_at_top

    def test_clear_visual_indicators_for_status(self, mock_client):
        """Test that status indicators use clear visual symbols."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Check datasets for visual indicators
        datasets = dashboard_json["datasets"]

        # Check validation details
        validation_details = next(
            d for d in datasets if d["name"] == "ds_validation_details"
        )
        query = " ".join(validation_details["queryLines"])

        # Should use checkmarks and X marks
        assert "✅" in query
        assert "❌" in query

        # Check business impact for health indicators
        business_impact = next(d for d in datasets if d["name"] == "ds_business_impact")
        impact_query = " ".join(business_impact["queryLines"])

        # Should have color-coded health status
        assert "🟢 Excellent" in impact_query
        assert "🟡 Good" in impact_query
        assert "🟠 Fair" in impact_query
        assert "🔴 Needs Attention" in impact_query

    def test_percentage_formatting_for_clarity(self, mock_client):
        """Test that percentages are formatted clearly for executives."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Check KPI dataset
        kpi_dataset = next(
            d for d in dashboard_json["datasets"] if d["name"] == "ds_kpi"
        )
        query = " ".join(kpi_dataset["queryLines"])

        # Should format percentages properly
        assert "success_rate_percent" in query
        assert "* 100.0 / COUNT(*)" in query

        # Check counter widget formats
        main_page = dashboard_json["pages"][0]
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if widget["spec"]["widgetType"] == "counter":
                title = widget["spec"]["frame"]["title"]
                if "Data Quality Score" in title:
                    # Should use percentage format
                    assert (
                        widget["spec"]["encodings"]["value"]["format"]["type"]
                        == "number-percent"
                    )

    def test_actionable_insights_for_executives(self, mock_client):
        """Test that dashboard provides actionable insights."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Check for failure classification
        datasets = dashboard_json["datasets"]
        failures_dataset = next(
            d for d in datasets if d["name"] == "ds_failures_by_type"
        )
        query = " ".join(failures_dataset["queryLines"])

        # Should categorize failures for action
        assert "Row Count Mismatch" in query
        assert "Data Integrity Issue" in query
        assert "Data Completeness" in query
        assert "Business Rule Violation" in query

        # Check for top failures widget
        main_page = dashboard_json["pages"][0]
        widget_titles = []
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            title = widget["spec"]["frame"].get("title", "")
            widget_titles.append(title)

        assert "Top Failing Validations" in widget_titles
        assert "Issue Classification" in widget_titles
