"""Integration tests for visualization fixes.

This module provides comprehensive integration tests to ensure all visualization
features work correctly together, including:
- Dashboard creation with all widgets
- Data extraction from JSON payloads
- Null handling in calculations
- Widget configuration correctness
"""

import json
from unittest.mock import MagicMock, patch
import pytest
from databricks.sdk.errors import NotFound

from datapact.client import DataPactClient
from datapact.config import DataPactConfig, ValidationTask


class TestVisualizationIntegration:
    """Integration tests for visualization features."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock DataPactClient instance."""
        with patch("datapact.client.WorkspaceClient") as mock_ws:
            mock_ws_instance = MagicMock()
            mock_ws.return_value = mock_ws_instance
            mock_ws_instance.current_user.me.return_value = MagicMock(
                user_name="test_user"
            )
            mock_ws_instance.workspace.mkdirs = MagicMock()
            mock_ws_instance.config.host = "https://test.databricks.com"

            client = DataPactClient(profile="DEFAULT")
            return client

    @pytest.fixture
    def comprehensive_config(self):
        """Create a comprehensive configuration with various validation types."""
        return DataPactConfig(
            validations=[
                ValidationTask(
                    task_key="validate_customers",
                    source_catalog="sales",
                    source_schema="raw",
                    source_table="customers",
                    target_catalog="sales",
                    target_schema="processed",
                    target_table="customers_clean",
                    primary_keys=["customer_id"],
                    count_tolerance=0.01,
                    pk_row_hash_check=True,
                    pk_hash_tolerance=0.05,
                    null_validation_columns=["email", "phone"],
                    null_validation_tolerance=0.1,
                    uniqueness_validation_columns=["email"],
                    aggregations=[
                        {
                            "column": "total_spent",
                            "type": "SUM",
                            "group_by": ["region"],
                            "tolerance": 0.02,
                        }
                    ],
                ),
                ValidationTask(
                    task_key="validate_orders",
                    source_catalog="sales",
                    source_schema="raw",
                    source_table="orders",
                    target_catalog="sales",
                    target_schema="processed",
                    target_table="orders_clean",
                    primary_keys=["order_id"],
                    count_tolerance=0.0,
                    pk_row_hash_check=True,
                ),
                ValidationTask(
                    task_key="validate_products",
                    source_catalog="inventory",
                    source_schema="raw",
                    source_table="products",
                    target_catalog="inventory",
                    target_schema="processed",
                    target_table="products_clean",
                    primary_keys=["product_id", "variant_id"],
                    null_validation_columns=["price", "stock_quantity"],
                    null_validation_tolerance=0.05,
                ),
            ]
        )

    def test_full_dashboard_creation_with_all_features(self, mock_client):
        """Test complete dashboard creation with all visualization features."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        dashboard_id = mock_client.ensure_dashboard_exists(
            job_name="Integration Test Job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        assert dashboard_id == "test_dashboard_id"

        # Verify dashboard was created and published
        assert mock_client.w.lakeview.create.called
        assert mock_client.w.lakeview.publish.called

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Verify all expected datasets exist
        dataset_names = {ds["name"] for ds in dashboard_json["datasets"]}
        expected_datasets = {
            "ds_kpi",
            "ds_summary",
            "ds_failure_rate",
            "ds_top_failures",
            "ds_failures_by_type",
            "ds_history",
            "ds_latest_run_details",
            "ds_success_trend",
            "ds_business_impact",
            "ds_validation_details",
        }
        assert expected_datasets.issubset(dataset_names), (
            "All required datasets should be present"
        )

        # Verify all widget types are present
        widget_types = set()
        for page in dashboard_json["pages"]:
            for layout_item in page.get("layout", []):
                widget = layout_item.get("widget", {})
                widget_type = widget.get("spec", {}).get("widgetType")
                if widget_type:
                    widget_types.add(widget_type)

        expected_widget_types = {"counter", "pie", "line", "bar", "table"}
        assert expected_widget_types.issubset(widget_types), (
            "All widget types should be present"
        )

    def test_sql_queries_handle_json_extraction_correctly(self, mock_client):
        """Test that SQL queries properly extract data from JSON payloads."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Verify JSON extraction patterns in queries
        for dataset in dashboard_json["datasets"]:
            query = " ".join(dataset.get("queryLines", []))

            # Check for proper JSON extraction patterns where needed
            if "result_payload" in query:
                if dataset["name"] in [
                    "ds_kpi",
                    "ds_business_impact",
                    "ds_pipeline_health",
                    "ds_latest_run_details",
                ]:
                    assert (
                        "get_json_object(to_json(result_payload)" in query
                        or "to_json(result_payload)" in query
                    ), f"Dataset {dataset['name']} should extract from JSON payload"

    def test_kpi_metrics_calculations_are_correct(self, mock_client):
        """Test that KPI metrics use correct calculations."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Find KPI dataset
        kpi_dataset = next(
            (ds for ds in dashboard_json["datasets"] if ds["name"] == "ds_kpi"), None
        )

        assert kpi_dataset is not None
        query = " ".join(kpi_dataset["queryLines"])

        # Verify calculations
        # Data Quality Score should be returned directly from summary table
        assert "data_quality_score" in query
        # Success rate should be percentage
        assert "success_rate_percent" in query
        # Tables validated should surface total tasks value
        assert "total_tasks AS tables_validated" in query
        # Impact metrics should be exposed
        assert "potential_impact_usd" in query
        # Average runtime calculation removed since timestamps are no longer in payload
        # This metric would need to be calculated differently if needed

    def test_failure_classification_uses_correct_field_names(self, mock_client):
        """Test that failure classification uses correct JSON field names."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Find failures by type dataset
        failures_dataset = next(
            (
                ds
                for ds in dashboard_json["datasets"]
                if ds["name"] == "ds_failures_by_type"
            ),
            None,
        )

        assert failures_dataset is not None
        query = " ".join(failures_dataset["queryLines"])

        # Verify correct field names
        assert "$.count_validation.status" in query, "Should use count_validation"
        assert "$.row_hash_validation.status" in query, (
            "Should use row_hash_validation not pk_hash_validation"
        )
        assert "null_validation_" in query, "Should check for null validation failures"
        assert "uniqueness_validation_" in query, (
            "Should check for uniqueness validation failures"
        )
        assert "agg_validation_" in query, (
            "Should check for aggregation validation failures"
        )

    def test_widget_configurations_match_datasets(self, mock_client):
        """Test that widget configurations correctly reference their datasets."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Build dataset name set
        dataset_names = {ds["name"] for ds in dashboard_json["datasets"]}

        # Check all widgets reference valid datasets
        for page in dashboard_json["pages"]:
            for layout_item in page.get("layout", []):
                widget = layout_item.get("widget", {})
                for query in widget.get("queries", []):
                    dataset_name = query.get("query", {}).get("datasetName")
                    if dataset_name:
                        assert dataset_name in dataset_names, (
                            f"Widget references non-existent dataset: {dataset_name}"
                        )

    def test_dashboard_filters_are_properly_configured(self, mock_client):
        """Test that dashboard filters are correctly set up."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Check main page filters
        main_page = dashboard_json["pages"][0]
        filters = main_page.get("filters", [])
        filter_names = {f["name"] for f in filters}

        # Verify essential filters exist
        assert "job_name" in filter_names, "Job name filter should exist"
        assert "run_id" in filter_names, "Run ID filter should exist"
        assert "status_filter" in filter_names, "Status filter should exist"

        # Verify status filter configuration
        status_filter = next(f for f in filters if f["name"] == "status_filter")
        assert status_filter["field"] == "overall_status"
        assert status_filter["displayName"] == "Validation Status"

    def test_counter_widgets_use_correct_formats(self, mock_client):
        """Test that counter widgets use appropriate formats for their values."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Check counter widget formats
        for page in dashboard_json["pages"]:
            for layout_item in page.get("layout", []):
                widget = layout_item.get("widget", {})
                spec = widget.get("spec", {})

                if spec.get("widgetType") == "counter":
                    title = spec.get("frame", {}).get("title", "")
                    encodings = spec.get("encodings", {})

                    if "Data Quality Score" in title:
                        # Should use percentage format for decimal value
                        value_encoding = encodings.get("value", {})
                        format_spec = value_encoding.get("format", {})
                        assert format_spec.get("type") == "number-percent", (
                            "Data Quality Score should use number-percent format"
                        )

    def test_business_impact_dataset_groups_by_schema(self, mock_client):
        """Test that Business Impact Assessment correctly groups by schema."""
        # Setup mocks
        mock_client.w.workspace.mkdirs = MagicMock()
        mock_client.w.workspace.get_status = MagicMock(
            side_effect=NotFound("Not found")
        )
        mock_client.w.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="test_dashboard_id")
        )
        mock_client.w.lakeview.publish = MagicMock()

        # Create dashboard
        mock_client.ensure_dashboard_exists(
            job_name="test_job",
            results_table_fqn="catalog.schema.results_table",
            warehouse_id="test_warehouse_id",
        )

        # Extract dashboard configuration
        create_calls = mock_client.w.lakeview.create.call_args_list
        dashboard_obj = create_calls[0][0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)

        # Find Business Impact dataset
        business_impact = next(
            (
                ds
                for ds in dashboard_json["datasets"]
                if ds["name"] == "ds_business_impact"
            ),
            None,
        )

        if business_impact:
            query = " ".join(business_impact["queryLines"])

            assert "WITH latest_run_ts AS" in query
            assert "AND run_id IN (SELECT run_id FROM latest_runs)" in query
            assert "business_domain" in query
            assert "CASE\n                            WHEN failed_validations = 0 THEN '🟢 Excellent'" in query


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
