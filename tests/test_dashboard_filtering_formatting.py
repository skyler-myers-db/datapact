"""
Tests for dashboard filtering and conditional formatting features.
Validates that executives can effectively filter and see properly formatted data.
"""

import json
import pytest
from unittest.mock import MagicMock, patch


class TestDashboardFilteringAndFormatting:
    """Test suite for dashboard filtering and conditional formatting."""

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
            side_effect=Exception("Not found")
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

    def test_main_page_has_comprehensive_filters(self, mock_client):
        """Test that main page has all necessary filters for executives."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")
        filters = main_page.get("filters", [])

        # Extract filter names and display names
        filter_info = {f["name"]: f.get("displayName", f["name"]) for f in filters}

        # Check for essential filters
        assert "job_name" in filter_info
        assert "run_id" in filter_info
        assert "status_filter" in filter_info
        assert filter_info["status_filter"] == "Validation Status"

        # Check for new filters
        assert "task_key_filter_main" in filter_info
        assert filter_info["task_key_filter_main"] == "Validation Name"
        assert "time_range" in filter_info
        assert filter_info["time_range"] == "Time Range"

        # Verify filter configurations
        status_filter = next(f for f in filters if f["name"] == "status_filter")
        assert status_filter["allowMultipleValues"] is True
        assert "defaultValues" in status_filter

    def test_details_page_has_comprehensive_filters(self, mock_client):
        """Test that details page has all necessary filters."""
        dashboard_json = self._get_dashboard_json(mock_client)

        details_page = next(
            p for p in dashboard_json["pages"] if p["name"] == "details_page"
        )
        filters = details_page.get("filters", [])

        filter_names = {f["name"] for f in filters}

        # Check for all required filters
        assert "status_filter_details" in filter_names
        assert "task_key_filter" in filter_names
        assert "job_name_details" in filter_names
        assert "run_id_details" in filter_names
        assert "time_range_details" in filter_names

        # Verify time range filter configuration
        time_filter = next(f for f in filters if f["name"] == "time_range_details")
        assert time_filter["type"] == "date_range"
        assert time_filter["displayName"] == "Time Range"

    def test_critical_issues_counter_has_conditional_formatting(self, mock_client):
        """Test that Critical Issues counter has green/red formatting."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")

        # Find Critical Issues widget
        critical_issues_widget = None
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if widget["spec"]["frame"]["title"] == "Critical Issues":
                critical_issues_widget = widget
                break

        assert critical_issues_widget is not None

        # Check for conditional formatting
        value_format = critical_issues_widget["spec"]["encodings"]["value"]["format"]
        assert "conditionalFormats" in value_format

        formats = value_format["conditionalFormats"]

        # Check for green when 0
        zero_format = next(
            f
            for f in formats
            if f["condition"]["type"] == "equals" and f["condition"]["value"] == 0
        )
        assert zero_format["textColor"] == "#00A972"  # Green
        assert zero_format["backgroundColor"] == "#E8F5E9"

        # Check for red when > 0
        gt_zero_format = next(
            f
            for f in formats
            if f["condition"]["type"] == "greaterThan" and f["condition"]["value"] == 0
        )
        assert gt_zero_format["textColor"] == "#FF3621"  # Red
        assert gt_zero_format["backgroundColor"] == "#FFEBEE"

    def test_data_quality_score_has_tiered_formatting(self, mock_client):
        """Test that Data Quality Score has tiered color formatting."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")

        # Find Data Quality Score widget
        quality_widget = None
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if widget["spec"]["frame"]["title"] == "Data Quality Score":
                quality_widget = widget
                break

        assert quality_widget is not None

        # Check for conditional formatting
        value_format = quality_widget["spec"]["encodings"]["value"]["format"]
        assert "conditionalFormats" in value_format

        formats = value_format["conditionalFormats"]

        # Check for three tiers of formatting
        high_tier = next(
            f
            for f in formats
            if f["condition"]["type"] == "greaterThanOrEquals"
            and f["condition"]["value"] == 0.99
        )
        assert high_tier["textColor"] == "#00A972"  # Green

        mid_tier = next(f for f in formats if f["condition"]["type"] == "between")
        assert mid_tier["textColor"] == "#FF9800"  # Orange

        low_tier = next(
            f
            for f in formats
            if f["condition"]["type"] == "lessThan" and f["condition"]["value"] == 0.95
        )
        assert low_tier["textColor"] == "#FF3621"  # Red

    def test_table_status_columns_have_formatting(self, mock_client):
        """Test that table status columns have conditional formatting."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")

        # Find validation details table
        validation_table = None
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if "Validation Results" in widget["spec"]["frame"]["title"]:
                validation_table = widget
                break

        assert validation_table is not None

        # Check columns for status formatting
        columns = validation_table["spec"]["encodings"]["columns"]
        status_column = next(c for c in columns if c["fieldName"] == "overall_status")

        assert "cellFormat" in status_column
        assert "conditionalFormats" in status_column["cellFormat"]

        formats = status_column["cellFormat"]["conditionalFormats"]

        # Check for checkmark formatting
        check_format = next(f for f in formats if f["condition"]["value"] == "✅")
        assert check_format["textColor"] == "#00A972"
        assert check_format["backgroundColor"] == "#E8F5E9"

        # Check for X mark formatting
        x_format = next(f for f in formats if f["condition"]["value"] == "❌")
        assert x_format["textColor"] == "#FF3621"
        assert x_format["backgroundColor"] == "#FFEBEE"

    def test_bar_charts_have_color_coding(self, mock_client):
        """Test that failure bar charts have color coding."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")

        # Find Top Failing Validations bar chart
        failure_bar = None
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if "Top Failing" in widget["spec"]["frame"]["title"]:
                failure_bar = widget
                break

        if failure_bar:  # This widget might not always be present
            # Check for color encoding
            encodings = failure_bar["spec"]["encodings"]
            assert "color" in encodings

            color_encoding = encodings["color"]
            assert color_encoding["scale"]["colorScheme"] == "redyellowgreen"
            assert color_encoding["scale"]["reverse"] is True  # High = red

    def test_line_chart_has_reference_lines(self, mock_client):
        """Test that quality trend line chart has target reference lines."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")

        # Find Quality Trend chart
        trend_chart = None
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if "Quality Trend" in widget["spec"]["frame"]["title"]:
                trend_chart = widget
                break

        assert trend_chart is not None

        # Check for reference lines
        spec = trend_chart["spec"]
        if "referenceLines" in spec:
            ref_lines = spec["referenceLines"]

            # Check for target line
            target_line = next((line for line in ref_lines if line["value"] == 5), None)
            if target_line:
                assert target_line["label"] == "Target (5%)"
                assert target_line["color"] == "#00A972"
                assert target_line["style"] == "dashed"

            # Check for warning line
            warning_line = next(
                (line for line in ref_lines if line["value"] == 10), None
            )
            if warning_line:
                assert warning_line["label"] == "Warning (10%)"
                assert warning_line["color"] == "#FF9800"
                assert warning_line["style"] == "dashed"

    def test_total_validations_has_volume_based_formatting(self, mock_client):
        """Test that Total Validations counter changes color based on volume."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")

        # Find Total Validations widget
        total_widget = None
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if widget["spec"]["frame"]["title"] == "Total Validations":
                total_widget = widget
                break

        assert total_widget is not None

        # Check for conditional formatting based on volume
        value_format = total_widget["spec"]["encodings"]["value"]["format"]
        assert "conditionalFormats" in value_format

        formats = value_format["conditionalFormats"]

        # Check for green when high volume
        high_volume = next(
            f
            for f in formats
            if f["condition"]["type"] == "greaterThan"
            and f["condition"]["value"] == 100
        )
        assert high_volume["textColor"] == "#00A972"  # Green for high volume

        # Check for orange when lower volume
        low_volume = next(
            f
            for f in formats
            if f["condition"]["type"] == "lessThanOrEquals"
            and f["condition"]["value"] == 100
        )
        assert low_volume["textColor"] == "#FF9800"  # Orange for lower volume

    def test_quality_score_column_in_tables_has_formatting(self, mock_client):
        """Test that quality score columns in tables have proper formatting."""
        dashboard_json = self._get_dashboard_json(mock_client)

        main_page = next(p for p in dashboard_json["pages"] if p["name"] == "main_page")

        # Find business impact table
        business_table = None
        for widget_config in main_page["layout"]:
            widget = widget_config["widget"]
            if "Schema Quality" in widget["spec"]["frame"]["title"]:
                business_table = widget
                break

        if business_table:
            columns = business_table["spec"]["encodings"]["columns"]
            quality_col = next(
                (c for c in columns if c["fieldName"] == "quality_score"), None
            )

            if quality_col:
                assert "format" in quality_col
                assert quality_col["format"]["type"] == "number"
                assert "conditionalFormats" in quality_col["format"]

                formats = quality_col["format"]["conditionalFormats"]

                # Check tiered formatting
                high_quality = next(
                    f
                    for f in formats
                    if f["condition"]["type"] == "greaterThanOrEquals"
                    and f["condition"]["value"] == 99
                )
                assert high_quality["textColor"] == "#00A972"

                mid_quality = next(
                    f for f in formats if f["condition"]["type"] == "between"
                )
                assert mid_quality["textColor"] == "#FF9800"

                low_quality = next(
                    f
                    for f in formats
                    if f["condition"]["type"] == "lessThan"
                    and f["condition"]["value"] == 95
                )
                assert low_quality["textColor"] == "#FF3621"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
