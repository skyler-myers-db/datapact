"""
Tests for metadata extraction and performance dashboard improvements.
Validates that metadata is outside payload, timestamps are tracked,
and performance metrics are available.
"""

import json
import pytest
from unittest.mock import MagicMock, patch
import importlib


class TestMetadataAndPerformanceImprovements:
    """Test suite for metadata extraction and performance improvements."""

    def test_metadata_moved_outside_payload(self):
        """Test that source/target metadata is outside the result payload."""
        jinja2 = importlib.import_module("jinja2")
        env = jinja2.Environment(
            loader=jinja2.PackageLoader("datapact", "templates"),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
            extensions=["jinja2.ext.do"],
        )
        template = env.get_template("validation.sql.j2")

        payload = {
            "task_key": "test",
            "source_catalog": "src_cat",
            "source_schema": "src_sch",
            "source_table": "src_tbl",
            "target_catalog": "tgt_cat",
            "target_schema": "tgt_sch",
            "target_table": "tgt_tbl",
            "count_tolerance": 0.01,
            "results_table": "results",
            "job_name": "test_job",
        }

        sql = template.render(**payload)

        # Check metadata is in SELECT but not in struct
        assert "'src_cat' AS source_catalog," in sql
        assert "'src_sch' AS source_schema," in sql
        assert "'src_tbl' AS source_table," in sql
        assert "'tgt_cat' AS target_catalog," in sql
        assert "'tgt_sch' AS target_schema," in sql
        assert "'tgt_tbl' AS target_table," in sql

        # Should not be in the parse_json struct
        assert (
            "'src_cat' AS source_catalog"
            not in sql.split("parse_json(to_json(struct(")[1].split("))")[0]
        )

    def test_timestamps_added_as_columns(self):
        """Test that started_at and completed_at are top-level columns."""
        jinja2 = importlib.import_module("jinja2")
        env = jinja2.Environment(
            loader=jinja2.PackageLoader("datapact", "templates"),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
            extensions=["jinja2.ext.do"],
        )
        template = env.get_template("validation.sql.j2")

        payload = {
            "task_key": "test",
            "source_catalog": "c",
            "source_schema": "s",
            "source_table": "a",
            "target_catalog": "c",
            "target_schema": "s",
            "target_table": "b",
            "count_tolerance": 0.01,
            "results_table": "results",
            "job_name": "test_job",
        }

        sql = template.render(**payload)

        # Check timestamps are in SELECT
        assert "current_timestamp() AS started_at" in sql
        assert "current_timestamp() AS completed_at" in sql

        # Check they're in the INSERT statement
        assert "started_at, completed_at" in sql

    def test_insert_statement_includes_all_columns(self):
        """Test that INSERT statement includes all new metadata columns."""
        jinja2 = importlib.import_module("jinja2")
        env = jinja2.Environment(
            loader=jinja2.PackageLoader("datapact", "templates"),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
            extensions=["jinja2.ext.do"],
        )
        template = env.get_template("validation.sql.j2")

        payload = {
            "task_key": "test",
            "source_catalog": "c",
            "source_schema": "s",
            "source_table": "a",
            "target_catalog": "c",
            "target_schema": "s",
            "target_table": "b",
            "count_tolerance": 0.01,
            "results_table": "`catalog`.`schema`.`results`",
            "job_name": "test_job",
        }

        sql = template.render(**payload)

        # Check INSERT has all columns
        insert_line = [line for line in sql.split("\n") if "INSERT INTO" in line][0]
        assert "task_key" in insert_line
        assert "status" in insert_line
        assert "run_id" in insert_line
        assert "job_id" in insert_line
        assert "job_name" in insert_line
        assert "timestamp" in insert_line
        assert "started_at" in insert_line
        assert "completed_at" in insert_line
        assert "source_catalog" in insert_line
        assert "source_schema" in insert_line
        assert "source_table" in insert_line
        assert "target_catalog" in insert_line
        assert "target_schema" in insert_line
        assert "target_table" in insert_line
        assert "result_payload" in insert_line

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

    def test_performance_metrics_page_exists(self, mock_client):
        """Test that Performance Metrics dashboard page is created."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Check that performance page exists
        pages = dashboard_json["pages"]
        page_names = {p["name"] for p in pages}

        assert "performance_page" in page_names

        # Find performance page
        perf_page = next(p for p in pages if p["name"] == "performance_page")
        assert perf_page["displayName"] == "Performance Metrics"

    def test_performance_datasets_exist(self, mock_client):
        """Test that performance metric datasets are created."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Check datasets
        dataset_names = {ds["name"] for ds in dashboard_json["datasets"]}

        assert "ds_performance_metrics" in dataset_names
        assert "ds_job_performance" in dataset_names
        assert "ds_runtime_trend" in dataset_names

    def test_performance_queries_use_timestamps(self, mock_client):
        """Test that performance queries use started_at and completed_at."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Find performance datasets
        perf_metrics = next(
            d
            for d in dashboard_json["datasets"]
            if d["name"] == "ds_performance_metrics"
        )
        query = " ".join(perf_metrics["queryLines"])

        # Should use started_at and completed_at
        assert "started_at" in query
        assert "completed_at" in query
        assert "unix_timestamp(completed_at) - unix_timestamp(started_at)" in query

    def test_bar_chart_labels_are_clean(self, mock_client):
        """Test that bar charts don't show 'sum(failure_count)' labels."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Check all bar charts
        for page in dashboard_json["pages"]:
            for widget_def in page.get("layout", []):
                widget = widget_def.get("widget", {})
                spec = widget.get("spec", {})

                if spec.get("widgetType") == "bar":
                    encodings = spec.get("encodings", {})
                    y_encoding = encodings.get("y", {})

                    # Check that display name is not the raw field name
                    display_name = y_encoding.get("displayName", "")
                    y_encoding.get("fieldName", "")

                    # Display name should be user-friendly
                    assert "sum(" not in display_name.lower()
                    assert display_name in ["Failures", "Count", "Failure Count"]

    def test_exploded_checks_details_standardized(self, mock_client):
        """Test that exploded checks have consistent detail formatting."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Find exploded checks dataset
        exploded = next(
            d for d in dashboard_json["datasets"] if d["name"] == "ds_exploded_checks"
        )
        query = " ".join(exploded["queryLines"])

        # Check for consistent separators (using |)
        assert " | Target: " in query
        assert " | Diff: " in query
        assert " | Tolerance: " in query or " | Threshold: " in query

        # Should not use commas as separators
        assert ", Target: " not in query
        assert ", Diff: " not in query

    def test_exploded_checks_shows_all_aggregations(self, mock_client):
        """Test that exploded checks dynamically finds all aggregation validations."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Find exploded checks dataset
        exploded = next(
            d for d in dashboard_json["datasets"] if d["name"] == "ds_exploded_checks"
        )
        query = " ".join(exploded["queryLines"])

        # Should use dynamic extraction for aggregations
        assert "from_json(to_json(result_payload), 'map<string,string>')" in query
        assert "WHERE key LIKE 'agg_validation_%'" in query
        assert "regexp_extract(key, 'agg_validation_(.*)', 1)" in query

    def test_validation_details_uses_new_metadata_columns(self, mock_client):
        """Test that validation details queries use new metadata columns."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Find latest run details dataset
        latest_details = next(
            d
            for d in dashboard_json["datasets"]
            if d["name"] == "ds_latest_run_details"
        )
        query = " ".join(latest_details["queryLines"])

        # Should use direct columns, not JSON extraction for metadata
        assert "CONCAT(source_catalog, '.', source_schema, '.', source_table)" in query
        assert "CONCAT(target_catalog, '.', target_schema, '.', target_table)" in query

        # Should not extract these from payload
        assert (
            "get_json_object(to_json(result_payload), '$.source_catalog')" not in query
        )

    def test_business_impact_uses_new_schema_column(self, mock_client):
        """Test that business impact assessment uses the new schema column."""
        dashboard_json = self._get_dashboard_json(mock_client)

        # Find business impact dataset
        business_impact = next(
            d for d in dashboard_json["datasets"] if d["name"] == "ds_business_impact"
        )
        query = " ".join(business_impact["queryLines"])

        # Should use direct column
        assert "SELECT source_schema" in query
        assert "GROUP BY source_schema" in query

        # Should not extract from payload
        assert (
            "get_json_object(to_json(result_payload), '$.source_schema')" not in query
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
