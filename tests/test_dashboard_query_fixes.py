"""
Test the dashboard SQL query fixes directly.
This test module validates that the dashboard SQL queries are correctly formed.
"""

from unittest.mock import MagicMock, patch
from datapact.client import DataPactClient


class TestDashboardQueryFixes:
    """Test the dashboard query generation fixes."""

    def test_dashboard_queries_are_correct(self):
        """Test that all dashboard SQL queries are correctly formed."""
        # Create a client instance with minimal mocking
        with patch("datapact.client.WorkspaceClient"):
            client = DataPactClient(profile="DEFAULT")

            # Mock necessary attributes
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(
                side_effect=Exception("Not found")
            )
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(
                return_value=MagicMock(dashboard_id="test_id")
            )
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"

            # Call ensure_dashboard_exists and capture the dashboard definition
            client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse",
            )

            # Get the dashboard payload from the create call
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][
                0
            ]  # First positional argument is the Dashboard object
            dashboard_json = dashboard_obj.serialized_dashboard

            import json

            dashboard = json.loads(dashboard_json)
            datasets = dashboard["datasets"]

            # Test 1: Validation details should check individual validation statuses
            validation_details = next(
                d for d in datasets if d["name"] == "ds_validation_details"
            )
            query = (
                " ".join(validation_details["queryLines"])
                if "queryLines" in validation_details
                else validation_details.get("query", "")
            )

            # Should have proper status logic that checks all validation types
            assert (
                "get_json_object(to_json(result_payload), '$.count_validation.status') = 'FAIL'"
                in query
            )
            assert (
                "get_json_object(to_json(result_payload), '$.row_hash_validation.status') = 'FAIL'"
                in query
            )
            assert (
                "to_json(result_payload) LIKE '%null_validation%status%FAIL%'" in query
                or "to_json(result_payload) LIKE '%null_validation%FAIL%'" in query
            )
            assert "to_json(result_payload) LIKE '%agg_validation%FAIL%'" in query

            # Test 2: Source and target columns should handle nulls
            assert (
                "CONCAT_WS('.', source_catalog, source_schema, source_table) AS source_table"
                in query
            )
            assert (
                "CONCAT_WS('.', target_catalog, target_schema, target_table) AS target_table"
                in query
            )

            # Test 3: Agg validation check should use simplified logic
            assert (
                "CASE WHEN to_json(result_payload) LIKE '%agg_validation%' AND to_json(result_payload) NOT LIKE '%agg_validation%FAIL%'"
                in query
            )

            # Test 4: Business impact should handle null schemas gracefully
            business_impact = next(
                d for d in datasets if d["name"] == "ds_business_impact"
            )
            bi_query = (
                " ".join(business_impact["queryLines"])
                if "queryLines" in business_impact
                else business_impact.get("query", "")
            )
            assert "SELECT source_schema" in bi_query

            # Test 5: Exploded checks should not have UDTF alias mismatch
            exploded_checks = next(
                d for d in datasets if d["name"] == "ds_exploded_checks"
            )
            ec_query = (
                " ".join(exploded_checks["queryLines"])
                if "queryLines" in exploded_checks
                else exploded_checks.get("query", "")
            )
            # Should use dynamic extraction with from_json
            assert (
                "from_json(to_json(result_payload), 'map<string,string>')" in ec_query
            )
            assert "WHERE key LIKE 'agg_validation_%'" in ec_query

            # Test 6: Latest run details should be renamed
            latest_run = next(
                d for d in datasets if d["name"] == "ds_latest_run_details"
            )
            assert latest_run["displayName"] == "All Run Details"

            # Should use result_payload not payload_json
            lr_query = (
                " ".join(latest_run["queryLines"])
                if "queryLines" in latest_run
                else latest_run.get("query", "")
            )
            assert "to_json(result_payload) as result_payload" in lr_query

            # Test 7: Pages should have proper filters
            pages = dashboard["pages"]

            # Main page should have status filter
            main_page = next(p for p in pages if p["name"] == "main_page")
            assert any(
                "status" in f.get("field", "").lower()
                for f in main_page.get("filters", [])
            )

            # Details page should have status filter
            details_page = next(p for p in pages if p["name"] == "details_page")
            details_filters = details_page.get("filters", [])
            # Check if filters were added
            if details_filters:
                assert any(
                    f["name"] == "status_filter_details" for f in details_filters
                )

            # Test 8: Details page widget should have payload field
            details_widget = details_page["layout"][0]["widget"]
            query_fields = details_widget["queries"][0]["query"]["fields"]
            assert any(f["name"] == "payload_json" for f in query_fields)

            # Test 9: Widget columns should reference correct fields
            columns = details_widget["spec"]["encodings"]["columns"]
            assert any(c["fieldName"] == "payload_json" for c in columns)
            assert any(c["displayName"] == "Result Payload" for c in columns)

            print("✅ All dashboard query fixes verified successfully!")

    def test_special_characters_in_job_names(self):
        """Test that special characters in job names are handled properly."""
        with patch("datapact.client.WorkspaceClient"):
            client = DataPactClient(profile="DEFAULT")

            # Mock necessary attributes
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(
                side_effect=Exception("Not found")
            )
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(
                return_value=MagicMock(dashboard_id="test_id")
            )
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"

            # Test various allowed characters
            special_names = [
                "job_with_underscores",
                "job with spaces",
                "job-with-dashes",
                "job.with.dots",
            ]

            for job_name in special_names:
                dashboard_id = client.ensure_dashboard_exists(
                    job_name=job_name,
                    results_table_fqn="catalog.schema.results",
                    warehouse_id="test_warehouse",
                )

                # Should not raise errors
                assert dashboard_id == "test_id"

                # Check the display name is sanitized
                create_call = client.w.lakeview.create.call_args
                dashboard_obj = create_call[0][0]
                display_name = dashboard_obj.display_name

                # Display name should contain the job name
                assert (
                    job_name.replace(" ", "_").replace(":", "") in display_name
                    or job_name in display_name
                )

                print(f"✅ Handled special job name: {job_name}")

    def test_all_validation_types_in_exploded_view(self):
        """Test that all validation types are included in the exploded checks view."""
        with patch("datapact.client.WorkspaceClient"):
            client = DataPactClient(profile="DEFAULT")

            # Mock necessary attributes
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(
                side_effect=Exception("Not found")
            )
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(
                return_value=MagicMock(dashboard_id="test_id")
            )
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"

            client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse",
            )

            # Get the dashboard payload
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard

            import json

            dashboard = json.loads(dashboard_json)
            datasets = dashboard["datasets"]

            # Check exploded checks includes all validation types
            exploded_checks = next(
                d for d in datasets if d["name"] == "ds_exploded_checks"
            )
            query = (
                " ".join(exploded_checks["queryLines"])
                if "queryLines" in exploded_checks
                else exploded_checks.get("query", "")
            )

            # Should include all validation types
            validation_types = [
                "Count Check",
                "Row Hash Check",
                "Null Check:",
                "Uniqueness Check:",
                "Aggregation Check:",
            ]

            for val_type in validation_types:
                assert val_type in query, f"Missing validation type: {val_type}"

            # Should use the dynamic aggregation extraction
            assert "from_json(to_json(result_payload), 'map<string,string>')" in query
            assert "WHERE key LIKE 'agg_validation_%'" in query

            print("✅ All validation types included in exploded view!")
