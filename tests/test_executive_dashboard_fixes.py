"""
Comprehensive tests for executive dashboard fixes and enhancements.
Tests all recent fixes from an executive stakeholder perspective.
"""

import json
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import importlib


class TestExecutiveDashboardFixes:
    """Test all executive dashboard fixes and improvements."""
    
    def test_timestamps_removed_from_payload(self):
        """Test that started_at and completed_at are not in the result payload."""
        # This improves executive readability by reducing clutter
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
            "job_name": "test_job"
        }
        
        sql = template.render(**payload)
        
        # Check that started_at and completed_at are not in the struct
        assert "current_timestamp() AS started_at" not in sql
        assert "current_timestamp() AS completed_at" not in sql
        
        # But timestamp should still be in the INSERT statement
        assert "current_timestamp()" in sql
        assert "INSERT INTO" in sql
    
    def test_null_validation_percentage_fix(self):
        """Test that null validation shows 100% when source=0 and target has values."""
        # Critical for executives to see accurate percentage differences
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
            "null_validation_columns": ["col1"],
            "null_validation_threshold": 0.01,
            "results_table": "results",
            "job_name": "test_job"
        }
        
        sql = template.render(**payload)
        
        # Check for the fix: CASE WHEN source = 0 AND target > 0 THEN 100.0
        assert "CASE WHEN source_nulls_col1 = 0 AND target_nulls_col1 > 0 THEN 100.0" in sql
    
    def test_dashboard_total_validations_count(self):
        """Test that Total Validations widget counts all validations correctly."""
        # Executives need accurate counts for oversight
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            # Find the KPI dataset
            kpi_dataset = next(d for d in dashboard["datasets"] if d["name"] == "ds_kpi")
            query = " ".join(kpi_dataset["queryLines"])
            
            # Should count all validations, not distinct tables
            assert "COUNT(*) as tables_validated" in query
            assert "COUNT(DISTINCT" not in query or "COUNT(DISTINCT" not in query.split("tables_validated")[0].split("\n")[-1]
            
            # Check widget title reflects the change
            pages = dashboard["pages"]
            main_page = next(p for p in pages if p["name"] == "main_page")
            widgets = main_page["layout"]
            
            # Find the Total Validations widget
            for widget_def in widgets:
                widget = widget_def.get("widget", {})
                if widget.get("spec", {}).get("frame", {}).get("title") == "Total Validations":
                    assert True
                    break
            else:
                assert False, "Total Validations widget not found"
    
    def test_validation_status_display_simplified(self):
        """Test that validation status shows clear checkmarks or X marks."""
        # Executives need clear visual indicators
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            # Find validation details dataset
            val_details = next(d for d in dashboard["datasets"] if d["name"] == "ds_validation_details")
            query = " ".join(val_details["queryLines"])
            
            # Should use simple CASE status logic
            assert "CASE status" in query
            assert "WHEN 'SUCCESS' THEN '✅'" in query
            assert "WHEN 'FAILURE' THEN '❌'" in query
    
    def test_source_target_columns_at_end(self):
        """Test that Source and Target columns appear after check statuses."""
        # Executives want to see check results first, then details
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            # Find the validation details widget
            pages = dashboard["pages"]
            main_page = next(p for p in pages if p["name"] == "main_page")
            
            for widget_def in main_page["layout"]:
                widget = widget_def.get("widget", {})
                if "ds_validation_details" in str(widget.get("queries", [])):
                    columns = widget["spec"]["encodings"]["columns"]
                    column_names = [c["fieldName"] for c in columns]
                    
                    # Source and Target should be last
                    assert column_names[-2] == "source_table"
                    assert column_names[-1] == "target_table"
                    
                    # Check columns should come before
                    check_columns = ["count_check", "hash_check", "null_check", "unique_check", "agg_check"]
                    for check in check_columns:
                        if check in column_names:
                            assert column_names.index(check) < column_names.index("source_table")
                    break
    
    def test_schema_quality_summary_clarity(self):
        """Test that Schema Quality Summary has clear title."""
        # Executives need clear labeling
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            # Check that the widget title is clear
            pages = dashboard["pages"]
            main_page = next(p for p in pages if p["name"] == "main_page")
            
            for widget_def in main_page["layout"]:
                widget = widget_def.get("widget", {})
                if "ds_business_impact" in str(widget.get("queries", [])):
                    title = widget["spec"]["frame"]["title"]
                    assert "Source Schema" in title
                    break
    
    def test_exploded_checks_no_unresolved_column(self):
        """Test that exploded checks query doesn't have UNRESOLVED_COLUMN error."""
        # Executives need error-free dashboards
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            # Find exploded checks dataset
            exploded = next(d for d in dashboard["datasets"] if d["name"] == "ds_exploded_checks")
            query = " ".join(exploded["queryLines"])
            
            # Should not reference undefined col_name or agg_type
            assert "col_name, '_', agg_type" not in query
            # Should use col_agg directly
            assert "CONCAT('Aggregation Check: ', col_agg)" in query
    
    def test_executive_friendly_metrics(self):
        """Test that all metrics are formatted for executive consumption."""
        # Verify number formatting, percentages, and clear status indicators
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            # Check KPI metrics
            kpi_dataset = next(d for d in dashboard["datasets"] if d["name"] == "ds_kpi")
            query = " ".join(kpi_dataset["queryLines"])
            
            # Should have executive-friendly metrics
            assert "success_rate_percent" in query
            assert "data_quality_score" in query
            assert "failed_tasks" in query
            
            # Check business impact dataset
            impact_dataset = next(d for d in dashboard["datasets"] if d["name"] == "ds_business_impact")
            impact_query = " ".join(impact_dataset["queryLines"])
            
            # Should have health status indicators
            assert "🟢 Excellent" in impact_query
            assert "🟡 Good" in impact_query
            assert "🟠 Fair" in impact_query
            assert "🔴 Needs Attention" in impact_query


class TestExecutiveUsability:
    """Test that the dashboard meets executive usability requirements."""
    
    def test_critical_metrics_prominently_displayed(self):
        """Test that critical metrics are in top positions."""
        # Executives need key metrics immediately visible
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            pages = dashboard["pages"]
            main_page = next(p for p in pages if p["name"] == "main_page")
            
            # Find widgets at y=0 (top row)
            top_widgets = []
            for widget_def in main_page["layout"]:
                pos = widget_def.get("position", {})
                if pos.get("y") == 0:
                    title = widget_def.get("widget", {}).get("spec", {}).get("frame", {}).get("title", "")
                    top_widgets.append(title)
            
            # Critical metrics should be at top
            assert "Data Quality Score" in top_widgets
            assert "Critical Issues" in top_widgets
            assert "Total Validations" in top_widgets
    
    def test_clear_actionable_insights(self):
        """Test that insights are clear and actionable for executives."""
        # Verify that failure information is immediately actionable
        with patch('datapact.client.WorkspaceClient'):
            from datapact.client import DataPactClient
            
            client = DataPactClient(profile="DEFAULT")
            client.w = MagicMock()
            client.w.workspace = MagicMock()
            client.w.workspace.mkdirs = MagicMock()
            client.w.workspace.get_status = MagicMock(side_effect=Exception("Not found"))
            client.w.lakeview = MagicMock()
            client.w.lakeview.create = MagicMock(return_value=MagicMock(dashboard_id="test_id"))
            client.w.lakeview.publish = MagicMock()
            client.user_name = "test_user"
            client.root_path = "/test"
            
            dashboard_id = client.ensure_dashboard_exists(
                job_name="test_job",
                results_table_fqn="catalog.schema.results",
                warehouse_id="test_warehouse"
            )
            
            create_call = client.w.lakeview.create.call_args
            dashboard_obj = create_call[0][0]
            dashboard_json = dashboard_obj.serialized_dashboard
            dashboard = json.loads(dashboard_json)
            
            # Check for issue classification widget
            pages = dashboard["pages"]
            main_page = next(p for p in pages if p["name"] == "main_page")
            
            widget_titles = []
            for widget_def in main_page["layout"]:
                title = widget_def.get("widget", {}).get("spec", {}).get("frame", {}).get("title", "")
                widget_titles.append(title)
            
            # Should have clear categorization of issues
            assert "Issue Classification" in widget_titles
            assert "Top Failing Validations" in widget_titles
            
            # Check failure categorization query
            failures_dataset = next(d for d in dashboard["datasets"] if d["name"] == "ds_failures_by_type")
            query = " ".join(failures_dataset["queryLines"])
            
            # Should categorize failures clearly
            assert "Row Count Mismatch" in query
            assert "Data Integrity Issue" in query
            assert "Data Completeness" in query
            assert "Business Rule Violation" in query