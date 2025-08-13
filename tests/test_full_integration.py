"""Full integration test to verify datapact run works end-to-end."""

import json
from unittest.mock import MagicMock, Mock, patch, call
from datetime import datetime

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service import sql as sql_service

from datapact.client import DataPactClient
from datapact.config import DataPactConfig


def test_full_datapact_run_with_demo_config():
    """Test complete datapact run with demo configuration."""
    
    # Load the demo configuration
    import yaml
    with open("demo/demo_config.yml", "r") as f:
        config_dict = yaml.safe_load(f)
    config = DataPactConfig(**config_dict)
    
    with patch("datapact.client.WorkspaceClient") as mock_ws:
        # Setup mock workspace client
        mock_ws_instance = MagicMock()
        mock_ws.return_value = mock_ws_instance
        
        # Mock user
        mock_ws_instance.current_user.me.return_value = MagicMock(
            user_name="test_user@example.com"
        )
        
        # Mock workspace operations
        mock_ws_instance.workspace.mkdirs = MagicMock()
        mock_ws_instance.workspace.upload = MagicMock()
        mock_ws_instance.workspace.get_status = MagicMock(side_effect=NotFound("Not found"))
        
        # Mock warehouse
        mock_warehouse = MagicMock()
        mock_warehouse.id = "test_warehouse_id"
        mock_warehouse.name = "test_warehouse"
        mock_warehouse.state = sql_service.State.RUNNING
        mock_ws_instance.warehouses.list.return_value = [mock_warehouse]
        mock_ws_instance.warehouses.get.return_value = mock_warehouse
        
        # Mock SQL execution
        mock_ws_instance.statement_execution.execute_statement.return_value = MagicMock(
            statement_id="stmt_123"
        )
        mock_ws_instance.statement_execution.get_statement.return_value = MagicMock(
            status=MagicMock(state=sql_service.StatementState.SUCCEEDED)
        )
        
        # Mock dashboard creation
        mock_ws_instance.lakeview.create = MagicMock(
            return_value=MagicMock(dashboard_id="dashboard_123")
        )
        mock_ws_instance.lakeview.publish = MagicMock()
        
        # Mock job creation and execution
        mock_ws_instance.jobs.list.return_value = []
        mock_ws_instance.jobs.create = MagicMock(
            return_value=MagicMock(job_id=123)
        )
        
        # Mock job run
        mock_run = MagicMock()
        mock_run.run_id = 456
        mock_run.run_page_url = "https://test.databricks.com/jobs/123/runs/456"
        mock_run.state = MagicMock(
            life_cycle_state=RunLifeCycleState.TERMINATED,
            result_state=RunResultState.SUCCESS,
            state_message=None
        )
        mock_run.tasks = []
        
        mock_ws_instance.jobs.run_now = MagicMock(return_value=mock_run)
        mock_ws_instance.jobs.get_run = MagicMock(return_value=mock_run)
        mock_ws_instance.config.host = "https://test.databricks.com"
        
        # Initialize client
        client = DataPactClient(profile="DEFAULT")
        
        # Run validation
        with patch("time.sleep"):  # Mock sleep to avoid delays
            client.run_validation(
                config=config,
                job_name="Integration Test Job",
                warehouse_name="test_warehouse",
            )
        
        # Verify all components were called
        
        # 1. Warehouse was found/started
        assert mock_ws_instance.warehouses.list.called
        
        # 2. Infrastructure was set up (catalog, schema, table)
        execute_calls = mock_ws_instance.statement_execution.execute_statement.call_args_list
        assert len(execute_calls) >= 3  # At least catalog, schema, and table creation
        
        # 3. Dashboard was created
        assert mock_ws_instance.lakeview.create.called
        dashboard_call = mock_ws_instance.lakeview.create.call_args_list[0]
        dashboard_obj = dashboard_call[0][0]
        dashboard_json = json.loads(dashboard_obj.serialized_dashboard)
        
        # Verify dashboard has all expected datasets
        dataset_names = {ds["name"] for ds in dashboard_json["datasets"]}
        assert "ds_kpi" in dataset_names
        assert "ds_summary" in dataset_names
        assert "ds_failure_rate" in dataset_names
        assert "ds_failures_by_type" in dataset_names
        
        # 4. SQL scripts were uploaded
        upload_calls = mock_ws_instance.workspace.upload.call_args_list
        uploaded_files = [call[1]["path"] for call in upload_calls]
        
        # Should have uploaded SQL for each validation task plus aggregate plus genie datasets
        expected_tasks = len(config.validations) + 2  # validations + aggregate_results + setup_genie_datasets
        assert len(uploaded_files) == expected_tasks
        
        # 5. Job was created with correct tasks
        assert mock_ws_instance.jobs.create.called
        job_create_call = mock_ws_instance.jobs.create.call_args_list[0]
        created_tasks = job_create_call[1]["tasks"]
        
        # Should have validation tasks + aggregate + dashboard refresh + genie datasets
        assert len(created_tasks) == len(config.validations) + 3
        
        # Verify task structure
        task_keys = {t.task_key for t in created_tasks}
        assert "aggregate_results" in task_keys
        assert "refresh_dashboard" in task_keys
        assert "setup_genie_datasets" in task_keys
        
        # Verify aggregate depends on all validations
        agg_task = next(t for t in created_tasks if t.task_key == "aggregate_results")
        assert len(agg_task.depends_on) == len(config.validations)
        
        # Verify Genie datasets and dashboard refresh run in parallel (both depend only on aggregate)
        genie_task = next(t for t in created_tasks if t.task_key == "setup_genie_datasets")
        dashboard_task = next(t for t in created_tasks if t.task_key == "refresh_dashboard")
        assert len(genie_task.depends_on) == 1
        assert genie_task.depends_on[0].task_key == "aggregate_results"
        assert len(dashboard_task.depends_on) == 1
        assert dashboard_task.depends_on[0].task_key == "aggregate_results"
        
        # 6. Job was run
        assert mock_ws_instance.jobs.run_now.called
        assert mock_ws_instance.jobs.get_run.called
        
        print("✅ Full integration test passed!")
        print(f"  - Processed {len(config.validations)} validation tasks")
        print(f"  - Created dashboard with {len(dataset_names)} datasets")
        print(f"  - Uploaded {len(uploaded_files)} SQL scripts")
        print(f"  - Created job with {len(created_tasks)} tasks")


def test_datapact_handles_missing_warehouse():
    """Test that datapact provides clear error when warehouse is not found."""
    
    with patch("datapact.client.WorkspaceClient") as mock_ws:
        mock_ws_instance = MagicMock()
        mock_ws.return_value = mock_ws_instance
        mock_ws_instance.current_user.me.return_value = MagicMock(user_name="test_user")
        mock_ws_instance.workspace.mkdirs = MagicMock()
        
        # No warehouses available
        mock_ws_instance.warehouses.list.return_value = []
        
        client = DataPactClient(profile="DEFAULT")
        
        config = DataPactConfig(validations=[])
        
        with pytest.raises(ValueError, match="SQL Warehouse 'missing_warehouse' not found"):
            client.run_validation(
                config=config,
                job_name="Test Job",
                warehouse_name="missing_warehouse",
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])