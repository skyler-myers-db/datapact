"""Comprehensive tests for DataPactClient to achieve full coverage."""

# pylint: disable=protected-access

from datetime import datetime
from unittest.mock import Mock, call, patch

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs
from databricks.sdk.service import sql as sql_service

from datapact.client import DataPactClient
from datapact.config import DataPactConfig, ValidationTask


class TestDataPactClientInit:
    """Test DataPactClient initialization."""

    def test_init_creates_workspace_and_sets_attributes(self):
        """Test that __init__ properly initializes the client."""
        mock_ws = Mock()
        mock_user = Mock()
        mock_user.user_name = "test.user@example.com"
        mock_ws.current_user.me.return_value = mock_user
        mock_ws.workspace.mkdirs = Mock()

        with patch("datapact.client.WorkspaceClient", return_value=mock_ws):
            client = DataPactClient(profile="TEST")

            assert client.user_name == "test.user@example.com"
            assert client.root_path == "/Users/test.user@example.com/datapact"
            mock_ws.workspace.mkdirs.assert_called_once_with(
                "/Users/test.user@example.com/datapact"
            )

    def test_init_with_service_principal(self):
        """Test initialization with a service principal."""
        mock_ws = Mock()
        mock_user = Mock()
        # Service principal has UUID-like name without @
        mock_user.user_name = "12345678-1234-1234-1234-123456789012"
        mock_ws.current_user.me.return_value = mock_user

        with patch("datapact.client.WorkspaceClient", return_value=mock_ws):
            client = DataPactClient(profile="SP")

            assert client.user_name == "12345678-1234-1234-1234-123456789012"
            assert client.root_path == "/Users/12345678-1234-1234-1234-123456789012/datapact"


class TestExecuteSql:
    """Test _execute_sql method."""

    def test_execute_sql_success(self):
        """Test successful SQL execution."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        # Mock successful execution
        mock_resp = Mock()
        mock_resp.statement_id = "stmt-123"
        client.w.statement_execution.execute_statement.return_value = mock_resp

        mock_status = Mock()
        mock_status.status = Mock()
        mock_status.status.state = sql_service.StatementState.SUCCEEDED
        client.w.statement_execution.get_statement.return_value = mock_status

        # Should complete without raising
        client._execute_sql("SELECT 1", "warehouse-123")

        client.w.statement_execution.execute_statement.assert_called_once_with(
            statement="SELECT 1", warehouse_id="warehouse-123", wait_timeout="0s"
        )

    def test_execute_sql_failure(self):
        """Test SQL execution failure."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_resp = Mock()
        mock_resp.statement_id = "stmt-123"
        client.w.statement_execution.execute_statement.return_value = mock_resp

        mock_status = Mock()
        mock_status.status = Mock()
        mock_status.status.state = sql_service.StatementState.FAILED
        mock_status.status.error = Mock()
        mock_status.status.error.message = "SQL syntax error"
        client.w.statement_execution.get_statement.return_value = mock_status

        with pytest.raises(RuntimeError, match="SQL execution failed: SQL syntax error"):
            client._execute_sql("SELECT bad", "warehouse-123")

    def test_execute_sql_timeout(self):
        """Test SQL execution timeout."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_resp = Mock()
        mock_resp.statement_id = "stmt-123"
        client.w.statement_execution.execute_statement.return_value = mock_resp

        mock_status = Mock()
        mock_status.status = Mock()
        mock_status.status.state = sql_service.StatementState.PENDING
        client.w.statement_execution.get_statement.return_value = mock_status

        with patch("datapact.client.datetime") as mock_dt:
            # Make it timeout immediately
            mock_dt.now.side_effect = [
                datetime(2024, 1, 1, 0, 0, 0),  # Start time
                datetime(2024, 1, 1, 0, 6, 0),  # After deadline
            ]
            with pytest.raises(TimeoutError, match="SQL statement timed out"):
                client._execute_sql("SELECT 1", "warehouse-123")

    def test_execute_sql_no_statement_id(self):
        """Test when statement_id is None."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_resp = Mock()
        mock_resp.statement_id = None
        client.w.statement_execution.execute_statement.return_value = mock_resp

        with pytest.raises(ValueError, match="Statement ID is None"):
            client._execute_sql("SELECT 1", "warehouse-123")

    def test_execute_sql_no_status(self):
        """Test when status is None."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_resp = Mock()
        mock_resp.statement_id = "stmt-123"
        client.w.statement_execution.execute_statement.return_value = mock_resp

        mock_status = Mock()
        mock_status.status = None
        client.w.statement_execution.get_statement.return_value = mock_status

        with pytest.raises(RuntimeError, match="Statement status is None"):
            client._execute_sql("SELECT 1", "warehouse-123")


class TestSetupInfrastructure:
    """Test infrastructure setup methods."""

    def test_setup_default_infrastructure(self):
        """Test _setup_default_infrastructure creates catalog and schema."""
        client = object.__new__(DataPactClient)
        client.user_name = "test.user@example.com"
        execute_sql_mock = Mock()
        client._execute_sql = execute_sql_mock  # assign mock directly for clarity

        # Call the protected helper directly in tests
        client._setup_default_infrastructure("warehouse-123")

        expected_calls = [
            call("CREATE CATALOG IF NOT EXISTS `datapact`", "warehouse-123"),
            call(
                "GRANT USAGE ON CATALOG `datapact` TO `test.user@example.com`;",
                "warehouse-123",
            ),
            call("CREATE SCHEMA IF NOT EXISTS `datapact`.`results`", "warehouse-123"),
        ]
        execute_sql_mock.assert_has_calls(expected_calls)


class TestDashboardNotebook:
    """Test dashboard notebook generation."""

    def test_generate_dashboard_notebook_content(self):
        """Test _generate_dashboard_notebook_content returns valid Python code."""
        client = object.__new__(DataPactClient)
        # Call the protected helper directly in tests
        content = client._generate_dashboard_notebook_content()

        # Check that it contains expected imports and code
        assert "from databricks.sdk import WorkspaceClient" in content
        assert "dbutils.widgets.text" in content
        assert 'dashboard_name = f"DataPact Results: {job_name}"' in content
        assert "w.dashboards.create" in content


class TestEnsureDashboard:
    """Test dashboard creation and management."""

    def test_ensure_dashboard_exists_creates_new(self):
        """Test creating a new dashboard when none exists."""
        client = object.__new__(DataPactClient)
        client.w = Mock()
        client.root_path = "/Users/test/datapact"

        # Mock workspace operations
        client.w.workspace.mkdirs = Mock()
        client.w.workspace.get_status.side_effect = NotFound("Not found")
        client.w.workspace.delete = Mock()

        # Mock lakeview operations
        mock_draft = Mock()
        mock_draft.dashboard_id = "dashboard-123"
        client.w.lakeview.create.return_value = mock_draft
        client.w.lakeview.publish = Mock()

        # Mock config
        client.w.config.host = "https://test.databricks.com"

        dashboard_id = client.ensure_dashboard_exists(
            "Test Job", "`catalog`.`schema`.`table`", "warehouse-123"
        )

        assert dashboard_id == "dashboard-123"
        client.w.lakeview.publish.assert_called_once_with(
            dashboard_id="dashboard-123",
            embed_credentials=True,
            warehouse_id="warehouse-123",
        )

    def test_ensure_dashboard_exists_deletes_existing(self):
        """Test that existing dashboard file is deleted before creating new one."""
        client = object.__new__(DataPactClient)
        client.w = Mock()
        client.root_path = "/Users/test/datapact"

        # Mock finding existing dashboard file
        client.w.workspace.get_status.return_value = Mock()  # File exists
        client.w.workspace.delete = Mock()

        mock_draft = Mock()
        mock_draft.dashboard_id = "dashboard-456"
        client.w.lakeview.create.return_value = mock_draft

        client.w.config.host = "https://test.databricks.com"

        with patch("time.sleep"):  # Skip sleep in test
            dashboard_id = client.ensure_dashboard_exists(
                "Test Job 2", "`catalog`.`schema`.`table`", "warehouse-789"
            )

        assert dashboard_id == "dashboard-456"
        client.w.workspace.delete.assert_called_once()

    def test_ensure_dashboard_exists_no_dashboard_id(self):
        """Test error when dashboard creation returns no ID."""
        client = object.__new__(DataPactClient)
        client.w = Mock()
        client.root_path = "/Users/test/datapact"

        client.w.workspace.get_status.side_effect = NotFound("Not found")

        mock_draft = Mock()
        mock_draft.dashboard_id = None
        client.w.lakeview.create.return_value = mock_draft

        with pytest.raises(RuntimeError, match="Failed to create dashboard"):
            client.ensure_dashboard_exists(
                "Test Job", "`catalog`.`schema`.`table`", "warehouse-123"
            )


class TestUploadSqlScripts:
    """Test SQL script upload functionality."""

    def test_upload_sql_scripts(self):
        """Test uploading SQL scripts and dashboard notebook."""
        client = object.__new__(DataPactClient)
        client.w = Mock()
        client.root_path = "/Users/test/datapact"
        client._generate_validation_sql = Mock(return_value="SELECT 1")
        client._generate_dashboard_notebook_content = Mock(return_value="# Python code")

        config = DataPactConfig(
            validations=[
                ValidationTask(
                    task_key="task1",
                    source_catalog="src",
                    source_schema="schema",
                    source_table="table1",
                    target_catalog="tgt",
                    target_schema="schema",
                    target_table="table1",
                ),
                ValidationTask(
                    task_key="task2",
                    source_catalog="src",
                    source_schema="schema",
                    source_table="table2",
                    target_catalog="tgt",
                    target_schema="schema",
                    target_table="table2",
                ),
            ]
        )

        asset_paths = client._upload_sql_scripts(config, "`results`.`table`", "Test Job")

        assert "task1" in asset_paths
        assert "task2" in asset_paths
        assert "aggregate_results" in asset_paths
        assert "setup_genie_datasets" in asset_paths  # Genie datasets setup task
        assert asset_paths["task1"] == "/Users/test/datapact/job_assets/Test Job/task1.sql"
        assert client.w.workspace.upload.call_count == 4  # 2 tasks + 1 aggregate + 1 genie datasets


class TestRunValidation:
    """Test the main run_validation method."""

    @patch("datapact.client.datetime")
    @patch("time.sleep")
    def test_run_validation_success(self, _mock_sleep, mock_datetime):
        """Test successful validation run."""
        # Setup time mocking
        mock_datetime.now.side_effect = [
            datetime(2024, 1, 1, 0, 0, 0),  # Start time
            datetime(2024, 1, 1, 0, 0, 30),  # First check
            datetime(2024, 1, 1, 0, 1, 0),  # Second check - job done
        ]

        client = object.__new__(DataPactClient)
        client.w = Mock()
        client.user_name = "test.user@example.com"
        client.root_path = "/Users/test/datapact"

        # Mock methods
        client._ensure_sql_warehouse = Mock()
        mock_warehouse = Mock()
        mock_warehouse.id = "warehouse-123"
        client._ensure_sql_warehouse.return_value = mock_warehouse

        client._setup_default_infrastructure = Mock()
        client._ensure_results_table_exists = Mock()
        client.ensure_dashboard_exists = Mock(return_value="dashboard-123")
        client._upload_sql_scripts = Mock(
            return_value={
                "task1": "/path/to/task1.sql",
                "aggregate_results": "/path/to/aggregate.sql",
            }
        )

        # Mock job operations
        client.w.jobs.list.return_value = []  # No existing job
        mock_job = Mock()
        mock_job.job_id = 456
        client.w.jobs.create.return_value = mock_job

        mock_run_info = Mock()
        mock_run_info.run_id = 789
        client.w.jobs.run_now.return_value = mock_run_info

        # Mock run metadata
        mock_run = Mock()
        mock_run.run_id = 789
        mock_run.run_page_url = "https://test.databricks.com/jobs/456/runs/789"
        mock_run.state = Mock()
        mock_run.state.life_cycle_state = jobs.RunLifeCycleState.RUNNING
        mock_run.state.result_state = jobs.RunResultState.SUCCESS
        mock_run.state.state_message = None
        mock_run.tasks = []

        # First call returns RUNNING, second returns TERMINATED
        client.w.jobs.get_run.side_effect = [
            mock_run,  # Initial get
            mock_run,  # First poll - still running
            Mock(
                run_id=789,
                state=Mock(
                    life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
                    result_state=jobs.RunResultState.SUCCESS,
                    state_message=None,
                ),
                tasks=[],
            ),  # Second poll - done
        ]

        config = DataPactConfig(
            validations=[
                ValidationTask(
                    task_key="task1",
                    source_catalog="src",
                    source_schema="schema",
                    source_table="table1",
                    target_catalog="tgt",
                    target_schema="schema",
                    target_table="table1",
                )
            ]
        )

        # Run validation
        client.run_validation(config, "Test Job", "test_warehouse")

        # Verify key operations were called
        client._ensure_sql_warehouse.assert_called_once_with("test_warehouse")
        client._setup_default_infrastructure.assert_called_once()
        client.ensure_dashboard_exists.assert_called_once()
        client.w.jobs.create.assert_called_once()
        client.w.jobs.run_now.assert_called_once_with(job_id=456)

    @patch("datapact.client.datetime")
    @patch("time.sleep")
    def test_run_validation_failure(self, mock_sleep, mock_datetime):
        """Test validation run that fails."""
        mock_datetime.now.side_effect = [
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 1, 1, 0, 0, 30),
        ]

        client = object.__new__(DataPactClient)
        client.w = Mock()
        client.user_name = "test.user@example.com"
        client.root_path = "/Users/test/datapact"

        # Setup mocks
        mock_warehouse = Mock()
        mock_warehouse.id = "warehouse-123"
        client._ensure_sql_warehouse = Mock(return_value=mock_warehouse)
        client._setup_default_infrastructure = Mock()
        client._ensure_results_table_exists = Mock()
        client.ensure_dashboard_exists = Mock(return_value="dashboard-123")
        client._upload_sql_scripts = Mock(
            return_value={
                "task1": "/path/to/task1.sql",
                "aggregate_results": "/path/to/agg.sql",
            }
        )

        # Mock job failure
        client.w.jobs.list.return_value = []
        client.w.jobs.create.return_value = Mock(job_id=456)
        client.w.jobs.run_now.return_value = Mock(run_id=789)

        mock_run = Mock()
        mock_run.run_id = 789
        mock_run.run_page_url = "https://test.databricks.com/jobs/456/runs/789"
        mock_run.state = Mock(
            life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
            result_state=jobs.RunResultState.FAILED,
            state_message="Task failed with error",
        )
        mock_run.tasks = []

        client.w.jobs.get_run.return_value = mock_run

        config = DataPactConfig(
            validations=[
                ValidationTask(
                    task_key="task1",
                    source_catalog="src",
                    source_schema="schema",
                    source_table="table1",
                    target_catalog="tgt",
                    target_schema="schema",
                    target_table="table1",
                )
            ]
        )

        # Run should surface failure to callers
        with pytest.raises(RuntimeError) as excinfo:
            client.run_validation(config, "Test Job", "test_warehouse")
        assert "RunResultState.FAILED" in str(excinfo.value)

    @patch("datapact.client.datetime")
    @patch("time.sleep")
    def test_run_validation_timeout(self, mock_sleep, mock_datetime):
        """Test validation run timeout."""
        # Make it timeout immediately
        mock_datetime.now.side_effect = [
            datetime(2024, 1, 1, 0, 0, 0),  # Start
            datetime(2024, 1, 1, 1, 1, 0),  # After deadline
        ]

        client = object.__new__(DataPactClient)
        client.w = Mock()
        client.user_name = "test.user@example.com"
        client.root_path = "/Users/test/datapact"

        # Setup mocks
        mock_warehouse = Mock(id="warehouse-123")
        client._ensure_sql_warehouse = Mock(return_value=mock_warehouse)
        client._setup_default_infrastructure = Mock()
        client._ensure_results_table_exists = Mock()
        client.ensure_dashboard_exists = Mock(return_value="dashboard-123")
        client._upload_sql_scripts = Mock(
            return_value={
                "task1": "/path/to/task1.sql",
                "aggregate_results": "/path/to/agg.sql",
            }
        )

        client.w.jobs.list.return_value = []
        client.w.jobs.create.return_value = Mock(job_id=456)
        client.w.jobs.run_now.return_value = Mock(run_id=789)

        mock_run = Mock(
            run_id=789,
            run_page_url="https://test.databricks.com/jobs/456/runs/789",
            state=Mock(life_cycle_state=jobs.RunLifeCycleState.RUNNING),
            tasks=[],
        )
        client.w.jobs.get_run.return_value = mock_run

        config = DataPactConfig(
            validations=[
                ValidationTask(
                    task_key="task1",
                    source_catalog="src",
                    source_schema="schema",
                    source_table="table1",
                    target_catalog="tgt",
                    target_schema="schema",
                    target_table="table1",
                )
            ]
        )

        with pytest.raises(TimeoutError, match="Job run timed out"):
            client.run_validation(config, "Test Job", "test_warehouse")

    def test_run_validation_with_service_principal(self):
        """Test run_validation with service principal."""
        client = object.__new__(DataPactClient)
        client.w = Mock()
        # Service principal UUID
        client.user_name = "12345678-1234-1234-1234-123456789012"
        client.root_path = "/Users/sp/datapact"

        # Setup mocks
        mock_warehouse = Mock(id="warehouse-123")
        client._ensure_sql_warehouse = Mock(return_value=mock_warehouse)
        client._setup_default_infrastructure = Mock()
        client._ensure_results_table_exists = Mock()
        client.ensure_dashboard_exists = Mock(return_value="dashboard-123")
        client._upload_sql_scripts = Mock(
            return_value={
                "task1": "/path/to/task1.sql",
                "aggregate_results": "/path/to/agg.sql",
            }
        )

        client.w.jobs.list.return_value = []
        mock_job = Mock(job_id=456)
        client.w.jobs.create.return_value = mock_job

        # Capture the job settings
        config = DataPactConfig(
            validations=[
                ValidationTask(
                    task_key="task1",
                    source_catalog="src",
                    source_schema="schema",
                    source_table="table1",
                    target_catalog="tgt",
                    target_schema="schema",
                    target_table="table1",
                )
            ]
        )

        # Mock the rest of the flow
        client.w.jobs.run_now.return_value = Mock(run_id=789)
        mock_run = Mock(
            run_id=789,
            run_page_url="https://test.databricks.com/jobs/456/runs/789",
            state=Mock(
                life_cycle_state=jobs.RunLifeCycleState.TERMINATED,
                result_state=jobs.RunResultState.SUCCESS,
                state_message=None,
            ),
            tasks=[],
        )
        client.w.jobs.get_run.return_value = mock_run

        with patch("datapact.client.datetime") as mock_dt, patch("time.sleep"):
            mock_dt.now.side_effect = [
                datetime(2024, 1, 1, 0, 0, 0),
                datetime(2024, 1, 1, 0, 0, 30),
            ]
            client.run_validation(config, "Test Job", "test_warehouse")

        # Check that service principal was used in run_as
        create_call = client.w.jobs.create.call_args
        assert create_call.kwargs["run_as"].service_principal_name == client.user_name
        assert create_call.kwargs["run_as"].user_name is None


class TestEnsureSqlWarehouse:
    """Test SQL warehouse management."""

    def test_ensure_sql_warehouse_running(self):
        """Test with already running warehouse."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_warehouse = Mock()
        mock_warehouse.name = "test_warehouse"
        mock_warehouse.id = "warehouse-123"
        mock_warehouse.state = sql_service.State.RUNNING

        client.w.warehouses.list.return_value = [mock_warehouse]
        client.w.warehouses.get.return_value = mock_warehouse

        result = client._ensure_sql_warehouse("test_warehouse")

        assert result == mock_warehouse
        client.w.warehouses.start.assert_not_called()

    def test_ensure_sql_warehouse_needs_start(self):
        """Test starting a stopped warehouse."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_warehouse = Mock()
        mock_warehouse.name = "test_warehouse"
        mock_warehouse.id = "warehouse-123"
        mock_warehouse.state = sql_service.State.STOPPED

        client.w.warehouses.list.return_value = [mock_warehouse]
        running_warehouse = Mock()
        running_warehouse.state = sql_service.State.RUNNING
        client.w.warehouses.get.side_effect = [
            mock_warehouse,
            running_warehouse,
            running_warehouse,
        ]
        client.w.warehouses.start.return_value.result.return_value = None

        result = client._ensure_sql_warehouse("test_warehouse")

        assert result == running_warehouse
        client.w.warehouses.start.assert_called_once_with("warehouse-123")

    def test_ensure_sql_warehouse_not_found(self):
        """Test when warehouse doesn't exist."""
        with patch("datapact.client.WorkspaceClient") as mock_ws_class:
            mock_ws = Mock()
            mock_ws_class.return_value = mock_ws
            mock_user = Mock()
            mock_user.user_name = "test@example.com"
            mock_ws.current_user.me.return_value = mock_user
            mock_ws.warehouses.list.return_value = []

            client = DataPactClient(profile="TEST")

            with pytest.raises(ValueError, match="SQL Warehouse 'missing' not found"):
                client._ensure_sql_warehouse("missing")

    def test_ensure_sql_warehouse_no_id(self):
        """Test when warehouse has no ID."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_warehouse = Mock()
        mock_warehouse.name = "test_warehouse"
        mock_warehouse.id = None
        mock_warehouse.state = sql_service.State.RUNNING

        client.w.warehouses.list.return_value = [mock_warehouse]

        with pytest.raises(ValueError, match="has no ID and cannot be retrieved"):
            client._ensure_sql_warehouse("test_warehouse")

    def test_ensure_sql_warehouse_starting(self):
        """Test with warehouse already starting."""
        client = object.__new__(DataPactClient)
        client.w = Mock()

        mock_warehouse = Mock()
        mock_warehouse.name = "test_warehouse"
        mock_warehouse.id = "warehouse-123"
        mock_warehouse.state = sql_service.State.STARTING

        client.w.warehouses.list.return_value = [mock_warehouse]
        client.w.warehouses.get.return_value = mock_warehouse

        result = client._ensure_sql_warehouse("test_warehouse")

        assert result == mock_warehouse
        client.w.warehouses.start.assert_not_called()
