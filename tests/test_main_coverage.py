"""Tests for main.py CLI functionality to improve coverage."""

import os
import sys
from unittest.mock import Mock, mock_open, patch

import pytest
import yaml  # type: ignore[import]

from datapact.main import main


class TestWarehouseResolution:
    """Test warehouse configuration resolution in main."""

    @patch("datapact.main.DataPactClient")
    def test_warehouse_from_cli_flag(self, mock_client_class):
        """Test that CLI flag has highest priority."""
        mock_client = Mock()
        mock_client.w.config = Mock()
        mock_client.w.config.datapact_warehouse = "config_warehouse"
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "config.yml",
            "--job-name",
            "Job",
            "--warehouse",
            "cli_warehouse",  # CLI flag provided
        ]

        with patch.object(sys, "argv", test_args):
            with patch.dict(os.environ, {"DATAPACT_WAREHOUSE": "env_warehouse"}):
                with patch("builtins.open", mock_open(read_data="test")):
                    with patch("yaml.safe_load", return_value={"validations": []}):
                        main()

                        # Should use CLI warehouse despite env var and config
                        mock_client.run_validation.assert_called_once()
                        call_args = mock_client.run_validation.call_args
                        assert call_args.kwargs["warehouse_name"] == "cli_warehouse"

    @patch("datapact.main.DataPactClient")
    def test_warehouse_from_env_var(self, mock_client_class):
        """Test that environment variable is used when no CLI flag."""
        mock_client = Mock()
        mock_client.w.config = Mock(spec=[])  # No datapact_warehouse attribute
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "config.yml",
            "--job-name",
            "Job",
            # No --warehouse flag
        ]

        with patch.object(sys, "argv", test_args):
            with patch.dict(os.environ, {"DATAPACT_WAREHOUSE": "env_warehouse"}):
                with patch("builtins.open", mock_open(read_data="test")):
                    with patch("yaml.safe_load", return_value={"validations": []}):
                        main()

                        mock_client.run_validation.assert_called_once()
                        call_args = mock_client.run_validation.call_args
                        assert call_args.kwargs["warehouse_name"] == "env_warehouse"

    @patch("datapact.main.DataPactClient")
    def test_warehouse_from_config(self, mock_client_class):
        """Test that config attribute is used when no CLI flag or env var."""
        mock_client = Mock()
        mock_client.w.config.datapact_warehouse = "config_warehouse"
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "config.yml",
            "--job-name",
            "Job",
        ]

        with patch.object(sys, "argv", test_args):
            with patch.dict(os.environ, {}, clear=True):  # No env var
                with patch("builtins.open", mock_open(read_data="test")):
                    with patch("yaml.safe_load", return_value={"validations": []}):
                        main()

                        mock_client.run_validation.assert_called_once()
                        call_args = mock_client.run_validation.call_args
                        assert call_args.kwargs["warehouse_name"] == "config_warehouse"


class TestMainFunction:
    """Test the main CLI entry point."""

    @patch("datapact.main.DataPactClient")
    def test_main_success_with_all_args(self, mock_client_class):
        """Test successful run with all arguments."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "test_config.yml",
            "--job-name",
            "Test Job",
            "--warehouse",
            "test_warehouse",
            "--results-table",
            "catalog.schema.table",
            "--profile",
            "myprofile",
        ]

        with patch.object(sys, "argv", test_args):
            with patch("builtins.open", mock_open(read_data="test")):
                with patch("yaml.safe_load", return_value={"validations": []}):
                    main()

                    mock_client_class.assert_called_once_with(profile="myprofile")
                    mock_client.run_validation.assert_called_once()
                    call_args = mock_client.run_validation.call_args
                    assert call_args[1]["job_name"] == "Test Job"
                    assert call_args[1]["warehouse_name"] == "test_warehouse"
                    assert call_args[1]["results_table"] == "catalog.schema.table"

    @patch("datapact.main.DataPactClient")
    def test_main_with_defaults(self, mock_client_class):
        """Test with minimal required arguments."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "config.yml",
            "--job-name",
            "Job",
            "--warehouse",
            "warehouse",
        ]

        with patch.object(sys, "argv", test_args):
            with patch("builtins.open", mock_open(read_data="test")):
                with patch("yaml.safe_load", return_value={"validations": []}):
                    main()

                    mock_client_class.assert_called_once_with(profile="DEFAULT")
                    mock_client.run_validation.assert_called_once()
                    call_args = mock_client.run_validation.call_args
                    assert call_args[1]["warehouse_name"] == "warehouse"
                    assert call_args[1]["results_table"] is None

    @patch("datapact.main.DataPactClient")
    def test_main_missing_warehouse(self, mock_client_class):
        """Test error when warehouse is not provided."""
        mock_client = Mock()
        mock_client.w.config = Mock(spec=[])  # No datapact_warehouse attribute
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "config.yml",
            "--job-name",
            "Job",
        ]

        with patch.object(sys, "argv", test_args):
            with patch.dict(os.environ, {}, clear=True):  # No env var
                with pytest.raises(ValueError, match="A warehouse must be provided"):
                    main()

    @patch("datapact.main.DataPactClient")
    def test_main_invalid_config(self, mock_client_class):
        """Test error when config file has validation errors."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "bad_config.yml",
            "--job-name",
            "Job",
            "--warehouse",
            "warehouse",
        ]

        with patch.object(sys, "argv", test_args):
            with patch("builtins.open", mock_open(read_data="test")):
                with patch("yaml.safe_load", return_value={"invalid": "config"}):
                    with pytest.raises(SystemExit) as exc_info:
                        main()
                    assert exc_info.value.code == 1

    def test_main_invalid_command(self):
        """Test error with invalid command."""
        test_args = [
            "datapact",
            "invalid_command",
            "--config",
            "config.yml",
        ]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit):
                main()

    def test_main_no_args(self):
        """Test help is shown when no args provided."""
        test_args = ["datapact"]

        with patch.object(sys, "argv", test_args):
            with pytest.raises(SystemExit):
                main()

    @patch("datapact.main.DataPactClient")
    def test_main_file_not_found(self, mock_client_class):
        """Test error when config file doesn't exist."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "missing.yml",
            "--job-name",
            "Job",
            "--warehouse",
            "warehouse",
        ]

        with patch.object(sys, "argv", test_args):
            with patch("builtins.open", side_effect=FileNotFoundError()):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1

    @patch("datapact.main.DataPactClient")
    def test_main_yaml_error(self, mock_client_class):
        """Test error when YAML parsing fails."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "bad.yml",
            "--job-name",
            "Job",
            "--warehouse",
            "warehouse",
        ]

        with patch.object(sys, "argv", test_args):
            with patch("builtins.open", mock_open(read_data="test")):
                with patch("yaml.safe_load", side_effect=yaml.YAMLError("Bad YAML")):
                    with pytest.raises(SystemExit) as exc_info:
                        main()
                    assert exc_info.value.code == 1

    @patch("datapact.main.DataPactClient")
    def test_main_validation_error(self, mock_client_class):
        """Test error when config validation fails."""

        mock_client = Mock()
        mock_client_class.return_value = mock_client

        test_args = [
            "datapact",
            "run",
            "--config",
            "invalid.yml",
            "--job-name",
            "Job",
            "--warehouse",
            "warehouse",
        ]

        with patch.object(sys, "argv", test_args):
            with patch("builtins.open", mock_open(read_data="test")):
                with patch("yaml.safe_load", return_value={"validations": "not_a_list"}):
                    with pytest.raises(SystemExit) as exc_info:
                        main()
                    assert exc_info.value.code == 1
