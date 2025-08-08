"""
Test that the demo configuration file can be successfully parsed and validated.

This is a smoke test that verifies the demo_config.yml file located in the demo/
directory can be loaded as YAML and successfully validated against the DataPactConfig
Pydantic model. The test ensures that the demo configuration serves as a valid
example for users.

Raises:
    pytest.fail: If the demo config file is not found at the expected path
    pytest.fail: If the demo config file contains invalid data that fails
                Pydantic validation

Notes:
    The test loads the YAML file, parses it, and attempts to create a DataPactConfig
    instance. Any ValidationError from Pydantic or FileNotFoundError will cause
    the test to fail with a descriptive message.
"""

import yaml
import pytest
from pydantic import ValidationError
from datapact.config import DataPactConfig


def test_demo_config_is_valid():
    """
    A simple smoke test to ensure that the demo_config.yml
    can be successfully parsed by the Pydantic models.
    """
    config_path = "demo/demo_config.yml"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f)
            # This is the line that performs the test. If it fails,
            # it will raise a ValidationError and the test will fail.
            DataPactConfig(**raw_config)
    except FileNotFoundError:
        pytest.fail(f"Demo config file not found at: {config_path}")
    except ValidationError as e:
        pytest.fail(f"Demo config file is invalid: {e}")
