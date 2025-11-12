from __future__ import annotations

from pathlib import Path

import pytest

from datapact import main as main_mod


class _DummyClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.w = object()

    def run_validation(self, *, config, job_name, warehouse_name, results_table=None):  # noqa: D401, ANN001
        """Record the call for assertions."""
        # Only persist essentials for the assertion below
        self.calls.append((job_name, bool(config), warehouse_name, results_table))


def test_cli_wires_through_to_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Create a tiny config file
    config_yaml = tmp_path / "cfg.yml"
    config_yaml.write_text(
        """
validations:
  - task_key: t
    source_catalog: c
    source_schema: s
    source_table: a
    target_catalog: c
    target_schema: s
    target_table: b
    primary_keys: [id]
    count_tolerance: 0.0
        """.strip()
    )

    dummy = _DummyClient()

    # Patch DataPactClient reference inside datapact.main
    monkeypatch.setattr(main_mod, "DataPactClient", lambda **_: dummy)

    # Provide argv for the CLI parser
    monkeypatch.setenv("DATABRICKS_PROFILE", "DEFAULT")
    monkeypatch.setenv("PYTHONWARNINGS", "ignore")
    argv = [
        "datapact",
        "run",
        "--config",
        str(config_yaml),
        "--job-name",
        "cli_job",
        "--warehouse",
        "Test Warehouse",
    ]
    monkeypatch.setattr(main_mod.sys, "argv", argv, raising=True)

    # Run main()
    main_mod.main()

    # Assert it called the client with our args
    assert dummy.calls == [("cli_job", True, "Test Warehouse", None)]


def test_cli_plan_skips_databricks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Plan command should not instantiate the client."""
    config_yaml = tmp_path / "cfg.yml"
    config_yaml.write_text(
        """
validations:
  - task_key: t
    source_catalog: c
    source_schema: s
    source_table: a
    target_catalog: c
    target_schema: s
    target_table: b
        """.strip()
    )

    def _fail(**_):
        raise AssertionError("DataPactClient should not be constructed for plan.")

    monkeypatch.setattr(main_mod, "DataPactClient", _fail)
    monkeypatch.setenv("PYTHONWARNINGS", "ignore")
    argv = ["datapact", "plan", "--config", str(config_yaml)]
    monkeypatch.setattr(main_mod.sys, "argv", argv, raising=True)

    main_mod.main()


def test_cli_run_dry_run_does_not_execute(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """--dry-run should exit before initializing the client."""
    config_yaml = tmp_path / "cfg.yml"
    config_yaml.write_text(
        """
validations:
  - task_key: t
    source_catalog: c
    source_schema: s
    source_table: a
    target_catalog: c
    target_schema: s
    target_table: b
        """.strip()
    )

    def _fail(**_):
        raise AssertionError("DataPactClient should not be constructed for dry runs.")

    monkeypatch.setattr(main_mod, "DataPactClient", _fail)
    monkeypatch.setenv("PYTHONWARNINGS", "ignore")
    argv = ["datapact", "run", "--config", str(config_yaml), "--dry-run"]
    monkeypatch.setattr(main_mod.sys, "argv", argv, raising=True)

    main_mod.main()


def test_cli_init_scaffolds_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """`datapact init` should create a starter config file."""
    target = tmp_path / "starter.yml"
    monkeypatch.setenv("PYTHONWARNINGS", "ignore")
    argv = ["datapact", "init", "--output", str(target)]
    monkeypatch.setattr(main_mod.sys, "argv", argv, raising=True)

    main_mod.main()
    assert target.exists()
    assert "validations" in target.read_text()
