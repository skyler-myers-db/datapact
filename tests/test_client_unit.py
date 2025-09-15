import importlib
from textwrap import dedent

from datapact.client import DataPactClient

# pylint: disable=protected-access
from datapact.config import ValidationTask


def _make_env():
    jinja2 = importlib.import_module("jinja2")
    return jinja2.Environment(
        loader=jinja2.PackageLoader("datapact", "templates"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=["jinja2.ext.do"],
    )


def _render_expected(payload: dict) -> str:
    env = _make_env()
    template = env.get_template("validation.sql.j2")
    return template.render(**payload).strip()


def _minimal_task(**overrides) -> ValidationTask:
    base: dict = {
        "task_key": "t",
        "source_catalog": "c",
        "source_schema": "s",
        "source_table": "a",
        "target_catalog": "c",
        "target_schema": "s",
        "target_table": "b",
        "primary_keys": [],
        "count_tolerance": 0.01,
        "pk_row_hash_check": False,
        "pk_hash_tolerance": None,
        "hash_columns": [],
        "null_validation_tolerance": None,
        "null_validation_columns": [],
        "agg_validations": [],
    }
    base.update(overrides)
    return ValidationTask(**base)


def test_generate_validation_sql_matches_template():
    cfg = _minimal_task()
    # Bypass __init__ to avoid real WorkspaceClient setup
    client = object.__new__(DataPactClient)
    sql = client._generate_validation_sql(
        cfg, "`datapact`.`results`.`run_history`", "unit_job"
    )

    payload = cfg.model_dump()
    payload["results_table"] = "`datapact`.`results`.`run_history`"
    payload["job_name"] = "unit_job"
    expected = _render_expected(payload)
    assert sql == expected


def test_ensure_results_table_exists_emits_expected_sql(monkeypatch):
    captured = {}

    def fake_exec(self, statement: str, warehouse_id: str):
        captured["sql"] = statement
        captured["wh"] = warehouse_id

    # Bypass __init__ and patch _execute_sql
    client = object.__new__(DataPactClient)
    monkeypatch.setattr(DataPactClient, "_execute_sql", fake_exec, raising=True)

    client._ensure_results_table_exists("`c`.`s`.`t`", "wh-123")

    def _norm(s: str) -> str:
        return "\n".join(line.strip() for line in s.strip().splitlines())

    assert captured["wh"] == "wh-123"
    assert _norm(captured["sql"]) == _norm(
        dedent(
            """
        CREATE TABLE IF NOT EXISTS `c`.`s`.`t` (
            task_key STRING, status STRING, run_id BIGINT, job_id BIGINT, job_name STRING,
            timestamp TIMESTAMP,
            started_at TIMESTAMP, completed_at TIMESTAMP,
            source_catalog STRING, source_schema STRING, source_table STRING,
            target_catalog STRING, target_schema STRING, target_table STRING,
            result_payload VARIANT) USING DELTA
        """
        )
    )
