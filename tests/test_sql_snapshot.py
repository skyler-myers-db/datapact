import importlib
from pathlib import Path


def _make_env():
    jinja2 = importlib.import_module("jinja2")
    return jinja2.Environment(
        loader=jinja2.PackageLoader("datapact", "templates"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=["jinja2.ext.do"],
    )


def _render(payload: dict) -> str:
    env = _make_env()
    template = env.get_template("validation.sql.j2")
    return template.render(**payload).strip()


def _read_fixture(name: str) -> str:
    p = Path(__file__).parent / "fixtures" / name
    with p.open("r", encoding="utf-8") as f:
        return f.read().strip()


def test_snapshot_full():
    payload = {
        "task_key": "t1",
        "source_catalog": "c",
        "source_schema": "s",
        "source_table": "a",
        "target_catalog": "c",
        "target_schema": "s",
        "target_table": "b",
        "primary_keys": ["id"],
        "count_tolerance": 0.01,
        "pk_row_hash_check": True,
        "pk_hash_tolerance": 0.0,
        "hash_columns": ["id", "v"],
        "null_validation_tolerance": 0.02,
        "null_validation_columns": ["v"],
        "agg_validations": [
            {"column": "v", "validations": [{"agg": "sum", "tolerance": 0.05}]}
        ],
        "results_table": "`datapact`.`results`.`run_history`",
        "job_name": "job_name_here",
    }
    sql = _render(payload)
    expected = _read_fixture("sql_expected_full.sql")
    assert sql == expected


def test_snapshot_complex():
    payload = {
        "task_key": "t_complex",
        "source_catalog": "cat",
        "source_schema": "sch",
        "source_table": "src",
        "target_catalog": "cat",
        "target_schema": "sch",
        "target_table": "tgt",
        "primary_keys": ["id1", "id2"],
        "count_tolerance": 0.05,
        "pk_row_hash_check": True,
        "pk_hash_tolerance": 0.000001,
        "hash_columns": [],
        "null_validation_tolerance": 0.0,
        "null_validation_columns": ["v1", "v2"],
        "agg_validations": [
            {
                "column": "v1",
                "validations": [
                    {"agg": "sum", "tolerance": 0.0},
                    {"agg": "avg", "tolerance": 0.000001},
                ],
            },
            {"column": "v2", "validations": [{"agg": "sum", "tolerance": 0.1}]},
        ],
        "results_table": "`cat`.`res`.`history`",
        "job_name": "complex_job",
    }
    sql = _render(payload)
    expected = _read_fixture("sql_expected_complex.sql")
    assert sql == expected


def test_snapshot_counts_only():
    payload = {
        "task_key": "t_counts",
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
        "results_table": "`datapact`.`results`.`run_history`",
        "job_name": "counts_job",
    }
    sql = _render(payload)
    expected = _read_fixture("sql_expected_counts_only.sql")
    assert sql == expected


def test_snapshot_counts_and_nulls_only():
    payload = {
        "task_key": "t_counts_nulls",
        "source_catalog": "c",
        "source_schema": "s",
        "source_table": "a",
        "target_catalog": "c",
        "target_schema": "s",
        "target_table": "b",
        "primary_keys": [],
        "count_tolerance": 0.02,
        "pk_row_hash_check": False,
        "pk_hash_tolerance": None,
        "hash_columns": [],
        "null_validation_tolerance": 0.05,
        "null_validation_columns": ["v1", "v2"],
        "agg_validations": [],
        "results_table": "`datapact`.`results`.`run_history`",
        "job_name": "counts_nulls_job",
    }
    sql = _render(payload)
    expected = _read_fixture("sql_expected_counts_and_nulls_only.sql")
    assert sql == expected
