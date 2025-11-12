import importlib


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


def test_hash_columns_empty_uses_struct_star():
    p = {
        "task_key": "t",
        "source_catalog": "c",
        "source_schema": "s",
        "source_table": "a",
        "target_catalog": "c",
        "target_schema": "s",
        "target_table": "b",
        "primary_keys": ["id"],
        "count_tolerance": None,
        "pk_row_hash_check": True,
        "pk_hash_tolerance": 0.0,
        "hash_columns": [],
        "null_validation_tolerance": None,
        "null_validation_columns": [],
        "agg_validations": [],
        "results_table": "`c`.`s`.`hist`",
        "job_name": "j",
    }
    sql = _render(p)
    assert "md5(to_json(struct(*))) AS row_hash" in sql


def test_multiple_pks_and_null_columns_and_aggs():
    p = {
        "task_key": "t2",
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
    sql = _render(p)
    assert "ON s.`id1` = t.`id1` AND s.`id2` = t.`id2`" in sql
    assert "null_join_metrics AS (" in sql
    assert "SUM(CASE WHEN s.`v1` IS NULL THEN 1 ELSE 0 END) AS source_nulls_v1" in sql
    assert "SUM(CASE WHEN t.`v2` IS NULL THEN 1 ELSE 0 END) AS target_nulls_v2" in sql
    assert "source_stats AS (" in sql and "target_stats AS (" in sql
    assert "TRY_CAST(SUM(`v1`) AS DECIMAL(38, 6)) AS source_value_v1_SUM" in sql
    assert "TRY_CAST(AVG(`v1`) AS DECIMAL(38, 6)) AS source_value_v1_AVG" in sql
    assert "TRY_CAST(SUM(`v2`) AS DECIMAL(38, 6)) AS source_value_v2_SUM" in sql
    assert "1e-06" in sql  # very small tolerance is rendered
