import importlib
import re


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


def _base_payload() -> dict:
    return {
        "task_key": "t1",
        "source_catalog": "c",
        "source_schema": "s",
        "source_table": "a",
        "target_catalog": "c",
        "target_schema": "s",
        "target_table": "b",
        "primary_keys": [],
        "count_tolerance": None,
        "pk_row_hash_check": False,
        "pk_hash_threshold": None,
        "hash_columns": [],
        "null_validation_threshold": None,
        "null_validation_columns": [],
        "agg_validations": [],
        "results_table": "`datapact`.`results`.`run_history`",
        "job_name": "job_name_here",
    }


def test_template_renders_no_validations():
    p = _base_payload()
    sql = _render(p)
    assert "CREATE OR REPLACE TEMP VIEW final_metrics_view AS" in sql
    assert "overall_validation_passed" in sql
    assert "No validations configured for task" in sql
    assert (
        "INSERT INTO" in sql and "SELECT RAISE_ERROR" in sql and "SELECT to_json" in sql
    )


def test_counts_only_block_exact_fragments():
    p = _base_payload()
    p.update({"count_tolerance": 0.01})
    sql = _render(p)
    # CTE name and structure
    assert re.search(r"\bWITH\s*\ncount_metrics AS \(\n\s*SELECT", sql)
    # Payload formatting lines must be exact
    expected_lines = [
        "FORMAT_NUMBER(source_count, '#,##0') AS source_count,",
        "FORMAT_NUMBER(target_count, '#,##0') AS target_count,",
        "FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,",
        "FORMAT_STRING('%.2f%%', CAST(0.01 * 100 AS DOUBLE)) AS tolerance_percent,",
        "CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01 THEN 'PASS' ELSE 'FAIL' END AS status",
    ]
    for line in expected_lines:
        assert line in sql
    # Overall flag includes only the count condition
    assert "overall_validation_passed" in sql
    assert sql.count("count_metrics") == 2  # CTE declaration + FROM


def test_row_hash_only_block_exact_fragments():
    p = _base_payload()
    p.update(
        {
            "primary_keys": ["id"],
            "pk_row_hash_check": True,
            "pk_hash_threshold": 0.0,
            "hash_columns": ["id", "v"],
        }
    )
    sql = _render(p)
    assert "row_hash_metrics AS (" in sql
    assert "md5(to_json(struct(`id`, `v`))) AS row_hash" in sql
    assert "ON s.`id` = t.`id`" in sql
    assert "FORMAT_NUMBER(total_compared_rows, '#,##0') AS compared_rows," in sql
    assert "FORMAT_NUMBER(mismatch_count, '#,##0') AS mismatch_count," in sql
    assert "FORMAT_STRING('%.2f%%', CAST(COALESCE((mismatch_count / NULLIF(CAST(" in sql
    assert "AS row_hash_validation" in sql
    assert "mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)" in sql


def test_nulls_with_pk_join_and_without_pk():
    # With PKs
    p1 = _base_payload()
    p1.update(
        {
            "primary_keys": ["id"],
            "null_validation_threshold": 0.02,
            "null_validation_columns": ["v"],
        }
    )
    sql1 = _render(p1)
    assert "null_metrics_v AS (" in sql1
    assert "JOIN `c`.`s`.`b` t" in sql1
    assert "ON s.`id` = t.`id`" in sql1
    assert "FORMAT_NUMBER(source_nulls_v, '#,##0') AS source_nulls," in sql1
    assert "FORMAT_NUMBER(target_nulls_v, '#,##0') AS target_nulls," in sql1
    assert "total_compared_v" in sql1
    assert "<= 0.02 THEN 'PASS'" in sql1

    # Without PKs
    p2 = _base_payload()
    p2.update(
        {
            "null_validation_threshold": 0.02,
            "null_validation_columns": ["v"],
        }
    )
    sql2 = _render(p2)
    assert "null_metrics_v AS (" in sql2
    assert "JOIN" not in sql2
    assert (
        "(SELECT COUNT(1) FROM `c`.`s`.`a` WHERE `v` IS NULL) as source_nulls_v" in sql2
    )
    assert "CASE WHEN source_nulls_v = 0 THEN target_nulls_v = 0 ELSE" in sql2


def test_aggregate_validations_block_exact_fragments():
    p = _base_payload()
    p.update(
        {
            "agg_validations": [
                {"column": "v", "validations": [{"agg": "sum", "tolerance": 0.05}]}
            ]
        }
    )
    sql = _render(p)
    assert "agg_metrics_v_SUM AS (" in sql
    assert (
        "TRY_CAST((SELECT SUM(`v`) FROM `c`.`s`.`a`) AS DECIMAL(38, 6)) AS source_value_v_SUM,"
        in sql
    )
    assert (
        "TRY_CAST((SELECT SUM(`v`) FROM `c`.`s`.`b`) AS DECIMAL(38, 6)) AS target_value_v_SUM"
        in sql
    )
    assert "FORMAT_NUMBER(source_value_v_SUM, '#,##0.00') as source_value," in sql
    assert (
        "FORMAT_STRING('%.2f%%', CAST(0.05 * 100 AS DOUBLE)) AS tolerance_percent,"
        in sql
    )
    assert (
        "COALESCE(ABS(source_value_v_SUM - target_value_v_SUM) / NULLIF(ABS(CAST(source_value_v_SUM AS DOUBLE)), 0), 0) <= 0.05"
        in sql
    )


def test_full_combo_contains_all_sections_and_cross_joins_in_order():
    p = _base_payload()
    p.update(
        {
            "count_tolerance": 0.01,
            "primary_keys": ["id"],
            "pk_row_hash_check": True,
            "pk_hash_threshold": 0.0,
            "hash_columns": ["id", "v"],
            "null_validation_threshold": 0.02,
            "null_validation_columns": ["v"],
            "agg_validations": [
                {"column": "v", "validations": [{"agg": "sum", "tolerance": 0.05}]}
            ],
        }
    )
    sql = _render(p)
    # CTE names order in CROSS JOIN list should match append sequence
    cj_idx = sql.index("FROM\n")
    cj_block = sql[cj_idx : cj_idx + 400]
    assert (
        "count_metrics CROSS JOIN row_hash_metrics CROSS JOIN null_metrics_v CROSS JOIN agg_metrics_v_SUM"
        in cj_block
    )
    # Ensure overall_validation_passed has all four conditions
    assert sql.count("<= 0.01") >= 1  # count
    assert (
        "mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)" in sql
    )  # pk hash
    assert "total_compared_v" in sql  # nulls joined
    assert "source_value_v_SUM - target_value_v_SUM" in sql  # agg
