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
        "filter": None,
        "count_tolerance": None,
        "pk_row_hash_check": False,
        "pk_hash_tolerance": None,
        "hash_columns": [],
        "null_validation_tolerance": None,
        "null_validation_columns": [],
        "agg_validations": [],
        "results_table": "`datapact`.`results`.`run_history`",
        "job_name": "job_name_here",
        "custom_sql_tests": [],
    }


def test_template_renders_no_validations():
    p = _base_payload()
    sql = _render(p)
    assert "CREATE OR REPLACE TEMP VIEW final_metrics_view AS" in sql
    assert "overall_validation_passed" in sql
    assert "No validations configured for task" in sql
    assert "INSERT INTO" in sql and "SELECT RAISE_ERROR" in sql and "SELECT to_json" in sql


def test_counts_only_block_exact_fragments():
    p = _base_payload()
    p.update({"count_tolerance": 0.01})
    sql = _render(p)
    # Stats CTEs and structure
    assert "source_stats AS (" in sql
    assert "COUNT(1) AS source_count" in sql
    assert "target_stats AS (" in sql
    assert "COUNT(1) AS target_count" in sql
    # Payload formatting lines must be exact
    expected_lines = [
        "FORMAT_NUMBER(source_count, 0) AS source_count,",
        "FORMAT_NUMBER(target_count, 0) AS target_count,",
        "FORMAT_STRING('%.2f%%', CAST(COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) * 100 AS DOUBLE)) as relative_diff_percent,",
        "FORMAT_STRING('%.2f%%', CAST(0.01 * 100 AS DOUBLE)) AS tolerance_percent,",
        "CASE WHEN COALESCE(ABS(source_count - target_count) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.01 THEN 'PASS' ELSE 'FAIL' END AS status",
    ]
    for line in expected_lines:
        assert line in sql
    # Overall flag includes only the count condition
    assert "overall_validation_passed" in sql
    assert "source_stats CROSS JOIN target_stats" in sql


def test_filter_applies_to_builtin_ctes_and_excludes_custom_sql():
    p = _base_payload()
    p.update(
        {
            "filter": "updated_ts >= '2025-01-01'",
            "count_tolerance": 0.02,
            "primary_keys": ["id"],
            "pk_row_hash_check": True,
            "pk_hash_tolerance": 0.0,
            "hash_columns": ["payload"],
            "custom_sql_tests": [
                {
                    "name": "Manual Slice",
                    "description": "User-defined SQL should not inherit filters.",
                    "cte_base_name": "manual_slice",
                    "source_sql": "SELECT updated_ts FROM `c`.`s`.`a`",
                    "target_sql": "SELECT updated_ts FROM `c`.`s`.`b`",
                    "base_sql": "SELECT updated_ts FROM {{ table_fqn }}",
                }
            ],
        }
    )
    sql = _render(p)
    assert "filtered_source AS (" in sql
    assert "filtered_target AS (" in sql
    assert "COUNT(1) AS source_count" in sql
    assert "FROM filtered_source" in sql
    assert "COUNT(1) AS target_count" in sql
    assert "FROM filtered_target" in sql
    assert "FROM filtered_source\n  ) s" in sql
    assert "FROM filtered_target\n  ) t" in sql
    assert "Manual Slice" in sql
    assert "SELECT updated_ts FROM `c`.`s`.`a`" in sql
    assert "SELECT updated_ts FROM `c`.`s`.`b`" in sql
    assert "applied_filter" in sql


def test_row_hash_only_block_exact_fragments():
    p = _base_payload()
    p.update(
        {
            "primary_keys": ["id"],
            "pk_row_hash_check": True,
            "pk_hash_tolerance": 0.0,
            "hash_columns": ["id", "v"],
        }
    )
    sql = _render(p)
    assert "row_hash_metrics AS (" in sql
    assert "md5(to_json(struct(`id`, `v`))) AS row_hash" in sql
    assert "ON s.`id` = t.`id`" in sql
    assert "FORMAT_NUMBER(total_compared_rows, 0) AS compared_rows," in sql
    assert "FORMAT_NUMBER(mismatch_count, 0) AS mismatch_count," in sql
    assert "FORMAT_STRING('%.2f%%', CAST(COALESCE((mismatch_count / NULLIF(CAST(" in sql
    assert "AS row_hash_validation" in sql
    assert "mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)" in sql


def test_nulls_with_pk_join_and_without_pk():
    # With PKs
    p1 = _base_payload()
    p1.update(
        {
            "primary_keys": ["id"],
            "null_validation_tolerance": 0.02,
            "null_validation_columns": ["v"],
        }
    )
    sql1 = _render(p1)
    sql1_compact = " ".join(sql1.split())
    assert "null_join_metrics AS (" in sql1
    assert "INNER JOIN `c`.`s`.`b` t" in sql1
    assert "ON s.`id` = t.`id`" in sql1
    assert "FORMAT_NUMBER(source_nulls_v, 0) AS source_nulls," in sql1
    assert "FORMAT_NUMBER(target_nulls_v, 0) AS target_nulls," in sql1
    assert "SUM(CASE WHEN s.`v` IS NULL THEN 1 ELSE 0 END) AS source_nulls_v" in sql1
    assert "SUM(CASE WHEN t.`v` IS NULL THEN 1 ELSE 0 END) AS target_nulls_v" in sql1
    assert (
        "CASE WHEN source_nulls_v = 0 THEN CASE WHEN target_nulls_v = 0 THEN 0.0 ELSE 100.0 END"
        in sql1_compact
    )
    assert "ABS(source_nulls_v - target_nulls_v) / CAST(source_nulls_v AS DOUBLE) * 100" in sql1
    assert "<= 0.02" in sql1

    # Without PKs
    p2 = _base_payload()
    p2.update(
        {
            "null_validation_tolerance": 0.02,
            "null_validation_columns": ["v"],
        }
    )
    sql2 = _render(p2)
    sql2_compact = " ".join(sql2.split())
    assert "source_stats AS (" in sql2
    assert "target_stats AS (" in sql2
    assert "SUM(CASE WHEN `v` IS NULL THEN 1 ELSE 0 END) AS source_nulls_v" in sql2
    assert "SUM(CASE WHEN `v` IS NULL THEN 1 ELSE 0 END) AS target_nulls_v" in sql2
    assert "INNER JOIN" not in sql2
    assert "CASE WHEN source_nulls_v = 0 THEN target_nulls_v = 0 ELSE" in sql2_compact


def test_aggregate_validations_block_exact_fragments():
    p = _base_payload()
    p.update(
        {"agg_validations": [{"column": "v", "validations": [{"agg": "sum", "tolerance": 0.05}]}]}
    )
    sql = _render(p)
    assert "source_stats AS (" in sql
    assert "target_stats AS (" in sql
    assert "TRY_CAST(SUM(`v`) AS DECIMAL(38, 6)) AS source_value_v_SUM" in sql
    assert "TRY_CAST(SUM(`v`) AS DECIMAL(38, 6)) AS target_value_v_SUM" in sql
    assert "FORMAT_NUMBER(source_value_v_SUM, 2) as source_value," in sql
    assert "FORMAT_STRING('%.2f%%', CAST(0.05 * 100 AS DOUBLE)) AS tolerance_percent," in sql
    assert "source_value_v_SUM - target_value_v_SUM" in sql
    assert "<= 0.05 THEN 'PASS'" in sql


def test_full_combo_contains_all_sections_and_cross_joins_in_order():
    p = _base_payload()
    p.update(
        {
            "count_tolerance": 0.01,
            "primary_keys": ["id"],
            "pk_row_hash_check": True,
            "pk_hash_tolerance": 0.0,
            "hash_columns": ["id", "v"],
            "null_validation_tolerance": 0.02,
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
        "source_stats CROSS JOIN target_stats CROSS JOIN row_hash_metrics CROSS JOIN null_join_metrics"
        in cj_block
    )
    # Ensure overall_validation_passed has all four conditions
    assert sql.count("<= 0.01") >= 1  # count
    assert "mismatch_count / NULLIF(CAST(total_compared_rows AS DOUBLE), 0)" in sql  # pk hash
    assert "null_join_metrics AS (" in sql  # nulls joined
    assert "source_value_v_SUM - target_value_v_SUM" in sql  # agg


def test_uniqueness_only_block_exact_fragments():
    p = _base_payload()
    p.update(
        {
            "uniqueness_columns": ["email"],
            "uniqueness_tolerance": 0.0,
        }
    )
    sql = _render(p)
    # Stats include distinct struct counts
    assert re.search(
        r"COUNT\(DISTINCT named_struct\(\s*'email',\s*`email`\s*\)\)\s+AS source_distinct_uniqs",
        sql,
    )
    assert re.search(
        r"COUNT\(DISTINCT named_struct\(\s*'email',\s*`email`\s*\)\)\s+AS target_distinct_uniqs",
        sql,
    )
    # Payload fields
    assert "AS uniqueness_validation_email" in sql
    assert "FORMAT_NUMBER(source_count - source_distinct_uniqs, 0) AS source_duplicates," in sql
    assert "FORMAT_NUMBER(target_count - target_distinct_uniqs, 0) AS target_duplicates," in sql
    assert "(source_count - source_distinct_uniqs) / NULLIF(CAST(source_count AS DOUBLE), 0)" in sql
    assert "(target_count - target_distinct_uniqs) / NULLIF(CAST(target_count AS DOUBLE), 0)" in sql
    # Overall pass condition includes both source and target constraints
    assert (
        "COALESCE((source_count - source_distinct_uniqs) / NULLIF(CAST(source_count AS DOUBLE), 0), 0) <= 0.0"
        in sql
    )
    assert (
        "COALESCE((target_count - target_distinct_uniqs) / NULLIF(CAST(target_count AS DOUBLE), 0), 0) <= 0.0"
        in sql
    )


def test_full_combo_includes_uniqueness_in_stats_and_payload():
    p = _base_payload()
    p.update(
        {
            "count_tolerance": 0.01,
            "primary_keys": ["id"],
            "pk_row_hash_check": True,
            "pk_hash_tolerance": 0.0,
            "hash_columns": ["id", "v"],
            "null_validation_tolerance": 0.02,
            "null_validation_columns": ["v"],
            "agg_validations": [
                {"column": "v", "validations": [{"agg": "sum", "tolerance": 0.05}]}
            ],
            "uniqueness_columns": ["email"],
            "uniqueness_tolerance": 0.0,
        }
    )
    sql = _render(p)
    cj_idx = sql.index("FROM\n")
    cj_block = sql[cj_idx : cj_idx + 600]
    # Cross join should only include stats, target stats, row hash, null join
    assert (
        "source_stats CROSS JOIN target_stats CROSS JOIN row_hash_metrics CROSS JOIN null_join_metrics"
        in cj_block
    )
    # Payload still includes uniqueness struct populated from stats
    assert "AS uniqueness_validation_email" in sql
    assert "source_count - source_distinct_uniqs" in sql


def test_uniqueness_multi_columns_alias():
    p = _base_payload()
    p.update(
        {
            "uniqueness_columns": ["email", "domain"],
            "uniqueness_tolerance": 0.0,
        }
    )
    sql = _render(p)
    assert "AS uniqueness_validation_email_domain" in sql


def test_custom_sql_validation_block_contains_metrics_and_payload():
    p = _base_payload()
    p.update(
        {
            "custom_sql_tests": [
                {
                    "name": "Status Distribution",
                    "description": "Ensure status counts align",
                    "cte_base_name": "status_distribution",
                    "source_sql": "SELECT status, COUNT(*) AS cnt FROM `c`.`s`.`a` GROUP BY status",
                    "target_sql": "SELECT status, COUNT(*) AS cnt FROM `c`.`s`.`b` GROUP BY status",
                    "base_sql": "SELECT status, COUNT(*) AS cnt FROM {{ table_fqn }} GROUP BY status",
                }
            ]
        }
    )
    sql = _render(p)
    assert "custom_sql_metrics_status_distribution AS (" in sql
    assert "rows_missing_in_target_status_distribution" in sql
    assert "rows_missing_in_source_status_distribution" in sql
    assert "AS custom_sql_validation_status_distribution" in sql
    assert "rendered_source_sql" in sql
    assert "COALESCE(rows_missing_in_target_status_distribution, 0) = 0" in sql
    assert "COALESCE(rows_missing_in_source_status_distribution, 0) = 0" in sql


def test_full_combo_with_custom_includes_all_sections_and_cross_joins():
    p = _base_payload()
    p.update(
        {
            "count_tolerance": 0.01,
            "primary_keys": ["id"],
            "pk_row_hash_check": True,
            "pk_hash_tolerance": 0.0,
            "hash_columns": ["id", "v"],
            "null_validation_tolerance": 0.02,
            "null_validation_columns": ["v"],
            "agg_validations": [
                {"column": "v", "validations": [{"agg": "sum", "tolerance": 0.05}]}
            ],
            "uniqueness_columns": ["email"],
            "uniqueness_tolerance": 0.0,
            "custom_sql_tests": [
                {
                    "name": "Segment Distribution",
                    "description": "Ensure per-segment counts remain aligned",
                    "cte_base_name": "segment_distribution",
                    "source_sql": "SELECT segment, COUNT(*) AS cnt FROM `c`.`s`.`a` GROUP BY segment",
                    "target_sql": "SELECT segment, COUNT(*) AS cnt FROM `c`.`s`.`b` GROUP BY segment",
                    "base_sql": "SELECT segment, COUNT(*) AS cnt FROM {{ table_fqn }} GROUP BY segment",
                }
            ],
        }
    )
    sql = _render(p)
    cj_idx = sql.index("FROM\n")
    cj_block = sql[cj_idx : cj_idx + 800]
    assert (
        "source_stats CROSS JOIN target_stats CROSS JOIN row_hash_metrics CROSS JOIN null_join_metrics CROSS JOIN custom_sql_metrics_segment_distribution"
        in cj_block
    )
    assert "AS custom_sql_validation_segment_distribution" in sql
    assert "COALESCE(rows_missing_in_target_segment_distribution, 0) = 0" in sql
    assert "COALESCE(rows_missing_in_source_segment_distribution, 0) = 0" in sql
    assert "rendered_target_sql" in sql
    assert "SELECT segment, COUNT(*) AS cnt FROM `c`.`s`.`b` GROUP BY segment" in sql
