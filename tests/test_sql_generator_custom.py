"""Tests for rendering validation SQL with custom SQL tests."""

import importlib

from datapact.config import (
    ValidationTask,
    AggValidation,
    AggValidationDetail,
    CustomSqlTest,
)
from datapact.sql_generator import render_validation_sql


def _env():
    jinja2 = importlib.import_module("jinja2")
    return jinja2.Environment(
        loader=jinja2.PackageLoader("datapact", "templates"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=["jinja2.ext.do"],
    )


def _task_with_all_features() -> ValidationTask:
    return ValidationTask(
        task_key="customers_global_health",
        source_catalog="source",
        source_schema="analytics",
        source_table="customers_daily",
        target_catalog="target",
        target_schema="analytics",
        target_table="customers_daily_curated",
        primary_keys=["customer_id"],
        count_tolerance=0.01,
        pk_row_hash_check=True,
        pk_hash_tolerance=0.02,
        hash_columns=["customer_id", "status", "segment"],
        null_validation_tolerance=0.05,
        null_validation_columns=["status", "segment"],
        uniqueness_columns=["email", "country"],
        uniqueness_tolerance=0.0,
        agg_validations=[
            AggValidation(
                column="lifetime_value",
                validations=[AggValidationDetail(agg="sum", tolerance=0.03)],
            )
        ],
        custom_sql_tests=[
            CustomSqlTest(
                name="Status Rollup",
                description="Compare daily status distribution by country.",
                sql="""
                    SELECT country,
                           status,
                           COUNT(*) AS record_count
                    FROM {{ table_fqn }}
                    GROUP BY country, status
                """,
            ),
            CustomSqlTest(
                name="Role Aware Template",
                description="Embeds the rendering role for auditing.",
                sql="""
                    SELECT '{{ rendered_role }}' AS rendered_role,
                           '{{ declared_source_catalog }}' AS declared_source_catalog,
                           '{{ table_fqn }}' AS table_name,
                           COUNT(*) AS total_rows
                    FROM {{ table_fqn }}
                """,
            ),
        ],
    )


def test_render_validation_sql_renders_custom_templates_per_role():
    env = _env()
    task = _task_with_all_features()
    sql = render_validation_sql(
        env, task, results_table="`datapact`.`results`.`run_history`", job_name="Run"
    )
    # Base SQL preserved for auditing
    assert "Status Rollup" in sql
    assert "Compare daily status distribution by country." in sql
    # Source rendering uses full FQN for source
    assert (
        "SELECT country,\n                           status,\n                           COUNT(*) AS record_count"
        in sql
    )
    assert "`source`.`analytics`.`customers_daily`" in sql
    assert "`target`.`analytics`.`customers_daily_curated`" in sql
    # Role-aware template renders both source and target entries
    assert "SELECT 'source' AS rendered_role" in sql
    assert "SELECT 'target' AS rendered_role" in sql
    # Ensure declared source catalog placeholder passes through
    assert "'source' AS declared_source_catalog" in sql
    assert "custom_sql_validation_status_rollup" in sql
    assert "custom_sql_validation_role_aware_template" in sql
