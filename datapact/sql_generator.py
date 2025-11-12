"""SQL generation utilities for DataPact.

This module centralizes rendering of SQL scripts from Jinja templates, keeping
client code small and focused.
"""

from __future__ import annotations

from jinja2 import Environment

from .config import ValidationTask
from .sql_utils import make_sql_identifier


def render_validation_sql(
    env: Environment,
    config: ValidationTask,
    results_table: str,
    job_name: str,
) -> str:
    """Render the validation SQL for a single task from the Jinja template."""
    template = env.get_template("validation.sql.j2")
    payload = config.model_dump()
    payload["results_table"] = results_table
    payload["job_name"] = job_name
    if config.custom_sql_tests:
        common_context = {
            "source_catalog": config.source_catalog,
            "source_schema": config.source_schema,
            "source_table": config.source_table,
            "source_fqn": f"`{config.source_catalog}`.`{config.source_schema}`.`{config.source_table}`",
            "target_catalog": config.target_catalog,
            "target_schema": config.target_schema,
            "target_table": config.target_table,
            "target_fqn": f"`{config.target_catalog}`.`{config.target_schema}`.`{config.target_table}`",
            "declared_source_catalog": config.source_catalog,
            "declared_source_schema": config.source_schema,
            "declared_source_table": config.source_table,
            "declared_target_catalog": config.target_catalog,
            "declared_target_schema": config.target_schema,
            "declared_target_table": config.target_table,
        }
        rendered_tests: list[dict[str, str | None]] = []
        seen_cte_names: dict[str, str] = {}
        for test in config.custom_sql_tests:
            cte_base_name = make_sql_identifier(test.name, prefix="custom_sql")
            previous = seen_cte_names.get(cte_base_name)
            if previous:
                raise ValueError(
                    "Custom SQL test names must remain unique after sanitization. "
                    f"'{test.name}' conflicts with '{previous}' because both render to "
                    f"'custom_sql_validation_{cte_base_name}'. Please choose distinct labels."
                )
            seen_cte_names[cte_base_name] = test.name

            sql_template = env.from_string(test.sql)
            source_context = {
                **common_context,
                "table_catalog": config.source_catalog,
                "table_schema": config.source_schema,
                "table_name": config.source_table,
                "table_fqn": common_context["source_fqn"],
                "rendered_role": "source",
            }
            target_context = {
                **common_context,
                "table_catalog": config.target_catalog,
                "table_schema": config.target_schema,
                "table_name": config.target_table,
                "table_fqn": common_context["target_fqn"],
                "rendered_role": "target",
            }

            rendered_tests.append(
                {
                    "name": test.name,
                    "description": test.description,
                    "cte_base_name": cte_base_name,
                    "source_sql": sql_template.render(**source_context).strip(),
                    "target_sql": sql_template.render(**target_context).strip(),
                    "base_sql": test.sql,
                }
            )
        payload["custom_sql_tests"] = rendered_tests
    return template.render(**payload).strip()


def render_aggregate_sql(
    env: Environment,
    results_table: str,
    run_id_literal: str | None = None,
) -> str:
    """Render the aggregate results SQL from the shared template."""
    template = env.get_template("aggregate_results.sql.j2")
    context: dict[str, str] = {"results_table": results_table}
    if run_id_literal is not None:
        context["run_id_literal"] = run_id_literal
    return template.render(**context).strip()
