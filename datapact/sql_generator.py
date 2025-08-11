"""SQL generation utilities for DataPact.

This module centralizes rendering of SQL scripts from Jinja templates, keeping
client code small and focused.
"""

from __future__ import annotations

from jinja2 import Environment

from .config import ValidationTask


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
    return template.render(**payload).strip()


def render_aggregate_sql(env: Environment, results_table: str) -> str:
    """Render the aggregate results SQL from the shared template."""
    template = env.get_template("aggregate_results.sql.j2")
    return template.render(results_table=results_table).strip()
