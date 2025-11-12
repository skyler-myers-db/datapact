"""SQL utility functions for safe query construction."""

from __future__ import annotations

import re
from typing import Any


def escape_sql_identifier(identifier: str) -> str:
    """Escape SQL identifier (table, column, schema names) for safe use in queries.

    Args:
        identifier: The identifier to escape

    Returns:
        Escaped identifier wrapped in backticks

    Raises:
        ValueError: If identifier contains invalid characters
    """
    # Validate identifier - only allow alphanumeric, underscore, dash, dots, and backticks
    if not re.match(r"^[\w\-\.`]+$", identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}")

    # Check for SQL injection patterns (but allow them as part of legitimate names)
    # Only block if they appear as standalone words or with suspicious syntax
    upper_id = identifier.upper()
    forbidden_keywords = [
        "DROP",
        "INSERT",
        "UPDATE",
        "EXEC",
        "DELETE",
        "TRUNCATE",
        "ALTER",
    ]
    if any(
        f" {keyword} " in f" {upper_id} "
        or upper_id.startswith(f"{keyword} ")
        or upper_id.endswith(f" {keyword}")
        for keyword in forbidden_keywords
    ):
        raise ValueError(f"Invalid SQL identifier: {identifier}")

    # Block obvious SQL comment/termination patterns
    if "--" in identifier or ";" in identifier:
        raise ValueError(f"Invalid SQL identifier: {identifier}")

    # Escape backticks if present and wrap in backticks
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def escape_sql_string(value: str) -> str:
    """Escape a string value for safe use in SQL queries.

    Args:
        value: The string value to escape

    Returns:
        Escaped string value wrapped in single quotes
    """
    # Escape single quotes by doubling them
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def parse_fully_qualified_name(fqn: str) -> tuple[str, str, str]:
    """Normalize catalog.schema.table input into its tokenized components."""

    cleaned = fqn.replace("`", "").replace("\n", " ").strip()
    parts = [part.strip() for part in cleaned.split(".") if part.strip()]
    if len(parts) != 3:
        raise ValueError(
            "Fully qualified table names must include catalog.schema.table (three parts)."
        )
    return parts[0], parts[1], parts[2]


def format_fully_qualified_name(catalog: str, schema: str, table: str) -> str:
    """Quote catalog/schema/table segments safely for use in SQL statements."""

    return ".".join(
        [
            escape_sql_identifier(catalog),
            escape_sql_identifier(schema),
            escape_sql_identifier(table),
        ]
    )


def validate_job_name(job_name: str) -> str:
    """Validate and sanitize a job name for safe use.

    Args:
        job_name: The job name to validate

    Returns:
        The validated job name

    Raises:
        ValueError: If job name contains invalid characters
    """
    # Allow alphanumeric, spaces, underscores, dashes, and dots
    if not re.match(r"^[\w\s\-\.]+$", job_name):
        raise ValueError(
            f"Invalid job name: {job_name}. "
            "Only alphanumeric characters, spaces, underscores, dashes, and dots are allowed."
        )

    # Limit length to prevent abuse
    if len(job_name) > 255:
        raise ValueError(f"Job name too long: {job_name}. Maximum 255 characters allowed.")

    return job_name


def build_safe_filter(column: str, value: Any, operator: str = "=") -> str:
    """Build a safe SQL WHERE clause filter.

    Args:
        column: The column name to filter on
        value: The value to filter for
        operator: The SQL operator to use (default: "=")

    Returns:
        A safe SQL filter expression
    """
    safe_column = escape_sql_identifier(column)

    if value is None:
        if operator == "=":
            return f"{safe_column} IS NULL"
        elif operator == "!=":
            return f"{safe_column} IS NOT NULL"
        else:
            raise ValueError(f"Invalid operator for NULL value: {operator}")

    if isinstance(value, bool):
        # Handle bool before int since bool is a subclass of int in Python
        safe_value = "TRUE" if value else "FALSE"
    elif isinstance(value, str):
        safe_value = escape_sql_string(value)
    elif isinstance(value, int | float):
        safe_value = str(value)
    else:
        raise ValueError(f"Unsupported value type: {type(value)}")

    # Validate operator
    valid_operators = ["=", "!=", "<", ">", "<=", ">=", "LIKE", "NOT LIKE"]
    if operator not in valid_operators:
        raise ValueError(f"Invalid operator: {operator}")

    return f"{safe_column} {operator} {safe_value}"


def make_sql_identifier(value: str, prefix: str = "cte") -> str:
    """Generate a lowercase SQL-safe identifier derived from free-form text."""

    cleaned = re.sub(r"\s+", "_", value.strip())
    cleaned = re.sub(r"[^\w]", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_").lower()
    if not cleaned:
        cleaned = prefix
    if cleaned[0].isdigit():
        cleaned = f"{prefix}_{cleaned}"
    return cleaned
