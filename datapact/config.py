"""
This module defines Pydantic models for configuring DataPact validation tasks.

Classes:
    AggValidationDetail: Represents the details for aggregate validation, including aggregation method and tolerance.
    AggValidation: Represents a set of aggregation validations for a specific column.
    ValidationTask: Represents a validation task configuration for comparing source and target tables, including options for primary key checks, row count tolerance, hash validation, null validation, and aggregate validations.
    DataPactConfig: The root configuration model containing a list of validation tasks.

These models are intended to be used for parsing and validating DataPact YAML configuration files.
"""

import re

from pydantic import BaseModel, Field, field_validator, model_validator

from .sql_utils import make_sql_identifier


class CustomSqlTest(BaseModel):
    """
    Represents a user-defined SQL validation that runs against both source and target tables.

    Attributes:
        name (str): Friendly identifier that appears in rendered results.
        sql (str): SQL query (without trailing semicolon) executed independently against the
            source and target tables. Results are compared for exact equality. The string
            may reference templated fields such as ``{{ table_fqn }}``, ``{{ source_fqn }}``,
            or ``{{ target_fqn }}``.
        description (str | None): Optional human-friendly context about what the custom SQL checks.
    """

    name: str
    sql: str
    description: str | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Ensure the custom SQL test name is non-empty and identifier-friendly."""

        trimmed = value.strip()
        if not trimmed:
            raise ValueError("Custom SQL test name cannot be empty.")
        if len(trimmed) > 128:
            raise ValueError("Custom SQL test name must be 128 characters or fewer.")
        # Allow letters, numbers, spaces, underscores, and hyphens to keep friendly labels.
        if not re.fullmatch(r"[A-Za-z0-9_\-\s]+", trimmed):
            raise ValueError(
                "Custom SQL test name may only contain letters, numbers, spaces, underscores, and hyphens."
            )
        return trimmed

    @field_validator("sql")
    @classmethod
    def validate_sql(cls, value: str) -> str:
        """Ensure SQL is present and does not include trailing semicolons."""

        stripped = value.strip()
        if not stripped:
            raise ValueError("Custom SQL must be provided.")
        if stripped.endswith(";"):
            raise ValueError("Custom SQL should not include a trailing semicolon.")
        return stripped


# Represents the innermost validation: { agg: "SUM", tolerance: 0.05 }
class AggValidationDetail(BaseModel):
    """
    Represents the details for aggregate validation.

    Attributes:
        agg (str): The aggregation method to be used (e.g., 'sum', 'mean').
        tolerance (float): The allowed tolerance for validation.
    """

    agg: str
    tolerance: float


class AggValidation(BaseModel):
    """
    Represents a set of aggregation validations for a specific column.

    Attributes:
        column (str): The name of the column to which the validations apply.
        validations (list[AggValidationDetail]): A list of aggregation validation details to be applied to the column.
    """

    column: str
    validations: list[AggValidationDetail]


# Represents a single validation task from the 'validations' list
class ValidationTask(BaseModel):
    """
    Represents a validation task configuration for comparing source and target tables.

    Attributes:
        task_key (str): Unique identifier for the validation task.
        source_catalog (str): Catalog name of the source table.
        source_schema (str): Schema name of the source table.
        source_table (str): Name of the source table.
        target_catalog (str): Catalog name of the target table.
        target_schema (str): Schema name of the target table.
        target_table (str): Name of the target table.
        primary_keys (list[str] | None): List of primary key columns used for row matching. Optional.
        filter (str | None): Optional SQL predicate applied to built-in validations to limit scanned rows.
        count_tolerance (float | None): Allowed tolerance for row count differences between source and target. Optional.
        pk_row_hash_check (bool | None): Whether to perform row hash validation on primary keys. Defaults to False.
        pk_hash_tolerance (float | None): Tolerance for acceptable row hash differences. Optional.
        hash_columns (list[str] | None): List of columns to include in hash calculations. Optional.
        null_validation_tolerance (float | None): Tolerance for acceptable null value differences. Optional.
        null_validation_columns (list[str] | None): Columns to include in null value validation. Optional.
        agg_validations (list[AggValidation] | None): List of aggregate validation rules to apply. Optional.
        business_domain (str | None): Business pillar (Sales, Finance, etc.) for executive grouping.
        business_owner (str | None): Executive or team accountable for this dataset.
        business_priority (str | None): Priority classification (Critical/High/Medium/Low).
        expected_sla_hours (float | None): Target remediation window in hours for failures.
        estimated_impact_usd (float | None): Estimated financial exposure if the check fails.
        custom_sql_tests (list[CustomSqlTest] | None): User-defined SQL validations to execute on source and target.
    """

    task_key: str
    source_catalog: str
    source_schema: str
    source_table: str
    target_catalog: str
    target_schema: str
    target_table: str
    primary_keys: list[str] | None = None
    filter: str | None = None
    count_tolerance: float | None = None
    pk_row_hash_check: bool | None = Field(default=False)
    pk_hash_tolerance: float | None = None
    hash_columns: list[str] | None = None
    null_validation_tolerance: float | None = None
    null_validation_columns: list[str] | None = None
    agg_validations: list[AggValidation] | None = None
    # New optional validations
    uniqueness_columns: list[str] | None = None
    uniqueness_tolerance: float | None = None
    # Business metadata used for executive dashboards and ROI calculations
    business_domain: str | None = None
    business_owner: str | None = None
    business_priority: str | None = None
    expected_sla_hours: float | None = None
    estimated_impact_usd: float | None = None
    custom_sql_tests: list[CustomSqlTest] | None = None

    @field_validator("filter")
    @classmethod
    def validate_filter(cls, value: str | None) -> str | None:
        """Normalize optional row-level filter statements."""

        if value is None:
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("Filter cannot be empty or whitespace.")
        if normalized.endswith(";"):
            raise ValueError("Filter should not include a trailing semicolon.")
        return normalized

    @field_validator(
        "count_tolerance",
        "pk_hash_tolerance",
        "null_validation_tolerance",
        "uniqueness_tolerance",
    )
    @classmethod
    def tolerance_must_be_a_ratio(cls, v: float | None) -> float | None:
        """
        Validates that the tolerance value is a valid ratio between 0.0 and 1.0.

        Args:
            v: The tolerance value to validate.

        Returns:
            The validated tolerance value.

        Raises:
            ValueError: If tolerance is not None and not between 0.0 and 1.0 inclusive.
        """
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError("Tolerance must be a float between 0.0 and 1.0")
        return v

    @field_validator("business_priority")
    @classmethod
    def validate_business_priority(cls, value: str | None) -> str | None:
        """Ensure business priority values stay within a simple controlled list."""

        if value is None:
            return value
        allowed = {"critical", "high", "medium", "low"}
        normalized = value.strip().lower()
        if normalized not in allowed:
            raise ValueError("business_priority must be one of: Critical, High, Medium, Low")
        return normalized.upper()

    @field_validator("expected_sla_hours", "estimated_impact_usd")
    @classmethod
    def validate_non_negative(cls, value: float | None, field) -> float | None:
        """Validate that numeric business metadata values are non-negative."""

        if value is None:
            return value
        if value < 0:
            field_name = getattr(field, "field_name", getattr(field, "name", "value"))
            raise ValueError(f"{field_name} must be greater than or equal to 0")
        return value

    @model_validator(mode="after")
    def validate_custom_sql_tests(self) -> "ValidationTask":
        """Ensure custom SQL test names are unique within the task."""

        tests = self.custom_sql_tests or []
        seen: set[str] = set()
        cte_names: dict[str, str] = {}
        for test in tests:
            lowered = test.name.lower()
            if lowered in seen:
                raise ValueError(
                    f"Duplicate custom SQL test name detected: '{test.name}'. Names must be unique per task."
                )
            seen.add(lowered)

            normalized = make_sql_identifier(test.name, prefix="custom_sql")
            collision = cte_names.get(normalized)
            if collision:
                raise ValueError(
                    "Custom SQL test names must remain unique even after sanitization. "
                    f"'{test.name}' conflicts with '{collision}' because both map "
                    f"to 'custom_sql_validation_{normalized}'."
                )
            cte_names[normalized] = test.name
        return self


# This is the root model for the entire YAML file
class DataPactConfig(BaseModel):
    """
    Configuration model for DataPact.

    Attributes:
        validations (list[ValidationTask]):
            A list of validation tasks to be executed as part of the DataPact configuration.
    """

    validations: list[ValidationTask]
