"""
This module defines Pydantic models for configuring DataPact validation tasks.

Classes:
    AggValidationDetail: Represents the details for aggregate validation, including aggregation method and tolerance.
    AggValidation: Represents a set of aggregation validations for a specific column.
    ValidationTask: Represents a validation task configuration for comparing source and target tables, including options for primary key checks, row count tolerance, hash validation, null validation, and aggregate validations.
    DataPactConfig: The root configuration model containing a list of validation tasks.

These models are intended to be used for parsing and validating DataPact YAML configuration files.
"""

from pydantic import BaseModel, Field, field_validator


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
    """

    task_key: str
    source_catalog: str
    source_schema: str
    source_table: str
    target_catalog: str
    target_schema: str
    target_table: str
    primary_keys: list[str] | None = None
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
            raise ValueError(
                "business_priority must be one of: Critical, High, Medium, Low"
            )
        return normalized.upper()

    @field_validator("expected_sla_hours", "estimated_impact_usd")
    @classmethod
    def validate_non_negative(cls, value: float | None, field) -> float | None:
        """Validate that numeric business metadata values are non-negative."""

        if value is None:
            return value
        if value < 0:
            raise ValueError(f"{field.name} must be greater than or equal to 0")
        return value


# This is the root model for the entire YAML file
class DataPactConfig(BaseModel):
    """
    Configuration model for DataPact.

    Attributes:
        validations (list[ValidationTask]):
            A list of validation tasks to be executed as part of the DataPact configuration.
    """

    validations: list[ValidationTask]
