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
        pk_hash_threshold (float | None): Threshold for acceptable row hash differences. Optional.
        hash_columns (list[str] | None): List of columns to include in hash calculations. Optional.
        null_validation_threshold (float | None): Threshold for acceptable null value differences. Optional.
        null_validation_columns (list[str] | None): Columns to include in null value validation. Optional.
        agg_validations (list[AggValidation] | None): List of aggregate validation rules to apply. Optional.
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
    pk_hash_threshold: float | None = None
    hash_columns: list[str] | None = None
    null_validation_threshold: float | None = None
    null_validation_columns: list[str] | None = None
    agg_validations: list[AggValidation] | None = None

    @field_validator(
        "count_tolerance",
        "pk_hash_threshold",
        "null_validation_threshold",
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


# This is the root model for the entire YAML file
class DataPactConfig(BaseModel):
    """
    Configuration model for DataPact.

    Attributes:
        validations (list[ValidationTask]):
            A list of validation tasks to be executed as part of the DataPact configuration.
    """

    validations: list[ValidationTask]
