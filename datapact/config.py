# datapact/config.py

from pydantic import BaseModel, Field

# Represents the innermost validation: { agg: "SUM", tolerance: 0.05 }
class AggValidationDetail(BaseModel):
    agg: str
    tolerance: float

# Represents a single column's aggregate validations
class AggValidation(BaseModel):
    column: str
    validations: list[AggValidationDetail]

# Represents a single validation task from the 'validations' list
class ValidationTask(BaseModel):
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

# This is the root model for the entire YAML file
class DataPactConfig(BaseModel):
    validations: list[ValidationTask]
