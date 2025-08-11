import pytest
from pydantic import ValidationError

from datapact.config import ValidationTask


def _minimal_task(**overrides):
    base = dict(
        task_key="t",
        source_catalog="c",
        source_schema="s",
        source_table="a",
        target_catalog="c",
        target_schema="s",
        target_table="b",
        primary_keys=["id"],
        count_tolerance=None,
        pk_row_hash_check=False,
        pk_hash_threshold=None,
        hash_columns=None,
        null_validation_threshold=None,
        null_validation_columns=None,
        agg_validations=None,
    )
    base.update(overrides)
    return ValidationTask(**base)


@pytest.mark.parametrize(
    "field,value",
    [
        ("count_tolerance", 0.0),
        ("count_tolerance", 1.0),
        ("pk_hash_threshold", 0.3),
        ("null_validation_threshold", None),
    ],
)
def test_tolerance_validator_accepts_valid_values(field, value):
    kwargs = {field: value}
    task = _minimal_task(**kwargs)
    assert getattr(task, field) == value


@pytest.mark.parametrize(
    "field,value",
    [
        ("count_tolerance", -0.0001),
        ("pk_hash_threshold", 1.00001),
        ("null_validation_threshold", 2.0),
    ],
)
def test_tolerance_validator_rejects_out_of_range(field, value):
    kwargs = {field: value}
    with pytest.raises(ValidationError) as ei:
        _minimal_task(**kwargs)
    assert "Tolerance must be a float between 0.0 and 1.0" in str(ei.value)
