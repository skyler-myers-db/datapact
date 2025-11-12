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
        filter=None,
        count_tolerance=None,
        pk_row_hash_check=False,
        pk_hash_tolerance=None,
        hash_columns=None,
        null_validation_tolerance=None,
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
        ("pk_hash_tolerance", 0.3),
        ("null_validation_tolerance", None),
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
        ("pk_hash_tolerance", 1.00001),
        ("null_validation_tolerance", 2.0),
    ],
)
def test_tolerance_validator_rejects_out_of_range(field, value):
    kwargs = {field: value}
    with pytest.raises(ValidationError) as ei:
        _minimal_task(**kwargs)
    assert "Tolerance must be a float between 0.0 and 1.0" in str(ei.value)


def test_business_priority_normalized_and_validated():
    task = _minimal_task(business_priority="Critical")
    assert task.business_priority == "CRITICAL"


def test_business_priority_invalid_value():
    with pytest.raises(ValidationError) as ei:
        _minimal_task(business_priority="urgent")
    assert "business_priority must be one of" in str(ei.value)


@pytest.mark.parametrize("field", ["expected_sla_hours", "estimated_impact_usd"])
def test_business_numeric_metadata_must_be_positive(field):
    task = _minimal_task(**{field: 12.5})
    assert getattr(task, field) == 12.5


@pytest.mark.parametrize("field", ["expected_sla_hours", "estimated_impact_usd"])
def test_business_numeric_metadata_cannot_be_negative(field):
    with pytest.raises(ValidationError) as ei:
        _minimal_task(**{field: -1})
    assert f"{field} must be greater than or equal to 0" in str(ei.value)


def test_custom_sql_test_requires_unique_names_case_insensitive():
    with pytest.raises(ValidationError) as ei:
        _minimal_task(
            custom_sql_tests=[
                {"name": "Balance Check", "sql": "SELECT 1"},
                {"name": "balance check", "sql": "SELECT 1"},
            ]
        )
    assert "Duplicate custom SQL test name detected" in str(ei.value)


def test_custom_sql_test_detects_identifier_collisions_after_sanitization():
    with pytest.raises(ValidationError) as ei:
        _minimal_task(
            custom_sql_tests=[
                {"name": "Daily Sales", "sql": "SELECT 1"},
                {"name": "Daily-Sales", "sql": "SELECT 1"},
            ]
        )
    assert "must remain unique even after sanitization" in str(ei.value)


def test_custom_sql_test_rejects_trailing_semicolon():
    with pytest.raises(ValidationError) as ei:
        _minimal_task(
            custom_sql_tests=[
                {"name": "Trailing", "sql": "SELECT 1;"},
            ]
        )
    assert "should not include a trailing semicolon" in str(ei.value)


def test_custom_sql_test_valid_config():
    task = _minimal_task(
        custom_sql_tests=[
            {
                "name": "Daily Totals",
                "sql": "SELECT date, SUM(amount) FROM {{ table_fqn }} GROUP BY date",
                "description": "Ensure aggregated totals remain stable",
            }
        ]
    )
    assert task.custom_sql_tests is not None
    assert task.custom_sql_tests[0].name == "Daily Totals"


def test_filter_is_trimmed_and_preserved():
    task = _minimal_task(filter="  status = 'active'  ")
    assert task.filter == "status = 'active'"


def test_filter_cannot_be_empty():
    with pytest.raises(ValidationError) as ei:
        _minimal_task(filter="   ")
    assert "Filter cannot be empty" in str(ei.value)


def test_filter_cannot_have_trailing_semicolon():
    with pytest.raises(ValidationError) as ei:
        _minimal_task(filter="status = 'active';")
    assert "Filter should not include a trailing semicolon." in str(ei.value)
