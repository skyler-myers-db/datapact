import importlib

from datapact.client import DataPactClient

# pylint: disable=protected-access
from datapact.config import (
    DataPactConfig,
    ValidationTask,
    AggValidation,
    AggValidationDetail,
)


class _DummyWorkspace:
    def __init__(self) -> None:
        self.uploads: dict[str, str] = {}
        self.dirs: list[str] = []

    def mkdirs(self, path: str) -> None:  # signature compatibility
        self.dirs.append(path)

    def upload(self, *, path: str, content: bytes, overwrite: bool, format):  # noqa: ANN001
        # Capture text content; in client code content is utf-8 bytes
        self.uploads[path] = content.decode("utf-8")


class _DummyW:
    def __init__(self) -> None:
        self.workspace = _DummyWorkspace()


def _make_env():
    jinja2 = importlib.import_module("jinja2")
    return jinja2.Environment(
        loader=jinja2.PackageLoader("datapact", "templates"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=["jinja2.ext.do"],
    )


def test_upload_sql_scripts_renders_and_uploads_expected_files() -> None:
    task = ValidationTask(
        task_key="t_upload",
        source_catalog="c",
        source_schema="s",
        source_table="a",
        target_catalog="c",
        target_schema="s",
        target_table="b",
        primary_keys=["id"],
        count_tolerance=0.01,
        pk_row_hash_check=True,
        pk_hash_threshold=0.0,
        hash_columns=["id", "v"],
        null_validation_threshold=0.02,
        null_validation_columns=["v"],
        agg_validations=[
            AggValidation(
                column="v",
                validations=[
                    AggValidationDetail(agg="sum", tolerance=0.05),
                ],
            )
        ],
    )
    config = DataPactConfig(validations=[task])

    # Bypass __init__ and attach a dummy workspace
    client = object.__new__(DataPactClient)
    client.w = _DummyW()  # type: ignore[assignment]
    client.root_path = "/tmp/datapact"

    results_table = "`datapact`.`results`.`run_history`"
    job_name = "unit_upload_job"
    assets = client._upload_sql_scripts(config, results_table, job_name)

    # Assert paths include task, aggregate, and genie datasets setup
    assert set(assets.keys()) == {
        "t_upload",
        "aggregate_results",
        "setup_genie_datasets",
    }

    dw = client.w.workspace  # type: ignore[assignment]
    # Validate uploaded content for task matches the template render
    env = _make_env()
    validation_template = env.get_template("validation.sql.j2")
    payload = task.model_dump()
    payload["results_table"] = results_table
    payload["job_name"] = job_name
    expected_validation_sql = validation_template.render(**payload).strip()

    task_path = assets["t_upload"]
    assert task_path in dw.uploads  # type: ignore[attr-defined]
    assert dw.uploads[task_path].strip() == expected_validation_sql  # type: ignore[attr-defined]

    # Validate aggregate_results content
    agg_template = env.get_template("aggregate_results.sql.j2")
    expected_agg_sql = agg_template.render(results_table=results_table).strip()

    agg_path = assets["aggregate_results"]
    assert agg_path in dw.uploads  # type: ignore[attr-defined]
    assert dw.uploads[agg_path].strip() == expected_agg_sql  # type: ignore[attr-defined]
