import importlib


def _make_env():
    jinja2 = importlib.import_module("jinja2")
    return jinja2.Environment(
        loader=jinja2.PackageLoader("datapact", "templates"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def test_aggregate_results_template_exact():
    env = _make_env()
    t = env.get_template("aggregate_results.sql.j2")
    sql = t.render(results_table="`datapact`.`results`.`run_history`").strip()
    assert "WITH run_results AS (" in sql
    assert (
        "SELECT * FROM `datapact`.`results`.`run_history` WHERE run_id = :run_id" in sql
    )
    assert "agg_metrics AS (" in sql
    assert "SELECT CASE" in sql
    assert "RAISE_ERROR(CONCAT('DataPact validation tasks failed: '" in sql
    assert sql.endswith(";")
