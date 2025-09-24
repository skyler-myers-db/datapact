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
    assert "CREATE OR REPLACE TEMP VIEW agg_run_results" in sql
    assert "FROM `datapact`.`results`.`run_history`" in sql
    assert "CREATE TABLE IF NOT EXISTS `datapact`.`results`.`exec_run_summary`" in sql
    assert "INSERT INTO `datapact`.`results`.`exec_run_summary`" in sql
    assert "INSERT INTO `datapact`.`results`.`exec_domain_breakdown`" in sql
    assert "SELECT CASE" in sql
    assert "failed_task_keys" in sql
    assert sql.endswith(";")
