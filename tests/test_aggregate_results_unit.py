import importlib


def _make_env():
    jinja2 = importlib.import_module("jinja2")
    return jinja2.Environment(
        loader=jinja2.PackageLoader("datapact", "templates"),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def test_aggregate_results_renders_with_table_fqn() -> None:
    env = _make_env()
    t = env.get_template("aggregate_results.sql.j2")
    sql = t.render(results_table="`c`.`s`.`t`\n").strip()
    compact = " ".join(sql.split())
    assert "FROM `c`.`s`.`t` WHERE run_id = :run_id" in compact
    assert sql.endswith(";")
