from datapact.client import DataPactClient

# pylint: disable=protected-access


def test_jinja_env_builds_and_loads_template():
    # Bypass __init__ to avoid WorkspaceClient
    client = object.__new__(DataPactClient)
    env = client._jinja_env()
    t = env.get_template("validation.sql.j2")
    assert t is not None
