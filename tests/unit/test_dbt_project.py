"""Static checks of the dbt project in dbt/, for what Cosmos and the stack rely on, without dbt."""

import re
from pathlib import Path

DBT = Path(__file__).resolve().parents[2] / "dbt"


def test_the_profile_has_no_jinja_filter_and_literal_numbers():
    # Cosmos 1.15.1 reads profiles.yml again, without dbt, to build the URIs of the assets that it
    # emits. Its readers know the filters of Jinja but not those of dbt: the reader of the local
    # mode knows only as_number of them, and the reader of the watcher mode none, so that as_number
    # there means no asset at all (astronomer/astronomer-cosmos#2948). So the profile keeps to one
    # plain env_var call.
    profile = (DBT / "profiles.yml").read_text(encoding="utf-8")
    assert re.findall(r"\{\{(.*?)\}\}", profile) == [
        " env_var('DBT_ENV_SECRET_WAREHOUSE_PASSWORD', '') "
    ]
    for line in ("host: warehouse-db", "port: 5432", "dbname: warehouse", "threads: 4"):
        # The host, the port and the database make the URIs of the assets that Cosmos emits.
        assert re.search(rf"^\s+{line}$", profile, flags=re.MULTILINE), line


def test_the_project_sends_no_usage_statistics():
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    pattern = r"^flags:\n(?:  #.*\n)*  send_anonymous_usage_stats: false$"
    assert re.search(pattern, project, flags=re.MULTILINE)


def test_the_project_needs_no_packages():
    # Without packages, dbt deps never runs, so a task of dbt needs no network.
    assert (DBT / "dbt_project.yml").is_file(), "there is no dbt project to check"
    assert not (DBT / "packages.yml").exists()
    assert not (DBT / "dependencies.yml").exists()


def test_every_model_has_a_description():
    models = {path.stem for path in (DBT / "models").rglob("*.sql")}
    assert models, "dbt/models holds no model"
    described = set()
    for path in (DBT / "models").rglob("*.yml"):
        text = path.read_text(encoding="utf-8")
        described |= set(
            re.findall(r"^  - name: (\w+)\n    description:", text, flags=re.MULTILINE)
        )
    assert models <= described, sorted(models - described)


def test_the_project_pins_the_dbt_line_of_the_image():
    # dbt_project.yml accepts the 1.12 line that airflow/dbt-requirements.in pins.
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    assert 'require-dbt-version: [">=1.12.0", "<1.13.0"]' in project
    pins = (DBT.parent / "airflow" / "dbt-requirements.in").read_text(encoding="utf-8")
    assert re.search(r"^dbt-core==1\.12\.\d+$", pins, flags=re.MULTILINE)
