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


def yaml_files() -> list[Path]:
    return sorted((DBT / "models").rglob("*.yml"))


def test_the_project_writes_its_descriptions_into_the_warehouse():
    # persist_docs makes dbt comment every table, view and column that it builds.
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    pattern = (
        r"^  agoranomos:\n(?:    #.*\n)*"
        r"    \+persist_docs:\n      relation: true\n      columns: true$"
    )
    assert re.search(pattern, project, flags=re.MULTILINE)


def test_a_key_of_a_contract_has_no_name():
    # dbt builds a table under a temporary name and renames it, and the index of a key keeps the
    # name that it got: a named key collides with itself on the next run ("relation ... already
    # exists"), while PostgreSQL gives an unnamed one a free name each time.
    keys = [
        (path.name, match.group(2))
        for path in yaml_files()
        for match in re.finditer(
            r"^( +)- type: primary_key\n((?:\1  .*\n)*)",
            path.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
    ]
    assert keys, "no model declares a primary key"
    named = [name for name, block in keys if re.search(r"^ +name:", block, flags=re.MULTILINE)]
    assert not named, f"a primary key has a name in {named}"


def test_a_description_over_several_lines_ends_without_a_line_break():
    # A folded description, >, ends with a line break, which the comment in the warehouse keeps;
    # >- drops it. A literal block, |, keeps every line break.
    for path in yaml_files():
        text = path.read_text(encoding="utf-8")
        found = re.findall(r"^ *description: *([>|][+0-9]*)$", text, flags=re.MULTILINE)
        assert set(found) <= {">-"}, f"{path.name}: write descriptions as >-, not {found}"


def test_the_mart_has_a_contract_and_its_key():
    text = (DBT / "models" / "marts" / "_marts__models.yml").read_text(encoding="utf-8")
    assert re.search(r"^      contract:\n        enforced: true$", text, flags=re.MULTILINE)
    assert re.search(
        r"^      - type: primary_key\n        columns: \[report_date, account_id, campaign_id\]$",
        text,
        flags=re.MULTILINE,
    )
