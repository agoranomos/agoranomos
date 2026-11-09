"""Static checks of the dbt project in dbt/, for what Cosmos and the stack rely on, without dbt."""

import ast
import csv
import re
from pathlib import Path

DBT = Path(__file__).resolve().parents[2] / "dbt"
SEEDS = DBT / "seeds"
REPORTING = "{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}"


def test_the_profile_has_no_jinja_filter_and_literal_numbers():
    # Cosmos 1.16.0 reads profiles.yml again, without dbt, to build the URIs of the assets that it
    # emits. Its readers know the filters of Jinja but not all those of dbt: the reader of the
    # local mode, that of openlineage-integration-common, knows only as_number of them. The reader
    # of the watcher mode knows the four of dbt since Cosmos 1.16.0
    # (astronomer/astronomer-cosmos#2950), and emits no asset when the type, the host or the port
    # keeps Jinja that it cannot render. So the profile keeps to one plain env_var call.
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


def test_the_packages_are_pinned_and_installed_where_the_image_puts_them():
    # The image installs the packages when it is built (ADR-0029): no task of dbt runs dbt deps,
    # so a task needs no network. Each package has an exact version.
    text = (DBT / "packages.yml").read_text(encoding="utf-8")
    pinned = dict(re.findall(r"^  - package: (\S+)\n    version: (\S+)$", text, re.MULTILINE))
    assert pinned == {"elementary-data/elementary": "0.26.1", "dbt-labs/dbt_utils": "1.4.1"}
    assert not (DBT / "package-lock.yml").exists(), "the image resolves the exact versions itself"
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    assert "\npackages-install-path: /opt/airflow/dbt_packages\n" in project


def test_the_views_of_ops_leave_out_the_rows_and_the_sql_that_elementary_keeps():
    # metabase_reader reads these views: no rows that a test found, no SQL of a model or a test,
    # and no variables of a run, which the tables of elementary keep.
    kept = ("result_rows", "test_results_query", "compiled_code", "test_params", "vars")
    for path in sorted((DBT / "models" / "ops").glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        found = [word for word in kept if re.search(rf"\b{word}\b", text)]
        assert not found, (path.name, found)


def test_elementary_records_no_rows_of_data_and_writes_into_ops():
    # The tables of elementary go to ops, which metabase_reader may enter but whose tables it may
    # not read; a test records its status and the number of rows, never the rows; the artifacts of
    # the project are written by the task of elementary, not by every task of dbt.
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    assert "\n  elementary:\n    +schema: ops\n" in project
    assert "\n  test_sample_row_count: 0\n" in project
    assert "\n  disable_dbt_artifacts_autoupload: true\n" in project
    ops = (
        "\n    ops:\n      +schema: ops\n      +materialized: view\n"
        "      +grants:\n        select: [metabase_reader]\n"
    )
    assert ops in project
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


# An item of a list of YAML in block style: its indentation, its first line after the dash, and the
# lines below it that are deeper than the dash. The lookahead also finds the items inside an item.
LIST_ITEM = re.compile(r"^(?=( *)- (.*)\n((?:\1  .*\n)*))", flags=re.MULTILINE)


def list_items(text: str) -> list[dict[str, str]]:
    """The keys of every item of a list in a YAML file, with their values, in any order: those on
    the line of the dash and those two spaces deeper, not the keys of the items nested in it."""
    items = []
    for match in LIST_ITEM.finditer(text):
        depth = len(match.group(1)) + 2
        lines = [match.group(2), *(line[depth:] for line in match.group(3).splitlines())]
        items.append(
            {
                key.strip(): value.strip().strip("'\"")
                for key, _, value in (line.partition(":") for line in lines)
                if key and not key.startswith((" ", "#"))
            }
        )
    return items


def test_a_key_of_a_contract_has_no_name():
    # dbt builds a table under a temporary name and renames it, and the index of a key keeps the
    # name that it got: a named key collides with itself on the next run ("relation ... already
    # exists"), while PostgreSQL gives an unnamed one a free name each time.
    keys = [
        (path.name, item)
        for path in yaml_files()
        for item in list_items(path.read_text(encoding="utf-8"))
        if item.get("type") == "primary_key"
    ]
    assert keys, "no model declares a primary key"
    named = [name for name, item in keys if "name" in item]
    assert not named, f"a primary key has a name in {named}"


def test_a_description_over_several_lines_ends_without_a_line_break():
    # A folded description, >, ends with a line break, which the comment in the warehouse keeps;
    # >- drops it. A literal block, |, keeps every line break.
    for path in yaml_files():
        text = path.read_text(encoding="utf-8")
        found = re.findall(r"^ *description: *([>|][-+0-9]*)$", text, flags=re.MULTILINE)
        assert set(found) <= {">-"}, f"{path.name}: write descriptions as >-, not {found}"


# The key of each mart (ADR-0025), which its contract makes a primary key of PostgreSQL.
MART_KEYS = {
    "ad_stats_campaign_daily": "[report_date, platform, account_id, campaign_id]",
    "ad_conversions_campaign_daily": (
        "[report_date, platform, account_id, campaign_id, conversion_action_id]"
    ),
    "web_sessions_daily": (
        "[report_date, property_id, session_source, session_medium, session_campaign_name, "
        "session_campaign_id]"
    ),
    "ad_platforms": "[platform]",
    "ad_accounts": "[platform, account_id]",
    "clients": "[client_id]",
    "ad_campaigns": "[platform, account_id, campaign_id]",
    "ad_campaign_names": "[platform, account_id, campaign_id, valid_from]",
    "fx_rates_daily": "[rate_date, currency, reporting_currency]",
    "ad_stats_ad_group_daily": "[report_date, platform, account_id, ad_group_id]",
    "ad_stats_ad_daily": "[report_date, platform, account_id, ad_group_id, ad_id]",
    "ad_groups": "[platform, account_id, ad_group_id]",
    "ads": "[platform, account_id, ad_group_id, ad_id]",
}


# The semantic types that Metabase 0.64.1 gives a field by its name, each time that it analyzes the
# field, over any type but a key (src/metabase/analyze/classifiers/name.clj, read on 2026-10-20):
# the pattern of the name in lower case, the kinds of base type that it applies to, and the type.
# The first entry that a field matches counts. The entries for times of day are left out: no mart
# has one. The pattern of deletion is written dele(?:te|ti): it matches the same names as that of
# Metabase, which the spell checker would take for a misspelled word.
METABASE_NAME_TYPES = (
    (r"^id$", "any", "type/PK"),
    (r"^lon$", "float", "type/Longitude"),
    (r"^.*_lon$", "float", "type/Longitude"),
    (r"^.*_lng$", "float", "type/Longitude"),
    (r"^.*_long$", "float", "type/Longitude"),
    (r"^.*_longitude$", "float", "type/Longitude"),
    (r"^lng$", "float", "type/Longitude"),
    (r"^long$", "float", "type/Longitude"),
    (r"^longitude$", "float", "type/Longitude"),
    (r"^lat$", "float", "type/Latitude"),
    (r"^.*_lat$", "float", "type/Latitude"),
    (r"^latitude$", "float", "type/Latitude"),
    (r"^.*_latitude$", "float", "type/Latitude"),
    (r"^.*_type$", "text", "type/Category"),
    (r"^.*_url$", "text", "type/URL"),
    (r"^city$", "text", "type/City"),
    (r"^country", "text", "type/Country"),
    (r"_country$", "text", "type/Country"),
    (r"^currency$", "text", "type/Category"),
    (r"^first(?:_?)name$", "text", "type/Name"),
    (r"^full(?:_?)name$", "text", "type/Name"),
    (r"^gender$", "text", "type/Category"),
    (r"^last(?:_?)name$", "text", "type/Name"),
    (r"^name$", "text", "type/Name"),
    (r"^postal(?:_?)code$", "int-or-text", "type/ZipCode"),
    (r"^role$", "text", "type/Category"),
    (r"^sex$", "text", "type/Category"),
    (r"^status$", "text", "type/Category"),
    (r"^type$", "text", "type/Category"),
    (r"^url$", "text", "type/URL"),
    (r"^zip(?:_?)code$", "int-or-text", "type/ZipCode"),
    (r"discount", "number", "type/Discount"),
    (r"income", "number", "type/Income"),
    (r"quantity", "int", "type/Quantity"),
    (r"count$", "int", "type/Quantity"),
    (r"number", "int", "type/Quantity"),
    (r"^num_", "int", "type/Quantity"),
    (r"join", "date", "type/JoinDate"),
    (r"join", "timestamp", "type/JoinTimestamp"),
    (r"create", "date", "type/CreationDate"),
    (r"create", "timestamp", "type/CreationTimestamp"),
    (r"start", "date", "type/CreationDate"),
    (r"start", "timestamp", "type/CreationTimestamp"),
    (r"cancel", "date", "type/CancelationDate"),
    (r"cancel", "timestamp", "type/CancelationTimestamp"),
    (r"dele(?:te|ti)", "date", "type/DeletionDate"),
    (r"dele(?:te|ti)", "timestamp", "type/DeletionTimestamp"),
    (r"update", "date", "type/UpdatedDate"),
    (r"update", "timestamp", "type/UpdatedTimestamp"),
    (r"source", "int-or-text", "type/Source"),
    (r"channel", "int-or-text", "type/Source"),
    (r"share", "float", "type/Share"),
    (r"percent", "float", "type/Share"),
    (r"rate$", "float", "type/Share"),
    (r"margin", "number", "type/GrossMargin"),
    (r"cost", "number", "type/Cost"),
    (r"duration", "number", "type/Duration"),
    (r"author", "int-or-text", "type/Author"),
    (r"creator", "int-or-text", "type/Author"),
    (r"created(?:_?)by", "int-or-text", "type/Author"),
    (r"owner", "int-or-text", "type/Owner"),
    (r"company", "int-or-text", "type/Company"),
    (r"vendor", "int-or-text", "type/Company"),
    (r"subscription", "int-or-text", "type/Subscription"),
    (r"score", "number", "type/Score"),
    (r"rating", "number", "type/Score"),
    (r"stars", "number", "type/Score"),
    (r"description", "text", "type/Description"),
    (r"title", "text", "type/Title"),
    (r"comment", "text", "type/Comment"),
    (r"birthda(?:te|y)", "date", "type/Birthdate"),
    (r"birthda(?:te|y)", "timestamp", "type/Birthdate"),
    (r"(?:te|y)(?:_?)of(?:_?)birth", "date", "type/Birthdate"),
    (r"(?:te|y)(?:_?)of(?:_?)birth", "timestamp", "type/Birthdate"),
)
# The kinds of base type of Metabase that the types of PostgreSQL of the marts are: numeric is
# type/Decimal, a float and a number; bigint is type/BigInteger, an integer and a number;
# timestamp and timestamptz are a date and time.
METABASE_KINDS = {
    "text": {"any", "text", "int-or-text"},
    "numeric": {"any", "float", "number"},
    "bigint": {"any", "int", "int-or-text", "number"},
    "date": {"any", "date"},
    "boolean": {"any"},
    "timestamp": {"any", "timestamp"},
    "timestamptz": {"any", "timestamp"},
}


def test_metabase_gives_no_column_of_a_mart_another_type_by_its_name():
    # A column whose name Metabase types gets that type in the code, or a name that it does not
    # type: otherwise every analysis in Metabase changes the type, and every start changes it back,
    # as fx_rates_daily.rate, a decimal ending in rate, became a percentage.
    text = (DBT / "models" / "marts" / "_marts__models.yml").read_text(encoding="utf-8")
    checked = 0
    for mart, block in blocks(text, 2).items():
        for column, lines in blocks(block, 6).items():
            data_type = re.search(r"^        data_type: (\w+)", lines, flags=re.MULTILINE)
            semantic = re.search(r"semantic_type: ([\w/]+)", lines)
            assert data_type, (mart, column)
            assert semantic, (mart, column)
            kinds = METABASE_KINDS[data_type.group(1)]
            named = next(
                (
                    given
                    for pattern, kind, given in METABASE_NAME_TYPES
                    if kind in kinds and re.search(pattern, column)
                ),
                None,
            )
            if named is not None:
                checked += 1
                assert semantic.group(1) == named, (mart, column, named)
    assert checked, "no column of the marts has a name that Metabase types"


# The columns of the marts that a filter offers as a list, each with a short list of values: the
# platform, the status, the state and the type of a thing, a currency, and other codes. A boolean
# is one too. The names are searched: ADR-0037.
LISTED_COLUMNS = {
    "ad_platform",
    "attribution_setting",
    "conversion_category",
    "currency",
    "kind",
    "mimics",
    "platform",
    "platform_name",
    "rate_source",
    "reporting_currency",
    "session_default_channel_group",
    "session_medium",
    "time_zone",
}
SEARCHED_COLUMNS = {"observed_name", "session_source"}


def field_values(data_type: str, column: str) -> str:
    """How a filter offers the values of a column of a mart (ADR-0037)."""
    if data_type == "boolean" or column in LISTED_COLUMNS or column.endswith(("_status", "_type")):
        return "list"
    if column in SEARCHED_COLUMNS or column.endswith("_name"):
        return "search"
    return "none"


def test_every_column_of_a_mart_declares_how_a_filter_offers_its_values():
    # Metabase makes nearly every field a list by itself and reads the whole table for each of them
    # in its daily scan: list for short lists, search for names, none for identifiers, dates and
    # measures.
    text = (DBT / "models" / "marts" / "_marts__models.yml").read_text(encoding="utf-8")
    found = {"list": 0, "search": 0, "none": 0}
    for mart, block in blocks(text, 2).items():
        for column, lines in blocks(block, 6).items():
            data_type = re.search(r"^        data_type: (\w+)", lines, flags=re.MULTILINE)
            declared = re.search(r"has_field_values: (\w+)", lines)
            assert declared, (mart, column)
            value = field_values(data_type.group(1), column)
            assert declared.group(1) == value, (mart, column)
            found[value] += 1
    assert all(found.values()), found


def test_every_mart_has_a_contract_and_its_key():
    text = (DBT / "models" / "marts" / "_marts__models.yml").read_text(encoding="utf-8")
    blocks = re.split(r"^  - name: ", text, flags=re.MULTILINE)[1:]
    found = {block.split("\n", 1)[0]: block for block in blocks}
    assert set(MART_KEYS) <= set(found)
    for name, key in MART_KEYS.items():
        contract = r"^      contract:\n        enforced: true$"
        assert re.search(contract, found[name], flags=re.MULTILINE), name
        assert f"      - type: primary_key\n        columns: {key}\n" in found[name], name
        # The constraint is checked only when dbt builds the table; the test checks the data.
        unique = f"      - unique_combination:\n          arguments:\n            columns: {key}\n"
        assert unique in found[name], name


# The foreign keys of the marts in Metabase (ADR-0034): the column of each name, in every mart but
# the one whose key it is, points at that key. Metabase joins a foreign key on its one column, so
# only a key of one column can be a target: an account, a campaign, an ad group and an ad are each
# a key only with its platform and its account, and their IDs have no foreign key.
FOREIGN_KEYS = {"client_id": "clients", "platform": "ad_platforms"}


def test_the_foreign_keys_of_the_marts_point_at_a_key_of_one_column():
    text = (DBT / "models" / "marts" / "_marts__models.yml").read_text(encoding="utf-8")
    found = 0
    for mart, block in blocks(text, 2).items():
        for column, lines in blocks(block, 6).items():
            semantic = re.search(r"semantic_type: ([\w/]+)", lines).group(1)
            target = re.search(r"fk_target_table: ([\w.]+)\n +fk_target_field: (\w+)\n", lines)
            home = FOREIGN_KEYS.get(column)
            if home is None or mart == home:
                assert "fk_target" not in lines, (mart, column)
            if home is None:
                assert semantic not in ("type/FK", "type/PK"), (mart, column)
            elif mart == home:
                assert (semantic, MART_KEYS[mart]) == ("type/PK", f"[{column}]")
            else:
                found += 1
                assert semantic == "type/FK", (mart, column)
                assert target is not None, (mart, column)
                assert target.groups() == (f"marts.{home}", column), (mart, column)
    assert found == 19


def seed_rows(name: str) -> list[dict[str, str]]:
    with (SEEDS / f"{name}.csv").open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def described_seed_columns(text: str) -> dict[str, set[str]]:
    """The seeds of _seeds.yml that have a description, each with its columns that have one."""
    found = {}
    for block in re.split(r"^(?=  - name: )", text, flags=re.MULTILINE):
        seed = re.match(r"  - name: (\w+)\n    description:", block)
        if seed:
            pattern = r"^      - name: (\w+)\n        description:"
            found[seed.group(1)] = set(re.findall(pattern, block, flags=re.MULTILINE))
    return found


def test_every_seed_has_a_description_and_its_columns_have_one():
    seeds = {path.stem for path in SEEDS.glob("*.csv")}
    assert seeds == {
        "seed_ad_platforms",
        "seed_conversion_actions",
        "seed_iso_4217_currencies",
        "seed_iana_time_zones",
    }
    described = described_seed_columns((SEEDS / "_seeds.yml").read_text(encoding="utf-8"))
    assert seeds <= described.keys(), sorted(seeds - described.keys())
    for seed in seeds:
        with (SEEDS / f"{seed}.csv").open(encoding="utf-8", newline="") as file:
            header = set(next(csv.reader(file)))
        assert header == described[seed], (seed, sorted(header ^ described[seed]))


def test_the_seeds_go_to_staging_and_comment_their_tables():
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    pattern = (
        r"^seeds:\n  agoranomos:\n(?:    #.*\n)*    \+schema: staging\n"
        r"    \+persist_docs:\n      relation: true\n      columns: true$"
    )
    assert re.search(pattern, project, flags=re.MULTILINE)
    assert 'seed-paths: ["seeds"]' in project


def test_the_intermediate_models_are_views_in_staging():
    # ADR-0025: until an AI role needs to tell them apart, intermediate models are views in staging,
    # but for the statistics of ad groups and of ads, whose many tests would each compute a view of
    # their size again (ADR-0034), and the rates of each day, which every unioned model of the facts
    # and five marts read; and the checks of G1 and the days of the windows of the facts, which
    # each run fills anew, as the days of the pin are (ADR-0035, ADR-0038).
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    pattern = r"^    intermediate:\n      \+schema: staging\n      \+materialized: view$"
    assert re.search(pattern, project, flags=re.MULTILINE)
    tables = {
        path.stem
        for path in (DBT / "models" / "intermediate").glob("*.sql")
        if "materialized=" in path.read_text(encoding="utf-8")
    }
    assert tables == {
        "int_ad_stats_ad_group__unioned",
        "int_ad_stats_ad__unioned",
        "int_fx_rates__filled",
        "int_g1_failures",
        "int_window_days",
    }
    for name in tables - {"int_g1_failures", "int_window_days"}:
        text = (DBT / "models" / "intermediate" / f"{name}.sql").read_text(encoding="utf-8")
        assert "{{ config(materialized='table') }}" in text, name


def test_the_reporting_currency_is_written_one_way_everywhere():
    # dbt renders env_var() in meta and in the arguments of tests, but not a project var whose
    # value holds Jinja (dbt 1.12.5): every YAML file writes the one expression.
    for path in [*(DBT / "models").rglob("*.yml"), DBT / "dbt_project.yml"]:
        text = path.read_text(encoding="utf-8")
        assert "var('reporting_currency" not in text, path.name
        for found in re.findall(r"\{\{[^}]*AGORANOMOS_REPORTING_CURRENCY[^}]*\}\}", text):
            assert found == REPORTING, (path.name, found)
    macro = (DBT / "macros" / "reporting_currency.sql").read_text(encoding="utf-8")
    assert "env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD')" in macro


def test_the_conversion_actions_of_the_seed_have_a_known_category_and_one_row_each():
    rows = seed_rows("seed_conversion_actions")
    keys = [(r["platform"], r["conversion_action_id"]) for r in rows]
    assert len(keys) == len(set(keys))
    categories = {"purchase", "lead", "sign_up", "add_to_cart", "app_install", "other"}
    assert {r["conversion_category"] for r in rows} <= categories
    assert {r["is_primary"] for r in rows} <= {"true", "false"}
    # Meta counts the same purchases twice, by the pixel and as omni_purchase: one of them counts.
    meta = {
        r["conversion_action_id"]: r["is_primary"] for r in rows if r["platform"] == "demo_social"
    }
    assert meta["offsite_conversion.fb_pixel_purchase"] == "true"
    assert meta["omni_purchase"] == "false"


def test_the_seed_of_platforms_names_every_demo_platform_and_what_it_mimics():
    rows = {r["platform"]: r for r in seed_rows("seed_ad_platforms")}
    from agoranomos_pipelines.demo import platforms

    # And the real sources, which mimic nothing: the rates of the ECB and Google Ads (ADR-0032).
    assert set(rows) == {*platforms.PLATFORMS, "ecb", "google_ads"}
    for code, platform in platforms.PLATFORMS.items():
        assert rows[code]["mimics"] == platform.mimics
        assert rows[code]["is_demo"] == "true"
    assert rows["ecb"] == {
        "platform": "ecb",
        "platform_name": "European Central Bank",
        "kind": "exchange_rates",
        "mimics": "",
        "is_demo": "false",
    }
    assert rows["google_ads"] == {
        "platform": "google_ads",
        "platform_name": "Google Ads",
        "kind": "ads",
        "mimics": "",
        "is_demo": "false",
    }


def test_the_seeds_of_currencies_and_time_zones_hold_what_the_demo_uses():
    currencies = {r["currency_code"] for r in seed_rows("seed_iso_4217_currencies")}
    zones = {r["time_zone"] for r in seed_rows("seed_iana_time_zones")}
    from agoranomos_pipelines.demo import world

    assert {a.currency for a in world.ACCOUNTS} <= currencies
    assert len(currencies) > 150
    assert {a.time_zone for a in world.ACCOUNTS} <= zones
    assert "Etc/GMT+8" in zones
    assert len(zones) > 400


STAGING = DBT / "models" / "staging"
DAGS_PIPELINES = DBT.parent / "airflow" / "dags" / "agoranomos_pipelines"


def meta_insights(level: str) -> list[str]:
    """The insights of Meta at a level, with their child tables of actions."""
    return [f"insights_{level}", f"insights_{level}__actions", f"insights_{level}__action_values"]


# The staging models of the platforms of the demo (ADR-0025), by platform: each is one call of the
# macro of the shape of its API, over these raw tables of the platform, in this order, with the
# level of a report of statistics or conversions where it is not that of campaigns (ADR-0034).
SHAPED = {
    "demo_search": {
        "campaign_stats": ("google_ads_stats", ["campaign_stats"]),
        "campaign_conversions": ("google_ads_conversions", ["campaign_conversions"]),
        "accounts": ("google_ads_accounts", ["customer_clients"]),
        "campaigns": ("google_ads_campaigns", ["campaigns"]),
        "ad_groups": ("google_ads_ad_groups", ["ad_groups"]),
        "ad_group_stats": ("google_ads_stats", ["ad_group_stats"], "ad_group"),
        "ad_group_conversions": ("google_ads_conversions", ["ad_group_conversions"], "ad_group"),
        "ads": ("google_ads_ads", ["ads"]),
        "ad_stats": ("google_ads_stats", ["ad_stats"], "ad"),
        "ad_conversions": ("google_ads_conversions", ["ad_conversions"], "ad"),
    },
    "demo_social": {
        "campaign_stats": ("meta_stats", ["insights_campaign"]),
        "campaign_conversions": ("meta_conversions", meta_insights("campaign")),
        "accounts": ("meta_accounts", ["ad_accounts"]),
        "campaigns": ("meta_campaigns", ["campaigns"]),
        "ad_groups": ("meta_ad_groups", ["adsets"]),
        "ad_group_stats": ("meta_stats", ["insights_adset"], "ad_group"),
        "ad_group_conversions": ("meta_conversions", meta_insights("adset"), "ad_group"),
        "ads": ("meta_ads", ["ads"]),
        "ad_stats": ("meta_stats", ["insights_ad"], "ad"),
        "ad_conversions": ("meta_conversions", meta_insights("ad"), "ad"),
    },
    "demo_video": {
        "campaign_stats": ("tiktok_stats", ["report_campaign_daily"]),
        "campaign_conversions": ("tiktok_conversions", ["report_campaign_daily"]),
        "accounts": ("tiktok_accounts", ["advertisers"]),
        "campaigns": ("tiktok_campaigns", ["campaigns"]),
        "ad_groups": ("tiktok_ad_groups", ["adgroups"]),
        "ad_group_stats": ("tiktok_stats", ["report_adgroup_daily"], "ad_group"),
        "ad_group_conversions": ("tiktok_conversions", ["report_adgroup_daily"], "ad_group"),
        "ads": ("tiktok_ads", ["ads"]),
        "ad_stats": ("tiktok_stats", ["report_ad_daily"], "ad"),
        "ad_conversions": ("tiktok_conversions", ["report_ad_daily"], "ad"),
    },
    "demo_web": {
        "properties": ("ga4_properties", ["properties"]),
        "sessions_daily": ("ga4_sessions_daily", ["report_sessions_daily"]),
    },
    "demo_fx": {"rates_daily": ("euro_rates", ["rates_daily"])},
}
STATS_KEY = ["report_date", "platform", "account_id", "campaign_id"]
ACCOUNT_KEY = ["platform", "account_id"]
AD_GROUP_KEY = ["report_date", "platform", "account_id", "ad_group_id"]
AD_KEY = [*AD_GROUP_KEY, "ad_id"]
METRICS = ["impressions", "clicks", "spend"]
CONVERTED = ["conversions", "conversion_value"]
# What the checks of int_g1_failures check for each kind of staging model of a platform, for each
# account, as the tests of staging checked it for all accounts at once before (ADR-0035): its key,
# the columns that must be there, and those that are zero or more; and, for the accounts and the
# properties, the currency and the time zone, against the seeds (IN_SEEDS).
CHECKED = {
    "campaign_stats": (
        STATS_KEY,
        [*STATS_KEY, "impressions", "clicks", "spend"],
        ["impressions", "clicks", "spend"],
    ),
    "campaign_conversions": (
        [*STATS_KEY, "conversion_action_id"],
        [*STATS_KEY, "conversion_action_id", "conversions", "conversion_value"],
        ["conversions", "conversion_value"],
    ),
    "accounts": (ACCOUNT_KEY, [*ACCOUNT_KEY, "currency", "time_zone"], []),
    "campaigns": (
        [*ACCOUNT_KEY, "campaign_id", "valid_from"],
        [*ACCOUNT_KEY, "campaign_id", "valid_from", "observed_name"],
        [],
    ),
    "ad_group_stats": (AD_GROUP_KEY, [*AD_GROUP_KEY, "campaign_id", *METRICS], METRICS),
    "ad_group_conversions": (
        [*AD_GROUP_KEY, "conversion_action_id"],
        [*AD_GROUP_KEY, "campaign_id", "conversion_action_id", *CONVERTED],
        CONVERTED,
    ),
    "ad_stats": (AD_KEY, [*AD_KEY, "campaign_id", *METRICS], METRICS),
    "ad_conversions": (
        [*AD_KEY, "conversion_action_id"],
        [*AD_KEY, "campaign_id", "conversion_action_id", *CONVERTED],
        CONVERTED,
    ),
    "ad_groups": (
        [*ACCOUNT_KEY, "ad_group_id", "valid_from"],
        [*ACCOUNT_KEY, "campaign_id", "ad_group_id", "valid_from", "observed_name"],
        [],
    ),
    "ads": (
        [*ACCOUNT_KEY, "ad_group_id", "ad_id", "valid_from"],
        [*ACCOUNT_KEY, "campaign_id", "ad_group_id", "ad_id", "valid_from"],
        [],
    ),
    "properties": (
        ["platform", "property_id"],
        ["platform", "property_id", "currency", "time_zone"],
        [],
    ),
    "sessions_daily": (
        [
            "report_date",
            "property_id",
            "session_source",
            "session_medium",
            "session_campaign_name",
            "session_campaign_id",
        ],
        [
            "report_date",
            "property_id",
            "session_source",
            "session_medium",
            "session_campaign_name",
            "session_campaign_id",
            "sessions",
        ],
        ["sessions", "engaged_sessions", "key_events", "total_revenue"],
    ),
    "rates_daily": (
        ["rate_date", "base_currency", "quote_currency"],
        ["rate_date", "base_currency", "quote_currency", "rate", "rate_source"],
        ["rate"],
    ),
}
# The kinds whose data G1 checks for each account: the facts and the lists of accounts and of
# properties. The lists of the entities, whose versions dlt keeps, and the rates, which name no
# account, keep their tests in staging.
BY_ACCOUNT = {
    "campaign_stats",
    "campaign_conversions",
    "ad_group_stats",
    "ad_group_conversions",
    "ad_stats",
    "ad_conversions",
    "sessions_daily",
    "accounts",
    "properties",
}
# The columns of a row that no account can get wrong alone, which the tests of staging still
# check for all accounts: the day, the platform and the account of a fact, which the join of
# newest_load guarantees, and the platform and the ID of an account or a property, without which
# no check can name the account.
NAMED_BY_THE_LOAD = {
    "sessions_daily": ["report_date", "property_id"],
    "accounts": ACCOUNT_KEY,
    "properties": ["platform", "property_id"],
}
# The critical tests of each kind of staging model: its key, which unique_combination checks, or
# none, the columns that not_null checks, and those that non_negative checks.
CRITICAL = {
    kind: (None, NAMED_BY_THE_LOAD.get(kind, STATS_KEY[:3]), []) if kind in BY_ACCOUNT else checked
    for kind, checked in CHECKED.items()
}
# The key test of a model, with its columns in flow style.
UNIQUE = r"^      - unique_combination:\n          arguments:\n            columns: \[(.*)\]$"
# The columns of an account or a property that must be in a seed: the seed and its column, which
# the checks of int_g1_failures read.
IN_SEEDS = {
    "currency": ("seed_iso_4217_currencies", "currency_code"),
    "time_zone": ("seed_iana_time_zones", "time_zone"),
}


def blocks(text: str, indent: int) -> dict[str, str]:
    """The items of a list of YAML with a name, by their name: those whose dash is at indent."""
    found = {}
    for block in re.split(rf"^(?= {{{indent}}}- name: )", text, flags=re.MULTILINE):
        item = re.match(rf" {{{indent}}}- name: (\w+)\n", block)
        if item:
            found[item.group(1)] = block
    return found


def column_tests(block: str) -> set[str]:
    """The names of the tests of a column: a list on the line of data_tests, or a list below it."""
    tests = set(re.findall(r"^          - (\w+)", block, flags=re.MULTILINE))
    flow = re.search(r"^        data_tests: \[(.*)\]$", block, flags=re.MULTILINE)
    if flow:
        tests |= {name.strip() for name in flow.group(1).split(",")}
    return tests


# The kinds of staging models that read a fact table of raw, whose loads each append their rows:
# each reads its table through newest_load, the rows of the newest whole load of each account and
# day (ADR-0038), and the other kinds read their tables as they are.
BY_LOAD = {
    "campaign_stats",
    "campaign_conversions",
    "ad_group_stats",
    "ad_group_conversions",
    "ad_stats",
    "ad_conversions",
    "sessions_daily",
    "rates_daily",
}


def test_each_staging_model_of_a_platform_is_one_call_of_the_shape_of_its_api():
    # With the code of the platform of its folder and the raw tables of its own dataset: a model
    # that passed the code of another platform would still pass its key tests. A model of a fact
    # reads the table of the fact by load, and the child tables of the actions of Meta follow their
    # rows. No model names a seed: the checks of int_g1_failures read the currencies and the time
    # zones, which the tests of the accounts and the properties read before.
    from agoranomos_pipelines.demo import platforms

    assert set(SHAPED) == set(platforms.PLATFORMS)
    found = {path.relative_to(STAGING).as_posix() for path in STAGING.glob("demo_*/*.sql")}
    wanted = {f"{code}/stg_{code}__{model}.sql" for code in SHAPED for model in SHAPED[code]}
    assert found == wanted, sorted(found ^ wanted)
    for code, models in SHAPED.items():
        dataset = platforms.PLATFORMS[code].dataset
        for model, (macro, tables, *level) in models.items():
            path = STAGING / code / f"stg_{code}__{model}.sql"
            comment, *body = path.read_text(encoding="utf-8").splitlines()
            relations = [f"demo_source('{dataset}', '{table}')" for table in tables]
            if model in BY_LOAD:
                facts = {fact.table for fact in platforms.PLATFORMS[code].facts}
                assert tables[0] in facts, (code, model)
                days = "ref('stg_loads__newest_days')"
                relations[0] = f"newest_load({relations[0]}, '{dataset}', {days})"
            relations = ", ".join(relations)
            arguments = ", ".join([relations, f"'{code}'", *(f"'{name}'" for name in level)])
            assert comment.startswith(f"-- {path.name}: "), path.name
            assert body == [f"{{{{ {macro}({arguments}) }}}}"], path.name


def test_staging_reads_the_newest_pinned_load_of_each_account_and_day():
    # The rule of ADR-0038 and ADR-0046, and the findings of the checks of November 2026 (#188,
    # #191): only the loads that a run pinned, only the accounts that loaded, the newest load as a
    # number among the pinned and the accepted, rows of the pinned only, and of the accepted only
    # those newer than the oldest pinned load of the account, which alone can win a day.
    text = (DBT / "macros" / "newest_load.sql").read_text(encoding="utf-8")
    pinned = text[text.index("{% macro pinned_loads(") :]
    pinned = pinned[: pinned.index("{%- endmacro %}")]
    assert "where p.source = '{{ source }}' and p.status = 'pinned' and p.outcome = 'loaded'" in (
        pinned
    )
    assert "where a.source = '{{ source }}' and a.status = 'accepted' and a.outcome = 'loaded'" in (
        pinned
    )
    assert "on oldest.account_id = a.account_id and a.load_id::numeric > oldest.load_number" in (
        pinned
    )
    assert "min(load_id::numeric) as load_number" in pinned
    assert "select distinct on (l.account_id, w.day)" in pinned
    assert "order by l.account_id, w.day, l.load_id::numeric desc" in pinned
    assert pinned.rstrip().endswith("where d.status = 'pinned'")
    # The pin is the one thing that reads the raw journals and _dlt_loads to choose loads.
    assert "_dlt_loads" not in pinned
    assert "_agoranomos_loads" not in pinned
    # The days are a table of each run, with the statistics of PostgreSQL, joined by one key.
    assert "    from {{ days }}\n" in text
    assert "    where source = '{{ source_name[4:] }}'\n) as d" in text
    # Before a run has filled the table, its rows as a query, as after an update that brings it.
    assert "{%- if execute and load_relation(days) is none %}" in text
    assert "    from ({{ pinned_loads(source_name, pinned_loads_journal()) }}) as days" in text
    assert "on d.load_key = {{ newest_load_key(source_name, 'r._dlt_load_id', account, day) }}" in (
        text
    )
    assert (
        "api.Relation.create(database=target.database, schema='core', identifier='loads')" in text
    )
    model = (STAGING / "loads" / "stg_loads__newest_days.sql").read_text(encoding="utf-8")
    assert "materialized='incremental'" in model
    assert 'pre_hook=["{% if is_incremental() %}delete from {{ this }}{% endif %}"]' in model
    assert 'post_hook=["analyze {{ this }}"]' in model
    assert "{%- for source_name in newest_load_sources() %}" in model
    assert "{{ pinned_loads(source_name, source('core', 'loads')) }}" in model
    sources = (STAGING / "loads" / "_loads__sources.yml").read_text(encoding="utf-8")
    assert "  - name: core\n" in sources
    assert "    schema: core\n" in sources
    assert "      - name: loads\n" in sources


PIN = DBT / "macros" / "pin_loads.sql"
MIGRATIONS = DBT.parent / "postgres" / "migrations"


def test_the_pin_copies_each_whole_load_once_and_skips_a_source_without_tables():
    # ADR-0046 and the check of #188: one insert ... select for each source, the journal joined
    # with _dlt_loads on the load and status 0, on conflict do nothing, a source whose tables do
    # not exist passed over, all of it in one transaction that commits, as dbt run-operation rolls
    # back what it does not commit (dbt-adapters 1.24.5, BaseConnectionManager.close).
    text = PIN.read_text(encoding="utf-8")
    assert "{%- for source_name in newest_load_sources() -%}" in text
    for table in ("_agoranomos_loads", "_dlt_loads"):
        assert (
            "adapter.get_relation(\n"
            f"            database=target.database, schema=source_name, identifier='{table}'\n"
        ) in text, table
    assert "{%- if journal is not none and finished is not none -%}" in text
    assert "insert into {{ loads }} (\n" in text
    assert (
        "    source, load_id, account_id, window_from, window_to, outcome, error, finished_at, "
        "invocation_id\n"
    ) in text
    assert "    l.inserted_at, '{{ invocation_id }}'::uuid\n" in text
    assert "join {{ finished }} as l\n    on l.load_id = j._dlt_load_id and l.status = 0\n" in text
    assert "on conflict (source, load_id, account_id) do nothing" in text
    assert "{%- set loads = pinned_loads_journal() -%}" in text
    # The rows of the pin and the months of the pinned windows, then one commit; then each
    # partition in a transaction of its own.
    body = text[text.index("{% macro pin_loads() %}") : text.index("{% endmacro %}")]
    assert body.count("{%- do adapter.commit() -%}") == 2
    assert body.index("statement('pin'") < body.index("statement('months'")
    assert body.index("statement('months'") < body.index("{%- do adapter.commit() -%}")
    assert "select core.ensure_partition('core.{{ fact }}'::regclass, date '{{ month }}')" in body
    assert "where l.status = 'pinned' and l.outcome = 'loaded'" in body
    # No model creates a partition, as a hook before_begin breaks the transaction of a model
    # (#191).
    for path in (DBT / "models").rglob("*"):
        if path.is_file():
            assert "ensure_partition" not in path.read_text(encoding="utf-8"), path.name


def test_the_pin_makes_the_partitions_of_the_facts_that_each_source_is_to_write():
    # The sources of the pin are those whose fact tables staging reads by load; the ad platforms
    # write the four facts of ads, the web analytics the fact of sessions, and the rates a table of
    # the core without partitions (ADR-0036). Every fact of the migrations is in the list.
    text = PIN.read_text(encoding="utf-8")
    macro = text[text.index("{% macro pinned_load_facts() -%}") :]
    ads = re.search(r"\{%- set ads = \[(.*?)\] -%\}", macro, flags=re.DOTALL).group(1)
    ad_facts = re.findall(r"'(\w+)'", ads)
    facts = {
        source: ad_facts if listed == "ads" else re.findall(r"'(\w+)'", listed)
        for source, listed in re.findall(r"^    '(raw_\w+)': (ads|\[.*\]),$", macro, re.MULTILINE)
    }
    newest = (DBT / "macros" / "newest_load.sql").read_text(encoding="utf-8")
    sources = set(re.findall(r"^    '(raw_\w+)': \(", newest, flags=re.MULTILINE))
    assert set(facts) == sources
    migration = (MIGRATIONS / "0005_create_the_facts.sql").read_text(encoding="utf-8")
    partitioned = set(
        re.findall(
            r"^CREATE TABLE core\.(\w+) \(\n(?:.*\n)*?\) PARTITION BY RANGE \(report_date\);$",
            migration,
            flags=re.MULTILINE,
        )
    )
    assert len(partitioned) == 5, sorted(partitioned)
    assert {fact for listed in facts.values() for fact in listed} == partitioned
    assert facts["raw_demo_web"] == ["web_sessions_daily"]
    assert facts["raw_demo_fx"] == facts["raw_ecb"] == []
    for source in ("raw_demo_search", "raw_demo_social", "raw_demo_video", "raw_google_ads"):
        assert sorted(facts[source]) == sorted(partitioned - {"web_sessions_daily"}), source
    rates = (MIGRATIONS / "0007_create_the_rates.sql").read_text(encoding="utf-8")
    assert "PARTITION BY" not in rates


def test_newest_load_names_the_account_and_the_day_of_each_source_as_its_load_does():
    # For the demo, as the fact tables of platforms.py hold them, through which each load records
    # its windows; Google Ads has the shape of demo_search, and the rates of the ECB, one account.
    from agoranomos_pipelines.demo import platforms

    text = (DBT / "macros" / "newest_load.sql").read_text(encoding="utf-8")
    keys = {
        source: ast.literal_eval(f"({pair.replace('(none,', '(None,')})")
        for source, pair in re.findall(r"^    '(raw_\w+)': (\(.*\)),$", text, flags=re.MULTILINE)
    }
    assert set(keys) == {
        *(platform.dataset for platform in platforms.PLATFORMS.values()),
        "raw_ecb",
        "raw_google_ads",
    }

    def plain(sql):
        return sql.replace("r.", "").replace('"', "") if sql is not None else None

    for platform in platforms.PLATFORMS.values():
        account, day = (plain(sql) for sql in keys[platform.dataset])
        for fact in platform.facts:
            assert day == plain(fact.day_date_sql), (platform.code, fact.table)
            if fact.account_sql is None:
                assert account is None, platform.code
            elif fact.account_text == "{}":
                assert account == plain(fact.account_sql), platform.code
            else:
                # An ID written with a prefix, as properties/{} of GA4.
                prefix = fact.account_text.removesuffix("{}")
                assert prefix.count("/") == 1, platform.code
                assert account == f"split_part({plain(fact.account_sql)}, '/', 2)", platform.code
    assert keys["raw_google_ads"] == keys["raw_demo_search"]
    assert keys["raw_ecb"] == (None, "cast(r.time_period as date)")


def test_every_table_of_the_demo_can_be_missing():
    # A deployment without the profile demo never loads these tables, and its model must build all
    # the same: each goes through demo_source, which needs the columns of the table.
    named = set(
        re.findall(
            r"^    '(raw_demo_\w+\.\w+)': \{$",
            (DBT / "macros" / "demo_source.sql").read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
    )
    used = set()
    for path in (DBT / "models").rglob("*.sql"):
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"(?<![_a-z])source\('raw_demo", text), path.name
        used |= {
            f"{schema}.{table}"
            for schema, table in re.findall(r"demo_source\('(raw_demo_\w+)', '(\w+)'\)", text)
        }
    assert used == named


def test_the_sources_of_a_platform_are_the_tables_that_its_load_writes():
    # dlt writes a table for each table of the shape of the platform, and a child table for each
    # list in it, such as insights_campaign__actions of Meta.
    from agoranomos_pipelines.demo import platforms, shapes

    for code, platform in platforms.PLATFORMS.items():
        # And the journal of the loads (sources.py), which staging and the Ops collection read,
        # and the loads of dlt, whose whole loads staging reads.
        written = {"_agoranomos_loads", "_dlt_loads"}
        for (owner, table), shape in shapes.SHAPES.items():
            if owner == code:
                lists = [field for field, kind in shape.items() if isinstance(kind, list)]
                written |= {table, *(f"{table}__{field}" for field in lists)}
        text = (STAGING / code / f"_{code}__sources.yml").read_text(encoding="utf-8")
        [source] = re.findall(r"^  - name: (\w+)\n    description:", text, flags=re.MULTILINE)
        assert source == platform.dataset
        assert re.search(rf"^    schema: {platform.dataset}$", text, flags=re.MULTILINE), code
        pattern = r"^      - name: (\w+)\n        description:"
        declared = set(re.findall(pattern, text, flags=re.MULTILINE))
        assert declared == written, (code, sorted(declared ^ written))


def test_the_staging_models_of_the_platforms_have_their_critical_tests():
    # Each test is an error, so a row that breaks one stops the models that read the model
    # (TestBehavior.BUILD of Cosmos): no test of staging is a warning. A test of the data of one
    # account would stop every account with it, so G1 checks those for each account instead
    # (ADR-0035), and staging keeps the tests that no account fails alone.
    for code, models in SHAPED.items():
        text = (STAGING / code / f"_{code}__models.yml").read_text(encoding="utf-8")
        assert "severity" not in text, code
        assert "relationships" not in text, code
        described = blocks(text, 2)
        assert set(described) == {f"stg_{code}__{model}" for model in models}, code
        for model in models:
            key, not_null, non_negative = CRITICAL[model]
            block = described[f"stg_{code}__{model}"]
            unique = re.findall(UNIQUE, block, flags=re.MULTILINE)
            assert [part.split(", ") for part in unique] == ([key] if key else []), (code, model)
            columns = blocks(block, 6)
            tested = {name: column_tests(column) for name, column in columns.items()}
            assert {c for c, tests in tested.items() if "not_null" in tests} == set(not_null), (
                code,
                model,
            )
            assert {c for c, tests in tested.items() if "non_negative" in tests} == set(
                non_negative
            ), (code, model)
            # Its description says which checks hold its accounts out.
            assert ("int_g1_failures checks" in folded(block, 4)) is (model in BY_ACCOUNT), model


def test_every_column_of_the_staging_of_the_platforms_takes_the_doc_block_of_its_name():
    # The staging models share their columns with the intermediate models and the marts, where
    # columns of one name say the same.
    docs = (DBT / "models" / "_docs.md").read_text(encoding="utf-8")
    written = set(re.findall(r"^\{% docs (\w+) %\}$", docs, flags=re.MULTILINE))
    for code in SHAPED:
        text = (STAGING / code / f"_{code}__models.yml").read_text(encoding="utf-8")
        for model, block in blocks(text, 2).items():
            columns = blocks(block, 6)
            assert columns, model
            for name, column in columns.items():
                description = re.search(r"^        description: (.*)$", column, flags=re.MULTILINE)
                assert description, (model, name)
                assert description.group(1) == f"'{{{{ doc(\"{name}\") }}}}'", (model, name)
                assert name in written, (model, name)


# A ref or a source in SQL or YAML, outside the comments of Jinja, which dbt does not render; dbt
# also reads one in a comment of SQL, such as -- depends_on: {{ ref('x') }}.
REF = re.compile(r"\bref\(\s*'(\w+)'\s*\)")
SOURCE = re.compile(r"\bsource\(\s*'(\w+)'\s*,\s*'(\w+)'\s*\)")
JINJA_COMMENT = re.compile(r"\{#.*?#\}", flags=re.DOTALL)


def referenced(text: str) -> list[str]:
    """The nodes that a text refs, by name, and the sources that it reads, as source:<name>.<table>,
    in their order."""
    text = JINJA_COMMENT.sub("", text)
    found = [(m.start(), m.group(1)) for m in REF.finditer(text)]
    found += [(m.start(), f"source:{m.group(1)}.{m.group(2)}") for m in SOURCE.finditer(text)]
    return [name for _, name in sorted(found)]


def ancestors(node: str, parents: dict[str, set[str]]) -> set[str]:
    """Every node that a node depends on, at any depth."""
    found: set[str] = set()
    todo = [node]
    while todo:
        for parent in parents.get(todo.pop(), set()) - found:
            found.add(parent)
            todo.append(parent)
    return found


def read_together() -> list[tuple[str, set[str]]]:
    """Each test that reads more than one node, with the nodes that it reads: a singular test of
    dbt/tests, by its file, or a generic test of a YAML file, by the node that it is on and the ref
    or source in its arguments."""
    found = []
    for path in sorted((DBT / "tests").glob("*.sql")):
        nodes = set(referenced(path.read_text(encoding="utf-8")))
        if len(nodes) > 1:
            found.append((path.name, nodes))
    for path in sorted([*(DBT / "models").rglob("*.yml"), *SEEDS.glob("*.yml")]):
        text = path.read_text(encoding="utf-8")
        if not text.startswith(("models:", "seeds:")):
            assert not referenced(text), f"{path.name}: a test outside models or seeds"
            continue
        for node, block in blocks(text, 2).items():
            found += [(f"{path.name}: {node}", {node, other}) for other in referenced(block)]
    return found


def test_a_test_of_several_nodes_is_on_one_that_depends_on_the_others():
    # The transform selects the tests of each task as buildable: a test that reads more than one
    # node runs in the test task of the node that depends on all the others, after them, and its
    # failure stops the models below that node. A test of nodes none of which depends on the others
    # runs in no task, so one of them names the others: a ref, or a -- depends_on: line.
    sql = [*(DBT / "macros").rglob("*.sql"), *(DBT / "tests" / "generic").glob("*.sql")]
    hidden = [path.name for path in sql if referenced(path.read_text(encoding="utf-8"))]
    assert not hidden, f"a ref in a macro or a generic test is not seen here: {hidden}"
    parents = {
        path.stem: set(referenced(path.read_text(encoding="utf-8")))
        for path in (DBT / "models").rglob("*.sql")
    }
    checked = read_together()
    assert len(checked) >= 8, "the tests of the intermediate models against each other"
    for where, nodes in checked:
        assert any(nodes - {node} <= ancestors(node, parents) for node in nodes), (
            where,
            sorted(nodes),
        )


INTERMEDIATE = DBT / "models" / "intermediate"
# The relationships of columns of an intermediate model to the columns of another model, which
# relationships_composite checks: the columns, the other model, and its columns.
TO_ACCOUNTS = (["platform", "account_id"], "int_ad_accounts__unioned", ["platform", "account_id"])
TO_CAMPAIGNS = (
    ["platform", "account_id", "campaign_id"],
    "int_ad_campaigns__unioned",
    ["platform", "account_id", "campaign_id"],
)
TO_AD_GROUPS = (
    ["platform", "account_id", "ad_group_id"],
    "int_ad_groups__unioned",
    ["platform", "account_id", "ad_group_id"],
)
TO_ADS = (
    ["platform", "account_id", "ad_group_id", "ad_id"],
    "int_ads__unioned",
    ["platform", "account_id", "ad_group_id", "ad_id"],
)
TO_RATES = (["report_date", "currency"], "int_fx_rates__filled", ["rate_date", "currency"])
# The columns of the statistics of an ad group or an ad that must not be null, after their key.
NAMED = ["currency", "client_id", "campaign_id", "campaign_name", "ad_group_name"]
SESSIONS_KEY = CHECKED["sessions_daily"][0]
# The critical tests of each intermediate model, every one an error: its key, which
# unique_combination checks, or unique for a key of one column; the columns that not_null checks;
# the relationships of a column to a column of another model; and those of columns to columns.
INTERMEDIATE_TESTS = {
    "int_ad_platforms__unioned": (["platform"], ["platform"], {}, []),
    "int_clients__resolved": (["client_id"], ["client_id", "client_name"], {}, []),
    "int_ad_accounts__unioned": (
        ACCOUNT_KEY,
        ["client_id", "currency", "time_zone"],
        {"platform": ("int_ad_platforms__unioned", "platform")},
        [],
    ),
    "int_ad_campaigns__unioned": ([*ACCOUNT_KEY, "campaign_id"], ["campaign_name"], {}, []),
    "int_ad_campaign_names__collapsed": ([*ACCOUNT_KEY, "campaign_id", "valid_from"], [], {}, []),
    "int_conversion_actions__resolved": (
        ["platform", "conversion_action_id"],
        ["is_primary"],
        {},
        [],
    ),
    "int_ad_conversions__unioned": (
        [*STATS_KEY, "conversion_action_id"],
        ["currency", "is_primary"],
        {},
        [TO_CAMPAIGNS, TO_RATES],
    ),
    "int_ad_stats__unioned": (
        STATS_KEY,
        [*STATS_KEY, "currency", "client_id", "campaign_name", "spend"],
        {},
        [TO_ACCOUNTS, TO_CAMPAIGNS, TO_RATES],
    ),
    "int_ad_groups__unioned": (
        [*ACCOUNT_KEY, "ad_group_id"],
        ["campaign_id", "ad_group_name"],
        {},
        [TO_CAMPAIGNS],
    ),
    "int_ads__unioned": (
        [*ACCOUNT_KEY, "ad_group_id", "ad_id"],
        ["campaign_id", "ad_name"],
        {},
        [TO_AD_GROUPS],
    ),
    "int_ad_stats_ad_group__unioned": (
        AD_GROUP_KEY,
        [*AD_GROUP_KEY, *NAMED, "spend"],
        {},
        [TO_ACCOUNTS, TO_CAMPAIGNS, TO_AD_GROUPS, TO_RATES],
    ),
    "int_ad_stats_ad__unioned": (
        AD_KEY,
        [*AD_KEY, *NAMED, "ad_name", "spend"],
        {},
        [TO_ACCOUNTS, TO_CAMPAIGNS, TO_ADS, TO_RATES],
    ),
    "int_fx_rates__filled": (["rate_date", "currency"], ["rate", "rate_source"], {}, []),
    "int_web_sessions__unioned": (SESSIONS_KEY, ["client_id", "currency"], {}, [TO_RATES]),
    # The views over which the core takes its dimensions and their versions (ADR-0036).
    "int_accounts__current": (ACCOUNT_KEY, [], {}, []),
    "int_campaigns__current": ([*ACCOUNT_KEY, "campaign_id"], [], {}, []),
    "int_ad_groups__current": ([*ACCOUNT_KEY, "ad_group_id"], ["campaign_id"], {}, []),
    "int_ads__current": ([*ACCOUNT_KEY, "ad_group_id", "ad_id"], ["campaign_id"], {}, []),
    # The checks of G1 and G1a, by load, account, reason and what they read (ADR-0035).
    "int_g1_failures": (
        ["source", "load_id", "account_id", "reason", "checked"],
        ["source", "account_id", "reason", "checked"],
        {},
        [],
    ),
    # The days of the windows that the facts of the core take (ADR-0038).
    "int_window_days": (["account_sk", "report_date"], [], {}, []),
}
COMPOSITE = (
    r"^      - relationships_composite:\n          arguments:\n            columns: \[(.*)\]\n"
    r"            to: ref\('(\w+)'\)\n            to_columns: \[(.*)\]$"
)
RELATIONSHIPS = r"^ +to: ref\('(\w+)'\)\n +field: (\w+)$"
# The one warning of the intermediate models, a missing day, under the name that the log prints.
GAP = (
    "      - gap:\n          name: int_ad_stats__unioned_has_no_missing_day\n"
    "          arguments:\n            account_columns: [platform, account_id]\n"
    "            date_column: report_date\n          config:\n            severity: warn\n"
)


def split(columns: str) -> list[str]:
    return [column.strip() for column in columns.split(",")]


def test_the_intermediate_models_have_their_critical_tests():
    # A test that is missing or a warning would pass every run of dbt, and the marts would add up
    # rows that it should have stopped (TestBehavior.BUILD of Cosmos).
    text = (INTERMEDIATE / "_intermediate__models.yml").read_text(encoding="utf-8")
    found = {path.stem for path in INTERMEDIATE.glob("*.sql")}
    assert found == set(INTERMEDIATE_TESTS), sorted(found ^ set(INTERMEDIATE_TESTS))
    described = blocks(text, 2)
    assert set(described) == set(INTERMEDIATE_TESTS)
    assert text.count("severity") == 1, "only the test of a missing day is a warning"
    assert text.count(GAP) == 1, "the warning of a missing day, by its name"
    assert GAP in described["int_ad_stats__unioned"]
    for model, (key, not_null, related, composite) in INTERMEDIATE_TESTS.items():
        block = described[model]
        columns = blocks(block, 6)
        tested = {name: column_tests(column) for name, column in columns.items()}
        unique = [split(part) for part in re.findall(UNIQUE, block, flags=re.MULTILINE)]
        if len(key) == 1:
            assert unique == [], model
            assert {c for c, tests in tested.items() if "unique" in tests} == set(key), model
        else:
            assert unique == [key], model
            assert not any("unique" in tests for tests in tested.values()), model
        assert {c for c, tests in tested.items() if "not_null" in tests} == set(not_null), model
        assert {
            column: re.search(RELATIONSHIPS, columns[column], flags=re.MULTILINE).groups()
            for column, tests in tested.items()
            if "relationships" in tests
        } == related, model
        pairs = re.findall(COMPOSITE, block, flags=re.MULTILINE)
        assert [(split(a), to, split(b)) for a, to, b in pairs] == composite, model
    categories = re.search(
        r"^ +values: \[(.*)\]$", described["int_conversion_actions__resolved"], re.M
    )
    assert categories, "the categories of conversion_category"
    assert [value.strip('"') for value in split(categories.group(1))] == [
        "purchase",
        "lead",
        "sign_up",
        "add_to_cart",
        "app_install",
        "other",
    ]


# The ad platforms of the model, whose staging the facts of ads of the core read.
AD_PLATFORMS = ("demo_search", "demo_social", "demo_video", "google_ads")
# The tests of the sums of G2: what each compares, the fact of the core, and the staging models.
G2_SUMS = [
    *(
        (f"statistics_of_{level}s", f"ad_stats_{level}_daily", f"{level}_stats")
        for level in ("campaign", "ad_group", "ad")
    ),
    ("conversions_of_campaigns", "ad_conversions_campaign_daily", "campaign_conversions"),
]
G2_SUMS = [
    (what, fact, {f"stg_{code}__{kind}" for code in AD_PLATFORMS}) for what, fact, kind in G2_SUMS
] + [("sessions", "web_sessions_daily", {"stg_demo_web__sessions_daily"})]


def test_the_singular_tests_of_the_model_are_errors_unless_a_row_can_be_right():
    # Clicks above impressions, an account of no client and lower levels above their parents can be
    # right, so they warn; primary actions that do not add up to the conversions of a campaign
    # cannot.
    wanted = {
        "assert_clicks_within_impressions": ({"int_ad_stats__unioned"}, True),
        "assert_every_account_has_a_client": ({"int_ad_accounts__unioned"}, True),
        "assert_primary_actions_add_up_to_the_conversions": (
            {"int_ad_conversions__unioned", "int_ad_stats__unioned"},
            False,
        ),
        # Reports of two levels loaded at different moments can disagree on recent days.
        "assert_ad_groups_add_up_to_no_more_than_their_campaigns": (
            {"int_ad_stats__unioned", "int_ad_stats_ad_group__unioned"},
            True,
        ),
        "assert_ads_add_up_to_no_more_than_their_ad_groups": (
            {"int_ad_stats_ad_group__unioned", "int_ad_stats_ad__unioned"},
            True,
        ),
        # A quarantine and the stop of a source are the run working as meant (ADR-0035): they warn,
        # and Ops shows them, as a test at error would stop the healthy accounts too (#188).
        "assert_no_account_is_in_quarantine": ({"int_g1_failures"}, True),
        "assert_no_source_is_stopped": ({"int_g1_failures"}, True),
        # G3 (ADR-0035, ADR-0042): a day of the windows of the facts without a rate in the core
        # would have no amount in the reporting currency.
        "assert_the_core_has_a_rate_for_every_day_of_the_windows": (
            {"int_window_days", RATES_MODEL},
            False,
        ),
        # G2 (ADR-0035): a fact that does not hold the sums of staging lost or doubled rows.
        **{
            f"assert_the_core_holds_the_{what}_of_staging": (
                {f"core_{fact}", "int_window_days", *staged},
                False,
            )
            for what, fact, staged in G2_SUMS
        },
    }
    assert {path.stem for path in (DBT / "tests").glob("*.sql")} == set(wanted)
    for name, (reads, warns) in wanted.items():
        text = (DBT / "tests" / f"{name}.sql").read_text(encoding="utf-8")
        assert set(referenced(text)) == reads, name
        assert ("{{ config(severity='warn') }}" in text) is warns, name
        assert text.count("severity") == int(warns), name


def test_every_column_of_the_intermediate_models_takes_the_doc_block_of_its_name():
    # The intermediate models share their columns with staging and the marts, where columns of
    # one name say the same.
    docs = (DBT / "models" / "_docs.md").read_text(encoding="utf-8")
    written = set(re.findall(r"^\{% docs (\w+) %\}$", docs, flags=re.MULTILINE))
    text = (INTERMEDIATE / "_intermediate__models.yml").read_text(encoding="utf-8")
    for model, block in blocks(text, 2).items():
        columns = blocks(block, 6)
        assert columns, model
        for name, column in columns.items():
            description = re.search(r"^        description: (.*)$", column, flags=re.MULTILINE)
            assert description, (model, name)
            assert description.group(1) == f"'{{{{ doc(\"{name}\") }}}}'", (model, name)
            assert name in written, (model, name)


ECB = STAGING / "ecb"
# The columns of the CSV of the ECB, as dlt names them, which the load keeps as text (ecb.py).
ECB_COLUMNS = (
    "key",
    "freq",
    "currency",
    "currency_denom",
    "exr_type",
    "exr_suffix",
    "time_period",
    "obs_value",
)


def test_the_staging_of_the_ecb_reads_its_csv_through_its_shape_even_before_a_load():
    # The DAG of the rates starts paused, so a deployment with the demo alone never has the table:
    # the model reads an empty query of the same columns until a load writes it (ADR-0032).
    text = (ECB / "stg_ecb__rates_daily.sql").read_text(encoding="utf-8")
    comment, *_ = text.splitlines()
    assert comment.startswith("-- stg_ecb__rates_daily.sql: ")
    columns = dict(re.findall(r"'(\w+)': '([\w ]+)'", text))
    assert columns == dict.fromkeys((*ECB_COLUMNS, "_dlt_load_id"), "text")
    call = "{{ ecb_exr(newest_load(relation_or_empty(source('raw_ecb', 'rates_daily'), {"
    assert text.count(call) == 1
    assert text.rstrip().endswith("}), 'raw_ecb', ref('stg_loads__newest_days')), 'ecb') }}")
    macro = (DBT / "macros" / "shapes" / "ecb_exr.sql").read_text(encoding="utf-8")
    for column in ("time_period", "currency_denom", "currency", "obs_value"):
        assert column in macro
    assert "'{{ platform }}' as rate_source" in macro


def test_the_sources_of_the_ecb_are_the_tables_that_its_load_writes():
    text = (ECB / "_ecb__sources.yml").read_text(encoding="utf-8")
    [source] = re.findall(r"^  - name: (\w+)\n    description:", text, flags=re.MULTILINE)
    assert source == "raw_ecb"
    assert re.search(r"^    schema: raw_ecb$", text, flags=re.MULTILINE)
    pattern = r"^      - name: (\w+)\n        description:"
    assert set(re.findall(pattern, text, flags=re.MULTILINE)) == {
        "rates_daily",
        "_agoranomos_loads",
        "_dlt_loads",
    }


def test_the_staging_of_the_ecb_has_the_critical_tests_of_the_rates_of_the_demo():
    # The model of the rates of the ECB and that of the demo give the same columns, and the same
    # tests guard both.
    ecb = (ECB / "_ecb__models.yml").read_text(encoding="utf-8").split("    data_tests:", 1)[1]
    demo = (STAGING / "demo_fx" / "_demo_fx__models.yml").read_text(encoding="utf-8")
    assert ecb == demo.split("    data_tests:", 1)[1]
    assert "severity" not in ecb


def test_the_rates_take_the_ecb_alone_once_it_has_any_and_the_loads_show_it_before_a_load():
    # Never a fictional rate of the demo on a day on which the ECB publishes none (ADR-0032).
    rates = (DBT / "models" / "intermediate" / "int_fx_rates__filled.sql").read_text(
        encoding="utf-8"
    )
    assert "    from {{ ref('stg_ecb__rates_daily') }}\n    union all\n" in rates
    assert "where not exists (select from {{ ref('stg_ecb__rates_daily') }})" in rates
    loads = (DBT / "models" / "ops" / "ops_loads.sql").read_text(encoding="utf-8")
    assert "('raw_ecb', 'ecb')," in loads


GOOGLE_ADS = STAGING / "google_ads"
# The staging models of Google Ads, each one call of a macro of the shape that demo_search mimics,
# over a table of raw_google_ads, through relation_or_empty, as the DAG starts paused (ADR-0032);
# the demo has no conversion actions of its own, which the connector alone reads.
GOOGLE_ADS_MODELS = {
    "campaign_stats": ("google_ads_stats", "campaign_stats"),
    "campaign_conversions": ("google_ads_conversions", "campaign_conversions"),
    "accounts": ("google_ads_customers", "customers"),
    "campaigns": ("google_ads_campaigns", "campaigns"),
    "ad_groups": ("google_ads_ad_groups", "ad_groups"),
    "ad_group_stats": ("google_ads_stats", "ad_group_stats", "ad_group"),
    "ad_group_conversions": ("google_ads_conversions", "ad_group_conversions", "ad_group"),
    "ads": ("google_ads_ads", "ads"),
    "ad_stats": ("google_ads_stats", "ad_stats", "ad"),
    "ad_conversions": ("google_ads_conversions", "ad_conversions", "ad"),
    "conversion_actions": ("google_ads_conversion_actions", "conversion_actions"),
}
# The tables of entities of Google Ads, whose versions dlt keeps (scd2).
GOOGLE_ADS_VERSIONED = ("campaigns", "ad_groups", "ads", "conversion_actions")


def test_each_staging_model_of_google_ads_is_one_call_of_the_shape_that_the_demo_shares():
    found = {path.name for path in GOOGLE_ADS.glob("*.sql")}
    assert found == {f"stg_google_ads__{model}.sql" for model in GOOGLE_ADS_MODELS}
    for model, (macro, table, *level) in GOOGLE_ADS_MODELS.items():
        text = (GOOGLE_ADS / f"stg_google_ads__{model}.sql").read_text(encoding="utf-8")
        assert text.startswith(f"-- stg_google_ads__{model}.sql: "), model
        relation = f"relation_or_empty(source('raw_google_ads', '{table}'), {{"
        by_load = model in BY_LOAD
        call = f"{{{{ {macro}(newest_load({relation}" if by_load else f"{{{{ {macro}({relation}"
        assert text.count(call) == 1, model
        arguments = ", ".join(["'google_ads'", *(f"'{name}'" for name in level)])
        days = "ref('stg_loads__newest_days')"
        end = (
            f"}}), 'raw_google_ads', {days}), {arguments}) }}}}"
            if by_load
            else f"}}), {arguments}) }}}}"
        )
        assert text.rstrip().endswith(end), model
        # No model names a seed, as none of the demo does: the checks of int_g1_failures read them.
        assert "depends_on" not in text, model
    macros = (DBT / "macros" / "shapes" / "google_ads_v25.sql").read_text(encoding="utf-8")
    assert "{% macro google_ads_customers(relation, platform) %}" in macros
    assert "{% macro google_ads_conversion_actions(relation, platform) %}" in macros


def test_the_columns_of_the_empty_tables_of_google_ads_are_the_fields_of_its_queries():
    # The empty query of a table before the first load has the columns that dlt makes from the
    # fields of its query, so that the model builds the same before and after it.
    # The source hints a column for each field of its queries (google_ads.columns), and dlt is not
    # in the development environment, so the fields come from the text of the source.
    source = ast.parse((DAGS_PIPELINES / "google_ads.py").read_text(encoding="utf-8"))
    [fields] = [
        ast.literal_eval(node.value)
        for node in source.body
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "FIELDS"
    ]
    for model, (_, table, *_) in GOOGLE_ADS_MODELS.items():
        text = (GOOGLE_ADS / f"stg_google_ads__{model}.sql").read_text(encoding="utf-8")
        columns = set(re.findall(r"'(\w+)': '[\w ]+'", text))
        hinted = {field.strip().replace(".", "__") for field in fields[table].split(",")}
        scd2 = {"_dlt_valid_from", "_dlt_valid_to"} if table in GOOGLE_ADS_VERSIONED else set()
        load = {"_dlt_load_id"} if model in BY_LOAD else set()
        wanted = hinted | scd2 | load
        assert columns == wanted, (model, sorted(columns ^ wanted))


def test_the_staging_of_google_ads_has_the_tests_of_the_staging_of_demo_search():
    # The same macros give the same columns, and the same critical tests guard both. The
    # conversion actions, which the demo does not have, come last.
    demo = (STAGING / "demo_search" / "_demo_search__models.yml").read_text(encoding="utf-8")
    text = (GOOGLE_ADS / "_google_ads__models.yml").read_text(encoding="utf-8")
    ads, actions = text.split("  - name: stg_google_ads__conversion_actions\n")
    assert "  - name: stg_" not in actions

    def tests(text):
        return [
            line
            for line in text.replace("demo_search", "google_ads").splitlines()
            if "description" not in line and not line.startswith("      ")
        ]

    assert [
        b.split("    data_tests:", 1)[1].split("  - name:", 1)[0]
        for b in ads.split("  - name: stg_")[1:]
    ] == [
        b.split("    data_tests:", 1)[1].split("  - name:", 1)[0]
        for b in demo.replace("demo_search", "google_ads").split("  - name: stg_")[1:]
    ]
    assert tests(ads) == tests(demo)


def test_the_sources_of_google_ads_are_the_tables_that_its_load_writes():
    text = (GOOGLE_ADS / "_google_ads__sources.yml").read_text(encoding="utf-8")
    [source] = re.findall(r"^  - name: (\w+)\n    description:", text, flags=re.MULTILINE)
    assert source == "raw_google_ads"
    pattern = r"^      - name: (\w+)\n        description:"
    assert set(re.findall(pattern, text, flags=re.MULTILINE)) == {
        "_agoranomos_loads",
        "_dlt_loads",
        "customers",
        "campaigns",
        "campaign_stats",
        "campaign_conversions",
        "ad_groups",
        "ad_group_stats",
        "ad_group_conversions",
        "ads",
        "ad_stats",
        "ad_conversions",
        "conversion_actions",
    }


def test_every_union_of_the_platforms_reads_google_ads_and_the_loads_show_it():
    unions = {
        "int_ad_stats__unioned": "stg_google_ads__campaign_stats",
        "int_ad_conversions__unioned": "stg_google_ads__campaign_conversions",
        "int_ad_accounts__unioned": "stg_google_ads__accounts",
        "int_ad_campaigns__unioned": "stg_google_ads__campaigns",
        "int_ad_campaign_names__collapsed": "stg_google_ads__campaigns",
        "int_conversion_actions__resolved": "stg_google_ads__campaign_conversions",
        "int_ad_groups__unioned": "stg_google_ads__ad_groups",
        "int_ads__unioned": "stg_google_ads__ads",
        "int_ad_stats_ad_group__unioned": "stg_google_ads__ad_group_stats",
        "int_ad_stats_ad__unioned": "stg_google_ads__ad_stats",
        "int_accounts__current": [
            "stg_google_ads__accounts",
            "stg_google_ads__campaign_stats",
            "stg_demo_web__properties",
            "stg_demo_web__sessions_daily",
        ],
        "int_campaigns__current": ["stg_google_ads__campaigns", "stg_google_ads__campaign_stats"],
        "int_ad_groups__current": ["stg_google_ads__ad_groups", "stg_google_ads__ad_group_stats"],
        "int_ads__current": ["stg_google_ads__ads", "stg_google_ads__ad_stats"],
    }
    for model, staged in unions.items():
        text = (INTERMEDIATE / f"{model}.sql").read_text(encoding="utf-8")
        for name in [staged] if isinstance(staged, str) else staged:
            assert f"from {{{{ ref('{name}') }}}}" in text, (model, name)
    # And the conversion actions that the accounts list, which say which of them are primary.
    resolved = (INTERMEDIATE / "int_conversion_actions__resolved.sql").read_text(encoding="utf-8")
    assert "from {{ ref('stg_google_ads__conversion_actions') }}" in resolved
    loads = (DBT / "models" / "ops" / "ops_loads.sql").read_text(encoding="utf-8")
    assert "('raw_google_ads', 'google_ads')," in loads


PROJECT_FILE = DBT / "dbt_project.yml"
SNAPSHOTS = DBT / "snapshots"
SNAPSHOTS_FILE = SNAPSHOTS / "_snapshots.yml"
# The directories of models whose tests take the tag critical from dbt_project.yml, by the
# directory of their YAML file, as dbt gives the fqn of a generic test (dbt-core 1.12.6).
TAGGED_DIRECTORIES = ("staging", "marts")
TESTS_LINE = re.compile(r"^( +)data_tests:(?: \[(.*)\])?$")
TEST_ITEM = re.compile(r"^( +)- (\w+):?$")


def generic_tests(text: str) -> list[tuple[str, bool, bool]]:
    """The generic tests of a YAML file, in order: the name of each, whether its config tags it
    critical, and whether it is a warning. A list on the line of data_tests holds tests without a
    config; below it, each item is one test, with its body until the next line that is no deeper,
    and a comment between the items belongs to none."""
    lines = text.splitlines()
    found = []
    i = 0
    while i < len(lines):
        match = TESTS_LINE.match(lines[i])
        i += 1
        if not match:
            continue
        indent = len(match.group(1))
        if match.group(2) is not None:
            found += [(name.strip(), False, False) for name in match.group(2).split(",")]
            continue
        while i < len(lines):
            line = lines[i]
            depth = len(line) - len(line.lstrip(" "))
            if line.lstrip().startswith("#") and depth >= indent + 2:
                i += 1
                continue
            item = TEST_ITEM.match(line)
            if not item or depth != indent + 2:
                break
            i += 1
            body = []
            while i < len(lines) and len(lines[i]) - len(lines[i].lstrip(" ")) > indent + 2:
                body.append(lines[i])
                i += 1
            joined = "\n".join(body)
            found.append((item.group(2), "tags: [critical]" in joined, "severity: warn" in joined))
    return found


def test_every_test_that_blocks_carries_the_tag_critical_and_no_warning_does():
    # ADR-0038: the tag critical marks the tests that block, every test with the severity error,
    # for a run of them by hand, dbt build --select tag:critical (CONTRIBUTING.md). Tags add up
    # and none can be taken back, so a directory gives the tag only where every test of it blocks:
    # the staging models and the marts. The intermediate models and the dimensions of the core hold
    # a warning, and the YAML of the seeds and that of the snapshots lie outside models, so each of
    # their tests that blocks carries the tag in its own config, as does the singular test that
    # blocks.
    project = PROJECT_FILE.read_text(encoding="utf-8")
    assert "\ndata_tests:\n  agoranomos:\n" in project
    for directory in TAGGED_DIRECTORIES:
        assert f"\n    {directory}:\n      +tags: [critical]\n" in project, directory
    assert project.count("+tags: [critical]") == len(TAGGED_DIRECTORIES)
    counted = 0
    files = [*sorted((DBT / "models").rglob("*.yml")), SEEDS / "_seeds.yml", SNAPSHOTS_FILE]
    for path in files:
        relative = path.relative_to(DBT)
        by_directory = relative.parts[0] == "models" and relative.parts[1] in TAGGED_DIRECTORIES
        tests = generic_tests(path.read_text(encoding="utf-8"))
        counted += len(tests)
        for name, tagged, warns in tests:
            assert not (by_directory and tagged), (str(relative), name, "tagged twice")
            assert (by_directory or tagged) is (not warns), (str(relative), name)
    # The tests of the base package, as of November 2026: a parser that found fewer would pass a
    # test that lost its tag.
    assert counted == 387, counted
    for path in sorted((DBT / "tests").glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        tagged = "{{ config(tags=['critical']) }}" in text
        warns = "{{ config(severity='warn') }}" in text
        assert tagged is (not warns), path.name
        assert text.count("config(") == 1, path.name


def test_each_name_of_a_shape_goes_through_to_name():
    # A load writes an empty name as NULL, and a row of an earlier release can keep it: to_name
    # makes them one (ADR-0038), and keeps the type of the column, which nullif would change.
    macro = (DBT / "macros" / "names.sql").read_text(encoding="utf-8")
    assert "case when {{ expression }} <> '' then {{ expression }} end" in macro
    name = re.compile(r" as (observed_name|loaded_\w+_name|account_name|property_name)\b")
    for path in sorted((DBT / "macros" / "shapes").glob("*.sql")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if name.search(line):
                assert "to_name(" in line, (path.name, line)


CORE = DBT / "models" / "core"
# The migrations that create the dimensions and the registries of the core (ADR-0036).
DIMENSION_MIGRATIONS = ("0003_create_the_registries.sql", "0004_create_the_dimensions.sql")
# The journals of the core that a model writes, by their migration: the journal of quarantine,
# from the checks of G1 and G1a (ADR-0035). The pin writes core.loads, the models of the facts the
# journals of restatements in their hooks, and the totals of G0a wait for #212.
JOURNAL_MODELS = {"0006_create_the_journals.sql": {"account_quarantine"}}
COMMENT = re.compile(
    r"^COMMENT ON (TABLE|COLUMN) core\.(\w+)(?:\.(\w+))? IS\n'((?:[^']|'')*)';$", flags=re.M
)
CREATE = re.compile(r"^CREATE TABLE core\.(\w+) \(\n(.*?)\n\);$", flags=re.M | re.S)
# The registries, with their surrogate key and the natural key on which the model merges.
REGISTRIES = {
    "dim_account": ("account_sk", ["platform", "account_id"]),
    "dim_conversion_action": ("conversion_action_sk", ["platform", "conversion_action_id"]),
}
# The views of staging over which the core takes the entities and their versions.
ENTITIES = {
    "dim_campaign": "int_campaigns__current",
    "dim_ad_group": "int_ad_groups__current",
    "dim_ad": "int_ads__current",
}
# The end of a version that holds (dbt_valid_to_current), in dbt_project.yml and in the tests of
# the versions.
CURRENT_TO = "\"'9999-12-31 00:00:00'::timestamp\""


def migration_tables(migrations: dict[str, set[str] | None]) -> dict[str, dict]:
    """The tables that the migrations create, of each migration those of its set, or all of them
    for none: for each table, its columns in order, its key, its natural key where it has one, and
    the comments of the table and of its columns."""
    tables: dict[str, dict] = {}
    for migration, kept in migrations.items():
        text = (MIGRATIONS / migration).read_text(encoding="utf-8")
        for name, body in CREATE.findall(text):
            if kept is not None and name not in kept:
                continue
            columns, key, unique = [], [], []
            for raw in body.splitlines():
                line = raw.strip().rstrip(",")
                if found := re.fullmatch(r"PRIMARY KEY \((.*)\)", line):
                    key = split(found.group(1))
                elif found := re.fullmatch(r"UNIQUE \((.*)\)", line):
                    unique = split(found.group(1))
                elif raw.startswith("  ") and not raw.startswith("   "):
                    column = re.match(r"(\w+) ", line)
                    if column and column.group(1) != "CHECK":
                        columns.append(column.group(1))
                        if " PRIMARY KEY" in line:
                            key = [column.group(1)]
            tables[name] = {"columns": columns, "key": key, "unique": unique, "comments": {}}
        for kind, table, column, body in COMMENT.findall(text):
            if table in tables:
                comment = body.replace("''", "'")
                tables[table]["comments"][None if kind == "TABLE" else column] = comment
    return tables


def dimension_tables() -> dict[str, dict]:
    """The tables of the migrations that the models of the core write by their names, the
    dimensions and registries and the journal of quarantine."""
    return migration_tables({**dict.fromkeys(DIMENSION_MIGRATIONS), **JOURNAL_MODELS})


# The rates of the core, which the model core_fx_rates_daily writes under the name of its table, as
# a mart has the name (ADR-0042).
RATES_MIGRATION = "0007_create_the_rates.sql"
RATES_MODEL = "core_fx_rates_daily"


def rates_table() -> dict:
    """The table of the rates of the core, as migration 0007 creates it."""
    return migration_tables({RATES_MIGRATION: {"fx_rates_daily"}})["fx_rates_daily"]


FACT_CREATE = re.compile(
    r"^CREATE TABLE core\.(\w+) \(\n(.*?)\n\) PARTITION BY RANGE \(report_date\);$",
    flags=re.M | re.S,
)
FACT_COLUMN = re.compile(r"^  (\w+) ([a-z]+(?:\(\d+(?:,\d+)?\))?)")


def fact_tables() -> dict[str, dict]:
    """The facts of the core, as migration 0005 creates them: for each, its columns in order with
    their types, without a collation, and the comments of the table and of its columns."""
    text = (MIGRATIONS / "0005_create_the_facts.sql").read_text(encoding="utf-8")
    tables: dict[str, dict] = {}
    for name, body in FACT_CREATE.findall(text):
        columns = {
            found.group(1): found.group(2)
            for found in map(FACT_COLUMN.match, body.splitlines())
            if found and found.group(1) not in ("PRIMARY", "CHECK", "UNIQUE")
        }
        tables[name] = {"columns": list(columns), "types": columns, "comments": {}}
    for kind, table, column, body in COMMENT.findall(text):
        if table in tables:
            tables[table]["comments"][None if kind == "TABLE" else column] = body.replace("''", "'")
    return tables


def folded(block: str, indent: int) -> str:
    """The description of an item of YAML whose keys are at indent, folded with >- as dbt reads it:
    its lines joined by spaces."""
    found = re.search(
        rf"^ {{{indent}}}description: >-\n((?: {{{indent + 2}}}.*\n?)+)", block, flags=re.M
    )
    assert found, block[:80]
    return " ".join(line.strip() for line in found.group(1).splitlines())


def model_config(text: str) -> str:
    """The text of the config() call of a model."""
    found = re.search(r"\{\{ config\((.*?)\) \}\}", text, flags=re.S)
    assert found, text[:80]
    return found.group(1)


def test_the_dimensions_of_the_core_merge_into_the_tables_of_the_migrations():
    # ADR-0036 and ADR-0038: incremental merge models, never a full refresh, that fail rather than
    # change their table, which only its owner may, and that write no comment, which needs the
    # ownership too (ADR-0035). A registry takes its key as coalesce(the key of the table,
    # nextval), as dbt writes every column, and the merge leaves the key out of what it updates.
    project = PROJECT_FILE.read_text(encoding="utf-8")
    section = (
        "\n    core:\n      +schema: core\n      +materialized: incremental\n"
        "      +incremental_strategy: merge\n      +full_refresh: false\n"
        "      +on_schema_change: fail\n      +persist_docs:\n        relation: false\n"
        "        columns: false\n"
    )
    assert section in project
    tables = dimension_tables()
    facts = {f"core_{fact}" for fact in fact_tables()}
    assert {path.stem for path in CORE.glob("*.sql")} == set(tables) | facts | {RATES_MODEL}
    for name, table in tables.items():
        text = (CORE / f"{name}.sql").read_text(encoding="utf-8")
        config = model_config(text)
        assert "materialized" not in config, name
        assert "source(" not in text, name
        assert "from {{ this }}" in text, name
        if name in REGISTRIES:
            sk, natural = REGISTRIES[name]
            assert table["key"] == [sk], name
            assert table["unique"] == natural, name
            assert f"unique_key={natural!r}" in config, name
            assert f"merge_exclude_columns=[{sk!r}]" in config, name
            sequence = f"core.{name}_{sk}_seq"
            assert re.search(rf"coalesce\(\s*\w+\.{sk},\s*nextval\('{sequence}'\)\s*\)", text), name
        else:
            key = table["key"]
            wanted = repr(key[0]) if len(key) == 1 else repr(key)
            assert f"unique_key={wanted}" in config, name
            assert "nextval" not in text, name


def test_the_descriptions_of_the_core_are_the_comments_of_its_migrations():
    # The migrations describe the tables of the core in the warehouse, as persist_docs needs the
    # ownership of a table (ADR-0038): the YAML of each model says the same of the table and of
    # every column, so that dbt and the warehouse describe them alike.
    text = (CORE / "_core__models.yml").read_text(encoding="utf-8")
    tables = dimension_tables()
    # The models of the facts write into the tables of their alias, as the marts have their names.
    tables |= {f"core_{fact}": table for fact, table in fact_tables().items()}
    tables[RATES_MODEL] = rates_table()
    described = blocks(text, 2)
    assert set(described) == set(tables)
    for name, table in tables.items():
        block = described[name]
        assert folded(block, 4) == table["comments"][None], name
        columns = blocks(block, 6)
        assert list(columns) == table["columns"], name
        for column, item in columns.items():
            assert folded(item, 8) == table["comments"][column], (name, column)


def test_the_rates_of_the_core_merge_the_days_that_a_run_brings():
    # ADR-0042: a rate for each calendar day, currency of the accounts and reporting currency,
    # through the euro with no rounding and carried over a day without one with a flag, by an
    # incremental merge into the table of migration 0007 in the types of its columns, which writes
    # only the rows that changed. The ECB once it has a rate, in staging or in the core, so that a
    # run that reads no load of the ECB takes no rate of the demo.
    text = (CORE / f"{RATES_MODEL}.sql").read_text(encoding="utf-8")
    config = model_config(text)
    assert "alias='fx_rates_daily'" in config
    assert "unique_key=['rate_date', 'currency', 'reporting_currency']" in config
    table = rates_table()
    assert table["key"] == ["rate_date", "currency", "reporting_currency"]
    [(_, body)] = CREATE.findall((MIGRATIONS / RATES_MIGRATION).read_text(encoding="utf-8"))
    columns = {
        found.group(1): found.group(2)
        for found in map(FACT_COLUMN.match, body.splitlines())
        if found and found.group(1) not in ("PRIMARY", "CHECK", "UNIQUE")
    }
    assert list(columns) == table["columns"]
    for column, kind in columns.items():
        assert re.search(rf" as {re.escape(kind)}\) as {column},?\n", text), column
    assert set(referenced(text)) == {
        "stg_ecb__rates_daily",
        "stg_demo_fx__rates_daily",
        "stg_loads__newest_days",
        "int_window_days",
        "dim_account",
    }
    assert "round(" not in text
    assert "published_on < rate_date as is_carried_forward" in text
    assert "{{ 'true' if var('recompute_rates') else 'false' }}" in text
    assert "\n  recompute_rates: false\n" in PROJECT_FILE.read_text(encoding="utf-8")
    rates = (DBT / "macros" / "rates.sql").read_text(encoding="utf-8")
    assert "or exists (select from {{ rates }} where rate_source = 'ecb')" in rates
    assert "r.rate / c.rate as rate_into_reporting_currency" in rates
    # A load of the ECB reads the last 14 days again (ADR-0042).
    ecb = (DAGS_PIPELINES / "ecb.py").read_text(encoding="utf-8")
    assert "\nWINDOW_DAYS = 14\n" in ecb


def test_the_views_of_the_core_take_their_accounts_from_one_place():
    # The registry of accounts and the views of the entities take their accounts from
    # int_accounts__current, so that an account that stays out of the core in a run, as one in
    # quarantine, stays out of every dimension by one filter (ADR-0035, ADR-0036).
    registry = (CORE / "dim_account.sql").read_text(encoding="utf-8")
    assert "from {{ ref('int_accounts__current') }} as c" in registry
    # The filter: an account that the checks of this run hold, once the journal has recorded it.
    current = (INTERMEDIATE / "int_accounts__current.sql").read_text(encoding="utf-8")
    assert "-- depends_on: {{ ref('account_quarantine') }}\n" in current
    assert current.endswith(
        "where {{ not_in_quarantine(ref('int_g1_failures'), 'n.platform', 'n.account_id') }}\n"
    )
    for dimension, view in ENTITIES.items():
        text = (INTERMEDIATE / f"{view}.sql").read_text(encoding="utf-8")
        assert "from {{ ref('int_accounts__current') }} as a" in text, view
        assert "join {{ ref('dim_account') }} as d" in text, view
        assert "from {{ source('core', 'loads') }}" in text, view
        model = (CORE / f"{dimension}.sql").read_text(encoding="utf-8")
        assert f"from {{{{ ref('{view}') }}}}" in model, dimension


def test_the_versions_of_the_entities_are_snapshots_of_the_views_of_staging():
    # ADR-0036 and R1 (#187): snapshots with the strategy check on a list of columns, hard_deletes
    # ignore and dbt_valid_to_current, in core, which transformer owns and so describes; each
    # reads a view of staging and never a source, which fails while its table does not exist.
    project = PROJECT_FILE.read_text(encoding="utf-8")
    assert 'snapshot-paths: ["snapshots"]' in project
    section = (
        "\nsnapshots:\n  agoranomos:\n    +schema: core\n    +strategy: check\n"
        f"    +hard_deletes: ignore\n    +dbt_valid_to_current: {CURRENT_TO}\n"
        "    +persist_docs:\n      relation: true\n      columns: true\n"
    )
    assert section in project
    tables = dimension_tables()
    text = SNAPSHOTS_FILE.read_text(encoding="utf-8")
    described = blocks(text, 2)
    wanted = {f"{dimension}_versions": view for dimension, view in ENTITIES.items()}
    assert set(described) == set(wanted)
    assert {path.stem for path in SNAPSHOTS.glob("*.sql")} == set(wanted)
    meta = ["dbt_scd_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to"]
    for name, view in wanted.items():
        sql = (SNAPSHOTS / f"{name}.sql").read_text(encoding="utf-8")
        assert f"{{% snapshot {name} %}}" in sql, name
        assert "{% endsnapshot %}" in sql, name
        assert referenced(sql) == [view], name
        selected = re.findall(r" as (\w+),?$", sql, flags=re.M)
        block = described[name]
        key = tables[name.removesuffix("_versions")]["key"]
        assert f"      unique_key: [{', '.join(key)}]" in block, name
        check = re.search(r"check_cols:\s*\[(.*?)\]", block, flags=re.S)
        assert check, name
        checked = split(check.group(1))
        assert set(checked) <= set(selected) - set(key), name
        assert not any(column.endswith("_state") for column in checked), name
        assert set(blocks(block, 6)) == {*selected, *meta}, name
        assert f"            current_to: {CURRENT_TO}\n" in block, name
        assert f"            key_columns: [{', '.join(key)}]\n" in block, name
        assert f"            columns: [{', '.join([*key, 'dbt_valid_from'])}]\n" in block, name


# The common state of an entity by the status of each shape of an API (ADR-0036), within the values
# that the tables of the core allow.
STATES = {
    "google_ads_v25.sql": (
        "google_ads_state",
        {"ENABLED": "active", "PAUSED": "paused", "REMOVED": "removed"},
        ("campaign__status", "ad_group__status", "ad_group_ad__status"),
    ),
    "meta_v26.sql": (
        "meta_state",
        {"ACTIVE": "active", "PAUSED": "paused", "DELETED": "removed", "ARCHIVED": "removed"},
        ("status", "status", "status"),
    ),
    "tiktok_v1_3.sql": (
        "tiktok_state",
        {"ENABLE": "active", "DISABLE": "paused", "DELETE": "removed"},
        ("operation_status", "operation_status", "operation_status"),
    ),
}


def test_each_shape_maps_the_statuses_of_its_platform_to_the_common_state():
    migration = (MIGRATIONS / "0004_create_the_dimensions.sql").read_text(encoding="utf-8")
    allowed = re.findall(r"_state IN \((.*?)\)", migration)
    assert len(allowed) == 3
    assert {value for found in allowed for value in split(found)} == {
        "'active'",
        "'paused'",
        "'removed'",
        "'other'",
    }
    for file, (macro, mapping, statuses) in STATES.items():
        text = (DBT / "macros" / "shapes" / file).read_text(encoding="utf-8")
        body = text[text.index(f"{{% macro {macro}(status) -%}}") :]
        body = body[: body.index("{%- endmacro %}")]
        found = dict(re.findall(r"when '(\w+)' then '(\w+)'", body))
        assert found == mapping, file
        assert "        else 'other'\n" in body, file
        for level, status in zip(("campaign", "ad_group", "ad"), statuses, strict=True):
            line = f"    {{{{ {macro}('{status}') }}}} as {level}_state,\n"
            assert f"as {level}_status,\n{line}" in text, (file, level)


def test_the_marts_read_the_old_path_and_nothing_of_the_core():
    # The dimensions of the core are laid beside the live marts (#177), which switch to views over
    # the core in one pull request (#201): until then no mart, and no model that a mart reads,
    # reads a model of the core or a version. They read the checks of int_g1_failures, which read
    # the tables of the core as sources, as the runs before this one left them.
    core = {path.stem for path in CORE.glob("*.sql")} | {
        path.stem for path in SNAPSHOTS.glob("*.sql")
    }
    parents = {
        path.stem: set(referenced(path.read_text(encoding="utf-8")))
        for path in (DBT / "models").rglob("*.sql")
    }
    for path in (DBT / "models" / "marts").glob("*.sql"):
        assert not ({path.stem} | ancestors(path.stem, parents)) & core, path.stem
        assert "int_g1_failures" in ancestors(path.stem, parents) or path.stem in (
            "ad_platforms",
            "clients",
            "fx_rates_daily",
        ), path.stem


QUARANTINE = DBT / "macros" / "quarantine.sql"
G1 = INTERMEDIATE / "int_g1_failures.sql"


def macro_text(name: str) -> str:
    """The body of a macro of macros/quarantine.sql."""
    text = QUARANTINE.read_text(encoding="utf-8")
    body = text[text.index(f"{{% macro {name}(") :]
    return body[: body.index("{%- endmacro %}")]


def returned(name: str) -> object:
    """The value that a macro of macros/quarantine.sql returns, a literal of lists and tuples of
    texts, with its variables set from the lists that it sets first."""
    body = macro_text(name)
    value = body[body.index("return(") + len("return(") : body.rindex(") -}}")]
    for variable, listed in re.findall(r"\{%- set (\w+) = (\[.*?\]) -%\}", body, flags=re.S):
        value = re.sub(rf"\b{variable}\b", listed, value)
    for jinja, python in (("true", "True"), ("false", "False"), ("none", "None")):
        value = re.sub(rf"\b{jinja}\b", python, value)
    return ast.literal_eval(value)


def g1_config() -> str:
    """The text of the config() call of int_g1_failures, whose hooks hold ) }} of their own."""
    text = G1.read_text(encoding="utf-8")
    return text[text.index("{{ config(\n") : text.index("\n) }}\n")]


def hooks() -> list[str]:
    """The statements of the post-hooks of int_g1_failures, in their order."""
    return re.findall(r'^        "(.*)",$', g1_config(), re.M)


# The measures of staging that the facts of the core take beyond those that the tests of staging
# checked (ADR-0036), by kind: those that a column NOT NULL of a fact takes, and those that a
# column of zero or more takes, which G1 checks too, so that one account cannot fail the write of a
# fact for all.
STATS_TAKEN = ([], ["platform_conversions", "platform_conversion_value"])
ACTIONS_TAKEN = (
    ["platform_conversions"],
    [
        "platform_conversions",
        "platform_conversion_value",
        "all_conversions",
        "all_conversion_value",
    ],
)
CORE_TAKES = {
    **dict.fromkeys(("campaign_stats", "ad_group_stats", "ad_stats"), STATS_TAKEN),
    **dict.fromkeys(
        ("campaign_conversions", "ad_group_conversions", "ad_conversions"), ACTIONS_TAKEN
    ),
    "sessions_daily": (["engaged_sessions"], []),
}


def test_g1_checks_each_kind_of_facts_as_the_tests_of_staging_did():
    # ADR-0035: the keys, the IDs and the measures of the facts are checked for each account, so
    # that one account that fails holds out itself alone; the tests of staging checked them for all
    # accounts at once, and a failure stopped them all. The measures that the core takes besides
    # are checked as well, as a NULL or a value below zero in a column of a fact that refuses it
    # would fail its write for every account (ADR-0036).
    kinds = returned("quarantine_fact_kinds")
    assert set(kinds) == BY_ACCOUNT - {"accounts", "properties"}
    assert set(CORE_TAKES) == set(kinds)
    for kind, (account, key, ids, required, counted) in kinds.items():
        unique, not_null, non_negative = CHECKED[kind]
        needed, positive = CORE_TAKES[kind]
        assert set(unique) - {"platform"} == {"report_date", account, *key}, kind
        assert set(not_null) - {"report_date", "platform", account} | set(needed) == {
            *key,
            *ids,
            *required,
        }, kind
        assert set(non_negative) | set(positive) == set(counted), kind
    # The accounts and the properties: listed twice, and a currency and a time zone of the seeds.
    accounts = macro_text("quarantine_account_findings")
    for seed, field in IN_SEEDS.values():
        assert f"on {'c' if 'iso' in seed else 'z'}.{field} = l." in accounts, seed
    assert "where listings > 1" in accounts
    model = G1.read_text(encoding="utf-8")
    for seed, _ in IN_SEEDS.values():
        assert f"ref('{seed}')" in model, seed


def test_g1_runs_each_kind_of_data_in_a_statement_of_its_own():
    # R5 (#191): as one statement over the three levels, G1 held a dozen aggregates at once and was
    # the largest process of the database. Each statement of the hooks inserts the failures of one
    # kind, or of the raw tables of one source, into the table, then G1a, then the stop, which reads
    # what the others wrote, and the statistics of the table come last.
    config = g1_config()
    assert "materialized='incremental'" in config
    assert 'pre_hook=["{% if is_incremental() %}delete from {{ this }}{% endif %}"]' in config
    statements = hooks()
    assert statements[-1] == "analyze {{ this }}"
    kinds = list(returned("quarantine_fact_kinds"))
    by_kind = statements[: len(kinds)]
    for statement, kind in zip(by_kind, kinds, strict=True):
        assert statement.count("quarantine_insert(this, quarantine_attributed(") == 1, kind
        assert f"quarantine_fact_findings('{kind}', [" in statement, kind
        codes = ["demo_web"] if kind == "sessions_daily" else [*SHAPED_ADS, "google_ads"]
        read = re.findall(r"ref\('(stg_\w+)'\)", statement)
        assert read == [*(f"stg_{code}__{kind}" for code in codes), *["stg_loads__newest_days"] * 2]
    raw, collapse, sessions, rates, stop = statements[len(kinds) : -1]
    assert raw.startswith("{% for source_name in quarantine_raw_tables() %}")
    assert raw.count("quarantine_insert(") == 1
    assert "quarantine_collapse_findings([" in collapse
    assert "source('core', 'dim_account'), source('core', 'ad_stats_campaign_daily'))" in collapse
    # Web analytics has no spend: G1a finds a load of a property with no row where the core holds
    # sessions, before a window erases them.
    assert "quarantine_collapse_findings([ref('stg_demo_web__sessions_daily')]" in sessions
    assert (
        "source('core', 'dim_account'), source('core', 'web_sessions_daily'), measure='sessions', "
        "checked='sessions_daily', account='property_id', fall=false)"
    ) in sessions
    # ADR-0042: the currency of an account with no rate, in the rates of the run or in the core,
    # before the stop, which counts it.
    assert "quarantine_rate_findings([" in rates
    assert (
        "ref('stg_ecb__rates_daily'), ref('stg_demo_fx__rates_daily'), "
        "source('core', 'fx_rates_daily'))"
    ) in rates
    assert "quarantine_stop_findings(this, source('core', 'loads'))" in stop
    # dbt orders the model after what the statements read: a line depends_on, or a ref of its
    # query.
    model = G1.read_text(encoding="utf-8")
    query = model[model.index("\n) }}\n") :]
    for name in set(re.findall(r"ref\('(stg_\w+)'\)", config)):
        assert (
            f"-- depends_on: {{{{ ref('{name}') }}}}" in model
            or f"ref('{name}')" in query
            or name == "stg_loads__newest_days"
        ), name
    assert "-- depends_on: {{ source('core', 'fx_rates_daily') }}" in model


# The ad platforms of the demo.
SHAPED_ADS = ["demo_search", "demo_social", "demo_video"]


def test_g1_reads_the_fact_tables_of_raw_of_each_source_that_names_an_account():
    # Rows outside the window of their load and a spend with more than six places are lost in
    # staging, which reads by window and rounds to numeric(18,6): the checks read raw as it is.
    from agoranomos_pipelines.demo import platforms

    tables = returned("quarantine_raw_tables")
    newest = (DBT / "macros" / "newest_load.sql").read_text(encoding="utf-8")
    named = {
        source
        for source, account in re.findall(
            r"^    '(raw_\w+)': \((none|'[^']*'|\"[^\"]*\"),", newest, re.M
        )
        if account != "none"
    }
    assert set(tables) == named
    for code in [*SHAPED_ADS, "demo_web"]:
        dataset = platforms.PLATFORMS[code].dataset
        assert [table for table, _, _ in tables[dataset]] == [
            fact.table for fact in platforms.PLATFORMS[code].facts
        ], code
    assert tables["raw_google_ads"] == tables["raw_demo_search"]
    # The spend of each table, as the shape of its platform reads it.
    shapes = {
        "raw_demo_search": "google_ads_v25.sql",
        "raw_demo_social": "meta_v26.sql",
        "raw_demo_video": "tiktok_v1_3.sql",
    }
    for dataset, shape in shapes.items():
        text = (DBT / "macros" / "shapes" / shape).read_text(encoding="utf-8")
        for table, spend, micros in tables[dataset]:
            if spend is not None:
                cast = "micros_to_number" if micros else "to_number"
                assert f"{{{{ {cast}('{spend}') }}}} as spend" in text, (dataset, table)
    assert all(spend is None for _, spend, _ in tables["raw_demo_web"])


def test_the_reasons_of_quarantine_are_one_list():
    reasons = returned("quarantine_reasons")
    yaml = (INTERMEDIATE / "_intermediate__models.yml").read_text(encoding="utf-8")
    [values] = re.findall(r"^                values: \[(\"load_failed\".*)\]$", yaml, re.M)
    assert [value.strip().strip('"') for value in values.split(",")] == reasons
    docs = (DBT / "models" / "_docs.md").read_text(encoding="utf-8")
    block = docs[docs.index("{% docs reason %}") :]
    block = block[: block.index("{% enddocs %}")]
    assert re.findall(r"\b([a-z]+(?:_[a-z]+)+)\b", block)[: len(reasons)] == reasons
    text = QUARANTINE.read_text(encoding="utf-8")
    body = text[text.index("{%- endmacro %}") :]
    for reason in reasons:
        assert f"'{reason}'" in body, reason


def test_an_account_in_quarantine_stays_out_of_the_marts_of_the_old_path():
    # Until #201 serves the marts from the core, they are tables of staging that every run builds
    # whole: each intermediate model that reads the data of an account from staging leaves out an
    # account that the checks hold, as int_accounts__current does for the core.
    exempt = {
        "int_g1_failures",
        "int_accounts__current",
        # Their accounts are those of int_accounts__current.
        "int_campaigns__current",
        "int_ad_groups__current",
        "int_ads__current",
        # The conversion actions and the rates are not the data of an account.
        "int_conversion_actions__resolved",
        "int_fx_rates__filled",
    }
    platforms = (*SHAPED_ADS, "demo_web", "google_ads")
    filtered = 0
    for path in sorted(INTERMEDIATE.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        reads = [
            name
            for name in referenced(text)
            if name.startswith(tuple(f"stg_{p}__" for p in platforms))
        ]
        if path.stem in exempt or not reads:
            continue
        unions = text.count("as unioned\n")
        filters = text.count("{{ not_in_quarantine(ref('int_g1_failures'), ")
        assert filters >= 1, path.stem
        assert filters == max(unions, 1), path.stem
        filtered += 1
    assert filtered == 10


def test_g1a_and_the_stop_of_a_source_follow_adr_0035():
    # G1a against the core as the runs before left it, by load, with a threshold in the vars; the
    # stop by a filter in SQL, strictly more than half of the pinned accounts of the source, and a
    # warning, not a test at error, which would stop the healthy sources too (#188).
    project = PROJECT_FILE.read_text(encoding="utf-8")
    assert "\n  quarantine_spend_fall: 0.5\n" in project
    collapse = macro_text("quarantine_collapse_findings")
    assert "{%- if fall %}\n        or read < held * (1 - {{ var('quarantine_spend_fall') }})" in (
        collapse
    )
    assert "case when read_days = 0 then 'empty_window' else 'spend_fall' end" in collapse
    assert "measure='spend', checked='campaign_stats',\n    account='account_id', fall=true" in (
        collapse
    )
    assert "group by w.source, w.account_id, w.load_id" in collapse
    stop = macro_text("quarantine_stop_findings")
    assert "where status = 'pinned'" in stop
    assert "having h.accounts * 2 > count(*)" in stop
    sources = (STAGING / "loads" / "_loads__sources.yml").read_text(encoding="utf-8")
    tables = re.findall(r"^      - name: (\w+)\n", sources, flags=re.M)
    assert tables == [
        "loads",
        "dim_account",
        "ad_stats_campaign_daily",
        "web_sessions_daily",
        "fx_rates_daily",
    ]
    for name in ("assert_no_account_is_in_quarantine", "assert_no_source_is_stopped"):
        text = (DBT / "tests" / f"{name}.sql").read_text(encoding="utf-8")
        assert "{{ config(severity='warn') }}" in text, name


def test_the_journal_of_quarantine_keeps_the_first_finding_of_each_load():
    # A load stays pinned until the core accepts it, so every run checks it again: the merge takes
    # only the loads, accounts and reasons that the journal does not hold yet, with the run of dbt
    # that found them and when.
    text = (CORE / "account_quarantine.sql").read_text(encoding="utf-8")
    assert "from {{ ref('int_g1_failures') }}\n    where load_id is not null\n" in text
    assert "left join existing as q\n" in text
    assert text.rstrip().endswith("where q.source is null")
    assert "'{{ invocation_id }}'::uuid as invocation_id" in text
    assert "now()::timestamptz as found_at" in text
    ops = (DBT / "models" / "ops" / "ops_quarantine.sql").read_text(encoding="utf-8")
    assert referenced(ops) == ["int_g1_failures", "account_quarantine", "source:core.loads"]


FACT_MACROS = DBT / "macros" / "facts.sql"


def facts_macro(name: str) -> str:
    """The body of a macro of macros/facts.sql."""
    text = FACT_MACROS.read_text(encoding="utf-8")
    body = text[text.index(f"{{% macro {name}(") :]
    return body[: body.index("{%- endmacro %}")]


def facts_returned(name: str) -> object:
    """The value that a macro of macros/facts.sql returns, as returned() reads one of
    macros/quarantine.sql."""
    body = facts_macro(name)
    value = body[body.index("return(") + len("return(") : body.rindex(") -}}")]
    for variable, listed in re.findall(r"\{%- set (\w+) = (\[.*?\]) -%\}", body, flags=re.S):
        value = re.sub(rf"\b{variable}\b", listed, value)
    for jinja, python in (("true", "True"), ("false", "False"), ("none", "None")):
        value = re.sub(rf"\b{jinja}\b", python, value)
    return ast.literal_eval(value)


def fact_config(fact: str) -> tuple[str, str]:
    """The text of the config() call of the model of a fact, whose hooks hold ) }} of their own,
    and the text of the model after it."""
    text = (CORE / f"core_{fact}.sql").read_text(encoding="utf-8")
    end = text.index("\n) }}\n")
    return text[text.index("{{ config(\n") : end], text[end + len("\n) }}\n") :]


# The lower bound of the windows of a run, as SQL that PostgreSQL computes when a statement runs.
WINDOW_START = (
    "(select min(window_from) from core.loads where status = 'pinned' and outcome = 'loaded')"
)


def test_the_facts_of_the_core_replace_whole_days_of_the_windows():
    # ADR-0038 and R5 (#191): the standard delete+insert by account and day, with the lower bound
    # of the windows in incremental_predicates, into the table of the migration under its name,
    # and the hooks in the transaction of the model: the thaw first, the journal of restatements of
    # a fact of statistics, and the empty days last. No hook before_begin, which makes dbt commit
    # before it, so that the hooks and the write commit one by one, and no partition from a model:
    # the pin makes them.
    journals = {
        name.removesuffix("_restatements")
        for name in re.findall(
            r"^CREATE TABLE core\.(\w+_restatements) \(",
            (MIGRATIONS / "0006_create_the_journals.sql").read_text(encoding="utf-8"),
            flags=re.M,
        )
    }
    facts = fact_tables()
    assert len(facts) == 5
    assert journals < set(facts)
    grains = facts_returned("fact_grains")
    assert set(grains) == set(facts)
    for fact in facts:
        config, body = fact_config(fact)
        assert f"    alias='{fact}',\n" in config, fact
        assert "    incremental_strategy='delete+insert',\n" in config, fact
        assert "    unique_key=['account_sk', 'report_date'],\n" in config, fact
        predicate = (
            "    incremental_predicates=['DBT_INTERNAL_DEST.report_date >= ' ~ window_start()],\n"
        )
        assert predicate in config, fact
        assert "post_hook" not in config, fact
        assert "before_begin" not in config, fact
        [rows] = re.findall(r"^\{\{ (fact_\w+_rows\(.*\)) \}\}$", body, flags=re.M)
        hooks = re.findall(r'^        "\{\{ (\w+)\((.*)\) \}\}",$', config, flags=re.M)
        days = "ref('int_window_days')"
        wanted = [("fact_thaw", days)]
        if fact in journals:
            wanted.append(("fact_restatements", f"{rows}, {days}"))
        wanted.append(("fact_empty_days", f"{rows}, {days}"))
        assert hooks == wanted, fact
        # The grain of the fact: its key between the account and the day, its other IDs and its
        # measures, which with the currency and the load are its columns, and its journal.
        key, others, measures, journal = grains[fact]
        columns = ["account_sk", *key, *others, "report_date", *measures, "currency", "load_sk"]
        assert sorted(columns) == sorted(facts[fact]["columns"]), fact
        assert journal is (fact in journals), fact


def test_the_rows_of_a_fact_take_the_columns_and_the_types_of_its_table():
    # A model casts to the exact types of its table (ADR-0038): a wider type would make dbt drop
    # the column with cascade, which only the lack of the right stops, and on_schema_change fail
    # stops a model whose columns differ.
    facts = fact_tables()
    macros = {
        "ad_stats_campaign_daily": ("fact_stats_rows", ["campaign_id"]),
        "ad_stats_ad_group_daily": ("fact_stats_rows", ["campaign_id", "ad_group_id"]),
        "ad_stats_ad_daily": ("fact_stats_rows", ["campaign_id", "ad_group_id", "ad_id"]),
        "ad_conversions_campaign_daily": ("fact_conversions_rows", []),
        "web_sessions_daily": ("fact_sessions_rows", []),
    }
    assert set(macros) == set(facts)
    cast = re.compile(r"^    cast\(.*? as ([\w(),]+)\) as (\w+|\{\{ id \}\}),?$", flags=re.M)
    for fact, (macro, ids) in macros.items():
        _, body = fact_config(fact)
        assert f"{{{{ {macro}(" in body, fact
        found = []
        for kind, column in cast.findall(facts_macro(macro)):
            found += [(id_, kind) for id_ in ids] if column == "{{ id }}" else [(column, kind)]
        assert found == list(facts[fact]["types"].items()), fact
        # The rows join the days of the windows on their key of one column.
        assert "on d.day_key = {{ window_day_key('r.platform', " in facts_macro(macro), fact


def test_the_window_of_the_facts_is_a_subquery_on_the_pinned_loads():
    # R5 (#191): dbt renders a config and the where of a test when it parses the project, with no
    # database, and refuses a macro of the project in the where of a test; a subquery on
    # core.loads in their text lets PostgreSQL prune the partitions when the statement runs.
    assert facts_macro("window_start").split("-%}", 1)[1].strip() == WINDOW_START
    text = (CORE / "_core__models.yml").read_text(encoding="utf-8")
    conditions = re.findall(r"^( +)where: >-\n((?:\1  .*\n)+)", text, flags=re.M)
    assert len(conditions) == 10, len(conditions)
    for _, lines in conditions:
        assert (
            " ".join(line.strip() for line in lines.splitlines())
            == f"report_date >= {WINDOW_START}"
        )
    # The bound of the freeze is that of the trigger of each fact, so that a fact lifts it when it
    # is to replace a row of a frozen month, as a load of history that comes again does.
    migration = (MIGRATIONS / "0005_create_the_facts.sql").read_text(encoding="utf-8")
    bounds = set(re.findall(r"WHEN \(OLD\.report_date < (.*)\)$", migration, flags=re.M))
    assert bounds == {facts_macro("frozen_before").split("-%}", 1)[1].strip()}
    thaw = facts_macro("fact_thaw")
    assert "select set_config('agoranomos.thaw', pg_current_xact_id()::text, true)" in thaw
    assert "where f.report_date < {{ frozen_before() }}" in thaw


def test_the_days_of_the_windows_leave_out_the_accounts_in_quarantine():
    # ADR-0035: a fact keeps the days of an account that the checks hold, and of a source that
    # stops, as they were: the one table of the days of the windows leaves them out, and every
    # model of a fact, its hooks and its tests read the days from it.
    text = (INTERMEDIATE / "int_window_days.sql").read_text(encoding="utf-8")
    assert referenced(text) == [
        "stg_loads__newest_days",
        "dim_account",
        "source:core.loads",
        "int_g1_failures",
    ]
    assert text.endswith(
        "where {{ not_in_quarantine(ref('int_g1_failures'), 'd.source', 'd.account_id') }}\n"
    )
    assert 'post_hook=["analyze {{ this }}"]' in text
    for fact in fact_tables():
        config, body = fact_config(fact)
        assert set(re.findall(r"ref\('(int_\w+)'\)", config + body)) == {"int_window_days"}, fact


def test_the_pin_makes_the_months_of_the_journals_of_restatements():
    # A model of a fact writes its journal with the moment of its transaction: the pin makes the
    # current month and the next of each journal, in UTC, before the tasks of dbt.
    text = PIN.read_text(encoding="utf-8")
    assert "{%- if fact_grains()[fact][3] -%}" in text
    assert """{%- do journals.append("('" ~ fact ~ "_restatements')") -%}""" in text
    assert (
        "cross join generate_series(\n    date_trunc('month', now() at time zone 'UTC'),\n"
        "    date_trunc('month', now() at time zone 'UTC') + interval '1 month',\n"
    ) in text


def test_g2_compares_every_fact_with_staging_and_its_entities_with_their_dimensions():
    # ADR-0035: the sums of staging and of the core by account and day over the windows, and the
    # share of the rows of a fact whose entity has no row in its dimension, after the write; both
    # are errors, which the tests of each fact run after it, in its task.
    assert {fact for _, fact, _ in G2_SUMS} == set(fact_tables())
    for what, fact, staged in G2_SUMS:
        test = (DBT / "tests" / f"assert_the_core_holds_the_{what}_of_staging.sql").read_text(
            encoding="utf-8"
        )
        assert f"{{{{ fact_sums_differ(ref('core_{fact}'), [" in test, what
        assert set(referenced(test)) == {f"core_{fact}", "int_window_days", *staged}, what
    text = (CORE / "_core__models.yml").read_text(encoding="utf-8")
    described = blocks(text, 2)
    dimensions = {
        "ad_stats_campaign_daily": ("[account_sk, campaign_id]", "dim_campaign"),
        "ad_stats_ad_group_daily": ("[account_sk, ad_group_id]", "dim_ad_group"),
        "ad_stats_ad_daily": ("[account_sk, ad_group_id, ad_id]", "dim_ad"),
        "ad_conversions_campaign_daily": ("[account_sk, campaign_id]", "dim_campaign"),
    }
    for fact in fact_tables():
        block = described[f"core_{fact}"]
        if fact in dimensions:
            columns, dimension = dimensions[fact]
            assert (
                "      - few_without_a_dimension:\n          arguments:\n"
                f"            columns: {columns}\n            to: ref('{dimension}')\n"
                f"            to_columns: {columns}\n"
            ) in block, fact
        else:
            assert "few_without_a_dimension" not in block, fact
    project = PROJECT_FILE.read_text(encoding="utf-8")
    assert "\n  dimensionless_share: 0.001\n" in project
