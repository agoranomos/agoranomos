"""Static checks of the dbt project in dbt/, for what Cosmos and the stack rely on, without dbt."""

import csv
import re
from pathlib import Path

DBT = Path(__file__).resolve().parents[2] / "dbt"
SEEDS = DBT / "seeds"
REPORTING = "{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}"


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
    # ADR-0025: until an AI role needs to tell them apart, intermediate models are views in staging.
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    pattern = r"^    intermediate:\n      \+schema: staging\n      \+materialized: view$"
    assert re.search(pattern, project, flags=re.MULTILINE)


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

    assert set(rows) == set(platforms.PLATFORMS)
    for code, platform in platforms.PLATFORMS.items():
        assert rows[code]["mimics"] == platform.mimics
        assert rows[code]["is_demo"] == "true"


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
# The staging models of the platforms of the demo (ADR-0025), by platform: each is one call of the
# macro of the shape of its API, over these raw tables of the platform, in this order.
SHAPED = {
    "demo_search": {
        "campaign_stats": ("google_ads_campaign_stats", ["campaign_stats"]),
        "campaign_conversions": ("google_ads_campaign_conversions", ["campaign_conversions"]),
        "accounts": ("google_ads_accounts", ["customer_clients"]),
        "campaigns": ("google_ads_campaigns", ["campaigns"]),
    },
    "demo_social": {
        "campaign_stats": ("meta_campaign_stats", ["insights_campaign"]),
        "campaign_conversions": (
            "meta_campaign_conversions",
            ["insights_campaign", "insights_campaign__actions", "insights_campaign__action_values"],
        ),
        "accounts": ("meta_accounts", ["ad_accounts"]),
        "campaigns": ("meta_campaigns", ["campaigns"]),
    },
    "demo_video": {
        "campaign_stats": ("tiktok_campaign_stats", ["report_campaign_daily"]),
        "campaign_conversions": ("tiktok_campaign_conversions", ["report_campaign_daily"]),
        "accounts": ("tiktok_accounts", ["advertisers"]),
        "campaigns": ("tiktok_campaigns", ["campaigns"]),
    },
    "demo_web": {
        "properties": ("ga4_properties", ["properties"]),
        "sessions_daily": ("ga4_sessions_daily", ["report_sessions_daily"]),
    },
    "demo_fx": {"rates_daily": ("euro_rates", ["rates_daily"])},
}
STATS_KEY = ["report_date", "platform", "account_id", "campaign_id"]
ACCOUNT_KEY = ["platform", "account_id"]
# The critical tests of each kind of staging model: its key, which unique_combination checks, the
# columns that not_null checks, and those that non_negative checks.
CRITICAL = {
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
# The key test of a model, with its columns in flow style.
UNIQUE = r"^      - unique_combination:\n          arguments:\n            columns: \[(.*)\]$"
# The columns of an account or a property that must be in a seed: the seed and its column.
IN_SEEDS = {
    "currency": ("seed_iso_4217_currencies", "currency_code"),
    "time_zone": ("seed_iana_time_zones", "time_zone"),
}
# The kinds of staging models whose tests read the seeds.
SEEDED = {"accounts", "properties"}


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


def test_each_staging_model_of_a_platform_is_one_call_of_the_shape_of_its_api():
    # With the code of the platform of its folder and the raw tables of its own dataset: a model
    # that passed the code of another platform would still pass its key tests. An account or a
    # property model also names the seeds that its tests read, so that they come first (below).
    from agoranomos_pipelines.demo import platforms

    assert set(SHAPED) == set(platforms.PLATFORMS)
    found = {path.relative_to(STAGING).as_posix() for path in STAGING.glob("demo_*/*.sql")}
    wanted = {f"{code}/stg_{code}__{model}.sql" for code in SHAPED for model in SHAPED[code]}
    assert found == wanted, sorted(found ^ wanted)
    for code, models in SHAPED.items():
        dataset = platforms.PLATFORMS[code].dataset
        for model, (macro, tables) in models.items():
            path = STAGING / code / f"stg_{code}__{model}.sql"
            comment, *body = path.read_text(encoding="utf-8").splitlines()
            relations = ", ".join(f"source('{dataset}', '{table}')" for table in tables)
            seeds = [seed for seed, _ in IN_SEEDS.values()] if model in SEEDED else []
            depends = [f"-- depends_on: {{{{ ref('{seed}') }}}}" for seed in seeds]
            assert comment.startswith(f"-- {path.name}: "), path.name
            assert body == [*depends, f"{{{{ {macro}({relations}, '{code}') }}}}"], path.name


def test_the_sources_of_a_platform_are_the_tables_that_its_load_writes():
    # dlt writes a table for each table of the shape of the platform, and a child table for each
    # list in it, such as insights_campaign__actions of Meta.
    from agoranomos_pipelines.demo import platforms, shapes

    for code, platform in platforms.PLATFORMS.items():
        written = set()
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
    # (TestBehavior.BUILD of Cosmos): no test of staging is a warning.
    for code, models in SHAPED.items():
        text = (STAGING / code / f"_{code}__models.yml").read_text(encoding="utf-8")
        assert "severity" not in text, code
        described = blocks(text, 2)
        assert set(described) == {f"stg_{code}__{model}" for model in models}, code
        for model in models:
            key, not_null, non_negative = CRITICAL[model]
            block = described[f"stg_{code}__{model}"]
            unique = re.findall(UNIQUE, block, flags=re.MULTILINE)
            assert [part.split(", ") for part in unique] == [key], (code, model)
            columns = blocks(block, 6)
            tested = {name: column_tests(column) for name, column in columns.items()}
            assert {c for c, tests in tested.items() if "not_null" in tests} == set(not_null), (
                code,
                model,
            )
            assert {c for c, tests in tested.items() if "non_negative" in tests} == set(
                non_negative
            ), (code, model)
            seeded = {c for c, tests in tested.items() if "relationships" in tests}
            assert seeded == (set(IN_SEEDS) if model in SEEDED else set())
            for column in seeded:
                seed, field = IN_SEEDS[column]
                target = rf"^ +to: ref\('{seed}'\)\n +field: {field}$"
                assert re.search(target, columns[column], flags=re.MULTILINE), (code, column)


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
    assert len(checked) >= 8, "the tests of the accounts and properties against the seeds"
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
TO_RATES = (["report_date", "currency"], "int_fx_rates__filled", ["rate_date", "currency"])
SESSIONS_KEY = CRITICAL["sessions_daily"][0]
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
    "int_fx_rates__filled": (["rate_date", "currency"], ["rate", "rate_source"], {}, []),
    "int_web_sessions__unioned": (SESSIONS_KEY, ["client_id", "currency"], {}, [TO_RATES]),
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


def test_the_singular_tests_of_the_model_are_errors_unless_a_row_can_be_right():
    # Clicks above impressions and an account of no client can be right, so they warn; primary
    # actions that do not add up to the conversions of a campaign cannot.
    wanted = {
        "assert_clicks_within_impressions": ({"int_ad_stats__unioned"}, True),
        "assert_every_account_has_a_client": ({"int_ad_accounts__unioned"}, True),
        "assert_primary_actions_add_up_to_the_conversions": (
            {"int_ad_conversions__unioned", "int_ad_stats__unioned"},
            False,
        ),
    }
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
