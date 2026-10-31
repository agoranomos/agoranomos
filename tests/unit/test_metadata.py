"""Tests of metabase/metadata.py: the export with a stand-in for dbt, the overrides of a deployment,
and the plan and its application against the stand-in for the Metabase REST API."""

from __future__ import annotations

import ast
import copy
import json
import math
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import metabase_init as mi
import metadata as md
import pytest
from metabase_stand_in import (
    EXPORT,
    MART_COLUMNS,
    FakeClock,
    FakeMetabase,
    metadata_env,
    serve,
)
from metabase_stand_in import column as export_column

# What dbt ls printed for the demo project, a model and the source, cut short: the stand-in for dbt
# prints these lines, or the error of a project that dbt cannot read.
MODEL_LINE = {
    "resource_type": "model",
    "schema": "marts",
    "name": "ad_stats_campaign_daily",
    "description": "Advertising spend and results by day and campaign.",
    "meta": {"metabase": {"display_name": "Ad Statistics by Campaign and Day"}},
    "columns": {
        "report_date": {
            "name": "report_date",
            "description": "The day that the statistics cover.",
            "meta": {"metabase": {"display_name": "Report Date", "semantic_type": None}},
            "data_type": "date",
            "doc_blocks": ["doc.agoranomos.report_date"],
        },
        "spend": {
            "name": "spend",
            "description": "What the ad platform charged.",
            "meta": {
                "metabase": {
                    "display_name": "Spend",
                    "semantic_type": "type/Currency",
                    "currency": "USD",
                }
            },
        },
    },
}
SOURCE_LINE = {
    "resource_type": "source",
    "schema": "raw_demo_search",
    "name": "campaign_stats",
    "source_name": "raw_demo_search",
    "description": "The demo statistics as dlt loads them.",
    "meta": {},
    "columns": {},
}
DBT_ERROR = """\
14:08:22  [ERROR]: Encountered an error:
Parsing Error
  Error reading agoranomos: marts/_marts__models.yml - Runtime Error
    Syntax error near line 4
    ------------------------------
    1  | models:
    2  |   - name: ad_stats_campaign_daily
"""
MART = ("marts", "ad_stats_campaign_daily")
SECRETS = {"the admin password": "Secret-password-1", "the session": "session-from-login"}


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr(md, "clock", fake.monotonic)
    monkeypatch.setattr(md, "pause", fake.sleep)
    return fake


@pytest.fixture
def metabase(clock):
    """A Metabase that metabase-init has set up, with the mart in the warehouse."""
    state = FakeMetabase(clock)
    state.set_up("admin@example.com", SECRETS["the admin password"])
    state.mart_in_warehouse = True
    state.mart_known = True
    server = serve(state, "X-Metabase-Session")
    yield state
    server.shutdown()
    server.server_close()


def client(metabase: FakeMetabase) -> mi.Metabase:
    signed_in = mi.Metabase(metabase.url)
    signed_in.session = "session-from-login"
    return signed_in


FAKE_DBT = """#!{python}
import json, pathlib, sys
pathlib.Path({arguments!r}).write_text(json.dumps(sys.argv[1:]))
sys.stdout.write({printed!r})
sys.exit({code})
"""


def fake_dbt(folder: Path, lines: list[dict[str, Any]], code: int = 0, error: str = "") -> str:
    """A stand-in for dbt: it notes its arguments, prints the lines as dbt ls does, and exits with
    code, after the error on standard output, as a quiet dbt prints it."""
    script = folder / "dbt"
    printed = "".join(json.dumps(line) + "\n" for line in lines) + error
    arguments = str(folder / "arguments.json")
    text = FAKE_DBT.format(python=sys.executable, arguments=arguments, printed=printed, code=code)
    script.write_text(text, encoding="utf-8")
    script.chmod(0o755)
    return str(script)


def loaded(folder: Path, overrides: str | None = None, **files: Any) -> md.Metadata:
    env = metadata_env(folder, overrides=overrides, **files)
    return md.load(env["METADATA_EXPORT"], env["METADATA_OVERRIDES"])


def puts(metabase: FakeMetabase) -> list[tuple[str, Any]]:
    return [(path, body) for method, path, body in metabase.bodies if method == "PUT"]


# --- The export -----------------------------------------------------------------------------


def test_the_export_asks_dbt_ls_for_json_lines_only():
    command = md.export_command("/dbt", "/project", "/work")
    assert command[:4] == ["/dbt", "--quiet", "--no-use-colors", "ls"]
    pairs = dict(zip(command[4::2], command[5::2], strict=False))
    assert pairs["--project-dir"] == pairs["--profiles-dir"] == "/project"
    assert (pairs["--target-path"], pairs["--log-path"]) == ("/work/target", "/work/logs")
    assert command.count("--resource-type") == 2
    assert command[command.index("--output") + 1] == "json"
    assert command[-len(md.EXPORT_KEYS) :] == list(md.EXPORT_KEYS)


def test_the_export_keeps_the_models_and_sources_with_their_columns_in_order(tmp_path):
    output = tmp_path / "metadata.json"
    line = md.export(fake_dbt(tmp_path, [SOURCE_LINE, MODEL_LINE]), "/p", "/w", str(output))
    assert line == f"exported 1 model and 1 source table to {output}"
    document = json.loads(output.read_text(encoding="utf-8"))
    assert document["format"] == md.FORMAT
    [model] = document["models"]
    assert (model["schema"], model["name"]) == MART
    assert [c["name"] for c in model["columns"]] == ["report_date", "spend"]
    assert model["columns"][1]["meta"]["metabase"]["currency"] == "USD"
    assert "data_type" not in model["columns"][0]  # the export keeps what the jobs read
    [source] = document["sources"]
    assert (source["source"], source["name"]) == ("raw_demo_search", "campaign_stats")
    assert json.loads((tmp_path / "arguments.json").read_text())[:3] == [
        "--quiet",
        "--no-use-colors",
        "ls",
    ]


def test_each_export_keeps_the_one_before_it(tmp_path):
    output = tmp_path / "metadata.json"
    md.write_export({"format": 1, "models": [], "sources": [], "n": 1}, output)
    md.write_export({"format": 1, "models": [], "sources": [], "n": 2}, output)
    assert json.loads(output.read_text())["n"] == 2
    assert json.loads(md.previous_path(output).read_text())["n"] == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == ["metadata.json", "metadata.previous.json"]


def test_an_export_that_did_not_change_keeps_the_one_before_it(tmp_path):
    # A start after an upgrade can stop before metabase-init applies anything, as when an override
    # names a column that the release renamed: the start after the fix exports the same again, and
    # the previous export must still be the release before, or its improvements would read as
    # reverts of edits.
    output = tmp_path / "metadata.json"
    md.write_export({"format": 1, "models": [], "sources": [], "n": 1}, output)
    md.write_export({"format": 1, "models": [], "sources": [], "n": 2}, output)
    md.write_export({"format": 1, "models": [], "sources": [], "n": 2}, output)
    assert json.loads(output.read_text())["n"] == 2
    assert json.loads(md.previous_path(output).read_text())["n"] == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == ["metadata.json", "metadata.previous.json"]


def test_a_project_that_dbt_cannot_read_ends_in_one_line_with_the_error_of_dbt(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr(md, "DBT", fake_dbt(tmp_path, [], code=2, error=DBT_ERROR))
    monkeypatch.setattr(md, "EXPORT", str(tmp_path / "metadata.json"))
    assert md.main(["export"]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err.splitlines() == [
        f"metadata-export: dbt could not read the project in {md.PROJECT} (dbt ls exited with 2): "
        "Parsing Error: Error reading agoranomos: marts/_marts__models.yml - Runtime Error: "
        "Syntax error near line 4"
    ]
    assert not (tmp_path / "metadata.json").exists()


def test_a_dbt_that_prints_no_error_is_named_too():
    assert md.dbt_error("") == "dbt printed no error"
    assert md.dbt_error("[ERROR]: Encountered an error:\n\n") == "dbt printed no error"


def test_a_dbt_that_cannot_run_is_one_line(tmp_path):
    with pytest.raises(md.MetadataError, match=r"^cannot run dbt ls \(.+\): FileNotFoundError$"):
        md.export(str(tmp_path / "no-dbt"), "/p", "/w", str(tmp_path / "metadata.json"))


def test_a_folder_that_the_job_cannot_write_names_the_owner_it_needs(tmp_path):
    # A new named volume belongs to root unless the folder in the image belongs to the user of the
    # job (airflow/Dockerfile): this is the line that the job then prints.
    folder = tmp_path / "volume"
    folder.mkdir()
    folder.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        with pytest.raises(md.MetadataError, match=r"PermissionError\. The folder must belong"):
            md.export(fake_dbt(tmp_path, [MODEL_LINE]), "/p", "/w", str(folder / "metadata.json"))
    finally:
        folder.chmod(stat.S_IRWXU)


def test_the_export_service_prints_one_line(tmp_path, monkeypatch, capsys):
    # The service exports the metadata and copies the manifest into the volume of MANIFEST.
    export, manifest = tmp_path / "metadata.json", tmp_path / "manifest.json"
    monkeypatch.setattr(md, "DBT", fake_dbt_with_manifest(tmp_path, [MODEL_LINE], MANIFEST_TEXT))
    monkeypatch.setattr(md, "WORK", str(tmp_path / "work"))
    monkeypatch.setattr(md, "EXPORT", str(export))
    monkeypatch.setattr(md, "MANIFEST", str(manifest))
    assert md.main(["export"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        f"metadata-export: exported 1 model and 0 source tables to {export}, and the manifest of "
        f"the dbt project to {manifest}"
    ]
    assert manifest.read_text(encoding="utf-8") == MANIFEST_TEXT
    assert md.main([]) == 2
    assert md.main(["diff"]) == 2


FAKE_DBT_WITH_MANIFEST = """#!{python}
import json, pathlib, sys
arguments = sys.argv[1:]
pathlib.Path({arguments_file!r}).write_text(json.dumps(arguments))
target = pathlib.Path(arguments[arguments.index("--target-path") + 1])
target.mkdir(parents=True, exist_ok=True)
manifest = {manifest!r}
if manifest is not None:
    (target / "manifest.json").write_text(manifest)
sys.stdout.write({printed!r})
"""


def fake_dbt_with_manifest(folder: Path, lines: list[dict[str, Any]], manifest: str | None) -> str:
    """A stand-in for dbt ls that, as dbt 1.12.5 does, writes manifest.json into its target folder;
    with manifest None it writes none."""
    script = folder / "dbt"
    printed = "".join(json.dumps(line) + "\n" for line in lines)
    script.write_text(
        FAKE_DBT_WITH_MANIFEST.format(
            python=sys.executable,
            arguments_file=str(folder / "arguments.json"),
            manifest=manifest,
            printed=printed,
        ),
        encoding="utf-8",
    )
    script.chmod(0o755)
    return str(script)


MANIFEST_TEXT = json.dumps(
    {"metadata": {"dbt_version": "1.12.5"}, "nodes": {"model.agoranomos.x": {}}}
)


def test_the_export_copies_the_manifest_that_dbt_ls_wrote(tmp_path):
    work, output, manifest = (
        tmp_path / "work",
        tmp_path / "metadata.json",
        tmp_path / "manifest.json",
    )
    dbt = fake_dbt_with_manifest(tmp_path, [MODEL_LINE], MANIFEST_TEXT)
    line = md.export(dbt, "/p", str(work), str(output), str(manifest))
    assert manifest.read_text(encoding="utf-8") == MANIFEST_TEXT
    assert line.endswith(f"and the manifest of the dbt project to {manifest}")
    assert not list(tmp_path.glob(".manifest.json.tmp")), "the copy left its temporary file"


def test_a_dbt_that_wrote_no_manifest_is_one_line(tmp_path):
    dbt = fake_dbt_with_manifest(tmp_path, [MODEL_LINE], None)
    with pytest.raises(md.MetadataError, match=r"^dbt ls wrote no manifest into .*target"):
        md.export(
            dbt,
            "/p",
            str(tmp_path / "work"),
            str(tmp_path / "metadata.json"),
            str(tmp_path / "m.json"),
        )


@pytest.mark.parametrize("text", ["not json", json.dumps(["a list"]), json.dumps({"metadata": {}})])
def test_a_manifest_that_is_not_one_is_refused_and_the_old_one_stays(tmp_path, text):
    old = tmp_path / "manifest.json"
    old.write_text(MANIFEST_TEXT, encoding="utf-8")
    dbt = fake_dbt_with_manifest(tmp_path, [MODEL_LINE], text)
    with pytest.raises(md.MetadataError, match="manifest"):
        md.export(dbt, "/p", str(tmp_path / "work"), str(tmp_path / "metadata.json"), str(old))
    assert old.read_text(encoding="utf-8") == MANIFEST_TEXT


def test_the_export_service_names_the_manifest_of_the_volume():
    assert md.MANIFEST == "/opt/agoranomos/manifest/manifest.json"


# --- The effective metadata -------------------------------------------------------------------


def test_the_base_is_the_marts_of_the_export(tmp_path):
    marts = loaded(tmp_path)
    assert list(marts.effective) == [MART]  # staging is not shown in Metabase
    table = marts.effective[MART]
    assert (table.display_name, table.description) == (
        "Ad Statistics by Campaign and Day",
        "Advertising spend and results by day and campaign.",
    )
    by_name = {column.name: column for column in table.columns}
    assert [column.name for column in table.columns][:2] == ["report_date", "account_id"]
    assert by_name["report_date"].semantic_type is None
    assert (by_name["spend"].semantic_type, by_name["spend"].currency) == ("type/Currency", "USD")


@pytest.mark.parametrize(
    ("files", "message"),
    [
        pytest.param(
            {"export": None}, "is missing: see ./agoranomos logs metadata-export", id="missing"
        ),
        pytest.param({"export": {"format": 2}}, "is not of format 1", id="format"),
    ],
)
def test_an_export_that_cannot_be_used_is_one_line(tmp_path, files, message):
    with pytest.raises(md.MetadataError, match=message):
        loaded(tmp_path, **files)


def test_an_export_that_is_not_json_is_named(tmp_path):
    env = metadata_env(tmp_path)
    Path(env["METADATA_EXPORT"]).write_text("{not json", encoding="utf-8")
    with pytest.raises(md.MetadataError, match=r"is not JSON: run \./agoranomos up"):
        md.load(env["METADATA_EXPORT"], env["METADATA_OVERRIDES"])


def test_without_an_overrides_file_the_base_is_the_effective_metadata(tmp_path):
    marts = loaded(tmp_path)
    assert marts.effective == md.base_tables(EXPORT)


def test_display_name_and_description_replace_the_base_and_a_note_follows_it(tmp_path):
    marts = loaded(
        tmp_path,
        overrides="""
[marts.ad_stats_campaign_daily]
display_name = "Ad Spend, Daily"

[marts.ad_stats_campaign_daily.columns.conversions]
display_name = "Purchases"
note = "In this business, a conversion is a paid order in the online shop."

[marts.ad_stats_campaign_daily.columns.clicks]
description = "Clicks on the ads."
note = '''
A second line.
'''
""",
    )
    table = marts.effective[MART]
    by_name = {column.name: column for column in table.columns}
    assert table.display_name == "Ad Spend, Daily"
    assert table.description == "Advertising spend and results by day and campaign."
    assert by_name["conversions"].display_name == "Purchases"
    assert by_name["conversions"].description == (
        "The conversions of a row.\n\n"
        "In this business, a conversion is a paid order in the online shop."
    )
    # A text over several lines loses the blanks around it.
    assert by_name["clicks"].description == "Clicks on the ads.\n\nA second line."
    assert by_name["spend"] == md.base_tables(EXPORT)[MART].columns[7]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        pytest.param(
            "[staging.stg_demo_search__campaign_stats]\ndisplay_name = 'X'\n",
            r"overrides\.toml: \[staging\]: overrides are for the tables of marts",
            id="schema",
        ),
        pytest.param(
            "[marts.ad_spend_weekly]\ndisplay_name = 'X'\n",
            r"overrides\.toml: \[marts\.ad_spend_weekly\]: dbt has no such table in marts",
            id="table",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily.columns.purchases]\ndisplay_name = 'X'\n",
            r"\[marts\.ad_stats_campaign_daily\.columns\.purchases\]: dbt has no such column",
            id="column",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily]\nlabel = 'X'\n",
            r"marts\.ad_stats_campaign_daily\.label: unknown property; use display_name, "
            r"description, note",
            id="property",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily.columns.spend]\nsemantic_type = 'type/Price'\n",
            r"spend\.semantic_type: unknown property",
            id="semantic-type",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily.columns.spend]\ndisplay_name = 42\n",
            r"spend\.display_name: a text in quotes is expected, not int",
            id="number",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily.columns.spend]\ndisplay_name = true\n",
            r"spend\.display_name: a text in quotes is expected, not bool",
            id="boolean",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily.columns.spend]\ndescription = '  '\n",
            r"spend\.description: the text is empty",
            id="blank",
        ),
        pytest.param(
            f"[marts.ad_stats_campaign_daily.columns.spend]\ndisplay_name = '{'x' * 101}'\n",
            r"spend\.display_name: the text has 101 characters, and it may have 100 at most",
            id="long",
        ),
        pytest.param(
            '[marts.ad_stats_campaign_daily.columns.spend]\ndisplay_name = "Two\\nlines"\n',
            r"spend\.display_name: a display name is one line",
            id="two-lines",
        ),
        pytest.param(
            "marts = 1\n", r"overrides\.toml: marts: a table of TOML is expected", id="kind"
        ),
        pytest.param(
            "[marts]\nad_stats_campaign_daily = 'X'\n",
            r"marts\.ad_stats_campaign_daily: a table of TOML is expected",
            id="table-kind",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily]\ncolumns = 'X'\n",
            r"marts\.ad_stats_campaign_daily\.columns: a table of TOML is expected",
            id="columns-kind",
        ),
        pytest.param(
            "[marts.ad_stats_campaign_daily\n",
            r"overrides\.toml: the file is not valid TOML: .*\(at line 1, column \d+\)",
            id="toml",
        ),
    ],
)
def test_a_mistake_in_the_overrides_is_one_line_that_says_where(tmp_path, overrides, message):
    with pytest.raises(md.MetadataError, match=message) as raised:
        loaded(tmp_path, overrides=overrides)
    assert "\n" not in str(raised.value)


def test_an_override_of_a_column_that_a_release_renamed_says_what_to_fix(tmp_path):
    # A release that renames or removes a column leaves the deployment's entry for it behind:
    # every start stops at it, so its line names the entry and why, to fix in the file.
    with pytest.raises(md.MetadataError) as raised:
        loaded(tmp_path, overrides="[marts.ad_stats_campaign_daily.columns.cost]\nnote = 'Ours.'\n")
    assert str(raised.value) == (
        "overrides.toml: [marts.ad_stats_campaign_daily.columns.cost]: dbt has no such column in "
        "marts.ad_stats_campaign_daily"
    )


def test_two_columns_may_not_share_a_display_name(tmp_path):
    with pytest.raises(md.MetadataError) as raised:
        loaded(
            tmp_path,
            overrides=(
                "[marts.ad_stats_campaign_daily.columns.conversions]\ndisplay_name = 'CLICKS'\n"
            ),
        )
    assert str(raised.value) == (
        "marts.ad_stats_campaign_daily: the columns clicks and conversions have the same display "
        "name 'CLICKS'"
    )


def test_an_override_in_another_language_is_kept_and_logged_as_written(tmp_path, metabase):
    # End to end: metabase-init runs as its container runs it, with its output a pipe, and the
    # words of the deployment reach Metabase and the log as they are written, not escaped.
    overrides = "[marts.ad_stats_campaign_daily.columns.spend]\ndisplay_name = 'Расход'\n"
    env = {
        "METABASE_URL": metabase.url,
        "METABASE_SITE_NAME": "agoranomos",
        "METABASE_ADMIN_EMAIL": "admin@example.com",
        "METABASE_ADMIN_PASSWORD": SECRETS["the admin password"],
        "WAREHOUSE_HOST": "warehouse-db",
        "WAREHOUSE_PORT": "5432",
        "WAREHOUSE_DB": "warehouse",
        "WAREHOUSE_USER": "metabase_reader",
        "WAREHOUSE_PASSWORD": "reader-password",
        # No registry of the content yet, as before the first start of demo-init.
        "CONTENT_REGISTRY": str(tmp_path / "content.json"),
        **metadata_env(tmp_path, overrides=overrides),
    }
    result = subprocess.run(
        [sys.executable, str(Path(mi.__file__))],
        env=env,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0
    assert metabase.field("spend")["display_name"] == "Расход"
    printed = result.stdout.decode("utf-8").splitlines()
    assert (
        "metabase-init: update from code: marts.ad_stats_campaign_daily.spend display_name: "
        '"Spend" -> '
        '"Расход"'
    ) in printed


def test_a_file_with_a_byte_order_mark_is_read(tmp_path):
    env = metadata_env(tmp_path)
    Path(env["METADATA_OVERRIDES"]).write_bytes(
        b"\xef\xbb\xbf[marts.ad_stats_campaign_daily]\ndisplay_name = 'Ours'\n"
    )
    assert (
        md.load(env["METADATA_EXPORT"], env["METADATA_OVERRIDES"]).effective[MART].display_name
        == "Ours"
    )


def test_a_file_that_is_not_utf_8_is_named(tmp_path):
    env = metadata_env(tmp_path)
    Path(env["METADATA_OVERRIDES"]).write_bytes("display_name = 'Dépense'\n".encode("latin-1"))
    with pytest.raises(md.MetadataError, match=r"^overrides\.toml: the file is not UTF-8$"):
        md.load(env["METADATA_EXPORT"], env["METADATA_OVERRIDES"])


@pytest.mark.parametrize(
    "nested",
    [
        pytest.param("a = " + "[" * 100_000 + "]" * 100_000, id="arrays"),
        pytest.param("a = " + "{b = " * 40_000 + "1" + "}" * 40_000, id="inline-tables"),
    ],
)
def test_a_file_nested_too_deeply_is_one_line(tmp_path, nested):
    # tomllib reads a value in a value by recursion, so a deep nesting, well within the limit of
    # the size of the file, would end in a traceback of Python's recursion.
    assert len(nested) < md.OVERRIDES_LIMIT
    with pytest.raises(md.MetadataError) as raised:
        loaded(tmp_path, overrides=nested + "\n")
    assert str(raised.value) == (
        "overrides.toml: the file is not valid TOML: it is nested too deeply"
    )


def test_a_file_that_is_gone_when_it_is_read_is_no_file(tmp_path, monkeypatch):
    # A bind mount of Docker Desktop on macOS can show a file for a moment after it is deleted: its
    # size can be read, and then it cannot be opened. The file is gone, so the base applies.
    env = metadata_env(
        tmp_path, overrides="[marts.ad_stats_campaign_daily]\ndisplay_name = 'Ours'\n"
    )

    def gone(self: Path) -> bytes:
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr(Path, "read_bytes", gone)
    marts = md.load(env["METADATA_EXPORT"], env["METADATA_OVERRIDES"])
    assert marts.effective == md.base_tables(EXPORT)


def make_a_folder(path: Path) -> None:
    path.mkdir()


def make_an_unreadable_file(path: Path) -> None:
    path.write_text("[marts.ad_stats_campaign_daily]\ndisplay_name = 'Ours'\n", encoding="utf-8")
    path.chmod(0)


@pytest.mark.parametrize(
    ("make", "error"),
    [
        pytest.param(make_a_folder, "IsADirectoryError", id="folder"),
        pytest.param(
            make_an_unreadable_file,
            "PermissionError",
            id="unreadable",
            marks=pytest.mark.skipif(
                os.geteuid() == 0, reason="root reads a file whatever its permissions"
            ),
        ),
    ],
)
def test_an_overrides_path_that_cannot_be_read_is_one_line(tmp_path, make, error):
    # Only a file that is gone counts as no file: anything else at the path stops the job, as a
    # mistake in the file does, so that no override is dropped without a word.
    env = metadata_env(tmp_path)
    path = Path(env["METADATA_OVERRIDES"])
    make(path)
    try:
        with pytest.raises(md.MetadataError) as raised:
            md.load(env["METADATA_EXPORT"], env["METADATA_OVERRIDES"])
    finally:
        if path.is_file():
            path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    assert str(raised.value) == f"overrides.toml: cannot read the file: {error}"


def test_a_file_over_the_limit_is_refused_before_it_is_read(tmp_path):
    env = metadata_env(tmp_path)
    Path(env["METADATA_OVERRIDES"]).write_bytes(b"#" * (md.OVERRIDES_LIMIT + 1))
    with pytest.raises(md.MetadataError, match=r"has 262145 bytes, and it may have 262144 at most"):
        md.load(env["METADATA_EXPORT"], env["METADATA_OVERRIDES"])


def test_what_the_code_said_before_comes_from_the_previous_export(tmp_path):
    previous = copy.deepcopy(EXPORT)
    previous["models"][0]["description"] = "The description of the release before."
    marts = loaded(tmp_path, previous=previous)
    descriptions = [tables[MART].description for tables in marts.earlier]
    assert descriptions == [
        "Advertising spend and results by day and campaign.",
        "The description of the release before.",
        "The description of the release before.",
    ]
    first = tmp_path / "first-start"
    first.mkdir()
    assert loaded(first).earlier == (md.base_tables(EXPORT),)


# --- The plan and its application -----------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "shown"),
    [
        ("report_date", "Report Date"),
        ("account_id", "Account ID"),
        ("ctr", "Ctr"),
        ("CTR", "Ctr"),
        ("ad_stats_campaign_daily", "Ad Stats Campaign Daily"),
        ("stg_demo_search__campaign_stats", "Stg Demo Search Campaign Stats"),
    ],
)
def test_humanize_names_a_field_as_metabase_does(name, shown):
    # Metabase's simple strategy, the default, as its jar answered offline (Metabase 0.63.19; 0.64.1
    # has the same code).
    assert md.humanize(name) == shown


def test_the_first_apply_fills_every_declared_property(tmp_path, metabase):
    lines = md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "see the logs"))
    table = metabase.mart_table()
    assert (table["display_name"], table["description"]) == (
        "Ad Statistics by Campaign and Day",
        "Advertising spend and results by day and campaign.",
    )
    fields = {f["name"]: f for f in table["fields"]}
    assert fields["account_id"]["display_name"] == "Account ID"
    assert fields["spend"]["semantic_type"] == "type/Currency"
    assert fields["spend"]["settings"] == {"currency": "USD"}
    assert fields["report_date"]["semantic_type"] is None
    assert lines[0] == (
        'first fill: marts.ad_stats_campaign_daily display_name: "Ad Stats Campaign Daily" -> "Ad '
        'Statistics by Campaign and Day"'
    )
    assert all(line.startswith(md.FIRST) for line in lines), lines
    # One PUT for the table and one for each field.
    assert len(puts(metabase)) == 1 + len(MART_COLUMNS)


def test_a_second_apply_changes_nothing(tmp_path, metabase):
    marts = loaded(tmp_path)
    md.apply(client(metabase), 2, marts, md.waiter(120, "see the logs"))
    writes = len(metabase.bodies)
    assert md.apply(client(metabase), 2, marts, md.waiter(120, "see the logs")) == []
    assert len(metabase.bodies) == writes


def test_no_request_carries_null_but_to_clear_a_semantic_type(tmp_path, metabase):
    # Metabase 0.63.19 ignores a null display name, and 0.64.1 clears the name that a person gave
    # the field and shows the one of its sync, so the job never sends one: a property with no value
    # in the code is left out. The one null is the semantic type of a column that the code declares
    # as none, sent only when Metabase shows a type.
    metabase.field("report_date")["semantic_type"] = "type/CreationDate"
    md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "see the logs"))
    nulls = []
    for path, body in puts(metabase):
        assert body, path
        assert None not in (v for k, v in body.items() if k != "semantic_type"), path
        if "semantic_type" in body and body["semantic_type"] is None:
            nulls.append(path)
    assert len(nulls) == 1  # report_date; conversions shows no type, so nothing is sent for it


def test_the_settings_of_a_field_are_sent_whole(tmp_path, metabase):
    # PUT /api/field/:id replaces the whole map, so the settings that Metabase has stay.
    metabase.field("spend")["settings"] = {"decimals": 0, "currency": "EUR"}
    md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "see the logs"))
    assert metabase.field("spend")["settings"] == {"decimals": 0, "currency": "USD"}


def test_an_edit_in_metabase_is_reverted_and_its_line_shows_the_old_value(tmp_path, metabase):
    marts = loaded(tmp_path, previous=EXPORT)
    md.apply(client(metabase), 2, marts, md.waiter(120, "see the logs"))
    metabase.field("conversions")["display_name"] = "Leads"  # edited in the Metabase UI
    lines = md.apply(client(metabase), 2, marts, md.waiter(120, "see the logs"))
    assert lines == [
        'revert of an edit: marts.ad_stats_campaign_daily.conversions display_name: "Leads" -> '
        '"Conversions"'
    ]
    assert metabase.field("conversions")["display_name"] == "Conversions"


def test_a_change_of_the_code_is_an_update_and_no_revert(tmp_path, metabase):
    previous = copy.deepcopy(EXPORT)
    previous["models"][0]["columns"][7]["description"] = "The spend, as the release before put it."
    before, now = tmp_path / "before", tmp_path / "now"
    before.mkdir()
    now.mkdir()
    md.apply(client(metabase), 2, loaded(before, export=previous), md.waiter(120, "x"))
    lines = md.apply(client(metabase), 2, loaded(now, previous=previous), md.waiter(120, "x"))
    assert lines == [
        'update from code: marts.ad_stats_campaign_daily.spend description: "The spend, as the '
        "release "
        'before put it." -> "The spend of a row."'
    ]


def test_an_override_that_takes_over_from_the_base_is_an_update(tmp_path, metabase):
    md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "x"))
    marts = loaded(
        tmp_path, overrides="[marts.ad_stats_campaign_daily.columns.spend]\ndisplay_name = 'Cost'\n"
    )
    assert md.apply(client(metabase), 2, marts, md.waiter(120, "x")) == [
        'update from code: marts.ad_stats_campaign_daily.spend display_name: "Spend" -> "Cost"'
    ]


def test_a_semantic_type_that_metabase_reset_is_set_again(tmp_path, metabase):
    # A change of the type of a column makes Metabase reset its semantic type: every run sets it.
    marts = loaded(tmp_path)
    md.apply(client(metabase), 2, marts, md.waiter(120, "x"))
    metabase.field("spend")["semantic_type"] = None
    assert md.apply(client(metabase), 2, marts, md.waiter(120, "x")) == [
        'first fill: marts.ad_stats_campaign_daily.spend semantic_type: none -> "type/Currency"'
    ]


def test_a_semantic_type_that_the_code_declares_none_is_cleared(tmp_path, metabase):
    # null declares that the column has no type of its own, and the code wins: a type that Metabase
    # guessed, or that someone set in its UI, is cleared and logged; the next run changes nothing.
    metabase.field("report_date")["semantic_type"] = "type/CreationDate"
    marts = loaded(tmp_path)
    lines = md.apply(client(metabase), 2, marts, md.waiter(120, "x"))
    assert (
        "revert of an edit: marts.ad_stats_campaign_daily.report_date semantic_type: "
        '"type/CreationDate" -> none'
    ) in lines
    assert metabase.field("report_date")["semantic_type"] is None
    assert md.apply(client(metabase), 2, marts, md.waiter(120, "x")) == []


def test_a_column_that_does_not_declare_a_semantic_type_keeps_the_one_of_metabase(
    tmp_path, metabase
):
    # Only a declared null clears a type. A column whose meta has no semantic_type leaves its type
    # to Metabase, as the jobs leave everything that the code does not declare.
    export = copy.deepcopy(EXPORT)
    report_date = export["models"][0]["columns"][0]
    assert report_date["name"] == "report_date"
    del report_date["meta"]["metabase"]["semantic_type"]
    metabase.field("report_date")["semantic_type"] = "type/CreationDate"
    marts = loaded(tmp_path, export=export)
    lines = md.apply(client(metabase), 2, marts, md.waiter(120, "x"))
    assert metabase.field("report_date")["semantic_type"] == "type/CreationDate"
    assert not any("report_date semantic_type" in line for line in lines), lines
    path = f"/api/field/{metabase.field('report_date')['id']}"
    assert all("semantic_type" not in body for put, body in puts(metabase) if put == path)


PLATFORMS = ("platform", "platform_name")
# The target of the foreign key of the column platform, as dbt-metabase names it.
TARGET = {"fk_target_table": "marts.ad_platforms", "fk_target_field": "platform"}


def with_platforms(platform: dict[str, Any] | None = None) -> dict[str, Any]:
    """The export with the mart ad_platforms, whose key platform is the target of the column
    platform of the mart, or of what platform says."""
    export = copy.deepcopy(EXPORT)
    [column] = [c for c in export["models"][0]["columns"] if c["name"] == "platform"]
    column["meta"]["metabase"] |= platform or {"semantic_type": "type/FK", **TARGET}
    columns = [
        export_column("platform", "Platform Code", "type/PK"),
        export_column("platform_name", "Platform", "type/Category"),
    ]
    export["models"].append(
        {
            "schema": "marts",
            "name": "ad_platforms",
            "description": "",
            "meta": {},
            "columns": columns,
        }
    )
    return export


def test_a_foreign_key_points_at_the_key_of_its_target_and_an_edit_of_it_goes_back(
    tmp_path, metabase
):
    # Metabase joins a foreign key on its target, so the field of the column gets the semantic type
    # and the ID of the field of the target in one PUT, and its line names the target by its path.
    metabase.key_marts = {"ad_platforms": PLATFORMS}
    marts = loaded(tmp_path, export=with_platforms())
    lines = md.apply(client(metabase), 2, marts, md.waiter(120, "x"))
    key = next(f for f in metabase.other_marts["ad_platforms"]["fields"] if f["name"] == "platform")
    field = metabase.field("platform")
    assert (field["semantic_type"], field["fk_target_field_id"]) == ("type/FK", key["id"])
    assert key["semantic_type"] == "type/PK"
    assert [line for line in lines if "fk_target" in line] == [
        "first fill: marts.ad_stats_campaign_daily.platform fk_target: none -> "
        '"marts.ad_platforms.platform"'
    ]
    [(_, body)] = [(p, b) for p, b in puts(metabase) if p == f"/api/field/{field['id']}"]
    assert (body["semantic_type"], body["fk_target_field_id"]) == ("type/FK", key["id"])
    assert "fk_target" not in body
    assert md.apply(client(metabase), 2, marts, md.waiter(120, "x")) == []
    # A target that someone changed goes back; so does a type, and Metabase drops the target with
    # it, which the job sets again in the same PUT.
    other = next(
        f for f in metabase.other_marts["ad_platforms"]["fields"] if f["name"] != "platform"
    )
    field["fk_target_field_id"] = other["id"]
    assert md.apply(client(metabase), 2, marts, md.waiter(120, "x")) == [
        "revert of an edit: marts.ad_stats_campaign_daily.platform fk_target: "
        '"marts.ad_platforms.platform_name" -> "marts.ad_platforms.platform"'
    ]
    field |= {"semantic_type": "type/Category", "fk_target_field_id": None}
    lines = md.apply(client(metabase), 2, marts, md.waiter(120, "x"))
    assert len(lines) == 2, lines
    assert field["fk_target_field_id"] == key["id"]


def test_a_foreign_key_waits_for_a_target_that_metabase_does_not_show(tmp_path, metabase):
    # Without its target, Metabase would show a foreign key that joins nothing: the field keeps its
    # type, and a note says what waits.
    marts = loaded(tmp_path, export=with_platforms())
    lines = md.apply(client(metabase), 2, marts, md.waiter(120, "x"))
    assert metabase.field("platform")["semantic_type"] is None
    assert metabase.field("platform")["fk_target_field_id"] is None
    assert (
        "marts.ad_stats_campaign_daily.platform: its foreign key waits for "
        "marts.ad_platforms.platform, which Metabase does not show yet: a start after the dbt run "
        "that builds it applies the key"
    ) in lines


def test_a_foreign_key_names_the_key_of_another_mart(tmp_path):
    foreign = {"semantic_type": "type/FK"}
    for wrong in (
        foreign,
        TARGET,
        {**foreign, "fk_target_table": "marts.ad_platforms"},
        {**foreign, **TARGET, "fk_target_field": "platform_name"},
        {**foreign, **TARGET, "fk_target_table": "ad_platforms"},
        {**foreign, **TARGET, "fk_target_table": "marts.clients"},
        {**foreign, **TARGET, "fk_target_table": "marts.ad_stats_campaign_daily"},
    ):
        export = with_platforms({"semantic_type": "type/Category", **wrong})
        with pytest.raises(md.MetadataError, match=r"^marts\.ad_stats_campaign_daily\.platform: "):
            md.base_tables(export)


def test_hidden_tables_and_fields_get_their_metadata_and_stay_hidden(tmp_path, metabase):
    metabase.mart_hidden = True
    metabase.field("spend")["visibility_type"] = "sensitive"
    md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "x"))
    assert metabase.field("spend")["display_name"] == "Spend"
    assert metabase.field("spend")["visibility_type"] == "sensitive"
    assert metabase.mart_hidden is True
    assert all(query.get("include_hidden") == ["true"] for query in metabase.queries)
    for _, body in puts(metabase):
        assert "visibility_type" not in body


def test_a_mart_that_the_warehouse_has_and_metabase_does_not_show_is_synced_once(
    tmp_path, metabase, clock
):
    metabase.mart_known = False
    lines = md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "see the logs"))
    assert metabase.calls.count(("POST", "/api/database/2/sync_schema")) == 1
    assert clock.now >= metabase.sync_seconds  # it waited for the sync
    assert metabase.mart_table()["display_name"] == "Ad Statistics by Campaign and Day"
    assert lines
    assert not any("not in the warehouse" in line for line in lines)


def test_a_sync_is_awaited_until_metabase_shows_the_fields_of_the_mart(tmp_path, metabase, clock):
    # A sync of Metabase makes the row of a new table before its fields: the wait ends when the
    # fields are there, so that the metadata of every column is applied, and none is a note.
    metabase.mart_known = False
    metabase.fields_seconds = 15.0
    lines = md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "see the logs"))
    assert clock.now >= metabase.sync_seconds + metabase.fields_seconds
    assert metabase.field("spend")["semantic_type"] == "type/Currency"
    assert not any("does not show this column" in line for line in lines), lines


def test_a_table_that_metabase_shows_without_its_fields_is_waited_for(tmp_path, metabase, clock):
    # A sync of Metabase's own, such as the one that a new connection starts, can be between the
    # row of a table and its fields when the job looks: the job asks for a sync and waits for them.
    metabase.synced_at, metabase.fields_seconds = 0.0, 15.0
    lines = md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "see the logs"))
    assert ("POST", "/api/database/2/sync_schema") in metabase.calls
    assert clock.now >= metabase.fields_seconds
    assert metabase.field("spend")["semantic_type"] == "type/Currency"
    assert not any("does not show this column" in line for line in lines), lines


def test_a_column_that_the_warehouse_does_not_have_yet_needs_no_sync(tmp_path):
    # After a release that adds a column, the warehouse keeps the old table until dbt rebuilds the
    # mart. A table that Metabase shows with a field for every column that the warehouse has needs
    # no sync and no wait: otherwise every start would wait for a column that no sync can bring.
    marts = loaded(tmp_path)
    columns = [c.name for c in marts.effective[MART].columns if c.name != "conversions"]
    fields = [{"id": 90 + i, "name": name, "parent_id": None} for i, name in enumerate(columns)]
    table = {"id": 9, "schema": "marts", "name": "ad_stats_campaign_daily", "fields": fields}

    class Warehouse:
        def call(self, method: str, path: str, body: Any = None) -> Any:
            assert (method, path) == ("POST", "/api/dataset")
            rows = [["marts", "ad_stats_campaign_daily", name] for name in columns]
            return {"status": "completed", "data": {"rows": rows}}

    assert md.sync_needed(Warehouse(), 2, marts, {"tables": [table]}) == md.Sync(tables={})


def test_a_column_that_metabase_does_not_show_is_a_note_that_says_when_it_comes(tmp_path):
    # As after a release that adds or renames a column: the warehouse keeps the old table until dbt
    # rebuilds the mart, and Metabase shows the new column only after the sync that follows.
    marts = loaded(tmp_path)
    fields = [
        {"id": 90 + index, "name": column.name, "parent_id": None}
        for index, column in enumerate(marts.effective[MART].columns)
        if column.name != "conversions"
    ]
    table = {"id": 9, "schema": "marts", "name": "ad_stats_campaign_daily", "fields": fields}
    assert md.make_plan(marts, {"tables": [table]}).notes == (
        "marts.ad_stats_campaign_daily.conversions: Metabase does not show this column yet: it "
        "comes when dbt rebuilds the mart, and the start after that syncs it and applies its "
        "metadata",
    )


def test_a_mart_that_the_warehouse_does_not_have_is_a_note_and_no_wait(tmp_path, metabase, clock):
    # As with the demo off: dbt declares the mart, and no run built it.
    metabase.mart_known = False
    metabase.mart_in_warehouse = False
    lines = md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "see the logs"))
    assert lines == [
        "marts.ad_stats_campaign_daily: not in the warehouse yet; the start after the dbt run that "
        "builds it applies its metadata"
    ]
    assert ("POST", "/api/database/2/sync_schema") not in metabase.calls
    assert clock.now == 0
    assert puts(metabase) == []


def test_a_sync_that_does_not_show_the_mart_in_time_names_the_step(tmp_path, metabase, clock):
    metabase.mart_known = False
    metabase.sync_seconds = math.inf
    with pytest.raises(md.MetadataError) as raised:
        md.apply(
            client(metabase), 2, loaded(tmp_path), md.waiter(120, "see ./agoranomos logs metabase")
        )
    assert str(raised.value) == (
        "Metabase did not find marts.ad_stats_campaign_daily after a sync within 120 seconds: see "
        "./agoranomos logs metabase"
    )
    assert 120 <= clock.now < 120 + md.POLL_INTERVAL


def test_a_list_of_the_warehouse_that_fails_is_one_line_and_no_answer(tmp_path, metabase):
    metabase.mart_known = False
    metabase.failures[("POST", "/api/dataset")] = 200  # an echo with a success status
    with pytest.raises(
        md.MetadataError, match=r"^Metabase could not list the tables of the warehouse"
    ):
        md.apply(client(metabase), 2, loaded(tmp_path), md.waiter(120, "x"))


def test_the_plan_leaves_other_tables_inactive_ones_and_nested_fields_alone(tmp_path):
    marts = loaded(tmp_path)
    field = {"name": "spend", "display_name": "Spend", "description": None, "parent_id": None}
    database = {
        "tables": [
            {
                "id": 7,
                "schema": "marts",
                "name": "ad_stats_campaign_daily",
                "active": False,
                "fields": [],
            },
            {
                "id": 8,
                "schema": "marts",
                "name": "ad_stats_campaign_daily__dbt_backup",
                "fields": [],
            },
            {
                "id": 9,
                "schema": "marts",
                "name": "ad_stats_campaign_daily",
                "display_name": "Ad Statistics by Campaign and Day",
                "description": "Advertising spend and results by day and campaign.",
                "fields": [
                    {**field, "id": 90, "parent_id": 5},
                    {**field, "id": 91, "semantic_type": "type/Currency", "settings": {}},
                ],
            },
        ]
    }
    plan = md.make_plan(marts, database)
    assert [request.path for request in plan.requests] == ["/api/field/91"]
    assert plan.requests[0].body == {
        "description": "The spend of a row.",
        "settings": {"currency": "USD"},
    }
    assert len(plan.notes) == len(MART_COLUMNS) - 1  # the others: Metabase does not show them


@pytest.mark.parametrize(
    ("prop", "old", "earlier", "kind"),
    [
        ("display_name", None, set(), md.FIRST),
        ("description", "  ", set(), md.FIRST),
        ("display_name", "Spend", set(), md.FIRST),  # Metabase's own name for spend
        ("display_name", "spend", set(), md.FIRST),
        ("display_name", "Cost", {"Cost"}, md.UPDATE),
        (
            "display_name",
            "Spend",
            {"Spend"},
            md.UPDATE,
        ),  # the code gave it, though Metabase would too
        ("display_name", "Leads", {"Cost"}, md.REVERT),
        ("semantic_type", "type/Category", set(), md.FIRST),  # the analysis of Metabase gave it
        ("semantic_type", "type/Category", {"type/Category"}, md.UPDATE),
        ("semantic_type", "type/Quantity", set(), md.REVERT),
    ],
)
def test_the_kind_of_a_change_depends_on_what_metabase_showed(prop, old, earlier, kind):
    assert md.kind_of(prop, old, "spend", earlier) == kind


def test_a_value_in_a_line_stays_on_one_line_and_is_cut():
    assert md.shown("Two\nlines") == '"Two\\nlines"'
    long = md.shown("x" * 200)
    assert len(long) == 80
    assert long.endswith('..."')
    assert md.shown(None) == "none"


def test_the_waiter_gives_up_at_its_deadline(clock):
    wait = md.waiter(30, "see the logs")
    with pytest.raises(md.MetadataError, match=r"^the table did not come within 30 seconds: see"):
        wait(lambda: None, "the table did not come")
    assert 30 <= clock.now < 30 + md.POLL_INTERVAL
    assert md.waiter(30, "x")(lambda: "found", "never shown") == "found"


def test_the_module_imports_only_the_standard_library():
    # metadata.py runs in a plain python image and in the Airflow image, and it imports nothing of
    # metabase_init.py, which imports it.
    tree = ast.parse(Path(md.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])
    assert imported <= set(sys.stdlib_module_names), sorted(imported - set(sys.stdlib_module_names))
