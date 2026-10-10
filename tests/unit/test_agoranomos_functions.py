"""Tests of single functions of ./agoranomos, run in sh and in dash with the real definitions.

The script runs its command as it loads, so a test cannot source it whole. These tests cut the
definitions they need out of the script, so the code under test is the real one. CI runs on Ubuntu,
where /bin/sh is dash, and on macOS /bin/sh is bash in POSIX mode: both are covered.
"""

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = (REPO_ROOT / "agoranomos").read_text(encoding="utf-8")


def available_shells() -> list[str]:
    """/bin/sh, and dash when it is installed and /bin/sh is not already dash."""
    shells = ["/bin/sh"]
    dash = shutil.which("dash")
    if dash and Path(dash).resolve() != Path("/bin/sh").resolve():
        shells.append(dash)
    return shells


SHELLS = available_shells()


def shell_function(name: str) -> str:
    """The definition of a function of the script: one line, or up to the first line that is }."""
    lines = SCRIPT.splitlines()
    for start, line in enumerate(lines):
        if line.startswith(f"{name}() {{"):
            end = start if line.endswith("}") else lines.index("}", start)
            return "\n".join(lines[start : end + 1])
    raise AssertionError(f"./agoranomos defines no function {name}")


def run_env_functions(shell, body, *args, env_file, path=None):
    """Run body, with the functions that env_set needs and ENV_FILE set, in a shell with set -eu."""
    names = ("say", "warn", "error_line", "die", "paint", "sgr", "env_get", "env_set")
    program = "set -eu\n" + "\n".join(shell_function(name) for name in names) + "\n" + body
    return subprocess.run(
        [shell, "-c", program, "sh", *args],
        env={"PATH": path or os.environ["PATH"], "ENV_FILE": str(env_file)},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def put_first_on_path(directory: Path) -> str:
    return f"{directory}{os.pathsep}{os.environ['PATH']}"


def write_tool(directory: Path, name: str, body: str) -> None:
    directory.mkdir(exist_ok=True)
    tool = directory / name
    tool.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    tool.chmod(0o755)


@pytest.mark.parametrize("shell", SHELLS)
def test_env_set_and_env_get_round_trip_backslashes(shell, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("WINDOWS_PATH=old\nOTHER=1\n", encoding="utf-8")
    env_file.chmod(0o600)
    values = {"WINDOWS_PATH": r"C:\data\new\\x&y", "ESCAPE": r"a\nb"}
    for key, value in values.items():
        result = run_env_functions(
            shell, 'env_set "$1" "$2"; env_get "$1"', key, value, env_file=env_file
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout == value
    # One line per setting: a backslash and an n in a value must not start a new line.
    assert env_file.read_text(encoding="utf-8").splitlines() == [
        r"WINDOWS_PATH=C:\data\new\\x&y",
        "OTHER=1",
        r"ESCAPE=a\nb",
    ]
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".env.tmp.*"))


@pytest.mark.parametrize("shell", SHELLS)
def test_env_set_keeps_the_single_quotes_of_the_session_timeout_and_env_get_reads_the_json(
    shell, tmp_path
):
    # init --server writes METABASE_SESSION_TIMEOUT with env_set, in single quotes: Compose reads
    # the JSON between them, and env_get must too.
    env_file = tmp_path / ".env"
    env_file.write_text("OTHER=1\n", encoding="utf-8")
    env_file.chmod(0o600)
    json = '{"amount":24,"unit":"hours"}'
    result = run_env_functions(
        shell,
        "env_set METABASE_SESSION_TIMEOUT \"'$1'\"; env_get METABASE_SESSION_TIMEOUT",
        json,
        env_file=env_file,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == json
    assert env_file.read_text(encoding="utf-8").splitlines() == [
        "OTHER=1",
        f"METABASE_SESSION_TIMEOUT='{json}'",
    ]


# What Compose reads from a line of .env (compose-go 2.16.1, dotenv/parser.go): a value without
# quotes ends before " #" and loses the blanks at its end, the carriage return of a line of Windows
# among them; a value in quotes is what is between them.
ENV_LINES = [
    ("KEY=plain", "plain"),
    ("KEY=plain # a comment", "plain"),
    ("KEY=plain # a comment\r", "plain"),
    ("KEY=plain\r", "plain"),
    ("KEY=plain   ", "plain"),
    ("KEY=  after blanks", "after blanks"),
    ("KEY=  vivid leaf \r", "vivid leaf"),
    ("KEY=\fform feed and vertical tab\v", "form feed and vertical tab"),
    ("KEY=a#b", "a#b"),
    (
        "KEY=tab\t# no space before the hash, so no comment",
        "tab\t# no space before the hash, so no comment",
    ),
    ("KEY='in single # quotes'", "in single # quotes"),
    ('KEY="in double quotes"\r', "in double quotes"),
    ("KEY='$dollar and spaces '", "$dollar and spaces "),
    ("KEY=", ""),
    # Compose trims the blanks after the = before it looks for a comment, so a comment after an
    # empty value is the value (./agoranomos preflight stops on such a line).
    ("KEY= # a comment", "# a comment"),
    # The rule of the sessions of Metabase in server mode, which ./agoranomos init --server writes.
    ('KEY=\'{"amount":24,"unit":"hours"}\'', '{"amount":24,"unit":"hours"}'),
]


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(("line", "value"), ENV_LINES)
def test_env_get_reads_a_value_as_docker_compose_does(shell, tmp_path, line, value):
    env_file = tmp_path / ".env"
    env_file.write_bytes(f"OTHER=1\n{line}\nKEYS=not this key\n".encode())
    result = run_env_functions(shell, 'env_get "$1"', "KEY", env_file=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout == value


@pytest.mark.parametrize("shell", SHELLS)
def test_env_get_takes_the_last_line_that_sets_the_key_and_nothing_for_a_missing_one(
    shell, tmp_path
):
    env_file = tmp_path / ".env"
    env_file.write_text("KEY=first\nKEY=second # the one that counts\n", encoding="utf-8")
    result = run_env_functions(shell, 'env_get KEY; printf "|"; env_get MISSING', env_file=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "second|"


@pytest.mark.parametrize("shell", SHELLS)
def test_env_set_replaces_the_first_line_drops_duplicates_and_appends(shell, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("A=1\nB=2\nA=3\n# A=4\n", encoding="utf-8")
    body = 'env_set A one; env_set C three; env_set "$1" "$2"'
    result = run_env_functions(shell, body, "D", "", env_file=env_file)
    assert result.returncode == 0, result.stderr
    assert env_file.read_text(encoding="utf-8").splitlines() == [
        "A=one",
        "B=2",
        "# A=4",
        "C=three",
        "D=",
    ]


@pytest.mark.parametrize("shell", SHELLS)
def test_env_set_keeps_the_value_off_the_command_line(shell, tmp_path):
    # The arguments of a process can be read by other users (ps); its environment cannot.
    log = tmp_path / "awk-arguments"
    spy = tmp_path / "spy"
    write_tool(spy, "awk", f'printf "%s\\n" "$@" >> "{log}"\nexec "{shutil.which("awk")}" "$@"')
    env_file = tmp_path / ".env"
    env_file.write_text("PRIVATE_SETTING=old\n", encoding="utf-8")
    result = run_env_functions(
        shell,
        'env_set "$1" "$2"',
        "PRIVATE_SETTING",
        "value-that-must-stay-private",
        env_file=env_file,
        path=put_first_on_path(spy),
    )
    assert result.returncode == 0, result.stderr
    assert env_file.read_text(encoding="utf-8") == "PRIVATE_SETTING=value-that-must-stay-private\n"
    arguments = log.read_text(encoding="utf-8")
    assert arguments  # the spy did see the call
    assert "value-that-must-stay-private" not in arguments
    assert "PRIVATE_SETTING" not in arguments


@pytest.mark.parametrize("shell", SHELLS)
def test_env_set_refuses_a_value_with_a_line_break(shell, tmp_path):
    # .env keeps one setting on a line: a line break in a value would add a setting of its own.
    env_file = tmp_path / ".env"
    env_file.write_text("KEEP=1\n", encoding="utf-8")
    result = run_env_functions(
        shell, 'env_set "$1" "$2"', "KEEP", "2\nINJECTED=1", env_file=env_file
    )
    assert result.returncode == 1
    assert "KEEP" in result.stderr
    assert env_file.read_text(encoding="utf-8") == "KEEP=1\n"
    assert not list(tmp_path.glob(".env.tmp.*"))


@pytest.mark.parametrize("shell", SHELLS)
def test_env_set_cleans_up_and_names_the_file_when_awk_fails(shell, tmp_path):
    broken = tmp_path / "broken"
    write_tool(broken, "awk", "exit 2")
    env_file = tmp_path / ".env"
    env_file.write_text("KEEP=1\n", encoding="utf-8")
    result = run_env_functions(
        shell, 'env_set "$1" "$2"', "KEEP", "2", env_file=env_file, path=put_first_on_path(broken)
    )
    assert result.returncode == 1
    assert str(env_file) in result.stderr
    assert env_file.read_text(encoding="utf-8") == "KEEP=1\n"
    assert not list(tmp_path.glob(".env.tmp.*"))


@pytest.mark.parametrize("shell", SHELLS)
def test_problem_and_resource_problem_work_outside_preflight(shell, tmp_path):
    # A later command may call them without running cmd_preflight first.
    script, _, _ = SCRIPT.rpartition('\nmain "$@"')
    assert script != SCRIPT
    program = f'{script}\nproblem "one"\nresource_problem "two"\nprintf "%s" "$problems"\n'
    result = subprocess.run(
        [shell, "-c", program, "sh"],
        cwd=tmp_path,
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "2"
    assert "error: one" in result.stderr
    assert "error: two" in result.stderr


def run_with_script_functions(shell, body, *args, cwd):
    """Run body in a shell that has every function of the script and has not run any command."""
    script, _, _ = SCRIPT.rpartition('\nmain "$@"')
    assert script != SCRIPT
    return subprocess.run(
        [shell, "-c", f"{script}\n{body}\n", "sh", *args],
        cwd=cwd,
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def answers(shell, tmp_path, function, *args):
    """Whether a function that is a test (true or false) says yes to its arguments."""
    result = run_with_script_functions(
        shell, f'if {function} "$@"; then printf yes; else printf no; fi', *args, cwd=tmp_path
    )
    assert result.returncode == 0, result.stderr
    return result.stdout == "yes"


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("name", "valid"),
    [
        ("bi.example.com", True),
        ("a.b", True),
        ("BI.Example.COM", True),
        ("xn--bcher-kva.example", True),
        ("a" * 63 + ".example.com", True),
        ("bi.example." + "a" * 63, True),
        ("a" * 64 + ".example.com", False),
        ("bi.example." + "a" * 64, False),
        ("", False),
        ("localhost", False),
        ("203.0.113.7", False),
        ("foo.1com", False),
        ("-bad.example.com", False),
        ("bad-.example.com", False),
        ("under_score.example.com", False),
        (".example.com", False),
        ("example.com.", False),
        ("bi.example.com/metabase", False),
        ("bi.example.com:8443", False),
        ("https://bi.example.com", False),
        ("bi example.com", False),
        ("bi.example.com\r", False),
        # grep reads lines: a name on one line of several is still not a name.
        ("bi.example.com\nother.example.com", False),
        ("other\nbi.example.com", False),
        ("bi.example.com\n", False),
    ],
    ids=repr,
)
def test_valid_domain_takes_a_name_of_labels_of_63_characters_and_nothing_else(
    shell, tmp_path, name, valid
):
    assert answers(shell, tmp_path, "valid_domain", name) is valid


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("name", "public"),
    [
        ("bi.example.com", True),
        ("example.org", True),
        ("bi.localhost.example.com", True),
        ("notlocalhost.com", True),
        ("testing.com", True),
        ("my.home.arpa.example.com", True),
        ("localhost", False),
        ("bi.localhost", False),
        ("bi.test", False),
        ("bi.example", False),
        ("bi.invalid", False),
        ("bi.local", False),
        ("bi.internal", False),
        ("bi.home.arpa", False),
        ("home.arpa", False),
        # DNS names are not case-sensitive.
        ("Bi.LOCALHOST", False),
        ("LocalHost", False),
        ("a.Test", False),
        ("HOME.ARPA", False),
        ("Bi.Home.Arpa", False),
        ("Bi.Example.COM", True),
    ],
    ids=repr,
)
def test_public_name_refuses_the_names_that_no_public_authority_certifies(
    shell, tmp_path, name, public
):
    assert answers(shell, tmp_path, "public_name", name) is public


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("profiles", "added", "expected"),
    [
        ("core,demo", ["server"], "core,demo,server"),
        ("core,server", ["server"], "core,server"),
        ("server,core", ["server", "ops"], "server,core,ops"),
        ("core", ["server", "ops", "server"], "core,server,ops"),
        ("", ["server"], "server"),
        ("core,demo", [], "core,demo"),
        # A profile that only contains the name is another profile.
        ("serverx,core", ["server"], "serverx,core,server"),
        ("xserver", ["server"], "xserver,server"),
    ],
    ids=repr,
)
def test_with_profiles_adds_each_profile_once_at_the_end(
    shell, tmp_path, profiles, added, expected
):
    result = run_with_script_functions(shell, 'with_profiles "$@"', profiles, *added, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == expected


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("line", "server"),
    [
        (None, False),
        ("COMPOSE_PROFILES=core,demo", False),
        ("COMPOSE_PROFILES=", False),
        ("COMPOSE_PROFILES=core,demo,server", True),
        ("COMPOSE_PROFILES=server", True),
        ("COMPOSE_PROFILES=x,server,y", True),
        ('COMPOSE_PROFILES="core,server" # mine', True),
        ("COMPOSE_PROFILES=serverx,core", False),
        ("COMPOSE_PROFILES=core,xserver", False),
    ],
    ids=repr,
)
def test_server_mode_is_on_when_compose_profiles_lists_server(shell, tmp_path, line, server):
    if line is not None:
        (tmp_path / ".env").write_text(f"OTHER=1\n{line}\n", encoding="utf-8")
    assert answers(shell, tmp_path, "server_mode") is server


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("line", "ops"),
    [
        (None, False),
        ("COMPOSE_PROFILES=core,demo", False),
        ("COMPOSE_PROFILES=core,demo,server", False),
        ("COMPOSE_PROFILES=core,demo,server,ops", True),
        ("COMPOSE_PROFILES=ops", True),
        ('COMPOSE_PROFILES="core,ops" # mine', True),
        ("COMPOSE_PROFILES=opsx,core", False),
        ("COMPOSE_PROFILES=core,xops", False),
    ],
    ids=repr,
)
def test_ops_mode_is_on_when_compose_profiles_lists_ops(shell, tmp_path, line, ops):
    if line is not None:
        (tmp_path / ".env").write_text(f"OTHER=1\n{line}\n", encoding="utf-8")
    assert answers(shell, tmp_path, "ops_mode") is ops


LATEST = (
    "{\n"
    '  "result": "ok",\n'
    '  "attempted": "2026-10-11T03:00:12Z",\n'
    '  "step": "",\n'
    '  "last_ok": "20261011T030012Z",\n'
    '  "last_ok_epoch": 1791687612,\n'
    '  "last_scheduled_local": "2026-10-11 03:00"\n'
    "}\n"
)


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("result", "ok"),
        ("attempted", "2026-10-11T03:00:12Z"),
        ("step", ""),
        ("last_ok", "20261011T030012Z"),
        # A number has no quotes, and a last field has no comma.
        ("last_ok_epoch", "1791687612"),
        ("last_scheduled_local", "2026-10-11 03:00"),
        ("missing", ""),
        # A key that only starts with the name is another key.
        ("last", ""),
    ],
)
def test_json_field_reads_the_fields_that_the_backup_script_writes(shell, tmp_path, key, value):
    (tmp_path / "latest.json").write_text(LATEST, encoding="utf-8")
    result = run_with_script_functions(shell, 'json_field latest.json "$1"', key, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    # The caller reads it with $(...), which drops the newline.
    assert result.stdout.rstrip("\n") == value


@pytest.mark.parametrize("shell", SHELLS)
def test_json_field_of_a_file_that_is_not_there_prints_nothing_and_succeeds(shell, tmp_path):
    result = run_with_script_functions(shell, "json_field none.json result", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("shell", SHELLS)
def test_image_refs_lists_the_images_of_the_checkout_once_each_and_leaves_out_built_images(
    shell, tmp_path
):
    (tmp_path / "compose.yaml").write_text(
        "services:\n"
        "  a:\n"
        "    image: postgres:18.6-trixie@sha256:aaa\n"
        "  b:\n"
        "    image:   metabase/metabase:v0.64.1@sha256:bbb   # a comment\n"
        "  c:\n"
        "    image: postgres:18.6-trixie@sha256:aaa\n"
        "  d:\n"
        "    image: agoranomos-airflow:local\n",
        encoding="utf-8",
    )
    (tmp_path / "compose.celery.yaml").write_text(
        "services:\n  e:\n    image: valkey/valkey:9.1.2-trixie@sha256:ccc\n", encoding="utf-8"
    )
    (tmp_path / "airflow").mkdir()
    (tmp_path / "airflow" / "Dockerfile").write_text(
        "# FROM not/an-image:1\n"
        "FROM apache/airflow:3.3.2-python3.13@sha256:ddd AS base\n"
        "RUN true\n",
        encoding="utf-8",
    )
    (tmp_path / "caddy").mkdir()
    (tmp_path / "caddy" / "Dockerfile").write_text(
        "FROM caddy:2.11.7-alpine@sha256:eee\nRUN true\n", encoding="utf-8"
    )
    result = run_with_script_functions(shell, "image_refs", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "apache/airflow:3.3.2-python3.13@sha256:ddd",
        "caddy:2.11.7-alpine@sha256:eee",
        "metabase/metabase:v0.64.1@sha256:bbb",
        "postgres:18.6-trixie@sha256:aaa",
        "valkey/valkey:9.1.2-trixie@sha256:ccc",
    ]


@pytest.mark.parametrize("shell", SHELLS)
def test_image_refs_of_a_checkout_without_files_prints_nothing_and_succeeds(shell, tmp_path):
    result = run_with_script_functions(shell, "image_refs", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("line", "path"),
    [
        (None, "{root}/backups"),
        ("AGORANOMOS_BACKUP_DIR=./backups", "{root}/backups"),
        ("AGORANOMOS_BACKUP_DIR=./data/backups", "{root}/data/backups"),
        ("AGORANOMOS_BACKUP_DIR=other", "{root}/other"),
        ("AGORANOMOS_BACKUP_DIR=/srv/backups", "/srv/backups"),
        ("AGORANOMOS_BACKUP_DIR=", "{root}/backups"),
    ],
    ids=repr,
)
def test_backups_path_is_absolute_and_a_relative_path_starts_at_the_checkout(
    shell, tmp_path, line, path
):
    if line is not None:
        (tmp_path / ".env").write_text(f"{line}\n", encoding="utf-8")
    result = run_with_script_functions(shell, "backups_path", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    # The script finds its folder with pwd, which resolves a link such as /var on macOS.
    assert result.stdout == path.format(root=tmp_path.resolve())


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize(
    ("ids", "problem"),
    [
        ("AGORANOMOS_UID=1000\nAGORANOMOS_GID=1000", ""),
        ("AGORANOMOS_UID=1000\nAGORANOMOS_GID=0", ""),
        ("AGORANOMOS_UID=0\nAGORANOMOS_GID=1000", "AGORANOMOS_UID in .env is 0"),
        ("AGORANOMOS_UID=0\nAGORANOMOS_GID=auto", "AGORANOMOS_UID in .env is 0"),
        ("AGORANOMOS_UID=auto\nAGORANOMOS_GID=auto", "must be numbers"),
        ("AGORANOMOS_UID=1000\nAGORANOMOS_GID=auto", "must be numbers"),
        ("AGORANOMOS_UID=1000\nAGORANOMOS_GID=", "must be numbers"),
        ("AGORANOMOS_UID=\nAGORANOMOS_GID=1000", "must be numbers"),
        ("AGORANOMOS_UID=1000", "must be numbers"),
        ("OTHER=1", "must be numbers"),
        ("AGORANOMOS_UID=-1\nAGORANOMOS_GID=1000", "must be numbers"),
        ("AGORANOMOS_UID=10x0\nAGORANOMOS_GID=1000", "must be numbers"),
    ],
    ids=repr,
)
def test_backup_user_problem_names_root_and_anything_that_is_not_a_number(
    shell, tmp_path, ids, problem
):
    (tmp_path / ".env").write_text(f"{ids}\n", encoding="utf-8")
    result = run_with_script_functions(shell, "backup_user_problem", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert (problem in result.stdout) if problem else (result.stdout == "")
