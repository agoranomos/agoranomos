"""Static checks of the Airflow image: airflow/Dockerfile and the files that it installs (ADR-0002).

The stack tests check the built image; these check the files, without Docker.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AIRFLOW = ROOT / "airflow"
# The shape of the base image, never its tag or digest: Renovate updates both, and Python stays on
# the version of ADR-0012.
BASE_IMAGE = r"apache/airflow:[0-9]+\.[0-9]+\.[0-9]+-python3\.13@sha256:[0-9a-f]{64}"
LOCK_COMMAND = (
    "uv pip compile dbt-requirements.in --universal --python-version 3.13 --generate-hashes "
    "--output-file dbt-requirements.txt"
)
TELEMETRY = {
    "RUNTIME__DLTHUB_TELEMETRY": "false",
    "DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS": "False",
    "AIRFLOW__COSMOS__ENABLE_TELEMETRY": "False",
    "DO_NOT_TRACK": "1",
}


def pins(path: Path) -> dict[str, str]:
    """The packages of a requirements file, name -> version; every one must be pinned with ==."""
    found = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip().removesuffix("\\").strip()
        if not line or line.startswith("--"):
            continue
        requirement = line.split(";", 1)[0].strip()
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)(\[[^\]]+\])?==(\S+)", requirement)
        assert match, f"{path.name}: not pinned with ==: {requirement}"
        found[match.group(1).lower()] = match.group(3)
    return found


def dockerfile() -> str:
    return (AIRFLOW / "Dockerfile").read_text(encoding="utf-8")


def test_the_image_starts_from_the_pinned_official_image():
    images = re.findall(r"^FROM\s+(\S+)", dockerfile(), flags=re.MULTILINE)
    assert len(images) == 1, images
    assert re.fullmatch(BASE_IMAGE, images[0]), images[0]


def test_airflow_gets_dlt_and_cosmos_under_its_constraints():
    # Renovate updates their versions: the test fixes the names, and pins() requires the ==.
    requirements = AIRFLOW / "requirements.txt"
    assert set(pins(requirements)) == {"dlt", "astronomer-cosmos"}
    # dlt keeps the extra that its postgres destination needs.
    extra = r"^dlt\[[^\]]*\bpostgres\b[^\]]*\]=="
    assert re.search(extra, requirements.read_text(encoding="utf-8"), flags=re.MULTILINE)
    text = dockerfile()
    assert '"apache-airflow==${AIRFLOW_VERSION}"' in text  # pip fails rather than move Airflow
    assert '--constraint "${HOME}/constraints.txt"' in text
    assert "&& pip check" in text


def test_dbt_has_a_locked_environment_of_its_own():
    # Renovate moves the patch releases of dbt, and a new minor or major version is a decision
    # (ADR-0005): the test fixes the names, and requires the lock file to hold the version that the
    # .in file asks for.
    wanted = pins(AIRFLOW / "dbt-requirements.in")
    assert set(wanted) == {"dbt-core", "dbt-postgres"}
    locked = pins(AIRFLOW / "dbt-requirements.txt")
    assert {name: locked.get(name) for name in wanted} == wanted
    text = dockerfile()
    assert "python -m venv /opt/airflow/dbt_venv" in text
    assert "--require-hashes" in text
    assert "/opt/airflow/dbt_venv/bin/pip check" in text


def test_every_locked_package_has_a_hash():
    lines = (AIRFLOW / "dbt-requirements.txt").read_text(encoding="utf-8").splitlines()
    packages = [i for i, line in enumerate(lines) if line and not line.startswith((" ", "#"))]
    assert packages
    unhashed = [lines[i] for i in packages if not lines[i + 1].strip().startswith("--hash=sha256:")]
    assert not unhashed


def test_the_lock_file_names_the_command_that_made_it():
    # Renovate's pip-compile manager reruns the command from this header.
    header = (AIRFLOW / "dbt-requirements.txt").read_text(encoding="utf-8").splitlines()[:2]
    assert header[1].removeprefix("#").strip() == LOCK_COMMAND
    assert LOCK_COMMAND in (AIRFLOW / "dbt-requirements.in").read_text(encoding="utf-8")


def test_the_image_turns_telemetry_off():
    text = dockerfile()
    for name, value in TELEMETRY.items():
        assert re.search(rf"\b{name}={re.escape(value)}\b", text), name


def test_the_build_context_holds_only_what_the_dockerfile_copies():
    ignore = (AIRFLOW / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert [line for line in ignore if line and not line.startswith("#")] == [
        "*",
        "!requirements.txt",
        "!dbt-requirements.txt",
    ]
    copied = re.findall(r"^COPY\s+(\S+)\s", dockerfile(), flags=re.MULTILINE)
    assert sorted(copied) == ["dbt-requirements.txt", "requirements.txt"]
