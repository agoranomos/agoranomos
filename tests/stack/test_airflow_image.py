"""The Airflow image of the stack: built from airflow/Dockerfile and run by every Airflow service.

The checks of the rendered Compose files need no running stack; the checks inside the containers do.
They print versions and names, never the environment, which holds secrets.
"""

import re

import pytest
from services import LONG_RUNNING, airflow_services
from stack_helpers import FILES, REPO_ROOT, compose

IMAGE = "agoranomos-airflow:local"
DBT = "/opt/airflow/dbt_venv/bin/dbt"
TELEMETRY = {
    "RUNTIME__DLTHUB_TELEMETRY": "false",
    "DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS": "False",
    "AIRFLOW__COSMOS__ENABLE_TELEMETRY": "False",
    "DO_NOT_TRACK": "1",
}
# The services that parse or run DAGs, and so need dlt, Cosmos and dbt.
USERS = [
    s for s in ("airflow-dag-processor", "airflow-scheduler", "airflow-worker") if s in LONG_RUNNING
]


def pinned(name):
    """The versions that airflow/requirements.txt and airflow/dbt-requirements.in pin."""
    versions = {}
    for line in (REPO_ROOT / "airflow" / name).read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([a-z0-9-]+)(\[[a-z]+\])?==(\S+)", line.strip())
        if match:
            versions[match.group(1)] = match.group(3)
    return versions


def airflow_version():
    """The Airflow version in the tag of the FROM line of airflow/Dockerfile."""
    dockerfile = (REPO_ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    match = re.search(r"^FROM\s+apache/airflow:([0-9.]+)-", dockerfile, flags=re.MULTILINE)
    assert match, "the FROM line of airflow/Dockerfile has no Airflow version in its tag"
    return match.group(1)


def versions(service, python, *packages):
    code = "import importlib.metadata as m, sys; print(*(m.version(p) for p in sys.argv[1:]))"
    result = compose("exec", "-T", service, python, "-c", code, *packages)
    return dict(zip(packages, result.stdout.split(), strict=True))


@pytest.mark.parametrize("variant", sorted(FILES))
def test_every_airflow_service_runs_the_image_built_here(variant):
    for name, service in airflow_services(variant).items():
        assert service["image"] == IMAGE, name


@pytest.mark.parametrize("variant", sorted(FILES))
def test_only_airflow_init_builds_the_image(variant):
    # With a build in every service, Compose runs one build per service, in parallel, and each
    # puts the name of its service on the image as a label: which of these images keeps the tag
    # depends on timing. Compose would find a new image at every ./agoranomos up and recreate the
    # Airflow containers, but a second start must change nothing. The other services only run the
    # image, so they must never pull it: it is in no registry.
    services = airflow_services(variant)
    assert {name for name, service in services.items() if "build" in service} == {"airflow-init"}
    init = services["airflow-init"]
    assert init["pull_policy"] == "build"
    assert init["build"]["context"] == str(REPO_ROOT / "airflow")
    for name, service in services.items():
        if name != "airflow-init":
            assert service["pull_policy"] == "never", name


@pytest.mark.parametrize("variant", sorted(FILES))
def test_compose_turns_telemetry_off_for_every_airflow_service(variant):
    for name, service in airflow_services(variant).items():
        environment = service["environment"]
        assert {key: environment.get(key) for key in TELEMETRY} == TELEMETRY, name


@pytest.mark.parametrize("service", USERS)
def test_airflow_has_the_pinned_dlt_and_cosmos(service):
    wanted = {"apache-airflow": airflow_version(), **pinned("requirements.txt")}
    assert versions(service, "python", *wanted) == wanted


@pytest.mark.parametrize("service", USERS)
def test_dbt_runs_from_its_own_environment_only(service):
    wanted = pinned("dbt-requirements.in")
    assert versions(service, "/opt/airflow/dbt_venv/bin/python", *wanted) == wanted
    in_airflow = compose("exec", "-T", service, "python", "-c", "import dbt.version", check=False)
    assert in_airflow.returncode != 0, "dbt is importable in Airflow's environment"
    # Not any failure of python will do: dbt must be missing. The message holds no secret.
    assert "No module named 'dbt'" in in_airflow.stderr


@pytest.mark.parametrize("service", USERS)
def test_the_dbt_executable_of_the_image_runs(service):
    # dbt is not on the PATH of Airflow's environment, so the DAGs that are to run it call this
    # path. --help works offline; --version asks PyPI for the latest release.
    result = compose("exec", "-T", service, DBT, "--help", check=False)
    assert result.returncode == 0, f"{DBT} --help exited with {result.returncode}"
    assert result.stdout.startswith("Usage:"), result.stdout[:200]


@pytest.mark.parametrize("service", USERS)
def test_the_running_containers_have_telemetry_off(service):
    for name, value in TELEMETRY.items():
        assert compose("exec", "-T", service, "printenv", name).stdout.strip() == value, name
