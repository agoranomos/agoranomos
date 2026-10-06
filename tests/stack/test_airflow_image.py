"""The Airflow image of the stack: built from airflow/Dockerfile and run by every Airflow service.

The checks of the rendered Compose files and of the reader of the pins need no running stack; the
checks inside the containers do. They print versions and names, never the environment, which holds
secrets.
"""

import json
import re

import pytest
from services import LONG_RUNNING, airflow_services
from stack_helpers import FILES, REPO_ROOT, NamesOnly, compose, docker

IMAGE = "agoranomos-airflow:local"
DBT = "/opt/airflow/dbt_venv/bin/dbt"
TELEMETRY = {
    "RUNTIME__DLTHUB_TELEMETRY": "false",
    "DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS": "False",
    "AIRFLOW__COSMOS__ENABLE_TELEMETRY": "False",
    "DO_NOT_TRACK": "1",
}
# The configuration providers of dlt: none of them reads Airflow Variables, which the Op role may
# create. Not telemetry, so a constant of its own.
DLT_PROVIDERS = {"PROVIDERS__ENABLE_AIRFLOW_SECRETS": "false"}
# The services that parse or run DAGs, and so need dlt, Cosmos and dbt.
USERS = [
    s for s in ("airflow-dag-processor", "airflow-scheduler", "airflow-worker") if s in LONG_RUNNING
]
# dlt builds its configuration providers in every process that reads its configuration, a task
# included, and adds the one that reads Airflow Variables when its setting enable_airflow_secrets
# is on (dlt 1.30.0, dlt/common/configuration/specs/config_providers_context.py). It needs no task
# for that, and no metadata database either, only an Airflow that it can import: with the setting
# on, a plain process gets the provider even when no database answers. So this probe, a plain
# process, gets what a task gets. It prints the names of the providers and the value of the setting.
DLT_PROBE = """
import json

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersConfiguration

settings = resolve_configuration(ConfigProvidersConfiguration())
names = [provider.name for provider in dlt.config.config_providers]
print(json.dumps({"providers": names, "enable_airflow_secrets": settings.enable_airflow_secrets}))
"""
# The providers that dlt keeps: its environment variables and its two TOML files, and none that
# reads Airflow Variables. A provider that a dlt update adds fails the test, so that someone reads
# what it reads.
DLT_PROVIDER_NAMES = ["Environment Variables", "secrets.toml", "config.toml"]
# The caches that Cosmos would keep in Airflow Variables, which the Op role may edit: the output of
# dbt ls, from which Cosmos renders the tasks of dbt and their arguments, and the YAML selectors of
# a project. Both off.
COSMOS_CACHES = {
    "AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS": "False",
    "AIRFLOW__COSMOS__ENABLE_CACHE_DBT_YAML_SELECTORS": "False",
}
# Cosmos 1.15.1 decides in DbtGraph whether to read and write each cache in a Variable
# (cosmos/dbt/graph.py), from its settings and the cache key that every DAG of Cosmos has. The
# probe asks a graph with a cache key, as the DAG processor builds one, so it gets the answer that
# a parse gets; a renamed setting or method fails it. It prints two flags.
COSMOS_PROBE = """
import json

from cosmos import ProjectConfig
from cosmos.dbt.graph import DbtGraph

graph = DbtGraph(project=ProjectConfig(), cache_identifier="probe")
print(json.dumps({
    "dbt_ls": graph.should_use_dbt_ls_cache(),
    "yaml_selectors": graph.should_use_yaml_selectors_cache(),
}))
"""


def pins_of(path):
    """The packages of a requirements file, name -> version, as pins() of the unit tests reads
    them: any extras, a comment or a marker after a pin, and no line that is not a pin with ==.
    Such a line fails here, rather than drop out of the check of the installed versions."""
    versions = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip().removesuffix("\\").strip()
        if not line or line.startswith("--"):
            continue
        requirement = line.split(";", 1)[0].strip()
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)(\[[^\]]+\])?==(\S+)", requirement)
        assert match, f"{path.name}: not pinned with ==: {requirement}"
        versions[match.group(1).lower()] = match.group(3)
    return versions


def pinned(name):
    """The versions that airflow/requirements.txt or airflow/dbt-requirements.in pins."""
    return pins_of(REPO_ROOT / "airflow" / name)


def test_the_pins_are_read_with_any_extras_and_a_line_that_is_no_pin_fails(tmp_path):
    # A pin that the reader dropped would leave the check of its installed version out, silently.
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        "# a comment\n\n"
        "dlt[postgres,parquet]==1.30.0  # two extras, and a comment\n"
        "Astronomer-Cosmos==1.15.1\n"
        "semver==3.1.0 ; python_version >= '3.10'\n",
        encoding="utf-8",
    )
    assert pins_of(requirements) == {
        "dlt": "1.30.0",
        "astronomer-cosmos": "1.15.1",
        "semver": "3.1.0",
    }
    for line in ("dlt>=1.30.0", "dlt", "dlt[postgres]"):
        requirements.write_text(f"{line}\n", encoding="utf-8")
        with pytest.raises(AssertionError, match="not pinned with =="):
            pins_of(requirements)


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


def image_environment():
    """The variables that the image itself sets, as docker image inspect shows them: apart from
    those that Compose gives a container. They hold no secret, and the dict prints its names
    only."""
    result = docker("image", "inspect", IMAGE, "--format", "{{json .Config.Env}}", check=False)
    assert result.returncode == 0, f"docker image inspect {IMAGE} exited with {result.returncode}"
    return NamesOnly(item.partition("=")[::2] for item in json.loads(result.stdout))


def missing(found, wanted):
    """The names of the wanted variables that found does not hold with their values."""
    return sorted(name for name, value in wanted.items() if found.get(name) != value)


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


@pytest.mark.parametrize("variant", sorted(FILES))
def test_compose_keeps_dlt_away_from_airflow_variables_in_every_airflow_service(variant):
    for name, service in airflow_services(variant).items():
        environment = service["environment"]
        assert {key: environment.get(key) for key in DLT_PROVIDERS} == DLT_PROVIDERS, name


@pytest.mark.parametrize("variant", sorted(FILES))
def test_compose_keeps_the_caches_of_cosmos_out_of_airflow_variables_in_every_airflow_service(
    variant,
):
    for name, service in airflow_services(variant).items():
        environment = service["environment"]
        assert {key: environment.get(key) for key in COSMOS_CACHES} == COSMOS_CACHES, name


@pytest.mark.parametrize("service", USERS)
def test_airflow_has_the_pinned_packages(service):
    # dlt, Cosmos, and the dependencies of dlt that the constraints of Airflow leave open.
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


@pytest.mark.parametrize("service", USERS)
def test_the_running_containers_keep_dlt_away_from_airflow_variables(service):
    for name, value in DLT_PROVIDERS.items():
        assert compose("exec", "-T", service, "printenv", name).stdout.strip() == value, name


@pytest.mark.parametrize("service", USERS)
def test_the_running_containers_keep_the_caches_of_cosmos_out_of_airflow_variables(service):
    for name, value in COSMOS_CACHES.items():
        result = compose("exec", "-T", service, "printenv", name, check=False)
        assert result.stdout.strip() == value, name


def test_the_image_itself_turns_telemetry_off():
    # A container gets the same variables from compose.yaml, so only the image shows that the
    # Dockerfile sets them.
    wrong = missing(image_environment(), TELEMETRY)
    assert not wrong, f"the image {IMAGE} does not set {wrong} as the Dockerfile does"


def test_the_image_itself_keeps_dlt_away_from_airflow_variables():
    wrong = missing(image_environment(), DLT_PROVIDERS)
    assert not wrong, f"the image {IMAGE} does not set {wrong} as the Dockerfile does"


def test_the_image_itself_keeps_the_caches_of_cosmos_out_of_airflow_variables():
    wrong = missing(image_environment(), COSMOS_CACHES)
    assert not wrong, f"the image {IMAGE} does not set {wrong} as the Dockerfile does"


@pytest.mark.parametrize("service", USERS)
def test_dlt_itself_builds_no_provider_that_reads_airflow_variables(service):
    # The pins above check the name and the value of the switch. This asks dlt what it builds: if
    # an update of dlt renamed or moved the switch, the provider would be back, and this would fail.
    result = compose("exec", "-T", service, "python", "-c", DLT_PROBE, check=False)
    assert result.returncode == 0, f"the probe of dlt exited with {result.returncode}"
    found = json.loads(result.stdout.strip().splitlines()[-1])
    assert found["enable_airflow_secrets"] is False, "dlt has enable_airflow_secrets on"
    assert found["providers"] == DLT_PROVIDER_NAMES, f"dlt has the providers {found['providers']}"


@pytest.mark.parametrize("service", USERS)
def test_cosmos_itself_keeps_no_cache_in_airflow_variables(service):
    # The pins above check the names and the values of the switches. This asks Cosmos what it does
    # with them: if an update of Cosmos renamed a setting, the cache would be back, and this would
    # fail.
    result = compose("exec", "-T", service, "python", "-c", COSMOS_PROBE, check=False)
    assert result.returncode == 0, f"the probe of Cosmos exited with {result.returncode}"
    found = json.loads(result.stdout.strip().splitlines()[-1])
    assert found == {"dbt_ls": False, "yaml_selectors": False}, f"Cosmos uses the caches {found}"
