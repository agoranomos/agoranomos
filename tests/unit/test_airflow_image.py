"""Static checks of the Airflow image: airflow/Dockerfile and the files that it installs (ADR-0002).

The stack tests check the built image; these check the files, without Docker.
"""

import re
import shlex
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
AIRFLOW = ROOT / "airflow"
# The shape of the base image, never its tag or digest: Renovate updates both, and Python stays on
# the version of ADR-0012.
BASE_IMAGE = r"apache/airflow:[0-9]+\.[0-9]+\.[0-9]+-python3\.13@sha256:[0-9a-f]{64}"
LOCK_COMMAND = (
    "uv pip compile dbt-requirements.in --universal --python-version=3.13 --generate-hashes "
    "--output-file=dbt-requirements.txt"
)
TELEMETRY = {
    "RUNTIME__DLTHUB_TELEMETRY": "false",
    "DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS": "False",
    "AIRFLOW__COSMOS__ENABLE_TELEMETRY": "False",
    "DO_NOT_TRACK": "1",
}
# The configuration providers of dlt: none of them reads Airflow Variables. Not telemetry, so a
# constant of its own.
DLT_PROVIDERS = {"PROVIDERS__ENABLE_AIRFLOW_SECRETS": "false"}
# The caches that Cosmos would keep in Airflow Variables: both off.
COSMOS_CACHES = {
    "AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS": "False",
    "AIRFLOW__COSMOS__ENABLE_CACHE_DBT_YAML_SELECTORS": "False",
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


# A backslash at the end of a line, maybe followed by blanks: the instruction goes on in the next.
CONTINUATION = re.compile(r"\\[ \t]*$")


def instructions(text: str) -> list[tuple[str, str]]:
    """The instructions of a Dockerfile as Docker reads them: (keyword in upper case, arguments).

    A comment line or a blank line is left out, also inside an instruction, and the lines of an
    instruction are joined as they are, once the backslash at the end of each is gone."""
    found, current = [], ""
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        current += line if current else line.lstrip()
        if CONTINUATION.search(current):
            current = CONTINUATION.sub("", current)
            continue
        keyword, *arguments = current.split(maxsplit=1)
        found.append((keyword.upper(), arguments[0] if arguments else ""))
        current = ""
    return found


def environment(text: str) -> dict[str, str]:
    """The variables that the ENV instructions of a Dockerfile set, with their values: each ENV
    holds NAME=value pairs, which shlex splits with the quotes that Docker reads. A mention of a
    variable anywhere else, such as in a comment or a RUN, sets nothing."""
    found = {}
    for keyword, arguments in instructions(text):
        if keyword == "ENV":
            for pair in shlex.split(arguments):
                name, equals, value = pair.partition("=")
                assert equals, f"ENV {name}: this test reads only the form NAME=value"
                found[name] = value
    return found


def test_the_environment_of_a_dockerfile_is_what_its_env_instructions_set():
    text = (
        "FROM base\n"
        "ENV A=1 \\\n"
        "    # a comment inside the instruction\n"
        '    B="two words" \\\n'
        "\n"
        "    C=three\n"
        "RUN echo D=4 \\\n"
        "    && true\n"
        "ENV\tE=5\n"
    )
    assert environment(text) == {"A": "1", "B": "two words", "C": "three", "E": "5"}


# Dockerfiles that name the switch of dlt but do not set it to exactly false in an ENV instruction.
NOT_SET = {
    "commented out": "FROM base\n# ENV PROVIDERS__ENABLE_AIRFLOW_SECRETS=false\n",
    "commented out inside an instruction": (
        "FROM base\nENV A=1 \\\n#    PROVIDERS__ENABLE_AIRFLOW_SECRETS=false \\\n    B=2\n"
    ),
    "a value with a suffix": "FROM base\nENV PROVIDERS__ENABLE_AIRFLOW_SECRETS=false-x\n",
    "in a RUN": "FROM base\nRUN echo PROVIDERS__ENABLE_AIRFLOW_SECRETS=false\n",
}


@pytest.mark.parametrize("text", NOT_SET.values(), ids=NOT_SET)
def test_a_variable_that_no_env_instruction_sets_exactly_is_not_set(text):
    assert environment(text).get("PROVIDERS__ENABLE_AIRFLOW_SECRETS") != "false"


def test_the_image_starts_from_the_pinned_official_image():
    images = re.findall(r"^FROM\s+(\S+)", dockerfile(), flags=re.MULTILINE)
    assert len(images) == 1, images
    assert re.fullmatch(BASE_IMAGE, images[0]), images[0]


# The dependencies of dlt 1.30.0 that the constraints of Airflow 3.3.2 leave open:
# airflow/requirements.txt pins them, so that every machine builds the same image.
DLT_DEPENDENCIES = {"giturlparse", "pathvalidate", "requirements-parser", "semver", "simplejson"}
# The packages pinned above the constraints of Airflow 3.3.2 for a security fix, with the first
# version that has it: certifi, for GHSA-6q48-6c7w-9gw2.
OVERRIDES = {"certifi": (2026, 10, 18)}


def test_airflow_gets_dlt_and_cosmos_under_its_constraints():
    # Renovate updates their versions: the test fixes the names, and pins() requires the ==.
    requirements = AIRFLOW / "requirements.txt"
    assert set(pins(requirements)) == {"dlt", "astronomer-cosmos"} | DLT_DEPENDENCIES | set(
        OVERRIDES
    )
    # dlt keeps the extra that its postgres destination needs.
    extra = r"^dlt\[[^\]]*\bpostgres\b[^\]]*\]=="
    assert re.search(extra, requirements.read_text(encoding="utf-8"), flags=re.MULTILINE)
    text = dockerfile()
    assert '"apache-airflow==${AIRFLOW_VERSION}"' in text  # pip fails rather than move Airflow
    assert "--constraint /tmp/constraints.txt" in text
    assert "&& pip check" in text


def test_the_overrides_of_the_constraints_keep_their_fix():
    # The Dockerfile takes exactly these lines out of the constraints, and the pin of each stays at
    # or above the version with the fix, also when Renovate moves it.
    text = dockerfile()
    taken_out = re.findall(r"grep -v '\^([a-z0-9-]+)==' \"\$\{HOME\}/constraints\.txt\"", text)
    assert taken_out == sorted(OVERRIDES)
    pinned = pins(AIRFLOW / "requirements.txt")
    for name, fixed in OVERRIDES.items():
        assert tuple(int(part) for part in pinned[name].split(".")) >= fixed, name


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
    # Renovate's pip-compile manager reruns the command from this header. It reads the header
    # only when an option that takes a value has it after an equal sign (renovatebot/renovate
    # 44.132.5): with a space, it skips the file with a warning and never updates the lock.
    header = (AIRFLOW / "dbt-requirements.txt").read_text(encoding="utf-8").splitlines()[:2]
    assert header[1].removeprefix("#").strip() == LOCK_COMMAND
    assert LOCK_COMMAND in (AIRFLOW / "dbt-requirements.in").read_text(encoding="utf-8")


def test_the_image_turns_telemetry_off():
    found = environment(dockerfile())
    assert {name: found.get(name) for name in TELEMETRY} == TELEMETRY


def test_the_image_keeps_dlt_away_from_airflow_variables():
    # Inside a task, dlt would take its settings from Airflow Variables, which the Op role may
    # create, and one of them could send the connection of a load somewhere else.
    found = environment(dockerfile())
    assert {name: found.get(name) for name in DLT_PROVIDERS} == DLT_PROVIDERS


def test_the_image_keeps_the_caches_of_cosmos_out_of_airflow_variables():
    # Cosmos would keep the output of dbt ls in an Airflow Variable, which the Op role may edit, and
    # render the tasks of dbt and their arguments from it.
    found = environment(dockerfile())
    assert {name: found.get(name) for name in COSMOS_CACHES} == COSMOS_CACHES


def test_the_build_context_holds_only_what_the_dockerfile_copies():
    ignore = (AIRFLOW / ".dockerignore").read_text(encoding="utf-8").splitlines()
    assert [line for line in ignore if line and not line.startswith("#")] == [
        "*",
        "!requirements.txt",
        "!dbt-requirements.txt",
        "!plugins/agoranomos_oom.py",
    ]
    # The files of the build context of the folder; the context dbt is the dbt project.
    own = [line for line in dockerfile().splitlines() if not line.startswith("COPY --from=")]
    copied = re.findall(r"^COPY\s+(?:--\S+\s+)*(\S+)\s", "\n".join(own), flags=re.MULTILINE)
    assert sorted(copied) == [
        "dbt-requirements.txt",
        "plugins/agoranomos_oom.py",
        "requirements.txt",
    ]


def test_the_image_installs_the_packages_of_the_dbt_project():
    # dbt deps runs when the image is built, from the two files of the dbt project that it reads,
    # into the packages-install-path of dbt_project.yml, so no task of dbt needs the network
    # (ADR-0029). compose.yaml gives the build the context dbt.
    text = dockerfile()
    [copy] = [line for line in text.splitlines() if line.startswith("COPY --from=dbt")]
    assert copy.split() == [
        "COPY",
        "--from=dbt",
        "--chown=airflow:root",
        "dbt_project.yml",
        "packages.yml",
        "/tmp/agoranomos/dbt-deps/",  # noqa: S108 (a folder of the build, not of this host)
    ]
    assert "/opt/airflow/dbt_venv/bin/dbt deps --project-dir /tmp/agoranomos/dbt-deps" in text
    assert text.index(copy) > text.index("/opt/airflow/dbt_venv/bin/pip check")
    project = (AIRFLOW.parent / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert re.search(r"^packages-install-path: /opt/airflow/dbt_packages$", project, re.MULTILINE)
    compose = (AIRFLOW.parent / "compose.yaml").read_text(encoding="utf-8")
    assert "      additional_contexts:\n        dbt: ./dbt\n" in compose


def test_the_image_gives_the_folder_of_the_export_to_nobody_and_keeps_its_user():
    # A new named volume takes the owner of the folder that it is first mounted on: without a
    # folder of nobody, the volume of metadata-export would belong to root, and the job, which runs
    # as nobody, could not write it. The volume of the manifest of the dbt project, which the job
    # writes too, needs the same. The image then goes back to the user of the official image.
    found = instructions(dockerfile())
    users = [arguments for keyword, arguments in found if keyword == "USER"]
    assert users == ["root", "${AIRFLOW_UID}"]
    folders = (
        "install -d -o 65534 -g 65534 -m 0755 /opt/agoranomos/metadata /opt/agoranomos/manifest"
    )
    assert ("RUN", folders) in found
    root = found.index(("USER", "root"))
    assert found[root + 1] == ("RUN", folders)
    # The next instruction makes the folder of the plugins, as root (below).
    assert found[root + 3] == ("USER", "${AIRFLOW_UID}")


def test_the_image_installs_the_plugins_where_only_root_may_write_them():
    # Airflow imports every file of its plugins folder in each of its processes, so a plugin that a
    # task could change would run in the scheduler too. The Airflow services run as 50000 in the
    # group root (compose.yaml), and COPY keeps the mode of the file on disk, which a umask of 002
    # makes writable by the group: so each plugin is copied with the mode 0644 set. The folder of
    # the official image, /opt/airflow/plugins, is not used: its parent is writable by that group.
    plugins = sorted(path.name for path in (AIRFLOW / "plugins").glob("*.py"))
    assert plugins == ["agoranomos_oom.py"]
    found = instructions(dockerfile())
    # COPY --chmod gives a folder that it has to create the mode of the file, 0644, and nobody could
    # enter /opt/agoranomos, where the DAGs and the dbt project are mounted: so root makes the
    # folder, with the mode 0755, before the plugins are copied into it.
    folder = ("RUN", "install -d -m 0755 /opt/agoranomos/plugins")
    assert folder in found
    for name in plugins:
        copy = ("COPY", f"--chmod=0644 plugins/{name} /opt/agoranomos/plugins/{name}")
        assert copy in found
        assert found.index(folder) < found.index(copy)
    assert environment(dockerfile())["AIRFLOW__CORE__PLUGINS_FOLDER"] == "/opt/agoranomos/plugins"
