import json
import shlex

from services import LONG_RUNNING, ONE_SHOT, SERVER
from stack_helpers import (
    LogEntries,
    NamesOnly,
    active_variant,
    compose,
    containers,
    docker,
    job_failed,
    leaked,
    process_uids,
    rendered,
    rendered_config,
    secrets,
)

# The services that must stay on the internal data network only (no route to the internet).
DATA_ONLY = {"warehouse-db", "platform-db", "db-init", "broker", "backup"}
# Prints the arguments of every process of a container, from the process files.
LISTING = "cat /proc/[0-9]*/cmdline | tr '\\0' ' '"


def definition(container_id: str) -> LogEntries:
    """The words of the command, the entry point and the health check of a container, as docker
    inspect shows them to everyone who can use Docker. The definition holds the environment of the
    container too, secrets included: its objects are read as NamesOnly dicts and the words come as
    LogEntries, so that a frame of a failed test that holds them shows no value."""
    [details] = json.loads(docker("inspect", container_id).stdout, object_hook=NamesOnly)
    config = details["Config"]
    return LogEntries(
        [
            details["Path"],
            *details["Args"],
            *(config["Cmd"] or []),
            *(config["Entrypoint"] or []),
            *((config.get("Healthcheck") or {}).get("Test") or []),
        ]
    )


def arguments_of_the_processes(service: str, values: dict[str, str]) -> tuple[bool, list[str]]:
    """List the arguments of every process of a running service: whether the listing ran, and the
    names of the secrets in it. The command that reads the arguments is itself one of the processes
    that it lists, so its own words show that the listing worked. The listing stays in this frame,
    which has ended before the test checks anything."""
    result = compose("exec", "-T", service, "sh", "-c", LISTING, check=False)
    return "cat /proc/" in result.stdout, leaked(values, result.stdout)


def test_long_running_services_are_healthy():
    running = containers()
    for service in sorted(LONG_RUNNING):
        assert service in running, f"{service} has no container"
        assert running[service]["State"] == "running", running[service]
        assert running[service]["Health"] in ("healthy", ""), running[service]


def test_one_shot_jobs_succeeded():
    running = containers()
    for service in sorted(ONE_SHOT):
        assert running[service]["State"] == "exited", running[service]
        assert running[service]["ExitCode"] == 0, running[service]


def test_ports_are_published_on_localhost_only_but_those_of_the_proxy_in_server_mode():
    # The proxy of server mode publishes TCP 80 and 443 on every address of the host
    # (tests/stack/test_server.py checks them); every other port is on 127.0.0.1. In local mode no
    # container publishes on every address, a proxy left from server mode included.
    for service, container in containers().items():
        for publisher in container.get("Publishers") or []:
            if not publisher.get("PublishedPort"):
                continue
            if service == "proxy" and SERVER:
                assert (publisher["PublishedPort"], publisher["Protocol"]) in {
                    (80, "tcp"),
                    (443, "tcp"),
                }, publisher
            else:
                assert publisher["URL"] == "127.0.0.1", (service, publisher)


def test_databases_publish_no_ports():
    running = containers()
    for service in ("warehouse-db", "platform-db"):
        published = [p for p in running[service].get("Publishers") or [] if p.get("PublishedPort")]
        assert published == [], service


def test_db_init_can_run_again():
    result = compose("run", "--rm", "--no-deps", "db-init", check=False)
    assert result.returncode == 0, job_failed("db-init", result)


# The configuration that these tests read holds the secrets of .env: rendered() and
# rendered_config() give its objects as NamesOnly dicts, so that a frame of a failed test, under
# --showlocals too, shows no value. They render every profile, demo-init included, whatever .env
# selects: the checks sweep all services or look at core ones, so a deployment with the demo off
# meets the same rules.


def test_no_service_mounts_the_docker_socket():
    for name, service in rendered(active_variant()).items():
        for volume in service.get("volumes", []):
            source = volume.get("source", "") if isinstance(volume, dict) else str(volume)
            assert "docker.sock" not in source, name


def test_databases_and_broker_are_on_the_internal_network_only():
    config = rendered_config(active_variant())
    assert config["networks"]["data"].get("internal") is True
    for name in sorted(DATA_ONLY & set(config["services"])):
        assert set(config["services"][name].get("networks", {})) == {"data"}, name


def test_the_network_that_docker_made_is_internal_and_the_databases_are_only_on_it(env):
    # compose config shows what the file says. This looks at what Docker created from it.
    network = f"{env.get('COMPOSE_PROJECT_NAME', 'agoranomos')}_data"
    [details] = json.loads(docker("network", "inspect", network).stdout)
    assert details["Internal"] is True
    running = containers()
    # The broker of the Celery variant and the backup service of the ops profile are on the data
    # network only, as the databases are.
    for service in (
        "warehouse-db",
        "platform-db",
        "db-init",
        *sorted({"broker", "backup"} & set(running)),
    ):
        # docker inspect shows the environment of the container, secrets included.
        [container] = json.loads(
            docker("inspect", running[service]["ID"]).stdout, object_hook=NamesOnly
        )
        assert set(container["NetworkSettings"]["Networks"]) == {network}, service


def test_the_network_that_docker_made_has_the_ipv6_that_env_asks_for(env):
    # compose config shows the option. This looks at what Docker made of it: IPv6 on, and a subnet
    # of IPv6 addresses that Docker picked because the file sets none, when AGORANOMOS_IPV6 is true
    # (init --server sets it), and no IPv6 otherwise. The data network never has it (ADR-0023).
    project = env.get("COMPOSE_PROJECT_NAME", "agoranomos")
    asked = (env.get("AGORANOMOS_IPV6") or "false") == "true"
    [default] = json.loads(docker("network", "inspect", f"{project}_default").stdout)
    assert default["EnableIPv6"] is asked
    assert default["Labels"]["dev.agoranomos.ipv6"] == str(asked).lower()
    subnets = [entry["Subnet"] for entry in default["IPAM"]["Config"]]
    assert any(":" in subnet for subnet in subnets) is asked
    [data] = json.loads(docker("network", "inspect", f"{project}_data").stdout)
    assert data["EnableIPv6"] is False


def test_the_database_health_checks_connect_over_tcp():
    # On the first start with an empty volume the image runs a temporary server that listens only
    # on the Unix socket, and a check over the socket could report it ready (see compose.yaml).
    services = rendered(active_variant())
    for service in ("warehouse-db", "platform-db"):
        # The texts of a health check could hold a password, so they print as their number only.
        test = LogEntries(services[service]["healthcheck"]["test"])
        words = LogEntries(shlex.split(" ".join(test[1:])))
        assert words[0] == "pg_isready", service
        assert words[words.index("-h") + 1] == "127.0.0.1", service


def test_db_init_prints_no_notices_when_it_runs_again():
    result = compose("run", "--rm", "--no-deps", "db-init", check=False)
    assert result.returncode == 0, job_failed("db-init", result)
    # A flag, not the output: a failed assertion prints what it compares.
    printed_a_notice = "NOTICE" in result.stdout + result.stderr
    assert not printed_a_notice, "db-init printed a notice: run it by hand to see it"


def test_no_secret_is_in_the_definition_of_a_container(env):
    # The command, the entry point and the health check of a container show in docker inspect to
    # everyone who can use Docker: a secret belongs in the environment, and never there. This looks
    # at every container, the finished ones too, and names the settings, never their values. The
    # words are scanned where they are read, and the loop holds the ID of a container only.
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    ids = {service: container["ID"] for service, container in containers().items()}
    for service, container_id in ids.items():
        found = leaked(values, definition(container_id))
        assert not found, f"{service} has the values of {found} in its definition"


def test_no_secret_is_in_the_arguments_of_a_running_process(env):
    # Other users of the host, and every process in a container, can read the arguments of the
    # processes in /proc. A failed assertion prints what it compares, and a listing can hold the
    # very values that the test looks for: the assertions here hold a flag and the name of a
    # service, never the listing.
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    for service in sorted(LONG_RUNNING):
        listed, found = arguments_of_the_processes(service, values)
        assert listed, f"{service}: the listing did not run"
        assert not found, f"a process of {service} has the values of {found} in its arguments"


def test_no_process_of_a_running_service_holds_a_capability():
    # cap_drop: [ALL] in compose.yaml, as the kernel applies it: the first process of each
    # container has no effective capability, and its bounding set is empty, so that no process of
    # the container can ever get one. A process that is not root has no effective capability even
    # without cap_drop; only the bounding set shows that cap_drop is there.
    for service in sorted(LONG_RUNNING):
        result = compose(
            "exec", "-T", service, "grep", "-E", "^Cap(Eff|Bnd):", "/proc/1/status", check=False
        )
        assert result.returncode == 0, f"{service}: {result.stderr}"
        empty = "0000000000000000"
        assert result.stdout.split() == ["CapEff:", empty, "CapBnd:", empty], service


# The services that start as a named user of their image, and the number of that user: the user
# that their entrypoint would switch to as root.
NAMED_USERS = {"warehouse-db": ("postgres", 999), "platform-db": ("postgres", 999)}


def test_the_databases_and_metabase_run_as_the_users_of_their_images():
    # The processes alone would show the same users without user: in compose.yaml, since the
    # entrypoint would switch to them as root. docker compose exec runs as the user: of the
    # service, and as root without one, so id -u shows that the line is there.
    for service, (process, uid) in {**NAMED_USERS, "metabase": ("java", 2000)}.items():
        result = compose("exec", "-T", service, "sh", "-c", process_uids(process), check=False)
        assert result.returncode == 0, f"{service}: {result.stderr}"
        uids = {line.split()[1] for line in result.stdout.splitlines()}
        assert uids == {str(uid)}, (service, sorted(uids))
        user = compose("exec", "-T", service, "id", "-u", check=False)
        assert user.returncode == 0, f"{service}: {user.stderr}"
        assert user.stdout.strip() == str(uid), (service, user.stdout.strip())


def test_db_init_leaves_no_volume_behind():
    # Its container has only the bind mount of postgres/: no anonymous volume for the data that
    # the image of PostgreSQL declares (see compose.yaml). A db-init container of an earlier
    # release has such a volume, and Compose carries it over to the container that it recreates
    # from the new compose.yaml, so on a stack that was upgraded this fails until the container
    # is new. The message names the types of the mounts only.
    [details] = json.loads(
        docker("inspect", containers()["db-init"]["ID"]).stdout, object_hook=NamesOnly
    )
    kinds = sorted(mount["Type"] for mount in details["Mounts"])
    assert kinds == ["bind"], (
        f"db-init has the mounts {kinds}. A deployment upgraded from an earlier release keeps the "
        "old anonymous volume of db-init until ./agoranomos down and ./agoranomos up. The volume "
        "that it left is then dangling, and docker volume rm <name> removes it (never prune)."
    )


def oom_kills(service: str) -> int | None:
    """How many processes of a running service the kernel killed for want of memory, from the
    cgroup v2 of its container, or None when the host has no cgroup v2."""
    result = compose(
        "exec", "-T", service, "grep", "^oom_kill ", "/sys/fs/cgroup/memory.events", check=False
    )
    return int(result.stdout.split()[1]) if result.returncode == 0 else None


def test_no_service_ran_out_of_memory_or_restarted():
    # The memory limits come from measurements (docs/reference/system-requirements.md). A process
    # that the kernel killed at a limit, or a container that Docker restarted, says that a limit is
    # too low, or that something else broke. CI runs the stack tests twice, so the second run also
    # checks what the first one did.
    problems = []
    for service, container in sorted(containers().items()):
        if service not in LONG_RUNNING:
            continue
        [details] = json.loads(docker("inspect", container["ID"]).stdout, object_hook=NamesOnly)
        if details["RestartCount"]:
            problems.append(f"{service} restarted {details['RestartCount']} times")
        kills = oom_kills(service)
        if kills:
            problems.append(f"the kernel killed {kills} processes of {service} at its limit")
    assert not problems, "; ".join(problems)
