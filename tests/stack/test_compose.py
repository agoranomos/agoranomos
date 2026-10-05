import json
import shlex

from services import LONG_RUNNING, ONE_SHOT
from stack_helpers import compose, containers, docker

# The services that must stay on the internal data network only (no route to the internet).
DATA_ONLY = {"warehouse-db", "platform-db", "db-init", "broker"}


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


def test_ports_are_published_on_localhost_only():
    for service, container in containers().items():
        for publisher in container.get("Publishers") or []:
            if publisher.get("PublishedPort"):
                assert publisher["URL"] == "127.0.0.1", (service, publisher)


def test_databases_publish_no_ports():
    running = containers()
    for service in ("warehouse-db", "platform-db"):
        published = [p for p in running[service].get("Publishers") or [] if p.get("PublishedPort")]
        assert published == [], service


def test_db_init_can_run_again():
    result = compose("run", "--rm", "--no-deps", "db-init", check=False)
    assert result.returncode == 0, result.stderr


def test_no_service_mounts_the_docker_socket():
    config = json.loads(compose("config", "--format", "json").stdout)
    for name, service in config["services"].items():
        for volume in service.get("volumes", []):
            source = volume.get("source", "") if isinstance(volume, dict) else str(volume)
            assert "docker.sock" not in source, name


def test_databases_and_broker_are_on_the_internal_network_only():
    config = json.loads(compose("config", "--format", "json").stdout)
    assert config["networks"]["data"].get("internal") is True
    for name in sorted(DATA_ONLY & set(config["services"])):
        assert set(config["services"][name].get("networks", {})) == {"data"}, name


def test_the_network_that_docker_made_is_internal_and_the_databases_are_only_on_it(env):
    # compose config shows what the file says. This looks at what Docker created from it.
    network = f"{env.get('COMPOSE_PROJECT_NAME', 'agoranomos')}_data"
    [details] = json.loads(docker("network", "inspect", network).stdout)
    assert details["Internal"] is True
    running = containers()
    for service in ("warehouse-db", "platform-db", "db-init"):
        [container] = json.loads(docker("inspect", running[service]["ID"]).stdout)
        assert set(container["NetworkSettings"]["Networks"]) == {network}, service


def test_the_database_health_checks_connect_over_tcp():
    # On the first start with an empty volume the image runs a temporary server that listens only
    # on the Unix socket, and a check over the socket could report it ready (see compose.yaml).
    config = json.loads(compose("config", "--format", "json").stdout)
    for service in ("warehouse-db", "platform-db"):
        test = config["services"][service]["healthcheck"]["test"]
        words = shlex.split(" ".join(test[1:]))
        assert words[0] == "pg_isready", service
        assert words[words.index("-h") + 1] == "127.0.0.1", service


def test_db_init_prints_no_notices_when_it_runs_again():
    result = compose("run", "--rm", "--no-deps", "db-init", check=False)
    assert result.returncode == 0, result.stderr
    assert "NOTICE" not in result.stdout + result.stderr
