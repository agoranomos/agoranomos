import json

from services import LONG_RUNNING, ONE_SHOT
from stack_helpers import compose, containers

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
