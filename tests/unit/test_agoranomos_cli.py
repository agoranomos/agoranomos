import base64
import re
import stat

GIB = 1024**3
SECRET_KEYS = [
    "WAREHOUSE_POSTGRES_PASSWORD",
    "PLATFORM_POSTGRES_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "METABASE_DB_PASSWORD",
    "METABASE_READER_PASSWORD",
    "VALKEY_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_JWT_SECRET",
    "AIRFLOW_API_SECRET_KEY",
    "AIRFLOW_ADMIN_PASSWORD",
    "METABASE_ENCRYPTION_KEY",
    "METABASE_ADMIN_PASSWORD",
]


def test_help_lists_the_commands(cli):
    result = cli.run("help")
    assert result.returncode == 0
    for command in ("init", "preflight", "help"):
        assert re.search(rf"^  {command}\s", result.stdout, flags=re.MULTILINE)


def test_no_command_shows_help(cli):
    result = cli.run()
    assert result.returncode == 0
    assert "Usage: ./agoranomos" in result.stdout


def test_unknown_command_fails(cli):
    result = cli.run("launch")
    assert result.returncode == 1
    assert "unknown command: launch" in result.stderr


def test_init_creates_env_with_every_secret(cli):
    result = cli.run("init")
    assert result.returncode == 0, result.stderr
    env = cli.read_env()
    for key in SECRET_KEYS:
        assert env[key], key
    for key in ("WAREHOUSE_POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD", "VALKEY_PASSWORD"):
        assert re.fullmatch(r"[A-Za-z0-9]{32}", env[key]), key
    for key in ("AIRFLOW_JWT_SECRET", "AIRFLOW_API_SECRET_KEY", "METABASE_ENCRYPTION_KEY"):
        assert re.fullmatch(r"[A-Za-z0-9]{64}", env[key]), key
    assert len(base64.urlsafe_b64decode(env["AIRFLOW_FERNET_KEY"])) == 32
    assert len({env[key] for key in SECRET_KEYS}) == len(SECRET_KEYS)
    assert stat.S_IMODE(cli.env_path.stat().st_mode) == 0o600
    assert "password manager" in result.stdout


def test_admin_passwords_have_a_digit_and_both_cases(cli):
    cli.run("init")
    env = cli.read_env()
    for key in ("AIRFLOW_ADMIN_PASSWORD", "METABASE_ADMIN_PASSWORD"):
        value = env[key]
        assert len(value) == 32
        assert re.search(r"[0-9]", value)
        assert re.search(r"[a-z]", value)
        assert re.search(r"[A-Z]", value)


def test_init_is_idempotent(cli):
    cli.run("init")
    first = cli.env_path.read_bytes()
    result = cli.run("init")
    assert result.returncode == 0
    assert cli.env_path.read_bytes() == first
    assert "Generated" not in result.stdout


def test_init_keeps_values_that_are_set(cli):
    cli.run("init")
    text = cli.env_path.read_text(encoding="utf-8")
    text = text.replace("AIRFLOW_PORT=8080", "AIRFLOW_PORT=18080")
    text = re.sub(
        r"^VALKEY_PASSWORD=.*$", "VALKEY_PASSWORD=MyOwnBrokerPassword1", text, flags=re.MULTILINE
    )
    cli.env_path.write_text(text, encoding="utf-8")
    cli.run("init")
    env = cli.read_env()
    assert env["AIRFLOW_PORT"] == "18080"
    assert env["VALKEY_PASSWORD"] == "MyOwnBrokerPassword1"


def test_init_fills_only_the_empty_secret(cli):
    cli.run("init")
    before = cli.read_env()
    text = re.sub(
        r"^VALKEY_PASSWORD=.*$",
        "VALKEY_PASSWORD=",
        cli.env_path.read_text(encoding="utf-8"),
        flags=re.MULTILINE,
    )
    cli.env_path.write_text(text, encoding="utf-8")
    result = cli.run("init")
    after = cli.read_env()
    assert "VALKEY_PASSWORD" in result.stdout
    assert after["VALKEY_PASSWORD"]
    assert after["VALKEY_PASSWORD"] != before["VALKEY_PASSWORD"]
    assert {k: v for k, v in after.items() if k != "VALKEY_PASSWORD"} == {
        k: v for k, v in before.items() if k != "VALKEY_PASSWORD"
    }


def test_init_adds_settings_that_a_new_release_brings(cli):
    cli.run("init")
    with (cli.root / ".env.example").open("a", encoding="utf-8") as example:
        example.write("NEW_SETTING=42\n")
    text = cli.env_path.read_text(encoding="utf-8").rstrip("\n")
    cli.env_path.write_text(text, encoding="utf-8")  # .env without a final newline
    result = cli.run("init")
    assert "Added new settings to .env: NEW_SETTING" in result.stdout
    lines = cli.env_path.read_text(encoding="utf-8").splitlines()
    assert lines.count("NEW_SETTING=42") == 1
    assert all("NEW_SETTING" not in line or line == "NEW_SETTING=42" for line in lines)


def test_init_creates_the_extensions_folder(cli):
    cli.run("init")
    assert (cli.root / "extensions" / "dags").is_dir()


def test_runs_from_another_directory(cli, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    result = cli.run("init", cwd=elsewhere)
    assert result.returncode == 0, result.stderr
    assert cli.env_path.exists()
    assert not (elsewhere / ".env").exists()


def test_preflight_passes_on_a_good_machine(cli):
    result = cli.run("preflight")
    assert result.returncode == 0, result.stderr
    assert "Preflight passed" in result.stdout


def test_preflight_stops_when_docker_is_down(cli):
    result = cli.run("preflight", FAKE_DOCKER_DOWN="1")
    assert result.returncode == 1
    assert "Docker is not running" in result.stderr


def test_preflight_stops_on_too_little_memory(cli):
    result = cli.run("preflight", FAKE_MEMTOTAL=str(4 * GIB))
    assert result.returncode == 1
    assert "Settings → Resources → Memory" in result.stderr


def test_preflight_warns_below_the_recommended_memory(cli):
    result = cli.run("preflight", FAKE_MEMTOTAL=str(7 * GIB))
    assert result.returncode == 0, result.stderr
    assert "8 GB is recommended" in result.stderr


def test_eight_gigabytes_as_docker_reports_them_pass_without_a_warning(cli):
    result = cli.run("preflight", FAKE_MEMTOTAL=str(int(7.6 * GIB)))
    assert result.returncode == 0
    assert "warning" not in result.stderr


def test_force_turns_resource_stops_into_warnings(cli):
    result = cli.run("preflight", "--force", FAKE_MEMTOTAL=str(4 * GIB), FAKE_NCPU="1")
    assert result.returncode == 0, result.stderr
    assert "ignored: --force" in result.stderr


def test_preflight_stops_on_one_cpu(cli):
    result = cli.run("preflight", FAKE_NCPU="1")
    assert result.returncode == 1
    assert "1 CPU" in result.stderr


def test_preflight_stops_on_old_docker(cli):
    result = cli.run("preflight", FAKE_SERVER_VERSION="24.0.7")
    assert result.returncode == 1
    assert "Docker Engine 24.0.7 is older than 25.0.0" in result.stderr


def test_preflight_stops_on_old_compose(cli):
    result = cli.run("preflight", FAKE_COMPOSE_VERSION="2.20.3")
    assert result.returncode == 1
    assert "Docker Compose 2.20.3 is older than 2.24.0" in result.stderr


def test_preflight_accepts_a_v_prefix_and_suffixes(cli):
    result = cli.run(
        "preflight", FAKE_COMPOSE_VERSION="v2.30.0-desktop.1", FAKE_SERVER_VERSION="29.0.0-rc.1"
    )
    assert result.returncode == 0, result.stderr


def test_preflight_stops_without_compose(cli):
    result = cli.run("preflight", FAKE_NO_COMPOSE="1")
    assert result.returncode == 1
    assert "Docker Compose plugin is missing" in result.stderr


def test_preflight_checks_x86_64_v2(cli, tmp_path):
    cpuinfo = tmp_path / "cpuinfo"
    cpuinfo.write_text("flags\t\t: fpu cx16 lahf_lm popcnt pni sse4_1 ssse3\n", encoding="utf-8")
    result = cli.run("preflight", FAKE_ARCH="x86_64", AGORANOMOS_CPUINFO=str(cpuinfo))
    assert result.returncode == 1
    assert "sse4_2" in result.stderr
    cpuinfo.write_text(
        "flags\t\t: fpu cx16 lahf_lm popcnt pni sse4_1 sse4_2 ssse3 avx\n", encoding="utf-8"
    )
    result = cli.run("preflight", FAKE_ARCH="x86_64", AGORANOMOS_CPUINFO=str(cpuinfo))
    assert result.returncode == 0, result.stderr


def test_preflight_rejects_other_architectures(cli):
    result = cli.run("preflight", FAKE_ARCH="riscv64")
    assert result.returncode == 1
    assert "riscv64 is not supported" in result.stderr


def test_preflight_stops_when_a_port_is_in_use(cli):
    cli.run("init")
    result = cli.run("preflight", FAKE_BUSY_PORTS="8080")
    assert result.returncode == 1
    assert "port 8080 is in use" in result.stderr
    assert "AIRFLOW_PORT" in result.stderr


def test_preflight_uses_the_ports_from_env(cli):
    cli.run("init")
    text = cli.env_path.read_text(encoding="utf-8").replace(
        "METABASE_PORT=3000", "METABASE_PORT=13000"
    )
    cli.env_path.write_text(text, encoding="utf-8")
    assert cli.run("preflight", FAKE_BUSY_PORTS="3000").returncode == 0
    assert cli.run("preflight", FAKE_BUSY_PORTS="13000").returncode == 1


def test_preflight_skips_the_ports_while_the_stack_runs(cli):
    cli.run("init")
    result = cli.run("preflight", FAKE_BUSY_PORTS="8080 3000", FAKE_RUNNING="abc123\n")
    assert result.returncode == 0, result.stderr


def test_preflight_checks_free_disk(cli, tmp_path):
    result = cli.run(
        "preflight", FAKE_DOCKER_ROOT=str(tmp_path), FAKE_DISK_AVAILABLE_KB=str(5 * 1024 * 1024)
    )
    assert result.returncode == 1
    assert "5 GB of disk is free" in result.stderr
    result = cli.run(
        "preflight", FAKE_DOCKER_ROOT=str(tmp_path), FAKE_DISK_AVAILABLE_KB=str(15 * 1024 * 1024)
    )
    assert result.returncode == 0
    assert "20 GB is recommended" in result.stderr


def test_preflight_rejects_unknown_options(cli):
    result = cli.run("preflight", "--fast")
    assert result.returncode == 1
    assert "unknown option: --fast" in result.stderr
