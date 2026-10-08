"""Tests of scripts/memory_report.py: how it reads docker stats and the cgroup, and the table."""

import json

import memory_report as mr
import pytest

MIB = 1024**2


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0B", 0),
        ("512B", 512),
        ("1.5KiB", 1536),
        ("100MiB", 100 * MIB),
        ("1.2GiB", round(1.2 * 1024**3)),
        ("2kB", 2000),
        (" 7.5MB ", 7_500_000),
    ],
)
def test_a_size_of_docker_stats_is_read_in_bytes(text, expected):
    assert mr.parse_size(text) == expected


@pytest.mark.parametrize("text", ["12 parsecs", "MiB", "", "1,5MiB"])
def test_a_size_that_is_not_one_is_refused(text):
    with pytest.raises(ValueError, match="unknown size"):
        mr.parse_size(text)


def stats(name: str, usage: str) -> str:
    return json.dumps({"Name": name, "MemUsage": usage, "CPUPerc": "0.01%"})


def test_the_samples_are_read_through_the_escapes_that_redraw_the_table():
    # What docker stats writes into a pipe: each line between the escapes that move the cursor
    # and clear the screen.
    output = (
        f"\x1b[H{stats('a-metabase-1', '1.25GiB / 2GiB')}\x1b[K\n"
        f"{stats('a-broker-1', '9.5MiB / 128MiB')}\x1b[K\n\x1b[J"
        f"\x1b[H{stats('a-metabase-1', '1.5GiB / 2GiB')}\x1b[K\n"
    )
    assert mr.samples(output) == [
        ("a-metabase-1", round(1.25 * 1024**3)),
        ("a-broker-1", round(9.5 * MIB)),
        ("a-metabase-1", round(1.5 * 1024**3)),
    ]


def test_a_container_without_a_size_yet_is_left_out():
    assert mr.samples(stats("a-db-init-1", "-- / --") + "\n") == []


def test_the_cgroup_gives_its_peak_and_its_oom_kills():
    assert mr.cgroup_values("peak 604291072\noom_kill 2\n") == (604291072, 2)


@pytest.mark.parametrize("text", ["peak \n", "", "peak max\noom_kill\n"])
def test_a_cgroup_without_the_files_of_cgroup_v2_gives_nothing(text):
    assert mr.cgroup_values(text) == (None, None)


CONTAINER = {"Service": "metabase", "Name": "a-metabase-1", "State": "running"}


def details(memory: int, restarts: int = 0) -> dict:
    return {"HostConfig": {"Memory": memory}, "RestartCount": restarts}


def test_a_row_takes_the_limit_the_samples_and_the_cgroup():
    row = mr.make_row(
        CONTAINER,
        details(2048 * MIB, restarts=1),
        "peak 1610612736\noom_kill 0\n",
        {"a-metabase-1": 1400 * MIB, "a-other-1": 1},
    )
    assert row == mr.Row("metabase", True, 2048 * MIB, 1400 * MIB, 1536 * MIB, 0, 1)


def test_a_container_without_a_limit_or_samples_has_none():
    job = {"Service": "db-init", "Name": "a-db-init-1", "State": "exited"}
    assert mr.make_row(job, details(0), "", {}) == mr.Row(
        "db-init", False, None, None, None, None, 0
    )


def test_the_table_has_a_row_for_each_service_and_the_total_of_the_running_ones():
    rows = [
        mr.Row("metabase", True, 2048 * MIB, 1400 * MIB, 1536 * MIB, 0, 0),
        mr.Row("db-init", False, 128 * MIB, 20 * MIB, None, None, 0),
        mr.Row("broker", True, 128 * MIB, 10 * MIB, 12 * MIB, 1, 2),
    ]
    assert mr.render("Celery, the stack tests", "aarch64", rows) == (
        "### Celery, the stack tests (aarch64)\n"
        "\n"
        "| Service | Limit | Peak | Since start | OOM kills | Restarts |\n"
        "|---|---:|---:|---:|---:|---:|\n"
        "| broker | 128 MiB | 10 MiB | 12 MiB | 1 | 2 |\n"
        "| db-init | 128 MiB | 20 MiB | n/a | n/a | 0 |\n"
        "| metabase | 2048 MiB | 1400 MiB | 1536 MiB | 0 | 0 |\n"
        "| **Running services, total** | **2176 MiB** | **1410 MiB** | | | |\n"
    )


def test_the_total_has_no_limit_when_a_running_service_has_none():
    rows = [
        mr.Row("metabase", True, None, 1400 * MIB, None, None, 0),
        mr.Row("broker", True, 128 * MIB, 10 * MIB, None, None, 0),
    ]
    assert "| **Running services, total** | **none** | **1410 MiB** | | | |" in mr.render(
        "Local", "x86_64", rows
    )


def test_without_a_command_or_seconds_there_is_nothing_to_measure(capsys):
    with pytest.raises(SystemExit) as exit_info:
        mr.main(["--title", "Local"])
    assert exit_info.value.code == 2
    assert "give a command after --, or --seconds" in capsys.readouterr().err
