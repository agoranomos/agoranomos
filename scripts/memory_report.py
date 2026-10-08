"""Measure the memory of the containers of the stack while a command runs, as a Markdown table.

Run it in the root of the checkout, with the stack up:

    uv run python scripts/memory_report.py --title "Local, the stack tests" \
        -- uv run pytest tests/stack
    uv run python scripts/memory_report.py --title "Local, idle" --seconds 600

While the command runs, and then for the given number of seconds, docker stats samples every
running container about twice a second. The table then gives, for each service of the Compose
project (docs/reference/system-requirements.md explains how the limits come from it):

- Limit: the memory limit of its container, or "none".
- Peak: the highest sample. docker stats leaves out the inactive page cache, which the kernel takes
  back first, so this is the memory that a limit has to hold.
- Since start: the highest use since the container started, from memory.peak of its cgroup. It
  counts all page cache, so it is an upper bound, and it also covers the time before the sampling,
  such as the first start. "n/a" for a job that has exited, or without cgroup v2.
- OOM kills: how many processes of the container the kernel killed for want of memory, from
  memory.events; Restarts: how often Docker restarted the container.

A cell says "unread" when docker inspect or docker exec failed for its container, such as one that
stopped while the table was made: the table still comes out. A line of docker stats that cannot be
read is left out, and the table says how many were.

The total adds up the services that run all the time. The script prints the table, and --output
also appends it to a file, such as $GITHUB_STEP_SUMMARY. It exits with the exit code of the command.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
MIB = 1024**2
UNITS = {"B": 1, "kB": 1000, "KiB": 1024, "MB": 1000**2, "MiB": MIB, "GB": 1000**3, "GiB": 1024**3}
# The escape sequences with which docker stats redraws its table, which it writes into a pipe too.
ESCAPE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
# Prints memory.peak and the oom_kill line of memory.events of the cgroup of a container.
CGROUP = (
    "echo peak $(cat /sys/fs/cgroup/memory.peak 2>/dev/null); "
    "grep '^oom_kill ' /sys/fs/cgroup/memory.events 2>/dev/null; true"
)


def parse_size(text: str) -> int:
    """A size as docker stats writes it, such as 512.3MiB, in bytes."""
    text = text.strip()
    for unit in sorted(UNITS, key=len, reverse=True):
        if text.endswith(unit):
            try:
                return round(float(text[: -len(unit)]) * UNITS[unit])
            except ValueError:
                break
    raise ValueError(f"unknown size: {text!r}")


def samples(output: str) -> list[tuple[str, int]]:
    """The container name and the memory in bytes of each line of JSON that docker stats wrote.

    A container that is starting or stopping shows -- instead of a size, and is left out."""
    found = []
    for line in ESCAPE.sub("", output).splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        stats = json.loads(line)
        used = stats["MemUsage"].split("/")[0].strip()
        if used != "--":
            found.append((stats["Name"], parse_size(used)))
    return found


def cgroup_values(text: str) -> tuple[int | None, int | None]:
    """memory.peak and the number of OOM kills, as CGROUP prints them; None for a missing one."""
    peak = kills = None
    for line in text.splitlines():
        name, _, value = line.partition(" ")
        if name == "peak" and value.strip().isdigit():
            peak = int(value)
        elif name == "oom_kill" and value.strip().isdigit():
            kills = int(value)
    return peak, kills


# The columns that come from docker inspect, and those that come from CGROUP through docker exec.
FROM_INSPECT = frozenset({"limit", "restarts"})
FROM_CGROUP = frozenset({"since_start", "oom_kills"})


@dataclass(frozen=True)
class Row:
    service: str
    running: bool
    limit: int | None
    peak: int | None
    since_start: int | None
    oom_kills: int | None
    restarts: int
    # The columns that could not be read, which the table shows as "unread".
    unread: frozenset[str] = frozenset()


def make_row(
    container: dict[str, Any],
    details: dict[str, Any] | None,
    cgroup: str | None,
    peaks: dict[str, int],
) -> Row:
    """The row of a container: from docker compose ps, docker inspect, CGROUP and the samples.

    details is None when docker inspect failed, and cgroup when docker exec did."""
    unread = (FROM_INSPECT if details is None else frozenset()) | (
        FROM_CGROUP if cgroup is None else frozenset()
    )
    since_start, kills = cgroup_values(cgroup or "")
    details = details or {"HostConfig": {}}
    return Row(
        service=container["Service"],
        running=container["State"] == "running",
        limit=details["HostConfig"].get("Memory") or None,
        peak=peaks.get(container["Name"]),
        since_start=since_start,
        oom_kills=kills,
        restarts=details.get("RestartCount", 0),
        unread=unread,
    )


def mib(value: int | None) -> str:
    return "n/a" if value is None else f"{value / MIB:.0f} MiB"


def render(title: str, arch: str, rows: list[Row], skipped: int = 0) -> str:
    """The Markdown table of the rows, by service, with the total of the running services, and
    the number of lines of docker stats that were left out, if any."""
    lines = [
        f"### {title} ({arch})",
        "",
        "| Service | Limit | Peak | Since start | OOM kills | Restarts |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in sorted(rows, key=lambda row: row.service):
        cells = {
            "limit": "none" if row.limit is None else mib(row.limit),
            "since_start": mib(row.since_start),
            "oom_kills": "n/a" if row.oom_kills is None else str(row.oom_kills),
            "restarts": str(row.restarts),
        }
        cells.update(dict.fromkeys(row.unread, "unread"))
        lines.append(
            f"| {row.service} | {cells['limit']} | {mib(row.peak)} | {cells['since_start']} "
            f"| {cells['oom_kills']} | {cells['restarts']} |"
        )
    running = [row for row in rows if row.running]
    limits = [row.limit for row in running]
    if any("limit" in row.unread for row in running):
        total_limit = "unread"
    else:
        total_limit = "none" if None in limits else mib(sum(limits))  # type: ignore[arg-type]
    total_peak = mib(sum(row.peak or 0 for row in running))
    lines.append(f"| **Running services, total** | **{total_limit}** | **{total_peak}** | | | |")
    if skipped:
        lines += ["", f"{skipped} lines of docker stats could not be read and were left out."]
    return "\n".join(lines) + "\n"


def docker(*args: str) -> str:
    # The arguments are fixed or come from docker itself, and docker comes from PATH, as in every
    # other command of the stack.
    return subprocess.run(  # noqa: S603
        ["docker", *args],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    ).stdout


def project_containers() -> list[dict[str, Any]]:
    """The containers of the Compose project in the checkout, as docker compose ps lists them."""
    output = docker("compose", "ps", "--all", "--format", "json").strip()
    if output.startswith("["):
        return json.loads(output)
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def collect(peaks: dict[str, int]) -> list[Row]:
    """A row for each container of the project. A container can stop between docker compose ps
    and docker inspect or docker exec, such as one that the kernel stopped at its limit: its row
    then says which cells could not be read, and the table still comes out."""
    rows = []
    for container in project_containers():
        details: dict[str, Any] | None = None
        try:
            [details] = json.loads(docker("inspect", container["ID"]))
        except (subprocess.SubprocessError, ValueError):
            details = None
        cgroup: str | None = ""
        if container["State"] == "running":
            try:
                cgroup = docker("exec", container["ID"], "sh", "-c", CGROUP)
            except subprocess.SubprocessError:
                cgroup = None
        rows.append(make_row(container, details, cgroup, peaks))
    return rows


class Sampler:
    """docker stats in the background, keeping the highest sample of each container."""

    def __init__(self) -> None:
        self.peaks: dict[str, int] = {}
        # The lines that could not be read: a sample less is better than no samples from then on.
        self.skipped = 0
        self.process = subprocess.Popen(
            ["docker", "stats", "--format", "{{json .}}"],  # noqa: S607
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self) -> None:
        if self.process.stdout is None:
            return
        for line in self.process.stdout:
            try:
                found = samples(line)
            except (ValueError, KeyError, TypeError, AttributeError):
                self.skipped += 1
                continue
            for name, used in found:
                self.peaks[name] = max(used, self.peaks.get(name, 0))

    def stop(self) -> dict[str, int]:
        self.process.terminate()
        self.process.wait(timeout=30)
        self.thread.join(timeout=30)
        return dict(self.peaks)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--title", required=True, help="the heading of the table")
    parser.add_argument("--seconds", type=float, default=0, help="how long to sample after")
    parser.add_argument("--output", type=Path, help="append the table to this file")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- and the command to run")
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command and args.seconds <= 0:
        parser.error("give a command after --, or --seconds")
    sampler = Sampler()
    code = 0
    try:
        if command:
            code = subprocess.run(command, check=False).returncode  # noqa: S603
        time.sleep(args.seconds)
    finally:
        peaks = sampler.stop()
    arch = docker("info", "--format", "{{.Architecture}}").strip()
    table = render(args.title, arch, collect(peaks), sampler.skipped)
    print(table, end="")
    if args.output:
        with args.output.open("a", encoding="utf-8") as summary:
            summary.write(table + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
