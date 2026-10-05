"""Fixtures for the unit tests of ./agoranomos.

The cli fixture copies the script and .env.example into a temporary checkout whose path contains a
space, and puts stand-ins for docker, ss, netstat, lsof and df first on PATH. The stand-ins answer
from FAKE_* environment variables, which a test passes to Cli.run, and the docker stand-in logs
every call. Cli.stub replaces a stand-in, or adds a tool, for one test.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# Answers match the --format strings that ./agoranomos passes.
FAKE_DOCKER = r"""#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_LOG"
case "$1" in
  info)
    if [ -n "${FAKE_DOCKER_DOWN:-}" ]; then
      echo "Cannot connect to the Docker daemon" >&2
      exit 1
    fi
    printf '%s|%s|%s|%s|%s\n' "${FAKE_SERVER_VERSION:-29.0.0}" "${FAKE_ARCH:-aarch64}" \
      "${FAKE_NCPU:-8}" "${FAKE_MEMTOTAL:-10737418240}" "${FAKE_DOCKER_ROOT:-/nonexistent}"
    ;;
  compose)
    shift
    case "$1" in
      version)
        if [ -n "${FAKE_NO_COMPOSE:-}" ]; then exit 1; fi
        echo "${FAKE_COMPOSE_VERSION:-5.6.0}"
        ;;
      ps)
        case " $* " in
          *" --quiet "*) printf '%s' "${FAKE_RUNNING:-}" ;;
          *) if [ -n "${FAKE_PS_FILE:-}" ]; then cat "$FAKE_PS_FILE"; fi ;;
        esac
        ;;
      *) exit "${FAKE_COMPOSE_EXIT:-0}" ;;
    esac
    ;;
esac
"""

# ss -ltn, netstat -an and lsof answer for FAKE_BUSY_PORTS: a space-separated list of ports that
# have a listener. ss and netstat print it in their real formats (netstat in the macOS and BSD
# one, address.port, or with FAKE_NETSTAT_STYLE=linux in the one of net-tools, address:port). They
# also print sockets that are not listening, as netstat does: a connection that is waiting to
# close on port 8080 and one to a remote port 3000, and for every busy port a connection to that
# port on a remote host. The script must not count any of these. FAKE_NETSTAT_BLIND=1 makes netstat
# list no listening socket at all, which it has been seen to do in some process contexts on macOS:
# the other lines are still there.
FAKE_SS = r"""#!/bin/sh
echo "State     Recv-Q Send-Q Local Address:Port    Peer Address:Port Process"
echo "TIME-WAIT 0      0          127.0.0.1:8080       127.0.0.1:52001"
echo "ESTAB     0      0        192.168.1.5:52346     203.0.113.9:3000"
for port in ${FAKE_BUSY_PORTS:-}; do
  echo "LISTEN    0      4096       127.0.0.1:$port           0.0.0.0:*"
  echo "LISTEN    0      4096            [::]:$port              [::]:*"
  echo "ESTAB     0      0        192.168.1.5:52347     203.0.113.9:$port"
done
"""

FAKE_NETSTAT = r"""#!/bin/sh
if [ "${FAKE_NETSTAT_STYLE:-bsd}" = "linux" ]; then
  echo "Active Internet connections (servers and established)"
  echo "Proto Recv-Q Send-Q Local Address           Foreign Address         State"
  echo "tcp        0      0 127.0.0.1:8080          127.0.0.1:52001         TIME_WAIT"
  echo "tcp        0      0 192.168.1.5:52346       203.0.113.9:3000        ESTABLISHED"
  for port in ${FAKE_BUSY_PORTS:-}; do
    if [ -z "${FAKE_NETSTAT_BLIND:-}" ]; then
      echo "tcp        0      0 127.0.0.1:$port          0.0.0.0:*               LISTEN"
      echo "tcp6       0      0 :::$port                 :::*                    LISTEN"
    fi
    echo "tcp        0      0 192.168.1.5:52347       203.0.113.9:$port        ESTABLISHED"
  done
  echo "Active UNIX domain sockets (servers and established)"
  echo "Proto RefCnt Flags       Type       State         I-Node   Path"
  echo "unix  2      [ ACC ]     STREAM     LISTENING     12345    /run/docker.sock"
else
  echo "Active Internet connections (including servers)"
  echo "Proto Recv-Q Send-Q  Local Address          Foreign Address        (state)"
  echo "tcp4       0      0  127.0.0.1.8080         127.0.0.1.52001        TIME_WAIT"
  echo "tcp4       0      0  192.168.1.5.52346      203.0.113.9.3000       ESTABLISHED"
  for port in ${FAKE_BUSY_PORTS:-}; do
    if [ -z "${FAKE_NETSTAT_BLIND:-}" ]; then
      echo "tcp4       0      0  127.0.0.1.$port         *.*                    LISTEN"
      echo "tcp6       0      0  *.$port                 *.*                    LISTEN"
    fi
    echo "tcp4       0      0  192.168.1.5.52347      203.0.113.9.$port       ESTABLISHED"
  done
fi
"""

# lsof lists only the sockets of the current user. FAKE_LSOF_BLIND=1 models a listener of another
# user: lsof sees nothing, while ss and netstat still list it. Like the real one, it prints a
# COMMAND header and a line for the listener when it finds one, and nothing when it finds none.
FAKE_LSOF = r"""#!/bin/sh
[ -z "${FAKE_LSOF_BLIND:-}" ] || exit 1
port=""
for arg in "$@"; do
  case "$arg" in -iTCP:*) port=${arg#-iTCP:} ;; esac
done
case " ${FAKE_BUSY_PORTS:-} " in
  *" $port "*)
    echo "COMMAND   PID USER   FD   TYPE DEVICE SIZE/OFF NODE NAME"
    echo "python3 12345 user    3u  IPv4 0x1234      0t0  TCP 127.0.0.1:$port (LISTEN)"
    exit 0
    ;;
esac
exit 1
"""

FAKE_DF = r"""#!/bin/sh
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
echo "/dev/fake 999999999 1 ${FAKE_DISK_AVAILABLE_KB:-104857600} 1% /"
"""


@dataclass
class Cli:
    root: Path
    bin: Path
    log: Path

    @property
    def env_path(self) -> Path:
        return self.root / ".env"

    def run(
        self, *args: str, cwd: Path | None = None, **fake: str
    ) -> subprocess.CompletedProcess[str]:
        environment = {
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(self.root),
            "FAKE_LOG": str(self.log),
            **fake,
        }
        return subprocess.run(
            [str(self.root / "agoranomos"), *args],
            cwd=cwd or self.root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

    def stub(self, name: str, body: str) -> None:
        """Put a tool named name, a shell script with this body, first on PATH for this checkout."""
        path = self.bin / name
        path.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
        path.chmod(0o755)

    def read_env(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for line in self.env_path.read_text(encoding="utf-8").splitlines():
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key] = value
        return values

    def docker_calls(self) -> list[str]:
        return self.log.read_text(encoding="utf-8").splitlines() if self.log.exists() else []


@pytest.fixture
def cli(tmp_path: Path) -> Cli:
    root = tmp_path / "my stack"
    root.mkdir()
    for name in ("agoranomos", ".env.example"):
        shutil.copy2(REPO_ROOT / name, root / name)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stand_ins = {
        "docker": FAKE_DOCKER,
        "ss": FAKE_SS,
        "netstat": FAKE_NETSTAT,
        "lsof": FAKE_LSOF,
        "df": FAKE_DF,
    }
    for name, body in stand_ins.items():
        path = bin_dir / name
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)
    return Cli(root=root, bin=bin_dir, log=tmp_path / "docker.log")
