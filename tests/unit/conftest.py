"""Fixtures for the unit tests of ./agoranomos.

The cli fixture copies the script and .env.example into a temporary checkout whose path contains a
space, and puts stand-ins for docker, lsof and df first on PATH. The stand-ins answer from FAKE_*
environment variables, which a test passes to Cli.run, and the docker stand-in logs every call.
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

FAKE_LSOF = r"""#!/bin/sh
port=""
for arg in "$@"; do
  case "$arg" in -iTCP:*) port=${arg#-iTCP:} ;; esac
done
case " ${FAKE_BUSY_PORTS:-} " in *" $port "*) exit 0 ;; esac
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
    for name, body in {"docker": FAKE_DOCKER, "lsof": FAKE_LSOF, "df": FAKE_DF}.items():
        path = bin_dir / name
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)
    return Cli(root=root, bin=bin_dir, log=tmp_path / "docker.log")
