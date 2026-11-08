"""Where connection settings come from outside a config file.

- The ``conn_proxy`` → environment-variable naming rule: every config resolved
  from the environment for a ``conn_proxy`` label reads ``{PREFIX}<FIELD>``,
  where *PREFIX* comes from :func:`proxy_env_prefix` unless the caller passes
  one explicitly.
- The settings of the bundled docker services: :func:`docker_env` reads
  ``docker/<service>/.env.example``, the file the compose scripts pass with
  ``--env-file``, so the containers and the configs agree.
"""

from __future__ import annotations

import re
from pathlib import Path

DOCKER_ROOT = Path(__file__).resolve().parents[2] / "docker"
"""The repository's ``docker/`` directory: one compose project per service."""

DOCKER_ENV_FILE = ".env.example"
"""The settings file of each docker service, read by compose and by graflo."""

_REF = re.compile(r"\$\{(\w+)\}")


def proxy_env_prefix(conn_proxy: str) -> str:
    """The environment-variable prefix of a ``conn_proxy`` label.

    ``user_service`` → ``USER_SERVICE_``; ``billing-api`` → ``BILLING_API_``.
    """
    return conn_proxy.upper().replace("-", "_") + "_"


def parse_env_file(path: Path) -> dict[str, str]:
    """``KEY=value`` lines of *path*, quotes stripped, ``${KEY}`` expanded.

    A reference expands to a key set on an earlier line, as compose does; any
    other reference is left as written. Blank lines and ``#`` comments are
    skipped.
    """
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        values[key.strip()] = _REF.sub(
            lambda m: values.get(m.group(1), m.group(0)), value
        )
    return values


def docker_env(
    service: str, docker_dir: str | Path | None = None, *, required: bool = True
) -> dict[str, str]:
    """The settings of a bundled docker service.

    Args:
        service: The folder under ``docker/``, e.g. ``"neo4j"``.
        docker_dir: Another folder holding the settings file, instead.
        required: Raise when the file is missing; otherwise return ``{}``.

    Raises:
        FileNotFoundError: The settings file is missing and *required*.
    """
    folder = Path(docker_dir) if docker_dir is not None else DOCKER_ROOT / service
    path = folder / DOCKER_ENV_FILE
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Docker settings file not found: {path}")
        return {}
    return parse_env_file(path)
