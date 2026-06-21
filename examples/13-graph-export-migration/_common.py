"""Shared helpers for example 13 (GraFlo file backend)."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import click

from graflo.db import ArangoConfig, Neo4jConfig, PostgresConfig
from graflo.db.graflo_backend.config import GraFloBackendConfig

EXAMPLE_DIR = Path(__file__).resolve().parent
DEFAULT_CSV_BACKEND_DIR = EXAMPLE_DIR / "artifacts" / "csv-backend"
DEFAULT_NEO4J_BACKEND_DIR = EXAMPLE_DIR / "artifacts" / "neo4j-backend"


def neo4j_config() -> Neo4jConfig:
    try:
        return Neo4jConfig.from_docker_env()
    except Exception:
        return Neo4jConfig.from_env()


def arango_config() -> ArangoConfig:
    try:
        return ArangoConfig.from_docker_env()
    except Exception:
        return ArangoConfig.from_env()


def postgres_config() -> PostgresConfig:
    try:
        return PostgresConfig.from_docker_env()
    except Exception:
        return PostgresConfig.from_env()


def backend_config(output_dir: str | Path) -> GraFloBackendConfig:
    return GraFloBackendConfig(output_dir=Path(output_dir))


def resolve_source_backend(
    from_backend: Path | None,
) -> Neo4jConfig | GraFloBackendConfig:
    if from_backend is not None:
        return backend_config(from_backend)
    return neo4j_config()


@contextmanager
def example_workdir() -> Iterator[Path]:
    """Manifest file connectors use ``sub_path: data`` relative to this example."""
    previous_cwd = os.getcwd()
    try:
        os.chdir(EXAMPLE_DIR)
        yield EXAMPLE_DIR
    finally:
        os.chdir(previous_cwd)


def add_migration_limits_options(func: Callable[..., Any]) -> Callable[..., Any]:
    func = click.option(
        "--sample-limit",
        type=int,
        default=100,
        help="Property samples per type during schema inference.",
    )(func)
    func = click.option(
        "--data-limit",
        type=int,
        default=None,
        help="Optional cap on rows fetched per vertex/edge type.",
    )(func)
    return func
