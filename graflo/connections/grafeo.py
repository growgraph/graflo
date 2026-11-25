"""Configuration for Grafeo, an embedded graph database run inside this process."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from graflo.connections.onto import DBConfig
from graflo.onto import DBType


class GrafeoConfig(DBConfig):
    """A Grafeo database: one file, or memory, holding named graphs.

    Grafeo runs in-process, so there is no server, URI, port or credential.
    ``path`` names the database file; without it the database lives in memory
    for the life of the process. ``database`` names the graph inside it, the
    target namespace, as a graph name does for FalkorDB.

    Grafeo structure: database (file or memory) -> named graph -> nodes/relationships
    Unified model: connection -> schema -> entities
    """

    model_config = SettingsConfigDict(
        env_prefix="GRAFEO_",
        case_sensitive=False,
    )

    db_type: Literal[DBType.GRAFEO] = DBType.GRAFEO  # ty: ignore[incompatible-mutable-override]

    path: Path | None = Field(
        default=None,
        description=(
            "Database file; created on first open. Unset: an in-memory database "
            "shared by every connection of this process."
        ),
    )

    def _get_default_port(self) -> int:
        return 0

    def _get_effective_database(self) -> str | None:
        return None

    def _get_effective_schema(self) -> str | None:
        return self.database

    @classmethod
    def from_docker_env(cls, docker_dir: str | Path | None = None) -> GrafeoConfig:
        raise NotImplementedError(
            "Grafeo is embedded and has no container; set path (or leave it unset "
            "for an in-memory database) instead."
        )
