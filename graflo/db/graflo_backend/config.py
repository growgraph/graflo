"""Configuration for GraFlo file backend connections."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from graflo.db.connection.onto import DBConfig
from graflo.onto import DBType


class GraFloBackendConfig(DBConfig):
    """Configuration for a GraFlo on-disk graph backend directory."""

    model_config = SettingsConfigDict(
        env_prefix="GRAFLO_BACKEND_",
        case_sensitive=False,
    )

    output_dir: Path = Field(
        ...,
        description="Root directory for schema, index, and chunked data files.",
    )
    chunk_size: int = Field(
        default=50_000,
        ge=1,
        description="Maximum records per gzip JSONL chunk file.",
    )
    target_flavor_hint: DBType | None = Field(
        default=None,
        description=(
            "When set, schema written to disk is sanitized for this target DB flavor."
        ),
    )

    def _get_default_port(self) -> int:
        return 0

    def _get_effective_database(self) -> str | None:
        return None

    def _get_effective_schema(self) -> str | None:
        return self.output_dir.name

    @classmethod
    def from_docker_env(
        cls, docker_dir: str | Path | None = None
    ) -> GraFloBackendConfig:
        raise NotImplementedError(
            "GraFlo file backend does not support from_docker_env; use output_dir instead."
        )
