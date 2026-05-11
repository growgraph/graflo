"""Full graph schema document (metadata + core + DB profile)."""

from __future__ import annotations

import pathlib
import re
from typing import TYPE_CHECKING

import yaml
from pydantic import AliasChoices, Field as PydanticField, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.onto import DBType

if TYPE_CHECKING:
    from .db_aware import SchemaDBAware


class Schema(ConfigBaseModel):
    """Graph schema (A+B): metadata, core schema, and DB profile."""

    metadata: GraphMetadata = PydanticField(
        ...,
        description="Schema metadata and versioning (name, version).",
    )
    core_schema: CoreSchema = PydanticField(
        ...,
        description="Core schema model (vertices + edges).",
        validation_alias=AliasChoices("core_schema", "graph"),
    )
    db_profile: DatabaseProfile = PydanticField(
        default_factory=DatabaseProfile,
        description=(
            "Database-specific physical profile (secondary indexes, naming, TigerGraph GSQL "
            "DEFAULT overrides via default_property_values, etc.)."
        ),
    )

    @model_validator(mode="after")
    def _init_schema(self) -> Schema:
        self.finish_init()
        return self

    def finish_init(self) -> None:
        self.core_schema.finish_init()

    def remove_disconnected_vertices(self) -> set[str]:
        return self.core_schema.remove_disconnected_vertices()

    def resolve_db_aware(self, db_flavor: DBType | None = None) -> SchemaDBAware:
        """Build DB-aware runtime wrappers without mutating logical schema."""
        from .db_aware import (
            EdgeConfigDBAware,
            SchemaDBAware,
            VertexConfigDBAware,
        )

        if db_flavor is not None:
            self.db_profile.db_flavor = db_flavor

        vertex_db = VertexConfigDBAware(self.core_schema.vertex_config, self.db_profile)
        edge_db = EdgeConfigDBAware(
            self.core_schema.edge_config, vertex_db, self.db_profile
        )
        edge_db.compile_identity_indexes()
        return SchemaDBAware(
            vertex_config=vertex_db,
            edge_config=edge_db,
            db_profile=self.db_profile,
        )

    @staticmethod
    def _slug_filename_token(token: str) -> str:
        """Normalize arbitrary token into filename-safe slug."""
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", token.strip())
        return cleaned.strip("-") or "schema"

    def default_dump_filename(self) -> str:
        """Return default schema dump filename: <name>-<version>.yaml."""
        schema_name = self._slug_filename_token(self.metadata.name)
        version = (
            self.metadata.version
            if self.metadata.version is not None
            else "unversioned"
        )
        schema_version = self._slug_filename_token(version)
        return f"{schema_name}-{schema_version}.yaml"

    def dump(
        self,
        path: str | pathlib.Path | None = None,
        *,
        exclude_defaults: bool = True,
    ) -> pathlib.Path:
        """Dump schema YAML to path, excluding defaults by default.

        If path is omitted, writes into current working directory using
        `<schema_name>-<version>.yaml`.
        """
        if path is None:
            target_path = pathlib.Path.cwd() / self.default_dump_filename()
        else:
            target_path = pathlib.Path(path)
            if target_path.is_dir():
                target_path = target_path / self.default_dump_filename()

        if exclude_defaults:
            payload = self.to_minimal_canonical_dict()
        else:
            payload = self.to_dict(skip_defaults=False)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            yaml.safe_dump(
                payload,
                default_flow_style=False,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return target_path
