"""Graph schema definitions.

This module defines:
    - `Schema`: logical graph + DB profile (A+B)
"""

from __future__ import annotations

import logging
import pathlib
import re
import yaml
from pydantic import (
    Field as PydanticField,
    field_validator,
    model_validator,
)

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.db_aware import (
    EdgeConfigDBAware,
    SchemaDBAware,
    VertexConfigDBAware,
)
from graflo.architecture.database_features import DatabaseProfile
from graflo.architecture.edge import EdgeConfig
from graflo.architecture.vertex import VertexConfig
from graflo.onto import DBType

logger = logging.getLogger(__name__)


_SEMVER_RE = re.compile(
    r"^\d+\.\d+\.\d+"
    r"(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?"
    r"(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$"
)


class GraphMetadata(ConfigBaseModel):
    """Schema metadata and versioning information.

    Holds metadata about the schema, including its name, version, and
    description.  Used for schema identification and versioning.
    Suitable for LLM-generated schema constituents.
    """

    name: str = PydanticField(
        ...,
        description="Name of the schema (e.g. graph or database identifier).",
    )
    version: str | None = PydanticField(
        default=None,
        description="Semantic version of the schema (e.g. '1.0.0', '2.1.3-beta+build.42').",
    )
    description: str | None = PydanticField(
        default=None,
        description="Optional human-readable description of the schema.",
    )

    @field_validator("version")
    @classmethod
    def _validate_semver(cls, v: str | None) -> str | None:
        if v is not None and not _SEMVER_RE.match(v):
            raise ValueError(
                f"version '{v}' is not a valid semantic version "
                f"(expected MAJOR.MINOR.PATCH[-prerelease][+build])"
            )
        return v


class GraphModel(ConfigBaseModel):
    """Logical graph model (A): vertices and edges."""

    vertex_config: VertexConfig = PydanticField(
        ...,
        description="Configuration for vertex collections (vertices, identities, fields).",
    )
    edge_config: EdgeConfig = PydanticField(
        ...,
        description="Configuration for edge collections (edges, weights).",
    )

    @model_validator(mode="after")
    def _init_graph(self) -> "GraphModel":
        self.finish_init()
        return self

    def finish_init(self) -> None:
        self.vertex_config.finish_init()
        self._validate_edge_vertices_defined()
        self.edge_config.finish_init(self.vertex_config)

    def _validate_edge_vertices_defined(self) -> None:
        """Ensure all edge endpoints reference defined vertex names."""
        declared_vertices = self.vertex_config.vertex_set
        edge_vertices = self.edge_config.vertices
        undefined_vertices = edge_vertices - declared_vertices
        if undefined_vertices:
            undefined_vertices_list = sorted(undefined_vertices)
            declared_vertices_list = sorted(declared_vertices)
            raise ValueError(
                "edge_config references undefined vertices: "
                f"{undefined_vertices_list}. "
                f"Declared vertices: {declared_vertices_list}"
            )

    def remove_disconnected_vertices(self) -> set[str]:
        """Remove disconnected vertices and return removed names."""
        connected = self.edge_config.vertices
        disconnected = self.vertex_config.vertex_set - connected
        if disconnected:
            self.vertex_config.remove_vertices(disconnected)
        return disconnected


class Schema(ConfigBaseModel):
    """Graph schema (A+B): metadata, graph model, and DB profile."""

    metadata: GraphMetadata = PydanticField(
        ...,
        description="Schema metadata and versioning (name, version).",
    )
    graph: GraphModel = PydanticField(
        ...,
        description="Logical graph model (vertices + edges).",
    )
    db_profile: DatabaseProfile = PydanticField(
        default_factory=DatabaseProfile,
        description="Database-specific physical profile (secondary indexes, naming, etc.).",
    )

    @model_validator(mode="after")
    def _init_schema(self) -> "Schema":
        self.finish_init()
        return self

    def finish_init(self) -> None:
        self.graph.finish_init()

    def remove_disconnected_vertices(self) -> set[str]:
        return self.graph.remove_disconnected_vertices()

    def resolve_db_aware(self, db_flavor: DBType | None = None) -> SchemaDBAware:
        """Build DB-aware runtime wrappers without mutating logical schema."""
        if db_flavor is not None:
            self.db_profile.db_flavor = db_flavor

        vertex_db = VertexConfigDBAware(self.graph.vertex_config, self.db_profile)
        edge_db = EdgeConfigDBAware(self.graph.edge_config, vertex_db, self.db_profile)
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

        payload = self.to_dict(skip_defaults=exclude_defaults)
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
