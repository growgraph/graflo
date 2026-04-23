"""Full graph schema document (metadata + core + DB profile)."""

from __future__ import annotations

import pathlib
import re
from collections.abc import Callable, Mapping
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

NameTransform = Mapping[str, str] | Callable[[str], str]


def _build_name_transformer(
    transform: NameTransform | None, *, label: str
) -> Callable[[str], str]:
    if transform is None:
        return lambda value: value
    if isinstance(transform, Mapping):
        return lambda value: transform.get(value, value)
    if callable(transform):

        def _apply(value: str) -> str:
            renamed = transform(value)
            if not isinstance(renamed, str):
                raise TypeError(
                    f"{label} transform must return str, got {type(renamed).__name__}"
                )
            return renamed

        return _apply
    raise TypeError(f"{label} transform must be a mapping or callable")


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

    def rename_entities(
        self,
        *,
        vertices: NameTransform | None = None,
        edges: NameTransform | None = None,
    ) -> "Schema":
        """Return a schema copy with renamed vertex names and edge relations."""
        vertex_name = _build_name_transformer(vertices, label="vertices")
        edge_name = _build_name_transformer(edges, label="edges")

        payload = self.to_dict(skip_defaults=False)
        graph_payload = payload.get("core_schema")
        if isinstance(graph_payload, dict):
            vertex_config = graph_payload.get("vertex_config")
            if isinstance(vertex_config, dict):
                vertices_payload = vertex_config.get("vertices")
                if isinstance(vertices_payload, list):
                    for vertex in vertices_payload:
                        if isinstance(vertex, dict) and isinstance(
                            vertex.get("name"), str
                        ):
                            vertex["name"] = vertex_name(vertex["name"])

                blank_vertices = vertex_config.get("blank_vertices")
                if isinstance(blank_vertices, list):
                    vertex_config["blank_vertices"] = [
                        vertex_name(name) if isinstance(name, str) else name
                        for name in blank_vertices
                    ]

                force_types = vertex_config.get("force_types")
                if isinstance(force_types, dict):
                    vertex_config["force_types"] = {
                        vertex_name(name): value for name, value in force_types.items()
                    }

            edge_config = graph_payload.get("edge_config")
            if isinstance(edge_config, dict):
                edges_payload = edge_config.get("edges")
                if isinstance(edges_payload, list):
                    for edge in edges_payload:
                        if not isinstance(edge, dict):
                            continue
                        if isinstance(edge.get("source"), str):
                            edge["source"] = vertex_name(edge["source"])
                        if isinstance(edge.get("target"), str):
                            edge["target"] = vertex_name(edge["target"])
                        if isinstance(edge.get("relation"), str):
                            edge["relation"] = edge_name(edge["relation"])

        return Schema.from_dict(payload)

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
