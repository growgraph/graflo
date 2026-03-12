"""Graph schema and ingestion model definitions.

This module defines:
    - `Schema`: logical graph + DB profile (A+B)
    - `IngestionModel`: ingestion resources and transforms (C)
"""

from __future__ import annotations

import logging
import pathlib
import re
from collections import Counter
from typing import Any

import yaml
from pydantic import (
    Field as PydanticField,
    PrivateAttr,
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
from graflo.architecture.resource import Resource
from graflo.architecture.transform import ProtoTransform
from graflo.architecture.vertex import VertexConfig
from graflo.onto import DBType

logger = logging.getLogger(__name__)


_SEMVER_RE = re.compile(
    r"^\d+\.\d+\.\d+"
    r"(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?"
    r"(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$"
)


def _split_root_config(
    data: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split full config into Schema and IngestionModel payloads."""
    if not isinstance(data, dict):
        raise TypeError("Configuration payload must be a mapping")

    top_level_keys = set(data.keys())
    allowed_root_keys = {"metadata", "graph", "db_profile", "ingestion_model"}
    unknown_root_keys = top_level_keys - allowed_root_keys
    if unknown_root_keys:
        unknown_s = ", ".join(sorted(unknown_root_keys))
        raise ValueError(
            f"Unknown top-level keys: {unknown_s}. "
            "Allowed keys: metadata, graph, db_profile, ingestion_model."
        )

    if "ingestion_model" not in data:
        raise ValueError(
            "Missing required 'ingestion_model' section in config payload."
        )

    if "metadata" not in data:
        raise ValueError("Missing required 'metadata' section in config payload.")
    if "graph" not in data:
        raise ValueError("Missing required 'graph' section in config payload.")

    ingestion_model = data["ingestion_model"]
    if not isinstance(ingestion_model, dict):
        raise TypeError("'ingestion_model' must be a mapping")
    allowed_ingestion_keys = {"resources", "transforms"}
    unknown_ingestion_keys = set(ingestion_model.keys()) - allowed_ingestion_keys
    if unknown_ingestion_keys:
        unknown_s = ", ".join(sorted(unknown_ingestion_keys))
        raise ValueError(
            f"Unknown keys under ingestion_model: {unknown_s}. "
            "Allowed keys: resources, transforms."
        )

    schema_payload = {
        "metadata": data["metadata"],
        "graph": data["graph"],
        "db_profile": data.get("db_profile", {}),
    }
    ingestion_payload = {
        "resources": ingestion_model.get("resources", []),
        "transforms": ingestion_model.get("transforms", {}),
    }
    return schema_payload, ingestion_payload


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
        self.edge_config.finish_init(self.vertex_config)

    def remove_disconnected_vertices(self) -> set[str]:
        """Remove disconnected vertices and return removed names."""
        connected = self.edge_config.vertices
        disconnected = self.vertex_config.vertex_set - connected
        if disconnected:
            self.vertex_config.remove_vertices(disconnected)
        return disconnected


class IngestionModel(ConfigBaseModel):
    """Ingestion model (C): resources and transform registry."""

    resources: list[Resource] = PydanticField(
        default_factory=list,
        description="List of resource definitions (data pipelines mapping to vertices/edges).",
    )
    transforms: dict[str, ProtoTransform] = PydanticField(
        default_factory=dict,
        description="Dictionary of named transforms available to resources (name -> ProtoTransform).",
    )

    _resources: dict[str, Resource] = PrivateAttr()

    @classmethod
    def from_config(cls, data: dict[str, Any]) -> "IngestionModel":
        """Load ingestion model from a canonical root config payload."""
        _, ingestion_payload = _split_root_config(data)
        return cls.model_validate(ingestion_payload)

    @model_validator(mode="after")
    def _init_model(self) -> "IngestionModel":
        """Set transform names and build resource lookup map."""
        for name, t in self.transforms.items():
            t.name = name
        self._rebuild_resource_map()
        return self

    def _rebuild_resource_map(self) -> None:
        """Validate resource name uniqueness and refresh lookup map."""
        names = [r.name for r in self.resources]
        c = Counter(names)
        for k, v in c.items():
            if v > 1:
                raise ValueError(f"resource name {k} used {v} times")
        object.__setattr__(self, "_resources", {r.name: r for r in self.resources})

    def finish_init(self, graph: GraphModel) -> None:
        """Initialize resources against graph model and transform library."""
        self._rebuild_runtime_state()
        for r in self.resources:
            r.finish_init(
                vertex_config=graph.vertex_config,
                edge_config=graph.edge_config,
                transforms=self.transforms,
            )

    def _rebuild_runtime_state(self) -> None:
        """Rebuild transform names and name lookup map."""
        for name, t in self.transforms.items():
            t.name = name
        self._rebuild_resource_map()

    def fetch_resource(self, name: str | None = None) -> Resource:
        """Fetch a resource by name or get the first available resource.

        Args:
            name: Optional name of the resource to fetch

        Returns:
            Resource: The requested resource

        Raises:
            ValueError: If the requested resource is not found or if no resources exist
        """
        _current_resource = None

        if name is not None:
            if name in self._resources:
                _current_resource = self._resources[name]
            else:
                raise ValueError(f"Resource {name} not found")
        else:
            if self._resources:
                _current_resource = self.resources[0]
            else:
                raise ValueError("Empty resource container 😕")
        return _current_resource

    def prune_to_graph(
        self, graph: GraphModel, disconnected: set[str] | None = None
    ) -> None:
        """Drop resource actors that reference disconnected vertices."""
        if disconnected is None:
            disconnected = graph.vertex_config.vertex_set - graph.edge_config.vertices
        if not disconnected:
            return

        def _mentions_disconnected(wrapper) -> bool:
            return bool(wrapper.actor.references_vertices() & disconnected)

        to_drop: list[Resource] = []
        for resource in self.resources:
            root = resource.root
            if _mentions_disconnected(root):
                to_drop.append(resource)
                continue
            root.remove_descendants_if(_mentions_disconnected)
            if not any(a.references_vertices() for a in root.collect_actors()):
                to_drop.append(resource)

        for r in to_drop:
            self.resources.remove(r)
            self._resources.pop(r.name, None)


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
    _ingestion_model: IngestionModel | None = PrivateAttr(default=None)

    @model_validator(mode="before")
    @classmethod
    def _reject_ingestion_keys(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        invalid_keys = {"ingestion_model", "resources", "transforms"} & set(data.keys())
        if invalid_keys:
            invalid_s = ", ".join(sorted(invalid_keys))
            raise ValueError(
                "Schema payload must contain only metadata/graph/db_profile. "
                f"Found ingestion key(s): {invalid_s}. "
                "Load ingestion via IngestionModel.from_config(config)."
            )
        return data

    @classmethod
    def from_config(cls, data: dict[str, Any]) -> "Schema":
        """Load schema from a canonical root config payload."""
        schema_payload, _ = _split_root_config(data)
        return cls.from_dict(schema_payload)

    @model_validator(mode="after")
    def _init_schema(self) -> "Schema":
        self.finish_init()
        return self

    def finish_init(self) -> None:
        self.graph.finish_init()

    def remove_disconnected_vertices(self) -> set[str]:
        return self.graph.remove_disconnected_vertices()

    def bind_ingestion_model(self, ingestion_model: IngestionModel) -> None:
        ingestion_model.finish_init(self.graph)
        object.__setattr__(self, "_ingestion_model", ingestion_model)

    @property
    def ingestion_model(self) -> IngestionModel | None:
        return self._ingestion_model

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
