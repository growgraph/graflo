"""Ingestion model definitions and runtime preparation helpers."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from pydantic import Field as PydanticField, PrivateAttr, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.onto import DBType

from ..resource import Resource
from ..transform import ProtoTransform

if TYPE_CHECKING:
    from graflo.architecture.schema import CoreSchema


class IngestionModel(ConfigBaseModel):
    """Ingestion model (C): resources and transform registry."""

    resources: list[Resource] = PydanticField(
        default_factory=list,
        description="List of resource definitions (data pipelines mapping to vertices/edges).",
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description="List of named transforms available to resources.",
    )

    _resources: dict[str, Resource] = PrivateAttr()
    _transforms: dict[str, ProtoTransform] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def _init_model(self) -> IngestionModel:
        """Build transform and resource lookup maps."""
        self._rebuild_runtime_state()
        return self

    def _rebuild_resource_map(self) -> None:
        """Validate resource name uniqueness and refresh lookup map."""
        names = [r.name for r in self.resources]
        c = Counter(names)
        for k, v in c.items():
            if v > 1:
                raise ValueError(f"resource name {k} used {v} times")
        object.__setattr__(self, "_resources", {r.name: r for r in self.resources})

    def _rebuild_transform_map(self) -> None:
        """Validate transform names and refresh name lookup map."""
        missing_names = [idx for idx, t in enumerate(self.transforms) if not t.name]
        if missing_names:
            raise ValueError(
                "All ingestion transforms must define a non-empty name. "
                f"Missing at indexes: {missing_names}"
            )

        transform_names = [t.name for t in self.transforms if t.name is not None]
        name_counts = Counter(transform_names)
        duplicates = sorted([name for name, count in name_counts.items() if count > 1])
        if duplicates:
            raise ValueError(f"Duplicate ingestion transform names found: {duplicates}")

        object.__setattr__(
            self,
            "_transforms",
            {t.name: t for t in self.transforms if t.name is not None},
        )

    def finish_init(
        self,
        core_schema: CoreSchema,
        *,
        strict_references: bool = False,
        dynamic_edge_feedback: bool = False,
        allowed_vertex_names: set[str] | None = None,
        target_db_flavor: DBType | None = None,
    ) -> None:
        """Initialize resources against graph model and transform library."""
        self._rebuild_runtime_state()
        for r in self.resources:
            r.finish_init(
                vertex_config=core_schema.vertex_config,
                edge_config=core_schema.edge_config,
                transforms=self._transforms,
                strict_references=strict_references,
                dynamic_edge_feedback=dynamic_edge_feedback,
                allowed_vertex_names=allowed_vertex_names,
                target_db_flavor=target_db_flavor,
            )

    def _rebuild_runtime_state(self) -> None:
        """Rebuild transform and resource lookup maps."""
        self._rebuild_transform_map()
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
                raise ValueError("Empty resource container :(")
        return _current_resource

    def prune_to_graph(
        self, core_schema: CoreSchema, disconnected: set[str] | None = None
    ) -> None:
        """Drop resource actors that reference disconnected vertices."""
        if disconnected is None:
            disconnected = (
                core_schema.vertex_config.vertex_set - core_schema.edge_config.vertices
            )
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
