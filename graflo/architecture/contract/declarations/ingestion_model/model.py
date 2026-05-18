"""Ingestion model definitions and runtime preparation helpers."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Literal

from pydantic import Field as PydanticField, PrivateAttr, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.pipeline.runtime.actor import ActorWrapper
from graflo.onto import DBType

from ..edge_derivation_registry import EdgeDerivationRegistry
from ..resource import ResourceConfig
from ..resource_runtime import ResourceRuntime
from ..transform import ProtoTransform

if TYPE_CHECKING:
    from graflo.architecture.schema import CoreSchema


class IngestionModel(ConfigBaseModel):
    """Ingestion model (C): resources and transform registry."""

    edges_on_duplicate: Literal["ignore", "upsert"] = PydanticField(
        default="ignore",
        description=(
            "How batch edge writes tolerate an already-matching edge. Passed through to "
            ":meth:`~graflo.db.conn.Connection.insert_edges_batch` where the target backend "
            "supports it."
        ),
    )
    resources: list[ResourceConfig] = PydanticField(
        default_factory=list,
        description="List of resource definitions (data pipelines mapping to vertices/edges).",
    )
    transforms: list[ProtoTransform] = PydanticField(
        default_factory=list,
        description="List of named transforms available to resources.",
    )

    _resources: dict[str, ResourceConfig] = PrivateAttr()
    _runtimes: dict[str, ResourceRuntime] = PrivateAttr(default_factory=dict)
    _transforms: dict[str, ProtoTransform] = PrivateAttr(default_factory=dict)
    _combined_edge_derivation: EdgeDerivationRegistry = PrivateAttr(
        default_factory=EdgeDerivationRegistry
    )

    @model_validator(mode="after")
    def _init_model(self) -> IngestionModel:
        """Build transform and resource lookup maps."""
        self._rebuild_config_state()
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
        """Build per-resource runtimes against graph model and transform library."""
        self._rebuild_config_state()
        runtimes: dict[str, ResourceRuntime] = {}
        for config in self.resources:
            runtimes[config.name] = ResourceRuntime(
                config,
                vertex_config=core_schema.vertex_config,
                edge_config=core_schema.edge_config,
                transforms=self._transforms,
                strict_references=strict_references,
                dynamic_edge_feedback=dynamic_edge_feedback,
                allowed_vertex_names=allowed_vertex_names,
                target_db_flavor=target_db_flavor,
            )
        object.__setattr__(self, "_runtimes", runtimes)

    def _rebuild_config_state(self) -> None:
        """Rebuild transform and resource lookup maps."""
        self._rebuild_transform_map()
        self._rebuild_resource_map()

    def fetch_resource(self, name: str | None = None) -> ResourceRuntime:
        """Fetch an initialized runtime resource by name."""
        if name is not None:
            runtime = self._runtimes.get(name)
            if runtime is None:
                raise ValueError(f"Resource {name} not found")
            return runtime
        if self._runtimes:
            return next(iter(self._runtimes.values()))
        if self.resources:
            raise RuntimeError(
                "IngestionModel resources exist but runtimes were not built; "
                "call finish_init() first."
            )
        raise ValueError("Empty resource container :(")

    def fetch_resource_config(self, name: str) -> ResourceConfig:
        """Fetch declarative resource config by name."""
        config = self._resources.get(name)
        if config is None:
            raise ValueError(f"Resource {name} not found")
        return config

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

        def _mentions_disconnected(wrapper: ActorWrapper) -> bool:
            return bool(wrapper.actor.references_vertices() & disconnected)

        to_drop: list[ResourceConfig] = []
        for resource_config in self.resources:
            root = ActorWrapper(*resource_config.pipeline)
            if _mentions_disconnected(root):
                to_drop.append(resource_config)
                continue
            root.remove_descendants_if(_mentions_disconnected)
            if not any(a.references_vertices() for a in root.collect_actors()):
                to_drop.append(resource_config)

        for dropped in to_drop:
            self.resources.remove(dropped)
            self._resources.pop(dropped.name, None)
            self._runtimes.pop(dropped.name, None)
        if to_drop:
            self._rebuild_config_state()
