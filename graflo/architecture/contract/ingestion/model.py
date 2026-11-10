"""Ingestion model definitions and runtime preparation helpers."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from pydantic import Field as PydanticField
from pydantic import PrivateAttr, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.edge_derivation import EdgeDerivationRegistry
from graflo.onto import (
    DEFAULT_ENDPOINT_AMBIGUITY,
    DBType,
    EndpointAmbiguityPolicy,
)

from .resource import (
    ResourceConfig,
    collect_vertex_names_from_pipeline,
    iter_steps,
)
from .steps.models import (
    ActorConfig,
    DescendActorConfig,
    EdgeActorConfig,
    VertexActorConfig,
    VertexRouterActorConfig,
)
from .sync import sync_errors
from .transform import ProtoTransform

if TYPE_CHECKING:
    # Runtime types are imported lazily: the declarative contract must stay
    # importable without the pipeline runtime (see finish_init).
    from graflo.architecture.pipeline.runtime.resource import ResourceRuntime
    from graflo.architecture.schema import CoreSchema


class IngestionModel(ConfigBaseModel):
    """Ingestion model (C): resources and transform registry."""

    endpoints_on_ambiguous: EndpointAmbiguityPolicy = PydanticField(
        default=DEFAULT_ENDPOINT_AMBIGUITY,
        description=(
            "What endpoint resolution does when a secondary identity matches more "
            "than one vertex. Secondary identities are only softly unique, so this "
            "is a data property rather than an error: 'all' (default) attaches the "
            "edge to every match, 'first' to one, 'skip' writes no edge and counts "
            "it, 'error' aborts. An edge step may override it via on_ambiguous. "
            "Endpoints matched by primary identity are unaffected."
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
        # Deferred: the single seam where the declarative model builds runtime state.
        from graflo.architecture.pipeline.runtime.resource import ResourceRuntime

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
        if any(config.sync is not None for config in self.resources):
            errors = sync_errors(
                self.resources,
                {name: rt.collect_vertex_names() for name, rt in runtimes.items()},
                core_schema,
            )
            if errors:
                raise ValueError("sync ownership: " + "; ".join(errors))

    @property
    def transforms_by_name(self) -> dict[str, ProtoTransform]:
        """The named transforms a ``call.use`` step can reference."""
        return dict(self._transforms)

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
        """Drop the steps that name a disconnected vertex, then the resources left empty.

        A ``vertex`` step of such a vertex, and an edge step with such an end,
        is removed at any level; a ``vertex_router`` is kept, since it routes
        whatever classes remain. A resource none of whose remaining steps names
        a vertex is dropped.
        """
        if disconnected is None:
            disconnected = (
                core_schema.vertex_config.vertex_set - core_schema.edge_config.vertices
            )
        if not disconnected:
            return
        gone: set[str] = disconnected

        def names_disconnected(step: ActorConfig) -> bool:
            if isinstance(step, VertexActorConfig):
                return step.vertex in gone
            if isinstance(step, EdgeActorConfig):
                return any(
                    end in gone
                    for intent in step.intents()
                    for end in (intent.source, intent.target)
                    if end is not None
                )
            return False

        def prune(steps: list[ActorConfig]) -> list[ActorConfig]:
            kept: list[ActorConfig] = []
            for step in steps:
                if isinstance(step, DescendActorConfig):
                    kept.append(
                        step.model_copy(update={"pipeline": prune(step.pipeline)})
                    )
                elif not names_disconnected(step):
                    kept.append(step)
            return kept

        resources: list[ResourceConfig] = []
        for resource_config in self.resources:
            pipeline = prune(resource_config.pipeline)
            if not collect_vertex_names_from_pipeline(pipeline) and not any(
                isinstance(step, VertexRouterActorConfig)
                for step in iter_steps(pipeline)
            ):
                self._runtimes.pop(resource_config.name, None)
                continue
            resources.append(resource_config.model_copy(update={"pipeline": pipeline}))
        self.resources = resources
        self._rebuild_config_state()
