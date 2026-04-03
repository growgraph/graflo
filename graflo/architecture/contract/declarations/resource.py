"""Resource management and processing for graph databases.

This module provides the core resource handling functionality for graph databases.
It defines how data resources are processed, transformed, and mapped to graph
structures through a system of actors and transformations.

Key Components:
    - Resource: Main class for resource processing and transformation
    - ActorWrapper: Wrapper for processing actors
    - ActionContext: Context for processing actions

The resource system allows for:
    - Data encoding and transformation
    - Vertex and edge creation
    - Weight management
    - Collection merging
    - Type casting and validation
    - Dynamic vertex-type routing via VertexRouterActor in the pipeline

Example:
    >>> resource = Resource(
    ...     name="users",
    ...     pipeline=[{"vertex": "user"}, {"edge": {"from": "user", "to": "user"}}],
    ...     encoding=EncodingType.UTF_8
    ... )
    >>> result = resource(doc)
"""

from __future__ import annotations

import builtins
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable

from pydantic import AliasChoices, Field as PydanticField, PrivateAttr, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import (
    EdgeId,
    EncodingType,
    GraphEntity,
    Weight,
)
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig
from graflo.onto import DBType

from .edge_derivation_registry import EdgeDerivationRegistry
from .transform import ProtoTransform

if TYPE_CHECKING:
    from graflo.architecture.pipeline.runtime.actor import (
        ActorWrapper,
    )
    from graflo.architecture.pipeline.runtime.executor import ActorExecutor

logger = logging.getLogger(__name__)

_SAFE_TYPE_CASTERS: dict[str, Callable[..., Any]] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "bytes": bytes,
    "list": list,
    "dict": dict,
    "tuple": tuple,
    "set": set,
}


def _resolve_type_caster(name: str) -> Callable[..., Any] | None:
    """Resolve a type caster by name from a strict allowlist."""
    if not isinstance(name, str):
        return None
    candidate = _SAFE_TYPE_CASTERS.get(name)
    if candidate is not None:
        return candidate
    # Support "builtins.int" style entries without evaluating expressions.
    if "." in name:
        module_name, attr_name = name.split(".", 1)
        if module_name == "builtins":
            builtin_attr = getattr(builtins, attr_name, None)
            if callable(builtin_attr) and attr_name in _SAFE_TYPE_CASTERS:
                return _SAFE_TYPE_CASTERS[attr_name]
    return None


def _strip_trivial_top_level_fields(doc: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of *doc* without None or empty-string values."""
    return {k: v for k, v in doc.items() if v is not None and v != ""}


def _filter_vertex_config_by_allowed(
    vertex_config: VertexConfig,
    *,
    allowed_vertex_names: set[str] | None,
) -> VertexConfig:
    """Derive a filtered VertexConfig for runtime actor execution.

    This intentionally filters only the vertex collections present in
    *allowed_vertex_names*; it does not attempt to rewrite edge configs.
    """
    if allowed_vertex_names is None:
        return vertex_config

    allowed = allowed_vertex_names
    filtered_vertices = [v for v in vertex_config.vertices if v.name in allowed]
    filtered_blank_vertices = [b for b in vertex_config.blank_vertices if b in allowed]
    filtered_force_types = {
        name: types
        for name, types in vertex_config.force_types.items()
        if name in allowed
    }
    return VertexConfig(
        vertices=filtered_vertices,
        blank_vertices=filtered_blank_vertices,
        force_types=filtered_force_types,
    )


class EdgeInferSpec(ConfigBaseModel):
    """Selector for controlling inferred edge emission."""

    source: str = PydanticField(..., description="Edge source vertex name.")
    target: str = PydanticField(..., description="Edge target vertex name.")
    relation: str | None = PydanticField(
        default=None,
        description=(
            "Optional relation discriminator. If omitted, selector applies to all relations "
            "for (source, target)."
        ),
    )

    @property
    def edge_id(self) -> EdgeId:
        return self.source, self.target, self.relation

    def matches(self, edge_id: EdgeId) -> bool:
        source, target, relation = edge_id
        return (
            self.source == source
            and self.target == target
            and (self.relation is None or self.relation == relation)
        )


class ResourceExtraWeightEntry(ConfigBaseModel):
    """Schema edge plus optional vertex-derived weight rules for DB enrichment."""

    edge: Edge
    vertex_weights: list[Weight] = PydanticField(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _from_yaml(cls, data: Any) -> Any:
        if data is None:
            return data
        if isinstance(data, Edge):
            return {"edge": data, "vertex_weights": []}
        if not isinstance(data, dict):
            raise TypeError(
                f"extra_weights item must be dict or Edge, got {type(data)}"
            )
        d = dict(data)
        vw_raw = d.pop("vertex_weights", None) or []
        if not isinstance(vw_raw, list):
            vw_raw = [vw_raw]
        v_w = [Weight.model_validate(x) for x in vw_raw]
        if "edge" in d and isinstance(d["edge"], dict):
            edge = Edge.model_validate(dict(d.pop("edge")))
            if d:
                raise ValueError(
                    f"extra_weights entry has unexpected keys with 'edge': {sorted(d)}"
                )
            return {"edge": edge, "vertex_weights": v_w}
        edge = Edge.model_validate(d)
        return {"edge": edge, "vertex_weights": v_w}


class Resource(ConfigBaseModel):
    """Resource configuration and processing.

    Represents a data resource that can be processed and transformed into graph
    structures. Manages the processing pipeline through actors and handles data
    encoding, transformation, and mapping. Suitable for LLM-generated schema
    constituents.

    Dynamic vertex-type routing is handled by ``vertex_router`` steps in the
    pipeline (see :class:`~graflo.architecture.pipeline.runtime.actor.VertexRouterActor`).
    Per-row relationship labels and location matching for edges belong on
    ``edge`` pipeline steps (:class:`~graflo.architecture.edge_derivation.EdgeDerivation`),
    not on ``Resource``.
    """

    model_config = {"extra": "forbid"}

    name: str = PydanticField(
        ...,
        description="Name of the resource (e.g. table or file identifier).",
    )
    pipeline: list[dict[str, Any]] = PydanticField(
        ...,
        description="Pipeline of actor steps to apply in sequence (vertex, edge, transform, descend). "
        'Each step is a dict, e.g. {"vertex": "user"} or {"edge": {"from": "a", "to": "b"}}.',
        validation_alias=AliasChoices("pipeline", "apply"),
    )
    encoding: EncodingType = PydanticField(
        default=EncodingType.UTF_8,
        description="Character encoding for input/output (e.g. utf-8, ISO-8859-1).",
    )
    merge_collections: list[str] = PydanticField(
        default_factory=list,
        description="List of collection names to merge when writing to the graph.",
    )
    extra_weights: list[ResourceExtraWeightEntry] = PydanticField(
        default_factory=list,
        description="Additional edge attribute / vertex-weight enrichment for this resource.",
    )
    types: dict[str, str] = PydanticField(
        default_factory=dict,
        description='Field name to Python type expression for casting (e.g. {"amount": "float"}).',
    )
    infer_edges: bool = PydanticField(
        default=True,
        description=(
            "If True, infer edges from current vertex population. "
            "If False, emit only edges explicitly declared as edge actors in the pipeline."
        ),
    )
    infer_edge_only: list[EdgeInferSpec] = PydanticField(
        default_factory=list,
        description=(
            "Optional allow-list for inferred edges. Applies only to inferred (greedy) edges, "
            "not explicit edge actors."
        ),
    )
    infer_edge_except: list[EdgeInferSpec] = PydanticField(
        default_factory=list,
        description=(
            "Optional deny-list for inferred edges. Applies only to inferred (greedy) edges, "
            "not explicit edge actors."
        ),
    )
    drop_trivial_input_fields: bool = PydanticField(
        default=False,
        description=(
            "If True, drop top-level input keys whose value is None or '' before the pipeline runs. "
            "Does not recurse into nested dicts or lists. Default False."
        ),
    )

    _root: ActorWrapper = PrivateAttr()
    _types: dict[str, Callable[..., Any]] = PrivateAttr(default_factory=dict)
    _vertex_config: VertexConfig = PrivateAttr()
    _edge_config: EdgeConfig = PrivateAttr()
    _executor: ActorExecutor = PrivateAttr()
    _initialized: bool = PrivateAttr(default=False)
    _edge_derivation_registry: EdgeDerivationRegistry | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _build_root_and_types(self) -> Resource:
        """Build root ActorWrapper and resolve safe cast functions."""
        from graflo.architecture.pipeline.runtime.actor import ActorWrapper
        from graflo.architecture.pipeline.runtime.executor import ActorExecutor

        object.__setattr__(self, "_root", ActorWrapper(*self.pipeline))
        object.__setattr__(self, "_executor", ActorExecutor(self._root))
        object.__setattr__(self, "_types", {})
        for k, v in self.types.items():
            caster = _resolve_type_caster(v)
            if caster is not None:
                self._types[k] = caster
            else:
                logger.error(
                    "For resource %s for field %s failed to resolve cast type %s",
                    self.name,
                    k,
                    v,
                )
        # Placeholders until schema binds real configs.
        object.__setattr__(self, "_vertex_config", VertexConfig(vertices=[]))
        object.__setattr__(self, "_edge_config", EdgeConfig())
        object.__setattr__(self, "_initialized", False)
        self._validate_infer_edge_spec_policy()
        return self

    def _validate_infer_edge_spec_policy(self) -> None:
        if self.infer_edge_only and self.infer_edge_except:
            raise ValueError(
                "Resource infer_edge_only and infer_edge_except are mutually exclusive."
            )

    def _validate_infer_edge_spec_targets(self, edge_config: EdgeConfig) -> None:
        known_edge_ids = {edge_id for edge_id, _ in edge_config.items()}

        def _validate_list(field_name: str, specs: list[EdgeInferSpec]) -> None:
            unknown: list[EdgeId] = []
            for spec in specs:
                if not any(spec.matches(edge_id) for edge_id in known_edge_ids):
                    unknown.append(spec.edge_id)
            if unknown:
                raise ValueError(
                    f"Resource {field_name} contains unknown edge selectors: {unknown}"
                )

        _validate_list("infer_edge_only", self.infer_edge_only)
        _validate_list("infer_edge_except", self.infer_edge_except)

    @property
    def vertex_config(self) -> VertexConfig:
        """Vertex configuration (set by Schema.finish_init)."""
        return self._vertex_config

    @property
    def edge_config(self) -> EdgeConfig:
        """Edge configuration (set by Schema.finish_init)."""
        return self._edge_config

    @property
    def root(self) -> ActorWrapper:
        """Root actor wrapper for the processing pipeline."""
        return self._root

    def finish_init(
        self,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, ProtoTransform],
        *,
        strict_references: bool = False,
        dynamic_edge_feedback: bool = False,
        allowed_vertex_names: set[str] | None = None,
        target_db_flavor: DBType | None = None,
    ) -> None:
        """Complete resource initialization.

        Initializes the resource with vertex and edge configurations,
        and sets up the processing pipeline. Called by Schema after load.

        Args:
            vertex_config: Configuration for vertices
            edge_config: Configuration for edges
            transforms: Dictionary of available transforms
            target_db_flavor: Target graph DB flavor (for ingestion-time defaults, e.g. TigerGraph).
        """
        self._rebuild_runtime(
            vertex_config=vertex_config,
            edge_config=edge_config,
            transforms=transforms,
            strict_references=strict_references,
            dynamic_edge_feedback=dynamic_edge_feedback,
            allowed_vertex_names=allowed_vertex_names,
            target_db_flavor=target_db_flavor,
        )

    def _edge_ids_from_edge_actors(self) -> set[EdgeId]:
        """Collect (source, target, None) for every EdgeActor in this resource's pipeline.

        Used to auto-add to infer_edge_except so inferred edges do not duplicate
        edges produced by explicit edge actors.
        """
        from graflo.architecture.pipeline.runtime.actor import EdgeActor

        edge_actors = [
            a for a in self.root.collect_actors() if isinstance(a, EdgeActor)
        ]
        return {(ea.edge.source, ea.edge.target, None) for ea in edge_actors}

    def _validate_dynamic_edge_vertices_exist(
        self, vertex_config: VertexConfig
    ) -> None:
        """Ensure all vertices implied by dynamic edge controls are declared."""
        known_vertices = set(vertex_config.vertex_set)
        referenced_vertices: set[str] = set()

        for spec in self.infer_edge_only:
            referenced_vertices.add(spec.source)
            referenced_vertices.add(spec.target)

        for spec in self.infer_edge_except:
            referenced_vertices.add(spec.source)
            referenced_vertices.add(spec.target)

        for source, target, _ in self._edge_ids_from_edge_actors():
            referenced_vertices.add(source)
            referenced_vertices.add(target)

        missing_vertices = sorted(referenced_vertices - known_vertices)
        if missing_vertices:
            raise ValueError(
                "Resource dynamic edge references undefined vertices: "
                f"{missing_vertices}. "
                "Declare these vertices in vertex_config before using dynamic/inferred edges."
            )

    def _rebuild_runtime(
        self,
        *,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, ProtoTransform],
        strict_references: bool = False,
        dynamic_edge_feedback: bool = False,
        allowed_vertex_names: set[str] | None = None,
        target_db_flavor: DBType | None = None,
    ) -> None:
        """Rebuild runtime actor initialization state from typed context."""
        # Keep the full schema vertex_config for correctness validations, but
        # use the filtered runtime vertex_config for actor execution.
        runtime_vertex_config = _filter_vertex_config_by_allowed(
            vertex_config, allowed_vertex_names=allowed_vertex_names
        )
        object.__setattr__(self, "_vertex_config", runtime_vertex_config)
        # Runtime actors may register dynamic edges; keep per-resource edge state.
        local_edge_config = EdgeConfig.model_validate(
            edge_config.to_dict(skip_defaults=False)
        )
        object.__setattr__(self, "_edge_config", local_edge_config)
        self._validate_dynamic_edge_vertices_exist(vertex_config)
        self._validate_infer_edge_spec_targets(self._edge_config)

        baseline_edge_ids = {edge_id for edge_id, _ in edge_config.items()}
        infer_edge_except = {spec.edge_id for spec in self.infer_edge_except}
        # When not using infer_edge_only, auto-add (s,t,None) to infer_edge_except
        # for any edge type handled by explicit EdgeActors in this resource.
        if not self.infer_edge_only:
            infer_edge_except |= self._edge_ids_from_edge_actors()

        from graflo.architecture.pipeline.runtime.actor import ActorInitContext

        edge_derivation_registry = EdgeDerivationRegistry()
        object.__setattr__(self, "_edge_derivation_registry", edge_derivation_registry)

        logger.debug("total resource actor count : %s", self.root.count())
        init_ctx = ActorInitContext(
            vertex_config=runtime_vertex_config,
            edge_config=self._edge_config,
            transforms=transforms,
            edge_derivation=edge_derivation_registry,
            allowed_vertex_names=allowed_vertex_names,
            infer_edges=self.infer_edges,
            infer_edge_only={spec.edge_id for spec in self.infer_edge_only},
            infer_edge_except=infer_edge_except,
            strict_references=strict_references,
            target_db_flavor=target_db_flavor,
        )
        self.root.finish_init(init_ctx=init_ctx)
        object.__setattr__(self, "_initialized", True)

        if dynamic_edge_feedback:
            # Edge actors register static edge definitions into the resource-local edge
            # config during finish_init(). Optionally propagate newly discovered edges
            # to the shared schema-level edge_config so schema definition and DB
            # writers can see them.
            for edge_id, edge in self._edge_config.items():
                if edge_id in baseline_edge_ids:
                    continue
                edge_config.update_edges(
                    edge.model_copy(deep=True), vertex_config=vertex_config
                )

        logger.debug("total resource actor count (after finit): %s", self.root.count())

        reg = self._edge_derivation_registry
        for entry in self.extra_weights:
            entry.edge.finish_init(vertex_config)
            if reg is not None and entry.vertex_weights:
                reg.merge_vertex_weights(entry.edge.edge_id, entry.vertex_weights)

    def __call__(self, doc: dict) -> defaultdict[GraphEntity, list]:
        """Process a document through the resource pipeline.

        Args:
            doc: Document to process

        Returns:
            defaultdict[GraphEntity, list]: Processed graph entities
        """
        if not self._initialized:
            raise RuntimeError(
                f"Resource '{self.name}' must be initialized via finish_init() before use."
            )
        work_doc: dict[str, Any] = (
            _strip_trivial_top_level_fields(doc)
            if self.drop_trivial_input_fields
            else doc
        )
        extraction_ctx = self._executor.extract(work_doc)
        result = self._executor.assemble_result(extraction_ctx)
        return result.entities

    def count(self) -> int:
        """Total number of actors in the resource pipeline."""
        return self.root.count()
