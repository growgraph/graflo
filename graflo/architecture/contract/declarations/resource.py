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
from graflo.architecture.pipeline.runtime.actor.config.normalize import (
    normalize_actor_step,
)
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig
from graflo.architecture.graph_types import ResourceCastResult
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


def _collect_vertex_names_from_pipeline(steps: list[Any]) -> set[str]:
    """Collect vertex names referenced by pipeline steps (including nested descend)."""
    names: set[str] = set()
    for step in steps:
        if not isinstance(step, dict):
            continue
        normalized = normalize_actor_step(dict(step))
        step_type = normalized.get("type")
        if step_type == "vertex" and isinstance(normalized.get("vertex"), str):
            names.add(normalized["vertex"])
        elif step_type == "vertex_router":
            type_map = normalized.get("type_map")
            if isinstance(type_map, dict):
                for value in type_map.values():
                    if isinstance(value, str):
                        names.add(value)
            vertex_from_map = normalized.get("vertex_from_map")
            if isinstance(vertex_from_map, dict):
                for key in vertex_from_map:
                    if isinstance(key, str):
                        names.add(key)
        elif step_type == "edge":
            source = normalized.get("source") or normalized.get("from")
            target = normalized.get("target") or normalized.get("to")
            if isinstance(source, str):
                names.add(source)
            if isinstance(target, str):
                names.add(target)
            vertex_weights = normalized.get("vertex_weights")
            if isinstance(vertex_weights, list):
                for weight in vertex_weights:
                    if isinstance(weight, dict) and isinstance(weight.get("name"), str):
                        names.add(weight["name"])
        elif step_type == "descend":
            sub_pipeline = normalized.get("pipeline")
            if isinstance(sub_pipeline, list):
                names |= _collect_vertex_names_from_pipeline(sub_pipeline)
    return names


def _filter_vertex_config_for_resource(
    vertex_config: VertexConfig,
    *,
    resource_vertex_names: set[str],
    allowed_vertex_names: set[str] | None,
) -> VertexConfig:
    """Derive a filtered VertexConfig for runtime actor execution.

    Only vertices named in *resource_vertex_names* are retained (typically vertices
    declared in the resource pipeline). When *allowed_vertex_names* is set, the
    result is further restricted to that ingestion-level subset.
    """
    if resource_vertex_names:
        effective = set(resource_vertex_names)
        if allowed_vertex_names is not None:
            effective &= allowed_vertex_names
    elif allowed_vertex_names is not None:
        # Dynamic vertex_router steps (no type_map) declare no static vertex names.
        effective = set(allowed_vertex_names)
    else:
        return vertex_config
    filtered_vertices = [v for v in vertex_config.vertices if v.name in effective]
    filtered_force_types = {
        name: types
        for name, types in vertex_config.force_types.items()
        if name in effective
    }
    return VertexConfig(
        vertices=filtered_vertices,
        force_types=filtered_force_types,
        identity_from_all_properties=vertex_config.identity_from_all_properties,
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
            "If True, remove top-level input keys whose value is None or the empty string before "
            "the actor pipeline runs. Only the outer dict is filtered: nested dicts and list "
            "elements are left unchanged, and keys whose values are containers (dict/list) are "
            "kept even when empty. Numeric 0 and boolean False are kept. Use with wide or "
            "sparse tabular rows so VertexActor projection sees fewer irrelevant columns."
        ),
    )
    skip_actors_on_missing_input_keys: bool | None = PydanticField(
        default=None,
        description=(
            "If True, actors that declare required input keys may skip execution when keys are "
            "missing in the current document instead of raising indexing errors. "
            "If None, defaults to drop_trivial_input_fields."
        ),
    )
    tolerate_transform_errors: bool = PydanticField(
        default=True,
        description=(
            "If True, a failing transform step sets its declared output fields to None, "
            "records the error, and continues the pipeline. If False, transform errors "
            "abort the document as today."
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

    def collect_vertex_names(self) -> set[str]:
        """Vertex types referenced by this resource (pipeline and related config)."""
        names = _collect_vertex_names_from_pipeline(self.pipeline)
        names.update(self.merge_collections)
        for spec in self.infer_edge_only:
            names.add(spec.source)
            names.add(spec.target)
        for spec in self.infer_edge_except:
            names.add(spec.source)
            names.add(spec.target)
        for entry in self.extra_weights:
            names.add(entry.edge.source)
            names.add(entry.edge.target)
            for weight in entry.vertex_weights:
                if weight.name is not None:
                    names.add(weight.name)
        return names

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
        # Dynamic EdgeActors (ea.edge is None) resolve types at row time;
        # exclude them from static inference suppression.
        return {
            (ea.edge.source, ea.edge.target, None)
            for ea in edge_actors
            if ea.edge is not None
        }

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
        # use a resource-scoped runtime vertex_config for actor execution.
        runtime_vertex_config = _filter_vertex_config_for_resource(
            vertex_config,
            resource_vertex_names=self.collect_vertex_names(),
            allowed_vertex_names=allowed_vertex_names,
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
        skip_on_missing_input_keys = (
            self.skip_actors_on_missing_input_keys
            if self.skip_actors_on_missing_input_keys is not None
            else self.drop_trivial_input_fields
        )
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
            skip_actors_on_missing_input_keys=skip_on_missing_input_keys,
            tolerate_transform_errors=self.tolerate_transform_errors,
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

    def cast_document(self, doc: dict) -> ResourceCastResult:
        """Process a document and return entities plus any tolerated transform failures."""
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
        return ResourceCastResult(
            entities=result.entities,
            transform_failures=list(extraction_ctx.transform_failures),
        )

    def __call__(self, doc: dict) -> defaultdict[GraphEntity, list]:
        """Process a document through the resource pipeline.

        Args:
            doc: Document to process

        Returns:
            defaultdict[GraphEntity, list]: Processed graph entities
        """
        return self.cast_document(doc).entities

    def count(self) -> int:
        """Total number of actors in the resource pipeline."""
        return self.root.count()
