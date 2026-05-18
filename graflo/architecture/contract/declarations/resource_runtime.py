"""Runtime resource executor (schema-bound, not serializable)."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Callable

from graflo.architecture.graph_types import EdgeId, GraphEntity, ResourceCastResult
from graflo.architecture.pipeline.runtime.actor import (
    ActorInitContext,
    ActorWrapper,
    EdgeActor,
)
from graflo.architecture.pipeline.runtime.executor import ActorExecutor
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import VertexConfig
from graflo.onto import DBType
from graflo.util.casting import apply_type_casters, resolve_type_casters

from .edge_derivation_registry import EdgeDerivationRegistry
from .resource_config import EdgeInferSpec, ResourceConfig
from .transform import ProtoTransform

logger = logging.getLogger(__name__)


def strip_trivial_top_level_fields(doc: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of *doc* without None or empty-string values."""
    return {k: v for k, v in doc.items() if v is not None and v != ""}


def filter_vertex_config_for_resource(
    vertex_config: VertexConfig,
    *,
    resource_vertex_names: set[str],
    allowed_vertex_names: set[str] | None,
) -> VertexConfig:
    """Derive a filtered VertexConfig for runtime actor execution."""
    if resource_vertex_names:
        effective = set(resource_vertex_names)
        if allowed_vertex_names is not None:
            effective &= allowed_vertex_names
    elif allowed_vertex_names is not None:
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


class ResourceRuntime:
    """Fully initialized resource executor for document casting."""

    def __init__(
        self,
        config: ResourceConfig,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, ProtoTransform],
        *,
        strict_references: bool = False,
        dynamic_edge_feedback: bool = False,
        allowed_vertex_names: set[str] | None = None,
        target_db_flavor: DBType | None = None,
    ) -> None:
        self.config = config
        self._type_casters = resolve_type_casters(config.types)
        self._root = ActorWrapper(*config.pipeline)
        self._executor = ActorExecutor(self._root)

        runtime_vertex_config, local_edge_config = self._filter_vertex_edge_configs(
            vertex_config,
            edge_config,
            allowed_vertex_names=allowed_vertex_names,
        )
        self._vertex_config = runtime_vertex_config
        self._edge_config = local_edge_config

        self._validate_vertex_references(vertex_config)
        self._validate_infer_edge_spec_targets(self._edge_config)

        edge_derivation_registry = EdgeDerivationRegistry()
        self._edge_derivation_registry = edge_derivation_registry

        infer_edge_except = self._build_infer_except()
        init_ctx = self._build_init_context(
            transforms=transforms,
            edge_derivation=edge_derivation_registry,
            infer_edge_except=infer_edge_except,
            strict_references=strict_references,
            allowed_vertex_names=allowed_vertex_names,
            target_db_flavor=target_db_flavor,
        )
        logger.debug("total resource actor count : %s", self._root.count())
        self._root.finish_init(init_ctx=init_ctx)

        if dynamic_edge_feedback:
            self._propagate_dynamic_edges(edge_config, vertex_config=vertex_config)

        logger.debug("total resource actor count (after init): %s", self._root.count())
        self._init_extra_weights(vertex_config)

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def vertex_config(self) -> VertexConfig:
        return self._vertex_config

    @property
    def edge_config(self) -> EdgeConfig:
        return self._edge_config

    @property
    def root(self) -> ActorWrapper:
        return self._root

    @property
    def type_casters(self) -> dict[str, Callable[..., Any]]:
        return self._type_casters

    def collect_vertex_names(self) -> set[str]:
        return self.config.collect_vertex_names()

    def count(self) -> int:
        return self._root.count()

    @staticmethod
    def edge_ids_from_pipeline(pipeline: list[dict[str, Any]]) -> set[EdgeId]:
        """Collect (source, target, None) for every static EdgeActor in *pipeline*."""
        root = ActorWrapper(*pipeline)
        edge_actors = [a for a in root.collect_actors() if isinstance(a, EdgeActor)]
        return {
            (ea.edge.source, ea.edge.target, None)
            for ea in edge_actors
            if ea.edge is not None
        }

    def _filter_vertex_edge_configs(
        self,
        vertex_config: VertexConfig,
        edge_config: EdgeConfig,
        *,
        allowed_vertex_names: set[str] | None,
    ) -> tuple[VertexConfig, EdgeConfig]:
        runtime_vertex_config = filter_vertex_config_for_resource(
            vertex_config,
            resource_vertex_names=self.collect_vertex_names(),
            allowed_vertex_names=allowed_vertex_names,
        )
        local_edge_config = EdgeConfig.model_validate(
            edge_config.to_dict(skip_defaults=False)
        )
        return runtime_vertex_config, local_edge_config

    def _validate_vertex_references(self, vertex_config: VertexConfig) -> None:
        known_vertices = set(vertex_config.vertex_set)
        referenced_vertices: set[str] = set()

        for spec in self.config.infer_edge_only:
            referenced_vertices.add(spec.source)
            referenced_vertices.add(spec.target)
        for spec in self.config.infer_edge_except:
            referenced_vertices.add(spec.source)
            referenced_vertices.add(spec.target)
        for source, target, _ in self.edge_ids_from_pipeline(self.config.pipeline):
            referenced_vertices.add(source)
            referenced_vertices.add(target)

        missing_vertices = sorted(referenced_vertices - known_vertices)
        if missing_vertices:
            raise ValueError(
                "Resource dynamic edge references undefined vertices: "
                f"{missing_vertices}. "
                "Declare these vertices in vertex_config before using dynamic/inferred edges."
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

        _validate_list("infer_edge_only", self.config.infer_edge_only)
        _validate_list("infer_edge_except", self.config.infer_edge_except)

    def _build_infer_except(self) -> set[EdgeId]:
        infer_edge_except = {spec.edge_id for spec in self.config.infer_edge_except}
        if not self.config.infer_edge_only:
            infer_edge_except |= self.edge_ids_from_pipeline(self.config.pipeline)
        return infer_edge_except

    def _build_init_context(
        self,
        *,
        transforms: dict[str, ProtoTransform],
        edge_derivation: EdgeDerivationRegistry,
        infer_edge_except: set[EdgeId],
        strict_references: bool,
        allowed_vertex_names: set[str] | None,
        target_db_flavor: DBType | None,
    ) -> ActorInitContext:
        skip_on_missing_input_keys = (
            self.config.skip_actors_on_missing_input_keys
            if self.config.skip_actors_on_missing_input_keys is not None
            else self.config.drop_trivial_input_fields
        )
        return ActorInitContext(
            vertex_config=self._vertex_config,
            edge_config=self._edge_config,
            transforms=transforms,
            edge_derivation=edge_derivation,
            allowed_vertex_names=allowed_vertex_names,
            infer_edges=self.config.infer_edges,
            infer_edge_only={spec.edge_id for spec in self.config.infer_edge_only},
            infer_edge_except=infer_edge_except,
            strict_references=strict_references,
            skip_actors_on_missing_input_keys=skip_on_missing_input_keys,
            tolerate_transform_errors=self.config.tolerate_transform_errors,
            target_db_flavor=target_db_flavor,
        )

    def _propagate_dynamic_edges(
        self,
        edge_config: EdgeConfig,
        *,
        vertex_config: VertexConfig,
    ) -> None:
        baseline_edge_ids = {edge_id for edge_id, _ in edge_config.items()}
        for edge_id, edge in self._edge_config.items():
            if edge_id in baseline_edge_ids:
                continue
            edge_config.update_edges(
                edge.model_copy(deep=True), vertex_config=vertex_config
            )

    def _init_extra_weights(self, vertex_config: VertexConfig) -> None:
        reg = self._edge_derivation_registry
        for entry in self.config.extra_weights:
            entry.edge.finish_init(vertex_config)
            if reg is not None and entry.vertex_weights:
                reg.merge_vertex_weights(entry.edge.edge_id, entry.vertex_weights)

    def cast_document(self, doc: dict) -> ResourceCastResult:
        """Process a document and return entities plus any tolerated transform failures."""
        work_doc: dict[str, Any] = (
            strip_trivial_top_level_fields(doc)
            if self.config.drop_trivial_input_fields
            else dict(doc)
        )
        if self._type_casters:
            apply_type_casters(work_doc, self._type_casters)
        extraction_ctx = self._executor.extract(work_doc)
        result = self._executor.assemble_result(extraction_ctx)
        return ResourceCastResult(
            entities=result.entities,
            transform_failures=list(extraction_ctx.transform_failures),
        )

    def __call__(self, doc: dict) -> defaultdict[GraphEntity, list]:
        return self.cast_document(doc).entities
