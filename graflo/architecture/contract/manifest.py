"""Graph manifest model for complete ingestion contracts."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from pydantic import AliasChoices, Field as PydanticField, model_validator, ConfigDict

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.schema import Schema

from .bindings import Bindings
from .declarations.ingestion_model import IngestionModel

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


def _rename_edge_step(
    payload: dict[str, Any],
    *,
    vertex_name: Callable[[str], str],
    edge_name: Callable[[str], str],
) -> None:
    for key in ("from", "to", "source", "target"):
        value = payload.get(key)
        if isinstance(value, str):
            payload[key] = vertex_name(value)

    relation = payload.get("relation")
    if isinstance(relation, str):
        payload["relation"] = edge_name(relation)

    relation_map = payload.get("relation_map")
    if isinstance(relation_map, dict):
        payload["relation_map"] = {
            raw: edge_name(mapped) if isinstance(mapped, str) else mapped
            for raw, mapped in relation_map.items()
        }

    links = payload.get("links")
    if isinstance(links, list):
        for link in links:
            if isinstance(link, dict):
                _rename_edge_step(
                    link,
                    vertex_name=vertex_name,
                    edge_name=edge_name,
                )


def _rename_pipeline_step(
    step: Any,
    *,
    vertex_name: Callable[[str], str],
    edge_name: Callable[[str], str],
) -> None:
    if isinstance(step, list):
        for item in step:
            _rename_pipeline_step(item, vertex_name=vertex_name, edge_name=edge_name)
        return
    if not isinstance(step, dict):
        return

    if isinstance(step.get("vertex"), str):
        step["vertex"] = vertex_name(step["vertex"])

    if isinstance(step.get("type_map"), dict):
        step["type_map"] = {
            raw: vertex_name(mapped) if isinstance(mapped, str) else mapped
            for raw, mapped in step["type_map"].items()
        }

    if isinstance(step.get("vertex_from_map"), dict):
        step["vertex_from_map"] = {
            vertex_name(k): v for k, v in step["vertex_from_map"].items()
        }

    edge_payload = step.get("edge")
    if isinstance(edge_payload, dict):
        _rename_edge_step(edge_payload, vertex_name=vertex_name, edge_name=edge_name)

    create_edge_payload = step.get("create_edge")
    if isinstance(create_edge_payload, dict):
        _rename_edge_step(
            create_edge_payload, vertex_name=vertex_name, edge_name=edge_name
        )

    descend_payload = step.get("descend")
    if isinstance(descend_payload, dict):
        apply_payload = descend_payload.get("apply")
        if apply_payload is not None:
            _rename_pipeline_step(
                apply_payload, vertex_name=vertex_name, edge_name=edge_name
            )
        pipeline_payload = descend_payload.get("pipeline")
        if pipeline_payload is not None:
            _rename_pipeline_step(
                pipeline_payload, vertex_name=vertex_name, edge_name=edge_name
            )

    if isinstance(step.get("apply"), list):
        _rename_pipeline_step(
            step["apply"], vertex_name=vertex_name, edge_name=edge_name
        )
    if isinstance(step.get("pipeline"), list):
        _rename_pipeline_step(
            step["pipeline"], vertex_name=vertex_name, edge_name=edge_name
        )


class GraphManifest(ConfigBaseModel):
    """Canonical config contract for graph schema, ingestion, and bindings."""

    model_config = ConfigDict(populate_by_name=True)

    graph_schema: Schema | None = PydanticField(
        default=None,
        description="Logical graph schema contract.",
        validation_alias=AliasChoices("schema", "graph_schema"),
        serialization_alias="schema",
    )
    ingestion_model: IngestionModel | None = PydanticField(
        default=None,
        description="Ingestion resources and transforms.",
    )
    bindings: Bindings | None = PydanticField(
        default=None,
        description="Bindings mapping resources to concrete data sources.",
    )

    @classmethod
    def from_config(cls, data: dict[str, Any]) -> "GraphManifest":
        """Build a manifest from a Python mapping payload."""
        return cls.from_dict(data)

    @model_validator(mode="after")
    def _validate_manifest(self) -> "GraphManifest":
        if (
            self.graph_schema is None
            and self.ingestion_model is None
            and self.bindings is None
        ):
            raise ValueError(
                "GraphManifest requires at least one block: "
                "schema, ingestion_model, or bindings."
            )
        return self

    def finish_init(
        self,
        *,
        strict_references: bool = False,
        dynamic_edge_feedback: bool = False,
    ) -> None:
        """Initialize model internals and cross-block runtime links."""
        if self.graph_schema is not None:
            self.graph_schema.finish_init()
        if self.graph_schema is not None and self.ingestion_model is not None:
            self.ingestion_model.finish_init(
                self.graph_schema.core_schema,
                strict_references=strict_references,
                dynamic_edge_feedback=dynamic_edge_feedback,
                target_db_flavor=self.graph_schema.db_profile.db_flavor,
            )

    def require_schema(self) -> Schema:
        if self.graph_schema is None:
            raise ValueError("GraphManifest is missing required 'schema' block.")
        return self.graph_schema

    def require_ingestion_model(self) -> IngestionModel:
        if self.ingestion_model is None:
            raise ValueError(
                "GraphManifest is missing required 'ingestion_model' block."
            )
        return self.ingestion_model

    def require_bindings(self) -> Bindings:
        if self.bindings is None:
            raise ValueError("GraphManifest is missing required 'bindings' block.")
        return self.bindings

    def rename_entities(
        self,
        *,
        vertices: NameTransform | None = None,
        edges: NameTransform | None = None,
        resources: NameTransform | None = None,
    ) -> "GraphManifest":
        """Return a manifest copy with renamed vertices/edges/resources.

        ``vertices`` renames vertex type names across schema edges and ingestion actors.
        ``edges`` renames edge ``relation`` labels.
        ``resources`` renames ingestion resource names and binding references.
        Each transform may be a mapping or ``Callable[[str], str]``.
        """
        vertex_name = _build_name_transformer(vertices, label="vertices")
        edge_name = _build_name_transformer(edges, label="edges")
        resource_name = _build_name_transformer(resources, label="resources")

        payload = self.to_dict(skip_defaults=False)

        schema_payload = payload.get("schema")
        if isinstance(schema_payload, dict):
            graph_payload = schema_payload.get("core_schema")
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
                            vertex_name(name): value
                            for name, value in force_types.items()
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

        ingestion_payload = payload.get("ingestion_model")
        if isinstance(ingestion_payload, dict):
            resources_payload = ingestion_payload.get("resources")
            if isinstance(resources_payload, list):
                for resource in resources_payload:
                    if not isinstance(resource, dict):
                        continue
                    if isinstance(resource.get("name"), str):
                        resource["name"] = resource_name(resource["name"])

                    pipeline = resource.get("pipeline")
                    if isinstance(pipeline, list):
                        _rename_pipeline_step(
                            pipeline,
                            vertex_name=vertex_name,
                            edge_name=edge_name,
                        )

                    for spec_key in ("infer_edge_only", "infer_edge_except"):
                        specs = resource.get(spec_key)
                        if not isinstance(specs, list):
                            continue
                        for spec in specs:
                            if not isinstance(spec, dict):
                                continue
                            if isinstance(spec.get("source"), str):
                                spec["source"] = vertex_name(spec["source"])
                            if isinstance(spec.get("target"), str):
                                spec["target"] = vertex_name(spec["target"])
                            if isinstance(spec.get("relation"), str):
                                spec["relation"] = edge_name(spec["relation"])

                    extra_weights = resource.get("extra_weights")
                    if isinstance(extra_weights, list):
                        for entry in extra_weights:
                            if not isinstance(entry, dict):
                                continue
                            edge = entry.get("edge")
                            if isinstance(edge, dict):
                                if isinstance(edge.get("source"), str):
                                    edge["source"] = vertex_name(edge["source"])
                                if isinstance(edge.get("target"), str):
                                    edge["target"] = vertex_name(edge["target"])
                                if isinstance(edge.get("relation"), str):
                                    edge["relation"] = edge_name(edge["relation"])

        bindings_payload = payload.get("bindings")
        if isinstance(bindings_payload, dict):
            connectors = bindings_payload.get("connectors")
            if isinstance(connectors, list):
                for connector in connectors:
                    if isinstance(connector, dict) and isinstance(
                        connector.get("resource_name"), str
                    ):
                        connector["resource_name"] = resource_name(
                            connector["resource_name"]
                        )

            resource_connector = bindings_payload.get("resource_connector")
            if isinstance(resource_connector, list):
                for mapping in resource_connector:
                    if isinstance(mapping, dict) and isinstance(
                        mapping.get("resource"), str
                    ):
                        mapping["resource"] = resource_name(mapping["resource"])

        return GraphManifest.from_dict(payload)
