"""Declarative resource configuration (YAML/manifest contract)."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import AliasChoices, Field as PydanticField, model_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeId, EncodingType, Weight
from graflo.architecture.pipeline.runtime.actor.config.normalize import (
    normalize_actor_step,
)
from graflo.architecture.schema.edge import Edge

logger = logging.getLogger(__name__)


def collect_vertex_names_from_pipeline(steps: list[Any]) -> set[str]:
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
                names |= collect_vertex_names_from_pipeline(sub_pipeline)
    return names


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


class ResourceConfig(ConfigBaseModel):
    """Declarative resource definition (serializable contract)."""

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
            "the actor pipeline runs."
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
            "records the error, and continues the pipeline."
        ),
    )

    @model_validator(mode="after")
    def _validate_policy(self) -> ResourceConfig:
        if self.infer_edge_only and self.infer_edge_except:
            raise ValueError(
                "Resource infer_edge_only and infer_edge_except are mutually exclusive."
            )
        return self

    def collect_vertex_names(self) -> set[str]:
        """Vertex types referenced by this resource (pipeline and related config)."""
        names = collect_vertex_names_from_pipeline(self.pipeline)
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

    def pipeline_actor_count(self) -> int:
        """Count actors in the pipeline without binding schema context."""
        from graflo.architecture.pipeline.runtime.actor import ActorWrapper

        return ActorWrapper(*self.pipeline).count()


# Internal-only alias; prefer ResourceConfig in new code.
Resource = ResourceConfig
