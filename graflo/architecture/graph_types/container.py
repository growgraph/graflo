"""Graph data container for vertices and edges."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from pydantic import Field, field_serializer, field_validator

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types.identifiers import (
    GraphEntity,
    VertexName,
    deserialize_edge_key,
    deserialize_entity_key,
    serialize_edge_key,
    serialize_entity_key,
)


def _pick_unique_dict(docs: list) -> list:
    """Deduplicate dicts by structure; preserves original objects (local copy of merge logic)."""

    def make_hashable(obj: object) -> object:
        if isinstance(obj, dict):
            return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
        if isinstance(obj, (list, tuple)):
            return tuple(make_hashable(item) for item in obj)
        if isinstance(obj, (datetime, date, time)):
            return ("__datetime__", obj.isoformat())
        if isinstance(obj, Decimal):
            return ("__decimal__", str(obj))
        if isinstance(obj, set):
            return tuple(sorted(make_hashable(item) for item in obj))
        return obj

    seen: dict[object, object] = {}
    for doc in docs:
        key = make_hashable(doc)
        if key not in seen:
            seen[key] = doc
    return list(seen.values())


def _serialize_linear_item(
    item: defaultdict[str | tuple[str, str, str | None], list[Any]],
) -> dict[str, list[Any]]:
    return {serialize_entity_key(k): v for k, v in item.items()}


def _deserialize_linear_item(
    item: dict[str | tuple[str, str, str | None], list[Any]] | dict[str, list[Any]],
) -> defaultdict[str | tuple[str, str, str | None], list[Any]]:
    result: defaultdict[str | tuple[str, str, str | None], list[Any]] = defaultdict(
        list
    )
    for k, v in item.items():
        result[deserialize_entity_key(k)] = v
    return result


class ItemsView:
    """View class for iterating over vertices and edges in a GraphContainer."""

    def __init__(self, gc: GraphContainer):
        self._dictlike = gc

    def __iter__(self):
        """Iterate over vertices and edges in the container."""
        for key in self._dictlike.vertices:
            yield key, self._dictlike.vertices[key]
        for key in self._dictlike.edges:
            yield key, self._dictlike.edges[key]


class GraphContainer(ConfigBaseModel):
    """Container for graph data including vertices and edges.

    Attributes:
        vertices: Dictionary mapping vertex names to lists of vertex data
        edges: Dictionary mapping edge IDs to lists of edge data
        linear: List of default dictionaries containing linear data
    """

    vertices: dict[VertexName, list] = Field(default_factory=dict)
    edges: dict[tuple[str, str, str | None], list] = Field(default_factory=dict)
    linear: list[defaultdict[str | tuple[str, str, str | None], list[Any]]] = Field(
        default_factory=list
    )

    @field_serializer("edges", when_used="json-unless-none")
    def _serialize_edges(
        self, edges: dict[tuple[str, str, str | None], list]
    ) -> dict[str, list]:
        return {serialize_edge_key(k): v for k, v in edges.items()}

    @field_validator("edges", mode="before")
    @classmethod
    def _validate_edges(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized: dict[tuple[str, str, str | None], list] = {}
        for key, docs in value.items():
            if isinstance(key, str):
                normalized[deserialize_edge_key(key)] = docs
            else:
                normalized[key] = docs
        return normalized

    @field_serializer("linear", when_used="json-unless-none")
    def _serialize_linear(
        self,
        linear: list[defaultdict[str | tuple[str, str, str | None], list[Any]]],
    ) -> list[dict[str, list[Any]]]:
        return [_serialize_linear_item(item) for item in linear]

    @field_validator("linear", mode="before")
    @classmethod
    def _validate_linear(cls, value: Any) -> Any:
        if not isinstance(value, list):
            return value
        return [
            _deserialize_linear_item(item) if isinstance(item, dict) else item
            for item in value
        ]

    def items(self):
        """Get an ItemsView of the container's contents."""
        return ItemsView(self)

    def pick_unique(self):
        """Remove duplicate entries from vertices and edges."""
        for k, v in self.vertices.items():
            self.vertices[k] = _pick_unique_dict(v)
        for k, v in self.edges.items():
            self.edges[k] = _pick_unique_dict(v)

    @classmethod
    def from_docs_list(
        cls, list_default_dicts: list[defaultdict[GraphEntity, list]]
    ) -> GraphContainer:
        """Create a GraphContainer from a list of default dictionaries.

        Args:
            list_default_dicts: List of default dictionaries containing vertex and edge data

        Returns:
            New GraphContainer instance

        Raises:
            ValueError: If edge IDs are not properly formatted
        """
        vdict: defaultdict[str, list] = defaultdict(list)
        edict: defaultdict[tuple[str, str, str | None], list] = defaultdict(list)

        for d in list_default_dicts:
            for k, v in d.items():
                if isinstance(k, str):
                    vdict[k].extend(v)
                elif isinstance(k, tuple):
                    if not (
                        len(k) == 3
                        and all(isinstance(item, str) for item in k[:-1])
                        and isinstance(k[-1], (str, type(None)))
                    ):
                        raise ValueError(
                            f"edge key must be (str, str, str|None), got {k}"
                        )
                    edict[k].extend(v)
        return GraphContainer(
            vertices=dict(vdict.items()),
            edges=dict(edict.items()),
            linear=list_default_dicts,
        )
