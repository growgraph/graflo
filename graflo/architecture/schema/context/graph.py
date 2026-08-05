"""Adjacency index over a schema's vertex types.

``EdgeConfig`` is keyed only by :data:`~graflo.architecture.graph_types.EdgeId`,
so "which edges touch this vertex type" has no answer without a scan. This module
builds that index once and exposes the schema graph as a navigable object.

**Naming discipline.** "Neighbours" here means *adjacent vertex types in the
schema*, never adjacent instances in a data graph. The instance-plane counterpart
is ``Connection.graph_neighbors``; the two must never share a name or an endpoint.
"""

from __future__ import annotations

from collections import deque

from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeDirection, EdgeId
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge


def edge_sort_key(edge_id: EdgeId) -> tuple[str, str, str]:
    """Total order over edge ids.

    ``relation`` is ``str | None``, so plain tuple comparison raises ``TypeError``
    as soon as a relation-less edge meets a named one. Every ordering in this
    package goes through here so results stay deterministic and comparable.
    """
    source, target, relation = edge_id
    return (source, target, relation or "")


class SchemaPath(ConfigBaseModel):
    """One path between two vertex types, as an alternating vertex/edge walk."""

    vertices: list[str] = PydanticField(
        ..., description="Vertex types visited, from source to target inclusive."
    )
    edges: list[EdgeId] = PydanticField(
        ..., description="Edges traversed, one fewer than ``vertices``."
    )

    @property
    def length(self) -> int:
        """Number of hops (edges) in this path."""
        return len(self.edges)


class SchemaNeighborhood(ConfigBaseModel):
    """Vertex types reachable from a seed within a hop bound."""

    origin: str = PydanticField(..., description="Vertex type the walk started from.")
    hops: int = PydanticField(..., description="Hop bound the walk honoured.")
    direction: EdgeDirection = PydanticField(
        ..., description="Orientation followed from each frontier vertex."
    )
    distances: dict[str, int] = PydanticField(
        ...,
        description="Reachable vertex type -> hop distance from origin (origin itself is 0).",
    )
    edges: list[EdgeId] = PydanticField(
        ..., description="Edges traversed to reach the neighbourhood, deduplicated."
    )

    @property
    def vertex_types(self) -> list[str]:
        """Reachable vertex types, nearest first then alphabetical."""
        return sorted(self.distances, key=lambda name: (self.distances[name], name))


class SchemaGraph:
    """Read-only adjacency index over a :class:`Schema`'s vertex types.

    Built once per schema and never mutates it. Plain dicts throughout — no
    networkx, because this is layer 2 and the whole point is to stay free of
    heavyweight dependencies.
    """

    def __init__(self, schema: Schema) -> None:
        self._schema = schema
        core = schema.core_schema
        self._vertex_types = frozenset(core.vertex_config.vertex_set)
        self._out: dict[str, list[EdgeId]] = {name: [] for name in self._vertex_types}
        self._in: dict[str, list[EdgeId]] = {name: [] for name in self._vertex_types}
        self._edges: dict[EdgeId, Edge] = {}

        for edge in core.edge_config.edges:
            edge_id = edge.edge_id
            self._edges[edge_id] = edge
            source, target, _relation = edge_id
            if source in self._out:
                self._out[source].append(edge_id)
            if target in self._in:
                self._in[target].append(edge_id)

        for adjacency in (self._out, self._in):
            for edge_ids in adjacency.values():
                edge_ids.sort(key=edge_sort_key)

    @classmethod
    def from_schema(cls, schema: Schema) -> SchemaGraph:
        """Build an index for *schema*."""
        return cls(schema)

    @property
    def schema(self) -> Schema:
        """The indexed schema. Treat as read-only."""
        return self._schema

    @property
    def vertex_types(self) -> frozenset[str]:
        """Every declared vertex type name."""
        return self._vertex_types

    @property
    def edge_ids(self) -> list[EdgeId]:
        """Every declared edge id, in deterministic order."""
        return sorted(self._edges, key=edge_sort_key)

    def edge(self, edge_id: EdgeId) -> Edge:
        """Return the declared edge for *edge_id*."""
        return self._edges[edge_id]

    def out_edges(self, vertex_type: str) -> list[EdgeId]:
        """Edges whose source is *vertex_type*."""
        return list(self._out.get(vertex_type, []))

    def in_edges(self, vertex_type: str) -> list[EdgeId]:
        """Edges whose target is *vertex_type*."""
        return list(self._in.get(vertex_type, []))

    def degree(self, vertex_type: str) -> int:
        """Total incident edge count (out + in), counting self-loops twice."""
        return len(self._out.get(vertex_type, [])) + len(self._in.get(vertex_type, []))

    def isolated_types(self) -> list[str]:
        """Vertex types with no incident edge at all."""
        return sorted(name for name in self._vertex_types if self.degree(name) == 0)

    def relation_vocabulary(self) -> list[str]:
        """Distinct non-null relation names across all edges."""
        return sorted(
            {
                relation
                for _source, _target, relation in self._edges
                if relation is not None
            }
        )

    def _traversable(
        self,
        edge_id: EdgeId,
        anchor: str,
        direction: EdgeDirection,
    ) -> str | None:
        """Return the far endpoint when *edge_id* may be followed from *anchor*.

        An edge declared ``directed=False`` is traversable both ways regardless of
        the requested direction — the same rule
        :func:`~graflo.db.edge_direction_support.default_direction_for_edge`
        applies on the instance plane.
        """
        source, target, _relation = edge_id
        undirected = not self._edges[edge_id].directed
        effective = EdgeDirection.ANY if undirected else direction

        forward = source == anchor and effective in (
            EdgeDirection.OUT,
            EdgeDirection.ANY,
        )
        backward = target == anchor and effective in (
            EdgeDirection.IN,
            EdgeDirection.ANY,
        )
        if forward:
            return target
        if backward:
            return source
        return None

    def _incident(self, vertex_type: str) -> list[EdgeId]:
        """Every edge touching *vertex_type*, deduplicated (self-loops appear once)."""
        seen: set[EdgeId] = set()
        incident: list[EdgeId] = []
        for edge_id in self._out.get(vertex_type, []) + self._in.get(vertex_type, []):
            if edge_id in seen:
                continue
            seen.add(edge_id)
            incident.append(edge_id)
        return sorted(incident, key=edge_sort_key)

    def schema_neighbors(
        self,
        vertex_type: str,
        *,
        hops: int = 1,
        direction: EdgeDirection = EdgeDirection.ANY,
        edge_relations: set[str | None] | None = None,
    ) -> SchemaNeighborhood:
        """Vertex types adjacent to *vertex_type* within *hops*.

        Args:
            vertex_type: Seed vertex type. Must be declared.
            hops: Maximum hop distance. ``0`` returns just the seed.
            direction: Orientation followed from each frontier vertex. Defaults to
                :attr:`EdgeDirection.ANY` — deliberately unlike
                ``Connection.fetch_edges``, which defaults to ``OUT``. "What is
                adjacent to ``person`` in the schema" almost never means "only
                where person is the source"; an agent asking that wants the whole
                local shape. Edges declared ``directed=False`` are followed both
                ways whatever is requested here.
            edge_relations: Restrict traversal to these relation names (``None`` is
                a valid member, matching edges with no relation).

        Returns:
            SchemaNeighborhood: distances per reachable type and the edges used.

        Raises:
            KeyError: if *vertex_type* is not declared in the schema.
        """
        if vertex_type not in self._vertex_types:
            raise KeyError(
                f"Unknown vertex type {vertex_type!r}; declared: {sorted(self._vertex_types)}"
            )
        if hops < 0:
            raise ValueError(f"hops must be >= 0, got {hops}")

        distances: dict[str, int] = {vertex_type: 0}
        used: set[EdgeId] = set()
        frontier: deque[tuple[str, int]] = deque([(vertex_type, 0)])

        while frontier:
            current, depth = frontier.popleft()
            if depth >= hops:
                continue
            for edge_id in self._incident(current):
                if edge_relations is not None and edge_id[2] not in edge_relations:
                    continue
                far = self._traversable(edge_id, current, direction)
                if far is None:
                    continue
                used.add(edge_id)
                if far not in distances:
                    distances[far] = depth + 1
                    frontier.append((far, depth + 1))

        return SchemaNeighborhood(
            origin=vertex_type,
            hops=hops,
            direction=direction,
            distances=distances,
            edges=sorted(used, key=edge_sort_key),
        )

    def relations_between(
        self,
        a: str,
        b: str,
        *,
        max_len: int = 3,
        max_paths: int = 20,
        direction: EdgeDirection = EdgeDirection.ANY,
    ) -> list[SchemaPath]:
        """Simple paths from vertex type *a* to *b*, shortest first.

        Bounded breadth-first enumeration: no vertex repeats within a path, so
        cycles terminate. Results are ordered by ``(length, edge ids)`` and are
        therefore reproducible run to run.

        Args:
            a: Source vertex type.
            b: Target vertex type.
            max_len: Maximum hops per path.
            max_paths: Maximum number of paths returned.
            direction: Orientation followed from each frontier vertex.

        Returns:
            list[SchemaPath]: paths found, possibly empty.

        Raises:
            KeyError: if either endpoint is not declared in the schema.
        """
        for name in (a, b):
            if name not in self._vertex_types:
                raise KeyError(
                    f"Unknown vertex type {name!r}; declared: {sorted(self._vertex_types)}"
                )
        if max_len < 1 or max_paths < 1:
            return []

        found: list[SchemaPath] = []
        queue: deque[tuple[str, list[str], list[EdgeId]]] = deque([(a, [a], [])])

        while queue and len(found) < max_paths:
            current, vertices, edges = queue.popleft()
            if len(edges) >= max_len:
                continue
            for edge_id in self._incident(current):
                far = self._traversable(edge_id, current, direction)
                if far is None:
                    continue
                # Paths stay simple, except that reaching the target closes the
                # walk — which is what makes ``relations_between(a, a)`` return
                # self-loops and cycles rather than nothing.
                if far in vertices and far != b:
                    continue
                next_vertices = [*vertices, far]
                next_edges = [*edges, edge_id]
                if far == b:
                    found.append(SchemaPath(vertices=next_vertices, edges=next_edges))
                    if len(found) >= max_paths:
                        break
                else:
                    queue.append((far, next_vertices, next_edges))

        found.sort(
            key=lambda path: (path.length, [edge_sort_key(e) for e in path.edges])
        )
        return found[:max_paths]
