"""Class and relation hierarchies: what ``subclass_of`` and ``subproperty_of`` entail.

``Vertex.subclass_of`` asserts ``rdfs:subClassOf`` and ``Edge.subproperty_of``
asserts ``rdfs:subPropertyOf``. Both are logical assertions, not descriptions:
this module checks that a schema's hierarchy is consistent and answers what it
entails. Nothing is inherited — a subclass gets no property, identity or storage
from its parent — and nothing at ingestion or query time reads either list.

Consistency (:func:`check_hierarchy`, run by ``CoreSchema.finish_init``):

- a local parent is declared, is not the element itself, and the local
  hierarchy is acyclic;
- an edge's endpoints are subsumed by its parent edge's endpoints, in either
  orientation when the parent is undirected, since a subproperty relates only
  pairs its parent relates;
- an undirected edge has an undirected parent, for the same reason.

:func:`check_hierarchy_iris` (run by ``Schema.finish_init``, which holds the
metadata) refuses a local element named by IRI — one fact must have one form —
and an IRI written with a declared namespace prefix as its scheme.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Hashable, Iterable, Mapping
from typing import TYPE_CHECKING, TypeVar

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import Edge, EdgeConfig, EdgeRef, SuperProperty
from graflo.architecture.schema.semantics import Semantics, iri_scheme
from graflo.architecture.schema.vertex import SuperClass, Vertex, VertexConfig

if TYPE_CHECKING:
    from graflo.architecture.schema.core import CoreSchema
    from graflo.architecture.schema.metadata import GraphMetadata

K = TypeVar("K", bound=Hashable)


def _climb(parents: Mapping[K, Iterable[K]], start: K) -> list[K]:
    """Every node reachable from *start* along *parents*, nearest first, *start* excluded."""
    seen: set[K] = {start}
    out: list[K] = []
    queue = deque(parents.get(start, ()))
    while queue:
        node = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        out.append(node)
        queue.extend(parents.get(node, ()))
    return out


def _find_cycle(parents: Mapping[K, list[K]]) -> list[K] | None:
    """One cycle along *parents* as ``[a, b, …, a]``, or None when acyclic."""
    done: set[K] = set()
    for root in parents:
        if root in done:
            continue
        path: list[K] = [root]
        stack = [iter(parents.get(root, ()))]
        while stack:
            child = next(stack[-1], None)
            if child is None:
                stack.pop()
                done.add(path.pop())
                continue
            if child in path:
                return [*path[path.index(child) :], child]
            if child in done:
                continue
            path.append(child)
            stack.append(iter(parents.get(child, ())))
    return None


def _invert(parents: Mapping[K, list[K]]) -> dict[K, list[K]]:
    children: dict[K, list[K]] = {}
    for child, ps in parents.items():
        for parent in ps:
            children.setdefault(parent, []).append(child)
    return children


class Hierarchy:
    """What a schema's ``subclass_of`` and ``subproperty_of`` entail.

    A read-only view, built once over the vertices and edges it is given; it
    assumes they passed :func:`check_hierarchy`. Subsumption is reflexive and
    transitive. External parents (IRIs) are reached through the local chain: a
    vertex under a local parent that is under ``foaf:Agent`` is under it too.
    """

    def __init__(self, vertices: Iterable[Vertex], edges: Iterable[Edge]) -> None:
        self._vertex_parents: dict[str, list[str]] = {}
        self._vertex_iris: dict[str, list[str]] = {}
        for vertex in vertices:
            self._vertex_parents[vertex.name] = [
                p.vertex for p in vertex.subclass_of if p.vertex is not None
            ]
            self._vertex_iris[vertex.name] = [
                p.iri for p in vertex.subclass_of if p.iri is not None
            ]
        self._edge_parents: dict[EdgeId, list[EdgeId]] = {}
        self._edge_iris: dict[EdgeId, list[str]] = {}
        for edge in edges:
            self._edge_parents[edge.edge_id] = [
                p.edge.edge_id() for p in edge.subproperty_of if p.edge is not None
            ]
            self._edge_iris[edge.edge_id] = [
                p.iri for p in edge.subproperty_of if p.iri is not None
            ]
        self._vertex_children = _invert(self._vertex_parents)
        self._edge_children = _invert(self._edge_parents)

    @classmethod
    def of(cls, core: CoreSchema) -> Hierarchy:
        """The hierarchy of *core*."""
        return cls(core.vertex_config.vertices, core.edge_config.edges)

    # ── classes ─────────────────────────────────────────────────────────────

    def superclasses(self, name: str, *, transitive: bool = True) -> list[str]:
        """Local superclasses of vertex *name*, nearest first."""
        if not transitive:
            return list(self._vertex_parents.get(name, []))
        return _climb(self._vertex_parents, name)

    def subclasses(self, name: str, *, transitive: bool = True) -> list[str]:
        """Local subclasses of vertex *name*, nearest first."""
        if not transitive:
            return list(self._vertex_children.get(name, []))
        return _climb(self._vertex_children, name)

    def is_subclass(self, child: str, parent: str) -> bool:
        """Whether *child* ⊑ *parent*; every vertex is a subclass of itself."""
        return child == parent or parent in self.superclasses(child)

    def external_superclasses(self, name: str) -> list[str]:
        """IRIs of the external classes *name* is entailed to be a subclass of."""
        out: list[str] = []
        for node in (name, *self.superclasses(name)):
            out.extend(iri for iri in self._vertex_iris.get(node, []) if iri not in out)
        return out

    def subclass_pairs(self) -> set[tuple[str, str]]:
        """Every entailed ``(child, parent)`` pair between two local vertices."""
        return {(c, p) for c in self._vertex_parents for p in self.superclasses(c)}

    # ── relations ───────────────────────────────────────────────────────────

    def superproperties(
        self, edge_id: EdgeId, *, transitive: bool = True
    ) -> list[EdgeId]:
        """Local superproperties of edge *edge_id*, nearest first."""
        if not transitive:
            return list(self._edge_parents.get(edge_id, []))
        return _climb(self._edge_parents, edge_id)

    def subproperties(
        self, edge_id: EdgeId, *, transitive: bool = True
    ) -> list[EdgeId]:
        """Local subproperties of edge *edge_id*, nearest first."""
        if not transitive:
            return list(self._edge_children.get(edge_id, []))
        return _climb(self._edge_children, edge_id)

    def is_subproperty(self, child: EdgeId, parent: EdgeId) -> bool:
        """Whether *child* ⊑ *parent*; every edge is a subproperty of itself."""
        return child == parent or parent in self.superproperties(child)

    def external_superproperties(self, edge_id: EdgeId) -> list[str]:
        """IRIs of the external properties *edge_id* is entailed to be under."""
        out: list[str] = []
        for node in (edge_id, *self.superproperties(edge_id)):
            out.extend(iri for iri in self._edge_iris.get(node, []) if iri not in out)
        return out

    def subproperty_pairs(self) -> set[tuple[EdgeId, EdgeId]]:
        """Every entailed ``(child, parent)`` pair between two local edges."""
        return {(c, p) for c in self._edge_parents for p in self.superproperties(c)}


class NewSubsumptions(ConfigBaseModel):
    """Subsumptions a candidate schema entails between elements of a base that the base does not."""

    classes: set[tuple[str, str]]
    properties: set[tuple[EdgeId, EdgeId]]

    def __bool__(self) -> bool:
        return bool(self.classes or self.properties)


def new_subsumptions(base: CoreSchema, candidate: CoreSchema) -> NewSubsumptions:
    """What *candidate* entails between *base*'s own elements that *base* does not.

    The conservativity check: a change that adds a narrower concept is
    conservative; one that makes two existing concepts subsume each other, or
    one subsume another, is not, whatever it was meant to add.
    """
    before, after = Hierarchy.of(base), Hierarchy.of(candidate)
    vertices = base.vertex_config.vertex_set
    edges = {edge.edge_id for edge in base.edge_config.edges}
    return NewSubsumptions(
        classes={
            pair
            for pair in after.subclass_pairs() - before.subclass_pairs()
            if pair[0] in vertices and pair[1] in vertices
        },
        properties={
            pair
            for pair in after.subproperty_pairs() - before.subproperty_pairs()
            if pair[0] in edges and pair[1] in edges
        },
    )


def check_hierarchy(vertex_config: VertexConfig, edge_config: EdgeConfig) -> None:
    """Refuse an inconsistent class or relation hierarchy.

    Raises:
        ValueError: An undeclared or self parent, a cycle, an edge whose
            endpoints its parent does not subsume, or an undirected edge under
            a directed one.
    """
    names = vertex_config.vertex_set
    vertex_parents: dict[str, list[str]] = {}
    for vertex in vertex_config.vertices:
        for parent in vertex.subclass_of:
            if parent.vertex is None:
                continue
            if parent.vertex == vertex.name:
                raise ValueError(f"vertex {vertex.name!r} is a subclass of itself")
            if parent.vertex not in names:
                raise ValueError(
                    f"vertex {vertex.name!r} is a subclass of {parent.vertex!r}, "
                    "which the schema does not declare"
                )
            vertex_parents.setdefault(vertex.name, []).append(parent.vertex)
    cycle = _find_cycle(vertex_parents)
    if cycle:
        raise ValueError(f"subclass_of forms a cycle: {' -> '.join(cycle)}")

    edges = {edge.edge_id: edge for edge in edge_config.edges}
    edge_parents: dict[EdgeId, list[EdgeId]] = {}
    for edge in edge_config.edges:
        for ref in edge.subproperty_of:
            if ref.edge is None:
                continue
            parent_id = ref.edge.edge_id()
            if parent_id == edge.edge_id:
                raise ValueError(f"edge {edge.edge_id} is a subproperty of itself")
            if parent_id not in edges:
                raise ValueError(
                    f"edge {edge.edge_id} is a subproperty of {parent_id}, "
                    "which the schema does not declare"
                )
            edge_parents.setdefault(edge.edge_id, []).append(parent_id)
    cycle = _find_cycle(edge_parents)
    if cycle:
        raise ValueError(
            f"subproperty_of forms a cycle: {' -> '.join(map(str, cycle))}"
        )

    hierarchy = Hierarchy(vertex_config.vertices, edge_config.edges)
    for child_id, parent_ids in edge_parents.items():
        child = edges[child_id]
        for parent_id in parent_ids:
            parent = edges[parent_id]
            if not child.directed and parent.directed:
                raise ValueError(
                    f"undirected edge {child_id} cannot be a subproperty of "
                    f"directed edge {parent_id}: it relates both orientations"
                )
            if not _endpoints_subsumed(hierarchy, child, parent):
                raise ValueError(
                    f"edge {child_id} is a subproperty of {parent_id}, but its "
                    "endpoints are not subclasses of the parent's"
                )


def _endpoints_subsumed(hierarchy: Hierarchy, child: Edge, parent: Edge) -> bool:
    def under(source: str, target: str) -> bool:
        return hierarchy.is_subclass(source, parent.source) and hierarchy.is_subclass(
            target, parent.target
        )

    return under(child.source, child.target) or (
        not parent.directed and under(child.target, child.source)
    )


def vertex_iri(metadata: GraphMetadata, vertex: Vertex) -> str | None:
    """The IRI a vertex type denotes: its ``semantics.iri``, else the one the schema mints.

    A schema mints ``{base_iri}{name}``; without a ``base_iri`` an ungrounded
    vertex has no IRI.
    """
    if vertex.semantics is not None and vertex.semantics.iri is not None:
        return vertex.semantics.iri
    if metadata.base_iri is not None:
        return f"{metadata.base_iri}{vertex.name}"
    return None


def _local_iris(
    metadata: GraphMetadata, vertices: Iterable[Vertex], edges: Iterable[Edge]
) -> tuple[dict[str, str], dict[str, EdgeId]]:
    vertex_iris: dict[str, str] = {}
    for vertex in vertices:
        iri = vertex_iri(metadata, vertex)
        if iri is not None:
            vertex_iris.setdefault(iri, vertex.name)
    edge_iris: dict[str, EdgeId] = {}
    for edge in edges:
        if edge.semantics is not None and edge.semantics.iri is not None:
            edge_iris.setdefault(edge.semantics.iri, edge.edge_id)
    return vertex_iris, edge_iris


def localize_refs(
    metadata: GraphMetadata, vertices: list[Vertex], edges: list[Edge]
) -> tuple[list[Vertex], list[Edge]]:
    """*vertices* and *edges*, each ``{iri}`` parent that denotes one of them made local.

    For a fold of two schemas, where a parent one side named by IRI now lives in
    the result. A changed model is a copy. :func:`check_hierarchy_iris` refuses
    what this rewrites.
    """
    vertex_iris, edge_iris = _local_iris(metadata, vertices, edges)
    out_vertices: list[Vertex] = []
    for vertex in vertices:
        if not any(p.iri in vertex_iris for p in vertex.subclass_of):
            out_vertices.append(vertex)
            continue
        parents = _own_superclasses(
            vertex.name,
            (
                SuperClass(vertex=vertex_iris[p.iri])
                if p.iri is not None and p.iri in vertex_iris
                else p
                for p in vertex.subclass_of
            ),
        )
        out_vertices.append(vertex.model_copy(update={"subclass_of": parents}))
    out_edges: list[Edge] = []
    for edge in edges:
        if not any(p.iri in edge_iris for p in edge.subproperty_of):
            out_edges.append(edge)
            continue
        refs = _own_superproperties(
            edge.edge_id,
            (
                SuperProperty(edge=EdgeRef.of(edge_iris[p.iri]))
                if p.iri is not None and p.iri in edge_iris
                else p
                for p in edge.subproperty_of
            ),
        )
        out_edges.append(edge.model_copy(update={"subproperty_of": refs}))
    return out_vertices, out_edges


def check_hierarchy_iris(metadata: GraphMetadata, core: CoreSchema) -> None:
    """Refuse a local parent named by IRI, and an IRI written with a declared prefix.

    Raises:
        ValueError: An ``{iri}`` parent that denotes a vertex or edge of this
            schema, or an IRI whose scheme is a prefix in ``metadata.namespaces``.
    """
    vertex_iris, edge_iris = _local_iris(
        metadata, core.vertex_config.vertices, core.edge_config.edges
    )
    for vertex in core.vertex_config.vertices:
        for parent in vertex.subclass_of:
            if parent.iri in vertex_iris:
                raise ValueError(
                    f"vertex {vertex.name!r}: superclass {parent.iri!r} is vertex "
                    f"{vertex_iris[parent.iri]!r} of this schema; write "
                    f"{{vertex: {vertex_iris[parent.iri]}}}"
                )
    for edge in core.edge_config.edges:
        for parent in edge.subproperty_of:
            if parent.iri in edge_iris:
                raise ValueError(
                    f"edge {edge.edge_id}: superproperty {parent.iri!r} is edge "
                    f"{edge_iris[parent.iri]} of this schema; write it as {{edge: …}}"
                )

    prefixes = set(metadata.namespaces)
    if not prefixes:
        return
    for owner, iri in _schema_iris(metadata, core):
        if iri_scheme(iri) in prefixes:
            raise ValueError(
                f"{owner}: {iri!r} uses the declared prefix {iri_scheme(iri)!r} as a "
                "scheme; write the full IRI"
            )


def _semantics_iris(semantics: Semantics | None) -> list[str]:
    if semantics is None:
        return []
    return [
        *([semantics.iri] if semantics.iri else []),
        *semantics.exact_match,
        *semantics.close_match,
        *semantics.broader,
    ]


def _schema_iris(
    metadata: GraphMetadata, core: CoreSchema
) -> Iterable[tuple[str, str]]:
    for iri in _semantics_iris(metadata.semantics):
        yield "schema", iri
    if metadata.base_iri:
        yield "base_iri", metadata.base_iri
    for vertex in core.vertex_config.vertices:
        owner = f"vertex {vertex.name!r}"
        for iri in _semantics_iris(vertex.semantics):
            yield owner, iri
        for parent in vertex.subclass_of:
            if parent.iri:
                yield owner, parent.iri
        for field in vertex.properties:
            for iri in _semantics_iris(field.semantics):
                yield f"{owner} field {field.name!r}", iri
    for edge in core.edge_config.edges:
        owner = f"edge {edge.edge_id}"
        for iri in _semantics_iris(edge.semantics):
            yield owner, iri
        for parent in edge.subproperty_of:
            if parent.iri:
                yield owner, parent.iri
        for field in edge.properties:
            for iri in _semantics_iris(field.semantics):
                yield f"{owner} field {field.name!r}", iri


def _own_superclasses(name: str, parents: Iterable[SuperClass]) -> list[SuperClass]:
    """*parents* once each, without one naming vertex *name* itself."""
    out: list[SuperClass] = []
    for parent in parents:
        if parent.vertex != name and parent not in out:
            out.append(parent)
    return out


def _own_superproperties(
    edge_id: EdgeId, parents: Iterable[SuperProperty]
) -> list[SuperProperty]:
    """*parents* once each, without one naming edge *edge_id* itself."""
    out: list[SuperProperty] = []
    for parent in parents:
        if parent.edge is not None and parent.edge.edge_id() == edge_id:
            continue
        if parent not in out:
            out.append(parent)
    return out


def rewrite_vertex_refs(
    vertices: Iterable[Vertex], vertex_map: Mapping[str, str]
) -> list[Vertex]:
    """*vertices*, each local superclass following *vertex_map* (old → new name).

    A superclass that now names its own vertex — two vertices folded into one —
    is dropped, as is a duplicate. A changed vertex is a copy; the rest are the
    given models.
    """
    out: list[Vertex] = []
    for vertex in vertices:
        if not any(p.vertex in vertex_map for p in vertex.subclass_of):
            out.append(vertex)
            continue
        parents = _own_superclasses(
            vertex.name,
            (
                SuperClass(vertex=vertex_map[p.vertex])
                if p.vertex is not None and p.vertex in vertex_map
                else p
                for p in vertex.subclass_of
            ),
        )
        out.append(vertex.model_copy(update={"subclass_of": parents}))
    return out


def rewrite_edge_refs(
    edges: Iterable[Edge],
    *,
    vertex_map: Mapping[str, str] | None = None,
    edge_map: Mapping[EdgeId, EdgeId] | None = None,
) -> list[Edge]:
    """*edges*, each local superproperty following renamed and merged elements.

    *edge_map* maps old edge ids to new ones; an id it does not name follows
    *vertex_map* on its endpoints. A reference that now names its own edge, or
    repeats another, is dropped. A changed edge is a copy; the rest are the
    given models.
    """
    vmap = dict(vertex_map or {})
    emap = dict(edge_map or {})

    def new_edge_id(edge_id: EdgeId) -> EdgeId:
        if edge_id in emap:
            return emap[edge_id]
        source, target, relation = edge_id
        return vmap.get(source, source), vmap.get(target, target), relation

    out: list[Edge] = []
    for edge in edges:
        refs = _own_superproperties(
            edge.edge_id,
            (
                SuperProperty(edge=EdgeRef.of(new_edge_id(p.edge.edge_id())))
                if p.edge is not None
                else p
                for p in edge.subproperty_of
            ),
        )
        out.append(
            edge
            if refs == edge.subproperty_of
            else edge.model_copy(update={"subproperty_of": refs})
        )
    return out


def _splice(
    parents: dict[K, list[K]],
    iris: dict[K, list[str]],
    removed: set[K],
    node: K,
) -> tuple[list[K], list[str]]:
    """*node*'s parents with each removed one replaced by its own, recursively."""
    local: list[K] = []
    external: list[str] = []
    seen: set[K] = set()
    queue = deque(parents.get(node, []))
    while queue:
        parent = queue.popleft()
        if parent in seen:
            continue
        seen.add(parent)
        if parent in removed:
            queue.extend(parents.get(parent, []))
            external.extend(i for i in iris.get(parent, []) if i not in external)
        elif parent != node and parent not in local:
            local.append(parent)
    return local, external


def spliced_superclasses(
    vertices: Iterable[Vertex], removed: set[str]
) -> dict[str, list[SuperClass]]:
    """New ``subclass_of`` for each surviving vertex that names a removed one.

    A removed class is spliced out of the hierarchy: its subclasses take over
    its parents, local and external, so every subsumption between the
    survivors still holds. The result does not depend on the order in which
    classes are removed.
    """
    by_name = {v.name: v for v in vertices}
    parents = {
        name: [p.vertex for p in v.subclass_of if p.vertex is not None]
        for name, v in by_name.items()
    }
    iris = {
        name: [p.iri for p in v.subclass_of if p.iri is not None]
        for name, v in by_name.items()
    }
    out: dict[str, list[SuperClass]] = {}
    for name, vertex in by_name.items():
        if name in removed or not removed.intersection(parents[name]):
            continue
        local, external = _splice(parents, iris, removed, name)
        own = [p.iri for p in vertex.subclass_of if p.iri is not None]
        out[name] = [
            *(SuperClass(vertex=v) for v in local),
            *(
                SuperClass(iri=i)
                for i in [*own, *(e for e in external if e not in own)]
            ),
        ]
    return out


def spliced_superproperties(
    edges: Iterable[Edge], removed: set[EdgeId]
) -> dict[EdgeId, list[SuperProperty]]:
    """New ``subproperty_of`` for each surviving edge that names a removed one.

    The edge counterpart of :func:`spliced_superclasses`. Endpoint subsumption
    and directedness hold for the spliced parents by transitivity.
    """
    by_id = {e.edge_id: e for e in edges}
    parents = {
        edge_id: [p.edge.edge_id() for p in e.subproperty_of if p.edge is not None]
        for edge_id, e in by_id.items()
    }
    iris = {
        edge_id: [p.iri for p in e.subproperty_of if p.iri is not None]
        for edge_id, e in by_id.items()
    }
    out: dict[EdgeId, list[SuperProperty]] = {}
    for edge_id, edge in by_id.items():
        if edge_id in removed or not removed.intersection(parents[edge_id]):
            continue
        local, external = _splice(parents, iris, removed, edge_id)
        own = [p.iri for p in edge.subproperty_of if p.iri is not None]
        out[edge_id] = [
            *(SuperProperty(edge=EdgeRef.of(e)) for e in local),
            *(
                SuperProperty(iri=i)
                for i in [*own, *(e for e in external if e not in own)]
            ),
        ]
    return out


__all__ = [
    "Hierarchy",
    "NewSubsumptions",
    "check_hierarchy",
    "check_hierarchy_iris",
    "localize_refs",
    "new_subsumptions",
    "rewrite_edge_refs",
    "rewrite_vertex_refs",
    "spliced_superclasses",
    "spliced_superproperties",
    "vertex_iri",
]
