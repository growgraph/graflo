"""Pure merge helpers for logical vertices and edges."""

from __future__ import annotations

from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.identity_funnel import IdentityFunnel
from graflo.architecture.schema.vertex import Field, SecondaryIdentity, Vertex


def merge_field_pair(a: Field, b: Field) -> Field:
    """Merge two fields with the same name; fail on incompatible types."""
    if a.type is not None and b.type is not None and a.type != b.type:
        raise ValueError(
            f"Cannot merge field {a.name!r}: incompatible types {a.type!r} vs {b.type!r}"
        )
    merged_type = a.type if a.type is not None else b.type
    desc = a.description if a.description else b.description
    return Field(name=a.name, type=merged_type, description=desc)


def _merge_identity_funnels(
    vertices: list[Vertex], into_name: str
) -> IdentityFunnel | None:
    """Carry an identity funnel through a merge, refusing to invent one.

    Branch order *is* the policy, and branch ids take part in the digest, so
    concatenating two different funnels would silently rekey existing data.
    A single funnel (or several identical ones) carries through; anything else
    is the author's call to make explicitly.
    """
    funnels = [v.identity_funnel for v in vertices if v.identity_funnel is not None]
    if not funnels:
        return None
    first = funnels[0]
    divergent = [f for f in funnels[1:] if f != first]
    if divergent:
        raise ValueError(
            f"Cannot merge into vertex '{into_name}': sources declare different "
            "identity funnels. Branch order and branch ids determine the key, so "
            "they cannot be unioned automatically — replace the identity "
            "explicitly with the funnel you want."
        )
    return first


def _merge_secondary_identities(
    vertices: list[Vertex], into_name: str, primary: list[str]
) -> list[SecondaryIdentity]:
    """Union secondary identities across *vertices*, subsumed entries dropped.

    A set that equals the merged primary identity is dropped rather than kept: it is
    subsumed by the primary, and ``Vertex._validated_secondary_identities`` rejects a
    restatement of the primary outright. A name reused for two different field-sets is
    a real conflict — edge steps select by name, so the merged vertex cannot honour
    both — and raises.
    """
    primary_set = frozenset(primary)
    by_field_set: dict[frozenset[str], SecondaryIdentity] = {}
    names: dict[str, frozenset[str]] = {}

    for vertex in vertices:
        for entry in vertex.secondary_identities:
            if entry.field_set == primary_set or entry.field_set in by_field_set:
                continue
            if entry.name is not None:
                claimed = names.get(entry.name)
                if claimed is not None and claimed != entry.field_set:
                    raise ValueError(
                        f"Cannot merge into vertex '{into_name}': secondary identity "
                        f"name '{entry.name}' refers to {sorted(claimed)} and "
                        f"{sorted(entry.field_set)} on different sources"
                    )
                names[entry.name] = entry.field_set
            by_field_set[entry.field_set] = entry

    return list(by_field_set.values())


def merge_vertex_models(vertices: list[Vertex], into_name: str) -> Vertex:
    """Union-merge vertex definitions into a single :class:`Vertex`.

    Identity mode is carried through the merge: ``blank`` / ``assigned`` propagate when
    any source declares them, and ``hash_identity_properties`` / ``secondary_identities``
    are unioned. The mutual exclusions enforced by :meth:`Vertex.set_identity` are
    checked here so the failure names the merge rather than surfacing from pydantic.
    """
    if not vertices:
        raise ValueError("merge_vertex_models requires at least one vertex")

    props: dict[str, Field] = {}
    for v in vertices:
        for f in v.properties:
            if f.name not in props:
                props[f.name] = f
            else:
                props[f.name] = merge_field_pair(props[f.name], f)

    identity_out: list[str] = []
    seen_id: set[str] = set()
    for v in vertices:
        for x in v.identity:
            if x not in seen_id:
                identity_out.append(x)
                seen_id.add(x)

    filters_out: list = []
    for v in vertices:
        filters_out.extend(list(v.filters))

    descriptions = [v.description for v in vertices if v.description]
    if not descriptions:
        desc_out: str | None = None
    elif len(descriptions) == 1:
        desc_out = descriptions[0]
    else:
        desc_out = " / ".join(descriptions)

    blank_out = any(v.blank for v in vertices)
    assigned_out = any(v.assigned for v in vertices)
    if blank_out and assigned_out:
        raise ValueError(
            f"Cannot merge into vertex '{into_name}': sources mix blank and assigned "
            "identity modes, which are mutually exclusive"
        )

    hash_out: list[str] = []
    seen_hash: set[str] = set()
    for v in vertices:
        for name in v.hash_identity_properties:
            if name not in seen_hash:
                hash_out.append(name)
                seen_hash.add(name)
    if assigned_out and hash_out:
        raise ValueError(
            f"Cannot merge into vertex '{into_name}': an assigned source cannot be "
            f"merged with hash-identity sources (hash properties: {hash_out})"
        )

    funnel_out = _merge_identity_funnels(vertices, into_name)
    if funnel_out is not None:
        if hash_out:
            raise ValueError(
                f"Cannot merge into vertex '{into_name}': sources mix an identity "
                f"funnel with flat hash properties {hash_out}. Express the flat key "
                "as a funnel branch first, then merge."
            )
        if assigned_out or blank_out:
            raise ValueError(
                f"Cannot merge into vertex '{into_name}': an identity funnel cannot "
                "be merged with assigned or blank sources"
            )

    secondary_out = _merge_secondary_identities(vertices, into_name, identity_out)
    if blank_out and secondary_out:
        raise ValueError(
            f"Cannot merge into vertex '{into_name}': a blank source cannot be merged "
            "with sources declaring secondary_identities — a blank vertex has no "
            "source-visible key to match on"
        )

    return Vertex(
        name=into_name,
        properties=list(props.values()),
        identity=identity_out,
        filters=filters_out,
        description=desc_out,
        blank=blank_out,
        assigned=assigned_out,
        hash_identity_properties=hash_out,
        identity_funnel=funnel_out,
        secondary_identities=secondary_out,
    )


def merge_edge_pair(a: Edge, b: Edge) -> Edge:
    """Merge two edges with the same :attr:`~graflo.architecture.schema.edge.Edge.edge_id`."""
    props: dict[str, Field] = {}
    for f in a.properties + b.properties:
        if f.name not in props:
            props[f.name] = f
        else:
            props[f.name] = merge_field_pair(props[f.name], f)

    identities_out: list[list[str]] = []
    seen_rows: set[tuple[str, ...]] = set()
    for row in a.identities + b.identities:
        t = tuple(row)
        if t not in seen_rows:
            seen_rows.add(t)
            identities_out.append(list(row))

    descriptions = [a.description, b.description]
    descriptions = [d for d in descriptions if d]
    desc_out: str | None = None
    if len(descriptions) == 1:
        desc_out = descriptions[0]
    elif len(descriptions) > 1:
        desc_out = " / ".join(descriptions)

    return Edge(
        source=a.source,
        target=a.target,
        relation=a.relation,
        description=desc_out,
        identities=identities_out,
        properties=list(props.values()),
        type=a.type,
        by=a.by,
        # Undirected wins: it is the weaker assertion, and treating a merged
        # undirected edge as directed would let AddInverseEdgesOp synthesize an
        # inverse that duplicates it.
        directed=a.directed and b.directed,
    )


def redirect_and_merge_edges(edges: list[Edge], mapping: dict[str, str]) -> list[Edge]:
    """Apply vertex *mapping* to endpoints, then merge duplicate edge identities."""

    def _map_endpoint(n: str) -> str:
        return mapping.get(n, n)

    redirected: list[Edge] = []
    for e in edges:
        redirected.append(
            e.model_copy(
                update={
                    "source": _map_endpoint(e.source),
                    "target": _map_endpoint(e.target),
                }
            )
        )

    by_id: dict[EdgeId, Edge] = {}
    for e in redirected:
        eid = e.edge_id
        if eid not in by_id:
            by_id[eid] = e
        else:
            by_id[eid] = merge_edge_pair(by_id[eid], e)
    return list(by_id.values())


def edge_config_from_edges(edges: list[Edge]) -> EdgeConfig:
    """Build a fresh :class:`EdgeConfig` from a list of edges."""
    return EdgeConfig(edges=edges)


def remap_relation_and_merge_edges(
    edges: list[Edge], relation_map: dict[str, str]
) -> list[Edge]:
    """Remap edge relation names and merge duplicate edge identities."""
    if not relation_map:
        return list(edges)
    remapped = [
        edge.model_copy(
            update={"relation": relation_map.get(edge.relation, edge.relation)}
        )
        if edge.relation is not None
        else edge
        for edge in edges
    ]
    by_id: dict[EdgeId, Edge] = {}
    for edge in remapped:
        if edge.edge_id in by_id:
            by_id[edge.edge_id] = merge_edge_pair(by_id[edge.edge_id], edge)
            continue
        by_id[edge.edge_id] = edge
    return list(by_id.values())
