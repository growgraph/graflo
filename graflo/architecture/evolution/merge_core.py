"""Pure merge helpers for logical vertices and edges."""

from __future__ import annotations

from graflo.architecture.graph_types import EdgeId
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex


def merge_field_pair(a: Field, b: Field) -> Field:
    """Merge two fields with the same name; fail on incompatible types."""
    if a.type is not None and b.type is not None and a.type != b.type:
        raise ValueError(
            f"Cannot merge field {a.name!r}: incompatible types {a.type!r} vs {b.type!r}"
        )
    merged_type = a.type if a.type is not None else b.type
    desc = a.description if a.description else b.description
    return Field(name=a.name, type=merged_type, description=desc)


def merge_vertex_models(vertices: list[Vertex], into_name: str) -> Vertex:
    """Union-merge vertex definitions into a single :class:`Vertex`."""
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

    return Vertex(
        name=into_name,
        properties=list(props.values()),
        identity=identity_out,
        filters=filters_out,
        description=desc_out,
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
