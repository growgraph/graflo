"""TigerGraph DDL-aligned edge projections (shared by bulk CSV and conn)."""

from __future__ import annotations

from graflo.architecture.schema.db_aware import EdgeConfigDBAware
from graflo.architecture.schema.edge import DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME, Edge


def tigergraph_ddl_edge_projection(edge: Edge, ec: EdgeConfigDBAware) -> Edge:
    """Mirror :meth:`TigerGraphConnection._edge_for_tigergraph_ddl` without a connection."""
    ew = ec.effective_weights(edge)
    edge_copy = edge.model_copy(deep=True)
    if ew is not None:
        edge_copy.properties = [f.model_copy(deep=True) for f in ew.direct]
    else:
        edge_copy.properties = []
    return edge_copy


def edge_identity_discriminator_field_names(edge: Edge) -> list[str]:
    """Sorted discriminator tokens (aligned with TigerGraph ADD DIRECTED EDGE)."""
    fields: set[str] = set()
    for identity_key in edge.identities:
        for token in identity_key:
            if token in {"source", "target"}:
                continue
            if token == "relation":
                fields.add(DEFAULT_TIGERGRAPH_RELATION_WEIGHTNAME)
                continue
            if token not in {"_from", "_to"}:
                fields.add(token)
    return sorted(fields)
