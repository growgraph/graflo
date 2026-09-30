"""Which introspected edges become schema edges."""

from __future__ import annotations

import logging

from graflo.db.graph_introspection import (
    GraphEdgeIntrospection,
    GraphIntrospectionResult,
    GraphSchemaInferencer,
    GraphVertexIntrospection,
)


def test_an_edge_with_an_endpoint_outside_the_vertex_set_is_skipped_and_named(
    caplog,
) -> None:
    introspection = GraphIntrospectionResult(
        name="g",
        vertices=[GraphVertexIntrospection(name="person", properties=["id"])],
        edges=[
            GraphEdgeIntrospection(source="person", target="person", relation="knows"),
            GraphEdgeIntrospection(source="node", target="node", relation="links"),
        ],
    )

    with caplog.at_level(logging.WARNING, logger="graflo.db.graph_introspection"):
        schema = GraphSchemaInferencer().infer_schema(introspection)

    assert [e.edge_id for e in schema.core_schema.edge_config.edges] == [
        ("person", "person", "knows")
    ]
    assert "Skipping edge node -> node: source vertex undefined" in caplog.text
