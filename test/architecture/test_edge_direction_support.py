from __future__ import annotations

import pytest

from graflo.architecture.schema.core import CoreSchema
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.metadata import GraphMetadata
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.onto import TARGET_DATABASES
from graflo.db.edge_direction_support import (
    ReverseTraversalCost,
    check_schema_edge_directions,
    iter_undirected_edges,
    reverse_traversal_cost,
    supports_native_undirected,
)
from graflo.onto import DBType


def _schema(*, undirected: bool) -> Schema:
    """Two edges: one always directed, one flipped by the parameter."""
    vertex_config = VertexConfig(
        vertices=[
            Vertex(name="person", properties=[Field(name="pid")], identity=["pid"]),
            Vertex(name="company", properties=[Field(name="cid")], identity=["cid"]),
        ]
    )
    edge_config = EdgeConfig(
        edges=[
            Edge(source="person", target="company", relation="works_at"),
            Edge(
                source="person",
                target="person",
                relation="knows",
                directed=not undirected,
            ),
        ]
    )
    return Schema(
        metadata=GraphMetadata(name="direction_test", version="1.0.0"),
        core_schema=CoreSchema(vertex_config=vertex_config, edge_config=edge_config),
    )


def test_every_target_backend_has_a_reverse_traversal_cost() -> None:
    """Guards the table against enum drift — the failure mode that hid this gap."""
    for db_type in TARGET_DATABASES:
        assert isinstance(reverse_traversal_cost(db_type), ReverseTraversalCost)


def test_reverse_traversal_cost_rejects_unrecorded_backend() -> None:
    with pytest.raises(KeyError, match="No reverse-traversal cost"):
        reverse_traversal_cost(DBType.MONGODB)


def test_only_tigergraph_supports_native_undirected() -> None:
    assert supports_native_undirected(DBType.TIGERGRAPH) is True
    for db_type in TARGET_DATABASES - {DBType.TIGERGRAPH}:
        assert supports_native_undirected(db_type) is False


def test_bare_string_flavor_is_accepted() -> None:
    """``db_flavor`` reaches these helpers as a plain string from validated config."""
    assert supports_native_undirected("tigergraph") is True  # ty: ignore
    assert reverse_traversal_cost("arango") is ReverseTraversalCost.FREE  # ty: ignore


def test_iter_undirected_edges_finds_only_undirected() -> None:
    assert list(iter_undirected_edges(_schema(undirected=False))) == []
    assert list(iter_undirected_edges(_schema(undirected=True))) == [
        ("person", "person", "knows")
    ]


def test_all_directed_schema_yields_no_diagnostics() -> None:
    schema = _schema(undirected=False)
    for db_type in TARGET_DATABASES:
        assert check_schema_edge_directions(db_type, schema) == []


def test_tigergraph_represents_undirected_natively() -> None:
    schema = _schema(undirected=True)
    assert check_schema_edge_directions(DBType.TIGERGRAPH, schema) == []


@pytest.mark.parametrize(
    ("db_type", "severity"),
    [
        (DBType.ARANGO, "info"),
        (DBType.NEO4J, "info"),
        (DBType.MEMGRAPH, "info"),
        (DBType.FALKORDB, "info"),
        (DBType.NEBULA, "info"),
        (DBType.POSTGRES, "warning"),
        (DBType.GRAFLO_BACKEND, "warning"),
    ],
)
def test_one_diagnostic_per_undirected_edge(db_type: DBType, severity: str) -> None:
    diagnostics = check_schema_edge_directions(db_type, _schema(undirected=True))
    assert len(diagnostics) == 1
    (diagnostic,) = diagnostics
    assert diagnostic.edge_id == ("person", "person", "knows")
    assert diagnostic.db_type is db_type
    assert diagnostic.severity == severity
    assert db_type.value in diagnostic.message
    assert diagnostic.remedy


def test_graflo_backend_reports_the_materialization_tier() -> None:
    """Direction is the storage partition key there — the one unhonourable case."""
    assert (
        reverse_traversal_cost(DBType.GRAFLO_BACKEND)
        is ReverseTraversalCost.MATERIALIZATION_REQUIRED
    )
    (diagnostic,) = check_schema_edge_directions(
        DBType.GRAFLO_BACKEND, _schema(undirected=True)
    )
    assert "cannot be honoured" in diagnostic.message


def test_unknown_backend_yields_no_diagnostics_rather_than_raising() -> None:
    assert check_schema_edge_directions(DBType.MONGODB, _schema(undirected=True)) == []
