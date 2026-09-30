"""How a traversal reads the endpoints of each backend's edge rows."""

from __future__ import annotations

import pytest

from graflo.db.traversal import normalize_edge_row
from graflo.onto import DBType

ROWS = [
    (DBType.ARANGO, {"_from": "person/1", "_to": "person/2", "since": 1}),
    (DBType.POSTGRES, {"source_id": "1", "target_id": "2", "since": 1}),
    (DBType.NEBULA, {"_src": "1", "_dst": "2", "since": 1}),
    (DBType.TIGERGRAPH, {"from_id": "1", "to_id": "2", "since": 1}),
    (DBType.GRAFLO_BACKEND, {"_from_key": "1", "_to_key": "2", "since": 1}),
]


@pytest.mark.parametrize("flavor,row", ROWS, ids=[f.value for f, _ in ROWS])
def test_each_backend_reports_its_endpoints_under_its_own_names(flavor, row) -> None:
    properties, source, target = normalize_edge_row(row, flavor)

    assert (source, target) == ("1", "2")
    assert properties == {"since": 1}


def test_another_backends_names_are_not_read(caplog) -> None:
    with caplog.at_level("WARNING"):
        _, source, target = normalize_edge_row(
            {"_from": "person/1", "_to": "person/2"}, DBType.POSTGRES
        )

    assert (source, target) == (None, None)
    assert "source_id" in caplog.text


def test_a_cypher_relationship_triple() -> None:
    properties, source, target = normalize_edge_row(
        ({"id": "1"}, {"since": 1}, {"id": "2"}), DBType.NEO4J
    )

    assert (properties, source, target) == ({"since": 1}, "1", "2")
