"""Read guards, scoped deletes and union-merged labels against a live Neo4j.

The guard and scope cases are shared with FalkorDB (``test/db/cypher_cases``).
"""

from __future__ import annotations

import pytest

from graflo.db.manager import ConnectionManager
from test.db.cypher_cases.access import (
    HIDDEN,
    OPEN,
    diamond,
    test_a_hidden_vertex_is_never_matched,
    test_a_scoped_delete_removes_exactly_its_scope,
    test_the_short_path_through_a_hidden_vertex_is_not_taken,
    write_zone,
)

__all__ = [
    "diamond",
    "test_a_hidden_vertex_is_never_matched",
    "test_a_scoped_delete_removes_exactly_its_scope",
    "test_the_short_path_through_a_hidden_vertex_is_not_taken",
]


@pytest.fixture
def zones_merge() -> str:
    return "union"


def test_a_union_field_holds_every_writer_s_zones(conn_conf, diamond):
    write_zone(conn_conf, diamond, ["d"], [], HIDDEN)

    with ConnectionManager(connection_config=conn_conf) as db:
        (doc,) = db.fetch_docs("Node", filters=["==", "d", "key"])

    assert sorted(doc["access_zones"]) == [OPEN, HIDDEN]
