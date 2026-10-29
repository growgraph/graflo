"""Read guards, scoped deletes and union-merged zones against a live FalkorDB.

The cases are shared with Neo4j (``test/db/cypher_cases``).
"""

from __future__ import annotations

import pytest

from test.db.cypher_cases.access import (
    diamond,
    test_a_hidden_vertex_is_never_matched,
    test_a_keyed_edge_unions_its_zones_with_and_without_its_key,
    test_a_scoped_delete_removes_exactly_its_scope,
    test_a_union_field_holds_every_writer_s_zones,
    test_an_edge_written_twice_holds_both_writers_zones,
    test_the_short_path_through_a_hidden_vertex_is_not_taken,
)

__all__ = [
    "diamond",
    "test_a_hidden_vertex_is_never_matched",
    "test_a_keyed_edge_unions_its_zones_with_and_without_its_key",
    "test_a_scoped_delete_removes_exactly_its_scope",
    "test_a_union_field_holds_every_writer_s_zones",
    "test_an_edge_written_twice_holds_both_writers_zones",
    "test_the_short_path_through_a_hidden_vertex_is_not_taken",
]


@pytest.fixture
def zones_merge() -> str:
    return "union"
