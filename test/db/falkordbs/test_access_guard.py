"""Read guards and scoped deletes against a live FalkorDB.

The cases are shared with Neo4j (``test/db/cypher_cases``). FalkorDB has no
union merge, so zones are replaced on write.
"""

from __future__ import annotations

import pytest

from test.db.cypher_cases.access import (
    diamond,
    test_a_hidden_vertex_is_never_matched,
    test_a_scoped_delete_removes_exactly_its_scope,
    test_the_short_path_through_a_hidden_vertex_is_not_taken,
)

__all__ = [
    "diamond",
    "test_a_hidden_vertex_is_never_matched",
    "test_a_scoped_delete_removes_exactly_its_scope",
    "test_the_short_path_through_a_hidden_vertex_is_not_taken",
]


@pytest.fixture
def zones_merge() -> str:
    return "replace"
