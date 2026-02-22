"""Tests for NebulaGraph edge insertion and fetching.

Vertices are seeded once per module in a fresh space; each test only inserts
edges (which is cheap).
"""

import pytest

pytestmark = pytest.mark.nebula


@pytest.fixture(scope="module")
def edge_db(_module_db):
    """Seed vertices once for all edge tests."""
    _module_db.upsert_docs_batch(
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ],
        "Person",
        match_keys=["name"],
    )
    _module_db.upsert_docs_batch(
        [
            {"name": "Berlin", "population": 3700000},
            {"name": "Munich", "population": 1500000},
        ],
        "City",
        match_keys=["name"],
    )
    return _module_db


def test_insert_edges(edge_db):
    """Test inserting edges between vertices."""
    edges = [[{"name": "Alice"}, {"name": "Berlin"}, {}]]
    edge_db.insert_edges_batch(
        edges,
        source_class="Person",
        target_class="City",
        relation_name="lives_in",
        match_keys_source=("name",),
        match_keys_target=("name",),
    )
    edge_docs = edge_db.fetch_edges("Person", "Alice", edge_type="lives_in")
    assert len(edge_docs) >= 1


def test_insert_multiple_edges(edge_db):
    """Test inserting multiple edges."""
    edges = [
        [{"name": "Alice"}, {"name": "Berlin"}, {}],
        [{"name": "Bob"}, {"name": "Munich"}, {}],
    ]
    edge_db.insert_edges_batch(
        edges,
        source_class="Person",
        target_class="City",
        relation_name="lives_in",
        match_keys_source=("name",),
        match_keys_target=("name",),
    )
    alice_edges = edge_db.fetch_edges("Person", "Alice", edge_type="lives_in")
    bob_edges = edge_db.fetch_edges("Person", "Bob", edge_type="lives_in")
    assert len(alice_edges) >= 1
    assert len(bob_edges) >= 1


def test_insert_self_referencing_edges(edge_db):
    """Test edges where source and target share the same type (Person -> Person)."""
    edges = [[{"name": "Alice"}, {"name": "Bob"}, {}]]
    edge_db.insert_edges_batch(
        edges,
        source_class="Person",
        target_class="Person",
        relation_name="knows",
        match_keys_source=("name",),
        match_keys_target=("name",),
    )
    edge_docs = edge_db.fetch_edges("Person", "Alice", edge_type="knows")
    assert len(edge_docs) >= 1


def test_insert_edges_empty_batch(edge_db):
    """Test that inserting an empty edge batch is a no-op."""
    edge_db.insert_edges_batch(
        [],
        source_class="Person",
        target_class="City",
        relation_name="lives_in",
        match_keys_source=("name",),
        match_keys_target=("name",),
    )
