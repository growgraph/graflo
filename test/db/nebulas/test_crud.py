"""Tests for NebulaGraph vertex write (mutation) operations.

These tests check exact vertex counts after mutations, so each test gets a
clean slate via the function-scoped ``nebula_db`` fixture.
"""

import pytest

pytestmark = pytest.mark.nebula


def test_upsert_single_vertex(nebula_db):
    """Test upserting a single vertex."""
    nebula_db.upsert_docs_batch(
        [{"name": "Alice", "age": 30}], "Person", match_keys=["name"]
    )
    result = nebula_db.fetch_docs("Person")
    assert len(result) == 1


def test_upsert_updates_existing(nebula_db):
    """Test that upsert updates an existing vertex."""
    nebula_db.upsert_docs_batch(
        [{"name": "Alice", "age": 30}], "Person", match_keys=["name"]
    )
    nebula_db.upsert_docs_batch(
        [{"name": "Alice", "age": 31}], "Person", match_keys=["name"]
    )
    result = nebula_db.fetch_docs("Person")
    assert len(result) == 1


def test_upsert_multiple(nebula_db):
    """Test upserting multiple vertices."""
    docs = [
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 30},
        {"name": "Charlie", "age": 25},
    ]
    nebula_db.upsert_docs_batch(docs, "Person", match_keys=["name"])
    result = nebula_db.fetch_docs("Person")
    assert len(result) == 3


def test_upsert_empty_batch(nebula_db):
    """Test that upserting an empty batch is a no-op."""
    nebula_db.upsert_docs_batch([], "Person", match_keys=["name"])
    result = nebula_db.fetch_docs("Person")
    assert len(result) == 0
