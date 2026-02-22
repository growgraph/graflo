"""Tests for NebulaGraph document fetch, filter, and presence/absence operations.

All tests are read-only queries against a shared dataset seeded once per
module in a fresh space.
"""

import pytest

pytestmark = pytest.mark.nebula


@pytest.fixture(scope="module")
def fetch_db(_module_db):
    """Seed a standard dataset once for all fetch tests."""
    persons = [
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 30},
        {"name": "Charlie", "age": 25},
    ] + [{"name": f"User_{i}", "age": 20 + i} for i in range(7)]
    _module_db.upsert_docs_batch(persons, "Person", match_keys=["name"])
    return _module_db


def test_fetch_docs_with_limit(fetch_db):
    """Test fetching documents with limit."""
    result = fetch_db.fetch_docs("Person", limit=5)
    assert len(result) == 5


def test_fetch_docs_with_filter(fetch_db):
    """Test fetching documents with filters."""
    result = fetch_db.fetch_docs("Person", filters=["==", "Alice", "name"])
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_fetch_docs_with_projection(fetch_db):
    """Test fetching documents with key projection."""
    result = fetch_db.fetch_docs("Person", return_keys=["name"])
    assert len(result) >= 1
    assert "name" in result[0]


def test_fetch_present_documents(fetch_db):
    """Test fetch_present_documents returns only existing docs."""
    present = fetch_db.fetch_present_documents(
        [{"name": "Alice"}, {"name": "Unknown"}],
        "Person",
        match_keys=["name"],
    )
    assert len(present) == 1


def test_keep_absent_documents(fetch_db):
    """Test keep_absent_documents returns only non-existing docs."""
    absent = fetch_db.keep_absent_documents(
        [{"name": "Alice"}, {"name": "Unknown"}],
        "Person",
        match_keys=["name"],
    )
    assert len(absent) == 1
    assert absent[0]["name"] == "Unknown"


def test_fetch_present_documents_empty_batch(fetch_db):
    """Test fetch_present_documents with empty batch returns empty list."""
    present = fetch_db.fetch_present_documents([], "Person", match_keys=["name"])
    assert present == []


def test_keep_absent_documents_empty_batch(fetch_db):
    """Test keep_absent_documents with empty batch returns empty list."""
    absent = fetch_db.keep_absent_documents([], "Person", match_keys=["name"])
    assert absent == []
