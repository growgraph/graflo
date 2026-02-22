"""Tests for NebulaGraph aggregation operations.

Data is seeded once per module in a fresh space -- no deletion needed.
"""

import pytest

from graflo.onto import AggregationType

pytestmark = pytest.mark.nebula


@pytest.fixture(scope="module")
def agg_db(_module_db):
    """Seed Person vertices once for all aggregate tests."""
    _module_db.upsert_docs_batch(
        [
            {"name": "Alice", "age": 20},
            {"name": "Bob", "age": 30},
            {"name": "Charlie", "age": 40},
        ],
        "Person",
        match_keys=["name"],
    )
    return _module_db


def test_aggregate_count(agg_db):
    """Test COUNT aggregation."""
    count = agg_db.aggregate("Person", aggregation_function="COUNT")
    assert count == 3


def test_aggregate_max(agg_db):
    """Test MAX aggregation."""
    result = agg_db.aggregate(
        "Person", aggregation_function="MAX", aggregated_field="age"
    )
    assert result == 40


def test_aggregate_min(agg_db):
    """Test MIN aggregation."""
    result = agg_db.aggregate(
        "Person", aggregation_function="MIN", aggregated_field="age"
    )
    assert result == 20


def test_aggregate_average(agg_db):
    """Test AVERAGE aggregation via AggregationType enum."""
    result = agg_db.aggregate(
        "Person",
        aggregation_function=AggregationType.AVERAGE,
        aggregated_field="age",
    )
    assert result == 30
