"""Tests for Memgraph data ingestion and aggregation (shared from cypher_cases)."""

from test.db.cypher_cases.lifecycle import (
    modes,
    test_aggregation_count,
    test_aggregation_grouped_by_every_function,
    test_aggregation_min_max_avg,
    test_aggregation_sorted_unique,
    test_ingest,
    test_keep_absent_documents,
)

__all__ = [
    "modes",
    "test_aggregation_count",
    "test_aggregation_grouped_by_every_function",
    "test_aggregation_min_max_avg",
    "test_aggregation_sorted_unique",
    "test_ingest",
    "test_keep_absent_documents",
]
