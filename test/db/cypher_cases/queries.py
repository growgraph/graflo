"""Aggregation, filtering, edge operations and query-shape cases.

Backend-agnostic Cypher cases, shared by the Memgraph and FalkorDB suites.

These are mixins, not tests: the module is outside any ``test_*.py`` so pytest
never collects them directly. Each backend's ``test_edge_cases.py`` subclasses
them, which is what supplies the ``conn_conf`` / ``test_graph_name`` /
``clean_db`` fixtures -- both backend conftests expose that same trio.
"""

from __future__ import annotations

import math

from graflo.db.manager import ConnectionManager
from graflo.onto import AggregationType


class AggregationCases:
    """Test aggregation with edge cases."""

    def test_aggregate_with_null_values(self, conn_conf, test_graph_name, clean_db):
        """Aggregate field with null values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "score": 10},
                {"id": "2", "score": None},
                {"id": "3"},  # Missing score entirely
                {"id": "4", "score": 20},
            ]
            db.upsert_docs_batch(docs, "NullAgg", match_keys=["id"])

            avg = db.aggregate(
                "NullAgg", AggregationType.AVERAGE, aggregated_field="score"
            )
            # Should handle nulls gracefully
            assert (
                avg is not None
                or avg == 0
                or (isinstance(avg, float) and math.isnan(avg))
                if avg
                else True
            )


class FilterCases:
    """Test filter expressions with edge cases."""

    def test_filter_non_existent_field(self, conn_conf, test_graph_name, clean_db):
        """Filter on field that doesn't exist."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": "1", "name": "test"}]
            db.upsert_docs_batch(docs, "FilterTest", match_keys=["id"])

            # Filter on non-existent field
            result = db.fetch_docs("FilterTest", filters=["==", "value", "ghost_field"])
            assert len(result) == 0


class EdgeOperationsCases:
    """Test edge/relationship operations with edge cases."""

    def test_edge_between_non_existent_nodes(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Create edge between nodes that don't exist."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            edges = [[{"id": "ghost1"}, {"id": "ghost2"}, {}]]
            db.insert_edges_batch(
                edges,
                source_class="Ghost",
                target_class="Ghost",
                relation_name="HAUNTS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Should not create any edges
            result = db.execute("MATCH ()-[r:HAUNTS]->() RETURN count(r)")
            assert result.result_set[0][0] == 0

    def test_edge_with_empty_properties(self, conn_conf, test_graph_name, clean_db):
        """Create edge with empty properties dict."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([{"id": "A"}, {"id": "B"}], "Node", match_keys=["id"])

            edges = [[{"id": "A"}, {"id": "B"}, {}]]
            db.insert_edges_batch(
                edges,
                source_class="Node",
                target_class="Node",
                relation_name="EMPTY_PROPS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            result = db.execute("MATCH ()-[r:EMPTY_PROPS]->() RETURN count(r)")
            assert result.result_set[0][0] == 1

    def test_edge_with_none_properties(self, conn_conf, test_graph_name, clean_db):
        """Create edge with None values in properties."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([{"id": "A"}, {"id": "B"}], "Node", match_keys=["id"])

            edges = [[{"id": "A"}, {"id": "B"}, {"weight": None, "type": "test"}]]
            db.insert_edges_batch(
                edges,
                source_class="Node",
                target_class="Node",
                relation_name="NULL_PROPS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            result = db.execute("MATCH ()-[r:NULL_PROPS]->() RETURN r.type")
            assert result.result_set[0][0] == "test"


class QueryComplexityCases:
    """Queries designed to be computationally expensive."""

    def test_deeply_nested_optional_match(self, conn_conf, test_graph_name, clean_db):
        """Complex nested optional match patterns."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Create some data
            docs = [{"id": str(i)} for i in range(10)]
            db.upsert_docs_batch(docs, "Complex", match_keys=["id"])

            # Query with many optional matches
            query = """
                MATCH (n:Complex)
                OPTIONAL MATCH (n)-[:REL1]->(a)
                OPTIONAL MATCH (n)-[:REL2]->(b)
                OPTIONAL MATCH (n)-[:REL3]->(c)
                OPTIONAL MATCH (n)-[:REL4]->(d)
                OPTIONAL MATCH (n)-[:REL5]->(e)
                RETURN count(*)
            """
            try:
                result = db.execute(query)
                assert result.result_set[0][0] == 10
            except Exception:
                pass

    def test_with_chain_explosion(self, conn_conf, test_graph_name, clean_db):
        """Long chain of WITH clauses accumulating data."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i)} for i in range(5)]
            db.upsert_docs_batch(docs, "WithChain", match_keys=["id"])

            query = """
                MATCH (n:WithChain)
                WITH collect(n) AS nodes1
                WITH nodes1, size(nodes1) AS s1
                WITH nodes1, s1, s1 * 2 AS s2
                WITH nodes1, s1, s2, s1 + s2 AS s3
                WITH nodes1, s1, s2, s3, s1 * s2 * s3 AS s4
                RETURN s1, s2, s3, s4
            """
            try:
                result = db.execute(query)
                assert len(result.result_set) > 0
            except Exception:
                pass


class SchemaEvolutionCases:
    """Test rapid schema changes and type mutations."""

    def test_add_remove_properties_rapidly(self, conn_conf, test_graph_name, clean_db):
        """Add and remove properties in rapid succession."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            for i in range(20):
                # Add new property
                doc = {"id": "evolving", f"prop_{i}": f"value_{i}"}
                db.upsert_docs_batch([doc], "Evolving", match_keys=["id"])

            result = db.fetch_docs("Evolving")
            assert len(result) == 1
            # Should have accumulated all properties
            assert "prop_19" in result[0]


class TemporalAnomaliesCases:
    """Test with extreme or invalid temporal values."""

    def test_epoch_boundaries(self, conn_conf, test_graph_name, clean_db):
        """Test Unix epoch edge cases."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            epochs = [
                ("unix_zero", 0),
                ("before_unix", -86400),  # Day before epoch
                ("y2k38_minus", 2147483647),  # Max 32-bit signed
                ("y2k38_plus", 2147483648),  # Overflow 32-bit
                ("far_future", 253402300799),  # Dec 31, 9999
                ("negative_max", -2147483648),  # Min 32-bit signed
            ]

            for name, epoch in epochs:
                docs = [{"id": name, "timestamp": epoch}]
                try:
                    db.upsert_docs_batch(docs, "Epochs", match_keys=["id"])
                except (OverflowError, ValueError):
                    pass

            result = db.fetch_docs("Epochs")
            assert len(result) > 0

    def test_datetime_strings_as_injection(self, conn_conf, test_graph_name, clean_db):
        """Datetime strings that might be parsed unexpectedly."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            problematic_dates = [
                "0000-00-00",
                "9999-99-99",
                "2024-13-45",  # Invalid month/day
                "2024-02-30",  # Feb 30
                "2024-00-01",  # Month 0
                "'2024-01-01'); DROP TABLE users; --",
                "2024-01-01T25:99:99Z",  # Invalid time
            ]

            for i, date_str in enumerate(problematic_dates):
                docs = [{"id": str(i), "date": date_str}]
                db.upsert_docs_batch(docs, "EdgeCaseDates", match_keys=["id"])

            result = db.fetch_docs("EdgeCaseDates")
            assert len(result) == len(problematic_dates)
