"""Volume and memory-pressure cases.

Backend-agnostic Cypher cases, shared by the Memgraph and FalkorDB suites.

These are mixins, not tests: the module is outside any ``test_*.py`` so pytest
never collects them directly. Each backend's ``test_edge_cases.py`` subclasses
them, which is what supplies the ``conn_conf`` / ``test_graph_name`` /
``clean_db`` fixtures -- both backend conftests expose that same trio.
"""

from __future__ import annotations

from graflo.db.manager import ConnectionManager
from graflo.onto import AggregationType


class BatchStressCases:
    """Stress test batch operations."""

    def test_large_batch_insert(self, conn_conf, test_graph_name, clean_db):
        """Insert very large batch."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # 10,000 documents
            docs = [{"id": str(i), "data": f"value_{i}"} for i in range(10000)]
            db.upsert_docs_batch(docs, "LargeBatch", match_keys=["id"])

            count = db.aggregate("LargeBatch", AggregationType.COUNT)
            assert count == 10000

    def test_batch_with_duplicates(self, conn_conf, test_graph_name, clean_db):
        """Batch containing duplicate keys."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "version": 1},
                {"id": "2", "version": 1},
                {"id": "1", "version": 2},  # Duplicate id
                {"id": "1", "version": 3},  # Another duplicate
            ]
            db.upsert_docs_batch(docs, "Duplicates", match_keys=["id"])

            result = db.fetch_docs("Duplicates")
            # Should have 2 unique ids
            ids = {r["id"] for r in result}
            assert len(ids) == 2

    def test_rapid_fire_small_batches(self, conn_conf, test_graph_name, clean_db):
        """Many small rapid batches."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            for i in range(100):
                docs = [{"id": str(i), "batch": i}]
                db.upsert_docs_batch(docs, "RapidFire", match_keys=["id"])

            count = db.aggregate("RapidFire", AggregationType.COUNT)
            assert count == 100


class MemoryExhaustionCases:
    """Memory exhaustion and resource abuse testing."""

    def test_exponential_property_growth(self, conn_conf, test_graph_name, clean_db):
        """Property value that doubles on each upsert - memory stress test."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            payload = "A"
            for i in range(20):  # 2^20 = 1MB final payload
                docs = [{"id": "growing", "payload": payload}]
                db.upsert_docs_batch(docs, "MemoryLoad", match_keys=["id"])
                payload = payload * 2

            result = db.fetch_docs("MemoryLoad")
            assert len(result) == 1

    def test_million_tiny_properties(self, conn_conf, test_graph_name, clean_db):
        """Document with extreme number of tiny properties."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # 10,000 single-char properties
            doc = {"id": "hydra"}
            for i in range(10000):
                doc[f"p{i}"] = "x"

            try:
                db.upsert_docs_batch([doc], "Hydra", match_keys=["id"])
            except Exception:
                pass

    def test_recursive_json_like_string(self, conn_conf, test_graph_name, clean_db):
        """String that looks like deeply nested JSON - parser confusion."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # String that might confuse naive JSON parsers
            nested_json = '{"a":' * 100 + '"value"' + "}" * 100
            docs = [{"id": "nested_json", "data": nested_json}]
            db.upsert_docs_batch(docs, "NestedJson", match_keys=["id"])

            result = db.fetch_docs("NestedJson")
            assert len(result) == 1
            assert result[0]["data"] == nested_json

    def test_binary_payload_in_string(self, conn_conf, test_graph_name, clean_db):
        """Binary data disguised as string."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Random-looking binary data
            binary_payload = bytes(range(256)).decode("latin-1")
            docs = [{"id": "binary", "payload": binary_payload}]
            try:
                db.upsert_docs_batch(docs, "Binary", match_keys=["id"])
            except Exception:
                pass
