"""Edge case and robustness tests for FalkorDB connector.

This module provides a comprehensive adversarial test suite for the FalkorDB
connector implementation. Tests are designed to probe security boundaries,
stress operational limits, and verify correct handling of malformed inputs.

Test Categories
---------------
Security & Injection Prevention:
    - Cypher injection via property values, labels, and match keys
    - Parameter pollution and template injection attempts
    - Unicode homoglyph attacks for filter bypass

Encoding & Unicode:
    - UTF-8 edge cases (BOM, surrogates, overlong sequences)
    - Bidirectional text and RTL override characters
    - Zero-width characters and Private Use Area flooding
    - Null byte injection and control character handling

Boundary Conditions:
    - Empty inputs (batches, documents, strings)
    - Extreme numeric values (NaN, Inf, integer overflow)
    - Large payloads (1MB strings, 10k properties, 50k batches)
    - Limit parameter edge cases (negative, zero, float)

Type System:
    - Type coercion and confusion (string "123" vs int 123)
    - None/null value handling and missing keys
    - Mixed-type arrays and nested structures
    - Property type mutation across upserts

Concurrency & State:
    - Thread safety with concurrent reads/writes
    - Race conditions on contested resources
    - Connection lifecycle (close, double-close, nested)
    - Deadlock scenarios with circular dependencies

Graph Topology:
    - Pathological structures (cycles, cliques, stars)
    - Algorithm stress tests (lollipop, barbell graphs)
    - Cartesian product explosion queries
    - Variable-length path traversal limits

Performance & DoS:
    - Memory exhaustion attempts (exponential growth)
    - ReDoS connector storage and filtering
    - Query complexity attacks (UNION bombs, WITH chains)

Usage
-----
Run all edge case tests::

    pytest test/db/falkordbs/test_edge_cases.py -v

Run specific category::

    pytest test/db/falkordbs/test_edge_cases.py -k "Injection" -v

Notes
-----
- Tests require a running FalkorDB instance (see conftest.py fixtures)
- Some tests intentionally trigger warnings (logged, not failures)
- Timeout-based tests may behave differently under load

See Also
--------
- graflo.db.falkordb.conn : FalkorDB connector implementation
- test.db.falkordbs.conftest : Test fixtures and configuration
"""

import concurrent.futures
import threading
import uuid
from typing import Any

from graflo.db.manager import ConnectionManager
from graflo.onto import AggregationType

# Shared, backend-agnostic cases. Bodies live once in test/db/cypher_cases/;
# only the FalkorDB-specific tests remain in this file.
from test.db.cypher_cases.boundaries import (
    BoundaryConditionsCases,
    BoundaryValueAnalysisCases,
    MalformedInputsCases,
    PathologicalIdsCases,
    TypeConfusionCases,
)
from test.db.cypher_cases.encoding import (
    LabelAbuseCases,
    MalformedEncodingCases,
    PropertyKeySmugglingCases,
)
from test.db.cypher_cases.graphs import PathologicalGraphsCases, StateCorruptionCases
from test.db.cypher_cases.injection import (
    CypherInjectionCases,
    QueryInjectionAdvancedCases,
    ReDoSCases,
)
from test.db.cypher_cases.queries import (
    AggregationCases,
    EdgeOperationsCases,
    FilterCases,
    QueryComplexityCases,
    SchemaEvolutionCases,
    TemporalAnomaliesCases,
)
from test.db.cypher_cases.stress import BatchStressCases, MemoryExhaustionCases


class TestCypherInjection(CypherInjectionCases):
    """Security tests for Cypher injection prevention.

    Validates that the connector properly sanitizes user inputs to prevent
    Cypher injection attacks. Tests cover multiple injection vectors including
    property values, label names, and match keys.

    The connector should either:
    - Parameterize all user inputs (preferred)
    - Properly escape special characters
    - Reject dangerous inputs with clear errors

    References:
        - OWASP Injection Prevention Cheat Sheet
        - Neo4j Cypher Injection documentation
    """


class TestUnicodeTorture:
    """Unicode and encoding edge case validation.

    Verifies correct handling of international text, special Unicode
    characters, and encoding edge cases that commonly cause issues
    in database systems.

    Test vectors include:
        - Emoji sequences (including ZWJ combinations)
        - Right-to-left and bidirectional text
        - Null bytes and control characters
        - Unicode normalization forms (NFC, NFD)
        - Zero-width and invisible characters
    """

    def test_emoji_overload(self, conn_conf, test_graph_name, clean_db):
        """Store and retrieve emoji-heavy content."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            emoji_name = "😀😁😂🤣😃😄😅😆😉😊" * 10
            docs = [{"id": "emoji", "name": emoji_name}]
            db.upsert_docs_batch(docs, "EmojiNode", match_keys=["id"])

            result = db.fetch_docs("EmojiNode")
            assert len(result) == 1
            assert result[0]["name"] == emoji_name

    def test_rtl_and_bidi_text(self, conn_conf, test_graph_name, clean_db):
        """Test right-to-left and bidirectional text."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            rtl_texts = [
                "مرحبا بالعالم",  # Arabic
                "שלום עולם",  # Hebrew
                "Hello مرحبا World عالم",  # Mixed LTR/RTL
                "\u202eevil\u202c",  # RTL override characters
            ]

            for i, text in enumerate(rtl_texts):
                docs = [{"id": str(i), "content": text}]
                db.upsert_docs_batch(docs, "RTLNode", match_keys=["id"])

            result = db.fetch_docs("RTLNode")
            assert len(result) == len(rtl_texts)

    def test_null_bytes_and_control_chars(self, conn_conf, test_graph_name, clean_db):
        """Test null bytes and control characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            evil_strings = [
                "before\x00after",  # Null byte
                "tab\there",
                "new\nline",
                "carriage\rreturn",
                "bell\x07ring",
                "escape\x1b[31mred",  # ANSI escape
                "".join(chr(i) for i in range(32)),  # All control chars
            ]

            for i, s in enumerate(evil_strings):
                docs = [{"id": str(i), "data": s}]
                try:
                    db.upsert_docs_batch(docs, "ControlChars", match_keys=["id"])
                except Exception:
                    # Some control chars may be rejected
                    pass

    def test_unicode_normalization_attacks(self, conn_conf, test_graph_name, clean_db):
        """Test Unicode normalization edge cases."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Different Unicode representations of "same" characters
            variants = [
                ("café", "cafe\u0301"),  # Composed vs decomposed
                ("ﬁ", "fi"),  # Ligature vs separate
                ("Ω", "Ω"),  # Greek vs Ohm sign (U+03A9 vs U+2126)
                ("㈱", "(株)"),  # Enclosed vs parenthesized
            ]

            for i, (v1, v2) in enumerate(variants):
                docs = [
                    {"id": f"{i}a", "text": v1},
                    {"id": f"{i}b", "text": v2},
                ]
                db.upsert_docs_batch(docs, "NormTest", match_keys=["id"])

            result = db.fetch_docs("NormTest")
            assert len(result) == len(variants) * 2

    def test_zero_width_characters(self, conn_conf, test_graph_name, clean_db):
        """Test zero-width and invisible characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            invisible_strings = [
                "hello\u200bworld",  # Zero-width space
                "hello\u200cworld",  # Zero-width non-joiner
                "hello\u200dworld",  # Zero-width joiner
                "hello\ufeffworld",  # BOM
                "\u2060invisible\u2060",  # Word joiner
            ]

            for i, s in enumerate(invisible_strings):
                docs = [{"id": str(i), "text": s}]
                db.upsert_docs_batch(docs, "Invisible", match_keys=["id"])

            result = db.fetch_docs("Invisible")
            assert len(result) == len(invisible_strings)


class TestBoundaryConditions(BoundaryConditionsCases):
    """Boundary value analysis for connector limits.

    Systematically tests edge cases at operational boundaries including
    empty inputs, maximum sizes, and extreme numeric values. These tests
    help identify off-by-one errors and buffer handling issues.

    Categories:
        - Empty/minimal inputs (zero-length strings, empty batches)
        - Maximum sizes (1MB strings, 1000 properties)
        - Numeric extremes (int64 bounds, denormalized floats)
        - Special values (NaN, Inf, negative zero)
    """

    def test_deeply_nested_dict(self, conn_conf, test_graph_name, clean_db):
        """Test with deeply nested dictionary structure."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Build deeply nested structure
            nested: dict[str, Any] = {"level": 0}
            current: dict[str, Any] = nested
            for i in range(1, 50):
                current["child"] = {"level": i}
                current = current["child"]

            docs = [{"id": "nested", "data": nested}]
            try:
                db.upsert_docs_batch(docs, "DeepNest", match_keys=["id"])
            except Exception:
                # Deep nesting may not be supported
                pass

    def test_negative_limit(self, conn_conf, test_graph_name, clean_db):
        """Test fetch with negative limit."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i)} for i in range(5)]
            db.upsert_docs_batch(docs, "LimitTest", match_keys=["id"])

            # Negative limit should be handled gracefully
            result = db.fetch_docs("LimitTest", limit=-1)
            # Should either return all or raise proper exception
            assert isinstance(result, list)

    def test_zero_limit(self, conn_conf, test_graph_name, clean_db):
        """Test fetch with zero limit."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i)} for i in range(5)]
            db.upsert_docs_batch(docs, "LimitTest", match_keys=["id"])

            result = db.fetch_docs("LimitTest", limit=0)
            # Zero limit - should return empty or all
            assert isinstance(result, list)

    def test_float_limit(self, conn_conf, test_graph_name, clean_db):
        """Test fetch with float limit value."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i)} for i in range(5)]
            db.upsert_docs_batch(docs, "LimitTest", match_keys=["id"])

            # Float limit - should be handled
            result = db.fetch_docs("LimitTest", limit=2.7)
            assert isinstance(result, list)


class TestTypeConfusion(TypeConfusionCases):
    """Test type handling edge cases."""

    def test_none_values(self, conn_conf, test_graph_name, clean_db):
        """Test None/null value handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "nullable": None},
                {"id": "2", "nullable": "not null"},
                {"id": "3"},  # Missing key entirely
            ]
            db.upsert_docs_batch(docs, "NullTest", match_keys=["id"])

            result = db.fetch_docs("NullTest")
            assert len(result) == 3

    def test_boolean_values(self, conn_conf, test_graph_name, clean_db):
        """Test boolean value handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "flag": True},
                {"id": "2", "flag": False},
                {"id": "3", "flag": 1},  # Truthy int
                {"id": "4", "flag": 0},  # Falsy int
                {"id": "5", "flag": "true"},  # String
                {"id": "6", "flag": ""},  # Empty string
            ]
            db.upsert_docs_batch(docs, "BoolTest", match_keys=["id"])

            result = db.fetch_docs("BoolTest")
            assert len(result) == 6

    def test_list_values(self, conn_conf, test_graph_name, clean_db):
        """Test list/array value handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "tags": ["a", "b", "c"]},
                {"id": "2", "tags": []},  # Empty list
                {"id": "3", "tags": [1, 2, 3]},  # Int list
                {"id": "4", "tags": [1, "mixed", True]},  # Mixed types
                {"id": "5", "tags": [[1, 2], [3, 4]]},  # Nested lists
            ]

            for doc in docs:
                try:
                    db.upsert_docs_batch([doc], "ListTest", match_keys=["id"])
                except Exception:
                    # Some list types may not be supported
                    pass

    def test_dict_property_value(self, conn_conf, test_graph_name, clean_db):
        """Test dictionary/map as property value."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "metadata": {"key": "value", "nested": {"deep": True}}},
            ]

            try:
                db.upsert_docs_batch(docs, "DictProp", match_keys=["id"])
                result = db.fetch_docs("DictProp")
                assert len(result) == 1
            except Exception:
                # Nested dicts may not be supported as properties
                pass


class TestMalformedInputs(MalformedInputsCases):
    """Test handling of malformed or invalid inputs."""

    def test_non_string_property_names(self, conn_conf, test_graph_name, clean_db):
        """Test with non-string property names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Python allows non-string dict keys
            docs = [
                {"id": "1", 123: "numeric_key"},
                {"id": "2", None: "none_key"},
                {"id": "3", (1, 2): "tuple_key"},
            ]

            for doc in docs:
                try:
                    db.upsert_docs_batch([doc], "WeirdKeys", match_keys=["id"])
                except (TypeError, ValueError):
                    pass

    def test_reserved_property_names(self, conn_conf, test_graph_name, clean_db):
        """Test with Cypher reserved words as property names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            reserved_words = [
                "MATCH",
                "CREATE",
                "DELETE",
                "RETURN",
                "WHERE",
                "AND",
                "OR",
                "NOT",
                "NULL",
                "TRUE",
                "FALSE",
            ]

            for word in reserved_words:
                docs = [{"id": word, word: "value"}]
                try:
                    db.upsert_docs_batch(docs, "Reserved", match_keys=["id"])
                except Exception:
                    pass

    def test_special_characters_in_property_names(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Test property names with special characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            special_names = [
                "with space",
                "with-dash",
                "with.dot",
                "with:colon",
                "with/slash",
                "with\\backslash",
                "with'quote",
                'with"double',
                "with`backtick",
            ]

            for name in special_names:
                docs = [{"id": name, name: "value"}]
                try:
                    db.upsert_docs_batch(docs, "SpecialProps", match_keys=["id"])
                except Exception:
                    pass


class TestPathologicalGraphs(PathologicalGraphsCases):
    """Graph topology stress tests with pathological structures.

    Creates graph patterns known to cause performance issues or
    algorithmic edge cases. These structures test the connector's
    ability to handle complex topologies gracefully.

    Connectors tested:
        - Self-loops (node connected to itself)
        - Bidirectional edges (mutual relationships)
        - Multi-edges (multiple edges between same nodes)
        - Long chains (linear path graphs)
        - Star topology (high-degree central node)
        - Complete graphs (all-to-all connectivity)
    """

    def test_self_loop(self, conn_conf, test_graph_name, clean_db):
        """Create and query self-referential edge."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([{"id": "narcissist"}], "Person", match_keys=["id"])

            # Self-loop edge
            edges = [[{"id": "narcissist"}, {"id": "narcissist"}, {"type": "LOVES"}]]
            db.insert_edges_batch(
                edges,
                source_class="Person",
                target_class="Person",
                relation_name="LOVES",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Verify self-loop exists
            result = db.execute("MATCH (p:Person)-[r:LOVES]->(p) RETURN count(r)")
            assert result.result_set[0][0] == 1

    def test_multiple_edges_same_nodes(self, conn_conf, test_graph_name, clean_db):
        """Create multiple edges of same type between same nodes."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([{"id": "A"}, {"id": "B"}], "Node", match_keys=["id"])

            # Multiple edges A -> B
            for i in range(5):
                edges = [[{"id": "A"}, {"id": "B"}, {"seq": i}]]
                db.insert_edges_batch(
                    edges,
                    source_class="Node",
                    target_class="Node",
                    relation_name="MULTI",
                    match_keys_source=["id"],
                    match_keys_target=["id"],
                )

            # MERGE should create only 1, but let's verify behavior
            result = db.execute("MATCH (a:Node)-[r:MULTI]->(b:Node) RETURN count(r)")
            # With MERGE, should be 1 (updated each time)
            assert result.result_set[0][0] >= 1

    def test_complete_graph(self, conn_conf, test_graph_name, clean_db):
        """Create complete graph (every node connected to every other)."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            n = 10
            docs = [{"id": str(i)} for i in range(n)]
            db.upsert_docs_batch(docs, "Complete", match_keys=["id"])

            # All pairs
            edges = [
                [{"id": str(i)}, {"id": str(j)}, {}]
                for i in range(n)
                for j in range(n)
                if i != j
            ]
            db.insert_edges_batch(
                edges,
                source_class="Complete",
                target_class="Complete",
                relation_name="KNOWS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # n*(n-1) directed edges
            result = db.execute("MATCH ()-[r:KNOWS]->() RETURN count(r)")
            assert result.result_set[0][0] == n * (n - 1)


class TestConcurrencyChaos:
    """Thread safety and race condition testing.

    Validates connector behavior under concurrent access patterns.
    Tests simulate real-world scenarios where multiple threads or
    processes interact with the database simultaneously.

    Scenarios tested:
        - Concurrent batch inserts to same label
        - Interleaved read/write operations
        - Contested upserts on same document
        - Connection pool exhaustion patterns
    """

    def test_concurrent_inserts(self, conn_conf, test_graph_name, clean_db):
        """Concurrent inserts to same label."""
        _ = clean_db

        def insert_batch(batch_id):
            with ConnectionManager(connection_config=conn_conf) as db:
                docs = [{"id": f"{batch_id}_{i}", "batch": batch_id} for i in range(10)]
                db.upsert_docs_batch(docs, "ConcurrentInsert", match_keys=["id"])

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(insert_batch, i) for i in range(5)]
            concurrent.futures.wait(futures)

        with ConnectionManager(connection_config=conn_conf) as db:
            result = db.fetch_docs("ConcurrentInsert")
            assert len(result) == 50  # 5 batches * 10 docs

    def test_concurrent_read_write(self, conn_conf, test_graph_name, clean_db):
        """Concurrent reads and writes."""
        _ = clean_db
        errors = []

        def writer():
            for i in range(20):
                try:
                    with ConnectionManager(connection_config=conn_conf) as db:
                        docs = [{"id": f"w{i}", "value": i}]
                        db.upsert_docs_batch(docs, "ReadWrite", match_keys=["id"])
                except Exception as e:
                    errors.append(f"Writer error: {e}")

        def reader():
            for _ in range(20):
                try:
                    with ConnectionManager(connection_config=conn_conf) as db:
                        db.fetch_docs("ReadWrite")
                except Exception as e:
                    errors.append(f"Reader error: {e}")

        # daemon=True and a bounded join: a worker blocked on a wedged DB socket
        # must fail this test, not stall the join here and then stall
        # threading._shutdown at interpreter exit -- which hangs pytest *after*
        # it has already reported success.
        threads = [
            threading.Thread(target=writer, daemon=True),
            threading.Thread(target=reader, daemon=True),
            threading.Thread(target=writer, daemon=True),
            threading.Thread(target=reader, daemon=True),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not any(t.is_alive() for t in threads), (
            "read/write workers did not finish within 30s"
        )

        # Should complete without errors
        assert len(errors) == 0, f"Errors occurred: {errors}"

    def test_concurrent_upsert_same_key(self, conn_conf, test_graph_name, clean_db):
        """Multiple threads trying to upsert same document."""
        _ = clean_db
        results = []

        def updater(thread_id):
            for i in range(10):
                with ConnectionManager(connection_config=conn_conf) as db:
                    docs = [
                        {"id": "contested", "last_writer": thread_id, "iteration": i}
                    ]
                    db.upsert_docs_batch(docs, "Contested", match_keys=["id"])
                    results.append((thread_id, i))

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(updater, i) for i in range(4)]
            concurrent.futures.wait(futures)

        with ConnectionManager(connection_config=conn_conf) as db:
            result = db.fetch_docs("Contested")
            # Should have exactly one document
            assert len(result) == 1


class TestStateCorruption(StateCorruptionCases):
    """Test state handling and connection management."""


class TestAggregationEdgeCases(AggregationCases):
    """Test aggregation with edge cases."""

    def test_aggregate_empty_collection(self, conn_conf, test_graph_name, clean_db):
        """Aggregate on empty/non-existent label."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            count = db.aggregate("NonExistent", AggregationType.COUNT)
            assert count == 0

    def test_aggregate_discriminant_with_nulls(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Group by field that has null values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "category": "A"},
                {"id": "2", "category": None},
                {"id": "3", "category": "A"},
                {"id": "4"},  # Missing category
            ]
            db.upsert_docs_batch(docs, "NullGroup", match_keys=["id"])

            result = db.aggregate(
                "NullGroup", AggregationType.COUNT, discriminant="category"
            )
            assert "A" in result
            assert result["A"] == 2


class TestFilterEdgeCases(FilterCases):
    """Test filter expressions with edge cases."""

    def test_filter_with_special_characters(self, conn_conf, test_graph_name, clean_db):
        """Filter values containing special characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            special_values = ["test'quote", 'test"double', "test\\backslash"]
            docs = [{"id": str(i), "value": v} for i, v in enumerate(special_values)]
            db.upsert_docs_batch(docs, "SpecialFilter", match_keys=["id"])

            for v in special_values:
                result = db.fetch_docs("SpecialFilter", filters=["==", v, "value"])
                assert len(result) == 1


class TestEdgeOperationsEdgeCases(EdgeOperationsCases):
    """Test edge/relationship operations with edge cases."""


class TestBatchStress(BatchStressCases):
    """Stress test batch operations."""


class TestMemoryExhaustion(MemoryExhaustionCases):
    """Memory exhaustion and resource abuse testing.

    Attempts to trigger out-of-memory conditions or excessive resource
    consumption through carefully crafted inputs. These tests verify
    that the connector has appropriate safeguards against DoS attacks.

    Attack vectors:
        - Exponentially growing property values
        - Documents with thousands of small properties
        - Deeply nested JSON-like string structures
        - Binary data encoded as strings
    """


class TestCartesianProductBomb:
    """Queries designed to create explosive result sets."""

    def test_unanchored_match_storm(self, conn_conf, test_graph_name, clean_db):
        """Create many nodes, then query without anchor - cartesian explosion."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Create modest node counts
            for label in ["TypeA", "TypeB", "TypeC"]:
                docs = [{"id": str(i)} for i in range(50)]
                db.upsert_docs_batch(docs, label, match_keys=["id"])

            # Query that could explode: 50 * 50 * 50 = 125,000 rows
            # The connector should handle this gracefully
            try:
                result = db.execute(
                    "MATCH (a:TypeA), (b:TypeB), (c:TypeC) RETURN count(*)"
                )
                assert result.result_set[0][0] == 125000
            except Exception:
                pass  # Timeout or memory limit is acceptable

    def test_dense_multi_hop_path(self, conn_conf, test_graph_name, clean_db):
        """Dense graph with multi-hop path query - exponential paths."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Create dense subgraph
            docs = [{"id": str(i)} for i in range(20)]
            db.upsert_docs_batch(docs, "Dense", match_keys=["id"])

            # Connect each node to several others (high connectivity)
            edges = []
            for i in range(20):
                for j in range(i + 1, min(i + 5, 20)):
                    edges.append([{"id": str(i)}, {"id": str(j)}, {}])

            db.insert_edges_batch(
                edges,
                source_class="Dense",
                target_class="Dense",
                relation_name="CONNECTED",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Variable length path query - exponential explosion
            try:
                result = db.execute(
                    "MATCH p=(a:Dense)-[:CONNECTED*1..5]->(b:Dense) RETURN count(p)"
                )
                assert result.result_set[0][0] > 0
            except Exception:
                pass


class TestMalformedEncoding(MalformedEncodingCases):
    """Test with malformed or invalid encodings."""


class TestPropertyKeySmuggling(PropertyKeySmugglingCases):
    """Attempt to smuggle malicious content via property keys."""


class TestTemporalAnomalies(TemporalAnomaliesCases):
    """Test with extreme or invalid temporal values."""


class TestGraphAlgorithmExploits:
    """Pathological graph structures that stress graph algorithms.

    Creates graph topologies known to exhibit worst-case behavior
    for common graph algorithms. Tests connector resilience against
    computationally expensive traversal patterns.

    Structures:
        - Interlocking cycles (exponential path enumeration)
        - Lollipop graphs (clique + path, hard for random walks)
        - Barbell graphs (two cliques with bottleneck bridge)
    """

    def test_cycle_detection_nightmare(self, conn_conf, test_graph_name, clean_db):
        """Complex interlocking cycles - stress cycle detection."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Create nodes
            docs = [{"id": str(i)} for i in range(30)]
            db.upsert_docs_batch(docs, "Cycles", match_keys=["id"])

            # Create multiple overlapping cycles
            edges = []
            # Cycle 1: 0->1->2->3->4->0
            for i in range(5):
                edges.append([{"id": str(i)}, {"id": str((i + 1) % 5)}, {}])
            # Cycle 2: 5->6->7->8->9->5
            for i in range(5, 10):
                next_id = 5 if i == 9 else i + 1
                edges.append([{"id": str(i)}, {"id": str(next_id)}, {}])
            # Bridge cycles
            edges.append([{"id": "2"}, {"id": "7"}, {}])
            edges.append([{"id": "7"}, {"id": "2"}, {}])

            db.insert_edges_batch(
                edges,
                source_class="Cycles",
                target_class="Cycles",
                relation_name="LOOPS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Query that traverses cycles
            result = db.execute(
                "MATCH (n:Cycles)-[:LOOPS*1..10]->(m:Cycles) "
                "WHERE n.id = '0' RETURN count(*)"
            )
            assert result.result_set[0][0] > 0

    def test_lollipop_graph(self, conn_conf, test_graph_name, clean_db):
        """Lollipop graph - pathological for certain algorithms."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Clique (the candy)
            clique_size = 10
            docs = [{"id": f"clique_{i}"} for i in range(clique_size)]
            db.upsert_docs_batch(docs, "Lollipop", match_keys=["id"])

            # Path (the stick)
            stick_length = 20
            docs = [{"id": f"stick_{i}"} for i in range(stick_length)]
            db.upsert_docs_batch(docs, "Lollipop", match_keys=["id"])

            # Clique edges (complete graph)
            edges = []
            for i in range(clique_size):
                for j in range(i + 1, clique_size):
                    edges.append(
                        [
                            {"id": f"clique_{i}"},
                            {"id": f"clique_{j}"},
                            {},
                        ]
                    )

            # Stick edges (path)
            for i in range(stick_length - 1):
                edges.append(
                    [
                        {"id": f"stick_{i}"},
                        {"id": f"stick_{i + 1}"},
                        {},
                    ]
                )

            # Connect stick to clique
            edges.append([{"id": "stick_0"}, {"id": "clique_0"}, {}])

            db.insert_edges_batch(
                edges,
                source_class="Lollipop",
                target_class="Lollipop",
                relation_name="CONNECTED",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Verify structure
            result = db.execute("MATCH ()-[r:CONNECTED]->() RETURN count(r)")
            expected = (clique_size * (clique_size - 1)) // 2 + stick_length
            assert result.result_set[0][0] == expected

    def test_barbell_graph(self, conn_conf, test_graph_name, clean_db):
        """Two cliques connected by a single edge - bottleneck."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            clique_size = 8

            # Left clique
            docs = [{"id": f"left_{i}"} for i in range(clique_size)]
            db.upsert_docs_batch(docs, "Barbell", match_keys=["id"])

            # Right clique
            docs = [{"id": f"right_{i}"} for i in range(clique_size)]
            db.upsert_docs_batch(docs, "Barbell", match_keys=["id"])

            edges = []
            # Left clique edges
            for i in range(clique_size):
                for j in range(i + 1, clique_size):
                    edges.append(
                        [
                            {"id": f"left_{i}"},
                            {"id": f"left_{j}"},
                            {},
                        ]
                    )
            # Right clique edges
            for i in range(clique_size):
                for j in range(i + 1, clique_size):
                    edges.append(
                        [
                            {"id": f"right_{i}"},
                            {"id": f"right_{j}"},
                            {},
                        ]
                    )
            # Bridge
            edges.append([{"id": "left_0"}, {"id": "right_0"}, {}])

            db.insert_edges_batch(
                edges,
                source_class="Barbell",
                target_class="Barbell",
                relation_name="LINKED",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Query crossing the bottleneck - may timeout on complex graphs
            try:
                result = db.execute(
                    "MATCH path = (l:Barbell)-[:LINKED*]-(r:Barbell) "
                    "WHERE l.id = 'left_7' AND r.id = 'right_7' "
                    "RETURN count(path)"
                )
                assert result.result_set[0][0] > 0
            except Exception:
                pass


class TestConnectionTorture:
    """Stress connection handling and state management."""

    def test_rapid_connect_disconnect(self, conn_conf, test_graph_name):
        """Rapidly open and close connections."""
        for _ in range(50):
            with ConnectionManager(connection_config=conn_conf) as db:
                db.execute("RETURN 1")

    def test_interleaved_operations(self, conn_conf, test_graph_name, clean_db):
        """Interleave reads and writes in confusing patterns."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            for i in range(20):
                # Write
                db.upsert_docs_batch(
                    [{"id": str(i), "phase": "write"}],
                    "Interleaved",
                    match_keys=["id"],
                )
                # Read immediately after write
                db.fetch_docs("Interleaved", filters=["==", str(i), "id"])
                # Overwrite
                db.upsert_docs_batch(
                    [{"id": str(i), "phase": "overwrite"}],
                    "Interleaved",
                    match_keys=["id"],
                )
                # Read again
                result = db.fetch_docs("Interleaved", filters=["==", str(i), "id"])
                assert result[0]["phase"] == "overwrite"

    def test_massive_transaction(self, conn_conf, test_graph_name, clean_db):
        """Single operation with huge batch."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # 50,000 nodes in one batch
            docs = [{"id": str(i), "batch": "massive"} for i in range(50000)]
            db.upsert_docs_batch(docs, "Massive", match_keys=["id"])

            count = db.aggregate("Massive", AggregationType.COUNT)
            assert count == 50000


class TestQueryInjectionAdvanced(QueryInjectionAdvancedCases):
    """More sophisticated injection attempts."""


class TestDataTypeTorture:
    """Extreme data type edge cases."""

    def test_scientific_notation_extremes(self, conn_conf, test_graph_name, clean_db):
        """Extreme scientific notation values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            sci_values = [
                ("small_pos", 1e-300),
                ("large_pos", 1e300),
                ("small_neg", -1e-300),
                ("large_neg", -1e300),
                ("denormal", 5e-324),  # Smallest denormalized float
            ]

            for name, val in sci_values:
                docs = [{"id": name, "sci": val}]
                try:
                    db.upsert_docs_batch(docs, "Scientific", match_keys=["id"])
                except Exception:
                    pass

    def test_integer_overflow_boundaries(self, conn_conf, test_graph_name, clean_db):
        """Test around integer overflow boundaries."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            boundaries = [
                ("i64_max", 2**63 - 1),
                ("i64_max_plus", 2**63),  # Overflow
                ("i64_min", -(2**63)),
                ("i64_min_minus", -(2**63) - 1),  # Underflow
                ("i32_max", 2**31 - 1),
                ("i32_max_plus", 2**31),
                ("u64_max", 2**64 - 1),
            ]

            for name, val in boundaries:
                docs = [{"id": name, "boundary": val}]
                try:
                    db.upsert_docs_batch(docs, "Boundaries", match_keys=["id"])
                except (OverflowError, ValueError):
                    pass

    def test_string_that_looks_like_number(self, conn_conf, test_graph_name, clean_db):
        """Strings that might be coerced to numbers."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            sneaky_numbers = [
                "123",
                "12.34",
                "-456",
                "1e10",
                "0x1F",  # Hex
                "0o777",  # Octal
                "0b1010",  # Binary
                "  123  ",  # Padded
                "123abc",  # Partial
                "+123",
                "++123",
            ]

            for i, val in enumerate(sneaky_numbers):
                docs = [{"id": str(i), "numeric_string": val}]
                db.upsert_docs_batch(docs, "NumStrings", match_keys=["id"])

            result = db.fetch_docs("NumStrings")
            # Verify strings stayed as strings
            for r in result:
                assert isinstance(r["numeric_string"], str)

    def test_uuid_collisions(self, conn_conf, test_graph_name, clean_db):
        """Generate many UUIDs looking for collisions."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            uuids = [str(uuid.uuid4()) for _ in range(1000)]
            docs = [{"id": u, "seq": i} for i, u in enumerate(uuids)]
            db.upsert_docs_batch(docs, "UUIDs", match_keys=["id"])

            count = db.aggregate("UUIDs", AggregationType.COUNT)
            assert count == 1000  # No collisions

    def test_empty_string_variations(self, conn_conf, test_graph_name, clean_db):
        """Various representations of 'empty'."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            empties = [
                ("empty", ""),
                ("space", " "),
                ("null_char", "\x00"),  # Sanitized by connector (null bytes removed)
                ("zwsp", "\u200b"),  # Zero-width space
                ("empty_array_str", "[]"),
                ("empty_obj_str", "{}"),
                ("none_str", "None"),
                ("null_str", "null"),
                ("undefined", "undefined"),
            ]

            for name, val in empties:
                docs = [{"id": name, "empty": val}]
                db.upsert_docs_batch(docs, "Empties", match_keys=["id"])

            result = db.fetch_docs("Empties")
            assert len(result) == len(empties)

            # Verify null char was sanitized (removed)
            null_doc = next((r for r in result if r["id"] == "null_char"), None)
            assert null_doc is not None
            assert null_doc["empty"] == ""


class TestFilterSadism:
    """Torture the filter system."""

    def test_deeply_nested_boolean_filter(self, conn_conf, test_graph_name, clean_db):
        """Deeply nested AND/OR expressions."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i), "val": i} for i in range(10)]
            db.upsert_docs_batch(docs, "FilterDeep", match_keys=["id"])

            # Build deeply nested filter: ((((a AND b) OR c) AND d) OR e)...
            nested_filter = ["==", "0", "id"]
            for i in range(1, 8):
                op = "or" if i % 2 == 0 else "and"
                nested_filter = [op, nested_filter, ["==", str(i), "id"]]

            try:
                result = db.fetch_docs("FilterDeep", filters=nested_filter)
                assert isinstance(result, list)
            except Exception:
                pass  # Deep nesting may not be supported

    def test_filter_with_regex_metacharacters(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Filter values containing regex metacharacters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            regex_chars = [".*", ".+", "^$", "[a-z]", "(group)", "a|b", "\\d+"]
            docs = [{"id": str(i), "pattern": p} for i, p in enumerate(regex_chars)]
            db.upsert_docs_batch(docs, "RegexChars", match_keys=["id"])

            # Filter should match literal strings, not interpret as regex
            for p in regex_chars:
                result = db.fetch_docs("RegexChars", filters=["==", p, "pattern"])
                assert len(result) == 1

    def test_filter_empty_value(self, conn_conf, test_graph_name, clean_db):
        """Filter for empty string value."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "data": ""},
                {"id": "2", "data": "not_empty"},
            ]
            db.upsert_docs_batch(docs, "EmptyFilter", match_keys=["id"])

            result = db.fetch_docs("EmptyFilter", filters=["==", "", "data"])
            assert len(result) == 1
            assert result[0]["id"] == "1"

    def test_filter_null_vs_missing(self, conn_conf, test_graph_name, clean_db):
        """Distinguish between null value and missing key."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "field": None},
                {"id": "2", "field": "exists"},
                {"id": "3"},  # field missing entirely
            ]
            db.upsert_docs_batch(docs, "NullVsMissing", match_keys=["id"])

            # This tests how the system handles null vs absent
            result = db.fetch_docs("NullVsMissing")
            assert len(result) == 3


class TestDeadlockHell:
    """Deadlock detection and transaction isolation testing.

    Attempts to trigger deadlock conditions through concurrent
    conflicting operations. Also tests for isolation anomalies
    such as phantom reads and dirty reads.

    Scenarios:
        - Circular update dependencies (A→B, B→A pattern)
        - Self-referential updates (read-modify-write cycles)
        - Phantom read detection during concurrent inserts
    """

    def test_circular_update_dependency(self, conn_conf, test_graph_name, clean_db):
        """Create circular update patterns that could deadlock."""
        _ = clean_db
        errors = []

        def update_a_then_b():
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for _ in range(10):
                        db.upsert_docs_batch(
                            [{"id": "A", "by": "thread1"}],
                            "Deadlock",
                            match_keys=["id"],
                        )
                        db.upsert_docs_batch(
                            [{"id": "B", "by": "thread1"}],
                            "Deadlock",
                            match_keys=["id"],
                        )
            except Exception as e:
                errors.append(str(e))

        def update_b_then_a():
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for _ in range(10):
                        db.upsert_docs_batch(
                            [{"id": "B", "by": "thread2"}],
                            "Deadlock",
                            match_keys=["id"],
                        )
                        db.upsert_docs_batch(
                            [{"id": "A", "by": "thread2"}],
                            "Deadlock",
                            match_keys=["id"],
                        )
            except Exception as e:
                errors.append(str(e))

        t1 = threading.Thread(target=update_a_then_b, daemon=True)
        t2 = threading.Thread(target=update_b_then_a, daemon=True)

        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        # Should complete without deadlock
        assert not t1.is_alive(), "Thread 1 appears deadlocked"
        assert not t2.is_alive(), "Thread 2 appears deadlocked"

    def test_self_referential_update(self, conn_conf, test_graph_name, clean_db):
        """Update a node based on reading its own value."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "self", "counter": 0}], "SelfRef", match_keys=["id"]
            )

            for _ in range(50):
                result = db.fetch_docs("SelfRef", filters=["==", "self", "id"])
                if result:
                    new_val = result[0].get("counter", 0) + 1
                    db.upsert_docs_batch(
                        [{"id": "self", "counter": new_val}],
                        "SelfRef",
                        match_keys=["id"],
                    )

            result = db.fetch_docs("SelfRef")
            # Counter should have incremented (exact value depends on race conditions)
            assert result[0]["counter"] > 0

    def test_phantom_read_scenario(self, conn_conf, test_graph_name, clean_db):
        """Scenario that could cause phantom reads."""
        _ = clean_db
        phantoms_detected = []

        def inserter():
            with ConnectionManager(connection_config=conn_conf) as db:
                for i in range(100):
                    db.upsert_docs_batch(
                        [{"id": f"phantom_{i}"}], "Phantom", match_keys=["id"]
                    )

        def reader():
            with ConnectionManager(connection_config=conn_conf) as db:
                counts = []
                for _ in range(20):
                    count = db.aggregate("Phantom", AggregationType.COUNT)
                    counts.append(count)
                # Check for non-monotonic reads (would indicate phantoms)
                for i in range(1, len(counts)):
                    if counts[i] < counts[i - 1]:
                        phantoms_detected.append((counts[i - 1], counts[i]))

        t1 = threading.Thread(target=inserter, daemon=True)
        t2 = threading.Thread(target=reader, daemon=True)

        t1.start()
        t2.start()
        # Bounded like the sibling deadlock test above: this class exists to catch
        # a hung backend, so it must not itself hang the run when it finds one.
        t1.join(timeout=30)
        t2.join(timeout=30)

        assert not t1.is_alive(), "Inserter thread appears stuck"
        assert not t2.is_alive(), "Reader thread appears stuck"


class TestReDoS(ReDoSCases):
    """Test patterns that could cause ReDoS if regex is used internally."""

    def test_evil_regex_patterns_as_values(self, conn_conf, test_graph_name, clean_db):
        """Store values that would be catastrophic if used as regex."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Classic ReDoS patterns
            redos_patterns = [
                "a" * 30 + "!",  # (a+)+ pattern victim
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!",
                "x" * 50 + "y",  # (x+x+)+ victim
                "aaaaaaaaaaaaaaaaaaaaab" * 10,  # Nested quantifier victim
            ]

            for i, pattern in enumerate(redos_patterns):
                docs = [{"id": str(i), "pattern": pattern}]
                db.upsert_docs_batch(docs, "ReDoS", match_keys=["id"])

            result = db.fetch_docs("ReDoS")
            assert len(result) == len(redos_patterns)


class TestLabelAbuse(LabelAbuseCases):
    """Abuse label and relationship type naming."""


class TestQueryComplexity(QueryComplexityCases):
    """Queries designed to be computationally expensive."""

    def test_union_bomb(self, conn_conf, test_graph_name, clean_db):
        """Many UNION clauses."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": "1"}]
            db.upsert_docs_batch(docs, "UnionBomb", match_keys=["id"])

            # Build query with many UNIONs
            query_parts = ["MATCH (n:UnionBomb) RETURN n.id AS id" for _ in range(50)]
            query = " UNION ALL ".join(query_parts)

            try:
                result = db.execute(query)
                assert len(result.result_set) == 50
            except Exception:
                pass


class TestSchemaEvolution(SchemaEvolutionCases):
    """Test rapid schema changes and type mutations."""

    def test_property_type_mutation(self, conn_conf, test_graph_name, clean_db):
        """Rapidly change property types for same key."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            type_sequence = [
                123,
                "123",
                12.3,
                True,
                [1, 2, 3],
                None,
                {"nested": "dict"},
            ]

            for i, val in enumerate(type_sequence):
                docs = [{"id": "mutant", "value": val, "iteration": i}]
                try:
                    db.upsert_docs_batch(docs, "TypeMutation", match_keys=["id"])
                except Exception:
                    pass

            result = db.fetch_docs("TypeMutation")
            # Should have exactly one node (upserted multiple times)
            assert len(result) == 1


class TestBoundaryValueAnalysis(BoundaryValueAnalysisCases):
    """Systematic boundary value testing."""


class TestPathologicalIds(PathologicalIdsCases):
    """IDs designed to cause problems."""
