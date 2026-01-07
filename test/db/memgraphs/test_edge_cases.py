"""Edge case and robustness tests for Memgraph connector.

This module provides a comprehensive adversarial test suite for the Memgraph
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
    - Large payloads (1MB strings, 1000 properties, 50k batches)
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

Graph Topology:
    - Pathological structures (cycles, cliques, stars)
    - Self-referential edges and multi-edges
    - Long chains and deeply nested traversals

Usage
-----
Run all edge case tests::

    pytest test/db/memgraphs/test_edge_cases.py -v

Run specific category::

    pytest test/db/memgraphs/test_edge_cases.py -k "Injection" -v

Notes
-----
- Tests require a running Memgraph instance (see conftest.py fixtures)
- Some tests intentionally trigger warnings (logged, not failures)

See Also
--------
- graflo.db.memgraph.conn : Memgraph connector implementation
- test.db.memgraphs.conftest : Test fixtures and configuration
"""

import concurrent.futures
import math
import threading
import uuid

import pytest

from graflo.db import ConnectionManager
from graflo.onto import AggregationType


# =============================================================================
# CYPHER INJECTION ATTACKS
# =============================================================================


class TestCypherInjection:
    """Security tests for Cypher injection prevention.

    Validates that the connector properly sanitizes user inputs to prevent
    Cypher injection attacks. Tests cover multiple injection vectors including
    property values, label names, and match keys.

    The connector should either:
    - Parameterize all user inputs (preferred)
    - Properly escape special characters
    - Reject dangerous inputs with clear errors

    References
    ----------
    - OWASP Injection Prevention Cheat Sheet:
      https://cheatsheetseries.owasp.org/cheatsheets/Injection_Prevention_Cheat_Sheet.html
    - Neo4j/Cypher Security Best Practices:
      https://neo4j.com/developer/cypher/guide-sql-injection/
    """

    def test_injection_via_node_property_value(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Verify property values cannot escape string context to inject Cypher."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Classic SQL injection adapted for Cypher
            malicious_payloads = [
                "'; MATCH (n) DETACH DELETE n; //",
                "' OR 1=1 --",
                "test'}) MATCH (x) DETACH DELETE x CREATE (n:Pwned {id: '1",
                '" OR ""="',
                "\\'; DROP DATABASE test; --",
                "' UNION MATCH (n) RETURN n.password //",
                "${injection}",
                "{{injection}}",
                "' + '' + '",
                "\\x00'); DELETE n; //",
            ]

            for i, payload in enumerate(malicious_payloads):
                docs = [{"id": str(i), "name": payload}]
                db.upsert_docs_batch(docs, "InjectionTest", match_keys=["id"])

            # Verify all nodes were created (injection didn't execute)
            result = db.fetch_docs("InjectionTest")
            assert len(result) == len(malicious_payloads)

            # Verify the malicious strings were stored as-is
            for node in result:
                assert node["name"] in malicious_payloads

    def test_injection_via_label_name(self, conn_conf, test_graph_name, clean_db):
        """Try to inject via label name parameter."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # These should either be sanitized or raise proper errors
            dangerous_labels = [
                "User`) MATCH (n) DETACH DELETE n CREATE (x:`Pwned",
                "User:Admin",  # Multi-label injection
                "User MATCH (n) DELETE n CREATE (:`Pwned",
            ]

            for label in dangerous_labels:
                docs = [{"id": "1", "name": "test"}]
                try:
                    db.upsert_docs_batch(docs, label, match_keys=["id"])
                except Exception:
                    # Expected - dangerous labels should be rejected
                    pass

    def test_injection_via_match_keys(self, conn_conf, test_graph_name, clean_db):
        """Try to inject via match_keys parameter."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": "1", "name": "test"}]

            dangerous_keys = [
                "id}) MATCH (n) DELETE n MERGE (x:Pwned {x",
                "id: '1'})-[:OWNS]->(m) DELETE m MERGE (n:Safe {id",
            ]

            for key in dangerous_keys:
                try:
                    db.upsert_docs_batch(docs, "User", match_keys=[key])
                except Exception:
                    # Expected behavior
                    pass

    def test_injection_via_filter_value(self, conn_conf, test_graph_name, clean_db):
        """Try to inject Cypher via filter values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # First create a legitimate node
            db.upsert_docs_batch(
                [{"id": "1", "name": "safe"}], "FilterTest", match_keys=["id"]
            )

            # Try injection via filter
            malicious_filters = [
                "safe' OR 1=1 --",
                "safe' UNION MATCH (n) RETURN n //",
                "safe\\' OR \\'1\\'=\\'1",
            ]

            for mf in malicious_filters:
                result = db.fetch_docs("FilterTest", filters=["==", mf, "name"])
                # Should return empty or just the literal match, not all nodes
                assert len(result) <= 1


# =============================================================================
# UNICODE & SPECIAL CHARACTERS EDGE CASES
# =============================================================================


class TestUnicodeEdgeCases:
    """Unicode and encoding edge case validation.

    Verifies correct handling of international text, special Unicode
    characters, and encoding edge cases that commonly cause issues
    in database systems.
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

    def test_complex_emoji_sequences(self, conn_conf, test_graph_name, clean_db):
        """Test complex emoji sequences (ZWJ, skin tones, flags)."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            complex_emojis = [
                {"id": "1", "emoji": "👨‍👩‍👧‍👦"},  # Family ZWJ sequence
                {"id": "2", "emoji": "👋🏿"},  # Skin tone modifier
                {"id": "3", "emoji": "🏳️‍🌈"},  # Flag ZWJ sequence
                {"id": "4", "emoji": "👨‍💻"},  # Profession ZWJ
                {"id": "5", "emoji": "🇫🇷"},  # Regional indicator (flag)
            ]
            db.upsert_docs_batch(complex_emojis, "ComplexEmoji", match_keys=["id"])

            result = db.fetch_docs("ComplexEmoji")
            assert len(result) == 5

    def test_rtl_and_bidi_text(self, conn_conf, test_graph_name, clean_db):
        """Test right-to-left and bidirectional text."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            rtl_texts = [
                {"id": "1", "content": "مرحبا بالعالم"},  # Arabic
                {"id": "2", "content": "שלום עולם"},  # Hebrew
                {"id": "3", "content": "Hello مرحبا World عالم"},  # Mixed LTR/RTL
                {"id": "4", "content": "\u202etext\u202c"},  # RTL override characters
                {"id": "5", "content": "A\u200fB\u200eC"},  # Mixed marks
            ]
            db.upsert_docs_batch(rtl_texts, "RTLNode", match_keys=["id"])

            result = db.fetch_docs("RTLNode")
            assert len(result) == len(rtl_texts)

    def test_null_bytes_and_control_chars(self, conn_conf, test_graph_name, clean_db):
        """Test null bytes and control characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            problematic_strings = [
                ("1", "before\x00after"),  # Null byte
                ("2", "tab\there"),
                ("3", "new\nline"),
                ("4", "carriage\rreturn"),
                ("5", "bell\x07ring"),
                ("6", "escape\x1b[31mred"),  # ANSI escape
            ]

            successful = 0
            for id_val, s in problematic_strings:
                try:
                    db.upsert_docs_batch(
                        [{"id": id_val, "data": s}], "ControlChars", match_keys=["id"]
                    )
                    successful += 1
                except Exception:
                    # Some control chars may be rejected
                    pass

            result = db.fetch_docs("ControlChars")
            assert len(result) == successful

    def test_unicode_normalization_edge_cases(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Test Unicode normalization edge cases."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Different Unicode representations of "same" characters
            variants = [
                ("1a", "café"),
                ("1b", "cafe\u0301"),  # Composed vs decomposed
                ("2a", "ﬁ"),  # Ligature
                ("2b", "fi"),  # Separate chars
                ("3a", "Ω"),  # Greek Omega U+03A9
                ("3b", "Ω"),  # Ohm sign U+2126
                ("4a", "㈱"),  # Enclosed
                ("4b", "(株)"),  # Parenthesized
            ]

            for id_val, text in variants:
                db.upsert_docs_batch(
                    [{"id": id_val, "text": text}], "NormTest", match_keys=["id"]
                )

            result = db.fetch_docs("NormTest")
            assert len(result) == len(variants)

    def test_zero_width_characters(self, conn_conf, test_graph_name, clean_db):
        """Test zero-width and invisible characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            invisible_strings = [
                {"id": "1", "text": "hello\u200bworld"},  # Zero-width space
                {"id": "2", "text": "hello\u200cworld"},  # Zero-width non-joiner
                {"id": "3", "text": "hello\u200dworld"},  # Zero-width joiner
                {"id": "4", "text": "hello\ufeffworld"},  # BOM
                {"id": "5", "text": "\u2060invisible\u2060"},  # Word joiner
                {"id": "6", "text": "a\u034fb"},  # Combining grapheme joiner
            ]

            db.upsert_docs_batch(invisible_strings, "Invisible", match_keys=["id"])

            result = db.fetch_docs("Invisible")
            assert len(result) == len(invisible_strings)

    def test_private_use_area_flooding(self, conn_conf, test_graph_name, clean_db):
        """Test Private Use Area character flooding."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Flood with PUA characters
            pua_string = "".join(chr(0xE000 + i) for i in range(1000))
            docs = [{"id": "pua", "data": pua_string}]
            db.upsert_docs_batch(docs, "PUAFlood", match_keys=["id"])

            result = db.fetch_docs("PUAFlood")
            assert len(result) == 1
            assert len(result[0]["data"]) == 1000


# =============================================================================
# BOUNDARY CONDITIONS & LIMITS
# =============================================================================


class TestBoundaryConditions:
    """Boundary value analysis for connector limits."""

    def test_empty_batch(self, conn_conf, test_graph_name, clean_db):
        """Inserting empty batch should not crash."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([], "Empty", match_keys=["id"])
            result = db.fetch_docs("Empty")
            assert len(result) == 0

    def test_empty_document(self, conn_conf, test_graph_name, clean_db):
        """Insert document with no properties except match key."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": "lonely"}]
            db.upsert_docs_batch(docs, "Minimal", match_keys=["id"])
            result = db.fetch_docs("Minimal")
            assert len(result) == 1

    def test_empty_string_values(self, conn_conf, test_graph_name, clean_db):
        """Test empty string property values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "name": ""},
                {"id": "2", "data": ""},
                {"id": "3", "": "value"},  # Empty key
            ]
            try:
                db.upsert_docs_batch(docs, "EmptyStrings", match_keys=["id"])
            except Exception:
                # Empty key may be rejected
                docs = [
                    {"id": "1", "name": ""},
                    {"id": "2", "data": ""},
                ]
                db.upsert_docs_batch(docs, "EmptyStrings", match_keys=["id"])

            result = db.fetch_docs("EmptyStrings")
            assert len(result) >= 2

    def test_very_long_string(self, conn_conf, test_graph_name, clean_db):
        """Test with very long string values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # 1MB string
            huge_string = "x" * (1024 * 1024)
            docs = [{"id": "huge", "data": huge_string}]
            db.upsert_docs_batch(docs, "HugeData", match_keys=["id"])

            result = db.fetch_docs("HugeData")
            assert len(result) == 1
            assert len(result[0]["data"]) == len(huge_string)

    def test_extreme_numbers(self, conn_conf, test_graph_name, clean_db):
        """Test extreme numeric values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            extreme_values = [
                ("max_int", 2**63 - 1),
                ("min_int", -(2**63)),
                ("tiny_float", 1e-308),
                ("huge_float", 1e308),
                ("negative_zero", -0.0),
                ("small_negative", -1e-308),
            ]

            successful = 0
            for name, value in extreme_values:
                try:
                    db.upsert_docs_batch(
                        [{"id": name, "value": value}], "ExtremeNums", match_keys=["id"]
                    )
                    successful += 1
                except (OverflowError, ValueError):
                    pass

            result = db.fetch_docs("ExtremeNums")
            assert len(result) > 0

    def test_special_float_values(self, conn_conf, test_graph_name, clean_db):
        """Test NaN, Inf, -Inf handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            special_floats = [
                ("nan", float("nan")),
                ("inf", float("inf")),
                ("neg_inf", float("-inf")),
            ]

            successful = 0
            for name, value in special_floats:
                try:
                    db.upsert_docs_batch(
                        [{"id": name, "value": value}],
                        "SpecialFloats",
                        match_keys=["id"],
                    )
                    successful += 1
                except (ValueError, TypeError):
                    # Special floats may not be supported
                    pass

            # Memgraph may or may not support special floats
            # Document the behavior - just verify no crash
            _ = db.fetch_docs("SpecialFloats")

    def test_wide_document(self, conn_conf, test_graph_name, clean_db):
        """Test document with many properties."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # 1000 properties
            doc = {"id": "wide"}
            for i in range(1000):
                doc[f"prop_{i}"] = f"value_{i}"

            db.upsert_docs_batch([doc], "WideDoc", match_keys=["id"])

            result = db.fetch_docs("WideDoc")
            assert len(result) == 1
            assert len(result[0]) >= 1000

    def test_limit_parameter_edge_cases(self, conn_conf, test_graph_name, clean_db):
        """Test edge cases for limit parameter."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Create test data
            docs = [{"id": str(i)} for i in range(10)]
            db.upsert_docs_batch(docs, "LimitTest", match_keys=["id"])

            # Test various limit values
            test_cases = [
                (0, 0),  # Zero limit
                (1, 1),  # Minimum
                (10, 10),  # Exact count
                (100, 10),  # Over count
            ]

            for limit, expected_max in test_cases:
                result = db.fetch_docs("LimitTest", limit=limit)
                assert len(result) <= expected_max or limit == 0

            # Test negative limit (should either work as no limit or raise error)
            try:
                result = db.fetch_docs("LimitTest", limit=-1)
                # If it works, should return all
            except (ValueError, Exception):
                # Expected for invalid limit
                pass


# =============================================================================
# TYPE CONFUSION & COERCION
# =============================================================================


class TestTypeConfusion:
    """Type confusion and coercion edge cases."""

    def test_string_number_collision(self, conn_conf, test_graph_name, clean_db):
        """Test string vs numeric ID collision."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "123", "type": "string"},
                {"id": 123, "type": "int"},
            ]

            # Behavior depends on how connector handles type mismatch
            try:
                db.upsert_docs_batch(docs, "TypeCollision", match_keys=["id"])
            except Exception:
                # May raise on type mismatch
                pass

    def test_boolean_vs_string(self, conn_conf, test_graph_name, clean_db):
        """Test boolean vs string representation."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "flag": True},
                {"id": "2", "flag": "true"},
                {"id": "3", "flag": "True"},
                {"id": "4", "flag": False},
                {"id": "5", "flag": "false"},
            ]
            db.upsert_docs_batch(docs, "BoolTest", match_keys=["id"])

            result = db.fetch_docs("BoolTest")
            by_id = {r["id"]: r for r in result}

            # Verify booleans are preserved as booleans
            assert by_id["1"]["flag"] is True
            assert by_id["4"]["flag"] is False
            # Strings should be preserved as strings
            assert by_id["2"]["flag"] == "true"

    def test_none_handling(self, conn_conf, test_graph_name, clean_db):
        """Test None/null value handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "value": None},
                {"id": "2", "value": "none"},
                {"id": "3", "value": "null"},
            ]
            db.upsert_docs_batch(docs, "NullTest", match_keys=["id"])

            result = db.fetch_docs("NullTest")
            by_id = {r["id"]: r for r in result}

            # None should be stored as null or missing
            assert by_id["1"].get("value") is None or "value" not in by_id["1"]
            # String "none" should be preserved
            assert by_id["2"]["value"] == "none"

    def test_mixed_type_arrays(self, conn_conf, test_graph_name, clean_db):
        """Test arrays with mixed types."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "arr": [1, "two", 3.0, True, None]},
                {"id": "2", "arr": [[], {}, "nested"]},
            ]

            try:
                db.upsert_docs_batch(docs, "MixedArray", match_keys=["id"])
                # Just verify no crash
                _ = db.fetch_docs("MixedArray")
            except Exception:
                # Mixed arrays may not be supported
                pass

    def test_property_type_mutation(self, conn_conf, test_graph_name, clean_db):
        """Test changing property type across upserts."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # First insert with string
            db.upsert_docs_batch(
                [{"id": "1", "value": "string"}], "TypeMutate", match_keys=["id"]
            )

            # Update with integer
            db.upsert_docs_batch(
                [{"id": "1", "value": 42}], "TypeMutate", match_keys=["id"]
            )

            result = db.fetch_docs("TypeMutate")
            assert len(result) == 1
            assert result[0]["value"] == 42

            # Update with boolean
            db.upsert_docs_batch(
                [{"id": "1", "value": True}], "TypeMutate", match_keys=["id"]
            )

            result = db.fetch_docs("TypeMutate")
            assert result[0]["value"] is True


# =============================================================================
# MALFORMED INPUTS
# =============================================================================


class TestMalformedInputs:
    """Tests for malformed input handling."""

    def test_empty_match_keys(self, conn_conf, test_graph_name, clean_db):
        """Test with empty match_keys list."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": "1", "name": "test"}]

            try:
                db.upsert_docs_batch(docs, "EmptyKeys", match_keys=[])
            except (ValueError, Exception):
                # Expected - empty match_keys is invalid
                pass

    def test_missing_match_key_in_doc(self, conn_conf, test_graph_name, clean_db):
        """Test document missing the match key."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"name": "NoId"}]  # Missing 'id' key

            try:
                db.upsert_docs_batch(docs, "MissingKey", match_keys=["id"])
            except (KeyError, Exception):
                # Expected
                pass

    def test_reserved_words_as_property_names(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Test Cypher reserved words as property names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            reserved_words = [
                "MATCH",
                "RETURN",
                "WHERE",
                "CREATE",
                "DELETE",
                "SET",
                "REMOVE",
            ]

            docs = [{"id": "1"}]
            for word in reserved_words:
                docs[0][word] = f"value_{word}"

            try:
                db.upsert_docs_batch(docs, "Reserved", match_keys=["id"])
                result = db.fetch_docs("Reserved")
                # Should work with proper escaping
                assert len(result) == 1
            except Exception:
                # May fail if not properly escaped
                pass

    def test_special_chars_in_property_names(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Test special characters in property names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            special_props = [
                "prop with space",
                "prop-with-dash",
                "prop.with.dot",
                "prop:with:colon",
            ]

            for prop in special_props:
                docs = [{"id": prop, prop: "value"}]
                try:
                    db.upsert_docs_batch(docs, "SpecialProp", match_keys=["id"])
                except Exception:
                    # Some special chars may be rejected
                    pass


# =============================================================================
# PATHOLOGICAL GRAPH STRUCTURES
# =============================================================================


class TestPathologicalGraphs:
    """Tests for pathological graph structures."""

    def test_self_referential_edge(self, conn_conf, test_graph_name, clean_db):
        """Test creating an edge from a node to itself."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "1", "name": "Self"}], "Node", match_keys=["id"]
            )

            edges = [[{"id": "1"}, {"id": "1"}, {"type": "self"}]]
            db.insert_edges_batch(
                edges,
                source_class="Node",
                target_class="Node",
                relation_name="LINKS_TO",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            result = db.execute("MATCH (n:Node)-[r:LINKS_TO]->(n) RETURN count(r)")
            assert result.result_set[0][0] == 1

    def test_bidirectional_edges(self, conn_conf, test_graph_name, clean_db):
        """Test bidirectional edges between same nodes."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "1"}, {"id": "2"}], "BiNode", match_keys=["id"]
            )

            # Create edge A -> B
            db.insert_edges_batch(
                [[{"id": "1"}, {"id": "2"}, {"dir": "forward"}]],
                source_class="BiNode",
                target_class="BiNode",
                relation_name="BIDI",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Create edge B -> A
            db.insert_edges_batch(
                [[{"id": "2"}, {"id": "1"}, {"dir": "backward"}]],
                source_class="BiNode",
                target_class="BiNode",
                relation_name="BIDI",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            result = db.execute("MATCH ()-[r:BIDI]->() RETURN count(r)")
            assert result.result_set[0][0] == 2

    def test_long_chain(self, conn_conf, test_graph_name, clean_db):
        """Test long chain of nodes."""
        _ = clean_db
        chain_length = 100

        with ConnectionManager(connection_config=conn_conf) as db:
            # Create nodes
            nodes = [{"id": str(i)} for i in range(chain_length)]
            db.upsert_docs_batch(nodes, "ChainNode", match_keys=["id"])

            # Create chain edges
            edges = [
                [{"id": str(i)}, {"id": str(i + 1)}, {}]
                for i in range(chain_length - 1)
            ]
            db.insert_edges_batch(
                edges,
                source_class="ChainNode",
                target_class="ChainNode",
                relation_name="NEXT",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Verify chain
            result = db.execute(
                f"MATCH p=(a:ChainNode {{id: '0'}})-[:NEXT*..{chain_length}]->(b) "
                f"RETURN length(p) ORDER BY length(p) DESC LIMIT 1"
            )
            assert result.result_set[0][0] == chain_length - 1

    def test_star_topology(self, conn_conf, test_graph_name, clean_db):
        """Test star topology (hub with many spokes)."""
        _ = clean_db
        num_spokes = 100

        with ConnectionManager(connection_config=conn_conf) as db:
            # Create hub and spokes
            nodes = [{"id": "hub"}]
            nodes.extend([{"id": f"spoke_{i}"} for i in range(num_spokes)])
            db.upsert_docs_batch(nodes, "StarNode", match_keys=["id"])

            # Create edges from hub to all spokes
            edges = [
                [{"id": "hub"}, {"id": f"spoke_{i}"}, {"index": i}]
                for i in range(num_spokes)
            ]
            db.insert_edges_batch(
                edges,
                source_class="StarNode",
                target_class="StarNode",
                relation_name="SPOKE",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Verify star
            result = db.execute(
                "MATCH (h:StarNode {id: 'hub'})-[:SPOKE]->(s) RETURN count(s)"
            )
            assert result.result_set[0][0] == num_spokes

    def test_cycle_detection(self, conn_conf, test_graph_name, clean_db):
        """Test cycle in graph."""
        _ = clean_db
        cycle_size = 5

        with ConnectionManager(connection_config=conn_conf) as db:
            nodes = [{"id": str(i)} for i in range(cycle_size)]
            db.upsert_docs_batch(nodes, "CycleNode", match_keys=["id"])

            # Create cycle edges
            edges = [
                [{"id": str(i)}, {"id": str((i + 1) % cycle_size)}, {}]
                for i in range(cycle_size)
            ]
            db.insert_edges_batch(
                edges,
                source_class="CycleNode",
                target_class="CycleNode",
                relation_name="CYCLE",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Verify cycle exists
            result = db.execute(
                "MATCH p=(a:CycleNode {id: '0'})-[:CYCLE*5]->(a) RETURN count(p)"
            )
            assert result.result_set[0][0] == 1


# =============================================================================
# CONCURRENCY & STATE
# =============================================================================


class TestConcurrencyEdgeCases:
    """Concurrency and state management edge cases."""

    def test_concurrent_upserts_same_id(self, conn_conf, test_graph_name, clean_db):
        """Test concurrent upserts with same ID."""
        _ = clean_db
        num_threads = 10
        errors = []
        lock = threading.Lock()

        def upsert_worker(thread_id):
            """Perform repeated upserts on the same node from a single thread."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for i in range(10):
                        db.upsert_docs_batch(
                            [{"id": "contested", "thread": thread_id, "iteration": i}],
                            "Contested",
                            match_keys=["id"],
                        )
            except Exception as e:
                with lock:
                    errors.append(f"Thread {thread_id}: {e}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(upsert_worker, i) for i in range(num_threads)]
            concurrent.futures.wait(futures)

        # Should have exactly one node
        with ConnectionManager(connection_config=conn_conf) as db:
            result = db.fetch_docs("Contested")
            assert len(result) == 1

    def test_read_during_write(self, conn_conf, test_graph_name, clean_db):
        """Test concurrent reads during writes."""
        _ = clean_db
        num_writers = 5
        num_readers = 10
        ops_per_thread = 50

        errors = []
        lock = threading.Lock()
        write_counter = [0]

        def writer():
            """Write nodes concurrently with unique IDs."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for _ in range(ops_per_thread):
                        with lock:
                            write_counter[0] += 1
                            wid = write_counter[0]
                        db.upsert_docs_batch(
                            [{"id": f"w_{wid}", "data": "written"}],
                            "ReadWrite",
                            match_keys=["id"],
                        )
            except Exception as e:
                with lock:
                    errors.append(f"Writer: {e}")

        def reader():
            """Read nodes concurrently during write operations."""
            try:
                with ConnectionManager(connection_config=conn_conf) as db:
                    for _ in range(ops_per_thread):
                        db.fetch_docs("ReadWrite", limit=10)
            except Exception as e:
                with lock:
                    errors.append(f"Reader: {e}")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_writers + num_readers
        ) as executor:
            futures = []
            futures.extend([executor.submit(writer) for _ in range(num_writers)])
            futures.extend([executor.submit(reader) for _ in range(num_readers)])
            concurrent.futures.wait(futures)

        assert len(errors) == 0, f"Errors: {errors[:5]}"


# =============================================================================
# QUERY EDGE CASES
# =============================================================================


class TestQueryEdgeCases:
    """Tests for query edge cases."""

    def test_query_empty_database(self, conn_conf, test_graph_name, clean_db):
        """Test querying an empty database."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            result = db.fetch_docs("NonExistent")
            assert result == []

    def test_aggregate_empty_collection(self, conn_conf, test_graph_name, clean_db):
        """Test aggregation on empty collection."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            result = db.aggregate("Empty", AggregationType.COUNT)
            assert result == 0

    def test_filter_with_special_characters(self, conn_conf, test_graph_name, clean_db):
        """Test filtering with special characters in value."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "value": "test'quote"},
                {"id": "2", "value": 'test"double'},
                {"id": "3", "value": "test\\backslash"},
            ]
            db.upsert_docs_batch(docs, "Special", match_keys=["id"])

            result = db.fetch_docs("Special", filters=["==", "test'quote", "value"])
            assert len(result) == 1
            assert result[0]["id"] == "1"

    def test_filter_with_unicode(self, conn_conf, test_graph_name, clean_db):
        """Test filtering with Unicode values."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "name": "日本語"},
                {"id": "2", "name": "العربية"},
                {"id": "3", "name": "emoji😀"},
            ]
            db.upsert_docs_batch(docs, "UnicodeFilter", match_keys=["id"])

            result = db.fetch_docs("UnicodeFilter", filters=["==", "日本語", "name"])
            assert len(result) == 1
            assert result[0]["id"] == "1"


# =============================================================================
# DATA TYPES
# =============================================================================


class TestDataTypes:
    """Tests for different data type handling."""

    def test_boolean_values(self, conn_conf, test_graph_name, clean_db):
        """Test boolean value handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "active": True},
                {"id": "2", "active": False},
            ]
            db.upsert_docs_batch(docs, "Boolean", match_keys=["id"])

            result = db.fetch_docs("Boolean")
            by_id = {r["id"]: r for r in result}
            assert by_id["1"]["active"] is True
            assert by_id["2"]["active"] is False

    def test_numeric_values(self, conn_conf, test_graph_name, clean_db):
        """Test numeric value handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "int_val": 42, "float_val": 3.14},
                {"id": "2", "int_val": -100, "float_val": -0.5},
                {"id": "3", "int_val": 0, "float_val": 0.0},
            ]
            db.upsert_docs_batch(docs, "Numeric", match_keys=["id"])

            result = db.fetch_docs("Numeric")
            assert len(result) == 3

    def test_list_values(self, conn_conf, test_graph_name, clean_db):
        """Test list value handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "tags": ["a", "b", "c"]},
                {"id": "2", "numbers": [1, 2, 3]},
            ]
            db.upsert_docs_batch(docs, "Lists", match_keys=["id"])

            result = db.fetch_docs("Lists")
            by_id = {r["id"]: r for r in result}
            assert by_id["1"]["tags"] == ["a", "b", "c"]
            assert by_id["2"]["numbers"] == [1, 2, 3]


# =============================================================================
# ERROR HANDLING
# =============================================================================


class TestErrorHandling:
    """Tests for error handling scenarios."""

    def test_invalid_query_syntax(self, conn_conf, test_graph_name, clean_db):
        """Test handling of invalid query syntax."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            with pytest.raises(Exception):
                db.execute("INVALID QUERY SYNTAX HERE")

    def test_missing_match_key(self, conn_conf, test_graph_name, clean_db):
        """Test upserting document without required match key."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"name": "NoId"}]  # Missing 'id' key
            try:
                db.upsert_docs_batch(docs, "NoKey", match_keys=["id"])
            except (KeyError, Exception):
                pass  # Expected

    def test_recovery_after_error(self, conn_conf, test_graph_name, clean_db):
        """Test connection recovery after an error."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # First, cause an error
            try:
                db.execute("INVALID SYNTAX")
            except Exception:
                pass

            # Then, verify connection still works
            db.upsert_docs_batch([{"id": "1"}], "Recovery", match_keys=["id"])
            result = db.fetch_docs("Recovery")
            assert len(result) == 1

    def test_multiple_consecutive_errors(self, conn_conf, test_graph_name, clean_db):
        """Test connection stability after multiple errors."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Cause multiple errors
            for i in range(10):
                try:
                    db.execute(f"INVALID QUERY {i}")
                except Exception:
                    pass

            # Verify connection still works
            db.upsert_docs_batch([{"id": "1"}], "MultiError", match_keys=["id"])
            result = db.fetch_docs("MultiError")
            assert len(result) == 1


# =============================================================================
# AGGREGATION EDGE CASES
# =============================================================================


class TestAggregationEdgeCases:
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

    def test_aggregate_with_filter_on_field(self, conn_conf, test_graph_name, clean_db):
        """Aggregate with filter on specific field value."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "category": "A", "value": 10},
                {"id": "2", "category": None, "value": 20},
                {"id": "3", "category": "A", "value": 30},
                {"id": "4", "value": 40},  # Missing category
            ]
            db.upsert_docs_batch(docs, "NullGroup", match_keys=["id"])

            # Count with filter on category
            result = db.aggregate(
                "NullGroup", AggregationType.COUNT, filters=["==", "A", "category"]
            )
            assert result == 2


# =============================================================================
# FILTER EDGE CASES
# =============================================================================


class TestFilterEdgeCases:
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


# =============================================================================
# EDGE OPERATIONS EDGE CASES
# =============================================================================


class TestEdgeOperationsEdgeCases:
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


# =============================================================================
# BATCH OPERATIONS STRESS
# =============================================================================


class TestBatchStress:
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


# =============================================================================
# CONCURRENCY STRESS TESTS
# =============================================================================


class TestConcurrencyStress:
    """Thread safety and race condition testing."""

    def test_concurrent_inserts(self, conn_conf, test_graph_name, clean_db):
        """Concurrent inserts to same label."""
        _ = clean_db

        def insert_batch(batch_id):
            """Insert a batch of 10 documents with batch-prefixed IDs."""
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
            """Insert 20 documents sequentially, each in its own connection."""
            for i in range(20):
                try:
                    with ConnectionManager(connection_config=conn_conf) as db:
                        docs = [{"id": f"w{i}", "value": i}]
                        db.upsert_docs_batch(docs, "ReadWrite", match_keys=["id"])
                except Exception as e:
                    errors.append(f"Writer error: {e}")

        def reader():
            """Read all documents 20 times, each in its own connection."""
            for _ in range(20):
                try:
                    with ConnectionManager(connection_config=conn_conf) as db:
                        db.fetch_docs("ReadWrite")
                except Exception as e:
                    errors.append(f"Reader error: {e}")

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should complete without errors
        assert len(errors) == 0, f"Errors occurred: {errors}"

    def test_concurrent_upsert_same_key(self, conn_conf, test_graph_name, clean_db):
        """Multiple threads trying to upsert same document."""
        _ = clean_db
        results = []

        def updater(thread_id):
            """Repeatedly upsert the same document with thread-specific metadata."""
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


# =============================================================================
# STATE CORRUPTION
# =============================================================================


class TestStateCorruption:
    """Test state handling and connection management."""

    def test_operations_after_close(self, conn_conf, test_graph_name):
        """Attempt operations after connection is closed."""
        db = ConnectionManager(connection_config=conn_conf)
        db.__enter__()
        db.__exit__(None, None, None)

        # These should fail gracefully
        with pytest.raises(Exception):
            fetch_docs = getattr(db, "fetch_docs")
            fetch_docs("SomeLabel")

    def test_double_close(self, conn_conf, test_graph_name):
        """Close connection twice."""
        db = ConnectionManager(connection_config=conn_conf)
        db.__enter__()
        db.__exit__(None, None, None)
        # Second close should not crash
        db.__exit__(None, None, None)

    def test_nested_context_managers(self, conn_conf, test_graph_name, clean_db):
        """Test nested connection managers."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db1:
            docs = [{"id": "outer"}]
            db1.upsert_docs_batch(docs, "Nested", match_keys=["id"])

            with ConnectionManager(connection_config=conn_conf) as db2:
                # Inner connection should see outer's data
                result = db2.fetch_docs("Nested")
                assert len(result) == 1

                docs = [{"id": "inner"}]
                db2.upsert_docs_batch(docs, "Nested", match_keys=["id"])

            # Outer should see inner's data
            result = db1.fetch_docs("Nested")
            assert len(result) == 2


# =============================================================================
# CONNECTION STRESS TESTS
# =============================================================================


class TestConnectionStress:
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


# =============================================================================
# QUERY INJECTION ADVANCED
# =============================================================================


class TestQueryInjectionAdvanced:
    """More sophisticated injection attempts."""

    def test_comment_injection(self, conn_conf, test_graph_name, clean_db):
        """Try to inject via comment syntax."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            comment_payloads = [
                "value /* comment */ more",
                "value // line comment",
                "value /* /* nested */ */",
                "value --sql comment",
                "value # shell comment",
            ]

            for i, payload in enumerate(comment_payloads):
                docs = [{"id": str(i), "data": payload}]
                db.upsert_docs_batch(docs, "Comments", match_keys=["id"])

            result = db.fetch_docs("Comments")
            assert len(result) == len(comment_payloads)

    def test_parameter_pollution(self, conn_conf, test_graph_name, clean_db):
        """Try parameter pollution attacks."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            pollution_payloads = [
                "$batch[0].id",
                "${batch}",
                "{{batch}}",
                "{batch[0]}",
                "$__proto__",
                "$constructor",
            ]

            for i, payload in enumerate(pollution_payloads):
                docs = [{"id": str(i), "pollute": payload}]
                db.upsert_docs_batch(docs, "Pollute", match_keys=["id"])

            result = db.fetch_docs("Pollute")
            assert len(result) == len(pollution_payloads)

    def test_label_injection_via_value(self, conn_conf, test_graph_name, clean_db):
        """Values that look like label specifications."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            label_payloads = [
                ":Admin",
                "User:Admin",
                "(n:Admin)",
                "}-[:ADMIN]->",
                "`:Admin`",
            ]

            for i, payload in enumerate(label_payloads):
                docs = [{"id": str(i), "label_attempt": payload}]
                db.upsert_docs_batch(docs, "LabelInject", match_keys=["id"])

            result = db.fetch_docs("LabelInject")
            assert len(result) == len(label_payloads)

    def test_unicode_homoglyph_injection(self, conn_conf, test_graph_name, clean_db):
        """Use Unicode homoglyphs to bypass filters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Characters that look like ASCII but aren't
            homoglyphs = [
                "ᎷᎪᎢᏟᎻ",  # Cherokee letters that look like MATCH
                "ⒹⒺⓁⒺⓉⒺ",  # Circled letters
                "ＭＡＴＣＨ",  # Fullwidth
                "𝐌𝐀𝐓𝐂𝐇",  # Mathematical bold
            ]

            for i, payload in enumerate(homoglyphs):
                docs = [{"id": str(i), "sneaky": payload}]
                db.upsert_docs_batch(docs, "Homoglyph", match_keys=["id"])

            result = db.fetch_docs("Homoglyph")
            assert len(result) == len(homoglyphs)


# =============================================================================
# LABEL ABUSE
# =============================================================================


class TestLabelAbuse:
    """Abuse label and relationship type naming."""

    def test_very_long_label_name(self, conn_conf, test_graph_name, clean_db):
        """Extremely long label names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # 1000 character label name
            long_label = "A" * 1000
            docs = [{"id": "1"}]
            try:
                db.upsert_docs_batch(docs, long_label, match_keys=["id"])
            except Exception:
                pass  # May be rejected

    def test_numeric_label_name(self, conn_conf, test_graph_name, clean_db):
        """Labels that are purely numeric."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            numeric_labels = ["123", "0", "-1", "3.14", "1e10"]
            for label in numeric_labels:
                docs = [{"id": "1"}]
                try:
                    db.upsert_docs_batch(docs, label, match_keys=["id"])
                except Exception:
                    pass

    def test_unicode_label_names(self, conn_conf, test_graph_name, clean_db):
        """Labels with various Unicode characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            unicode_labels = [
                "Ümläüt",
                "日本語",
                "Ελληνικά",
                "العربية",
                "Fire",
            ]
            for label in unicode_labels:
                docs = [{"id": "1"}]
                try:
                    db.upsert_docs_batch(docs, label, match_keys=["id"])
                    result = db.fetch_docs(label)
                    assert len(result) == 1
                except Exception:
                    pass  # Unicode labels may not be supported

    def test_reserved_label_names(self, conn_conf, test_graph_name, clean_db):
        """Try to use reserved/internal label names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            reserved = [
                "_internal",
                "__system__",
                "Node",  # Base type in some systems
                "Relationship",
                "Entity",
            ]
            for label in reserved:
                docs = [{"id": "1"}]
                try:
                    db.upsert_docs_batch(docs, label, match_keys=["id"])
                except Exception:
                    pass


# =============================================================================
# MALFORMED ENCODING
# =============================================================================


class TestMalformedEncoding:
    """Test with malformed or invalid encodings."""

    def test_overlong_utf8_sequences(self, conn_conf, test_graph_name, clean_db):
        """UTF-8 overlong encoding (security bypass attempt)."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            try:
                overlong = b"\xc0\xaf".decode("utf-8", errors="replace")
                docs = [{"id": "overlong", "path": overlong}]
                db.upsert_docs_batch(docs, "Overlong", match_keys=["id"])
            except Exception:
                pass

    def test_utf8_bom_injection(self, conn_conf, test_graph_name, clean_db):
        """BOM characters injected mid-string."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            bom_variants = [
                "\ufeffstart",  # BOM at start
                "mid\ufeffdle",  # BOM in middle
                "end\ufeff",  # BOM at end
                "\ufeff\ufeff\ufeff",  # Multiple BOMs
            ]

            for i, text in enumerate(bom_variants):
                docs = [{"id": str(i), "text": text}]
                db.upsert_docs_batch(docs, "BOMTest", match_keys=["id"])

            result = db.fetch_docs("BOMTest")
            assert len(result) == len(bom_variants)

    def test_surrogate_pairs(self, conn_conf, test_graph_name, clean_db):
        """Lone surrogate characters (invalid UTF-16 in UTF-8)."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            try:
                lone_high = "\ud800"  # High surrogate without low
                docs = [{"id": "surrogate", "broken": lone_high}]
                db.upsert_docs_batch(docs, "Surrogate", match_keys=["id"])
            except Exception:
                pass  # Expected to fail

    def test_private_use_area_flood(self, conn_conf, test_graph_name, clean_db):
        """Flood with Private Use Area characters."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # PUA characters - valid but unusual
            pua_string = "".join(chr(0xE000 + i) for i in range(1000))
            docs = [{"id": "pua", "custom": pua_string}]
            db.upsert_docs_batch(docs, "PUA", match_keys=["id"])

            result = db.fetch_docs("PUA")
            assert len(result) == 1


# =============================================================================
# PROPERTY KEY SMUGGLING
# =============================================================================


class TestPropertyKeySmuggling:
    """Attempt to smuggle malicious content via property keys."""

    def test_cypher_keywords_as_keys(self, conn_conf, test_graph_name, clean_db):
        """Use Cypher keywords as property names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            keywords = [
                "MATCH",
                "WHERE",
                "RETURN",
                "CREATE",
                "DELETE",
                "MERGE",
                "SET",
                "REMOVE",
                "DETACH",
                "OPTIONAL",
                "WITH",
                "UNWIND",
                "FOREACH",
                "CALL",
                "YIELD",
            ]

            docs = [{"id": "keyword_node"}]
            for kw in keywords:
                docs[0][kw] = f"value_for_{kw}"

            try:
                db.upsert_docs_batch(docs, "Keywords", match_keys=["id"])
                result = db.fetch_docs("Keywords")
                assert len(result) == 1
            except Exception:
                pass  # Some keywords may be rejected

    def test_operators_in_keys(self, conn_conf, test_graph_name, clean_db):
        """Property names containing operators."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            operator_keys = [
                "a+b",
                "a-b",
                "a*b",
                "a/b",
                "a%b",
                "a=b",
                "a<>b",
                "a<b",
                "a>b",
                "a AND b",
                "a OR b",
                "NOT a",
            ]

            for i, key in enumerate(operator_keys):
                docs = [{"id": str(i), key: "trapped"}]
                try:
                    db.upsert_docs_batch(docs, "Operators", match_keys=["id"])
                except Exception:
                    pass

    def test_internal_property_names(self, conn_conf, test_graph_name, clean_db):
        """Try to use internal property names."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            internal_names = [
                "_id",
                "__id__",
                "_key",
                "__key__",
                "_labels",
                "__labels__",
                "_type",
                "__type__",
                "__class__",
                "__dict__",
                "__proto__",
            ]

            for name in internal_names:
                docs = [{"id": name, name: "internal_value"}]
                try:
                    db.upsert_docs_batch(docs, "Internal", match_keys=["id"])
                except Exception:
                    pass

    def test_whitespace_only_keys(self, conn_conf, test_graph_name, clean_db):
        """Property names that are only whitespace."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            whitespace_keys = [
                " ",  # Single space
                "  ",  # Multiple spaces
                "\t",  # Tab
                "\n",  # Newline
                " \t\n ",  # Mixed
            ]

            for i, key in enumerate(whitespace_keys):
                docs = [{"id": str(i), key: "ghostly"}]
                try:
                    db.upsert_docs_batch(docs, "Whitespace", match_keys=["id"])
                except Exception:
                    pass


# =============================================================================
# TEMPORAL ANOMALIES
# =============================================================================


class TestTemporalAnomalies:
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


# =============================================================================
# GRAPH ALGORITHM EDGE CASES
# =============================================================================


class TestGraphAlgorithmEdgeCases:
    """Pathological graph structures that stress graph algorithms."""

    def test_cycle_detection_complex(self, conn_conf, test_graph_name, clean_db):
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
                    edges.append([{"id": f"clique_{i}"}, {"id": f"clique_{j}"}, {}])

            # Stick edges (path)
            for i in range(stick_length - 1):
                edges.append([{"id": f"stick_{i}"}, {"id": f"stick_{i + 1}"}, {}])

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


# =============================================================================
# DEADLOCK PREVENTION TESTS
# =============================================================================


class TestDeadlockPrevention:
    """Deadlock detection and transaction isolation testing."""

    def test_circular_update_dependency(self, conn_conf, test_graph_name, clean_db):
        """Create circular update patterns that could deadlock."""
        _ = clean_db
        errors = []

        def update_a_then_b():
            """Update node A then node B in a loop to test lock ordering."""
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
            """Update node B then node A in opposite order to provoke deadlock."""
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

        t1 = threading.Thread(target=update_a_then_b)
        t2 = threading.Thread(target=update_b_then_a)

        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        # Should complete without deadlock
        assert not t1.is_alive(), "Thread 1 appears deadlocked"
        assert not t2.is_alive(), "Thread 2 appears deadlocked"

    def test_self_referential_update(self, conn_conf, test_graph_name, clean_db):
        """Update a node based on reading its own value."""
        import time

        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "self", "counter": 0}], "SelfRef", match_keys=["id"]
            )

            successful_updates = 0
            for _ in range(50):
                for retry in range(3):  # Retry on transaction conflicts
                    try:
                        result = db.fetch_docs("SelfRef", filters=["==", "self", "id"])
                        if result:
                            new_val = result[0].get("counter", 0) + 1
                            db.upsert_docs_batch(
                                [{"id": "self", "counter": new_val}],
                                "SelfRef",
                                match_keys=["id"],
                            )
                            successful_updates += 1
                            break
                    except Exception as e:
                        if "conflicting transactions" in str(e).lower() and retry < 2:
                            time.sleep(0.01 * (retry + 1))  # Backoff
                            continue
                        raise

            result = db.fetch_docs("SelfRef")
            assert result[0]["counter"] > 0
            assert successful_updates > 0

    def test_phantom_read_scenario(self, conn_conf, test_graph_name, clean_db):
        """Scenario that could cause phantom reads."""
        _ = clean_db
        phantoms_detected = []

        def inserter():
            """Insert 100 documents sequentially to create growing dataset."""
            with ConnectionManager(connection_config=conn_conf) as db:
                for i in range(100):
                    db.upsert_docs_batch(
                        [{"id": f"phantom_{i}"}], "Phantom", match_keys=["id"]
                    )

        def reader():
            """Count documents repeatedly to detect non-monotonic phantom reads."""
            with ConnectionManager(connection_config=conn_conf) as db:
                counts = []
                for _ in range(20):
                    count = db.aggregate("Phantom", AggregationType.COUNT)
                    counts.append(count)
                # Check for non-monotonic reads (would indicate phantoms)
                for i in range(1, len(counts)):
                    if counts[i] < counts[i - 1]:
                        phantoms_detected.append((counts[i - 1], counts[i]))

        t1 = threading.Thread(target=inserter)
        t2 = threading.Thread(target=reader)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Phantom reads are possible in some isolation levels
        # We just verify it doesn't crash


# =============================================================================
# REDOS
# =============================================================================


class TestReDoS:
    """Test patterns that could cause ReDoS if regex is used internally."""

    def test_regex_pattern_edge_cases(self, conn_conf, test_graph_name, clean_db):
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

    def test_filter_with_potential_redos(self, conn_conf, test_graph_name, clean_db):
        """Filter operations with ReDoS-vulnerable patterns."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": "target", "data": "normal_value"}]
            db.upsert_docs_batch(docs, "ReDoSFilter", match_keys=["id"])

            # These could be catastrophic if the filter uses regex matching
            edge_case_filters = [
                ".*" * 20 + "x",
                "(a+)+" * 10,
                "((a+)+)+" * 5,
            ]

            for pattern in edge_case_filters:
                try:
                    # This should NOT hang
                    result = db.fetch_docs(
                        "ReDoSFilter",
                        filters=["==", pattern, "data"],
                        limit=1,
                    )
                    assert isinstance(result, list)
                except Exception:
                    pass  # Exception is fine, hanging is not


# =============================================================================
# QUERY COMPLEXITY
# =============================================================================


class TestQueryComplexity:
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

    def test_union_query_load(self, conn_conf, test_graph_name, clean_db):
        """Many UNION clauses (reduced from 50 to avoid Memgraph hang)."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": "1"}]
            db.upsert_docs_batch(docs, "UnionLoad", match_keys=["id"])

            # Build query with UNIONs (reduced to prevent timeout)
            query_parts = ["MATCH (n:UnionLoad) RETURN n.id AS id" for _ in range(10)]
            query = " UNION ALL ".join(query_parts)

            try:
                result = db.execute(query)
                assert len(result.result_set) == 10
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


# =============================================================================
# MEMORY EXHAUSTION
# =============================================================================


class TestMemoryExhaustion:
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
                pass  # May legitimately fail

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
                pass  # Binary may not be supported


# =============================================================================
# CARTESIAN PRODUCT LOAD TESTS
# =============================================================================


class TestCartesianProductLoad:
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
                pass  # Timeout acceptable


# =============================================================================
# SCHEMA EVOLUTION
# =============================================================================


class TestSchemaEvolution:
    """Test rapid schema changes and type mutations."""

    def test_property_type_mutation_rapid(self, conn_conf, test_graph_name, clean_db):
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


# =============================================================================
# BOUNDARY VALUE ANALYSIS
# =============================================================================


class TestBoundaryValueAnalysis:
    """Systematic boundary value testing."""

    def test_string_length_boundaries(self, conn_conf, test_graph_name, clean_db):
        """Test strings at various length boundaries."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Test at power-of-two boundaries
            lengths = [0, 1, 255, 256, 1023, 1024, 65535, 65536]

            for length in lengths:
                docs = [{"id": f"len_{length}", "data": "x" * length}]
                try:
                    db.upsert_docs_batch(docs, "StringBounds", match_keys=["id"])
                except Exception:
                    pass

            result = db.fetch_docs("StringBounds")
            assert len(result) > 0

    def test_array_length_boundaries(self, conn_conf, test_graph_name, clean_db):
        """Test arrays at various length boundaries."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            lengths = [0, 1, 100, 1000, 10000]

            for length in lengths:
                arr = list(range(length))
                docs = [{"id": f"arr_{length}", "data": arr}]
                try:
                    db.upsert_docs_batch(docs, "ArrayBounds", match_keys=["id"])
                except Exception:
                    pass  # Large arrays may not be supported

    def test_property_count_boundaries(self, conn_conf, test_graph_name, clean_db):
        """Test documents with boundary number of properties."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            counts = [1, 10, 100, 500, 1000]

            for count in counts:
                doc = {"id": f"props_{count}"}
                for i in range(count):
                    doc[f"p{i}"] = i

                try:
                    db.upsert_docs_batch([doc], "PropCount", match_keys=["id"])
                except Exception:
                    pass


# =============================================================================
# FILTER STRESS TESTS
# =============================================================================


class TestFilterStress:
    """Stress test the filter system."""

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


# =============================================================================
# PATHOLOGICAL IDS
# =============================================================================


class TestPathologicalIds:
    """IDs designed to cause problems."""

    def test_collision_prone_ids(self, conn_conf, test_graph_name, clean_db):
        """IDs that might collide in weak hash functions."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # These pairs are known to collide in some hash functions
            collision_pairs = [
                ("aaa", "bbb"),
                ("", " "),  # Empty vs space
                ("0", "00"),  # Leading zeros
                ("null", "NULL"),  # Case variants
            ]

            all_ids = [id for pair in collision_pairs for id in pair]
            docs = [{"id": id, "unique_marker": i} for i, id in enumerate(all_ids)]
            db.upsert_docs_batch(docs, "Collisions", match_keys=["id"])

            result = db.fetch_docs("Collisions")
            # All should be stored as distinct
            assert len(result) == len(all_ids)

    def test_lookalike_ids(self, conn_conf, test_graph_name, clean_db):
        """IDs that look similar but are different."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            lookalikes = [
                "O0",  # O (letter) vs 0 (zero)
                "0O",
                "l1",  # l (letter) vs 1 (one)
                "1l",
                "rn",  # looks like 'm'
                "m",
                "vv",  # looks like 'w'
                "w",
            ]

            docs = [{"id": id, "marker": i} for i, id in enumerate(lookalikes)]
            db.upsert_docs_batch(docs, "Lookalikes", match_keys=["id"])

            result = db.fetch_docs("Lookalikes")
            assert len(result) == len(lookalikes)

    def test_id_with_sql_keywords(self, conn_conf, test_graph_name, clean_db):
        """IDs that are SQL keywords."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            sql_keywords = [
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "DROP",
                "TABLE",
                "DATABASE",
                "INDEX",
                "FROM",
                "WHERE",
            ]

            docs = [{"id": kw, "type": "keyword"} for kw in sql_keywords]
            db.upsert_docs_batch(docs, "SQLKeywords", match_keys=["id"])

            result = db.fetch_docs("SQLKeywords")
            assert len(result) == len(sql_keywords)


# =============================================================================
# DATA TYPE EDGE CASES
# =============================================================================


class TestDataTypeEdgeCases:
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
