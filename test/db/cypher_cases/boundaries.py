"""Boundary values, type confusion and pathological identifiers.

Backend-agnostic Cypher cases, shared by the Memgraph and FalkorDB suites.

These are mixins, not tests: the module is outside any ``test_*.py`` so pytest
never collects them directly. Each backend's ``test_edge_cases.py`` subclasses
them, which is what supplies the ``conn_conf`` / ``test_graph_name`` /
``clean_db`` fixtures -- both backend conftests expose that same trio.
"""

from __future__ import annotations

from graflo.db.manager import ConnectionManager


class BoundaryConditionsCases:
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


class BoundaryValueAnalysisCases:
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
                    pass

    def test_property_count_boundaries(self, conn_conf, test_graph_name, clean_db):
        """Test documents with boundary number of properties."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            counts = [1, 10, 100, 500, 1000]

            for count in counts:
                doc: dict[str, str | int] = {"id": f"props_{count}"}
                for i in range(count):
                    doc[f"p{i}"] = i

                try:
                    db.upsert_docs_batch([doc], "PropCount", match_keys=["id"])
                except Exception:
                    pass


class TypeConfusionCases:
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


class PathologicalIdsCases:
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


class MalformedInputsCases:
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
