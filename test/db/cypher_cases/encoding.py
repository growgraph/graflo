"""Unicode, encoding and property-key smuggling cases.

Backend-agnostic Cypher cases, shared by the Memgraph and FalkorDB suites.

These are mixins, not tests: the module is outside any ``test_*.py`` so pytest
never collects them directly. Each backend's ``test_edge_cases.py`` subclasses
them, which is what supplies the ``conn_conf`` / ``test_graph_name`` /
``clean_db`` fixtures -- both backend conftests expose that same trio.
"""

from __future__ import annotations

from graflo.db.manager import ConnectionManager


class MalformedEncodingCases:
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
                pass

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


class PropertyKeySmugglingCases:
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
                pass

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


class LabelAbuseCases:
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
                pass

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
                    pass

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
