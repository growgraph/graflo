"""Cypher injection and query-smuggling cases.

Backend-agnostic Cypher cases, shared by the Memgraph and FalkorDB suites.

These are mixins, not tests: the module is outside any ``test_*.py`` so pytest
never collects them directly. Each backend's ``test_edge_cases.py`` subclasses
them, which is what supplies the ``conn_conf`` / ``test_graph_name`` /
``clean_db`` fixtures -- both backend conftests expose that same trio.
"""

from __future__ import annotations

from graflo.db.manager import ConnectionManager


class CypherInjectionCases:
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


class QueryInjectionAdvancedCases:
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


class ReDoSCases:
    """Test patterns that could cause ReDoS if regex is used internally."""

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
                    pass
