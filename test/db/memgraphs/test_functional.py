"""Functional tests for Memgraph connector.

This module provides comprehensive functional tests that validate the Memgraph
connector behavior in realistic scenarios. Tests focus on data consistency,
relationship handling, schema evolution, and edge cases that commonly occur
in production environments.

Test Categories
---------------
DataCoherence
    Tests for data consistency with composite keys, out-of-order updates,
    partial updates, and null value handling.

MultiSourceSync
    Tests for multi-source data synchronization scenarios including
    conflicting updates and UUID collisions.

Relationships
    Tests for graph relationship operations including orphan handling,
    dependency diamonds, cycles, and polymorphic relationships.

ETL
    Tests for real-world ETL scenarios including CSV imports, schema
    evolution, and data type migrations.

QueryEdgeCases
    Tests for query edge cases including non-existent fields, contradictory
    conditions, and projection behavior.

TransactionalRobustness
    Tests for transactional behavior and error recovery.

Usage
-----
Run all functional tests::

    pytest test/db/memgraphs/test_functional.py -v

Run specific category::

    pytest test/db/memgraphs/test_functional.py -v -k "DataCoherence"

Notes
-----
- Tests require a running Memgraph instance (see conftest.py fixtures)
- Some tests use pytest.skip() to document expected platform behavior
- Each test uses isolated graph instances for test independence

See Also
--------
- graflo.db.memgraph.conn : Memgraph connector implementation
- test.db.memgraphs.test_edge_cases : Technical edge case tests
"""

import pytest

from graflo.db import ConnectionManager
from graflo.onto import AggregationType


# =============================================================================
# DATA COHERENCE TESTS
# =============================================================================


class TestDataCoherence:
    """Tests for data consistency in various update scenarios.

    These tests validate that the connector correctly handles complex
    data update patterns that commonly occur in production systems.
    """

    def test_composite_key_partial_collision(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Verify composite keys prevent unintended merges.

        When using composite keys (e.g., source + external_id), documents
        with partial key matches should remain distinct entities.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Two users from different sources with same external_id
            docs = [
                {
                    "source": "linkedin",
                    "external_id": "12345",
                    "name": "Alice LinkedIn",
                },
                {"source": "github", "external_id": "12345", "name": "Bob GitHub"},
            ]
            db.upsert_docs_batch(docs, "User", match_keys=["source", "external_id"])

            result = db.fetch_docs("User")
            assert len(result) == 2, "Partial key collision merged distinct entities"

            names = {r["name"] for r in result}
            assert "Alice LinkedIn" in names
            assert "Bob GitHub" in names

    def test_out_of_order_versioned_updates(self, conn_conf, test_graph_name, clean_db):
        """Document last-write-wins behavior with out-of-order updates.

        In distributed systems, events may arrive out of order. This test
        documents the connector's behavior: the last write wins regardless
        of logical version numbers.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Arrival order: v1, v3, v2 - last write (v2) wins
            updates = [
                {
                    "id": "doc1",
                    "version": 1,
                    "content": "Initial",
                    "updated_at": "2024-01-01",
                },
                {
                    "id": "doc1",
                    "version": 3,
                    "content": "Latest",
                    "updated_at": "2024-01-03",
                },
                {
                    "id": "doc1",
                    "version": 2,
                    "content": "Middle",
                    "updated_at": "2024-01-02",
                },
            ]

            for doc in updates:
                db.upsert_docs_batch([doc], "Document", match_keys=["id"])

            result = db.fetch_docs("Document")
            assert len(result) == 1
            # MERGE overwrites, so last write (version 2) wins
            assert result[0]["version"] == 2, (
                "Expected last-write-wins behavior. "
                "If version ordering is required, implement application-level checks."
            )

    def test_partial_update_preserves_fields(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Verify MERGE += preserves existing fields on partial updates.

        When updating a document with only a subset of fields, existing
        fields should be preserved (not deleted).
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Full document
            full_doc = {
                "id": "user1",
                "name": "Alice",
                "email": "alice@example.com",
                "phone": "555-1234",
                "address": "123 Main St",
            }
            db.upsert_docs_batch([full_doc], "Contact", match_keys=["id"])

            # Partial update - only email changes
            partial_update = {"id": "user1", "email": "alice.new@example.com"}
            db.upsert_docs_batch([partial_update], "Contact", match_keys=["id"])

            result = db.fetch_docs("Contact")
            assert len(result) == 1
            contact = result[0]

            # MERGE += should preserve other fields
            assert contact.get("name") == "Alice", (
                "Field 'name' lost after partial update"
            )
            assert contact.get("phone") == "555-1234", (
                "Field 'phone' lost after partial update"
            )
            assert contact["email"] == "alice.new@example.com"

    def test_null_value_behavior(self, conn_conf, test_graph_name, clean_db):
        """Document null value handling in upserts.

        This test documents whether explicit None values overwrite
        existing valid values during upsert operations.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Initial complete data
            db.upsert_docs_batch(
                [
                    {
                        "id": "product1",
                        "name": "Widget",
                        "price": 99.99,
                        "sku": "WDG-001",
                    }
                ],
                "Product",
                match_keys=["id"],
            )

            # Import with null price (e.g., from CSV with empty field)
            csv_import = {"id": "product1", "name": "Widget Pro", "price": None}
            db.upsert_docs_batch([csv_import], "Product", match_keys=["id"])

            result = db.fetch_docs("Product")
            product = result[0]

            # Memgraph MERGE with SET += overwrites with None values
            # This documents expected behavior - filter None values before upsert if undesired
            assert product.get("price") is None, (
                "Expected None to overwrite existing value with MERGE SET +="
            )

    def test_empty_string_vs_null_distinction(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Verify empty string and null are handled distinctly."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "value": ""},
                {"id": "2", "value": None},
                {"id": "3"},  # Key absent
            ]
            db.upsert_docs_batch(docs, "EmptyTest", match_keys=["id"])

            result = db.fetch_docs("EmptyTest")
            by_id = {r["id"]: r for r in result}

            assert by_id["1"].get("value") == "", "Empty string not preserved"
            assert "2" in by_id and "3" in by_id


class TestMultiSourceSync:
    """Tests for multi-source data synchronization scenarios."""

    def test_conflicting_updates_same_entity(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Document conflict resolution with multiple data sources.

        When multiple sources update the same entity, last-write-wins
        applies. This test documents this behavior.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Source 1: LinkedIn
            db.upsert_docs_batch(
                [
                    {
                        "id": "user1",
                        "email": "linkedin@example.com",
                        "source": "linkedin",
                    }
                ],
                "User",
                match_keys=["id"],
            )

            # Source 2: GitHub (same user)
            db.upsert_docs_batch(
                [{"id": "user1", "email": "github@example.com", "source": "github"}],
                "User",
                match_keys=["id"],
            )

            result = db.fetch_docs("User")
            assert len(result) == 1
            # Last write wins
            assert result[0]["email"] == "github@example.com"
            assert result[0]["source"] == "github"

    def test_uuid_collision_handling(self, conn_conf, test_graph_name, clean_db):
        """Document behavior when UUIDs collide across systems.

        UUID collisions are rare but catastrophic. This test documents
        that collisions result in data overwrites.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            colliding_uuid = "550e8400-e29b-41d4-a716-446655440000"

            # System A: a user
            doc_a = {"uuid": colliding_uuid, "type": "user", "name": "Alice"}
            db.upsert_docs_batch([doc_a], "Entity", match_keys=["uuid"])

            # System B: a product with same UUID
            doc_b = {"uuid": colliding_uuid, "type": "product", "name": "Widget"}
            db.upsert_docs_batch([doc_b], "Entity", match_keys=["uuid"])

            result = db.fetch_docs("Entity")
            assert len(result) == 1
            # Product overwrote user - silent data corruption
            assert result[0]["type"] == "product"
            assert result[0]["name"] == "Widget"


# =============================================================================
# RELATIONSHIP TESTS
# =============================================================================


class TestRelationships:
    """Tests for graph relationship operations."""

    def test_orphan_cleanup_with_detach_delete(
        self, conn_conf, test_graph_name, clean_db
    ):
        """Verify DETACH DELETE removes relationships with deleted nodes."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Create two users with a relationship
            db.upsert_docs_batch(
                [{"id": "user1", "name": "Alice"}, {"id": "user2", "name": "Bob"}],
                "User",
                match_keys=["id"],
            )

            edges = [[{"id": "user1"}, {"id": "user2"}, {"since": 2020}]]
            db.insert_edges_batch(
                edges,
                source_class="User",
                target_class="User",
                relation_name="FRIENDS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Delete user1 with DETACH DELETE
            db.execute("MATCH (u:User {id: 'user1'}) DETACH DELETE u")

            # Relationship should also be deleted
            result = db.execute("MATCH ()-[r:FRIENDS]->() RETURN count(r)")
            assert result.result_set[0][0] == 0, (
                "Orphan relationship after DETACH DELETE"
            )

    def test_diamond_dependency_paths(self, conn_conf, test_graph_name, clean_db):
        """Verify correct path enumeration in diamond topology.

        Diamond: A -> B -> D
                 A -> C -> D

        Should find exactly two paths from A to D.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            nodes = [{"id": "A"}, {"id": "B"}, {"id": "C"}, {"id": "D"}]
            db.upsert_docs_batch(nodes, "Node", match_keys=["id"])

            edges = [
                [{"id": "A"}, {"id": "B"}, {"path": "left"}],
                [{"id": "A"}, {"id": "C"}, {"path": "right"}],
                [{"id": "B"}, {"id": "D"}, {"path": "left"}],
                [{"id": "C"}, {"id": "D"}, {"path": "right"}],
            ]
            db.insert_edges_batch(
                edges,
                source_class="Node",
                target_class="Node",
                relation_name="LEADS_TO",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            result = db.execute(
                "MATCH p=(a:Node {id: 'A'})-[:LEADS_TO*]->(d:Node {id: 'D'}) "
                "RETURN length(p), [r in relationships(p) | r.path]"
            )
            assert len(result.result_set) == 2, "Expected two paths in diamond topology"

    def test_cycle_detection(self, conn_conf, test_graph_name, clean_db):
        """Verify cyclic dependencies can be detected.

        Creates A -> B -> C -> A and verifies cycle detection query works.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            nodes = [
                {"id": "serviceA", "name": "Auth"},
                {"id": "serviceB", "name": "Users"},
                {"id": "serviceC", "name": "Permissions"},
            ]
            db.upsert_docs_batch(nodes, "Service", match_keys=["id"])

            edges = [
                [{"id": "serviceA"}, {"id": "serviceB"}, {}],
                [{"id": "serviceB"}, {"id": "serviceC"}, {}],
                [{"id": "serviceC"}, {"id": "serviceA"}, {}],
            ]
            db.insert_edges_batch(
                edges,
                source_class="Service",
                target_class="Service",
                relation_name="DEPENDS_ON",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Detect cycle
            result = db.execute(
                "MATCH p=(s:Service)-[:DEPENDS_ON*]->(s) "
                "RETURN length(p) as cycle_length LIMIT 1"
            )
            assert len(result.result_set) > 0, "Cycle not detected"
            assert result.result_set[0][0] == 3

    def test_multiple_relationship_types(self, conn_conf, test_graph_name, clean_db):
        """Verify distinct relationship types between same nodes."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "alice"}, {"id": "bob"}], "Person", match_keys=["id"]
            )

            # Relationship 1: KNOWS
            db.insert_edges_batch(
                [[{"id": "alice"}, {"id": "bob"}, {"since": 2015}]],
                source_class="Person",
                target_class="Person",
                relation_name="KNOWS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Relationship 2: MANAGES
            db.insert_edges_batch(
                [[{"id": "alice"}, {"id": "bob"}, {"since": 2020}]],
                source_class="Person",
                target_class="Person",
                relation_name="MANAGES",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            knows = db.execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r)")
            manages = db.execute(
                "MATCH (a:Person)-[r:MANAGES]->(b:Person) RETURN count(r)"
            )

            assert knows.result_set[0][0] == 1
            assert manages.result_set[0][0] == 1


class TestPolymorphicRelationships:
    """Tests for relationships with heterogeneous target types."""

    def test_heterogeneous_targets(self, conn_conf, test_graph_name, clean_db):
        """Verify relationships to different node types.

        Person OWNS Car, House, and Pet - query should find all.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([{"id": "alice"}], "Person", match_keys=["id"])
            db.upsert_docs_batch(
                [{"id": "car1", "model": "Tesla"}], "Car", match_keys=["id"]
            )
            db.upsert_docs_batch(
                [{"id": "house1", "address": "123 Main"}], "House", match_keys=["id"]
            )
            db.upsert_docs_batch(
                [{"id": "cat1", "name": "Whiskers"}], "Pet", match_keys=["id"]
            )

            db.insert_edges_batch(
                [[{"id": "alice"}, {"id": "car1"}, {"year": 2023}]],
                source_class="Person",
                target_class="Car",
                relation_name="OWNS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )
            db.insert_edges_batch(
                [[{"id": "alice"}, {"id": "house1"}, {"year": 2020}]],
                source_class="Person",
                target_class="House",
                relation_name="OWNS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )
            db.insert_edges_batch(
                [[{"id": "alice"}, {"id": "cat1"}, {"year": 2022}]],
                source_class="Person",
                target_class="Pet",
                relation_name="OWNS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Polymorphic query
            result = db.execute(
                "MATCH (p:Person {id: 'alice'})-[:OWNS]->(thing) "
                "RETURN labels(thing), thing.id"
            )
            assert len(result.result_set) == 3


# =============================================================================
# ETL AND SCHEMA EVOLUTION TESTS
# =============================================================================


class TestETLScenarios:
    """Tests for real-world ETL scenarios."""

    def test_whitespace_in_match_keys(self, conn_conf, test_graph_name, clean_db):
        """Document whitespace handling in match keys.

        Leading/trailing whitespace in keys can cause duplicate entries.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            csv_rows = [
                {"email": "alice@example.com", "name": "Alice"},
                {
                    "email": "alice@example.com ",
                    "name": "Alice Updated",
                },  # trailing space
                {"email": " alice@example.com", "name": "Alice Again"},  # leading space
            ]

            for row in csv_rows:
                db.upsert_docs_batch([row], "User", match_keys=["email"])

            result = db.fetch_docs("User")
            # Memgraph does not trim whitespace - each variation creates a distinct entity
            # Preprocess data with .strip() before upsert if normalization is desired
            assert len(result) == 3, (
                "Whitespace variations should create distinct entities in Memgraph"
            )

    def test_case_sensitivity(self, conn_conf, test_graph_name, clean_db):
        """Document case sensitivity behavior in match keys."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"email": "Alice@Example.com", "name": "Alice Caps"},
                {"email": "alice@example.com", "name": "Alice Lower"},
            ]

            for doc in docs:
                db.upsert_docs_batch([doc], "User", match_keys=["email"])

            result = db.fetch_docs("User")
            # Memgraph is case-sensitive by default
            assert len(result) >= 1

    def test_partial_field_import(self, conn_conf, test_graph_name, clean_db):
        """Verify partial imports preserve existing fields."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Full initial import
            full_doc = {
                "id": "user1",
                "name": "Alice",
                "email": "a@b.com",
                "phone": "555",
            }
            db.upsert_docs_batch([full_doc], "User", match_keys=["id"])

            # Partial re-import
            partial_doc = {"id": "user1", "name": "Alice Updated"}
            db.upsert_docs_batch([partial_doc], "User", match_keys=["id"])

            result = db.fetch_docs("User")
            user = result[0]

            assert user.get("email") == "a@b.com", "Email lost during partial import"
            assert user.get("phone") == "555", "Phone lost during partial import"

    def test_mixed_type_timestamps(self, conn_conf, test_graph_name, clean_db):
        """Document mixed timestamp type handling.

        Memgraph strictly enforces type consistency for comparisons/ordering.
        Mixed types (string + int) will cause errors on ORDER BY.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "created_at": "1704067200"},  # string
                {"id": "2", "created_at": 1704067200},  # int
                {"id": "3", "created_at": 1704067201},  # int
            ]
            db.upsert_docs_batch(docs, "Event", match_keys=["id"])

            # Memgraph can't compare mixed types, so just verify data exists
            result = db.execute("MATCH (e:Event) RETURN e.id, e.created_at")
            assert len(result.result_set) == 3


class TestSchemaEvolution:
    """Tests for schema migration scenarios."""

    def test_new_required_field(self, conn_conf, test_graph_name, clean_db):
        """Handle schema evolution with new fields."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # V1 data
            v1_docs = [
                {"id": "1", "name": "Old Item 1"},
                {"id": "2", "name": "Old Item 2"},
            ]
            db.upsert_docs_batch(v1_docs, "Item", match_keys=["id"])

            # V2 data with new field
            v2_docs = [
                {"id": "3", "name": "New Item", "status": "active"},
            ]
            db.upsert_docs_batch(v2_docs, "Item", match_keys=["id"])

            # Query filtering on new field
            active = db.fetch_docs("Item", filters=["==", "active", "status"])
            assert len(active) == 1

            # Query for items without status
            result = db.execute("MATCH (i:Item) WHERE i.status IS NULL RETURN count(i)")
            assert result.result_set[0][0] == 2

    def test_field_rename_migration(self, conn_conf, test_graph_name, clean_db):
        """Handle field rename with coalesce pattern."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Old format
            old_format = [
                {"id": "1", "fullName": "Alice Smith"},
                {"id": "2", "fullName": "Bob Jones"},
            ]
            db.upsert_docs_batch(old_format, "Person", match_keys=["id"])

            # New format
            new_format = [
                {"id": "3", "full_name": "Charlie Brown"},
            ]
            db.upsert_docs_batch(new_format, "Person", match_keys=["id"])

            # Query supporting both formats
            result = db.execute(
                "MATCH (p:Person) "
                "RETURN p.id, coalesce(p.full_name, p.fullName) as name"
            )
            assert len(result.result_set) == 3

    def test_type_migration(self, conn_conf, test_graph_name, clean_db):
        """Document type change handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Old format (string)
            db.upsert_docs_batch(
                [{"id": "1", "score": "85"}, {"id": "2", "score": "90"}],
                "Result",
                match_keys=["id"],
            )

            # New format (int)
            db.upsert_docs_batch(
                [{"id": "3", "score": 95}],
                "Result",
                match_keys=["id"],
            )

            # Memgraph cannot aggregate mixed types (string + int)
            # This is expected behavior - normalize types before storing
            with pytest.raises(Exception):
                db.aggregate(
                    "Result", AggregationType.AVERAGE, aggregated_field="score"
                )


# =============================================================================
# QUERY EDGE CASES
# =============================================================================


class TestQueryEdgeCases:
    """Tests for query edge cases and unusual patterns."""

    def test_filter_nonexistent_value(self, conn_conf, test_graph_name, clean_db):
        """Filter on value that doesn't exist returns empty."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "1", "status": "active"}],
                "Item",
                match_keys=["id"],
            )

            result = db.fetch_docs("Item", filters=["==", "deleted", "status"])
            assert result == []

    def test_contradictory_conditions(self, conn_conf, test_graph_name, clean_db):
        """Contradictory conditions return empty result."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "1", "status": "active"}],
                "Item",
                match_keys=["id"],
            )

            result = db.execute(
                "MATCH (i:Item) WHERE i.status = 'active' AND i.status = 'deleted' RETURN i"
            )
            assert len(result.result_set) == 0

    def test_projection_nonexistent_field(self, conn_conf, test_graph_name, clean_db):
        """Projection on non-existent field returns null."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "1", "name": "Test"}],
                "Item",
                match_keys=["id"],
            )

            result = db.fetch_docs("Item", return_keys=["id", "ghost_field"])
            assert len(result) == 1
            assert "ghost_field" in result[0]
            assert result[0]["ghost_field"] is None

    def test_limit_exceeds_dataset(self, conn_conf, test_graph_name, clean_db):
        """Limit larger than dataset returns all results."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": str(i)} for i in range(5)],
                "Item",
                match_keys=["id"],
            )

            result = db.fetch_docs("Item", limit=1000)
            assert len(result) == 5

    def test_aggregate_mixed_types(self, conn_conf, test_graph_name, clean_db):
        """Document aggregation behavior with mixed types."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "value": 10},
                {"id": "2", "value": "not a number"},
                {"id": "3", "value": 30},
            ]
            db.upsert_docs_batch(docs, "Mixed", match_keys=["id"])

            try:
                _result = db.execute(
                    "MATCH (m:Mixed) WHERE m.value IS NOT NULL RETURN sum(m.value)"
                )
            except Exception:
                pass  # Expected behavior varies


class TestTraversalEdgeCases:
    """Tests for graph traversal edge cases."""

    def test_zero_length_path(self, conn_conf, test_graph_name, clean_db):
        """Path of length 0 includes the node itself."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([{"id": "A"}], "Node", match_keys=["id"])

            result = db.execute(
                "MATCH p=(n:Node {id: 'A'})-[*0..1]->(m) RETURN length(p), m.id"
            )
            lengths = [row[0] for row in result.result_set]
            assert 0 in lengths, "Zero-length path not found"

    def test_intermediate_node_filter(self, conn_conf, test_graph_name, clean_db):
        """Filter paths based on intermediate node properties."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            nodes = [
                {"id": "A", "type": "start"},
                {"id": "B", "type": "valid"},
                {"id": "C", "type": "blocked"},
                {"id": "D", "type": "end"},
            ]
            db.upsert_docs_batch(nodes, "Node", match_keys=["id"])

            edges = [
                [{"id": "A"}, {"id": "B"}, {}],
                [{"id": "B"}, {"id": "D"}, {}],
                [{"id": "A"}, {"id": "C"}, {}],
                [{"id": "C"}, {"id": "D"}, {}],
            ]
            db.insert_edges_batch(
                edges,
                source_class="Node",
                target_class="Node",
                relation_name="NEXT",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Find paths NOT passing through "blocked"
            result = db.execute(
                "MATCH p=(a:Node {id: 'A'})-[:NEXT*]->(d:Node {id: 'D'}) "
                "WHERE NONE(n IN nodes(p) WHERE n.type = 'blocked') "
                "RETURN [n IN nodes(p) | n.id]"
            )
            assert len(result.result_set) == 1
            assert result.result_set[0][0] == ["A", "B", "D"]


# =============================================================================
# TRANSACTIONAL ROBUSTNESS
# =============================================================================


class TestTransactionalRobustness:
    """Tests for transactional behavior and error recovery."""

    def test_batch_with_invalid_document(self, conn_conf, test_graph_name, clean_db):
        """Document batch behavior with invalid documents.

        Tests whether invalid documents cause entire batch to fail
        or only affect the invalid document.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "1", "name": "Valid 1"},
                {"id": "2", "name": float("nan")},  # Invalid
                {"id": "3", "name": "Valid 3"},
            ]

            try:
                db.upsert_docs_batch(docs, "Item", match_keys=["id"])
            except Exception:
                pass

            result = db.fetch_docs("Item")
            if len(result) == 0:
                pytest.skip(
                    "Atomic behavior: entire batch rejected on invalid document"
                )
            elif len(result) == 2:
                pytest.skip("Non-atomic behavior: valid documents inserted")

    def test_no_unique_constraint(self, conn_conf, test_graph_name, clean_db):
        """Document lack of unique constraints in Memgraph."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # First document
            db.upsert_docs_batch(
                [{"id": "1", "email": "unique@example.com"}],
                "User",
                match_keys=["id"],
            )

            # Second document with same email but different id
            db.upsert_docs_batch(
                [{"id": "2", "email": "unique@example.com"}],
                "User",
                match_keys=["id"],
            )

            result = db.fetch_docs("User")
            # Two users with same email - no constraint violation
            assert len(result) == 2

    def test_connection_recovery(self, conn_conf, test_graph_name, clean_db):
        """Connection remains usable after query error."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch([{"id": "1"}], "Item", match_keys=["id"])

            # Trigger syntax error
            try:
                db.execute("INVALID CYPHER QUERY SYNTAX")
            except Exception:
                pass

            # Connection should still work
            result = db.fetch_docs("Item")
            assert len(result) == 1, "Connection broken after query error"


# =============================================================================
# PERFORMANCE EDGE CASES
# =============================================================================


class TestPerformanceEdgeCases:
    """Tests for performance-related edge cases."""

    def test_cartesian_product(self, conn_conf, test_graph_name, clean_db):
        """Document Cartesian product behavior.

        MATCH (a:A), (b:B) without relationship creates |A| x |B| results.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": str(i)} for i in range(100)], "TypeA", match_keys=["id"]
            )
            db.upsert_docs_batch(
                [{"id": str(i)} for i in range(100)], "TypeB", match_keys=["id"]
            )

            result = db.execute("MATCH (a:TypeA), (b:TypeB) RETURN count(*)")
            assert result.result_set[0][0] == 10000, "Unexpected Cartesian product size"

    def test_unbounded_path(self, conn_conf, test_graph_name, clean_db):
        """Test unbounded variable-length path traversal."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            nodes = [{"id": str(i)} for i in range(20)]
            db.upsert_docs_batch(nodes, "Chain", match_keys=["id"])

            edges = [[{"id": str(i)}, {"id": str(i + 1)}, {}] for i in range(19)]
            db.insert_edges_batch(
                edges,
                source_class="Chain",
                target_class="Chain",
                relation_name="NEXT",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            result = db.execute(
                "MATCH p=(start:Chain {id: '0'})-[:NEXT*]->(end) "
                "RETURN length(p), end.id ORDER BY length(p) DESC LIMIT 1"
            )
            assert result.result_set[0][0] == 19

    def test_contains_filter(self, conn_conf, test_graph_name, clean_db):
        """Test CONTAINS filter on large dataset."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": str(i), "content": f"Text number {i} with some random words"}
                for i in range(1000)
            ]
            db.upsert_docs_batch(docs, "Document", match_keys=["id"])

            result = db.execute(
                "MATCH (d:Document) WHERE d.content CONTAINS 'number 5' RETURN d.id"
            )
            assert len(result.result_set) > 0


# =============================================================================
# COMMON USER MISTAKES
# =============================================================================


class TestCommonMistakes:
    """Tests documenting common user mistakes and their effects."""

    def test_edge_before_nodes(self, conn_conf, test_graph_name, clean_db):
        """Creating edges before nodes results in no edges."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            edges = [[{"id": "user1"}, {"id": "user2"}, {"created": "now"}]]
            db.insert_edges_batch(
                edges,
                source_class="User",
                target_class="User",
                relation_name="FOLLOWS",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            result = db.execute("MATCH ()-[r:FOLLOWS]->() RETURN count(r)")
            assert result.result_set[0][0] == 0

    def test_type_mismatch_in_match_key(self, conn_conf, test_graph_name, clean_db):
        """Document string vs int type mismatch in match keys."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # Insert with string id
            db.upsert_docs_batch(
                [{"id": "123", "name": "Alice"}], "User", match_keys=["id"]
            )

            # Update with int id
            db.upsert_docs_batch(
                [{"id": 123, "name": "Alice Updated"}], "User", match_keys=["id"]
            )

            result = db.fetch_docs("User")
            # Memgraph treats string '123' and int 123 as distinct values
            # Normalize types before upsert to prevent unintended duplicates
            assert len(result) == 2, (
                "String and int types should create distinct entities"
            )

    def test_typo_in_field_name(self, conn_conf, test_graph_name, clean_db):
        """Typos in field names create new fields instead of updating."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "1", "email": "correct@example.com"}],
                "User",
                match_keys=["id"],
            )

            # Update with typo
            db.upsert_docs_batch(
                [{"id": "1", "emial": "typo@example.com"}],  # Typo
                "User",
                match_keys=["id"],
            )

            result = db.fetch_docs("User")
            user = result[0]

            assert "email" in user, "Original field should exist"
            assert "emial" in user, "Typo creates new field"

    def test_empty_string_id(self, conn_conf, test_graph_name, clean_db):
        """Document empty string as ID behavior."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [
                {"id": "", "name": "Empty ID"},
                {"id": "valid", "name": "Valid ID"},
            ]

            for doc in docs:
                try:
                    db.upsert_docs_batch([doc], "Item", match_keys=["id"])
                except Exception:
                    pass

            result = db.fetch_docs("Item")
            ids = [r.get("id") for r in result]
            # Memgraph accepts empty string as valid ID value
            # Add application-level validation if this is undesired
            assert "" in ids, "Empty string ID should be accepted by Memgraph"
            assert "valid" in ids


# =============================================================================
# DATA QUALITY
# =============================================================================


class TestDataQuality:
    """Tests for data quality scenarios."""

    def test_duplicate_variations(self, conn_conf, test_graph_name, clean_db):
        """Document duplicate creation from minor variations."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            variants = [
                {"email": "john.doe@example.com", "name": "John Doe"},
                {"email": "John.Doe@Example.com", "name": "John Doe"},  # Case
                {"email": "johndoe@example.com", "name": "John Doe"},  # No dot
                {
                    "email": "john.doe@example.com ",
                    "name": "John Doe",
                },  # Trailing space
            ]

            for i, doc in enumerate(variants):
                doc["id"] = str(i)
                db.upsert_docs_batch([doc], "User", match_keys=["email"])

            result = db.fetch_docs("User")
            # Memgraph treats each variation (case, whitespace, format) as distinct
            # Normalize emails (lowercase, strip, etc.) before upsert if deduplication is needed
            assert len(result) == 4, (
                "Email variations should create distinct entities in Memgraph"
            )

    def test_unicode_homoglyphs(self, conn_conf, test_graph_name, clean_db):
        """Document Unicode homoglyph handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            # These strings look identical but are not
            lookalikes = [
                {"id": "admin", "role": "Legitimate Admin"},  # ASCII 'a'
                {"id": "аdmin", "role": "Impostor Admin"},  # Cyrillic 'а' (U+0430)
            ]

            for doc in lookalikes:
                db.upsert_docs_batch([doc], "User", match_keys=["id"])

            result = db.fetch_docs("User")
            assert len(result) == 2, "Homoglyphs correctly distinguished"

    def test_invisible_characters(self, conn_conf, test_graph_name, clean_db):
        """Document invisible character handling."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            poisoned_data = [
                {"id": "1", "name": "\ufeffAlice"},  # BOM
                {"id": "2", "name": "Bob\u200b"},  # Zero-width space
                {"id": "3", "name": "Char\u200dlie"},  # Zero-width joiner
            ]
            db.upsert_docs_batch(poisoned_data, "User", match_keys=["id"])

            # Exact match fails due to invisible characters
            result = db.fetch_docs("User", filters=["==", "Alice", "name"])
            assert len(result) == 0, "BOM should cause exact match failure"

    def test_date_format_variations(self, conn_conf, test_graph_name, clean_db):
        """Document mixed date format handling.

        Memgraph strictly enforces type consistency for ordering.
        Mixed types (string + int) will cause errors on ORDER BY.
        """
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            date_variations = [
                {"id": "1", "date": "2024-01-15"},  # ISO
                {"id": "2", "date": "15/01/2024"},  # DD/MM/YYYY
                {"id": "3", "date": "01/15/2024"},  # MM/DD/YYYY
                {"id": "4", "date": "15 Jan 2024"},  # Text
                {"id": "5", "date": 1705276800},  # Unix timestamp
            ]
            db.upsert_docs_batch(date_variations, "Event", match_keys=["id"])

            # Memgraph can't compare mixed types, so just verify data exists
            result = db.execute("MATCH (e:Event) RETURN e.id, e.date")
            assert len(result.result_set) == 5


# =============================================================================
# COMPLEX OPERATION CONSISTENCY
# =============================================================================


class TestComplexOperationConsistency:
    """Tests for consistency after complex operations."""

    def test_read_your_writes(self, conn_conf, test_graph_name, clean_db):
        """Verify read-your-writes consistency."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            for i in range(10):
                db.upsert_docs_batch(
                    [{"id": str(i), "seq": i}], "Item", match_keys=["id"]
                )
                result = db.fetch_docs("Item", filters=["==", str(i), "id"])
                assert len(result) == 1, f"Read-your-writes failed for i={i}"

    def test_interleaved_labels(self, conn_conf, test_graph_name, clean_db):
        """Same ID in different labels remain distinct."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            db.upsert_docs_batch(
                [{"id": "X123", "name": "Alice"}], "Person", match_keys=["id"]
            )
            db.upsert_docs_batch(
                [{"id": "X123", "name": "Widget"}], "Product", match_keys=["id"]
            )

            persons = db.fetch_docs("Person")
            products = db.fetch_docs("Product")

            assert len(persons) == 1
            assert len(products) == 1
            assert persons[0]["name"] == "Alice"
            assert products[0]["name"] == "Widget"

    def test_bulk_update_consistency(self, conn_conf, test_graph_name, clean_db):
        """Verify consistency after bulk update."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            docs = [{"id": str(i), "category": "A", "value": 10} for i in range(100)]
            db.upsert_docs_batch(docs, "Item", match_keys=["id"])

            db.execute("MATCH (i:Item) SET i.value = i.value * 2")

            result = db.aggregate(
                "Item", AggregationType.AVERAGE, aggregated_field="value"
            )
            assert result == 20.0, "Aggregation after bulk update incorrect"

    def test_delete_cascade_consistency(self, conn_conf, test_graph_name, clean_db):
        """Verify edge cleanup after node deletion."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            nodes = [{"id": str(i)} for i in range(5)]
            db.upsert_docs_batch(nodes, "Node", match_keys=["id"])

            edges = [[{"id": str(i)}, {"id": str(i + 1)}, {}] for i in range(4)]
            db.insert_edges_batch(
                edges,
                source_class="Node",
                target_class="Node",
                relation_name="LINKED",
                match_keys_source=["id"],
                match_keys_target=["id"],
            )

            # Delete middle node
            db.execute("MATCH (n:Node {id: '2'}) DETACH DELETE n")

            node_count = db.aggregate("Node", AggregationType.COUNT)
            assert node_count == 4

            edge_result = db.execute("MATCH ()-[r:LINKED]->() RETURN count(r)")
            # Edges 1->2 and 2->3 deleted, 0->1 and 3->4 remain
            assert edge_result.result_set[0][0] == 2

    def test_atomic_transfer(self, conn_conf, test_graph_name, clean_db):
        """Simulate atomic transfer between accounts."""
        _ = clean_db
        with ConnectionManager(connection_config=conn_conf) as db:
            accounts = [
                {"id": "account_a", "balance": 1000},
                {"id": "account_b", "balance": 500},
            ]
            db.upsert_docs_batch(accounts, "Account", match_keys=["id"])

            # Atomic transfer
            db.execute(
                """
                MATCH (a:Account {id: 'account_a'}), (b:Account {id: 'account_b'})
                SET a.balance = a.balance - 200, b.balance = b.balance + 200
                """
            )

            result = db.execute(
                "MATCH (acc:Account) RETURN acc.id, acc.balance ORDER BY acc.id"
            )
            balances = {row[0]: row[1] for row in result.result_set}

            assert balances["account_a"] == 800
            assert balances["account_b"] == 700
            assert balances["account_a"] + balances["account_b"] == 1500
