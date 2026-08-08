"""Pathological graph topologies and state-corruption cases.

Backend-agnostic Cypher cases, shared by the Memgraph and FalkorDB suites.

These are mixins, not tests: the module is outside any ``test_*.py`` so pytest
never collects them directly. Each backend's ``test_edge_cases.py`` subclasses
them, which is what supplies the ``conn_conf`` / ``test_graph_name`` /
``clean_db`` fixtures -- both backend conftests expose that same trio.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from graflo.db.manager import ConnectionManager


class PathologicalGraphsCases:
    """Tests for pathological graph structures."""

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


class StateCorruptionCases:
    """Test state handling and connection management."""

    def test_operations_after_close(self, conn_conf, test_graph_name):
        """Attempt operations after connection is closed."""
        db = ConnectionManager(connection_config=conn_conf)
        db.__enter__()
        db.__exit__(None, None, None)

        # These should fail gracefully
        with pytest.raises(Exception):
            fetch_docs = cast(Any, db).fetch_docs
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
