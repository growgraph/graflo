"""FalkorDB index DDL: each field indexed, uniqueness as a native constraint."""

from __future__ import annotations

from typing import Any, cast

import pytest

from graflo.architecture.graph_types.index_config import Index
from graflo.architecture.schema import Schema
from graflo.db.falkordb.conn import FalkordbConnection


class _Graph:
    """Records constraint calls; refuses them with *refusal* when given."""

    def __init__(
        self,
        refusal: str | None = None,
        constraints: list[dict[str, object]] | None = None,
    ) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._refusal = refusal
        self._constraints = constraints or []

    def create_node_unique_constraint(self, label: str, *properties: str) -> None:
        self._create("node", label, *properties)

    def create_edge_unique_constraint(self, relation: str, *properties: str) -> None:
        self._create("edge", relation, *properties)

    def _create(self, *call: str) -> None:
        if self._refusal is not None:
            raise RuntimeError(self._refusal)
        self.calls.append(("create", *call))

    def list_constraints(self) -> list[dict[str, object]]:
        return self._constraints

    def drop_node_unique_constraint(self, label: str, *properties: str) -> None:
        self.calls.append(("drop node", label, *properties))

    def drop_edge_unique_constraint(self, relation: str, *properties: str) -> None:
        self.calls.append(("drop edge", relation, *properties))


def _falkordb(graph: _Graph) -> tuple[Any, list[str]]:
    sent: list[str] = []
    conn = cast(Any, object.__new__(FalkordbConnection))
    conn.graph = graph
    conn.execute = lambda query, **_: sent.append(query)
    return conn, sent


def _people() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "people"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "person",
                            "properties": ["tenant", "id", "name"],
                            "identity": ["tenant", "id"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {
                "db_flavor": "falkordb",
                "vertex_indexes": {"person": [{"fields": ["name", "tenant"]}]},
            },
        }
    )


def test_a_composite_identity_is_indexed_and_constrained_unique() -> None:
    graph = _Graph()
    db, sent = _falkordb(graph)
    schema = _people()

    db.define_vertex_indexes(schema.core_schema.vertex_config, schema)

    assert sent == [
        "CREATE INDEX FOR (n:person) ON (n.tenant)",
        "CREATE INDEX FOR (n:person) ON (n.id)",
        "CREATE INDEX FOR (n:person) ON (n.name)",
        "CREATE INDEX FOR (n:person) ON (n.tenant)",
    ]
    assert graph.calls == [("create", "node", "person", "tenant", "id")]


def test_a_unique_edge_index_is_an_edge_constraint() -> None:
    graph = _Graph()
    db, sent = _falkordb(graph)

    db._add_index(
        "KNOWS", Index(fields=["since", "role"], unique=True), is_vertex_index=False
    )

    assert sent == [
        "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.since)",
        "CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.role)",
    ]
    assert graph.calls == [("create", "edge", "KNOWS", "since", "role")]


def test_a_refused_identity_constraint_only_warns(caplog) -> None:
    db, _ = _falkordb(_Graph(refusal="pending constraint failed"))

    db._add_index("person", Index(fields=["id"], unique=True), is_identity=True)

    assert "not enforced as unique" in caplog.text


def test_a_refused_declared_constraint_raises() -> None:
    db, _ = _falkordb(_Graph(refusal="pending constraint failed"))

    with pytest.raises(RuntimeError, match="pending constraint failed"):
        db._add_index("person", Index(fields=["code"], unique=True))


def test_an_existing_constraint_is_kept() -> None:
    db, _ = _falkordb(_Graph(refusal="Constraint already exists"))

    db._add_index("person", Index(fields=["code"], unique=True))


def test_deleting_a_label_drops_its_unique_constraints() -> None:
    graph = _Graph(
        constraints=[
            {
                "type": "UNIQUE",
                "label": "person",
                "properties": ["tenant", "id"],
                "entitytype": "NODE",
                "status": "OPERATIONAL",
            },
            {
                "type": "MANDATORY",
                "label": "person",
                "properties": ["id"],
                "entitytype": "NODE",
                "status": "OPERATIONAL",
            },
            {
                "type": "UNIQUE",
                "label": "org",
                "properties": ["code"],
                "entitytype": "NODE",
                "status": "OPERATIONAL",
            },
        ]
    )
    db, _ = _falkordb(graph)
    db.client = object()

    db.delete_graph_structure(vertex_types=["person"])

    assert graph.calls == [("drop node", "person", "tenant", "id")]
