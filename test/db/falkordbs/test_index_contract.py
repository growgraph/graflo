"""Live index catalogues and composite constraints on both FalkorDB engines."""

import time
from typing import Any

import pytest
from redis.exceptions import ResponseError

from graflo.architecture.schema import Schema
from graflo.connections.onto import FalkordbConfig
from graflo.db.cypher import cypher_name
from graflo.db.manager import ConnectionManager


def _schema(label: str) -> Schema:
    """A composite identity, a unique secondary key and a unique edge key."""
    return Schema.model_validate(
        {
            "metadata": {"name": "indexed_people"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": label,
                            "properties": ["tenant", "id", "email", "region", "active"],
                            "identity": ["tenant", "id"],
                        }
                    ]
                },
                "edge_config": {
                    "edges": [
                        {
                            "source": label,
                            "target": label,
                            "relation": "KNOWS",
                            "properties": ["code", "scope"],
                        }
                    ]
                },
            },
            "db_profile": {
                "db_flavor": "falkordb",
                "vertex_indexes": {
                    label: [
                        {"fields": ["region", "email"], "unique": True},
                        {"fields": ["active", "tenant"]},
                    ]
                },
                "edge_specs": [
                    {
                        "source": label,
                        "target": label,
                        "relation": "KNOWS",
                        "indexes": [{"fields": ["scope", "code"], "unique": True}],
                    }
                ],
            },
        }
    )


def _ready_constraints(db: Any) -> list[dict[str, Any]]:
    """Wait for asynchronous constraint creation, failing on a refused key."""
    deadline = time.monotonic() + 5
    while True:
        constraints = db.graph.list_constraints()
        assert all(row["status"] != "FAILED" for row in constraints), constraints
        if len(constraints) == 3 and all(
            row["status"] == "OPERATIONAL" for row in constraints
        ):
            return constraints
        if time.monotonic() >= deadline:
            pytest.fail(f"Constraints did not become operational: {constraints}")
        time.sleep(0.01)


@pytest.mark.parametrize("identity", [0, 0.0, False])
def test_composite_identity_and_declared_indexes(
    conn_conf: FalkordbConfig,
    test_graph_name: str,
    clean_db: None,
    identity: float,
) -> None:
    """Repeated DDL, false-like keys and composite uniqueness agree in 4.x/6.x."""
    label = test_graph_name
    schema = _schema(label)
    with ConnectionManager(connection_config=conn_conf) as db:
        for _ in range(2):
            db.define_indexes(schema)
        constraints = _ready_constraints(db)
        assert {
            (row["entitytype"], row["label"], frozenset(row["properties"]))
            for row in constraints
        } == {
            ("NODE", label, frozenset(["tenant", "id"])),
            ("NODE", label, frozenset(["region", "email"])),
            ("RELATIONSHIP", "KNOWS", frozenset(["scope", "code"])),
        }
        indexes = db.execute(
            "CALL db.indexes() YIELD label, properties, entitytype "
            "RETURN label, properties, entitytype"
        ).result_set
        assert {row[0]: set(row[1]) for row in indexes} == {
            label: {"tenant", "id", "region", "email", "active"},
            "KNOWS": {"scope", "code"},
        }

        db.upsert_docs_batch(
            [
                {
                    "tenant": "a",
                    "id": identity,
                    "email": "alice",
                    "region": "es",
                    "active": False,
                },
                {
                    "tenant": "b",
                    "id": identity,
                    "email": "alice",
                    "region": "fr",
                    "active": False,
                },
            ],
            label,
            ["tenant", "id"],
        )
        db.upsert_docs_batch(
            [{"tenant": "a", "id": identity, "active": True}],
            label,
            ["tenant", "id"],
        )
        assert sorted(db.fetch_docs(label), key=lambda row: row["tenant"]) == [
            {
                "tenant": "a",
                "id": identity,
                "email": "alice",
                "region": "es",
                "active": True,
            },
            {
                "tenant": "b",
                "id": identity,
                "email": "alice",
                "region": "fr",
                "active": False,
            },
        ]
        for props in [
            {"tenant": "a", "id": identity},
            {"tenant": "c", "id": identity, "email": "alice", "region": "es"},
        ]:
            with pytest.raises(ResponseError, match="[Uu]nique"):
                db.execute(
                    f"CREATE (n:{cypher_name(label)}) SET n = $props", props=props
                )
        assert len(db.fetch_docs(label)) == 2


def test_composite_edge_constraint_and_cleanup(
    conn_conf: FalkordbConfig, test_graph_name: str, clean_db: None
) -> None:
    """Edge uniqueness covers the field combination and is removed by cleanup."""
    label = test_graph_name
    with ConnectionManager(connection_config=conn_conf) as db:
        db.define_indexes(_schema(label))
        _ready_constraints(db)
        db.upsert_docs_batch(
            [{"tenant": "a", "id": "1"}, {"tenant": "b", "id": "1"}],
            label,
            ["tenant", "id"],
        )
        for scope in ["one", "two"]:
            db.insert_edges_batch(
                [
                    [
                        {"tenant": "a", "id": "1"},
                        {"tenant": "b", "id": "1"},
                        {"scope": scope, "code": "same"},
                    ]
                ],
                label,
                label,
                "KNOWS",
                ("tenant", "id"),
                ("tenant", "id"),
                relationship_merge_properties=("scope", "code"),
            )
        with pytest.raises(ResponseError, match="[Uu]nique"):
            db.execute(
                f"MATCH (a:{cypher_name(label)} {{tenant: 'a'}}), "
                f"(b:{cypher_name(label)} {{tenant: 'b'}}) "
                "CREATE (a)-[:KNOWS {scope: 'one', code: 'same'}]->(b)"
            )
        assert db.execute("MATCH ()-[r:KNOWS]->() RETURN count(r)").result_set == [[2]]
        db.delete_graph_structure(delete_all=True)
        assert db.graph.list_constraints() == []
        assert db.fetch_docs(label) == []
        db.define_indexes(_schema(label))
        _ready_constraints(db)
