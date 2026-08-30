"""Declaring a secondary identity must produce a real lookup index.

Endpoint resolution filters on secondary-identity fields, so without an index
the lookup degrades into a scan — and on NebulaGraph a tag index is required
for the property lookup to run at all.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from graflo.architecture.schema import Schema
from graflo.db.manager import ConnectionManager
from test.db.backends import config_for
from test.db.test_resolve_vertices import (
    BACKENDS,
    PROBE_SPACE,
    PROBE_VERTEX,
    SEED_DOCS,
)

SCHEMA_WITH_SECONDARY: dict[str, Any] = {
    "metadata": {"name": "gf_secondary_ix", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": PROBE_VERTEX,
                    "properties": [
                        {"name": "id", "type": "STRING"},
                        {"name": "isin", "type": "STRING"},
                        {"name": "org", "type": "STRING"},
                        {"name": "code", "type": "STRING"},
                    ],
                    "identity": ["id"],
                    "secondary_identities": [
                        {"name": "by_isin", "fields": ["isin"]},
                        ["org", "code"],
                    ],
                }
            ]
        },
        "edge_config": {"edges": []},
    },
}


def test_profile_declares_non_unique_indexes() -> None:
    """One non-unique index per declared secondary identity, compiled at init.

    Compilation happens in ``Schema.finish_init`` rather than
    ``resolve_db_aware`` so backends that define indexes without resolving a
    DB-aware view (PostgreSQL) still see them.
    """
    schema = Schema.model_validate(SCHEMA_WITH_SECONDARY)
    indexes = schema.db_profile.vertex_secondary_indexes(PROBE_VERTEX)
    assert [(list(ix.fields), ix.unique) for ix in indexes] == [
        (["isin"], False),
        (["org", "code"], False),
    ]


def test_compilation_is_idempotent_across_resolutions() -> None:
    """resolve_db_aware mutates the profile; repeating it must not accumulate."""
    from graflo.onto import DBType

    schema = Schema.model_validate(SCHEMA_WITH_SECONDARY)
    for flavor in (DBType.NEO4J, DBType.ARANGO, DBType.NEO4J):
        schema.resolve_db_aware(flavor)
    assert len(schema.db_profile.vertex_secondary_indexes(PROBE_VERTEX)) == 2


@pytest.fixture(scope="module")
def indexed_db(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Any]:
    """Connection whose probe vertex declares two secondary identities."""
    schema = Schema.model_validate(SCHEMA_WITH_SECONDARY)
    try:
        config = config_for(
            request.param, space=PROBE_SPACE, tmp_path_factory=tmp_path_factory
        )
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{request.param} config unavailable: {error}")

    try:
        manager = ConnectionManager(connection_config=config)
        db = manager.__enter__()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"backend unreachable: {error}")

    try:
        db.init_db(schema, recreate_schema=True)
        db.upsert_docs_batch(list(SEED_DOCS), PROBE_VERTEX, ["id"])
    except Exception as error:  # pragma: no cover - environment dependent
        manager.__exit__(None, None, None)
        pytest.skip(f"backend setup failed: {error}")

    try:
        yield db
    finally:
        try:
            db.delete_graph_structure(vertex_types=(PROBE_VERTEX,), delete_all=False)
        except Exception:
            pass
        manager.__exit__(None, None, None)


def _neo4j_probe_indexes(db) -> set[tuple[str, ...]]:
    return {
        tuple(row["properties"])
        for row in db.execute("SHOW INDEXES").data()
        if PROBE_VERTEX in str(row.get("labelsOrTypes")) and row.get("properties")
    }


def test_neo4j_creates_secondary_indexes() -> None:
    """Both the unary and the composite secondary identity become real indexes."""
    from graflo.connections.onto import Neo4jConfig

    config = Neo4jConfig.from_docker_env()
    if not config.database:
        config.database = "_system"
    try:
        manager = ConnectionManager(connection_config=config)
        db = manager.__enter__()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"neo4j unreachable: {error}")
    try:
        db.init_db(Schema.model_validate(SCHEMA_WITH_SECONDARY), recreate_schema=True)
        indexes = _neo4j_probe_indexes(db)
        assert ("isin",) in indexes
        assert ("org", "code") in indexes
    finally:
        try:
            db.execute(f"MATCH (n:{PROBE_VERTEX}) DETACH DELETE n")
        except Exception:
            pass
        manager.__exit__(None, None, None)


def test_postgres_creates_secondary_indexes() -> None:
    """PostgreSQL defines indexes during schema apply, not only vertex tables."""
    from graflo.connections.onto import PostgresConfig

    config = PostgresConfig.from_docker_env()
    if not config.database:
        config.database = "postgres"
    config.schema_name = "gf_secondary_ix"
    try:
        manager = ConnectionManager(connection_config=config)
        db = manager.__enter__()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"postgres unreachable: {error}")
    try:
        db.init_db(Schema.model_validate(SCHEMA_WITH_SECONDARY), recreate_schema=True)
        definitions = [
            row["indexdef"]
            for row in db.read(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = %s",
                (config.schema_name,),
            )
        ]
        assert any("(isin)" in d for d in definitions), definitions
        assert any("(org, code)" in d for d in definitions), definitions
    finally:
        try:
            db.execute(f'DROP SCHEMA IF EXISTS "{config.schema_name}" CASCADE')
        except Exception:
            pass
        manager.__exit__(None, None, None)


@pytest.mark.parametrize("indexed_db", BACKENDS, indirect=True)
def test_index_definition_is_accepted(indexed_db) -> None:
    """Every backend accepts the declared indexes and still resolves correctly."""
    resolved = indexed_db.resolve_vertices(
        PROBE_VERTEX, [{"isin": "US002"}], ("isin",), ("id",)
    )
    assert sorted(doc["id"] for doc in resolved[0]) == ["A2", "A3"]

    composite = indexed_db.resolve_vertices(
        PROBE_VERTEX, [{"org": "acme", "code": "x3"}], ("org", "code"), ("id",)
    )
    assert [doc["id"] for doc in composite[0]] == ["A3"]
