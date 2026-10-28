"""A declared index is a lookup index unless it says ``unique: true``."""

from __future__ import annotations

from typing import Any, cast

import pytest

from graflo.architecture.graph_types.index_config import Index
from graflo.architecture.schema import CoreSchema, GraphMetadata, Schema
from graflo.architecture.schema.edge import EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.db.neo4j.conn import Neo4jConnection
from graflo.onto import DBType


def _schema() -> Schema:
    return Schema(
        metadata=GraphMetadata(name="people"),
        core_schema=CoreSchema(
            vertex_config=VertexConfig(
                vertices=[
                    Vertex(
                        name="person",
                        properties=[Field(name="id"), Field(name="email")],
                        identity=["id"],
                    )
                ]
            ),
            edge_config=EdgeConfig(edges=[]),
        ),
    )


def test_a_declared_index_is_not_unique_by_default() -> None:
    assert Index(fields=["email"]).unique is False
    assert (
        Index.model_validate({"fields": ["email"]}).db_form(DBType.ARANGO)["unique"]
        is False
    )


def test_the_identity_index_stays_unique() -> None:
    vertex_config = _schema().resolve_db_aware(DBType.ARANGO).vertex_config

    assert vertex_config.index("person").unique is True


class _Recorder:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, query: str) -> None:
        self.queries.append(query)


def test_a_declared_relationship_index_is_a_plain_index_on_neo4j() -> None:
    fake = _Recorder()

    Neo4jConnection._add_index(
        cast(Any, fake), "KNOWS", Index(fields=["since"]), is_vertex_index=False
    )
    Neo4jConnection._add_index(
        cast(Any, fake),
        "KNOWS",
        Index(fields=["since"], unique=True),
        is_vertex_index=False,
    )

    plain, constraint = fake.queries
    assert plain.startswith("CREATE INDEX")
    assert constraint.startswith("CREATE CONSTRAINT")


# -- vertex uniqueness on Neo4j ----------------------------------------------------


def _client_error(code: str) -> Exception:
    from neo4j.exceptions import Neo4jError

    return Neo4jError._hydrate_neo4j(code=code, message="refused")


class _Refusing(_Recorder):
    """Refuses every constraint with *code*; indexes go through."""

    def __init__(self, code: str) -> None:
        super().__init__()
        self._code = code

    def execute(self, query: str) -> None:
        if query.startswith("CREATE CONSTRAINT"):
            raise _client_error(self._code)
        super().execute(query)


def test_a_single_field_vertex_identity_is_a_uniqueness_constraint() -> None:
    fake = _Recorder()

    Neo4jConnection._add_index(
        cast(Any, fake), "person", Index(fields=["id"], unique=True)
    )

    assert fake.queries == [
        (
            "CREATE CONSTRAINT person_id IF NOT EXISTS "
            "FOR (x:person) REQUIRE x.id IS UNIQUE;"
        )
    ]


def test_a_composite_vertex_identity_is_a_composite_constraint() -> None:
    fake = _Recorder()

    Neo4jConnection._add_index(
        cast(Any, fake), "person", Index(fields=["name", "born"], unique=True)
    )

    assert fake.queries == [
        (
            "CREATE CONSTRAINT person_name_born IF NOT EXISTS "
            "FOR (x:person) REQUIRE (x.name, x.born) IS UNIQUE;"
        )
    ]


def test_a_refused_composite_identity_falls_back_to_an_index(caplog) -> None:
    fake = _Refusing("Neo.ClientError.Statement.UnsupportedAdministrationCommand")

    Neo4jConnection._add_index(
        cast(Any, fake),
        "person",
        Index(fields=["name", "born"], unique=True),
        is_identity=True,
    )

    assert fake.queries == [
        (
            "CREATE INDEX person_name_born IF NOT EXISTS FOR (x:person) "
            "ON (x.name, x.born);"
        )
    ]
    assert "not enforced as unique" in caplog.text


def test_a_refused_single_field_constraint_raises() -> None:
    import pytest

    fake = _Refusing("Neo.ClientError.Schema.ConstraintCreationFailed")

    with pytest.raises(Exception, match="refused"):
        Neo4jConnection._add_index(
            cast(Any, fake), "person", Index(fields=["id"], unique=True)
        )


def test_a_non_unique_vertex_index_stays_an_index() -> None:
    fake = _Recorder()

    Neo4jConnection._add_index(cast(Any, fake), "person", Index(fields=["email"]))

    (query,) = fake.queries
    assert query.startswith("CREATE INDEX person_email")


def test_a_refused_declared_composite_constraint_raises() -> None:
    """Only the identity falls back: a declared unique index is never weakened."""
    from neo4j.exceptions import ClientError

    fake = _Refusing("Neo.ClientError.Statement.UnsupportedAdministrationCommand")

    with pytest.raises(ClientError):
        Neo4jConnection._add_index(
            cast(Any, fake), "person", Index(fields=["name", "born"], unique=True)
        )
