"""Every kind of connection as one typed union, and the roles each kind plays.

A connection is a database, a REST API, a stream or a place files live. Each
config carries a ``kind`` (and a database its ``db_type``), so a stored
connection always parses back to the same class, and what it can be used for
follows from the config alone, without contacting anything.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, TypeAlias

from pydantic import Discriminator, Field, Tag, TypeAdapter

from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.connections.onto import (
    SOURCE_DATABASES,
    TARGET_DATABASES,
    ArangoConfig,
    DBConfig,
    FalkordbConfig,
    GraphDBConfig,
    MemgraphConfig,
    NebulaConfig,
    Neo4jConfig,
    PostgresConfig,
    SparqlEndpointConfig,
    TigergraphConfig,
)
from graflo.connections.sources import (
    FilesConnConfig,
    KafkaConnConfig,
    RestApiConnConfig,
)


class ConnectionKind(StrEnum):
    """What a connection reaches."""

    DATABASE = "database"
    API = "api"
    STREAM = "stream"
    FILES = "files"


class Role(StrEnum):
    """What a connection is used as: read from, or written to."""

    SOURCE = "source"
    TARGET = "target"


def _database_tag(value: Any) -> str | None:
    if isinstance(value, dict):
        db_type = value.get("db_type")
        return str(db_type) if db_type is not None else None
    db_type = getattr(value, "db_type", None)
    return str(db_type) if db_type is not None else None


DatabaseConnection: TypeAlias = Annotated[
    Annotated[ArangoConfig, Tag("arango")]
    | Annotated[Neo4jConfig, Tag("neo4j")]
    | Annotated[TigergraphConfig, Tag("tigergraph")]
    | Annotated[FalkordbConfig, Tag("falkordb")]
    | Annotated[MemgraphConfig, Tag("memgraph")]
    | Annotated[NebulaConfig, Tag("nebula")]
    | Annotated[GraFloBackendConfig, Tag("graflo_backend")]
    | Annotated[PostgresConfig, Tag("postgres")]
    | Annotated[SparqlEndpointConfig, Tag("sparql")]
    | Annotated[GraphDBConfig, Tag("graphdb")],
    Discriminator(_database_tag),
]
"""A database connection, discriminated by ``db_type``."""

Connection: TypeAlias = Annotated[
    DatabaseConnection | RestApiConnConfig | KafkaConnConfig | FilesConnConfig,
    Field(discriminator="kind"),
]
"""Any connection, discriminated by ``kind`` (a database further by ``db_type``)."""

AnyConnection: TypeAlias = (
    DBConfig | RestApiConnConfig | KafkaConnConfig | FilesConnConfig
)
"""The runtime type of a parsed :data:`Connection`."""

#: The JSON schema and validator of :data:`Connection`, for callers that publish it.
CONNECTION_ADAPTER: TypeAdapter[AnyConnection] = TypeAdapter(Connection)

_CONFIG_BY_KIND: dict[
    str, type[RestApiConnConfig | KafkaConnConfig | FilesConnConfig]
] = {
    ConnectionKind.API: RestApiConnConfig,
    ConnectionKind.STREAM: KafkaConnConfig,
    ConnectionKind.FILES: FilesConnConfig,
}


def parse_connection(data: dict[str, Any]) -> AnyConnection:
    """The connection *data* describes.

    ``kind`` is required. A database also needs ``db_type`` and is read by
    :meth:`DBConfig.from_dict`, which accepts its URI spelt in parts.

    Raises:
        ValueError: The kind is missing or unknown, or the config does not
            validate for it.
    """
    kind = data.get("kind")
    if kind is None:
        raise ValueError(
            "a connection needs a 'kind': one of "
            + ", ".join(k.value for k in ConnectionKind)
        )
    if kind == ConnectionKind.DATABASE:
        return DBConfig.from_dict(data)
    config_class = _CONFIG_BY_KIND.get(str(kind))
    if config_class is None:
        raise ValueError(
            f"unknown connection kind {kind!r}; expected one of "
            + ", ".join(k.value for k in ConnectionKind)
        )
    return config_class.model_validate(data)


def connection_kind(conn: AnyConnection) -> ConnectionKind:
    """The kind of *conn*."""
    return ConnectionKind(conn.kind)


def roles(conn: AnyConnection) -> frozenset[Role]:
    """What *conn* can be used as, from its kind and flavour alone.

    An API, a stream and a files connection are read from. A database is read
    from when a connector reads its rows (:data:`SOURCE_DATABASES`) and written
    to when it holds a graph (:data:`TARGET_DATABASES`); PostgreSQL is both.
    """
    if not isinstance(conn, DBConfig):
        return frozenset({Role.SOURCE})
    found: set[Role] = set()
    if conn.db_type in SOURCE_DATABASES:
        found.add(Role.SOURCE)
    if conn.db_type in TARGET_DATABASES:
        found.add(Role.TARGET)
    return frozenset(found)


__all__ = [
    "CONNECTION_ADAPTER",
    "AnyConnection",
    "Connection",
    "ConnectionKind",
    "DatabaseConnection",
    "Role",
    "connection_kind",
    "parse_connection",
    "roles",
]
