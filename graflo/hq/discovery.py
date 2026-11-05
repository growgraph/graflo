"""What a connection offers to read, before any model exists.

:func:`discover` turns one connection into candidate connectors: the tables of
a database, the classes of a SPARQL endpoint, the topics of a Kafka cluster,
the files under a files connection. A pick of them becomes a bindings block
(:meth:`Discovery.bindings`), sampled and ingested like any other.

Files in one directory whose names differ only in runs of digits
(``part-0001.csv``, ``part-0002.csv``) are one candidate, read by a connector
whose ``regex`` lets those runs vary; a local directory and an S3 prefix are
listed alike.

A REST API cannot list its own endpoints, so its discovery is empty: its
connectors come from the models that bind them.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.bindings.connectors import (
    FileConnector,
    KafkaConnector,
    SparqlConnector,
    TableConnector,
)
from graflo.architecture.contract.bindings.core import AnyConnector, Bindings
from graflo.architecture.onto_sample import ForeignKeyHint
from graflo.connections.kinds import (
    AnyConnection,
    ConnectionKind,
    Role,
    connection_kind,
    roles,
)
from graflo.connections.onto import PostgresConfig, SparqlEndpointConfig
from graflo.connections.sources import (
    FilesConnConfig,
    KafkaConnConfig,
    RestApiConnConfig,
)

logger = logging.getLogger(__name__)

#: Candidates listed per connection unless the caller asks for another cap.
DEFAULT_DISCOVERY_LIMIT = 500

#: Seconds a Kafka cluster gets to answer a metadata request.
KAFKA_METADATA_TIMEOUT_S = 10.0


class DiscoveredColumn(ConfigBaseModel):
    """A column a table declares."""

    name: str
    type: str
    description: str | None = None
    is_pk: bool = False
    is_unique: bool = False
    nullable: bool = True


class DiscoveredResource(ConfigBaseModel):
    """One thing a connection offers to read, as the connector that reads it.

    Attributes:
        name: Resource name the pick is sampled under; unique in its discovery.
        connector: The connector that reads it; its ``name`` is ``name``.
        row_estimate: Approximate rows (tables) or instances (SPARQL classes).
        size_bytes: Total size of the files a file connector matches.
        file_count: Files a file connector matches.
        description: What the source says the thing is (a table comment).
        columns, primary_key, foreign_keys: What a table declares.
    """

    name: str
    connector: AnyConnector
    row_estimate: int | None = None
    size_bytes: int | None = None
    file_count: int | None = None
    description: str | None = None
    columns: list[DiscoveredColumn] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyHint] = Field(default_factory=list)


class Discovery(ConfigBaseModel):
    """The candidates one connection offers.

    Attributes:
        kind: Kind of the connection discovered.
        resources: Candidates, in the source's order.
        truncated: More candidates exist than were listed.
    """

    kind: ConnectionKind
    resources: list[DiscoveredResource] = Field(default_factory=list)
    truncated: bool = False

    def bindings(self, conn_proxy: str, names: list[str] | None = None) -> Bindings:
        """A bindings block reading the picked candidates through *conn_proxy*.

        See :func:`bindings_for`.

        Raises:
            ValueError: A name in *names* is not a candidate.
        """
        by_name = {resource.name: resource for resource in self.resources}
        wanted = list(by_name) if names is None else names
        unknown = [name for name in wanted if name not in by_name]
        if unknown:
            raise ValueError(f"not discovered here: {', '.join(unknown)}")
        return bindings_for(conn_proxy, [by_name[name] for name in wanted])


def bindings_for(conn_proxy: str, resources: Iterable[DiscoveredResource]) -> Bindings:
    """A bindings block reading *resources* through the connection *conn_proxy* names.

    Each resource is read by its connector, renamed to the resource's name so
    connector names are unique when resource names are, and wired to
    *conn_proxy*.
    """
    picked = list(resources)
    return Bindings(
        connectors=[
            resource.connector.model_copy(update={"name": resource.name})
            for resource in picked
        ],
        resource_connector=[
            {"resource": resource.name, "connector": resource.name}
            for resource in picked
        ],
        connector_connection=[
            {"connector": resource.name, "conn_proxy": conn_proxy}
            for resource in picked
        ],
    )


_UNSAFE = re.compile(r"[^0-9A-Za-z_]+")


def _resource_name(raw: str, taken: set[str]) -> str:
    """*raw* as a resource name: word characters only, unique among *taken*."""
    base = _UNSAFE.sub("_", raw).strip("_") or "resource"
    name, n = base, 2
    while name in taken:
        name, n = f"{base}_{n}", n + 1
    taken.add(name)
    return name


def discover(conn: AnyConnection, *, limit: int = DEFAULT_DISCOVERY_LIMIT) -> Discovery:
    """The candidates *conn* offers to read, at most *limit* of them.

    Raises:
        ValueError: *conn* is not read from (a graph database), or is a
            database flavour no connector reads.
    """
    if Role.SOURCE not in roles(conn):
        raise ValueError(
            f"a {getattr(conn, 'db_type', connection_kind(conn))} connection is "
            "written to, not read from"
        )
    if isinstance(conn, PostgresConfig):
        return _discover_postgres(conn, limit)
    if isinstance(conn, SparqlEndpointConfig):
        return _discover_sparql(conn, limit)
    if isinstance(conn, KafkaConnConfig):
        return _discover_kafka(conn, limit)
    if isinstance(conn, FilesConnConfig):
        return _discover_files(conn, limit)
    if isinstance(conn, RestApiConnConfig):
        return Discovery(kind=ConnectionKind.API)
    raise ValueError(f"nothing discovers a {type(conn).__name__}")


def _discover_postgres(conn: PostgresConfig, limit: int) -> Discovery:
    from graflo.hq.graph_engine import GraphEngine

    tables = GraphEngine().introspect(conn).raw_tables
    taken: set[str] = set()
    resources = []
    for table in tables[:limit]:
        name = _resource_name(table.name, taken)
        resources.append(
            DiscoveredResource(
                name=name,
                connector=TableConnector(
                    name=name,
                    table_name=table.name,
                    schema_name=table.schema_name or None,
                ),
                row_estimate=(
                    table.row_count_estimate
                    if table.row_count_estimate is not None
                    and table.row_count_estimate >= 0
                    else None
                ),
                description=table.description or None,
                columns=[
                    DiscoveredColumn(
                        name=column.name,
                        type=column.type,
                        description=column.description or None,
                        is_pk=column.is_pk,
                        is_unique=column.is_unique,
                        nullable=column.is_nullable.upper() == "YES",
                    )
                    for column in table.columns
                ],
                primary_key=list(table.primary_key),
                foreign_keys=[
                    ForeignKeyHint(
                        field=fk.column,
                        references_resource=fk.references_table,
                        references_field=fk.references_column,
                    )
                    for fk in table.foreign_keys
                ],
            )
        )
    return Discovery(
        kind=ConnectionKind.DATABASE,
        resources=resources,
        truncated=len(tables) > limit,
    )


#: Classes with instances, the most populated first.
_CLASSES_QUERY = """SELECT ?class (COUNT(?s) AS ?n)
WHERE {{ ?s a ?class . FILTER(isIRI(?class)) }}
GROUP BY ?class ORDER BY DESC(?n) ?class LIMIT {limit}"""


def sparql_select(conn: SparqlEndpointConfig, query: str) -> list[dict[str, Any]]:
    """The bindings of a ``SELECT`` *query* against *conn*'s endpoint."""
    from SPARQLWrapper import JSON, SPARQLWrapper

    client = SPARQLWrapper(conn.query_endpoint)
    client.setQuery(query)
    client.setReturnFormat(JSON)
    if conn.username and conn.password:
        client.setCredentials(conn.username, conn.password)
    result: Any = client.query().convert()
    return list(result.get("results", {}).get("bindings", []))


def _local_name(iri: str) -> str:
    return re.split(r"[#/]", iri.rstrip("#/"))[-1] or iri


def _discover_sparql(conn: SparqlEndpointConfig, limit: int) -> Discovery:
    rows = sparql_select(conn, _CLASSES_QUERY.format(limit=limit + 1))
    taken: set[str] = set()
    resources = []
    for row in rows[:limit]:
        iri = str(row["class"]["value"])
        name = _resource_name(_local_name(iri), taken)
        count = row.get("n", {}).get("value")
        resources.append(
            DiscoveredResource(
                name=name,
                connector=SparqlConnector(name=name, rdf_class=iri),
                row_estimate=int(count) if count is not None else None,
            )
        )
    return Discovery(
        kind=ConnectionKind.DATABASE,
        resources=resources,
        truncated=len(rows) > limit,
    )


def kafka_topics(conn: KafkaConnConfig) -> list[str]:
    """The topics *conn*'s cluster holds, internal ones (``__``) left out."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(dict[str, Any](conn.to_consumer_config()))
    metadata = admin.list_topics(timeout=KAFKA_METADATA_TIMEOUT_S)
    return sorted(topic for topic in metadata.topics if not topic.startswith("__"))


def _discover_kafka(conn: KafkaConnConfig, limit: int) -> Discovery:
    topics = kafka_topics(conn)
    taken: set[str] = set()
    resources = []
    for topic in topics[:limit]:
        name = _resource_name(topic, taken)
        resources.append(
            DiscoveredResource(
                name=name,
                connector=KafkaConnector(
                    name=name, topics=[topic], group_id=f"graflo-{name}"
                ),
            )
        )
    return Discovery(
        kind=ConnectionKind.STREAM,
        resources=resources,
        truncated=len(topics) > limit,
    )


def _readable(path: str) -> bool:
    """Whether a file's name says how to read it."""
    from graflo.data_source.chunker import ChunkerFactory

    try:
        ChunkerFactory._guess_chunker_type(Path(path))
    except ValueError:
        return False
    return True


_DIGITS = re.compile(r"\d+")


def _name_pattern(name: str) -> str:
    """The regex a file's name stands for when each run of digits may vary."""
    return "^" + r"\d+".join(re.escape(part) for part in _DIGITS.split(name)) + "$"


def _discover_files(conn: FilesConnConfig, limit: int) -> Discovery:
    from graflo.data_source.files import FileEntry, walk

    groups: dict[tuple[str, str], list[FileEntry]] = {}
    for entry in walk(conn):
        if _readable(entry.relative):
            path = PurePosixPath(entry.relative)
            groups.setdefault((str(path.parent), _name_pattern(path.name)), []).append(
                entry
            )
    taken: set[str] = set()
    resources = []
    for (parent, pattern), members in list(groups.items())[:limit]:
        first = PurePosixPath(members[0].relative)
        stem = first.name.split(".", 1)[0]
        if len(members) > 1:
            stem = _DIGITS.sub("", stem)
        else:
            pattern = f"^{re.escape(first.name)}$"
        raw = "/".join(part for part in (parent, stem) if part and part != ".")
        name = _resource_name(raw, taken)
        sizes = [member.size for member in members]
        resources.append(
            DiscoveredResource(
                name=name,
                connector=FileConnector(name=name, regex=pattern, sub_path=parent),
                size_bytes=None if None in sizes else sum(s or 0 for s in sizes),
                file_count=len(members),
            )
        )
    return Discovery(
        kind=ConnectionKind.FILES,
        resources=resources,
        truncated=len(groups) > limit,
    )


__all__ = [
    "DEFAULT_DISCOVERY_LIMIT",
    "DiscoveredColumn",
    "DiscoveredResource",
    "Discovery",
    "bindings_for",
    "discover",
    "kafka_topics",
    "sparql_select",
]
