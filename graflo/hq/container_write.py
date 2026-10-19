"""Write a graph container to a database by identity upsert, without a resource.

For producers that assemble a :class:`GraphContainer` themselves rather than
casting rows through an ingestion resource. The schema is defined on the
target first, and an existing definition is reused. Edges whose endpoint is
known only by a secondary identity (:class:`PendingEdge`) are looked up in the
target after the container is written, so an endpoint the container itself
carries is found. Everything is upserted by identity through :class:`DBWriter`, so writing the same container twice leaves the
database as it was after the first write. The file backend is the exception:
it appends.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from pydantic import Field

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.sanitize import with_physical_names
from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.graph_types.identifiers import EdgeId
from graflo.architecture.graph_types.pending import PendingEdge
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.connections.onto import DBConfig
from graflo.db.conn import SchemaExistsError
from graflo.db.manager import ConnectionManager
from graflo.hq.db_writer import DBWriter
from graflo.hq.endpoint_resolve import resolve_edge_endpoints
from graflo.hq.graph_engine import GraphEngine
from graflo.onto import EndpointAmbiguityPolicy


class ContainerWriteReport(ConfigBaseModel):
    """What :func:`write_container` sent to the database."""

    target: str = Field(description="Connection type written to.")
    schema_defined: bool = Field(
        description="False when an existing definition was reused."
    )
    vertices: dict[str, int] = Field(description="Documents per vertex type.")
    edges: dict[str, int] = Field(
        description="Edges per edge, keyed ``source-relation-target``."
    )
    lookups_resolved: int = Field(
        default=0, description="Pending edges whose endpoints were found."
    )
    lookups_unresolved: int = Field(
        default=0, description="Pending edges dropped: no match, or an ambiguous one."
    )


def edge_label(edge_id: EdgeId) -> str:
    """``source-relation-target``, the key of :attr:`ContainerWriteReport.edges`."""
    return f"{edge_id[0]}-{edge_id[2] or ''}-{edge_id[1]}"


def define_schema(schema: Schema, conn_conf: DBConfig, recreate: bool) -> bool:
    """Define *schema* on the target, or reuse its definition; True when defined."""
    manifest = GraphManifest(
        graph_schema=schema, ingestion_model=IngestionModel(resources=[])
    )
    try:
        GraphEngine(target_db_flavor=conn_conf.connection_type).define_schema(
            manifest=manifest, target_db_config=conn_conf, recreate_schema=recreate
        )
    except SchemaExistsError:
        schema.db_profile.db_flavor = conn_conf.connection_type
        schema.finish_init()
        return False
    return True


def _resolve_pending(
    schema: Schema,
    conn_conf: DBConfig,
    pending: Sequence[PendingEdge],
    ambiguity: EndpointAmbiguityPolicy,
) -> tuple[dict[EdgeId, list[Any]], int, int]:
    """Edges by id with looked-up endpoints, how many resolved, how many dropped."""
    flavor = conn_conf.connection_type
    named = with_physical_names(schema, flavor)
    keys = PhysicalKeys(named.db_profile)
    vertex_config = named.resolve_db_aware(flavor).vertex_config

    # One lookup per edge and pair of match-key sets.
    groups: dict[tuple, list[PendingEdge]] = defaultdict(list)
    for edge in pending:
        groups[
            (
                edge.edge_id,
                tuple(sorted(edge.source.match or {})),
                tuple(sorted(edge.target.match or {})),
            )
        ].append(edge)

    resolved: dict[EdgeId, list[Any]] = defaultdict(list)
    written = dropped = 0
    with ConnectionManager(connection_config=conn_conf) as db:
        for (edge_id, source_match, target_match), edges in groups.items():
            source, target = edge_id[0], edge_id[1]
            source_identity = list(vertex_config.identity_fields(source))
            target_identity = list(vertex_config.identity_fields(target))
            docs = [
                (
                    keys.vertex_doc(source, e.source.match or e.source.identity or {}),
                    keys.vertex_doc(target, e.target.match or e.target.identity or {}),
                    e.props,
                )
                for e in edges
            ]
            triples, stats = resolve_edge_endpoints(
                db,
                docs,
                source_class=vertex_config.vertex_dbname(source),
                target_class=vertex_config.vertex_dbname(target),
                source_match_fields=keys.vertex_fields(
                    source, list(source_match) or source_identity
                ),
                target_match_fields=keys.vertex_fields(
                    target, list(target_match) or target_identity
                ),
                source_identity_fields=keys.vertex_fields(source, source_identity),
                target_identity_fields=keys.vertex_fields(target, target_identity),
                resolve_source=bool(source_match),
                resolve_target=bool(target_match),
                policy=ambiguity,
            )
            for triple in triples:
                resolved[edge_id].append(
                    (
                        keys.logical_vertex_doc(source, triple[0]),
                        keys.logical_vertex_doc(target, triple[1]),
                        triple[2],
                    )
                )
            written += stats.written
            dropped += stats.dropped
    return resolved, written, dropped


async def write_container(
    schema: Schema,
    container: GraphContainer,
    conn_conf: DBConfig,
    *,
    pending: Sequence[PendingEdge] = (),
    recreate_schema: bool = False,
    ambiguity: EndpointAmbiguityPolicy = "skip",
) -> ContainerWriteReport:
    """Define *schema* on the target and upsert *container* by identity.

    Args:
        schema: The schema every document conforms to. Never mutated.
        container: Vertices and edges keyed by primary identity. Never mutated.
        conn_conf: The target database.
        pending: Edges with an endpoint known only by a secondary identity;
            each is looked up in the target once *container* is written, and
            written when found.
        recreate_schema: Drop and redefine the schema instead of reusing it.
        ambiguity: What a secondary identity matching several vertices does.
            ``skip`` (the default) drops the edge rather than guess.

    Returns:
        ContainerWriteReport: counts of what was sent and what was looked up.
    """
    schema = schema.model_copy(deep=True)
    defined = await asyncio.to_thread(define_schema, schema, conn_conf, recreate_schema)
    to_write = GraphContainer(
        vertices={
            name: [dict(doc) for doc in docs]
            for name, docs in container.vertices.items()
        },
        edges={edge_id: list(triples) for edge_id, triples in container.edges.items()},
    )

    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    resolved: dict[EdgeId, list[Any]] = {}
    resolved_count = unresolved_count = 0
    writer = DBWriter(schema=schema, ingestion_model=ingestion_model)
    try:
        await writer.write(gc=to_write, conn_conf=conn_conf, resource_name=None)
        if pending:
            resolved, resolved_count, unresolved_count = await asyncio.to_thread(
                _resolve_pending, schema, conn_conf, pending, ambiguity
            )
            if resolved:
                await writer.write(
                    gc=GraphContainer(edges=dict(resolved)),
                    conn_conf=conn_conf,
                    resource_name=None,
                )
    finally:
        writer.close()

    edges: dict[str, int] = {}
    for source in (to_write.edges, resolved):
        for edge_id, triples in source.items():
            label = edge_label(edge_id)
            edges[label] = edges.get(label, 0) + len(triples)
    return ContainerWriteReport(
        target=str(conn_conf.connection_type),
        schema_defined=defined,
        vertices={name: len(docs) for name, docs in to_write.vertices.items()},
        edges=edges,
        lookups_resolved=resolved_count,
        lookups_unresolved=unresolved_count,
    )
