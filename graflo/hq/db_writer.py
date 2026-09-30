"""Database writer for pushing graph data to the target database.

Handles vertex upserts (including blank-node resolution), extra-weight
enrichment, and edge insertion.  All heavy DB I/O lives here so that
:class:`Caster` stays a lightweight orchestrator.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any
from uuid import uuid4

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.evolution.sanitize import (
    materialize_physical_schema,
    with_physical_names,
)
from graflo.architecture.graph_types import EdgeId, GraphContainer, Weight
from graflo.architecture.schema import EdgeRuntime, Schema, SchemaDBAware
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.identity_digest import (
    ensure_digest_identities_on_docs,
)
from graflo.architecture.schema.identity_uuid import (
    ensure_assigned_uuids_on_docs,
    validate_uuid_typed_identity_fields,
)
from graflo.architecture.schema.physical_keys import PhysicalKeys
from graflo.connections.onto import DBConfig
from graflo.db.conn import ConnectionCapability
from graflo.db.manager import ConnectionManager
from graflo.hq.endpoint_resolve import resolve_edge_endpoints
from graflo.onto import DBType

logger = logging.getLogger(__name__)

# Backends whose upsert primitive is atomic per key under concurrency:
# Postgres INSERT .. ON CONFLICT, Arango UPSERT with OPTIONS {exclusive: true}
# (collection-level write lock), and the keyed REST/NGQL upserts of TigerGraph
# and Nebula cannot create duplicate vertices for the same identity. Cypher
# MERGE, by contrast, is check-then-create per transaction: two concurrent
# writers merging the same key can BOTH create, leaving duplicate nodes unless
# a uniqueness constraint exists (which graflo does not require). Flavors not
# listed here get a per-collection write lock when batches or sources overlap.
_CONCURRENT_UPSERT_SAFE_FLAVORS = frozenset(
    {DBType.POSTGRES, DBType.ARANGO, DBType.TIGERGRAPH, DBType.NEBULA}
)

#: Targets written by one operation at a time, whatever ``max_concurrent`` says.
#: The chunked-file backend numbers a new chunk from the index it read when the
#: connection opened, and rewrites the whole index on close, so two connections
#: writing at once overwrite each other's chunks and index entries.
_SINGLE_WRITER_FLAVORS = frozenset({DBType.GRAFLO_BACKEND})


def _weight_source_fields(weight: Weight) -> list[str]:
    """Vertex fields a weight reads: ``fields`` then ``map`` keys, deduplicated."""
    return list(dict.fromkeys([*weight.fields, *weight.map]))


def _weight_attributes(weight: Weight, doc: dict[str, Any]) -> dict[str, Any]:
    """The edge attributes *weight* derives from one vertex document.

    Same projection as the pipeline's edge render: ``fields`` are written under
    :meth:`Weight.cfield`, ``map`` entries under the attribute they name.
    """
    attributes = {weight.cfield(k): doc[k] for k in weight.fields if k in doc}
    attributes.update({q: doc[k] for k, q in weight.map.items() if k in doc})
    return attributes


def _document_edges(item: dict[Any, list], edge: Edge) -> list[tuple]:
    """The ``(source, target, attributes)`` triples of *edge* one document produced.

    A document keys its edges by the relation it resolved, so an *edge* that
    names none covers every relation between its endpoints.
    """
    triples: list[tuple] = []
    for key, docs in item.items():
        if not isinstance(key, tuple) or key[:2] != (edge.source, edge.target):
            continue
        if edge.relation is None or key[2] == edge.relation:
            triples.extend(docs)
    return triples


class DBWriter:
    """Push :class:`GraphContainer` data to the target graph database.

    The orchestrator (e.g. :class:`Caster`) must initialize ``schema`` and
    ``ingestion_model`` for the target database (``db_profile.db_flavor``,
    :meth:`Schema.finish_init`, :meth:`IngestionModel.finish_init`) before
    calling :meth:`write`; this class does not repeat that work on every batch.

    Concurrency contract: one instance may serve concurrent :meth:`write`
    calls (sibling batches in flight) from a single event loop. All per-batch
    mutation happens on the caller's ``gc``; instance state is limited to the
    cached db-aware schema (pre-warm it via :meth:`_db_aware_for` before
    fanning out) and one shared semaphore, so ``max_concurrent`` bounds DB
    operations across every in-flight batch, not per call.

    Naming contract: documents in ``gc`` carry logical property names, and
    stay that way. Storage, relation and property names the target stores
    differently come from the profile (completed by
    :func:`~graflo.architecture.evolution.sanitize.with_physical_names`), and
    every document and field list is translated to them at the backend call,
    then translated back for anything the backend returns.

    Attributes:
        schema: Schema configuration providing vertex/edge metadata.
        dry: When ``True`` no database mutations are performed.
        max_concurrent: Upper bound on concurrent DB operations (semaphore size).
            A file-backend target is written by one operation at a time.
    """

    def __init__(
        self,
        schema: Schema,
        ingestion_model: IngestionModel,
        *,
        dry: bool = False,
        max_concurrent: int = 1,
    ):
        self.schema = schema
        self.ingestion_model = ingestion_model
        self.dry = dry
        self.max_concurrent = max_concurrent
        self._schema_db_aware: SchemaDBAware | None = None
        self._schema_db_aware_flavor: DBType | None = None
        self._named_schema: Schema | None = None
        self._keys: PhysicalKeys | None = None
        self._semaphore: asyncio.Semaphore | None = None
        self._semaphore_loop: asyncio.AbstractEventLoop | None = None
        self._semaphore_bound: int | None = None
        self._collection_locks: dict[str, asyncio.Lock] = {}
        self._collection_locks_loop: asyncio.AbstractEventLoop | None = None
        self._reported_undeclared_edges: set[tuple] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def write(
        self,
        gc: GraphContainer,
        conn_conf: DBConfig,
        resource_name: str | None,
        *,
        bulk_session_id: str | None = None,
    ) -> None:
        """Push *gc* to the database (vertices, extra weights, then edges).

        When *bulk_session_id* is provided, appends rows using the connection's
        native bulk interface instead of using per-record writes.

        .. note::
            *gc* is mutated in-place for the REST path: blank-vertex keys are
            updated and blank edges are extended after the vertex round-trip.
            The bulk path does not support blank vertices or ``extra_weights``.
        """
        if bulk_session_id:
            self._validate_bulk_resource(resource_name)
            if self.dry:
                logger.debug(
                    "Dry run: would append batch to bulk session %s",
                    bulk_session_id,
                )
                return

            keys = self._keys_for(conn_conf)
            stored_gc = keys.container(gc, self.schema.core_schema.edge_config)
            stored_schema = materialize_physical_schema(self._named_for(conn_conf))

            def _append() -> None:
                with ConnectionManager(connection_config=conn_conf) as db:
                    db.bulk_load_append(bulk_session_id, stored_gc, stored_schema)

            await asyncio.to_thread(_append)
            return

        # A container that no resource produced (``migrate_graph`` writes an
        # exported graph through an empty ingestion model) has no resource to
        # consult, and needs none: extra weights and endpoint selections are
        # resource-level ingestion features.
        resource = (
            self.ingestion_model.fetch_resource(resource_name)
            if resource_name is not None or self.ingestion_model.resources
            else None
        )

        await self._push_vertices(gc, conn_conf)
        # O(blank vertices x edges x documents) of pure Python. Called straight from
        # the event loop it stalled every other coroutine — including the prefetch
        # that is supposed to overlap with the write.
        await asyncio.to_thread(self._resolve_blank_edges, gc, conn_conf)
        if resource is not None:
            await self._enrich_extra_weights(gc, conn_conf, resource)
        await self._push_edges(gc, conn_conf, resource)

    def delete_vertices(
        self, conn_conf: DBConfig, vertex: str, key_docs: list[dict[str, Any]]
    ) -> None:
        """Remove vertices of *vertex*, with every edge incident to them.

        *key_docs* carry the vertex's identity fields, under logical names; a
        document missing one removes nothing. Nothing is removed in a dry run.

        Raises:
            ValueError: If the target does not support deleting vertices.
        """
        ConnectionManager.require(conn_conf, ConnectionCapability.INSTANCE_DELETE)
        if self.dry:
            return
        vc = self._db_aware_for(conn_conf).vertex_config
        keys = self._keys_for(conn_conf)
        match_keys = tuple(keys.vertex_fields(vertex, vc.identity_fields(vertex)))
        with ConnectionManager(connection_config=conn_conf) as db:
            db.delete_vertices(
                vc.vertex_dbname(vertex), keys.vertex_docs(vertex, key_docs), match_keys
            )

    def delete_edges(
        self,
        conn_conf: DBConfig,
        edge_id: EdgeId,
        endpoints: list[tuple[dict[str, Any], dict[str, Any]]],
    ) -> None:
        """Remove the edges *edge_id* between the given ``(source, target)`` pairs.

        Endpoints carry their vertices' identity fields, under logical names.
        Nothing is removed in a dry run.

        Raises:
            ValueError: If the target does not support deleting edges, or the
                schema does not declare *edge_id*.
        """
        ConnectionManager.require(conn_conf, ConnectionCapability.INSTANCE_DELETE)
        core_ec = self.schema.core_schema.edge_config
        if edge_id not in core_ec:
            raise ValueError(f"Edge {edge_id} is not declared in the schema")
        if self.dry:
            return
        schema_db = self._db_aware_for(conn_conf)
        vc = schema_db.vertex_config
        keys = self._keys_for(conn_conf)
        edge = core_ec.edge_for(edge_id)
        runtime = schema_db.edge_config.runtime(edge)
        _, relation_name = self._project_edge_docs_for_db(
            docs=[],
            relation=edge.relation,
            runtime=runtime,
            conn_type=conn_conf.connection_type,
        )
        source_keys = vc.identity_fields(edge.source)
        target_keys = vc.identity_fields(edge.target)
        stored = [
            (keys.vertex_doc(edge.source, source), keys.vertex_doc(edge.target, target))
            for source, target in endpoints
        ]
        with ConnectionManager(connection_config=conn_conf) as db:
            db.delete_edges(
                vc.vertex_dbname(edge.source),
                vc.vertex_dbname(edge.target),
                relation_name,
                stored,
                tuple(keys.vertex_fields(edge.source, source_keys)),
                tuple(keys.vertex_fields(edge.target, target_keys)),
                collection_name=runtime.storage_name(),
            )

    def _validate_bulk_resource(self, resource_name: str | None) -> None:
        if resource_name is None:
            return
        resource = self.ingestion_model.fetch_resource(resource_name)
        if resource.config.extra_weights:
            raise ValueError(
                "Native bulk ingest does not support resources with extra_weights "
                "(those require DB round-trips). Use REST ingest or disable extra_weights."
            )

    # ------------------------------------------------------------------
    # Vertices
    # ------------------------------------------------------------------

    async def _push_vertices(self, gc: GraphContainer, conn_conf: DBConfig) -> None:
        """Upsert all vertex collections in *gc*.

        Pre-write hooks depend on :attr:`~graflo.architecture.schema.vertex.Vertex.identity_mode`:
        ``hash`` vertices get deterministic SHA256 ids;
        ``assigned`` vertices get idempotent uuid4 fill (usually already minted at assemble);
        ``blank`` vertices get random UUIDs;
        ``natural`` vertices upsert directly on ``identity`` fields (one or many).
        UUID-typed natural identity fields are validated when present.
        """
        vc = self._db_aware_for(conn_conf).vertex_config
        keys = self._keys_for(conn_conf)

        async def _push_one(vcol: str, data: list[dict]):
            async with AsyncExitStack() as stack:
                await self._acquire_write_slot(stack, conn_conf, vc.vertex_dbname(vcol))

                def _sync():
                    with ConnectionManager(connection_config=conn_conf) as db:
                        if vcol in vc.hash_identity_vertices:
                            self._assign_hash_identity_ids(
                                vcol=vcol, data=data, conn_conf=conn_conf
                            )
                        elif vcol in vc.assigned_vertices:
                            self._assign_assigned_vertex_ids(
                                vcol=vcol, data=data, conn_conf=conn_conf
                            )
                        elif vcol in vc.blank_vertices:
                            self._assign_blank_vertex_ids(
                                vcol=vcol, data=data, conn_conf=conn_conf
                            )
                        else:
                            self._validate_uuid_natural_identity(
                                vcol=vcol, data=data, conn_conf=conn_conf
                            )
                        writable = self._drop_unkeyed_docs(
                            vcol=vcol, data=data, conn_conf=conn_conf
                        )
                        db.upsert_docs_batch(
                            keys.vertex_docs(vcol, writable),
                            vc.vertex_dbname(vcol),
                            keys.vertex_fields(vcol, vc.identity_fields(vcol)),
                            update_keys="doc",
                            filter_uniques=True,
                            dry=self.dry,
                        )
                        return vcol, None

                return await asyncio.to_thread(_sync)

        results = await asyncio.gather(
            *[_push_one(vcol, data) for vcol, data in gc.vertices.items()]
        )

        for vcol, result in results:
            if result is not None:
                gc.vertices[vcol] = result

    def _drop_unkeyed_docs(
        self, vcol: str, data: list[dict], conn_conf: DBConfig
    ) -> list[dict]:
        """Drop documents that carry none of their vertex's identity fields.

        Such a document cannot be upserted: with no key at all, every backend
        either invents one or folds the whole batch onto a single keyless
        vertex. It normally means the resource references the vertex rather than
        owning it — declare ``lookup_only`` on that step to say so explicitly.

        Runs after the blank/assigned/hash hooks, so generated identities count.
        """
        vc = self._db_aware_for(conn_conf).vertex_config
        identity_fields = vc.identity_fields(vcol)
        if not identity_fields:
            return data

        writable = [
            doc
            for doc in data
            if any(doc.get(field) is not None for field in identity_fields)
        ]
        dropped = len(data) - len(writable)
        if dropped:
            logger.warning(
                "Skipped %s '%s' document(s) with no identity value for %s; "
                "they cannot be upserted. Mark the step lookup_only if the "
                "resource only references this vertex.",
                dropped,
                vcol,
                identity_fields,
            )
        return writable

    def _assign_blank_vertex_ids(
        self, vcol: str, data: list[dict], conn_conf: DBConfig
    ) -> None:
        """Assign deterministic in-memory IDs to blank vertices before persistence."""
        vc = self._db_aware_for(conn_conf).vertex_config
        identity_fields = vc.identity_fields(vcol)
        default_field = "_key" if conn_conf.connection_type == DBType.ARANGO else "id"
        preferred_field = identity_fields[0] if identity_fields else default_field

        for doc in data:
            current_value = doc.get(preferred_field)
            if current_value is None or current_value == "":
                generated = str(uuid4())
                doc[preferred_field] = generated
                if default_field != preferred_field and default_field not in doc:
                    doc[default_field] = generated

    def _assign_assigned_vertex_ids(
        self, vcol: str, data: list[dict], conn_conf: DBConfig
    ) -> None:
        """Idempotent uuid4 fill for assigned vertices (assemble-time mint is primary)."""
        vc = self._db_aware_for(conn_conf).vertex_config
        identity_fields = vc.identity_fields(vcol)
        default_field = "_key" if conn_conf.connection_type == DBType.ARANGO else "id"
        preferred_field = identity_fields[0] if identity_fields else default_field
        ensure_assigned_uuids_on_docs(
            data,
            preferred_field=preferred_field,
            arango_key_mirror=(
                conn_conf.connection_type == DBType.ARANGO and preferred_field != "_key"
            ),
        )
        if default_field != preferred_field:
            for doc in data:
                if default_field not in doc:
                    doc[default_field] = doc[preferred_field]

    def _validate_uuid_natural_identity(
        self, vcol: str, data: list[dict], conn_conf: DBConfig
    ) -> None:
        """Validate UUID-typed natural identity fields; do not invent values."""
        vc = self._db_aware_for(conn_conf).vertex_config
        vertex = vc.logical._get_vertex_by_name(vcol)
        for doc in data:
            validate_uuid_typed_identity_fields(doc, vertex)

    def _assign_hash_identity_ids(
        self, vcol: str, data: list[dict], conn_conf: DBConfig
    ) -> None:
        """Idempotent digest-identity fill for hash- and funnel-mode vertices.

        Identities are normally materialized at assemble time
        (``ensure_digest_identities_in_acc_vertex``); this is the safety net for
        docs that reach the writer another way. Never overwrites a value, so an
        assemble-time key survives. Docs where no branch fires keep an empty
        identity and are dropped by ``_drop_unkeyed_docs``.
        """
        vc = self._db_aware_for(conn_conf).vertex_config
        vertex = vc.logical._get_vertex_by_name(vcol)
        identity_fields = vc.identity_fields(vcol)
        default_field = "_key" if conn_conf.connection_type == DBType.ARANGO else "id"
        preferred_field = identity_fields[0] if identity_fields else default_field

        ensure_digest_identities_on_docs(data, vertex, preferred_field=preferred_field)
        if default_field != preferred_field:
            for doc in data:
                value = doc.get(preferred_field)
                if value is not None and value != "" and default_field not in doc:
                    doc[default_field] = value

    # ------------------------------------------------------------------
    # Blank-edge resolution
    # ------------------------------------------------------------------

    def _resolve_blank_edges(self, gc: GraphContainer, conn_conf: DBConfig) -> None:
        """Extend edge lists for blank vertices after their keys are resolved."""
        vc = self._db_aware_for(conn_conf).vertex_config
        for vcol in vc.blank_vertices:
            for edge_id, _ in self.schema.core_schema.edge_config.items():  # noqa: PERF102
                vfrom, vto, _relation = edge_id
                if vcol == vfrom or vcol == vto:
                    if vfrom not in gc.vertices or vto not in gc.vertices:
                        continue
                    if edge_id not in gc.edges:
                        gc.edges[edge_id] = []
                    source_docs = gc.vertices[vfrom]
                    target_docs = gc.vertices[vto]
                    source_id_fields = vc.identity_fields(vfrom)
                    target_id_fields = vc.identity_fields(vto)
                    shared_fields = [
                        f for f in source_id_fields if f in target_id_fields
                    ]

                    if shared_fields:
                        target_by_key: dict[tuple, list[dict]] = {}
                        for target_doc in target_docs:
                            key = tuple(target_doc.get(f) for f in shared_fields)
                            if any(item is None for item in key):
                                continue
                            target_by_key.setdefault(key, []).append(target_doc)
                        for source_doc in source_docs:
                            key = tuple(source_doc.get(f) for f in shared_fields)
                            if any(item is None for item in key):
                                continue
                            for target_doc in target_by_key.get(key, []):
                                gc.edges[edge_id].append((source_doc, target_doc, {}))
                    else:
                        gc.edges[edge_id].extend(
                            (x, y, {}) for x, y in zip(source_docs, target_docs)
                        )

    # ------------------------------------------------------------------
    # Extra weights
    # ------------------------------------------------------------------

    async def _enrich_extra_weights(
        self, gc: GraphContainer, conn_conf: DBConfig, resource
    ) -> None:
        """Copy stored vertex fields onto the edges of the documents that name the vertex.

        For each ``extra_weights`` rule, the weight vertex a document produced
        is read back from the database, and its fields become attributes of
        that document's edges. A document that produced several takes the first
        one the database holds; one that produced none, or none the database
        holds, keeps its edges as cast.
        """
        rules = [
            (entry.edge, weight)
            for entry in resource.config.extra_weights
            for weight in entry.vertex_weights
        ]
        if self.dry or not rules:
            return
        vc = self._db_aware_for(conn_conf).vertex_config

        def _sync() -> None:
            with ConnectionManager(connection_config=conn_conf) as db:
                for edge, weight in rules:
                    if weight.name not in vc.vertex_set:
                        logger.error(f"{weight.name} not a valid vertex")
                        continue
                    self._enrich_edges_from_stored_vertex(
                        db, gc, conn_conf, edge=edge, weight=weight
                    )

        await asyncio.to_thread(_sync)

    def _enrich_edges_from_stored_vertex(
        self,
        db: Any,
        gc: GraphContainer,
        conn_conf: DBConfig,
        *,
        edge: Edge,
        weight: Weight,
    ) -> None:
        """Apply one ``extra_weights`` rule to every document of *gc*."""
        name = weight.name
        source_fields = _weight_source_fields(weight)
        if name is None or not source_fields:
            return
        vc = self._db_aware_for(conn_conf).vertex_config
        keys = self._keys_for(conn_conf)

        # One lookup for the batch; `owners` says which document asked for each key.
        key_docs: list[dict[str, Any]] = []
        owners: list[int] = []
        pending: dict[int, list[tuple]] = {}
        for position, item in enumerate(gc.linear):
            triples = _document_edges(item, edge)
            if not triples:
                continue
            pending[position] = triples
            for doc in item.get(name, ()):
                key_docs.append(doc)
                owners.append(position)
        if not key_docs:
            return

        resolved = db.resolve_vertices(
            vc.vertex_dbname(name),
            keys.vertex_docs(name, key_docs),
            tuple(keys.vertex_fields(name, vc.identity_fields(name))),
            tuple(keys.vertex_fields(name, source_fields)),
        )
        for index in sorted(resolved):
            matches = resolved[index]
            if not matches:
                continue
            # Popped, so a document's first stored vertex is the one it takes.
            triples = pending.pop(owners[index], None)
            if triples is None:
                continue
            attributes = _weight_attributes(
                weight, keys.logical_vertex_doc(name, matches[0])
            )
            for triple in triples:
                # The attribute dict is the one `gc.edges` holds, so the edge
                # write sees it.
                triple[2].update(attributes)

    # ------------------------------------------------------------------
    # Edges
    # ------------------------------------------------------------------

    async def _push_edges(
        self,
        gc: GraphContainer,
        conn_conf: DBConfig,
        resource: Any | None = None,
    ) -> None:
        """Insert all edges in *gc*.

        Each key in ``gc.edges`` is a concrete ``(source, target, relation)``
        triple produced by the extraction pipeline.  We look up the matching
        schema :class:`Edge` for each key (trying an exact match first, then a
        ``relation=None`` schema entry for dynamic-relation edges) and fire one
        async task per key — one DB write per concrete relation, no inner loop.

        Endpoints declared by a secondary identity are resolved to their primary
        identity first, so the write itself stays a plain primary-key operation
        on every backend.
        """
        schema_db = self._db_aware_for(conn_conf)
        vc = schema_db.vertex_config
        ec = schema_db.edge_config
        core_ec = self.schema.core_schema.edge_config
        keys = self._keys_for(conn_conf)

        def _schema_edge_for(edge_id: tuple) -> Edge | None:
            """Return the schema Edge for a gc edge key, or None if not declared."""
            if edge_id in core_ec:
                return core_ec.edge_for(edge_id)
            # Dynamic-relation edges: schema declares (source, target, None).
            null_id = (edge_id[0], edge_id[1], None)
            if null_id in core_ec:
                return core_ec.edge_for(null_id)
            return None

        endpoint_match_for = self._endpoint_match_lookup(resource)

        async def _push_one(edge_id: tuple, docs: list) -> None:
            edge = _schema_edge_for(edge_id)
            if edge is None:
                self._report_undeclared_edge(edge_id, len(docs))
                return
            async with AsyncExitStack() as stack:
                # Cypher relationship MERGE has the same concurrent
                # check-then-create race as node MERGE; lock per relation store.
                await self._acquire_write_slot(
                    stack, conn_conf, f"edge:{ec.runtime(edge).storage_name()}"
                )

                def _sync() -> None:
                    _, _, relation = edge_id
                    with ConnectionManager(connection_config=conn_conf) as db:
                        runtime = ec.runtime(edge)
                        endpoint_match = endpoint_match_for(edge_id)
                        source_keys = tuple(vc.identity_fields(edge.source))
                        target_keys = tuple(vc.identity_fields(edge.target))
                        edge_docs = docs
                        if endpoint_match is not None:
                            edge_docs = self._resolve_endpoints(
                                db=db,
                                docs=docs,
                                edge=edge,
                                edge_id=edge_id,
                                match=endpoint_match,
                                vertex_config=vc,
                            )
                            if not edge_docs:
                                return
                        merge_props: tuple[str, ...] | None = None
                        mp = ec.relationship_merge_property_names(edge)
                        if mp:
                            merge_props = tuple(keys.edge_fields(edge.edge_id, mp))
                        if not self.dry:
                            data, relation_name = self._project_edge_docs_for_db(
                                docs=edge_docs,
                                relation=relation,
                                runtime=runtime,
                                conn_type=conn_conf.connection_type,
                            )
                            data = keys.edge_triples(
                                edge.edge_id, edge.source, edge.target, data
                            )
                            edge_kw: dict = {
                                "filter_uniques": False,
                                "dry": self.dry,
                                "collection_name": runtime.storage_name(),
                            }
                            if conn_conf.connection_type in (
                                DBType.NEO4J,
                                DBType.FALKORDB,
                                DBType.MEMGRAPH,
                            ):
                                if merge_props is not None:
                                    edge_kw["relationship_merge_properties"] = (
                                        merge_props
                                    )
                            elif (
                                conn_conf.connection_type == DBType.ARANGO
                                and self.ingestion_model.edges_on_duplicate == "upsert"
                            ):
                                edge_kw["on_duplicate"] = "upsert"
                                if merge_props is not None:
                                    edge_kw["uniq_weight_fields"] = list(merge_props)
                            db.insert_edges_batch(
                                docs_edges=data,
                                source_class=vc.vertex_dbname(edge.source),
                                target_class=vc.vertex_dbname(edge.target),
                                relation_name=relation_name,
                                match_keys_source=tuple(
                                    keys.vertex_fields(edge.source, source_keys)
                                ),
                                match_keys_target=tuple(
                                    keys.vertex_fields(edge.target, target_keys)
                                ),
                                **edge_kw,
                            )

                await asyncio.to_thread(_sync)

        await asyncio.gather(
            *[_push_one(edge_id, docs) for edge_id, docs in gc.edges.items()]
        )

    def _report_undeclared_edge(self, edge_id: tuple, count: int) -> None:
        """Say, once per edge id, that its edges are not written."""
        if edge_id in self._reported_undeclared_edges:
            return
        self._reported_undeclared_edges.add(edge_id)
        logger.warning(
            "Edge %s is not written: the schema does not declare it (%s in this "
            "batch; reported once). Declare the edge, or name the declared "
            "relation on the step that produces it.",
            edge_id,
            count,
        )

    def _endpoint_match_lookup(self, resource: Any | None) -> Any:
        """Return a lookup for a resource's endpoint identity selections.

        Only edges that select a secondary identity have an entry, so edges
        matched on the primary identity never touch the resolution path.
        """
        registry = getattr(resource, "edge_derivation", None) if resource else None
        if registry is None:
            return lambda edge_id: None
        return registry.endpoint_match_for

    def _resolve_endpoints(
        self,
        *,
        db: Any,
        docs: list,
        edge: Edge,
        edge_id: tuple,
        match: Any,
        vertex_config: Any,
    ) -> list:
        """Map secondary-identity endpoints to primary identities before writing."""
        source_fields = vertex_config.match_fields(edge.source, match.source)
        target_fields = vertex_config.match_fields(edge.target, match.target)
        source_identity = vertex_config.identity_fields(edge.source)
        target_identity = vertex_config.identity_fields(edge.target)
        policy = match.on_ambiguous or self.ingestion_model.endpoints_on_ambiguous
        keys = self._keys

        # Resolution reads the database, so it runs on stored names; the
        # resolved endpoints come back to logical names for the rest of the write.
        stored_docs = docs
        if keys is not None and keys.active:
            stored_docs = [
                (
                    keys.vertex_doc(edge.source, doc[0]),
                    keys.vertex_doc(edge.target, doc[1]),
                    *doc[2:],
                )
                for doc in docs
            ]

        def _stored(vertex: str, fields: list[str]) -> list[str]:
            return keys.vertex_fields(vertex, fields) if keys is not None else fields

        resolved, stats = resolve_edge_endpoints(
            db,
            stored_docs,
            source_class=vertex_config.vertex_dbname(edge.source),
            target_class=vertex_config.vertex_dbname(edge.target),
            source_match_fields=_stored(edge.source, source_fields),
            target_match_fields=_stored(edge.target, target_fields),
            source_identity_fields=_stored(edge.source, source_identity),
            target_identity_fields=_stored(edge.target, target_identity),
            resolve_source=list(source_fields) != list(source_identity),
            resolve_target=list(target_fields) != list(target_identity),
            policy=policy,
        )
        if keys is not None and keys.active:
            resolved = [
                (
                    keys.logical_vertex_doc(edge.source, doc[0]),
                    keys.logical_vertex_doc(edge.target, doc[1]),
                    *doc[2:],
                )
                for doc in resolved
            ]
        if stats.has_findings():
            logger.warning(
                "Edge %s endpoint resolution (policy=%s): %s",
                edge_id,
                policy,
                stats.summary(),
            )
        else:
            logger.debug("Edge %s endpoint resolution: %s", edge_id, stats.summary())
        return resolved

    def _db_semaphore(self, conn_conf: DBConfig) -> asyncio.Semaphore:
        """Shared semaphore so ``max_concurrent`` bounds the whole run.

        Created lazily per event loop: a writer reused across separate
        ``asyncio.run`` calls must not carry a semaphore bound to a closed loop.
        """
        loop = asyncio.get_running_loop()
        bound = (
            1
            if conn_conf.connection_type in _SINGLE_WRITER_FLAVORS
            else self.max_concurrent
        )
        if (
            self._semaphore is None
            or self._semaphore_loop is not loop
            or self._semaphore_bound != bound
        ):
            self._semaphore = asyncio.Semaphore(bound)
            self._semaphore_loop = loop
            self._semaphore_bound = bound
        return self._semaphore

    async def _acquire_write_slot(
        self, stack: AsyncExitStack, conn_conf: DBConfig, collection: str
    ) -> None:
        """Enter the semaphore and, when the backend's upsert can race with
        itself (Cypher MERGE has no cross-transaction atomicity), a
        per-collection lock so the same collection is written by one batch at a
        time while distinct collections still proceed in parallel."""
        await stack.enter_async_context(self._db_semaphore(conn_conf))
        if conn_conf.connection_type in _CONCURRENT_UPSERT_SAFE_FLAVORS:
            return
        loop = asyncio.get_running_loop()
        if self._collection_locks_loop is not loop:
            self._collection_locks = {}
            self._collection_locks_loop = loop
        lock = self._collection_locks.setdefault(collection, asyncio.Lock())
        await stack.enter_async_context(lock)

    def _db_aware_for(self, conn_conf: DBConfig) -> SchemaDBAware:
        """Return a cached :class:`SchemaDBAware` for *conn_conf*'s DB flavor.

        Resolved over the schema with its physical names filled in, so the
        storage and relation names written to are the ones DDL declared, and
        the stored property names are known for :meth:`_keys_for`.
        """
        flavor = conn_conf.connection_type
        if self._schema_db_aware is None or self._schema_db_aware_flavor != flavor:
            named = with_physical_names(self.schema, flavor)
            self._named_schema = named
            self._keys = PhysicalKeys(named.db_profile)
            self._schema_db_aware = named.resolve_db_aware(flavor)
            self._schema_db_aware_flavor = flavor
        return self._schema_db_aware

    def _named_for(self, conn_conf: DBConfig) -> Schema:
        """The schema with physical names filled in for *conn_conf*'s flavor."""
        self._db_aware_for(conn_conf)
        assert self._named_schema is not None
        return self._named_schema

    def _keys_for(self, conn_conf: DBConfig) -> PhysicalKeys:
        """Logical <-> stored key translation for *conn_conf*'s flavor."""
        self._db_aware_for(conn_conf)
        assert self._keys is not None
        return self._keys

    def _project_edge_docs_for_db(
        self,
        *,
        docs: list,
        relation: str | None,
        runtime: EdgeRuntime,
        conn_type: DBType,
    ) -> tuple[list, str | None]:
        """Project logical edge docs into DB-specific relation representation."""
        if conn_type != DBType.TIGERGRAPH:
            return docs, relation

        relation_name = runtime.relation_name
        relation_field = runtime.effective_relation_field
        if not runtime.store_extracted_relation_as_weight or relation_field is None:
            return docs, relation_name

        # TigerGraph stores dynamic extracted relation as an edge attribute while
        # keeping the edge type stable.
        projected: list = []
        for source_doc, target_doc, weight in docs:
            next_weight = dict(weight)
            if relation is not None:
                next_weight[relation_field] = relation
            projected.append((source_doc, target_doc, next_weight))
        return projected, relation_name
