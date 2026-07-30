"""Database writer for pushing graph data to the target database.

Handles vertex upserts (including blank-node resolution), extra-weight
enrichment, and edge insertion.  All heavy DB I/O lives here so that
:class:`Caster` stays a lightweight orchestrator.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import uuid4

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import EdgeRuntime, Schema, SchemaDBAware
from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema.identity_digest import (
    ensure_digest_identities_on_docs,
)
from graflo.architecture.schema.identity_uuid import (
    ensure_assigned_uuids_on_docs,
    validate_uuid_typed_identity_fields,
)
from graflo.connections.onto import DBConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.endpoint_resolve import resolve_edge_endpoints
from graflo.onto import DBType

logger = logging.getLogger(__name__)


class DBWriter:
    """Push :class:`GraphContainer` data to the target graph database.

    The orchestrator (e.g. :class:`Caster`) must initialize ``schema`` and
    ``ingestion_model`` for the target database (``db_profile.db_flavor``,
    :meth:`Schema.finish_init`, :meth:`IngestionModel.finish_init`) before
    calling :meth:`write`; this class does not repeat that work on every batch.

    Attributes:
        schema: Schema configuration providing vertex/edge metadata.
        dry: When ``True`` no database mutations are performed.
        max_concurrent: Upper bound on concurrent DB operations (semaphore size).
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

            def _append() -> None:
                with ConnectionManager(connection_config=conn_conf) as db:
                    db.bulk_load_append(bulk_session_id, gc, self.schema)

            await asyncio.to_thread(_append)
            return

        resource = self.ingestion_model.fetch_resource(resource_name)

        await self._push_vertices(gc, conn_conf)
        self._resolve_blank_edges(gc, conn_conf)
        await self._enrich_extra_weights(gc, conn_conf, resource)
        await self._push_edges(gc, conn_conf, resource)

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
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def _push_one(vcol: str, data: list[dict]):
            async with semaphore:

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
                            writable,
                            vc.vertex_dbname(vcol),
                            vc.identity_fields(vcol),
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
        """Fetch extra-weight vertex data from the DB and attach to edges."""
        vc = self._db_aware_for(conn_conf).vertex_config

        def _sync():
            with ConnectionManager(connection_config=conn_conf) as db:
                for entry in resource.config.extra_weights:
                    edge = entry.edge
                    if not entry.vertex_weights:
                        continue
                    for weight in entry.vertex_weights:
                        if weight.name not in vc.vertex_set:
                            logger.error(f"{weight.name} not a valid vertex")
                            continue
                        index_fields = vc.identity_fields(weight.name)
                        if self.dry or weight.name not in gc.vertices:
                            continue
                        weights_per_item = db.fetch_present_documents(
                            class_name=vc.vertex_dbname(weight.name),
                            batch=gc.vertices[weight.name],
                            match_keys=index_fields,
                            keep_keys=weight.properties,
                        )
                        for j, item in enumerate(gc.linear):
                            weights = weights_per_item[j]
                            for ee in item[edge.edge_id]:
                                ee.update(
                                    {weight.cfield(k): v for k, v in weights[0].items()}
                                )

        await asyncio.to_thread(_sync)

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
        semaphore = asyncio.Semaphore(self.max_concurrent)

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
                return
            async with semaphore:

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
                            merge_props = tuple(mp)
                        if not self.dry:
                            data, relation_name = self._project_edge_docs_for_db(
                                docs=edge_docs,
                                relation=relation,
                                runtime=runtime,
                                conn_type=conn_conf.connection_type,
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
                                match_keys_source=source_keys,
                                match_keys_target=target_keys,
                                **edge_kw,
                            )

                await asyncio.to_thread(_sync)

        await asyncio.gather(
            *[_push_one(edge_id, docs) for edge_id, docs in gc.edges.items()]
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

        resolved, stats = resolve_edge_endpoints(
            db,
            docs,
            source_class=vertex_config.vertex_dbname(edge.source),
            target_class=vertex_config.vertex_dbname(edge.target),
            source_match_fields=source_fields,
            target_match_fields=target_fields,
            source_identity_fields=source_identity,
            target_identity_fields=target_identity,
            resolve_source=list(source_fields) != list(source_identity),
            resolve_target=list(target_fields) != list(target_identity),
            policy=policy,
        )
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

    def _db_aware_for(self, conn_conf: DBConfig) -> SchemaDBAware:
        """Return a cached :class:`SchemaDBAware` for *conn_conf*'s DB flavor."""
        flavor = conn_conf.connection_type
        if self._schema_db_aware is None or self._schema_db_aware_flavor != flavor:
            self._schema_db_aware = self.schema.resolve_db_aware(flavor)
            self._schema_db_aware_flavor = flavor
        return self._schema_db_aware

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
