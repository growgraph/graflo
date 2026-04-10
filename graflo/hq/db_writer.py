"""Database writer for pushing graph data to the target database.

Handles vertex upserts (including blank-node resolution), extra-weight
enrichment, and edge insertion.  All heavy DB I/O lives here so that
:class:`Caster` stays a lightweight orchestrator.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from graflo.architecture.schema.edge import Edge
from graflo.architecture.schema import EdgeRuntime, SchemaDBAware
from graflo.architecture.contract.declarations.ingestion_model import IngestionModel
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import Schema
from graflo.db import ConnectionManager
from graflo.db import DBConfig
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
        await self._push_edges(gc, conn_conf)

    def _validate_bulk_resource(self, resource_name: str | None) -> None:
        if resource_name is None:
            return
        resource = self.ingestion_model.fetch_resource(resource_name)
        if resource.extra_weights:
            raise ValueError(
                "Native bulk ingest does not support resources with extra_weights "
                "(those require DB round-trips). Use REST ingest or disable extra_weights."
            )

    # ------------------------------------------------------------------
    # Vertices
    # ------------------------------------------------------------------

    async def _push_vertices(self, gc: GraphContainer, conn_conf: DBConfig) -> None:
        """Upsert all vertex collections in *gc*, resolving blank nodes."""
        vc = self._db_aware_for(conn_conf).vertex_config
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def _push_one(vcol: str, data: list[dict]):
            async with semaphore:

                def _sync():
                    with ConnectionManager(connection_config=conn_conf) as db:
                        if vcol in vc.blank_vertices:
                            self._assign_blank_vertex_ids(
                                vcol=vcol, data=data, conn_conf=conn_conf
                            )
                        db.upsert_docs_batch(
                            data,
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

    # ------------------------------------------------------------------
    # Blank-edge resolution
    # ------------------------------------------------------------------

    def _resolve_blank_edges(self, gc: GraphContainer, conn_conf: DBConfig) -> None:
        """Extend edge lists for blank vertices after their keys are resolved."""
        vc = self._db_aware_for(conn_conf).vertex_config
        for vcol in vc.blank_vertices:
            for edge_id, _edge in self.schema.core_schema.edge_config.items():
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
                for entry in resource.extra_weights:
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

    async def _push_edges(self, gc: GraphContainer, conn_conf: DBConfig) -> None:
        """Insert all edges in *gc*."""
        schema_db = self._db_aware_for(conn_conf)
        vc = schema_db.vertex_config
        ec = schema_db.edge_config
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def _push_one(edge_id: tuple, edge: Edge):
            async with semaphore:

                def _sync():
                    with ConnectionManager(connection_config=conn_conf) as db:
                        runtime = ec.runtime(edge)
                        merge_props: tuple[str, ...] | None = None
                        mp = ec.relationship_merge_property_names(edge)
                        if mp:
                            merge_props = tuple(mp)
                        for ee in gc.loop_over_relations(edge_id):
                            _, _, relation = ee
                            if not self.dry:
                                data, relation_name = self._project_edge_docs_for_db(
                                    docs=gc.edges[ee],
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
                                elif conn_conf.connection_type == DBType.ARANGO:
                                    if (
                                        self.ingestion_model.edges_on_duplicate
                                        == "upsert"
                                    ):
                                        edge_kw["on_duplicate"] = "upsert"
                                        if merge_props is not None:
                                            edge_kw["uniq_weight_fields"] = list(
                                                merge_props
                                            )
                                db.insert_edges_batch(
                                    docs_edges=data,
                                    source_class=vc.vertex_dbname(edge.source),
                                    target_class=vc.vertex_dbname(edge.target),
                                    relation_name=relation_name,
                                    match_keys_source=tuple(
                                        vc.identity_fields(edge.source)
                                    ),
                                    match_keys_target=tuple(
                                        vc.identity_fields(edge.target)
                                    ),
                                    **edge_kw,
                                )

                await asyncio.to_thread(_sync)

        await asyncio.gather(
            *[
                _push_one(edge_id, edge)
                for edge_id, edge in self.schema.core_schema.edge_config.items()
            ]
        )

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
