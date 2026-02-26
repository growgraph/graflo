"""Database writer for pushing graph data to the target database.

Handles vertex upserts (including blank-node resolution), extra-weight
enrichment, and edge insertion.  All heavy DB I/O lives here so that
:class:`Caster` stays a lightweight orchestrator.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from graflo.architecture.edge import Edge
from graflo.architecture.onto import GraphContainer
from graflo.architecture.schema import Schema
from graflo.db import ConnectionManager
from graflo.db import DBConfig
from graflo.onto import DBType

logger = logging.getLogger(__name__)


class DBWriter:
    """Push :class:`GraphContainer` data to the target graph database.

    Attributes:
        schema: Schema configuration providing vertex/edge metadata.
        dry: When ``True`` no database mutations are performed.
        max_concurrent: Upper bound on concurrent DB operations (semaphore size).
    """

    def __init__(self, schema: Schema, *, dry: bool = False, max_concurrent: int = 1):
        self.schema = schema
        self.dry = dry
        self.max_concurrent = max_concurrent

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def write(
        self,
        gc: GraphContainer,
        conn_conf: DBConfig,
        resource_name: str | None,
    ) -> None:
        """Push *gc* to the database (vertices, extra weights, then edges).

        .. note::
            *gc* is mutated in-place: blank-vertex keys are updated and blank
            edges are extended after the vertex round-trip.
        """
        self.schema.vertex_config.bind_database_features(self.schema.database_features)
        self.schema.edge_config.finish_init(self.schema.vertex_config)
        resource = self.schema.fetch_resource(resource_name)

        await self._push_vertices(gc, conn_conf)
        self._resolve_blank_edges(gc)
        await self._enrich_extra_weights(gc, conn_conf, resource)
        await self._push_edges(gc, conn_conf)

    # ------------------------------------------------------------------
    # Vertices
    # ------------------------------------------------------------------

    async def _push_vertices(self, gc: GraphContainer, conn_conf: DBConfig) -> None:
        """Upsert all vertex collections in *gc*, resolving blank nodes."""
        vc = self.schema.vertex_config
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
        vc = self.schema.vertex_config
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

    def _resolve_blank_edges(self, gc: GraphContainer) -> None:
        """Extend edge lists for blank vertices after their keys are resolved."""
        vc = self.schema.vertex_config
        for vcol in vc.blank_vertices:
            for edge_id, _edge in self.schema.edge_config.edges_items():
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
        vc = self.schema.vertex_config

        def _sync():
            with ConnectionManager(connection_config=conn_conf) as db:
                for edge in resource.extra_weights:
                    if edge.weights is None:
                        continue
                    for weight in edge.weights.vertices:
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
                            keep_keys=weight.fields,
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
        vc = self.schema.vertex_config
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def _push_one(edge_id: tuple, edge: Edge):
            async with semaphore:

                def _sync():
                    with ConnectionManager(connection_config=conn_conf) as db:
                        for ee in gc.loop_over_relations(edge_id):
                            _, _, relation = ee
                            if not self.dry:
                                data = gc.edges[ee]
                                db.insert_edges_batch(
                                    docs_edges=data,
                                    source_class=vc.vertex_dbname(edge.source),
                                    target_class=vc.vertex_dbname(edge.target),
                                    relation_name=relation,
                                    match_keys_source=tuple(
                                        vc.identity_fields(edge.source)
                                    ),
                                    match_keys_target=tuple(
                                        vc.identity_fields(edge.target)
                                    ),
                                    filter_uniques=False,
                                    dry=self.dry,
                                    collection_name=edge.database_name,
                                )

                await asyncio.to_thread(_sync)

        await asyncio.gather(
            *[
                _push_one(edge_id, edge)
                for edge_id, edge in self.schema.edge_config.edges_items()
            ]
        )
