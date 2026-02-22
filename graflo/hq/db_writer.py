"""Database writer for pushing graph data to the target database.

Handles vertex upserts (including blank-node resolution), extra-weight
enrichment, and edge insertion.  All heavy DB I/O lives here so that
:class:`Caster` stays a lightweight orchestrator.
"""

from __future__ import annotations

import asyncio
import logging

from graflo.architecture.edge import Edge
from graflo.architecture.onto import GraphContainer
from graflo.architecture.schema import Schema
from graflo.db import ConnectionManager
from graflo.db import DBConfig

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
        _ = self.schema.vertex_config
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
                            query = db.insert_return_batch(data, vc.vertex_dbname(vcol))
                            cursor = db.execute(query)
                            return vcol, list(cursor)
                        db.upsert_docs_batch(
                            data,
                            vc.vertex_dbname(vcol),
                            vc.index(vcol),
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
                    gc.edges[edge_id].extend(
                        (x, y, {}) for x, y in zip(gc.vertices[vfrom], gc.vertices[vto])
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
                        index_fields = vc.index(weight.name)
                        if self.dry or weight.name not in gc.vertices:
                            continue
                        weights_per_item = db.fetch_present_documents(
                            class_name=vc.vertex_dbname(weight.name),
                            batch=gc.vertices[weight.name],
                            match_keys=index_fields.fields,
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
                                    match_keys_source=vc.index(edge.source).fields,
                                    match_keys_target=vc.index(edge.target).fields,
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
