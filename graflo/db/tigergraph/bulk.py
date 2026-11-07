"""The TigerGraph bulk writer: a loading job fed chunk by chunk over REST++.

``bulk_begin`` applies the schema through the first-write guard and (re)creates
the job; each chunk is encoded by :mod:`graflo.db.tigergraph.loading_job` and
posted to ``POST /restpp/ddl/{graph}``. Nothing is read from the server's disk.

The job's column layout is a pure function of the schema, so each call derives
it from the schema it is given; the writer keeps no state between calls.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from graflo.architecture.schema import Schema
from graflo.connections.bulk import BulkConfig
from graflo.db.bulk import (
    BulkCounts,
    BulkEdgeChunk,
    BulkPlan,
    BulkVertexChunk,
    StoredEdgeId,
)
from graflo.db.tigergraph.gsql_parsers import gsql_result_has_error
from graflo.db.tigergraph.loading_job import (
    EOL,
    MAX_BODY_BYTES,
    SEP,
    LoadingJob,
    check_load_response,
    create_job_gsql,
    edge_line,
    loading_job,
    split_body,
    vertex_line,
)

if TYPE_CHECKING:
    from graflo.db.tigergraph.conn import TigerGraphConnection

logger = logging.getLogger(__name__)


class TigerGraphBulkWriter:
    """The bulk protocol on one connection."""

    def __init__(self, conn: TigerGraphConnection) -> None:
        self._conn = conn

    def _job(self, schema: Schema) -> LoadingJob:
        return loading_job(schema, self._conn._require_configured_graph_name())

    def _gsql(self, graph: str, statement: str) -> str:
        result = str(self._conn._execute_gsql(f"USE GRAPH {graph}\n{statement}"))
        if gsql_result_has_error(result):
            raise RuntimeError(f"GSQL failed on graph {graph!r}: {result}")
        return result

    def begin(
        self, schema: Schema, cfg: BulkConfig, *, recreate: bool, resume: bool
    ) -> BulkPlan:
        conn = self._conn
        graph = conn._admin._resolve_graph_name(schema)
        applied = conn.graph_exists(graph) and conn._admin.schema_has_artifacts(graph)
        if not (resume and applied):
            # Creates the graph when missing; refuses a schema unless recreate.
            conn.apply_target_schema(schema, recreate=recreate)
        job = loading_job(schema, graph)
        # Recreated on resume too: the job is derived from the schema, and
        # nothing is loading while bulk_begin runs.
        self._drop_job(graph, job.name)
        self._gsql(graph, create_job_gsql(job))
        return BulkPlan()

    def _drop_job(self, graph: str, name: str) -> None:
        try:
            conn_result = self._conn._execute_gsql(
                f"USE GRAPH {graph}\nDROP JOB {name}"
            )
            logger.debug("DROP JOB %s on %s: %s", name, graph, conn_result)
        except Exception as e:  # a missing job is the usual case
            logger.debug("DROP JOB %s on %s: %s", name, graph, e)

    def _post(self, job: LoadingJob, file_var: str, lines: list[bytes]) -> None:
        for body, count in split_body(lines, MAX_BODY_BYTES):
            response = self._conn._rest._post_loading_data(
                job.graph, job.name, file_var, body, sep=SEP, eol=EOL
            )
            check_load_response(response, lines=count)

    def write_vertices(self, schema: Schema, chunk: BulkVertexChunk) -> None:
        job = self._job(schema)
        load = job.vertex(chunk.class_name)
        self._post(job, load.file_var, [vertex_line(load, doc) for doc in chunk.rows])

    def write_edges(self, schema: Schema, chunk: BulkEdgeChunk) -> None:
        job = self._job(schema)
        load = job.edge(chunk.stored_id)
        self._post(
            job,
            load.file_var,
            [
                edge_line(load, row, chunk.match_keys_source, chunk.match_keys_target)
                for row in chunk.rows
            ],
        )

    def finish(self, schema: Schema) -> BulkCounts:
        conn = self._conn
        graph = conn._require_configured_graph_name()
        job = loading_job(schema, graph)
        rest = conn._rest
        vertices = {
            v.vertex_type: rest._builtin_count(
                graph, {"function": "stat_vertex_number", "type": v.vertex_type}
            )
            for v in job.vertices
        }
        edges: dict[StoredEdgeId, int] = {
            e.stored_id: rest._builtin_count(
                graph,
                {
                    "function": "stat_edge_number",
                    "type": e.edge_type,
                    "from_type": e.source_type,
                    "to_type": e.target_type,
                },
            )
            for e in job.edges
        }
        self._drop_job(graph, job.name)
        return BulkCounts(vertices=vertices, edges=edges)
