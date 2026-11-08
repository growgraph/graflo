"""An in-memory graph that implements the bulk and sync writer protocols.

It applies the semantics the Cypher writers give their statements: a node is
its label and key, ``SET n = row`` drops nulls, an edge is created between
existing endpoints, and an owner chunk rewrites only the owners whose stored
hash differs. Registered in place of a backend, it lets a whole bulk or sync
run be checked without a database.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

from graflo.connections.bulk import BulkConfig
from graflo.db.bulk import BulkCounts, BulkEdgeChunk, BulkPlan, BulkVertexChunk
from graflo.db.cypher.sync import changed_owners
from graflo.db.sync import SyncEdgeType, SyncOwnerChunk, SyncWritten

Node = tuple[str, tuple]


def _props(row: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in row.items() if v is not None}


class MemoryGraph:
    """One graph per config ``uri``, shared by every connection to it."""

    graphs: ClassVar[dict[str, dict[str, Any]]] = {}
    supports_bulk_load = True
    supports_sync = True
    bulk_dense_ids = False
    bulk_partition_edges = False
    writes: ClassVar[list[str]] = []
    """Every owner rewritten, ``label:key``, across graphs."""

    def __init__(self, config: Any) -> None:
        self.state = self.graphs.setdefault(str(config.uri), {"nodes": {}, "edges": []})

    def close(self) -> None:
        pass

    # -- reading ------------------------------------------------------------

    @classmethod
    def export(cls, uri: str) -> dict[str, list[str]]:
        """Every node and edge of the graph at *uri*, as sorted canonical JSON."""
        state = cls.graphs[uri]
        nodes = sorted(
            json.dumps([label, props], sort_keys=True, default=str)
            for (label, _), props in state["nodes"].items()
        )
        edges = sorted(
            json.dumps(
                [
                    e["rel"],
                    e["s"][0],
                    list(e["s"][1]),
                    e["t"][0],
                    list(e["t"][1]),
                    e["props"],
                ],
                sort_keys=True,
                default=str,
            )
            for e in state["edges"]
        )
        return {"nodes": nodes, "edges": edges}

    def fetch_all_docs(self, class_name: str, **_: Any) -> list[dict[str, Any]]:
        return [
            dict(p)
            for (label, _k), p in self.state["nodes"].items()
            if label == class_name
        ]

    def fetch_all_edges(
        self, source_class: str, target_class: str, relation_name: str | None, **_: Any
    ) -> list[list[dict[str, Any]]]:
        nodes = self.state["nodes"]
        return [
            [dict(nodes[e["s"]]), dict(nodes[e["t"]]), dict(e["props"])]
            for e in self.state["edges"]
            if (e["s"][0], e["rel"], e["t"][0])
            == (source_class, relation_name, target_class)
        ]

    def _node(self, label: str, keys: list[str], row: dict[str, Any]) -> Node:
        return (label, tuple(row.get(k) for k in keys))

    # -- bulk -----------------------------------------------------------------

    def bulk_begin(self, schema, cfg: BulkConfig, *, recreate=False, resume=False):
        if not resume and self.state["nodes"] and not recreate:
            raise ValueError("graph is not empty")
        return BulkPlan()

    def bulk_write_vertices(self, schema, chunk: BulkVertexChunk) -> None:
        for row in chunk.rows:
            self.state["nodes"][self._node(chunk.class_name, chunk.match_keys, row)] = (
                _props(row)
            )

    def bulk_prepare_edges(self, schema) -> None:
        pass

    def bulk_write_edges(self, schema, chunk: BulkEdgeChunk) -> None:
        assert chunk.relation_name is not None
        for source, target, props in chunk.rows:
            self._create(
                chunk.relation_name,
                self._node(chunk.source_class, chunk.match_keys_source, source),
                self._node(chunk.target_class, chunk.match_keys_target, target),
                props,
            )

    def bulk_finish(self, schema) -> BulkCounts:
        vertices: dict[str, int] = {}
        for label, _ in self.state["nodes"]:
            vertices[label] = vertices.get(label, 0) + 1
        edges: dict[Any, int] = {}
        for e in self.state["edges"]:
            key = (e["s"][0], e["rel"], e["t"][0])
            edges[key] = edges.get(key, 0) + 1
        return BulkCounts(vertices=vertices, edges=edges)

    def _create(self, rel: str, s: Node, t: Node, props: dict[str, Any]) -> None:
        if s in self.state["nodes"] and t in self.state["nodes"]:
            self.state["edges"].append(
                {"rel": rel, "s": s, "t": t, "props": _props(props)}
            )

    # -- sync -----------------------------------------------------------------

    def sync_begin(self, schema, edge_types: list[SyncEdgeType]) -> None:
        pass

    def sync_ensure_vertices(self, schema, chunk: BulkVertexChunk) -> None:
        for row in chunk.rows:
            node = self._node(chunk.class_name, chunk.match_keys, row)
            self.state["nodes"].setdefault(node, _props(row))

    def sync_write_owners(self, schema, chunk: SyncOwnerChunk) -> SyncWritten:
        nodes = self.state["nodes"]
        stored = {
            i: nodes.get(self._node(chunk.class_name, chunk.match_keys, o.doc), {}).get(
                chunk.hash_field
            )
            for i, o in enumerate(chunk.owners)
        }
        changed = changed_owners(chunk, stored)
        for i in changed:
            owner = chunk.owners[i]
            node = self._node(chunk.class_name, chunk.match_keys, owner.doc)
            nodes[node] = _props(owner.doc)
            self.writes.append(f"{chunk.class_name}:{node[1]}")
            for t, edge in enumerate(chunk.edge_types):
                self.state["edges"] = [
                    e
                    for e in self.state["edges"]
                    if not _owned_by(e, edge, node, owner.doc, chunk)
                ]
                for source, target, props in owner.edges[t]:
                    self._create(
                        edge.relation_name,
                        self._node(edge.source_class, edge.match_keys_source, source),
                        self._node(edge.target_class, edge.match_keys_target, target),
                        props,
                    )
        return SyncWritten(
            written=len(changed), skipped=len(chunk.owners) - len(changed)
        )


def _owned_by(
    e: dict[str, Any],
    edge: SyncEdgeType,
    node: Node,
    doc: dict[str, Any],
    chunk: SyncOwnerChunk,
) -> bool:
    if e["rel"] != edge.relation_name or (e["s"][0], e["t"][0]) != (
        edge.source_class,
        edge.target_class,
    ):
        return False
    if edge.side == "source":
        return e["s"] == node
    if edge.side == "target":
        return e["t"] == node
    assert edge.owner_property is not None
    return e["props"].get(edge.owner_property) == doc[chunk.match_keys[0]]
