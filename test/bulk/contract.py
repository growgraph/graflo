"""The bulk contract: a bulk load holds the graph an upsert ingestion holds.

One manifest and one input, ingested twice through :class:`GraphEngine`, once
per mode, into two empty targets; the exported graphs must be equal. A backend
that declares ``supports_bulk_load`` runs :func:`assert_bulk_equals_upsert`
with a factory of fresh targets for it.

The input has, across several batches: repeated vertices whose later rows
replace some fields, set others to null and leave others absent; ``merge:
union`` lists holding nulls and repeats; parallel edges told apart by an edge
identity field; and repeated edges whose properties change.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest
from graflo.connections.onto import DBConfig
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.db.manager import ConnectionManager
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.write_prepare import WriteContext, project_edges

MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": "bulk_contract", "version": "1.0.0"},
        "core_schema": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Person",
                        "properties": [
                            "pid",
                            "name",
                            "nick",
                            {
                                "name": "tags",
                                "type": "LIST",
                                "item_type": "STRING",
                                "merge": "union",
                            },
                        ],
                        "identity": ["pid"],
                    },
                    {
                        "name": "City",
                        "properties": ["cid", "label"],
                        "identity": ["cid"],
                    },
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": "Person",
                        "target": "City",
                        "relation": "lives_in",
                        "properties": ["role", "since"],
                        "identities": [["source", "target", "relation", "role"]],
                    }
                ]
            },
        },
    },
    "ingestion_model": {
        "resources": [
            {
                "name": "people",
                "apply": [
                    {
                        "vertex": "Person",
                        "from": {
                            "pid": "pid",
                            "name": "name",
                            "nick": "nick",
                            "tags": "tags",
                        },
                    },
                    {"vertex": "City", "from": {"cid": "cid", "label": "label"}},
                    {
                        "edge": {
                            "from": "Person",
                            "to": "City",
                            "relation": "lives_in",
                            "properties": ["role", "since"],
                        }
                    },
                ],
            }
        ],
        "transforms": [],
    },
}

ROWS: list[dict[str, Any]] = [
    {"pid": "p1", "name": "Ann", "nick": "a", "tags": ["x", None], "cid": "c1",
     "label": "Paris", "role": "home", "since": 2001},
    {"pid": "p2", "name": "Bo", "tags": ["y"], "cid": "c1", "role": "home", "since": 2010},
    {"pid": "p1", "name": "Ann B.", "nick": None, "tags": ["z", "x"], "cid": "c2",
     "label": "Lyon", "role": "work", "since": 2015},
    {"pid": "p3", "cid": "c2", "role": "home"},
    {"pid": "p1", "tags": None, "cid": "c1", "role": "home", "since": 2020},
    {"pid": "p2", "name": "Bob", "nick": "b", "cid": "c3", "label": "Nice",
     "role": "work", "since": 2021},
    {"pid": "p3", "name": "Cy", "tags": ["w"], "cid": "c2", "label": "Lyon 2e",
     "role": "home", "since": 1999},
]  # fmt: skip


def manifest() -> GraphManifest:
    out = GraphManifest.from_config(json.loads(json.dumps(MANIFEST)))
    out.finish_init()
    return out


def _registry() -> DataSourceRegistry:
    registry = DataSourceRegistry()
    registry.register(
        DataSourceFactory.create_in_memory_data_source(data=[dict(r) for r in ROWS]),
        resource_name="people",
    )
    return registry


def ingest(conf: DBConfig, params: IngestionParams) -> None:
    """Ingest the contract input into *conf* in *params*' mode."""
    engine = GraphEngine(target_db_flavor=conf.connection_type)
    m = manifest()
    if params.mode != "bulk":
        engine.define_schema(m, conf)
    engine.ingest(m, conf, params, data_source_registry=_registry())


def export(conf: DBConfig) -> dict[str, list[str]]:
    """The graph *conf* holds, every element as canonical JSON, sorted per type."""
    m = manifest()
    schema = m.require_schema()
    ctx = WriteContext.for_flavor(schema, conf.connection_type)
    vc = ctx.db_aware.vertex_config
    out: dict[str, list[str]] = {}
    with ConnectionManager(connection_config=conf) as db:
        for vertex in schema.core_schema.vertex_config.vertex_set:
            docs = db.fetch_all_docs(vc.vertex_dbname(vertex))
            out[vertex] = sorted(json.dumps(d, sort_keys=True) for d in docs)
        for edge in schema.core_schema.edge_config.values():
            stored = project_edges(ctx, edge, edge.relation, [])
            rows = db.fetch_all_edges(
                stored.source_class,
                stored.target_class,
                stored.relation_name,
                match_keys_source=stored.match_keys_source,
                match_keys_target=stored.match_keys_target,
                collection_name=stored.collection_name,
            )
            out[str(edge.edge_id)] = sorted(
                json.dumps(
                    [
                        [row[0].get(k) for k in stored.match_keys_source],
                        [row[1].get(k) for k in stored.match_keys_target],
                        row[2] if len(row) > 2 else {},
                    ],
                    sort_keys=True,
                )
                for row in rows
            )
    return out


def assert_bulk_equals_upsert(
    fresh_target: Callable[[str], DBConfig], staging_dir: Path, *, batch_size: int = 2
) -> dict[str, list[str]]:
    """Ingest the input in both modes into fresh targets; return the common graph.

    Args:
        fresh_target: Returns a new, empty target for the given label.
        staging_dir: A new directory for the bulk run.
        batch_size: Source rows per batch; small, so repeats span batches.
    """
    upsert_conf = fresh_target("upsert")
    bulk_conf = fresh_target("bulk")
    ingest(upsert_conf, IngestionParams(batch_size=batch_size))
    ingest(
        bulk_conf,
        IngestionParams(mode="bulk", staging_dir=staging_dir, batch_size=batch_size),
    )
    upserted = export(upsert_conf)
    assert export(bulk_conf) == upserted
    return upserted
