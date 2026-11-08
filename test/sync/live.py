"""Record sync on a live backend: update(A→B) equals rebuild(B).

A backend that declares ``supports_sync`` runs :func:`assert_update_equals_rebuild`
with a factory of fresh, empty targets. Each scenario exports the rebuilt graph
before it asks for the next target, so a backend with one database may hand out
the same one, emptied.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from graflo.connections.onto import DBConfig
from graflo.data_source.factory import DataSourceFactory
from graflo.data_source.registry import DataSourceRegistry
from graflo.db.manager import ConnectionManager
from graflo.db.sync import SyncWritten
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams
from graflo.hq.write_prepare import WriteContext, project_edges
from test.sync.test_update_equals_rebuild import (
    RELEASE_A,
    _manifest,
    derive_release_b,
)

Release = dict[str, list[dict[str, Any]]]

#: Owners a sync of B onto A rewrites, by stored label.
CHANGED = {"Work": 3, "Author": 1}


def ingest(conf: DBConfig, release: Release, params: IngestionParams) -> None:
    registry = DataSourceRegistry()
    for resource, rows in release.items():
        registry.register(
            DataSourceFactory.create_in_memory_data_source(data=copy.deepcopy(rows)),
            resource_name=resource,
        )
    engine = GraphEngine(target_db_flavor=conf.connection_type)
    manifest = _manifest()
    if params.mode == "upsert":
        engine.define_schema(manifest, conf)
    engine.ingest(manifest, conf, params, data_source_registry=registry)


def export(conf: DBConfig) -> dict[str, list[str]]:
    """Every vertex and edge *conf* holds, as sorted canonical JSON."""
    schema = _manifest().require_schema()
    ctx = WriteContext.for_flavor(schema, conf.connection_type)
    vc = ctx.db_aware.vertex_config
    out: dict[str, list[str]] = {}
    with ConnectionManager(connection_config=conf) as db:
        for vertex in sorted(schema.core_schema.vertex_config.vertex_set):
            docs = db.fetch_all_docs(vc.vertex_dbname(vertex))
            out[vertex] = sorted(
                json.dumps(d, sort_keys=True, default=str) for d in docs
            )
        for edge in schema.core_schema.edge_config.values():
            stored = project_edges(ctx, edge, edge.relation, [])
            rows = db.fetch_all_edges(
                stored.source_class,
                stored.target_class,
                stored.relation_name,
                match_keys_source=stored.match_keys_source,
                match_keys_target=stored.match_keys_target,
            )
            out[str(edge.edge_id)] = sorted(
                json.dumps(
                    [
                        [row[0].get(k) for k in stored.match_keys_source],
                        [row[1].get(k) for k in stored.match_keys_target],
                        row[2],
                    ],
                    sort_keys=True,
                    default=str,
                )
                for row in rows
            )
    return out


def assert_update_equals_rebuild(
    fresh_target: Callable[[str], DBConfig],
    tmp_path: Path,
    conn_cls: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rebuild B; then load A, sync B, sync B again; the graphs must agree.

    Also checks that the first sync rewrites exactly the changed owners and the
    second rewrites none.
    """
    b = derive_release_b(RELEASE_A)
    written: dict[str, int] = {}
    real = conn_cls.sync_write_owners

    def recording(self, schema, chunk) -> SyncWritten:
        done = real(self, schema, chunk)
        written[chunk.class_name] = written.get(chunk.class_name, 0) + done.written
        return done

    monkeypatch.setattr(conn_cls, "sync_write_owners", recording)

    conf = fresh_target("rebuild")
    ingest(
        conf, b, IngestionParams(mode="bulk", staging_dir=tmp_path / "r", batch_size=2)
    )
    rebuilt = export(conf)

    conf = fresh_target("update")
    ingest(
        conf,
        RELEASE_A,
        IngestionParams(mode="bulk", staging_dir=tmp_path / "a", batch_size=2),
    )
    ingest(
        conf, b, IngestionParams(mode="sync", staging_dir=tmp_path / "s1", batch_size=2)
    )
    assert export(conf) == rebuilt
    assert written == CHANGED

    written.clear()
    ingest(
        conf, b, IngestionParams(mode="sync", staging_dir=tmp_path / "s2", batch_size=2)
    )
    assert export(conf) == rebuilt
    assert sum(written.values()) == 0


def assert_sync_after_upsert_rewrites_every_owner(
    fresh_target: Callable[[str], DBConfig], tmp_path: Path
) -> None:
    """An upsert leaves no hash, so a sync onto it rewrites every owner; B results."""
    b = derive_release_b(RELEASE_A)
    conf = fresh_target("rebuild")
    ingest(
        conf, b, IngestionParams(mode="bulk", staging_dir=tmp_path / "r", batch_size=2)
    )
    rebuilt = export(conf)

    conf = fresh_target("update")
    ingest(conf, RELEASE_A, IngestionParams(batch_size=2))
    ingest(
        conf, b, IngestionParams(mode="sync", staging_dir=tmp_path / "s", batch_size=2)
    )
    assert export(conf) == rebuilt
