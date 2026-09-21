"""Live drift on a real database: a property written outside the schema shows up.

Reuses the mixed-relation traversal graph, then writes one undeclared property
straight into the database -- the way a hand-run fix or a second loader would --
and expects `GraphEngine.diff_live_schema` to report exactly that property.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from graflo.db.manager import ConnectionManager
from graflo.hq.caster import IngestionParams
from graflo.hq.graph_engine import GraphEngine
from test.db.backends import backend_params, config_for
from test.db.test_traversal_mixed_e2e import ROWS, TYPES, _manifest

SPACE = "gf_live_drift_e2e"

#: The Cypher family, where a property can be planted with one statement.
BACKENDS = backend_params(["neo4j", "memgraph", "falkordb"])


@pytest.fixture(scope="module", params=BACKENDS)
def live(request: pytest.FixtureRequest, tmp_path_factory) -> Iterator[Any]:
    flavor = request.param
    root = tmp_path_factory.mktemp(SPACE)
    for name, rows in ROWS.items():
        (root / f"{name}.csv").write_text(rows)
    try:
        config = config_for(flavor, space=SPACE)
        ConnectionManager(connection_config=config).__enter__().close()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{flavor} unavailable: {error}")

    manifest = _manifest(root)
    engine = GraphEngine(target_db_flavor=config.connection_type)
    engine.define_schema(
        manifest=manifest, target_db_config=config, recreate_schema=True
    )
    engine.ingest(
        manifest=manifest,
        target_db_config=config,
        ingestion_params=IngestionParams(n_cores=1, clear_data=True),
    )
    yield engine, config, manifest.require_schema()

    try:
        with ConnectionManager(connection_config=config) as db:
            db.delete_graph_structure(vertex_types=TYPES, delete_all=False)
    except Exception:
        pass


def test_a_freshly_ingested_graph_shows_no_undeclared_property(live) -> None:
    engine, config, schema = live
    drift = engine.diff_live_schema(config, schema)
    assert drift.undeclared_properties == {}
    assert not drift.undeclared_edges


def test_a_planted_property_is_reported(live) -> None:
    engine, config, schema = live
    with ConnectionManager(connection_config=config) as db:
        db.execute("MATCH (n:server {key: 'S1'}) SET n.os_family = 'linux'")
    try:
        drift = engine.diff_live_schema(config, schema)
    finally:
        with ConnectionManager(connection_config=config) as db:
            db.execute("MATCH (n:server {key: 'S1'}) REMOVE n.os_family")
    assert drift.undeclared_properties == {"server": ["os_family"]}
    assert drift.has_drift
