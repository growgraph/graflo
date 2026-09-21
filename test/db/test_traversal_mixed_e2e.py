"""A multi-hop neighbourhood must cross relation types on every backend.

The question an impact analysis asks -- which services does this change reach?
-- walks change -> server <- app <- service, three different relations. A
backend that follows one relation per pattern answers "only the server", which
is wrong rather than partial, so the assertions compare exact reached sets.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.bindings import Bindings, FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection, GraphContainer
from graflo.db.manager import ConnectionManager
from graflo.hq.caster import IngestionParams
from graflo.hq.graph_engine import GraphEngine
from test.db.backends import backend_params, config_for

SPACE = "gf_traversal_mixed_e2e"
TYPES = ("change", "server", "app", "service")

# change C1 -targets-> server S1; app A1 -runs_on-> S1; service P1 -depends_on-> A1
#   hops=1 from C1 (any): {S1}
#   hops=2 from C1 (any): {S1, A1}
#   hops=3 from C1 (any): {S1, A1, P1}
LINKS: dict[str, tuple[str, str, str]] = {
    # resource: (source type, target type, relation)
    "targets": ("change", "server", "targets"),
    "runs_on": ("app", "server", "runs_on"),
    "depends_on": ("service", "app", "depends_on"),
}
ROWS: dict[str, str] = {
    "targets": "src,dst\nC1,S1\n",
    "runs_on": "src,dst\nA1,S1\n",
    "depends_on": "src,dst\nP1,A1\n",
}


def _link_resource(name: str) -> dict[str, Any]:
    source, target, relation = LINKS[name]
    return {
        "name": name,
        "pipeline": [
            {
                "vertex": source,
                "role": "a",
                "from": {"key": "src"},
                "keep_fields": ["key"],
            },
            {
                "vertex": target,
                "role": "b",
                "from": {"key": "dst"},
                "keep_fields": ["key"],
            },
            {
                "edge": {
                    "links": [
                        {"source_role": "a", "target_role": "b", "relation": relation}
                    ]
                }
            },
        ],
    }


MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": SPACE, "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": name,
                        "properties": [{"name": "key", "type": "STRING"}],
                        "identity": ["key"],
                    }
                    for name in TYPES
                ]
            },
            "edge_config": {
                "edges": [
                    {"source": s, "target": t, "relation": r}
                    for s, t, r in LINKS.values()
                ]
            },
        },
    },
    "ingestion_model": {"resources": [_link_resource(name) for name in LINKS]},
    "bindings": {},
}

#: TigerGraph is excluded for the same reason as the single-relation suite:
#: its reverse reachability is decided at DDL time.
BACKENDS = backend_params(
    ["neo4j", "arango", "memgraph", "falkordb", "postgres", "nebula"]
)


def _manifest(root: Path) -> GraphManifest:
    manifest = GraphManifest.model_validate(MANIFEST)
    bindings = Bindings()
    for resource in manifest.require_ingestion_model().resources:
        connector = FileConnector(regex=f"^{resource.name}\\.csv$", sub_path=root)
        bindings.add_connector(connector)
        bindings.bind_resource(resource.name, connector)
    manifest.bindings = bindings
    manifest.finish_init()
    return manifest


@pytest.fixture(scope="module", params=BACKENDS)
def ingested(request: pytest.FixtureRequest, tmp_path_factory) -> Iterator[Any]:
    flavor = request.param
    root = tmp_path_factory.mktemp(SPACE)
    for name, rows in ROWS.items():
        (root / f"{name}.csv").write_text(rows)

    try:
        config = config_for(flavor, space=SPACE)
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{flavor} config unavailable: {error}")
    try:
        ConnectionManager(connection_config=config).__enter__().close()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{flavor} unreachable: {error}")

    manifest = _manifest(root)
    engine = GraphEngine(target_db_flavor=config.connection_type)
    engine.define_schema(
        manifest=manifest, target_db_config=config, recreate_schema=True
    )
    engine.ingest(
        manifest=manifest,
        target_db_config=config,
        ingestion_params=IngestionParams(n_cores=1, clear_data=False),
    )

    with ConnectionManager(connection_config=config) as db:
        yield db, manifest

    try:
        with ConnectionManager(connection_config=config) as db:
            db.delete_graph_structure(vertex_types=TYPES, delete_all=False)
    except Exception:
        pass


def reached(container: GraphContainer) -> dict[str, set[str]]:
    """Reached identities by logical type, ignoring backend-internal keys."""
    return {
        vertex_type: {str(doc["key"]) for doc in docs if doc.get("key") is not None}
        for vertex_type, docs in container.vertices.items()
        if docs
    }


@pytest.mark.parametrize(
    "hops,expected",
    [
        (1, {"server": {"S1"}}),
        (2, {"server": {"S1"}, "app": {"A1"}}),
        (3, {"server": {"S1"}, "app": {"A1"}, "service": {"P1"}}),
    ],
)
def test_a_walk_crosses_relation_types(ingested, hops, expected):
    db, manifest = ingested
    container = db.graph_neighbors(
        "change",
        "C1",
        hops=hops,
        direction=EdgeDirection.ANY,
        schema=manifest.require_schema(),
    )
    assert reached(container) == expected


def test_a_mapping_key_on_a_declared_property_anchors_the_walk(ingested):
    db, manifest = ingested
    container = db.graph_neighbors(
        "change",
        {"key": "C1"},
        hops=1,
        direction=EdgeDirection.ANY,
        schema=manifest.require_schema(),
    )
    assert reached(container) == {"server": {"S1"}}


def test_a_mapping_key_on_an_undeclared_property_is_refused(ingested):
    """The field name reaches the query text, so it must come from the schema."""
    db, manifest = ingested
    with pytest.raises(ValueError, match="not a declared property"):
        db.graph_neighbors(
            "change",
            {"key` == 1 RETURN 1 //": "C1"},
            hops=1,
            schema=manifest.require_schema(),
        )


def test_the_relation_filter_bounds_the_walk(ingested):
    db, manifest = ingested
    container = db.graph_neighbors(
        "change",
        "C1",
        hops=3,
        direction=EdgeDirection.ANY,
        edge_types=["targets", "runs_on"],
        schema=manifest.require_schema(),
    )
    assert reached(container) == {"server": {"S1"}, "app": {"A1"}}
