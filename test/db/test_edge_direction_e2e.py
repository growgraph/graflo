"""Reading an undirected edge from the endpoint it was *not* anchored on.

This is the claim `Edge.directed` makes and, until the read path grew a
``direction``, the one thing no backend could honour: a symmetric relationship
ingested as ``a -> b`` must be reachable when you start from ``b``.

The suite deliberately asserts both halves. ``OUT`` from the far endpoint must
find nothing — otherwise ``ANY`` finding the edge proves only that the query is
indiscriminate, not that direction is being handled.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.bindings import Bindings, FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.graph_types import EdgeDirection
from graflo.db.edge_direction_support import default_direction_for_edge
from graflo.db.manager import ConnectionManager
from graflo.hq.caster import IngestionParams
from graflo.hq.graph_engine import GraphEngine
from test.db.backends import backend_params, config_for

PERSON = "person"
RELATION = "knows"
SPACE = "gf_edge_direction_e2e"

# P1 -> P2 and P1 -> P3. Nothing is ever ingested anchored on P2 or P3, so any
# edge found from those vertices was reached by traversing backwards.
PEOPLE_CSV = "id,name\nP1,Ada\nP2,Bo\nP3,Cy\n"
LINKS_CSV = "src,dst\nP1,P2\nP1,P3\n"

MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": SPACE, "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": PERSON,
                        "properties": [
                            {"name": "id", "type": "STRING"},
                            {"name": "name", "type": "STRING"},
                        ],
                        "identity": ["id"],
                    }
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": PERSON,
                        "target": PERSON,
                        "relation": RELATION,
                        "directed": False,
                    }
                ]
            },
        },
        "db_profile": {},
    },
    "ingestion_model": {
        "resources": [
            {"name": "people", "pipeline": [{"vertex": PERSON}]},
            {
                "name": "links",
                "pipeline": [
                    {
                        "vertex": PERSON,
                        "role": "a",
                        "from": {"id": "src"},
                        "keep_fields": ["id"],
                    },
                    {
                        "vertex": PERSON,
                        "role": "b",
                        "from": {"id": "dst"},
                        "keep_fields": ["id"],
                    },
                    {
                        "edge": {
                            "links": [
                                {
                                    "source_role": "a",
                                    "target_role": "b",
                                    "relation": RELATION,
                                }
                            ]
                        }
                    },
                ],
            },
        ]
    },
    "bindings": {},
}

# Backends whose fetch_edges can answer a direction at all. TigerGraph is
# excluded on purpose: reverse reachability there is a DDL-time decision, and
# PostgreSQL / the file backend do not implement fetch_edges.
BACKENDS = backend_params(["neo4j", "arango", "memgraph", "falkordb", "nebula"])


def _config(flavor: str):
    return config_for(flavor, space=SPACE)


def _manifest(root: Path) -> GraphManifest:
    manifest = GraphManifest.model_validate(MANIFEST)
    bindings = Bindings()
    for resource in manifest.require_ingestion_model().resources:
        connector = FileConnector(regex=f".*{resource.name}.*", sub_path=root)
        bindings.add_connector(connector)
        bindings.bind_resource(resource.name, connector)
    manifest.bindings = bindings
    manifest.finish_init()
    return manifest


@pytest.fixture(scope="module")
def ingested(request: pytest.FixtureRequest, tmp_path_factory) -> Iterator[Any]:
    flavor = request.param
    root = tmp_path_factory.mktemp(SPACE)
    (root / "people.csv").write_text(PEOPLE_CSV)
    (root / "links.csv").write_text(LINKS_CSV)

    try:
        config = _config(flavor)
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
        yield flavor, db, manifest

    try:
        with ConnectionManager(connection_config=config) as db:
            db.delete_graph_structure(vertex_types=(PERSON,), delete_all=False)
    except Exception:
        pass


def _edge_type(manifest: GraphManifest, flavor: str) -> str:
    schema = manifest.require_schema()
    edge = next(iter(schema.core_schema.edge_config.values()))
    if flavor == "arango":
        return (
            schema.db_profile.edge_storage_name(
                edge.edge_id, source_storage=PERSON, target_storage=PERSON
            )
            or f"{PERSON}_{PERSON}_edges"
        )
    return (
        schema.db_profile.edge_relation_name(
            edge.edge_id, default_relation=edge.relation
        )
        or RELATION
    )


def _anchor(db, flavor: str, business_id: str) -> str:
    """Address the anchor vertex the way the backend addresses it.

    ArangoDB mints its own ``_key``; everywhere else ``fetch_edges`` matches on
    the ``id`` property directly.
    """
    if flavor != "arango":
        return business_id
    matches = [d for d in db.fetch_docs(PERSON) if d.get("id") == business_id]
    assert matches, f"vertex {business_id} not found"
    return matches[0]["_key"]


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_out_from_the_anchor_endpoint_finds_both_edges(ingested) -> None:
    """Baseline: the direction the data was written in still works."""
    flavor, db, manifest = ingested
    edges = db.fetch_edges(
        PERSON,
        _anchor(db, flavor, "P1"),
        edge_type=_edge_type(manifest, flavor),
        direction=EdgeDirection.OUT,
    )
    assert len(edges) == 2


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_out_from_the_far_endpoint_finds_nothing(ingested) -> None:
    """The control. Without it, an ANY match proves nothing about direction."""
    flavor, db, manifest = ingested
    edges = db.fetch_edges(
        PERSON,
        _anchor(db, flavor, "P2"),
        edge_type=_edge_type(manifest, flavor),
        direction=EdgeDirection.OUT,
    )
    assert edges == []


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_in_reaches_the_edge_from_the_far_endpoint(ingested) -> None:
    flavor, db, manifest = ingested
    edges = db.fetch_edges(
        PERSON,
        _anchor(db, flavor, "P2"),
        edge_type=_edge_type(manifest, flavor),
        direction=EdgeDirection.IN,
    )
    assert len(edges) == 1


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_any_reaches_the_undirected_edge_from_either_end(ingested) -> None:
    """The claim `directed: false` makes, now true on every backend here."""
    flavor, db, manifest = ingested
    edge_type = _edge_type(manifest, flavor)
    counts = {
        business_id: len(
            db.fetch_edges(
                PERSON,
                _anchor(db, flavor, business_id),
                edge_type=edge_type,
                direction=EdgeDirection.ANY,
            )
        )
        for business_id in ("P1", "P2", "P3")
    }
    assert counts == {"P1": 2, "P2": 1, "P3": 1}


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_schema_derived_direction_reaches_the_far_endpoint(ingested) -> None:
    """The end-to-end path: `directed: false` in YAML steers the query itself."""
    flavor, db, manifest = ingested
    edge = next(iter(manifest.require_schema().core_schema.edge_config.values()))
    direction = default_direction_for_edge(edge)
    assert direction is EdgeDirection.ANY
    edges = db.fetch_edges(
        PERSON,
        _anchor(db, flavor, "P2"),
        edge_type=_edge_type(manifest, flavor),
        direction=direction,
    )
    assert len(edges) == 1
