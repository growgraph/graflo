"""Every backend must answer the same multi-hop question the same way.

The claim traversal makes is not "each backend returns something" but "each
backend returns the *same* neighbourhood, in the same container shape". So the
assertions here normalize away backend-internal keys and compare structure —
otherwise a backend that silently under-reports one hop still passes.

The fixture graph is a chain with a branch, a cycle, and an undirected edge, so
hop bounds, revisits and direction handling are all exercised by one ingest.
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

NODE = "node"
LINK = "links"
SPACE = "gf_traversal_e2e"

# A -> B -> C -> D chain, a branch B -> E, and a cycle D -> A.
#   hops=1 from A: {B}
#   hops=2 from A: {B, C, E}
#   hops=3 from A: {B, C, E, D}
NODES_CSV = "id,name\nA,alpha\nB,beta\nC,gamma\nD,delta\nE,epsilon\n"
LINKS_CSV = "src,dst\nA,B\nB,C\nC,D\nB,E\nD,A\n"

MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": SPACE, "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": NODE,
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
                        "source": NODE,
                        "target": NODE,
                        "relation": LINK,
                    }
                ]
            },
        },
    },
    "ingestion_model": {
        "resources": [
            {"name": "nodes", "pipeline": [{"vertex": NODE}]},
            {
                "name": "links",
                "pipeline": [
                    {
                        "vertex": NODE,
                        "role": "a",
                        "from": {"id": "src"},
                        "keep_fields": ["id"],
                    },
                    {
                        "vertex": NODE,
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
                                    "relation": LINK,
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

#: TigerGraph is excluded: its reverse reachability is a DDL-time decision and
#: the outbound-only case is covered by the query-shape suite.
BACKENDS = backend_params(
    ["neo4j", "arango", "memgraph", "falkordb", "postgres", "nebula"]
)


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


@pytest.fixture(scope="module", params=BACKENDS)
def ingested(request: pytest.FixtureRequest, tmp_path_factory) -> Iterator[Any]:
    flavor = request.param
    root = tmp_path_factory.mktemp(SPACE)
    (root / "nodes.csv").write_text(NODES_CSV)
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
            db.delete_graph_structure(vertex_types=(NODE,), delete_all=False)
    except Exception:
        pass


def reached_ids(container: GraphContainer) -> set[str]:
    """Normalized vertex identities in *container*.

    Backends decorate documents differently (``_id`` / ``_rev`` / ``_key`` /
    element ids), so cross-backend comparison has to happen on the logical
    identity alone.
    """
    ids: set[str] = set()
    for docs in container.vertices.values():
        for doc in docs:
            value = doc.get("id") or doc.get("_key")
            if value is not None:
                ids.add(str(value))
    return ids


@pytest.mark.parametrize("hops,expected", [(1, {"B"}), (2, {"B", "C", "E"})])
def test_outbound_neighbourhood_by_hop_count(ingested, hops, expected):
    """Hop bounds must be honoured exactly, not approximately."""
    _flavor, db, manifest = ingested
    container = db.graph_neighbors(
        NODE,
        "A",
        hops=hops,
        direction=EdgeDirection.OUT,
        schema=manifest.require_schema(),
    )
    assert reached_ids(container) == expected


def test_inbound_is_not_outbound(ingested):
    """The control: A's only inbound neighbour is D, via the cycle edge."""
    _flavor, db, manifest = ingested
    container = db.graph_neighbors(
        NODE, "A", hops=1, direction=EdgeDirection.IN, schema=manifest.require_schema()
    )
    assert reached_ids(container) == {"D"}


def test_any_direction_sees_both_sides(ingested):
    _flavor, db, manifest = ingested
    container = db.graph_neighbors(
        NODE, "A", hops=1, direction=EdgeDirection.ANY, schema=manifest.require_schema()
    )
    assert reached_ids(container) == {"B", "D"}


def test_traversal_terminates_on_the_cycle(ingested):
    """D -> A closes a loop; a hop bound past it must still return."""
    _flavor, db, manifest = ingested
    container = db.graph_neighbors(
        NODE,
        "A",
        hops=6,
        direction=EdgeDirection.OUT,
        schema=manifest.require_schema(),
    )
    assert reached_ids(container) <= {"B", "C", "D", "E"}


def test_limit_bounds_the_result(ingested):
    _flavor, db, manifest = ingested
    container = db.graph_neighbors(
        NODE,
        "A",
        hops=3,
        direction=EdgeDirection.OUT,
        limit=1,
        schema=manifest.require_schema(),
    )
    assert sum(len(rows) for rows in container.edges.values()) <= 1


def test_result_is_a_graph_container(ingested):
    """The shape promise: same container type on every backend."""
    _flavor, db, manifest = ingested
    container = db.graph_neighbors(NODE, "A", hops=2, schema=manifest.require_schema())
    assert isinstance(container, GraphContainer)
    assert all(isinstance(key, tuple) for key in container.edges)


def test_unknown_vertex_type_raises(ingested):
    _flavor, db, manifest = ingested
    with pytest.raises(ValueError, match="Unknown vertex type"):
        db.graph_neighbors("nope", "A", schema=manifest.require_schema())


def test_schema_is_required(ingested):
    """Without a schema there is no logical -> storage mapping to apply."""
    _flavor, db, _manifest = ingested
    with pytest.raises(ValueError, match="requires a schema"):
        db.graph_neighbors(NODE, "A")


def test_zero_hops_is_rejected(ingested):
    _flavor, db, manifest = ingested
    with pytest.raises(ValueError, match="hops"):
        db.graph_neighbors(NODE, "A", hops=0, schema=manifest.require_schema())


# ── multi-seed traverse ─────────────────────────────────────────────────────


def test_traverse_merges_every_seed(ingested):
    """Two seeds, one container — and a vertex reached from both appears once.

    Deduplication is the whole reason `traverse` is not just a client-side loop
    over `graph_neighbors`.
    """
    from graflo.architecture.query import TraverseQuery

    _flavor, db, manifest = ingested
    query = TraverseQuery(
        seeds=[
            {"vertex_type": NODE, "key": "A"},
            {"vertex_type": NODE, "key": "E"},
        ],
        max_hops=2,
        direction=EdgeDirection.ANY,
    ).finish_init()
    container = db.traverse(query, schema=manifest.require_schema())
    assert reached_ids(container) == {"A", "B", "C", "D", "E"}
    for docs in container.vertices.values():
        identities = [str(d.get("id") or d.get("_key")) for d in docs]
        assert len(identities) == len(set(identities)), (
            "a seed's overlap was duplicated"
        )


def test_traverse_honours_the_hop_bound(ingested):
    """The cap has to survive the trip into the driver, not just the model."""
    from graflo.architecture.query import TraverseQuery

    _flavor, db, manifest = ingested
    query = TraverseQuery(
        seeds=[{"vertex_type": NODE, "key": "A"}],
        max_hops=1,
        direction=EdgeDirection.OUT,
    ).finish_init()
    container = db.traverse(query, schema=manifest.require_schema())
    assert reached_ids(container) == {"B"}
