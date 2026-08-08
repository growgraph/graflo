"""Every backend must recover the schema it was given.

Introspection is only useful if the result is *modelable* — something you can
hand back to graflo and ingest against. So the assertions here are a round trip:
ingest an authored schema, read it back out of the live database, and check the
recovered `Schema` both re-validates and matches the original on the facts that
matter (vertex names, edge endpoints, identity, `directed`).

Fidelity varies sharply and deliberately by backend. TigerGraph and Nebula have
real catalogues; the Cypher family and PostgreSQL recover what was written but
cannot recover what was never expressed in storage. Where a backend genuinely
cannot know something, the assertion says so rather than being weakened to pass
everywhere — a suite that only tests the intersection tests almost nothing.

Reuses the traversal fixture graph, so the two suites exercise one ingest shape.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.bindings import Bindings, FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.schema import Schema
from graflo.db.manager import ConnectionManager
from graflo.hq.caster import IngestionParams
from graflo.hq.graph_engine import GraphEngine

NODE = "node"
LINK = "links"
SPACE = "gf_introspect_e2e"

NODES_CSV = "id,name\nA,alpha\nB,beta\nC,gamma\n"
LINKS_CSV = "src,dst\nA,B\nB,C\n"

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
                "edges": [{"source": NODE, "target": NODE, "relation": LINK}]
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

#: Every backend that declares `supports_schema_introspection`, except the file
#: backend, which stores the authored `Schema` verbatim and so round-trips by
#: construction rather than by introspection.
BACKENDS = [
    pytest.param("neo4j", id="neo4j"),
    pytest.param("arango", id="arango"),
    pytest.param("memgraph", id="memgraph"),
    pytest.param("falkordb", id="falkordb"),
    pytest.param("postgres", id="postgres"),
    pytest.param("tigergraph", id="tigergraph", marks=pytest.mark.tigergraph),
    pytest.param("nebula", id="nebula", marks=pytest.mark.nebula),
]

#: Backends whose catalogue names the edge relation. The Cypher family stores a
#: relationship type, Nebula and TigerGraph an edge type, PostgreSQL encodes it
#: in the table name — but ArangoDB stores edges in a collection whose name is a
#: graflo naming convention, not a recorded relation, so it is excluded.
RELATION_NAMING_BACKENDS = {
    "neo4j",
    "memgraph",
    "falkordb",
    "postgres",
    "tigergraph",
    "nebula",
}

#: Backends with a real DDL catalogue, which therefore report declared property
#: types rather than leaving them to default.
TYPED_CATALOGUE_BACKENDS = {"tigergraph", "nebula", "postgres"}


def _config(flavor: str):
    from graflo.connections.onto import (
        ArangoConfig,
        FalkordbConfig,
        MemgraphConfig,
        NebulaConfig,
        Neo4jConfig,
        PostgresConfig,
        TigergraphConfig,
    )

    if flavor == "neo4j":
        config = Neo4jConfig.from_docker_env()
        config.database = config.database or "_system"
        return config
    if flavor == "arango":
        config = ArangoConfig.from_docker_env()
        config.database = SPACE
        return config
    if flavor == "memgraph":
        return MemgraphConfig.from_docker_env()
    if flavor == "falkordb":
        config = FalkordbConfig.from_docker_env()
        config.database = SPACE
        return config
    if flavor == "postgres":
        config = PostgresConfig.from_docker_env()
        config.schema_name = SPACE
        return config
    if flavor == "tigergraph":
        config = TigergraphConfig.from_docker_env()
        config.database = SPACE
        return config
    config = NebulaConfig.from_docker_env()
    config.uri = f"nebula://localhost:{config.port}"
    config.schema_name = SPACE
    return config


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
def introspected(request: pytest.FixtureRequest, tmp_path_factory) -> Iterator[Any]:
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
        recovered = db.introspect_graph_schema(sample_limit=100)
        yield flavor, recovered, manifest.require_schema()

    try:
        with ConnectionManager(connection_config=config) as db:
            db.delete_graph_structure(vertex_types=(NODE,), delete_all=False)
    except Exception:
        pass


def _node_vertex(schema: Schema):
    """The recovered counterpart of the authored `node` type.

    Backends normalize type names differently, so match case-insensitively
    rather than assuming the authored spelling survived.
    """
    for vertex in schema.core_schema.vertex_config.vertices:
        if vertex.name.lower() == NODE:
            return vertex
    raise AssertionError(
        f"no vertex matching {NODE!r}; recovered: "
        f"{[v.name for v in schema.core_schema.vertex_config.vertices]}"
    )


def test_recovered_schema_is_a_valid_schema(introspected):
    """The contract is a modelable artifact, not a report.

    If the recovered object does not re-validate, nothing downstream —
    persisting it as a registry artifact, opening it in the studio, ingesting
    against it — is possible, and every richer assertion below is moot.
    """
    _flavor, recovered, _authored = introspected
    assert isinstance(recovered, Schema)
    assert Schema.model_validate(recovered.to_dict()) is not None


def test_authored_vertex_type_is_recovered(introspected):
    _flavor, recovered, _authored = introspected
    assert _node_vertex(recovered) is not None


def test_authored_properties_are_recovered(introspected):
    """Both authored properties must come back, however they were stored."""
    _flavor, recovered, _authored = introspected
    names = {f.name.lower() for f in _node_vertex(recovered).properties}
    assert {"id", "name"} <= names


def test_identity_is_recovered(introspected):
    """`id` must be picked as identity, not merely present as a property.

    A recovered schema whose identity is wrong ingests duplicates rather than
    matching existing vertices, so this is the field that decides whether the
    artifact is safe to model against.
    """
    _flavor, recovered, _authored = introspected
    identity = [name.lower() for name in _node_vertex(recovered).identity]
    assert "id" in identity


def test_edge_endpoints_are_recovered(introspected):
    """The self-referencing `node -> node` edge must survive the round trip."""
    _flavor, recovered, _authored = introspected
    pairs = {
        (e.source.lower(), e.target.lower())
        for e in recovered.core_schema.edge_config.edges
    }
    assert (NODE, NODE) in pairs


def test_relation_name_is_recovered_where_the_backend_records_it(introspected):
    """Arango is the honest exception: its collection name is a graflo convention."""
    flavor, recovered, _authored = introspected
    if flavor not in RELATION_NAMING_BACKENDS:
        pytest.skip(f"{flavor} does not record a relation name for an edge")
    relations = {
        (e.relation or "").lower() for e in recovered.core_schema.edge_config.edges
    }
    assert LINK in relations


def test_declared_property_types_are_recovered(introspected):
    """A catalogue-backed backend must report types, not default them.

    Sampling backends legitimately cannot: a Cypher property has a runtime type
    per value, not a declared one per label.
    """
    flavor, recovered, _authored = introspected
    if flavor not in TYPED_CATALOGUE_BACKENDS:
        pytest.skip(f"{flavor} samples rather than reads declared types")
    types = {f.name.lower(): f.type for f in _node_vertex(recovered).properties}
    assert types.get("name") is not None


def test_directed_defaults_true_without_a_declaration(introspected):
    """Only TigerGraph can *state* undirectedness; nobody may infer it.

    An unproven `directed: false` silently widens every query built on the
    recovered schema, so the default has to hold everywhere else.
    """
    _flavor, recovered, _authored = introspected
    assert all(e.directed for e in recovered.core_schema.edge_config.edges)


def test_recovered_flavor_matches_the_backend(introspected):
    """The recovered profile must target the database it was read from."""
    flavor, recovered, _authored = introspected
    recovered_flavor = recovered.db_profile.db_flavor
    assert str(getattr(recovered_flavor, "value", recovered_flavor)).lower() == flavor
