"""End-to-end ingest of an edge-only source keyed by secondary identities.

The scenario this feature exists for: instruments and issuers are loaded with
their own primary keys, then a third source relates them using only business
keys (ISIN, LEI) that appear nowhere as a primary identity.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.contract.bindings import Bindings, FileConnector
from graflo.architecture.contract.manifest import GraphManifest
from graflo.db import ConnectionManager
from graflo.hq import GraphEngine
from graflo.hq.caster import IngestionParams

INSTRUMENT = "instrument"
ISSUER = "issuer"
RELATION = "issued_by"

INSTRUMENTS_CSV = "sid,isin\nS1,US001\nS2,US002\nS3,US003\n"
ISSUERS_CSV = "iid,lei\nI1,LEI-A\nI2,LEI-B\n"
# Keyed only by business keys — no sid, no iid anywhere in this file.
LINKS_CSV = "isin,lei,share\nUS001,LEI-A,0.5\nUS002,LEI-B,0.25\nUS003,LEI-A,0.25\n"

MANIFEST: dict[str, Any] = {
    "schema": {
        "metadata": {"name": "gf_secondary_e2e", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": INSTRUMENT,
                        "properties": [
                            {"name": "sid", "type": "STRING"},
                            {"name": "isin", "type": "STRING"},
                        ],
                        "identity": ["sid"],
                        "secondary_identities": [
                            {"name": "by_isin", "fields": ["isin"]}
                        ],
                    },
                    {
                        "name": ISSUER,
                        "properties": [
                            {"name": "iid", "type": "STRING"},
                            {"name": "lei", "type": "STRING"},
                        ],
                        "identity": ["iid"],
                        "secondary_identities": [{"name": "by_lei", "fields": ["lei"]}],
                    },
                ]
            },
            "edge_config": {
                "edges": [
                    {
                        "source": INSTRUMENT,
                        "target": ISSUER,
                        "relation": RELATION,
                        "properties": [{"name": "share", "type": "STRING"}],
                    }
                ]
            },
        },
        "db_profile": {},
    },
    "ingestion_model": {
        "resources": [
            {"name": "instruments", "pipeline": [{"vertex": INSTRUMENT}]},
            {"name": "issuers", "pipeline": [{"vertex": ISSUER}]},
            {
                "name": "links",
                "pipeline": [
                    {"vertex": INSTRUMENT, "lookup_only": True},
                    {"vertex": ISSUER, "lookup_only": True},
                    {
                        "from": INSTRUMENT,
                        "to": ISSUER,
                        "relation": RELATION,
                        "source_match": "by_isin",
                        "target_match": "by_lei",
                    },
                ],
            },
        ]
    },
    "bindings": {},
}


def _write_sources(root: Path) -> None:
    (root / "instruments.csv").write_text(INSTRUMENTS_CSV)
    (root / "issuers.csv").write_text(ISSUERS_CSV)
    (root / "links.csv").write_text(LINKS_CSV)


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


BACKENDS = [
    pytest.param("neo4j", id="neo4j"),
    pytest.param("arango", id="arango"),
    pytest.param("memgraph", id="memgraph"),
    pytest.param("falkordb", id="falkordb"),
    # Endpoints addressed by key rather than matched by property — the case
    # that a write-time MATCH could not have covered.
    pytest.param("postgres", id="postgres"),
    pytest.param("nebula", id="nebula", marks=pytest.mark.nebula),
    pytest.param("tigergraph", id="tigergraph", marks=pytest.mark.slow),
    pytest.param("graflo_backend", id="graflo_backend"),
]

SPACE = "gf_secondary_e2e"


def _config(flavor: str, output_dir: Path):
    from graflo.db import (
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
    if flavor == "nebula":
        config = NebulaConfig.from_docker_env()
        config.uri = f"nebula://localhost:{config.port}"
        config.schema_name = SPACE
        return config
    if flavor == "tigergraph":
        config = TigergraphConfig.from_docker_env()
        config.database = SPACE
        return config
    if flavor == "graflo_backend":
        from graflo.db.graflo_backend.config import GraFloBackendConfig

        return GraFloBackendConfig(output_dir=output_dir)
    config = PostgresConfig.from_docker_env()
    config.database = config.database or "postgres"
    config.schema_name = SPACE
    return config


@pytest.fixture(scope="module")
def ingested(request: pytest.FixtureRequest, tmp_path_factory) -> Iterator[Any]:
    """Run the three-resource ingest once per backend."""
    flavor = request.param
    root = tmp_path_factory.mktemp("gf_secondary_e2e")
    _write_sources(root)

    try:
        config = _config(flavor, root / "_backend_out")
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{flavor} config unavailable: {error}")

    try:
        ConnectionManager(connection_config=config).__enter__().close()
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{flavor} unreachable: {error}")

    # Deliberately not guarded: an ingest failure is a real defect, and
    # swallowing it as a skip is how a broken backend goes unnoticed.
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
        yield flavor, db

    try:
        with ConnectionManager(connection_config=config) as db:
            db.delete_graph_structure(
                vertex_types=(INSTRUMENT, ISSUER), delete_all=False
            )
    except Exception:
        pass


def _instruments(db) -> list[dict[str, Any]]:
    return db.fetch_docs(INSTRUMENT, return_keys=["sid", "isin"])


def _issuers(db) -> list[dict[str, Any]]:
    return db.fetch_docs(ISSUER, return_keys=["iid", "lei"])


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_edge_only_source_creates_no_extra_vertices(ingested) -> None:
    """lookup_only endpoints must not be written, even keyed by a business key."""
    _, db = ingested
    instruments = _instruments(db)
    issuers = _issuers(db)
    assert sorted(v["sid"] for v in instruments) == ["S1", "S2", "S3"]
    assert sorted(v["iid"] for v in issuers) == ["I1", "I2"]


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_seeded_vertices_keep_their_primary_identity(ingested) -> None:
    """The edge-only source must not overwrite or blank the primary keys."""
    _, db = ingested
    by_sid = {v["sid"]: v for v in _instruments(db)}
    assert by_sid["S1"]["isin"] == "US001"
    assert by_sid["S3"]["isin"] == "US003"


@pytest.mark.parametrize("ingested", BACKENDS, indirect=True)
def test_edges_attach_to_the_resolved_primary_keys(ingested) -> None:
    """Every link row becomes an edge between the right primary identities."""
    flavor, db = ingested
    pairs = _edge_pairs(flavor, db)
    assert sorted(pairs) == [("S1", "I1"), ("S2", "I2"), ("S3", "I1")]


CYPHER_PAIRS = (
    f"MATCH (s:{INSTRUMENT})-[:{RELATION}]->(t:{ISSUER}) "
    "RETURN s.sid AS sid, t.iid AS iid"
)


def _edge_pairs(flavor: str, db) -> list[tuple[str, str]]:
    """Read back (instrument.sid, issuer.iid) pairs for the ingested relation."""
    if flavor == "neo4j":
        rows = db.execute(CYPHER_PAIRS).data()
        return [(row["sid"], row["iid"]) for row in rows]

    if flavor in ("memgraph", "falkordb"):
        # Both expose rows positionally via result_set, not a .data() mapping.
        result = db.execute(CYPHER_PAIRS)
        return [(row[0], row[1]) for row in result.result_set]

    if flavor == "arango":
        collection = f"{INSTRUMENT}_{ISSUER}_edges"
        rows = db.execute(
            f"FOR e IN {collection} "
            f"LET s = DOCUMENT(e._from) LET t = DOCUMENT(e._to) "
            "RETURN {sid: s.sid, iid: t.iid}"
        )
        return [(row["sid"], row["iid"]) for row in rows]

    if flavor == "nebula":
        result = db._execute(
            f"MATCH (s:`{INSTRUMENT}`)-[:`{RELATION}`]->(t:`{ISSUER}`) "
            f"RETURN s.`{INSTRUMENT}`.sid AS sid, t.`{ISSUER}`.iid AS iid"
        )
        return [(row["sid"], row["iid"]) for row in result.rows_as_dicts()]

    if flavor == "tigergraph":
        pairs: list[tuple[str, str]] = []
        for vertex in db.fetch_docs(INSTRUMENT, return_keys=["sid"]):
            sid = vertex["sid"]
            for edge in db.fetch_edges(INSTRUMENT, sid, edge_type=RELATION):
                target = edge.get("to_id") or edge.get("to") or edge.get("target_id")
                if target is not None:
                    pairs.append((sid, str(target)))
        return pairs

    if flavor == "graflo_backend":
        return [
            (triple[0]["sid"], triple[1]["iid"])
            for triple in db.fetch_all_edges(INSTRUMENT, ISSUER, RELATION)
        ]

    table = f"{INSTRUMENT}_{ISSUER}_{RELATION}_edges"
    rows = db.read(f'SELECT source_id, target_id FROM "{SPACE}"."{table}"')
    return [(row["source_id"], row["target_id"]) for row in rows]
