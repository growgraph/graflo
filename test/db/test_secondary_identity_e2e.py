"""End-to-end ingest of an edge-only source keyed by secondary identities.

The scenario this feature exists for: instruments and issuers are loaded with
their own primary keys, then a third source relates them using only business
keys (ISIN, LEI) that appear nowhere as a primary identity.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

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
    pytest.param("postgres", id="postgres"),
]


def _config(flavor: str):
    from graflo.db import ArangoConfig, Neo4jConfig, PostgresConfig

    if flavor == "neo4j":
        config = Neo4jConfig.from_docker_env()
        config.database = config.database or "_system"
        return config
    if flavor == "arango":
        config = ArangoConfig.from_docker_env()
        config.database = "gf_secondary_e2e"
        return config
    config = PostgresConfig.from_docker_env()
    config.database = config.database or "postgres"
    config.schema_name = "gf_secondary_e2e"
    return config


@pytest.fixture(scope="module")
def ingested(request: pytest.FixtureRequest, tmp_path_factory) -> Iterator[Any]:
    """Run the three-resource ingest once per backend."""
    flavor = request.param
    root = tmp_path_factory.mktemp("gf_secondary_e2e")
    _write_sources(root)

    try:
        config = _config(flavor)
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{flavor} config unavailable: {error}")

    manifest = _manifest(root)
    engine = GraphEngine(target_db_flavor=config.connection_type)
    try:
        engine.define_schema(
            manifest=manifest, target_db_config=config, recreate_schema=True
        )
        engine.ingest(
            manifest=manifest,
            target_db_config=config,
            ingestion_params=IngestionParams(n_cores=1, clear_data=False),
        )
    except Exception as error:  # pragma: no cover - environment dependent
        pytest.skip(f"{flavor} ingest failed: {error}")

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


def _edge_pairs(flavor: str, db) -> list[tuple[str, str]]:
    """Read back (instrument.sid, issuer.iid) pairs for the ingested relation."""
    if flavor == "neo4j":
        rows = db.execute(
            f"MATCH (s:{INSTRUMENT})-[:{RELATION}]->(t:{ISSUER}) "
            "RETURN s.sid AS sid, t.iid AS iid"
        ).data()
        return [(row["sid"], row["iid"]) for row in rows]

    if flavor == "arango":
        collection = f"{INSTRUMENT}_{ISSUER}_edges"
        rows = db.execute(
            f"FOR e IN {collection} "
            f"LET s = DOCUMENT(e._from) LET t = DOCUMENT(e._to) "
            "RETURN {sid: s.sid, iid: t.iid}"
        )
        return [(row["sid"], row["iid"]) for row in rows]

    table = f"{INSTRUMENT}_{ISSUER}_{RELATION}_edges"
    rows = db.read(f'SELECT source_id, target_id FROM "gf_secondary_e2e"."{table}"')
    return [(row["source_id"], row["target_id"]) for row in rows]
