"""The bulk contract on ArangoDB, resume after an interrupted write, and refusal.

ArangoDB writes no ``merge: union`` field, so the contract manifest is run
without the union.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator

import pytest

from graflo.connections.onto import ArangoConfig
from graflo.db.arango.conn import ArangoConnection
from graflo.db.arango.keys import edge_key
from graflo.db.manager import ConnectionManager
from graflo.hq.graph_engine import GraphEngine
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk import contract
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest
from test.db.backends import config_for


@pytest.fixture
def arango_contract(monkeypatch) -> None:
    monkeypatch.setattr(contract, "MANIFEST", contract.without_union(contract.MANIFEST))


@pytest.fixture
def fresh_target() -> Iterator[Callable[[str], ArangoConfig]]:
    """New ArangoDB databases, one per label, dropped after the test."""
    made: list[ArangoConfig] = []
    run = uuid.uuid4().hex[:8]

    def fresh(label: str) -> ArangoConfig:
        conf = config_for("arango", space=f"bulk_{label}_{run}")
        made.append(conf)
        return conf

    yield fresh
    for conf in made:
        assert conf.database is not None
        ArangoConnection(conf).delete_database(conf.database)


def test_a_bulk_load_holds_the_graph_an_upsert_holds(
    arango_contract, fresh_target, tmp_path
) -> None:
    graph = assert_bulk_equals_upsert(fresh_target, tmp_path / "staging")

    assert len(graph[str(("Person", "City", "lives_in"))]) == 5


def test_both_paths_key_documents_alike_so_an_upsert_adds_nothing(
    arango_contract, fresh_target, tmp_path
) -> None:
    conf = fresh_target("keys")
    ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))
    loaded = export(conf)

    # Upserted onto the load, not into a new schema: every key must be found.
    GraphEngine(target_db_flavor=conf.connection_type).ingest(
        contract.manifest(),
        conf,
        IngestionParams(batch_size=2),
        data_source_registry=contract._registry(),
    )

    assert export(conf) == loaded
    with ConnectionManager(connection_config=conf) as db:
        keys = sorted(db.execute("FOR p IN Person RETURN p._key"))
        edge = next(
            iter(
                db.execute(
                    "FOR e IN Person_City_edges FILTER e._from == 'Person/p1' "
                    "AND e._to == 'City/c1' RETURN e"
                )
            )
        )
    assert keys == ["p1", "p2", "p3"]
    assert edge["_key"] == edge_key("Person/p1", "City/c1", "lives_in", ["home"])


@pytest.mark.parametrize("phase", ["bulk_write_vertices", "bulk_write_edges"])
def test_an_interrupted_bulk_write_resumes_to_the_same_graph(
    arango_contract, fresh_target, tmp_path, monkeypatch, phase
) -> None:
    upsert_conf = fresh_target("upsert")
    ingest(upsert_conf, IngestionParams(batch_size=2))
    bulk_conf = fresh_target("bulk")
    params = IngestionParams(
        mode="bulk", staging_dir=tmp_path / "staging", batch_size=2
    )
    real = getattr(ArangoConnection, phase)

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(ArangoConnection, phase, _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(bulk_conf, params)
    monkeypatch.setattr(ArangoConnection, phase, real)
    ingest(bulk_conf, params)

    assert export(bulk_conf) == export(upsert_conf)


def test_a_bulk_load_refuses_a_target_that_holds_a_schema(
    arango_contract, fresh_target, tmp_path
) -> None:
    conf = fresh_target("held")
    ingest(conf, IngestionParams())
    with pytest.raises(Exception, match="already exist"):
        ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))
