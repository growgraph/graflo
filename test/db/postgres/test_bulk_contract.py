"""The bulk contract on PostgreSQL, and resume after an interrupted write.

PostgreSQL writes no ``merge: union`` field and keeps a stored edge's properties
unless ``edges_on_duplicate`` is ``upsert``, so the contract
manifest is run without the union and with edge upserts.
"""

from __future__ import annotations

import copy
import json
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from graflo.connections.onto import PostgresConfig
from graflo.db.postgres.conn import PostgresConnection
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk import contract
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest
from test.db.backends import config_for


def _postgres_manifest() -> dict[str, Any]:
    manifest = copy.deepcopy(contract.MANIFEST)
    for vertex in manifest["schema"]["core_schema"]["vertex_config"]["vertices"]:
        for prop in vertex["properties"]:
            if isinstance(prop, dict):
                prop.pop("merge", None)
    manifest["ingestion_model"]["edges_on_duplicate"] = "upsert"
    return manifest


@pytest.fixture
def postgres_contract(monkeypatch) -> None:
    monkeypatch.setattr(contract, "MANIFEST", _postgres_manifest())


@pytest.fixture
def fresh_target() -> Iterator[Callable[[str], PostgresConfig]]:
    """New PostgreSQL schemas, one per label, dropped after the test."""
    made: list[PostgresConfig] = []
    run = uuid.uuid4().hex[:8]

    def fresh(label: str) -> PostgresConfig:
        conf = config_for("postgres", space=f"bulk_{label}_{run}")
        made.append(conf)
        return conf

    yield fresh
    for conf in made:
        with PostgresConnection(conf) as db:
            assert conf.schema_name is not None
            db.delete_database(conf.schema_name)


def test_a_bulk_load_holds_the_graph_an_upsert_holds(
    postgres_contract, fresh_target, tmp_path
) -> None:
    graph = assert_bulk_equals_upsert(fresh_target, tmp_path / "staging")

    people = [json.loads(d) for d in graph["Person"]]
    ann = next(p for p in people if p["pid"] == "p1")
    assert ann["name"] is None and ann["nick"] is None
    assert len(graph[str(("Person", "City", "lives_in"))]) == 5


def test_the_load_leaves_keys_and_no_load_log(
    postgres_contract, fresh_target, tmp_path
) -> None:
    conf = fresh_target("keys")
    ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))

    with PostgresConnection(conf) as db:
        constraints = db.read(
            "SELECT c.contype, c.convalidated FROM pg_constraint c "
            "JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = %s",
            (conf.schema_name,),
        )
        tables = {row["table_name"] for row in db.get_tables(conf.schema_name)}
    assert sorted(r["contype"] for r in constraints).count("f") == 2
    assert all(r["convalidated"] for r in constraints)
    assert "_graflo_bulk_log" not in tables


def test_an_interrupted_bulk_write_resumes_to_the_same_graph(
    postgres_contract, fresh_target, tmp_path, monkeypatch
) -> None:
    upsert_conf = fresh_target("upsert")
    ingest(upsert_conf, IngestionParams(batch_size=2))
    bulk_conf = fresh_target("bulk")
    params = IngestionParams(
        mode="bulk", staging_dir=tmp_path / "staging", batch_size=2
    )

    real = PostgresConnection.bulk_write_edges

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(PostgresConnection, "bulk_write_edges", _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(bulk_conf, params)
    monkeypatch.setattr(PostgresConnection, "bulk_write_edges", real)
    ingest(bulk_conf, params)

    assert export(bulk_conf) == export(upsert_conf)


def test_a_bulk_load_refuses_a_target_that_holds_a_schema(
    postgres_contract, fresh_target, tmp_path
) -> None:
    conf = fresh_target("held")
    ingest(conf, IngestionParams())
    with pytest.raises(Exception, match="already exist"):
        ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))
