"""The bulk contract on NebulaGraph, resume after an interrupted write, and refusal.

NebulaGraph stores no list property, so the contract manifest is run without
the ``tags`` list; ``since`` is typed so it is stored as an integer.
"""

from __future__ import annotations

import copy
import json
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from graflo.connections.onto import NebulaConfig
from graflo.db.nebula.conn import NebulaConnection
from graflo.db.nebula.util import fitted_vid_length
from graflo.hq.ingestion_parameters import IngestionParams
from test.bulk import contract
from test.bulk.contract import assert_bulk_equals_upsert, export, ingest
from test.db.backends import config_for

pytestmark = pytest.mark.nebula


def _nebula_manifest() -> dict[str, Any]:
    manifest = copy.deepcopy(contract.MANIFEST)
    core = manifest["schema"]["core_schema"]
    person = core["vertex_config"]["vertices"][0]
    person["properties"] = [p for p in person["properties"] if isinstance(p, str)]
    edge = core["edge_config"]["edges"][0]
    edge["properties"] = ["role", {"name": "since", "type": "INT"}]
    steps = manifest["ingestion_model"]["resources"][0]["apply"]
    steps[0]["from"].pop("tags")
    return manifest


@pytest.fixture
def nebula_contract(monkeypatch) -> None:
    monkeypatch.setattr(contract, "MANIFEST", _nebula_manifest())


@pytest.fixture
def fresh_target() -> Iterator[Callable[[str], NebulaConfig]]:
    """New NebulaGraph spaces, one per label, dropped after the test."""
    made: list[NebulaConfig] = []
    run = uuid.uuid4().hex[:8]

    def fresh(label: str) -> NebulaConfig:
        conf = config_for("nebula", space=f"bulk_{label}_{run}")
        made.append(conf)
        return conf

    yield fresh
    for conf in made:
        db = NebulaConnection(conf.model_copy(update={"schema_name": None}))
        try:
            assert conf.schema_name is not None
            db.delete_database(conf.schema_name)
        finally:
            db.close()


def test_a_bulk_load_holds_the_graph_an_upsert_holds(
    nebula_contract, fresh_target, tmp_path
) -> None:
    graph = assert_bulk_equals_upsert(fresh_target, tmp_path / "staging")

    people = [json.loads(d) for d in graph["Person"]]
    ann = next(p for p in people if p["pid"] == "p1")
    assert ann["name"] is None and ann["nick"] is None
    assert len(graph[str(("Person", "City", "lives_in"))]) == 5


def test_the_load_sizes_the_space_and_indexes_after_it(
    nebula_contract, fresh_target, tmp_path
) -> None:
    conf = fresh_target("space")
    ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))

    db = NebulaConnection(conf)
    try:
        [space] = db.execute(f"DESCRIBE SPACE `{conf.schema_name}`").rows_as_dicts()
        indexes = {
            row.get("Index Name", row.get("Name"))
            for row in db.execute("SHOW TAG INDEXES").rows_as_dicts()
        }
        found = db.execute(
            'LOOKUP ON `Person` WHERE `Person`.`pid` == "p1" YIELD id(vertex) AS v'
        ).rows_as_dicts()
    finally:
        db.close()
    schema = contract.manifest().require_schema()
    length = fitted_vid_length(schema, conf.vid_string_length)
    assert space["Vid Type"] == f"FIXED_STRING({length})"
    assert space["Partition Number"] >= 20
    assert {"idx_Person_pid", "idx_City_cid"} <= indexes
    assert found == [{"v": "Person::p1"}]


@pytest.mark.parametrize("phase", ["bulk_write_vertices", "bulk_write_edges"])
def test_an_interrupted_bulk_write_resumes_to_the_same_graph(
    nebula_contract, fresh_target, tmp_path, monkeypatch, phase
) -> None:
    upsert_conf = fresh_target("upsert")
    ingest(upsert_conf, IngestionParams(batch_size=2))
    bulk_conf = fresh_target("bulk")
    params = IngestionParams(
        mode="bulk", staging_dir=tmp_path / "staging", batch_size=2
    )
    real = getattr(NebulaConnection, phase)

    def _fail(self, schema, chunk) -> None:
        real(self, schema, chunk)
        raise RuntimeError("connection lost")

    monkeypatch.setattr(NebulaConnection, phase, _fail)
    with pytest.raises(RuntimeError, match="connection lost"):
        ingest(bulk_conf, params)
    monkeypatch.setattr(NebulaConnection, phase, real)
    ingest(bulk_conf, params)

    assert export(bulk_conf) == export(upsert_conf)


def test_a_bulk_load_refuses_a_target_that_holds_a_schema(
    nebula_contract, fresh_target, tmp_path
) -> None:
    conf = fresh_target("held")
    ingest(conf, IngestionParams())
    with pytest.raises(Exception, match="already exist"):
        ingest(conf, IngestionParams(mode="bulk", staging_dir=tmp_path / "staging"))
