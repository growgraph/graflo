from __future__ import annotations

import asyncio

import pytest

from graflo.architecture.contract.ingestion import IngestionModel
from graflo.architecture.graph_types import GraphContainer
from graflo.architecture.schema import (
    CoreSchema,
    GraphMetadata,
    Schema,
)
from graflo.architecture.schema.database_features import DatabaseProfile
from graflo.architecture.schema.edge import Edge, EdgeConfig
from graflo.architecture.schema.vertex import Field, Vertex, VertexConfig
from graflo.connections.onto import ArangoConfig, Neo4jConfig, TigergraphConfig
from graflo.hq.db_writer import DBWriter
from graflo.hq.endpoint_resolve import AmbiguousEndpointError
from graflo.onto import DBType


class _FakeDB:
    def __init__(self):
        self.upsert_calls: list[tuple[list[dict], str, list[str]]] = []

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        self.upsert_calls.append((docs, class_name, list(match_keys)))

    def insert_return_batch(self, docs, class_name):
        raise AssertionError("insert_return_batch must not be used for blank vertices")


class _FakeConnectionManager:
    db = _FakeDB()

    def __init__(self, connection_config):
        self.connection_config = connection_config

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc, tb):
        return False


def _build_schema() -> Schema:
    vertex_config = VertexConfig(
        vertices=[
            Vertex(name="blank_v", properties=[], identity=[], blank=True),
            Vertex(name="target_v", properties=[Field(name="id")], identity=["id"]),
        ],
    )
    edge_config = EdgeConfig(edges=[Edge(source="blank_v", target="target_v")])
    schema = Schema(
        metadata=GraphMetadata(name="test"),
        core_schema=CoreSchema(vertex_config=vertex_config, edge_config=edge_config),
        db_profile=DatabaseProfile(db_flavor=DBType.NEO4J),
    )
    return schema


def _build_ingestion_model(schema: Schema) -> IngestionModel:
    ingestion_model = IngestionModel(resources=[])
    ingestion_model.finish_init(schema.core_schema)
    return ingestion_model


def test_push_vertices_blank_uses_python_generated_identity(monkeypatch):
    schema = _build_schema()
    writer = DBWriter(
        schema=schema,
        ingestion_model=_build_ingestion_model(schema),
        dry=False,
        max_concurrent=1,
    )
    gc = GraphContainer(vertices={"blank_v": [{}]}, edges={}, linear=[])

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _FakeConnectionManager)

    conn_conf = ArangoConfig(uri="http://localhost:8529", username="root", password="x")
    asyncio.run(writer._push_vertices(gc, conn_conf))

    assert "_key" in gc.vertices["blank_v"][0]
    assert isinstance(gc.vertices["blank_v"][0]["_key"], str)
    assert gc.vertices["blank_v"][0]["_key"]


def test_resolve_blank_edges_prefers_identity_join_over_zip():
    schema = _build_schema()
    writer = DBWriter(
        schema=schema,
        ingestion_model=_build_ingestion_model(schema),
        dry=False,
        max_concurrent=1,
    )
    gc = GraphContainer(
        vertices={
            "blank_v": [{"id": "b-2"}, {"id": "b-1"}],
            "target_v": [{"id": "b-1"}, {"id": "b-2"}],
        },
        edges={},
        linear=[],
    )

    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")
    writer._resolve_blank_edges(gc, conn_conf)
    edge_id = ("blank_v", "target_v", None)
    pairs = gc.edges[edge_id]

    assert len(pairs) == 2
    assert pairs[0][0]["id"] == pairs[0][1]["id"]
    assert pairs[1][0]["id"] == pairs[1][1]["id"]


def test_blank_vertex_default_identity_depends_on_db_flavor():
    arango_cfg = VertexConfig(
        vertices=[Vertex(name="blank_v", properties=[], identity=[], blank=True)],
    )
    neo4j_cfg = VertexConfig(
        vertices=[Vertex(name="blank_v", properties=[], identity=[], blank=True)],
    )
    arango_cfg.finish_init()
    neo4j_cfg.finish_init()

    assert arango_cfg["blank_v"].identity == ["id"]
    assert neo4j_cfg["blank_v"].identity == ["id"]


def test_max_concurrent_bounds_concurrent_write_calls(monkeypatch):
    """One writer instance serves sibling in-flight batches; the semaphore is
    shared, so max_concurrent bounds DB operations across all of them."""
    schema = _build_schema()
    writer = DBWriter(
        schema=schema,
        ingestion_model=_build_ingestion_model(schema),
        dry=False,
        max_concurrent=1,
    )

    state = {"active": 0, "high_water": 0}

    class _CountingDB(_FakeDB):
        def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
            state["active"] += 1
            state["high_water"] = max(state["high_water"], state["active"])
            try:
                import time

                time.sleep(0.02)
                super().upsert_docs_batch(docs, class_name, match_keys, **kwargs)
            finally:
                state["active"] -= 1

    class _CountingConnectionManager(_FakeConnectionManager):
        db = _CountingDB()

    monkeypatch.setattr(
        "graflo.hq.db_writer.ConnectionManager", _CountingConnectionManager
    )
    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")

    def _gc() -> GraphContainer:
        return GraphContainer(
            vertices={"target_v": [{"id": "a"}], "blank_v": [{}]},
            edges={},
            linear=[],
        )

    async def _two_concurrent_writes() -> None:
        await asyncio.gather(
            writer._push_vertices(_gc(), conn_conf),
            writer._push_vertices(_gc(), conn_conf),
        )

    asyncio.run(_two_concurrent_writes())
    assert state["high_water"] == 1


def _high_water_of_concurrent_writes(
    monkeypatch, conn_conf, max_concurrent: int
) -> int:
    schema = _build_schema()
    writer = DBWriter(
        schema=schema,
        ingestion_model=_build_ingestion_model(schema),
        max_concurrent=max_concurrent,
    )
    state = {"active": 0, "high_water": 0}

    class _SlowDB(_FakeDB):
        def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
            import time

            state["active"] += 1
            state["high_water"] = max(state["high_water"], state["active"])
            time.sleep(0.02)
            state["active"] -= 1

    class _SlowConnectionManager(_FakeConnectionManager):
        db = _SlowDB()

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _SlowConnectionManager)

    async def _writes() -> None:
        await asyncio.gather(
            *(
                # Two vertex types, so no per-collection lock serializes them.
                writer._push_vertices(
                    GraphContainer(
                        vertices={"target_v": [{"id": str(n)}], "blank_v": [{}]},
                        edges={},
                        linear=[],
                    ),
                    conn_conf,
                )
                for n in range(2)
            )
        )

    asyncio.run(_writes())
    return state["high_water"]


def test_the_file_backend_keeps_its_concurrency(monkeypatch, tmp_path):
    from graflo.connections.graflo_backend import GraFloBackendConfig

    conn_conf = GraFloBackendConfig(output_dir=tmp_path)

    assert _high_water_of_concurrent_writes(monkeypatch, conn_conf, 4) > 1


def test_other_targets_keep_their_concurrency(monkeypatch):
    conn_conf = ArangoConfig(uri="http://localhost:8529", username="root", password="x")

    assert _high_water_of_concurrent_writes(monkeypatch, conn_conf, 4) > 1


# -- extra weights: stored vertex fields copied onto a document's edges ---------


def _employment_schema() -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "hr"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {"name": "person", "properties": ["id"], "identity": ["id"]},
                        {"name": "org", "properties": ["id"], "identity": ["id"]},
                        {
                            "name": "contract",
                            "properties": ["id", "grade"],
                            "identity": ["id"],
                        },
                    ]
                },
                "edge_config": {
                    "edges": [
                        {"source": "person", "target": "org", "relation": "works_at"}
                    ]
                },
            },
            "db_profile": {"db_flavor": "neo4j"},
        }
    )


def _employment_model(schema: Schema) -> IngestionModel:
    model = IngestionModel.model_validate(
        {
            "resources": [
                {
                    "name": "employments",
                    "pipeline": [
                        {"vertex": "person", "from": {"id": "person"}},
                        {"vertex": "org", "from": {"id": "org"}},
                        {"vertex": "contract", "from": {"id": "contract"}},
                        {
                            "edge": {
                                "from": "person",
                                "to": "org",
                                "relation": "works_at",
                            }
                        },
                    ],
                    "extra_weights": [
                        {
                            "edge": {
                                "source": "person",
                                "target": "org",
                                "relation": "works_at",
                            },
                            "vertex_weights": [
                                {"name": "contract", "fields": ["grade"]}
                            ],
                        }
                    ],
                    "infer_edges": False,
                }
            ],
            "transforms": [],
        }
    )
    model.finish_init(schema.core_schema)
    return model


class _StoredContracts:
    """A database that holds contracts, with the grades another resource wrote."""

    stored = {"c1": {"id": "c1", "grade": "A"}, "c2": {"id": "c2", "grade": "B"}}

    def resolve_vertices(self, class_name, key_docs, match_keys, return_keys):
        assert (class_name, tuple(match_keys)) == ("contract", ("id",))
        return {
            index: [{key: self.stored[doc["id"]][key] for key in return_keys}]
            for index, doc in enumerate(key_docs)
            if doc.get("id") in self.stored
        }


def test_extra_weights_reach_the_edges_of_the_document_they_belong_to(monkeypatch):
    schema = _employment_schema()
    model = _employment_model(schema)
    runtime = model.fetch_resource("employments")
    writer = DBWriter(schema=schema, ingestion_model=model)

    class _Manager(_FakeConnectionManager):
        db = _StoredContracts()

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _Manager)
    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")

    # The second row has no contract, so contracts and rows do not line up by
    # position; the third names one the database does not hold.
    gc = GraphContainer.from_docs_list(
        [
            runtime({"person": "p1", "org": "o1", "contract": "c2"}),
            runtime({"person": "p2", "org": "o1"}),
            runtime({"person": "p3", "org": "o2", "contract": "c9"}),
            runtime({"person": "p4", "org": "o2", "contract": "c1"}),
        ]
    )

    asyncio.run(writer._enrich_extra_weights(gc, conn_conf, runtime))

    grades = {
        source["id"]: weight.get("contract@grade")
        for source, _, weight in gc.edges[("person", "org", "works_at")]
    }
    assert grades == {"p1": "B", "p2": None, "p3": None, "p4": "A"}


def test_an_edge_the_schema_does_not_declare_is_reported_once(caplog):
    schema = _employment_schema()
    writer = DBWriter(schema=schema, ingestion_model=_employment_model(schema))
    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")
    undeclared = ("org", "person", "employs")

    def _gc() -> GraphContainer:
        return GraphContainer(
            edges={undeclared: [({"id": "o1"}, {"id": "p1"}, {})]}, linear=[]
        )

    with caplog.at_level("WARNING", logger="graflo.hq.db_writer"):
        asyncio.run(writer._push_edges(_gc(), conn_conf))
        asyncio.run(writer._push_edges(_gc(), conn_conf))

    reports = [r for r in caplog.records if "does not declare" in r.getMessage()]
    assert len(reports) == 1
    assert str(undeclared) in reports[0].getMessage()


# -- attach: records written onto the vertices their secondary identity finds ----


def _cmdb_schema(flavor: str = "neo4j") -> Schema:
    return Schema.model_validate(
        {
            "metadata": {"name": "cmdb"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "ci",
                            "properties": ["id", "serial", "site", "tag", "owner"],
                            "identity": ["id"],
                            "secondary_identities": [
                                {"name": "by_serial", "fields": ["serial"]},
                                {"name": "by_site_tag", "fields": ["site", "tag"]},
                            ],
                        },
                        {
                            "name": "probe",
                            "properties": ["name", "serial", "owner"],
                            "hash_identity_properties": ["name"],
                            "secondary_identities": [
                                {"name": "by_serial", "fields": ["serial"]}
                            ],
                        },
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {"db_flavor": flavor},
        }
    )


def _cmdb_model(schema: Schema, policy: str = "all") -> IngestionModel:
    model = IngestionModel.model_validate(
        {
            "resources": [
                {
                    "name": "owners",
                    "pipeline": [{"vertex": "ci", "from": {"id": "id"}}],
                },
                {
                    "name": "attach",
                    "pipeline": [
                        {
                            "vertex": "ci",
                            "find": "by_serial",
                            "from": {"serial": "serial", "owner": "owner"},
                        }
                    ],
                },
            ],
            "transforms": [],
            "endpoints_on_ambiguous": policy,
        }
    )
    model.finish_init(schema.core_schema)
    return model


class _AttachDB:
    """Holds stored vertices; records every resolve and upsert it is asked for."""

    def __init__(self, stored: list[dict] | None = None) -> None:
        self.stored = stored or []
        self.upserts: list[dict] = []
        self.resolves: list[tuple] = []

    def upsert_docs_batch(self, docs, class_name, match_keys, **kwargs):
        self.upserts.append(
            {
                "docs": list(docs),
                "class_name": class_name,
                "match_keys": list(match_keys),
                **kwargs,
            }
        )

    def resolve_vertices(self, class_name, key_docs, match_keys, return_keys, **kw):
        self.resolves.append((class_name, tuple(match_keys), tuple(return_keys)))
        found: dict[int, list[dict]] = {}
        for position, doc in enumerate(key_docs):
            key = tuple(doc.get(f) for f in match_keys)
            if None in key:
                continue
            hits = [
                {f: node.get(f) for f in return_keys}
                for node in self.stored
                if tuple(node.get(f) for f in match_keys) == key
            ]
            if hits:
                found[position] = hits
        return found


def _manager_for(db: _AttachDB) -> type:
    class _Manager:
        def __init__(self, connection_config):
            self.connection_config = connection_config

        def __enter__(self) -> _AttachDB:
            return db

        def __exit__(self, exc_type, exc, tb):
            return False

    return _Manager


def _attach_write(
    monkeypatch,
    gc: GraphContainer,
    db: _AttachDB,
    *,
    policy: str = "all",
    dry: bool = False,
    schema: Schema | None = None,
    conn_conf=None,
    resource_name: str | None = "attach",
) -> DBWriter:
    schema = schema or _cmdb_schema()
    writer = DBWriter(
        schema=schema, ingestion_model=_cmdb_model(schema, policy), dry=dry
    )

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager_for(db))
    conn_conf = conn_conf or Neo4jConfig(
        uri="bolt://localhost:7687", username="u", password="p"
    )
    asyncio.run(writer.write(gc=gc, conn_conf=conn_conf, resource_name=resource_name))
    return writer


def _attached(
    docs: list[dict], selector: str = "by_serial", vertex: str = "ci"
) -> GraphContainer:
    return GraphContainer(
        vertices={}, edges={}, attached={vertex: docs}, attached_by={vertex: selector}
    )


class TestAttachVertices:
    def test_owners_and_attached_records_are_upserted_in_separate_calls(
        self, monkeypatch
    ):
        gc = _attached([{"serial": "S1", "owner": "ann"}])
        gc.vertices["ci"] = [{"id": "c1"}]
        db = _AttachDB(stored=[{"id": "c0", "serial": "S1"}])

        writer = _attach_write(monkeypatch, gc, db)

        owners, attached = db.upserts
        assert owners["docs"] == [{"id": "c1"}]
        assert attached["docs"] == [{"serial": "S1", "owner": "ann", "id": "c0"}]
        assert attached["class_name"] == "ci" and attached["match_keys"] == ["id"]
        assert attached["update_keys"] == "doc" and attached["filter_uniques"]
        assert db.resolves == [("ci", ("serial",), ("id",))]
        stats = writer.stats.attached["ci"]
        assert (stats.documents, stats.attached, stats.written) == (1, 1, 1)

    def test_an_unmatched_record_is_counted_and_never_written(self, monkeypatch):
        gc = _attached(
            [{"serial": "S1", "owner": "ann"}, {"serial": "NOPE", "owner": "bob"}]
        )
        db = _AttachDB(stored=[{"id": "c0", "serial": "S1"}])

        writer = _attach_write(monkeypatch, gc, db)

        (attached,) = db.upserts
        assert attached["docs"] == [{"serial": "S1", "owner": "ann", "id": "c0"}]
        stats = writer.stats.attached["ci"]
        assert (stats.documents, stats.attached, stats.unmatched) == (2, 1, 1)

    def test_attaching_never_creates_a_vertex(self, monkeypatch):
        db = _AttachDB(stored=[])

        writer = _attach_write(
            monkeypatch, _attached([{"serial": "S9", "owner": "x"}]), db
        )

        assert db.upserts == []
        assert writer.stats.attached["ci"].unmatched == 1

    @pytest.mark.parametrize(
        ("policy", "ids"),
        [("all", ["c2", "c1"]), ("first", ["c1"]), ("skip", [])],
    )
    def test_an_ambiguous_record_follows_the_policy(self, monkeypatch, policy, ids):
        db = _AttachDB(
            stored=[{"id": "c2", "serial": "S1"}, {"id": "c1", "serial": "S1"}]
        )

        writer = _attach_write(
            monkeypatch,
            _attached([{"serial": "S1", "owner": "ann"}]),
            db,
            policy=policy,
        )

        written = [doc for call in db.upserts for doc in call["docs"]]
        assert [doc["id"] for doc in written] == ids
        assert all(doc["owner"] == "ann" for doc in written)
        stats = writer.stats.attached["ci"]
        assert stats.ambiguous == 1 and stats.written == len(ids)

    def test_an_ambiguous_record_raises_under_error(self, monkeypatch):
        db = _AttachDB(
            stored=[{"id": "c2", "serial": "S1"}, {"id": "c1", "serial": "S1"}]
        )
        with pytest.raises(AmbiguousEndpointError, match="S1"):
            _attach_write(
                monkeypatch,
                _attached([{"serial": "S1", "owner": "ann"}]),
                db,
                policy="error",
            )
        assert db.upserts == []

    def test_a_partial_composite_key_is_unresolvable(self, monkeypatch):
        db = _AttachDB(stored=[{"id": "c0", "site": "x", "tag": "t"}])

        writer = _attach_write(
            monkeypatch,
            _attached([{"site": "x", "owner": "ann"}], selector="by_site_tag"),
            db,
            resource_name=None,
        )

        assert db.upserts == []
        stats = writer.stats.attached["ci"]
        assert (stats.documents, stats.unresolvable, stats.written) == (1, 1, 0)

    def test_a_dry_run_resolves_and_writes_nothing(self, monkeypatch):
        class _NoResolve(_AttachDB):
            def resolve_vertices(self, *args, **kwargs):
                raise AssertionError("a dry run must not read the database")

        db = _NoResolve()

        writer = _attach_write(
            monkeypatch, _attached([{"serial": "S1", "owner": "ann"}]), db, dry=True
        )

        assert db.upserts == []
        assert writer.stats.attached["ci"].documents == 1

    def test_a_hash_keyed_vertex_mirrors_the_arango_key(self, monkeypatch):
        schema = _cmdb_schema("arango")
        (identity,) = schema.core_schema.vertex_config.identity_fields("probe")
        db = _AttachDB(stored=[{identity: "h1", "serial": "S1"}])

        _attach_write(
            monkeypatch,
            _attached([{"serial": "S1", "owner": "ann"}], vertex="probe"),
            db,
            schema=schema,
            conn_conf=ArangoConfig(
                uri="http://localhost:8529", username="root", password="x"
            ),
            resource_name=None,
        )

        ((doc,),) = [call["docs"] for call in db.upserts]
        assert doc[identity] == "h1" and doc["_key"] == "h1"

    def test_the_summary_and_warning_report_the_counts(self, monkeypatch, caplog):
        db = _AttachDB(stored=[{"id": "c0", "serial": "S1"}])
        gc = _attached([{"serial": "S1"}, {"serial": "NOPE"}])

        with caplog.at_level("WARNING", logger="graflo.hq.db_writer"):
            writer = _attach_write(monkeypatch, gc, db)

        summary = writer.stats.summary()
        assert "ci" in summary and "unmatched=1" in summary
        assert not writer.stats.is_empty()
        (warning,) = [r.getMessage() for r in caplog.records]
        assert "ci" in warning and "by_serial" in warning and "unmatched=1" in warning

    def test_records_landing_on_one_vertex_become_one_row(self, monkeypatch):
        db = _AttachDB(stored=[{"id": "c0", "serial": "S1"}])
        gc = _attached(
            [
                {"serial": "S1", "owner": "ann", "site": "x"},
                {"serial": "S1", "owner": "bob", "tag": "t"},
            ]
        )

        writer = _attach_write(monkeypatch, gc, db)

        (call,) = db.upserts
        assert call["docs"] == [
            {"serial": "S1", "owner": "bob", "site": "x", "tag": "t", "id": "c0"}
        ]
        stats = writer.stats.attached["ci"]
        assert (stats.attached, stats.written) == (2, 1)

    def test_a_match_without_a_primary_identity_is_unmatched(self, monkeypatch):
        db = _AttachDB(stored=[{"serial": "S1"}])

        writer = _attach_write(
            monkeypatch, _attached([{"serial": "S1", "owner": "ann"}]), db
        )

        assert db.upserts == []
        stats = writer.stats.attached["ci"]
        assert (stats.unmatched, stats.attached, stats.written) == (1, 0, 0)

    def test_an_attached_class_without_a_selector_is_refused(self, monkeypatch):
        gc = GraphContainer(attached={"ci": [{"serial": "S1"}]})
        db = _AttachDB(stored=[{"id": "c0", "serial": "S1"}])

        with pytest.raises(ValueError, match="'ci'.*no secondary identity"):
            _attach_write(monkeypatch, gc, db)
        assert db.resolves == [] and db.upserts == []


class TestAttachRefusedInBulk:
    def test_a_resource_that_attaches_is_refused(self, monkeypatch):
        with pytest.raises(ValueError, match="REST ingest"):
            _attach_write_bulk(monkeypatch, GraphContainer(), "attach")

    def test_attached_records_are_refused(self, monkeypatch):
        gc = _attached([{"serial": "S1"}])
        with pytest.raises(ValueError, match="REST ingest"):
            _attach_write_bulk(monkeypatch, gc, None)


def _attach_write_bulk(monkeypatch, gc: GraphContainer, resource_name) -> None:
    schema = _cmdb_schema()
    writer = DBWriter(schema=schema, ingestion_model=_cmdb_model(schema))

    monkeypatch.setattr(
        "graflo.hq.db_writer.ConnectionManager", _manager_for(_AttachDB())
    )
    conn_conf = Neo4jConfig(uri="bolt://localhost:7687", username="u", password="p")
    asyncio.run(
        writer.write(
            gc=gc, conn_conf=conn_conf, resource_name=resource_name, bulk_session_id="s"
        )
    )


def test_attached_records_are_resolved_and_written_under_stored_names(monkeypatch):
    """``from`` and ``type`` are TigerGraph reserved words, so both are renamed."""
    schema = Schema.model_validate(
        {
            "metadata": {"name": "routes"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "route",
                            "properties": ["from", "type", "name"],
                            "identity": ["from"],
                            "secondary_identities": [
                                {"name": "by_type", "fields": ["type"]}
                            ],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
            "db_profile": {"db_flavor": "tigergraph"},
        }
    )
    model = IngestionModel(resources=[])
    model.finish_init(schema.core_schema)
    writer = DBWriter(schema=schema, ingestion_model=model)
    db = _AttachDB(stored=[{"from_attr": "A", "type_attr": "bus"}])

    monkeypatch.setattr("graflo.hq.db_writer.ConnectionManager", _manager_for(db))
    gc = _attached([{"type": "bus", "name": "x"}], selector="by_type", vertex="route")
    conn_conf = TigergraphConfig(
        uri="http://localhost:14240", username="u", password="p"
    )

    asyncio.run(writer.write(gc=gc, conn_conf=conn_conf, resource_name=None))

    assert db.resolves == [("route", ("type_attr",), ("from_attr",))]
    (call,) = db.upserts
    assert call["docs"] == [{"type_attr": "bus", "name": "x", "from_attr": "A"}]
    assert call["match_keys"] == ["from_attr"]
    assert gc.attached["route"] == [{"type": "bus", "name": "x"}]
