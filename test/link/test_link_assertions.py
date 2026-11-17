"""Statements written as Assertions, read back, and reconciled with the stored edges."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Literal

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects import LiftSpec, plan_lift
from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.graflo_backend.connection import GraFloBackendConnection
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.container_write import write_container
from graflo.link import reconcile as reconcile_module
from graflo.link.assertions import (
    graph_time,
    read_statements,
    stated_types,
    statement_container,
)
from graflo.link.reconcile import reconcile
from graflo.link.statements import Basis, Statement, entity_key

UNIT = entity_key({"serial": "U-1"}, ["serial"])


def _release(rid: str) -> str:
    return entity_key({"rid": rid}, ["rid"])


def _schema() -> Schema:
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "fleet", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Unit",
                                "properties": [{"name": "serial", "type": "STRING"}],
                                "identity": ["serial"],
                            },
                            {
                                "name": "Release",
                                "properties": [
                                    {"name": "rid", "type": "STRING"},
                                    {"name": "released_at", "type": "DATETIME"},
                                    {"name": "build", "type": "INT"},
                                ],
                                "identity": ["rid"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Unit",
                                "target": "Release",
                                "relation": "runs",
                                "merge": "replace",
                            }
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()
    lifted = apply_evolution(
        manifest,
        plan_lift(
            manifest,
            LiftSpec(mentions_for=["Unit", "Release"]),
            aspects=["provenance"],
        ),
    )
    return lifted.require_schema()


def _t(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=UTC)


def _runs(release: str, record: str, **kw: Any) -> Statement:
    return Statement(
        vertex_type="Unit",
        key=UNIT,
        slot="runs",
        kind="relation",
        value=_release(release),
        target_type="Release",
        record=record,
        quote=f"runs {release}",
        span_start=0,
        span_end=8,
        **kw,
    )


#: 3.1 stated, then denied for 3.0, then 3.2 planned for a later evening; and a
#: planned release date, a property.
STATEMENTS = [
    _runs("R-3.1", "m1"),
    _runs("R-3.1", "m2", polarity="denied"),
    _runs("R-3.0", "m2"),
    _runs("R-3.2", "m3", modality="planned", valid_from=_t(16, 20)),
    Statement(
        vertex_type="Release",
        key=_release("R-3.2"),
        slot="released_at",
        value="2026-10-19",
        modality="planned",
        valid_from=_t(19, 0),
        granularity="day",
        record="doc",
        quote="planned for 19 October",
    ),
    Statement(
        vertex_type="Release",
        key=_release("R-3.2"),
        slot="build",
        value=42,
        basis=Basis.RECORDED,
        record="doc",
    ),
]

RECORDS = [
    {"source_record_id": "m1", "written_at": "2026-10-13T11:05:00+00:00"},
    {"source_record_id": "m2", "written_at": "2026-10-14T12:00:00+00:00"},
    {"source_record_id": "m3", "written_at": "2026-10-16T09:00:00+00:00"},
    {"source_record_id": "doc"},
]


def _write(conf: GraFloBackendConfig, schema: Schema, runs: list[str]) -> None:
    """The entities, the records, the stated ``runs`` edges and the statements."""
    graph = GraphContainer(
        vertices={
            "Unit": [{"serial": "U-1"}],
            "Release": [{"rid": r} for r in ("R-3.0", "R-3.1", "R-3.2", "R-9.9")],
            "SourceRecord": RECORDS,
        },
        edges={
            ("Unit", "Release", "runs"): [
                ({"serial": "U-1"}, {"rid": rid}, {}) for rid in runs
            ]
        },
    )
    asyncio.run(write_container(schema, graph, conf))
    asyncio.run(write_container(schema, statement_container(schema, STATEMENTS), conf))


def test_an_id_is_minted_from_the_record_the_span_and_the_triple() -> None:
    one = _runs("R-3.0", "m2")
    assert one.id == _runs("R-3.0", "m2").id
    assert one.id != _runs("R-3.0", "m3").id
    assert one.id != _runs("R-3.1", "m2").id


def test_one_assertion_is_one_triple() -> None:
    schema = _schema()
    assert stated_types(schema) == {"Unit", "Release"}
    built = statement_container(schema, STATEMENTS)
    docs = {d["assertion_id"]: d for d in built.vertices["Assertion"]}
    relation = docs[STATEMENTS[0].id]
    assert relation["predicate"] == "runs"
    assert "value" not in relation
    assert relation["basis"] == "extracted"
    prop = docs[STATEMENTS[4].id]
    assert (prop["predicate"], prop["value"], prop["modality"]) == (
        "released_at",
        "2026-10-19",
        "planned",
    )
    assert docs[STATEMENTS[5].id]["value"] == "42"
    assert len(built.edges["Assertion", "Unit", "subject"]) == 4
    assert len(built.edges["Assertion", "Release", "object"]) == 4
    assert len(built.edges["Assertion", "Release", "subject"]) == 2
    assert len(built.edges["Assertion", "SourceRecord", "wasDerivedFrom"]) == 6


def test_a_statement_about_an_undeclared_type_is_refused() -> None:
    bad = Statement(vertex_type="Agent", key="{}", slot="label", value="x")
    with pytest.raises(ValueError, match="no Assertion -subject-> Agent"):
        statement_container(_schema(), [bad])


def test_statements_read_back_whole_and_about_one_entity(tmp_path) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    schema = _schema()
    _write(conf, schema, ["R-3.1"])
    with ConnectionManager(connection_config=conf) as conn:
        everything = read_statements(conn, schema)
        unit = read_statements(conn, schema, about=("Unit", {"serial": "U-1"}))
        release = read_statements(conn, schema, about=("Release", {"rid": "R-3.2"}))
        newest = graph_time(conn, schema)

    assert {s.id for s in everything.statements} == {s.id for s in STATEMENTS}
    by_id = {s.id: s for s in everything.statements}
    denied = by_id[STATEMENTS[1].id]
    assert (denied.kind, denied.polarity, denied.value) == (
        "relation",
        "denied",
        _release("R-3.1"),
    )
    assert denied.said_at == _t(14)
    assert (denied.record, denied.quote) == ("m2", "runs R-3.1")
    # A property's value comes back as the property's type.
    assert by_id[STATEMENTS[5].id].value == 42
    assert by_id[STATEMENTS[5].id].basis == Basis.RECORDED
    planned = by_id[STATEMENTS[4].id]
    assert (planned.kind, planned.value, planned.valid_from) == (
        "property",
        "2026-10-19",
        _t(19, 0),
    )
    assert planned.said_at is None

    assert {s.id for s in unit.statements} == {s.id for s in STATEMENTS[:4]}
    assert set(unit.records) == {"m1", "m2", "m3"}
    # R-3.2 is the object of one statement and the subject of two.
    assert {s.id for s in release.statements} == {
        STATEMENTS[3].id,
        STATEMENTS[4].id,
        STATEMENTS[5].id,
    }
    # The graph's own time is its newest record's.
    assert newest == _t(16, 9)


def _stored_runs(monkeypatch, rids: list[str]) -> None:
    """Reconcile sees *rids* as the stored ``runs`` targets, whatever the backend folds."""
    real = reconcile_module.stored_edges

    def stored(conn, schema, edge, **kw):
        if edge.relation != "runs":
            return real(conn, schema, edge, **kw)
        return [({"serial": "U-1"}, {"rid": rid}) for rid in rids]

    monkeypatch.setattr(reconcile_module, "stored_edges", stored)


def test_reconcile_on_a_graph_that_cannot_delete_keeps_and_rewrites(
    tmp_path, monkeypatch
) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    schema = _schema()
    _write(conf, schema, [])
    # The corrected 3.1 and the pending 3.2 are stored; 3.0 is not.
    _stored_runs(monkeypatch, ["R-3.1", "R-3.2", "R-9.9"])
    report = asyncio.run(reconcile(schema, conf))
    assert report.as_of == _t(16, 9)
    assert report.statements == 4
    assert (report.removed, report.kept, report.written) == (
        {},
        {"runs": 2},
        {"runs": 1},
    )
    with ConnectionManager(connection_config=conf) as conn:
        targets = [
            row[1]["rid"] for row in conn.fetch_all_edges("Unit", "Release", "runs")
        ]
    assert targets[-1:] == ["R-3.0"]


def test_reconcile_deletes_what_does_not_hold_and_restores_a_due_plan(
    tmp_path, monkeypatch
) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    schema = _schema()
    _write(conf, schema, [])
    deleted: list[Any] = []

    def _delete(schema, conn_conf, deletes):
        deleted.extend(
            (edge.relation, sorted(pair[1] for pair in pairs))
            for edge, pairs in deletes
        )

    monkeypatch.setattr(GraFloBackendConnection, "supports_instance_delete", True)
    monkeypatch.setattr(reconcile_module, "_delete", _delete)
    _stored_runs(monkeypatch, ["R-3.1", "R-3.0", "R-3.2", "R-9.9"])
    report = asyncio.run(reconcile(schema, conf))
    # R-9.9 no statement backs: never deleted.
    assert deleted == [("runs", sorted([_release("R-3.1"), _release("R-3.2")]))]
    assert (report.removed, report.kept, report.written) == ({"runs": 2}, {}, {})

    # Once the plan is due, 3.2 holds; deleted while pending, it is written back.
    deleted.clear()
    _stored_runs(monkeypatch, ["R-3.0", "R-9.9"])
    due = asyncio.run(reconcile(schema, conf, as_of=_t(17)))
    assert deleted == [("runs", [_release("R-3.0")])]
    assert (due.removed, due.written) == ({"runs": 1}, {"runs": 1})


def test_a_written_value_is_visible_where_its_statements_are(
    tmp_path, monkeypatch
) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    base = _schema()
    manifest = GraphManifest(graph_schema=base)
    manifest.finish_init()
    zoned: dict[str, Literal["replace", "union"]] = {
        name: "replace" for name in ("Unit", "Release", "SourceRecord", "Assertion")
    }
    lifted = apply_evolution(
        manifest,
        plan_lift(
            manifest,
            LiftSpec(
                access_for=zoned,
                access_edges={
                    r: "replace"
                    for r in ("runs", "subject", "object", "wasDerivedFrom")
                },
            ),
            aspects=["access"],
        ),
    )
    schema = lifted.require_schema()
    graph = GraphContainer(
        vertices={
            "Unit": [{"serial": "U-1"}],
            "Release": [{"rid": r} for r in ("R-3.0", "R-3.1", "R-3.2")],
            "SourceRecord": RECORDS,
        }
    )
    asyncio.run(write_container(schema, graph, conf, stamp={"access_zones": [1]}))
    first, rest = STATEMENTS[:2], STATEMENTS[2:]
    for zone, part in ((1, first), (2, rest)):
        asyncio.run(
            write_container(
                schema,
                statement_container(schema, part),
                conf,
                stamp={"access_zones": [zone]},
            )
        )
    # A reader of zone 1 sees the statements written there, and only those.
    with ConnectionManager(connection_config=conf) as conn:
        seen = read_statements(
            conn,
            schema,
            about=("Release", {"rid": "R-3.1"}),
            guard=FilterExpression(
                kind="leaf",
                field="access_zones",
                cmp_operator=ComparisonOperator.ANY_IN,
                value=[1],
            ),
        )
        everything = read_statements(
            conn,
            schema,
            guard=FilterExpression(
                kind="leaf",
                field="access_zones",
                cmp_operator=ComparisonOperator.ANY_IN,
                value=[1],
            ),
        )
    assert {s.id for s in seen.statements} == {s.id for s in first}
    assert {s.id for s in everything.statements} == {s.id for s in first}

    _stored_runs(monkeypatch, ["R-3.1"])
    report = asyncio.run(reconcile(schema, conf))
    assert report.written == {"runs": 1}
    with ConnectionManager(connection_config=conf) as conn:
        rows = conn.fetch_all_edges("Unit", "Release", "runs")
    assert [(row[1]["rid"], row[2].get("access_zones")) for row in rows] == [
        ("R-3.0", [2])
    ]


def test_an_exclusive_value_the_stored_graph_refuses_is_reported(tmp_path) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    manifest = GraphManifest.from_config(
        {
            "schema": {
                "metadata": {"name": "fleet", "version": "1.0.0"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "Unit",
                                "properties": [{"name": "serial", "type": "STRING"}],
                                "identity": ["serial"],
                            },
                            {
                                "name": "Product",
                                "properties": [{"name": "model", "type": "STRING"}],
                                "identity": ["model"],
                            },
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {
                                "source": "Unit",
                                "target": "Product",
                                "relation": "instanceOf",
                                "merge": "replace",
                                "exclusive": True,
                            }
                        ]
                    },
                },
            }
        }
    )
    manifest.finish_init()
    schema = apply_evolution(
        manifest,
        plan_lift(
            manifest,
            LiftSpec(mentions_for=["Unit", "Product"]),
            aspects=["provenance"],
        ),
    ).require_schema()
    # The catalog says SD-150; a text calls the charger an SD-60.
    graph = GraphContainer(
        vertices={
            "Unit": [{"serial": "U-1"}],
            "Product": [{"model": "SD-150"}, {"model": "SD-60"}],
            "SourceRecord": [{"source_record_id": "m1"}],
        },
        edges={
            ("Unit", "Product", "instanceOf"): [
                ({"serial": "U-1"}, {"model": "SD-150"}, {})
            ]
        },
    )
    asyncio.run(write_container(schema, graph, conf))
    stated = Statement(
        vertex_type="Unit",
        key=UNIT,
        slot="instanceOf",
        kind="relation",
        value=entity_key({"model": "SD-60"}, ["model"]),
        target_type="Product",
        record="m1",
    )
    asyncio.run(write_container(schema, statement_container(schema, [stated]), conf))

    report = asyncio.run(reconcile(schema, conf))
    assert report.written == {"instanceOf": 1}
    (line,) = report.refused
    assert "holds Product" in line and "SD-150" in line and "refused" in line
    with ConnectionManager(connection_config=conf) as conn:
        rows = conn.fetch_all_edges("Unit", "Product", "instanceOf")
    assert [row[1]["model"] for row in rows] == ["SD-150"]


def test_a_schema_without_statements_reads_and_reconciles_nothing(tmp_path) -> None:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    plain = Schema.model_validate(
        {
            "metadata": {"name": "plain", "version": "1.0.0"},
            "core_schema": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": "Unit",
                            "properties": [{"name": "serial", "type": "STRING"}],
                            "identity": ["serial"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    )
    asyncio.run(
        write_container(
            plain, GraphContainer(vertices={"Unit": [{"serial": "U-1"}]}), conf
        )
    )
    assert statement_container(plain, []).vertices == {}
    with ConnectionManager(connection_config=conf) as conn:
        assert read_statements(conn, plain).statements == []
    assert asyncio.run(reconcile(plain, conf)).statements == 0
