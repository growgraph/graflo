"""Resolving partial records by their keys, then writing what resolved."""

import asyncio
import copy
from typing import Any

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.identity_uuid import mint_uuid
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.container_write import write_container
from graflo.link.records import (
    PartialRecord,
    RecordGraph,
    RecordRelation,
    Resolution,
    Resolved,
    Staged,
    StatedProp,
)
from graflo.link.resolve import (
    dropped_relations,
    promote,
    resolve_records,
    write_record_graphs,
)

SCHEMA = {
    "metadata": {"name": "support", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Customer",
                    "properties": ["account", "email", "phone", "name"],
                    "identity": ["account"],
                    "secondary_identities": [
                        {"name": "by_email", "fields": ["email"]},
                        {"name": "by_phone", "fields": ["phone"]},
                    ],
                },
                {
                    "name": "Ticket",
                    "properties": ["number", "severity", "wait"],
                    "identity": ["number"],
                },
                {
                    "name": "Room",
                    "properties": ["building", "number"],
                    "identity": ["building", "number"],
                },
                {"name": "Comment", "properties": ["body"], "blank": True},
            ]
        },
        "edge_config": {
            "edges": [
                {"source": "Customer", "target": "Ticket", "relation": "RAISED"},
                {"source": "Comment", "target": "Ticket", "relation": "ON"},
                {"source": "Ticket", "target": "Ticket", "relation": "DUPLICATES"},
                {"source": "Customer", "target": "Customer", "relation": "REFERRED"},
            ]
        },
    },
}
RAISED = ("Customer", "Ticket", "RAISED")
ON = ("Comment", "Ticket", "ON")
DUPLICATES = ("Ticket", "Ticket", "DUPLICATES")
REFERRED = ("Customer", "Customer", "REFERRED")


@pytest.fixture
def schema() -> Schema:
    return Schema.model_validate(SCHEMA)


def _m(ref, type_, may_create=True, **props) -> PartialRecord:
    return PartialRecord(
        ref=ref,
        type=type_,
        may_create=may_create,
        props={k: StatedProp(value=v) for k, v in props.items()},
    )


def _r(edge_id, source, target) -> RecordRelation:
    return RecordRelation(edge_id=edge_id, source=source, target=target)


def _g(source_id, records, relations=()) -> RecordGraph:
    return RecordGraph(
        source_id=source_id,
        schema_ref="support@1.0.0",
        records=records,
        relations=list(relations),
    )


def _reasons(resolution):
    return {(s.source_id, s.record.ref): s.reason for s in resolution.staged.records}


def _by(resolution):
    return {(r.source_id, r.ref): r.by for r in resolution.resolved}


def test_a_complete_key_resolves_and_its_stated_props_are_written(schema):
    graph = _g(
        "note-88",
        [_m("t", "Ticket", may_create=False, number="HD-4411", wait=26.0)],
    )
    resolution = resolve_records(schema, [graph])
    assert _by(resolution) == {("note-88", "t"): "key"}
    container = promote(schema, resolution)
    assert container.vertices["Ticket"] == [{"number": "HD-4411", "wait": 26.0}]


def test_a_partial_composite_key_stages_and_holds_its_relations(schema):
    graph = _g("m1", [_m("r", "Room", number="12")])
    resolution = resolve_records(schema, [graph])
    assert _reasons(resolution) == {("m1", "r"): "partial_key"}
    assert promote(schema, resolution).vertices == {}


def test_an_alternate_key_resolves_against_another_record_of_the_batch(schema):
    first = _g("mail-1", [_m("c", "Customer", account="ACC-1", email="jo@x.com")])
    second = _g(
        "mail-2",
        [_m("c", "Customer", email="jo@x.com"), _m("t", "Ticket", number="HD-2")],
        [_r(RAISED, "c", "t")],
    )
    resolution = resolve_records(schema, [first, second])
    resolved = {(r.source_id, r.ref): r for r in resolution.resolved}
    assert resolved[("mail-2", "c")].identity == {"account": "ACC-1"}
    assert resolved[("mail-2", "c")].found_in == "batch"
    container = promote(schema, resolution)
    assert container.vertices["Customer"] == [{"account": "ACC-1", "email": "jo@x.com"}]
    assert container.edges[RAISED] == [({"account": "ACC-1"}, {"number": "HD-2"}, {})]


def test_alternate_keys_resolve_through_a_chain(schema):
    graphs = [
        _g("a", [_m("c", "Customer", phone="+1")]),
        _g("b", [_m("c", "Customer", email="jo@x.com", phone="+1")]),
        _g("c", [_m("c", "Customer", account="ACC-1", email="jo@x.com")]),
    ]
    resolution = resolve_records(schema, graphs)
    assert {r.source_id: r.identity for r in resolution.resolved} == {
        "a": {"account": "ACC-1"},
        "b": {"account": "ACC-1"},
        "c": {"account": "ACC-1"},
    }


def test_an_alternate_key_naming_two_entities_is_ambiguous(schema):
    graphs = [
        _g("a", [_m("c", "Customer", account="ACC-1", phone="+1")]),
        _g("b", [_m("c", "Customer", account="ACC-2", phone="+1")]),
        _g("c", [_m("c", "Customer", phone="+1")]),
    ]
    resolution = resolve_records(schema, graphs)
    staged = resolution.staged.records
    assert [(s.source_id, s.reason, s.matches) for s in staged] == [
        ("c", "ambiguous", 2)
    ]


def test_an_unmatched_alternate_key_stages_and_holds_the_relation(schema):
    graph = _g(
        "mail-1042",
        [_m("c", "Customer", email="jo@x.com"), _m("t", "Ticket", number="HD-4415")],
        [_r(RAISED, "c", "t")],
    )
    resolution = resolve_records(schema, [graph])
    assert _reasons(resolution) == {("mail-1042", "c"): "unmatched"}
    assert [h.waiting_on for h in resolution.staged.relations] == [["c"]]
    container = promote(schema, resolution)
    assert container.vertices == {"Ticket": [{"number": "HD-4415"}]}
    assert container.edges == {}


def test_a_keyless_type_is_minted_only_when_introduced(schema):
    graph = _g(
        "note-88",
        [
            _m("n", "Comment", body="shipped"),
            _m("old", "Comment", may_create=False),
            _m("t", "Ticket", may_create=False, number="HD-4411"),
        ],
        [_r(ON, "n", "t")],
    )
    resolution = resolve_records(schema, [graph])
    minted = mint_uuid("support@1.0.0", "note-88", "Comment", "n")
    assert resolution.identity_of("note-88", "n") == {"id": minted}
    assert _reasons(resolution) == {("note-88", "old"): "keyless_reference"}
    assert promote(schema, resolution).edges[ON] == [
        ({"id": minted}, {"number": "HD-4411"}, {})
    ]


def test_an_owned_type_needs_the_graph_and_is_never_written(schema):
    graph = _g("chat-1", [_m("t", "Ticket", number="HD-9", severity=1)])
    resolution = resolve_records(schema, [graph], owned=frozenset({"Ticket"}))
    assert _reasons(resolution) == {("chat-1", "t"): "owned_unmatched"}


def test_an_unknown_type_is_refused(schema):
    with pytest.raises(ValueError, match="unknown vertex type"):
        resolve_records(schema, [_g("x", [_m("v", "Robot")])])


def test_resolution_is_deterministic(schema):
    graphs = [
        _g("a", [_m("c", "Customer", phone="+1"), _m("n", "Comment", body="x")]),
        _g("b", [_m("c", "Customer", account="ACC-1", phone="+1")]),
    ]
    first = resolve_records(schema, graphs)
    assert resolve_records(schema, graphs) == first


def _seed(conf, schema):
    asyncio.run(
        write_container(
            schema,
            GraphContainer(
                vertices={
                    "Customer": [{"account": "ACC-1", "email": "jo@x.com"}],
                    "Ticket": [{"number": "HD-1"}],
                }
            ),
            conf,
        )
    )


def test_the_graph_resolves_alternate_and_owned_keys(schema, tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _seed(conf, schema)
    graph = _g(
        "chat-1",
        [
            _m("c", "Customer", email="jo@x.com", name="Jo Park"),
            _m("t", "Ticket", may_create=False, number="HD-1", severity=2),
            _m("u", "Ticket", number="HD-404"),
        ],
        [_r(RAISED, "c", "t"), _r(RAISED, "c", "u")],
    )
    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(
            schema, [graph], db=db, owned=frozenset({"Ticket"})
        )
    resolved = {r.ref: r for r in resolution.resolved}
    assert resolved["c"].identity == {"account": "ACC-1"}
    assert resolved["c"].found_in == "graph"
    assert resolved["t"].owned and resolved["t"].found_in == "graph"
    assert _reasons(resolution) == {("chat-1", "u"): "owned_unmatched"}
    container = promote(schema, resolution)
    # The owned ticket links; text writes nothing onto it.
    assert "Ticket" not in container.vertices
    assert container.edges[RAISED] == [({"account": "ACC-1"}, {"number": "HD-1"}, {})]


def test_write_record_graphs_resolves_then_writes(schema, tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _seed(conf, schema)
    graph = _g(
        "chat-1",
        [
            _m("c", "Customer", email="jo@x.com", name="Jo Park"),
            _m("t", "Ticket", number="HD-2"),
        ],
        [_r(RAISED, "c", "t")],
    )
    resolution, report = asyncio.run(write_record_graphs(schema, [graph], conf))
    assert not report.schema_defined
    assert resolution.staged.records == []
    with ConnectionManager(connection_config=conf) as db:
        assert len(db.fetch_all_edges("Customer", "Ticket", "RAISED")) == 1


def _zoned_schema() -> Schema:
    lifted: dict[str, Any] = copy.deepcopy(SCHEMA)
    ticket = lifted["core_schema"]["vertex_config"]["vertices"][1]
    ticket["properties"] = [
        *ticket["properties"],
        {"name": "access_zones", "type": "LIST", "item_type": "INT", "reserved": True},
    ]
    return Schema.model_validate(lifted)


def test_a_write_into_a_reserved_field_needs_a_stamp(tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    graph = _g("chat-1", [_m("t", "Ticket", number="HD-7")])

    with pytest.raises(ValueError, match="access_zones"):
        asyncio.run(write_record_graphs(_zoned_schema(), [graph], conf))


def test_the_stamp_reaches_what_a_record_graph_writes(tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    graph = _g("chat-1", [_m("t", "Ticket", number="HD-7")])

    asyncio.run(
        write_record_graphs(_zoned_schema(), [graph], conf, stamp={"access_zones": [7]})
    )

    with ConnectionManager(connection_config=conf) as db:
        (ticket,) = db.fetch_docs("Ticket")
    assert ticket["access_zones"] == [7]


def test_the_guard_reaches_every_lookup(schema, tmp_path, monkeypatch, guarded_backend):
    from graflo.hq import lookup

    seen: list[tuple[str, object]] = []
    real = lookup._Reader.find_by

    def find_by(self, vertex_type, fields, docs, returned):
        seen.append((vertex_type, self.guard))
        return real(self, vertex_type, fields, docs, returned)

    monkeypatch.setattr(lookup._Reader, "find_by", find_by)
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _seed(conf, schema)
    guard = FilterExpression.from_dict(["==", "x", "number"])
    graph = _g(
        "chat-1",
        [_m("c", "Customer", email="jo@x.com"), _m("t", "Ticket", number="HD-1")],
    )

    asyncio.run(write_record_graphs(schema, [graph], conf, guard=guard))

    # Ticket declares the guarded field: the guarded lookup, then the trusted
    # existence check. Customer does not: looked up unguarded.
    assert seen == [("Ticket", guard), ("Ticket", None), ("Customer", None)]


def _zoned_ticket(conf, number, zones):
    asyncio.run(
        write_container(
            _zoned_schema(),
            GraphContainer(vertices={"Ticket": [{"number": number}]}),
            conf,
            stamp={"access_zones": zones},
        )
    )


def _opens(*zones: int) -> FilterExpression:
    return FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[list(zones)],
    )


@pytest.fixture
def guarded_backend(monkeypatch):
    from graflo.db.graflo_backend.connection import GraFloBackendConnection

    # The file backend evaluates a guard in memory; declare it for these tests.
    monkeypatch.setattr(GraFloBackendConnection, "supports_read_guard", True)


def test_a_stated_key_of_a_hidden_entity_stages_as_hidden(tmp_path, guarded_backend):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _zoned_ticket(conf, "HD-9", [2])
    graph = _g(
        "chat-1",
        [
            _m("t", "Ticket", number="HD-9", severity=1),
            _m("c", "Customer", account="A"),
        ],
        [_r(RAISED, "c", "t")],
    )

    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(_zoned_schema(), [graph], db=db, guard=_opens(1))

    assert _reasons(resolution) == {("chat-1", "t"): "hidden"}
    assert [r.waiting_on for r in resolution.staged.relations] == [["t"]]
    assert "Ticket" not in promote(_zoned_schema(), resolution).vertices


def test_a_stated_key_the_guard_shows_or_nobody_holds_resolves(
    tmp_path, guarded_backend
):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _zoned_ticket(conf, "HD-1", [1])
    graph = _g(
        "chat-1",
        [_m("seen", "Ticket", number="HD-1"), _m("new", "Ticket", number="HD-2")],
    )

    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(_zoned_schema(), [graph], db=db, guard=_opens(1))

    resolved = {r.ref: r for r in resolution.resolved}
    assert resolved["seen"].found_in == "graph"
    assert resolved["new"].found_in is None
    assert resolution.staged.records == []


def _only(zone: int) -> FilterExpression:
    return FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.EQ,
        value=[[zone]],
    )


def test_an_entity_the_write_may_not_change_is_linked_not_written(
    tmp_path, guarded_backend
):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _zoned_ticket(conf, "HD-2", [2])
    _zoned_ticket(conf, "HD-1", [1])
    graph = _g(
        "chat-1",
        [
            _m("wide", "Ticket", number="HD-2", severity=99),
            _m("again", "Ticket", number="HD-2", wait=3),
            _m("own", "Ticket", number="HD-1", severity=5),
            _m("c", "Customer", account="A"),
        ],
        [_r(RAISED, "c", "wide"), _r(RAISED, "c", "own")],
    )

    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(
            _zoned_schema(),
            [graph],
            db=db,
            guard=_opens(1, 2),
            writable={"Ticket": _only(1)},
        )

    link_only = {r.ref for r in resolution.resolved if r.link_only}
    assert link_only == {"wide", "again"}
    container = promote(_zoned_schema(), resolution)
    assert container.vertices["Ticket"] == [{"number": "HD-1", "severity": 5}]
    assert len(container.edges[RAISED]) == 2


def test_a_writable_filter_on_fields_a_type_lacks_leaves_it_link_only(
    tmp_path, guarded_backend
):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _zoned_ticket(conf, "HD-1", [1])
    graph = _g("chat-1", [_m("t", "Ticket", number="HD-1", severity=5)])
    writable = {"Ticket": FilterExpression.from_dict(["==", "x", "owner"])}

    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(
            _zoned_schema(), [graph], db=db, guard=_opens(1), writable=writable
        )

    assert [r.link_only for r in resolution.resolved] == [True]


def test_a_guarded_type_the_writable_mapping_leaves_out_is_link_only(
    tmp_path, guarded_backend
):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _zoned_ticket(conf, "HD-1", [1])
    graph = _g("chat-1", [_m("t", "Ticket", number="HD-1", severity=5)])

    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(
            _zoned_schema(), [graph], db=db, guard=_opens(1), writable={}
        )

    assert [r.link_only for r in resolution.resolved] == [True]


def test_a_type_whose_filter_is_the_guard_is_written_wherever_the_guard_shows_it(
    tmp_path, guarded_backend
):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _zoned_ticket(conf, "HD-2", [2])
    graph = _g("chat-1", [_m("wide", "Ticket", number="HD-2", severity=9)])

    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(
            _zoned_schema(),
            [graph],
            db=db,
            guard=_opens(1, 2),
            writable={"Ticket": _opens(1, 2)},
        )

    assert [r.link_only for r in resolution.resolved] == [False]


def test_a_type_without_the_guarded_field_is_looked_up_unguarded(
    tmp_path, guarded_backend
):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _seed(conf, schema=Schema.model_validate(SCHEMA))
    graph = _g("chat-1", [_m("c", "Customer", email="jo@x.com")])

    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_records(_zoned_schema(), [graph], db=db, guard=_opens(1))

    assert resolution.staged.records == []
    assert {r.ref: r.identity for r in resolution.resolved} == {
        "c": {"account": "ACC-1"}
    }


# -- a relation states two entities ------------------------------------------


def test_a_relation_between_records_of_one_entity_is_dropped(schema):
    graph = _g(
        "c1",
        [
            _m("v1", "Ticket", may_create=False, number="HD-1"),
            _m("v2", "Ticket", may_create=False, number="HD-1"),
        ],
        [_r(DUPLICATES, "v1", "v2")],
    )
    resolution = resolve_records(schema, [graph])
    assert _by(resolution) == {("c1", "v1"): "key", ("c1", "v2"): "key"}
    assert resolution.staged.relations == []
    (dropped,) = resolution.dropped
    assert (dropped.source_id, dropped.reason) == ("c1", "same_entity")
    assert dropped.relation == graph.relations[0]
    container = promote(schema, resolution)
    assert container.vertices["Ticket"] == [{"number": "HD-1"}]
    assert DUPLICATES not in container.edges


def test_a_record_found_by_its_related_records_alternate_is_dropped(schema):
    graph = _g(
        "mail-1",
        [
            _m("a", "Customer", account="ACC-1", email="jo@x.com"),
            _m("b", "Customer", email="jo@x.com"),
        ],
        [_r(REFERRED, "a", "b")],
    )
    resolution = resolve_records(schema, [graph])
    assert _by(resolution) == {("mail-1", "a"): "key", ("mail-1", "b"): "alternate"}
    assert [d.reason for d in resolution.dropped] == ["same_entity"]
    assert REFERRED not in promote(schema, resolution).edges


def test_a_record_fixed_by_hand_to_its_related_records_entity_is_dropped(schema):
    graph = _g(
        "c1",
        [
            _m("v1", "Ticket", may_create=False),
            _m("v2", "Ticket", may_create=False, number="HD-42"),
        ],
        [_r(DUPLICATES, "v1", "v2")],
    )
    resolution = resolve_records(schema, [graph])
    assert _reasons(resolution) == {("c1", "v1"): "no_key"}
    assert resolution.dropped == []
    forced = resolution.model_copy(
        update={
            "resolved": [
                *resolution.resolved,
                Resolved(
                    source_id="c1",
                    ref="v1",
                    type="Ticket",
                    identity={"number": "HD-42"},
                    by="alternate",
                    found_in="graph",
                ),
            ],
            "staged": Staged(),
        }
    )
    assert DUPLICATES not in promote(schema, forced).edges
    (dropped,) = dropped_relations(forced)
    assert (dropped.relation.source, dropped.relation.target) == ("v1", "v2")
    assert dropped.reason == "same_entity"


def test_a_picked_key_naming_the_related_records_entity_is_dropped(schema, tmp_path):
    conf = GraFloBackendConfig(output_dir=tmp_path)
    asyncio.run(
        write_container(
            schema, GraphContainer(vertices={"Ticket": [{"number": "HD-42"}]}), conf
        )
    )
    # A pick restates the record with its candidate's key.
    graph = _g(
        "c1",
        [
            _m("v1", "Ticket", may_create=False, number="HD-42"),
            _m("v2", "Ticket", may_create=False, number="HD-42"),
        ],
        [_r(DUPLICATES, "v1", "v2")],
    )
    resolution, _ = asyncio.run(write_record_graphs(schema, [graph], conf))
    assert [d.reason for d in resolution.dropped] == ["same_entity"]
    with ConnectionManager(connection_config=conf) as db:
        assert db.fetch_all_edges("Ticket", "Ticket", "DUPLICATES") == []


def test_two_unrelated_records_of_one_entity_both_resolve(schema):
    graph = _g(
        "c1",
        [
            _m("a", "Ticket", may_create=False, number="HD-1"),
            _m("b", "Ticket", may_create=False, number="HD-1"),
            _m("c", "Customer", account="ACC-1"),
        ],
        [_r(RAISED, "c", "a"), _r(RAISED, "c", "b")],
    )
    resolution = resolve_records(schema, [graph])
    assert _by(resolution) == {
        ("c1", "a"): "key",
        ("c1", "b"): "key",
        ("c1", "c"): "key",
    }
    assert resolution.dropped == []
    container = promote(schema, resolution)
    assert container.vertices["Ticket"] == [{"number": "HD-1"}]
    assert len(container.edges[RAISED]) == 2


def test_a_relation_from_a_record_to_itself_is_kept(schema):
    graph = _g(
        "c1",
        [_m("v1", "Ticket", may_create=False, number="HD-1")],
        [_r(DUPLICATES, "v1", "v1")],
    )
    resolution = resolve_records(schema, [graph])
    assert resolution.dropped == []
    assert promote(schema, resolution).edges[DUPLICATES] == [
        ({"number": "HD-1"}, {"number": "HD-1"}, {})
    ]


def test_dropped_relations_round_trip_and_default_to_none(schema):
    graph = _g(
        "c1",
        [
            _m("v1", "Ticket", may_create=False, number="HD-1"),
            _m("v2", "Ticket", may_create=False, number="HD-1"),
        ],
        [_r(DUPLICATES, "v1", "v2")],
    )
    resolution = resolve_records(schema, [graph])
    assert Resolution.model_validate_json(resolution.model_dump_json()) == resolution
    earlier = resolution.model_dump(mode="json")
    del earlier["dropped"]
    assert Resolution.model_validate(earlier).dropped == []
