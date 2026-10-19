"""Resolving mentions by their keys, then writing what resolved."""

import asyncio

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.graph_types.mention import (
    Mention,
    MentionGraph,
    Relation,
    StatedProp,
    TextSpan,
)
from graflo.architecture.schema.document import Schema
from graflo.architecture.schema.identity_uuid import mint_uuid
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.container_write import write_container
from graflo.hq.mention_resolve import promote, resolve_mentions, write_mention_graphs

SPAN = TextSpan(start=0, end=1)
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
            ]
        },
    },
}
RAISED = ("Customer", "Ticket", "RAISED")
ON = ("Comment", "Ticket", "ON")


@pytest.fixture
def schema() -> Schema:
    return Schema.model_validate(SCHEMA)


def _m(ref, type_, introduces=True, **props) -> Mention:
    return Mention(
        ref=ref,
        type=type_,
        introduces=introduces,
        quote=ref,
        span=SPAN,
        props={
            k: StatedProp(value=v, quote=str(v), span=SPAN) for k, v in props.items()
        },
    )


def _r(edge_id, source, target) -> Relation:
    return Relation(edge_id=edge_id, source=source, target=target, quote="", span=SPAN)


def _g(source_id, mentions, relations=()) -> MentionGraph:
    return MentionGraph(
        source_id=source_id,
        schema_ref="support@1.0.0",
        mentions=mentions,
        relations=list(relations),
    )


def _reasons(resolution):
    return {(s.source_id, s.mention.ref): s.reason for s in resolution.staged.mentions}


def _by(resolution):
    return {(r.source_id, r.ref): r.by for r in resolution.resolved}


def test_a_complete_key_resolves_and_its_stated_props_are_written(schema):
    graph = _g(
        "note-88",
        [_m("t", "Ticket", introduces=False, number="HD-4411", wait=26.0)],
    )
    resolution = resolve_mentions(schema, [graph])
    assert _by(resolution) == {("note-88", "t"): "key"}
    container = promote(schema, resolution)
    assert container.vertices["Ticket"] == [{"number": "HD-4411", "wait": 26.0}]


def test_a_partial_composite_key_stages_and_holds_its_relations(schema):
    graph = _g("m1", [_m("r", "Room", number="12")])
    resolution = resolve_mentions(schema, [graph])
    assert _reasons(resolution) == {("m1", "r"): "partial_key"}
    assert promote(schema, resolution).vertices == {}


def test_an_alternate_key_resolves_against_another_mention_of_the_batch(schema):
    first = _g("mail-1", [_m("c", "Customer", account="ACC-1", email="jo@x.com")])
    second = _g(
        "mail-2",
        [_m("c", "Customer", email="jo@x.com"), _m("t", "Ticket", number="HD-2")],
        [_r(RAISED, "c", "t")],
    )
    resolution = resolve_mentions(schema, [first, second])
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
    resolution = resolve_mentions(schema, graphs)
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
    resolution = resolve_mentions(schema, graphs)
    staged = resolution.staged.mentions
    assert [(s.source_id, s.reason, s.matches) for s in staged] == [
        ("c", "ambiguous", 2)
    ]


def test_an_unmatched_alternate_key_stages_and_holds_the_relation(schema):
    graph = _g(
        "mail-1042",
        [_m("c", "Customer", email="jo@x.com"), _m("t", "Ticket", number="HD-4415")],
        [_r(RAISED, "c", "t")],
    )
    resolution = resolve_mentions(schema, [graph])
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
            _m("old", "Comment", introduces=False),
            _m("t", "Ticket", introduces=False, number="HD-4411"),
        ],
        [_r(ON, "n", "t")],
    )
    resolution = resolve_mentions(schema, [graph])
    minted = mint_uuid("support@1.0.0", "note-88", "Comment", "n")
    assert resolution.identity_of("note-88", "n") == {"id": minted}
    assert _reasons(resolution) == {("note-88", "old"): "keyless_reference"}
    assert promote(schema, resolution).edges[ON] == [
        ({"id": minted}, {"number": "HD-4411"}, {})
    ]


def test_an_owned_type_needs_the_graph_and_is_never_written(schema):
    graph = _g("chat-1", [_m("t", "Ticket", number="HD-9", severity=1)])
    resolution = resolve_mentions(schema, [graph], owned=frozenset({"Ticket"}))
    assert _reasons(resolution) == {("chat-1", "t"): "owned_unmatched"}


def test_an_unknown_type_is_refused(schema):
    with pytest.raises(ValueError, match="unknown vertex type"):
        resolve_mentions(schema, [_g("x", [_m("v", "Robot")])])


def test_resolution_is_deterministic(schema):
    graphs = [
        _g("a", [_m("c", "Customer", phone="+1"), _m("n", "Comment", body="x")]),
        _g("b", [_m("c", "Customer", account="ACC-1", phone="+1")]),
    ]
    first = resolve_mentions(schema, graphs)
    assert resolve_mentions(schema, graphs) == first


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
            _m("t", "Ticket", introduces=False, number="HD-1", severity=2),
            _m("u", "Ticket", number="HD-404"),
        ],
        [_r(RAISED, "c", "t"), _r(RAISED, "c", "u")],
    )
    with ConnectionManager(connection_config=conf) as db:
        resolution = resolve_mentions(
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


def test_write_mention_graphs_resolves_then_writes(schema, tmp_path):
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
    resolution, report = asyncio.run(write_mention_graphs(schema, [graph], conf))
    assert not report.schema_defined
    assert resolution.staged.mentions == []
    with ConnectionManager(connection_config=conf) as db:
        assert len(db.fetch_all_edges("Customer", "Ticket", "RAISED")) == 1
