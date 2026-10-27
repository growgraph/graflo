"""gather_candidates: the entities a staged record may be, by lane."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.filter.onto import ComparisonOperator, FilterExpression
from graflo.hq.container_write import write_container
from graflo.hq.lookup import AliasHop, KeyPattern, LookupSpec
from graflo.link.candidates import gather_candidates
from graflo.link.records import (
    CandidateHit,
    EntityCandidate,
    PartialRecord,
    RecordGraph,
    RecordRelation,
    StagedCandidates,
    StatedProp,
)
from graflo.link.resolve import resolve_records

NAME = {"iri": "https://schema.org/name"}
SCHEMA: dict[str, Any] = {
    "metadata": {"name": "support", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Customer",
                    "properties": [
                        "account",
                        "email",
                        {"name": "name", "semantics": NAME},
                    ],
                    "identity": ["account"],
                    "secondary_identities": [{"name": "by_email", "fields": ["email"]}],
                },
                {
                    "name": "Ticket",
                    "properties": ["number", {"name": "title", "semantics": NAME}],
                    "identity": ["number"],
                    "secondary_identities": [{"name": "by_title", "fields": ["title"]}],
                },
                {
                    "name": "Product",
                    "properties": [
                        "sku",
                        {"name": "name", "semantics": NAME},
                        "name_norm",
                    ],
                    "identity": ["sku"],
                },
                {
                    "name": "Alias",
                    "properties": [{"name": "label", "semantics": NAME}],
                    "identity": ["label"],
                },
            ]
        },
        "edge_config": {
            "edges": [
                {"source": "Customer", "target": "Ticket", "relation": "RAISED"},
                {"source": "Alias", "target": "Product", "relation": "ALIAS_OF"},
                {"source": "Ticket", "target": "Ticket", "relation": "DUPLICATES"},
                {"source": "Product", "target": "Product", "relation": "REPLACES"},
            ]
        },
    },
}
RAISED = ("Customer", "Ticket", "RAISED")
DUPLICATES = ("Ticket", "Ticket", "DUPLICATES")
REPLACES = ("Product", "Product", "REPLACES")
CUSTOMERS = [
    {"account": "ACC-1", "email": "jo@x.com", "name": "Jo Park"},
    {"account": "ACC-2", "name": "Ann Lee"},
    {"account": "ACC-3", "email": "ann@y.com", "name": "Ann Leeds"},
    {"account": "ACC-4", "email": "desk@x.com", "name": "Front Desk"},
    {"account": "ACC-5", "email": "desk@x.com", "name": "Back Desk"},
    {"account": "ACC-6", "name": "Jo Park"},
]
SPEC = LookupSpec(
    aliases=[AliasHop(alias_type="Alias", relation="ALIAS_OF", target_type="Product")]
)


def _schema() -> Schema:
    return Schema.model_validate(SCHEMA)


def _zoned_schema() -> Schema:
    lifted = copy.deepcopy(SCHEMA)
    customer = lifted["core_schema"]["vertex_config"]["vertices"][0]
    customer["properties"] = [
        *customer["properties"],
        {"name": "access_zones", "type": "LIST", "item_type": "INT", "reserved": True},
    ]
    return Schema.model_validate(lifted)


def _m(ref: str, type_: str, quote: str = "", **props: Any) -> PartialRecord:
    return PartialRecord(
        ref=ref,
        type=type_,
        may_create=False,
        props={k: StatedProp(value=v) for k, v in props.items()},
        evidence={"quote": quote or ref},
    )


def _quote(record: PartialRecord) -> str | None:
    return (record.evidence or {}).get("quote")


def _g(source_id: str, records: list[PartialRecord], relations=()) -> RecordGraph:
    return RecordGraph(
        source_id=source_id,
        schema_ref="support@1.0.0",
        records=records,
        relations=[
            RecordRelation(edge_id=e, source=s, target=t) for e, s, t in relations
        ],
    )


def _seed(conf: GraFloBackendConfig) -> None:
    asyncio.run(
        write_container(
            _schema(),
            GraphContainer(
                vertices={
                    "Customer": CUSTOMERS,
                    "Ticket": [
                        {"number": "HD-4411", "title": "Pump leak"},
                        {"number": "HD-9"},
                    ],
                    "Product": [
                        {"sku": "P-200", "name": "Pump X1", "name_norm": "pump x1"}
                    ],
                    "Alias": [{"label": "Berlin pump"}],
                },
                edges={
                    RAISED: [({"account": "ACC-2"}, {"number": "HD-4411"}, {})],
                    ("Alias", "Product", "ALIAS_OF"): [
                        ({"label": "Berlin pump"}, {"sku": "P-200"}, {})
                    ],
                    DUPLICATES: [({"number": "HD-9"}, {"number": "HD-4411"}, {})],
                },
            ),
            conf,
        )
    )


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Any]:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    _seed(conf)
    with ConnectionManager(connection_config=conf) as connection:
        yield connection


def _gather(db: Any, graphs: list[RecordGraph], **kwargs: Any):
    schema = kwargs.pop("schema", _schema())
    spec = kwargs.pop("spec", SPEC)
    resolution = resolve_records(schema, graphs, db=db)
    return gather_candidates(
        schema, resolution, db, spec=spec, lookup_text=_quote, **kwargs
    )


def _found(entry: StagedCandidates) -> list[tuple[str, str]]:
    """Each candidate's first key value and its best lane."""
    return [(next(iter(c.key.values())), c.lanes[0].lane) for c in entry.candidates]


def _lanes(candidate: EntityCandidate) -> list[str]:
    return [hit.lane for hit in candidate.lanes]


def _attributes(candidate: EntityCandidate) -> list[tuple[str, str, str]]:
    return [(a.field, a.verdict, a.compare) for a in candidate.attributes]


def test_a_stated_label_finds_its_entity_exactly(db):
    (entry,) = _gather(db, [_g("m1", [_m("c", "Customer", "Ann Lee", name="Ann Lee")])])
    assert (entry.record.ref, entry.reason) == ("c", "no_key")
    assert _found(entry) == [("ACC-2", "exact")]
    (candidate,) = entry.candidates
    assert candidate.label == "Ann Lee"
    assert candidate.lanes[0].matched == "Ann Lee"
    assert _attributes(candidate) == [("name", "agree", "literal")]


def test_an_alias_leads_to_what_it_names(db):
    (entry,) = _gather(db, [_g("m1", [_m("p", "Product", "the Berlin pump")])])
    assert _found(entry) == [("P-200", "alias")]


def test_a_stated_relation_to_a_resolved_anchor_gives_the_linked_entities(db):
    graph = _g(
        "m1",
        [_m("c", "Customer", "the customer"), _m("t", "Ticket", number="HD-4411")],
        [(RAISED, "c", "t")],
    )
    (entry,) = _gather(db, [graph])
    assert _found(entry) == [("ACC-2", "relation")]
    (candidate,) = entry.candidates
    (hit,) = candidate.lanes
    assert (hit.relation, hit.anchor, hit.matched) == ("RAISED", "t", None)
    assert candidate.label == "Ann Lee"


def test_the_relation_is_followed_from_either_end(db):
    graph = _g(
        "m1",
        [_m("c", "Customer", account="ACC-2"), _m("t", "Ticket", "that ticket")],
        [(RAISED, "c", "t")],
    )
    (entry,) = _gather(db, [graph])
    assert _found(entry) == [("HD-4411", "relation")]


def test_an_ambiguous_secondary_identity_lists_what_it_matched(db):
    graph = _g("m1", [_m("c", "Customer", "the desk", email="desk@x.com")])
    (entry,) = _gather(db, [graph])
    assert entry.reason == "ambiguous"
    assert sorted(_found(entry)) == [("ACC-4", "alternate"), ("ACC-5", "alternate")]


def test_an_ambiguous_record_finds_its_batch_entities_without_a_database():
    graphs = [
        _g("a", [_m("c", "Customer", account="ACC-8", email="x@x.com")]),
        _g("b", [_m("c", "Customer", account="ACC-9", email="x@x.com")]),
        _g("c", [_m("c", "Customer", email="x@x.com")]),
    ]
    schema = _schema()
    resolution = resolve_records(schema, graphs)
    (entry,) = gather_candidates(schema, resolution)
    assert entry.source_id == "c"
    assert sorted(_found(entry)) == [("ACC-8", "alternate"), ("ACC-9", "alternate")]


def test_a_candidate_contradicting_a_stated_key_is_left_out(db):
    # Two "Jo Park"s; the one holding another email is not this record.
    graph = _g("m1", [_m("c", "Customer", "Jo Park", name="Jo Park", email="zz@x.com")])
    (entry,) = _gather(db, [graph])
    assert entry.reason == "unmatched"
    assert _found(entry) == [("ACC-6", "exact")]


def test_one_candidate_per_entity_with_every_lane_best_first(db):
    graph = _g(
        "m1",
        [
            _m("c", "Customer", "Ann Lee", name="Ann Lee"),
            _m("t", "Ticket", number="HD-4411"),
        ],
        [(RAISED, "c", "t")],
    )
    (entry,) = _gather(db, [graph])
    (candidate,) = entry.candidates
    assert candidate.key == {"account": "ACC-2"}
    assert _lanes(candidate) == ["exact", "relation"]


def test_the_cap_counts_entities_not_lanes(db):
    graph = _g(
        "m1",
        [
            _m("c", "Customer", "Ann Lee", name="Ann Lee"),
            _m("t", "Ticket", number="HD-4411"),
        ],
        [(RAISED, "c", "t")],
    )
    (entry,) = _gather(db, [graph], max_candidates=1)
    assert _found(entry) == [("ACC-2", "exact")]
    assert not entry.truncated


def test_a_dropped_candidate_never_takes_the_place_of_a_kept_one(db):
    # Both desks and ACC-1 hold another email; only ACC-6 can be this record,
    # though the desks are found first by their longer names.
    graph = _g(
        "m1",
        [
            _m(
                "c",
                "Customer",
                "Front Desk and Back Desk and Jo Park",
                email="jo2@x.com",
            )
        ],
    )
    (entry,) = _gather(db, [graph], max_candidates=1)
    assert _found(entry) == [("ACC-6", "exact")]
    assert not entry.truncated


def test_a_non_key_contradiction_is_shown_not_dropped(db):
    graph = _g("m1", [_m("p", "Product", "Pump X1", name="Pump X2")])
    (entry,) = _gather(db, [graph])
    (candidate,) = entry.candidates
    assert candidate.key == {"sku": "P-200"}
    assert _attributes(candidate) == [("name", "contradict", "literal")]


def test_nothing_stored_is_unknown(db):
    # ACC-2 holds no email: the stated one neither agrees nor contradicts.
    graph = _g(
        "m1", [_m("c", "Customer", "Ann Lee", name="Ann Lee", email="ann@lee.com")]
    )
    (entry,) = _gather(db, [graph])
    (candidate,) = entry.candidates
    assert candidate.key == {"account": "ACC-2"}
    assert _attributes(candidate) == [
        ("name", "agree", "literal"),
        ("email", "unknown", "literal"),
    ]


def test_a_key_differing_only_in_case_contradicts(db):
    graph = _g("m1", [_m("c", "Customer", "Jo Park", name="Jo Park", email="JO@x.com")])
    (entry,) = _gather(db, [graph])
    assert _found(entry) == [("ACC-6", "exact")]


def test_a_batch_alternate_matches_literally():
    graphs = [
        _g("a", [_m("c", "Customer", account="ACC-8", email="x@x.com")]),
        _g("b", [_m("c", "Customer", account="ACC-9", email="x@x.com")]),
        _g("d", [_m("c", "Customer", account="ACC-7", email="X@x.com")]),
        _g("c", [_m("c", "Customer", email="x@x.com")]),
    ]
    schema = _schema()
    resolution = resolve_records(schema, graphs)
    (entry,) = gather_candidates(schema, resolution)
    assert sorted(_found(entry)) == [("ACC-8", "alternate"), ("ACC-9", "alternate")]
    assert all(
        _attributes(c) == [("email", "unknown", "literal")] for c in entry.candidates
    )


def test_a_normalized_field_compares_in_normal_form(db):
    spec = SPEC.model_copy(update={"normalized_fields": {"Product": ["name_norm"]}})
    graph = _g(
        "m1", [_m("p", "Product", "Pump X1", name="Pump X1", name_norm="PUMP-x1")]
    )
    (entry,) = _gather(db, [graph], spec=spec)
    (candidate,) = entry.candidates
    assert _attributes(candidate) == [
        ("name", "agree", "literal"),
        ("name_norm", "agree", "normalized"),
    ]


def test_a_field_the_caller_refuses_is_never_compared(db):
    # Compared, email leaves ACC-1 out; refused, it neither drops nor shows.
    graph = _g("m1", [_m("c", "Customer", "Jo Park", name="Jo Park", email="zz@x.com")])
    (entry,) = _gather(db, [graph], comparable=lambda _type, field: field != "email")
    assert sorted(_found(entry)) == [("ACC-1", "exact"), ("ACC-6", "exact")]
    assert all(
        _attributes(c) == [("name", "agree", "literal")] for c in entry.candidates
    )


def test_a_reserved_field_is_never_compared(zoned):
    graph = _g("m1", [_m("c", "Customer", "Ann Lee", name="Ann Lee", access_zones=[2])])
    (entry,) = _gather(zoned, [graph], schema=_zoned_schema())
    (candidate,) = entry.candidates
    assert _attributes(candidate) == [("name", "agree", "literal")]


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"lane": "relation", "relation": "RAISED"}, "anchor"),
        ({"lane": "exact"}, "matched"),
        ({"lane": "exact", "matched": "Ann", "role": "author"}, "role"),
        ({"lane": "metadata", "matched": "Ann"}, "role"),
    ],
)
def test_a_hit_carries_exactly_the_fields_of_its_lane(fields, message):
    with pytest.raises(ValueError, match=message):
        CandidateHit(**fields)


def test_a_candidate_has_at_least_one_lane():
    with pytest.raises(ValueError, match="lanes"):
        EntityCandidate(vertex_type="Customer", key={"account": "ACC-2"}, lanes=[])


def test_the_cap_holds_and_says_so(db):
    graph = _g("m1", [_m("c", "Customer", "Ann Lee or Jo Park or Ann Leeds")])
    (entry,) = _gather(db, [graph], max_candidates=2)
    assert len(entry.candidates) == 2
    assert entry.truncated
    (wide,) = _gather(db, [graph])
    assert len(wide.candidates) == 4
    assert not wide.truncated


def test_nothing_named_finds_nothing(db):
    graph = _g("m1", [_m("c", "Customer", "someone"), _m("t", "Ticket", number="HD-1")])
    (entry,) = _gather(db, [graph])
    assert entry.candidates == []
    assert not entry.truncated


def test_only_staged_records_get_an_entry_in_order(db):
    graph = _g(
        "m1",
        [
            _m("t", "Ticket", number="HD-4411"),
            _m("b", "Customer", "nobody"),
            _m("a", "Customer", "Ann Lee", name="Ann Lee"),
        ],
    )
    assert [e.record.ref for e in _gather(db, [graph])] == ["b", "a"]


def test_a_candidate_set_round_trips_as_json(db):
    graph = _g(
        "m1",
        [_m("c", "Customer", "the customer"), _m("t", "Ticket", number="HD-4411")],
        [(RAISED, "c", "t")],
    )
    entries = _gather(db, [graph])
    again = [StagedCandidates.model_validate_json(e.model_dump_json()) for e in entries]
    assert again == entries


def test_the_cap_must_be_positive(db):
    with pytest.raises(ValueError, match="max_candidates"):
        _gather(db, [], max_candidates=0)


# -- a relation states two entities ------------------------------------------

KEYS = SPEC.model_copy(
    update={
        "key_patterns": [
            KeyPattern(vertex_type="Ticket", field="number", pattern=r"HD-\d+")
        ]
    }
)


def _comment(quote: str, relations=()) -> RecordGraph:
    # The comment's own ticket, keyless, and the ticket it names by key.
    return _g(
        "c1",
        [_m("v1", "Ticket", quote), _m("v2", "Ticket", number="HD-4411")],
        relations,
    )


def test_the_key_lane_offers_a_ticket_another_record_names(db):
    (entry,) = _gather(db, [_comment("a duplicate of HD-4411")], spec=KEYS)
    assert entry.record.ref == "v1"
    assert _found(entry) == [("HD-4411", "key")]


def test_the_key_lane_never_offers_the_entity_of_a_related_record(db):
    graph = _comment("a duplicate of HD-4411", [(DUPLICATES, "v1", "v2")])
    (entry,) = _gather(db, [graph], spec=KEYS)
    assert entry.record.ref == "v1"
    # What duplicates HD-4411 is still offered; HD-4411 itself is not.
    assert _found(entry) == [("HD-9", "relation")]


def test_the_label_lane_never_offers_the_entity_of_a_related_record(db):
    (offered,) = _gather(db, [_comment("Pump leak")])
    assert _found(offered) == [("HD-4411", "exact")]
    (entry,) = _gather(db, [_comment("Pump leak", [(DUPLICATES, "v2", "v1")])])
    assert entry.candidates == []


def test_the_alias_lane_never_offers_the_entity_of_a_related_record(db):
    records = [_m("p", "Product", "the Berlin pump"), _m("q", "Product", sku="P-200")]
    (offered,) = _gather(db, [_g("m1", records)])
    assert _found(offered) == [("P-200", "alias")]
    (entry,) = _gather(db, [_g("m1", records, [(REPLACES, "p", "q")])])
    assert entry.candidates == []


def test_the_alternate_lane_never_offers_the_entity_of_a_related_record():
    records = [
        _m("v1", "Ticket", title="Pump leak"),
        _m("v2", "Ticket", number="HD-1", title="Pump leak"),
        _m("v3", "Ticket", number="HD-2", title="Pump leak"),
    ]
    schema = _schema()
    for relations, expected in (
        ((), [("HD-1", "alternate"), ("HD-2", "alternate")]),
        ([(DUPLICATES, "v1", "v2")], [("HD-2", "alternate")]),
    ):
        resolution = resolve_records(schema, [_g("c1", records, relations)])
        (entry,) = gather_candidates(schema, resolution)
        assert entry.reason == "ambiguous"
        assert sorted(_found(entry)) == expected


def test_the_relation_lane_never_offers_the_entity_of_a_related_record(db):
    # HD-9 duplicates HD-4411 in the graph; the text relates the comment's
    # ticket to both, so neither is a candidate through the other.
    graph = _g(
        "c1",
        [
            _m("v1", "Ticket", "that ticket"),
            _m("v2", "Ticket", number="HD-4411"),
            _m("v3", "Ticket", number="HD-9"),
        ],
        [(DUPLICATES, "v1", "v2"), (DUPLICATES, "v3", "v1")],
    )
    (entry,) = _gather(db, [graph])
    assert entry.candidates == []


# -- under a read guard ------------------------------------------------------


def _opens(*zones: int) -> FilterExpression:
    return FilterExpression(
        kind="leaf",
        field="access_zones",
        cmp_operator=ComparisonOperator.ANY_IN,
        value=[list(zones)],
    )


@pytest.fixture
def zoned(tmp_path: Path) -> Iterator[Any]:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    for zone, customers in ((1, [CUSTOMERS[1]]), (2, [CUSTOMERS[5]])):
        asyncio.run(
            write_container(
                _zoned_schema(),
                GraphContainer(vertices={"Customer": customers}),
                conf,
                stamp={"access_zones": [zone]},
            )
        )
    with ConnectionManager(connection_config=conf) as connection:
        yield connection


def test_the_guard_hides_a_candidate(zoned):
    graph = _g("m1", [_m("c", "Customer", "Ann Lee and Jo Park")])
    (seen,) = _gather(zoned, [graph], schema=_zoned_schema(), guard=_opens(1))
    assert _found(seen) == [("ACC-2", "exact")]
    (both,) = _gather(zoned, [graph], schema=_zoned_schema(), guard=_opens(1, 2))
    assert sorted(_found(both)) == [("ACC-2", "exact"), ("ACC-6", "exact")]


def test_the_guard_reaches_the_relation_path(zoned, monkeypatch):
    seen: list[dict[str, Any]] = []

    def neighbours(*args: Any, **kwargs: Any) -> GraphContainer:
        seen.append(kwargs)
        return GraphContainer()

    monkeypatch.setattr(zoned, "graph_neighbors", neighbours)
    guard = _opens(1)
    graph = _g(
        "m1",
        [_m("c", "Customer", "the customer"), _m("t", "Ticket", number="HD-4411")],
        [(RAISED, "c", "t")],
    )
    _gather(zoned, [graph], schema=_zoned_schema(), guard=guard)
    assert [kw["guard"] for kw in seen] == [guard]
    assert seen[0]["edge_types"] == ["RAISED"]


def test_a_guard_is_refused_where_the_backend_cannot_apply_it(db, monkeypatch):
    monkeypatch.setattr(type(db), "supports_read_guard", False)
    graph = _g("m1", [_m("c", "Customer", "Ann Lee")])
    with pytest.raises(NotImplementedError, match="read guard"):
        _gather(db, [graph], guard=_opens(1))


# -- order -------------------------------------------------------------------


def test_the_candidate_order_does_not_follow_the_database_row_order(db, monkeypatch):
    graph = _g("m1", [_m("c", "Customer", "Jo Park")])
    (forward,) = _gather(db, [graph])
    fetch = type(db).fetch_docs

    def reversed_rows(self: Any, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return list(reversed(fetch(self, *args, **kwargs)))

    monkeypatch.setattr(type(db), "fetch_docs", reversed_rows)
    (backward,) = _gather(db, [graph])

    assert len(forward.candidates) == 2
    assert backward.model_dump() == forward.model_dump()
