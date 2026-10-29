"""The context lane, the compatible lane through records, and the support features."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from graflo.architecture.graph_types.container import GraphContainer
from graflo.architecture.schema.document import Schema
from graflo.connections.graflo_backend import GraFloBackendConfig
from graflo.db.manager import ConnectionManager
from graflo.hq.container_write import write_container
from graflo.hq.lookup import AliasHop, LookupSpec
from graflo.link.candidates import gather_candidates, support
from graflo.link.records import (
    CandidateHit,
    EntityCandidate,
    KnownSubject,
    PartialRecord,
    RecordGraph,
    StatedProp,
    SupportFeatures,
)
from graflo.link.resolve import resolve_records

NAME = {"iri": "https://schema.org/name"}
WORDS = {"normalizer": "label_v1", "parts": {"split": "words", "order": "unordered"}}
SITE = {
    "normalizer": "label_v1",
    "parts": {"split": [","], "order": "general_first"},
}
SCHEMA: dict[str, Any] = {
    "metadata": {"name": "plant", "version": "1.0.0"},
    "core_schema": {
        "vertex_config": {
            "vertices": [
                {
                    "name": "Product",
                    "properties": ["sku", {"name": "name", "semantics": NAME}],
                    "identity": ["sku"],
                },
                {
                    "name": "Component",
                    "properties": [
                        "code",
                        {"name": "name", "semantics": NAME, "match": WORDS},
                        {"name": "kind", "match": {"exclusive": True}},
                    ],
                    "identity": ["code"],
                },
                {
                    "name": "Unit",
                    "properties": [
                        "serial",
                        {"name": "site", "match": SITE},
                        {
                            "name": "site_keys",
                            "type": "LIST",
                            "item_type": "STRING",
                            "derived": {"source": "site", "form": "keys"},
                        },
                    ],
                    "identity": ["serial"],
                },
                {
                    "name": "Alias",
                    "properties": [
                        {
                            "name": "label",
                            "semantics": NAME,
                            "match": {"normalizer": "label_v1"},
                        },
                        {
                            "name": "normalized_label",
                            "derived": {"source": "label", "form": "normal"},
                        },
                    ],
                    "identity": ["id"],
                    "hash_identity_properties": ["normalized_label"],
                },
            ]
        },
        "edge_config": {
            "edges": [
                {"source": "Product", "target": "Component", "relation": "HAS"},
                {"source": "Alias", "target": "Unit", "relation": "ALIAS_OF"},
            ]
        },
    },
}
HAS = ("Product", "Component", "HAS")
ALIAS_OF = ("Alias", "Unit", "ALIAS_OF")
SPEC = LookupSpec(
    aliases=[AliasHop(alias_type="Alias", relation="ALIAS_OF", target_type="Unit")]
)


def _schema() -> Schema:
    return Schema.model_validate(SCHEMA)


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Any]:
    conf = GraFloBackendConfig(output_dir=tmp_path)
    asyncio.run(
        write_container(
            _schema(),
            GraphContainer(
                vertices={
                    "Product": [{"sku": "P-200", "name": "Pump P-200"}],
                    "Component": [
                        {"code": "CTL-200", "name": "P-200 controller", "kind": "ctl"},
                        {"code": "DRV-200", "name": "P-200 drive", "kind": "drv"},
                    ],
                    "Unit": [
                        {"serial": "KP-1", "site": "Berlin, pump hall 2"},
                        {"serial": "KP-2", "site": "Berlin, hall 5"},
                    ],
                    "Alias": [{"label": "Berlin pump"}],
                },
                edges={
                    HAS: [
                        ({"sku": "P-200"}, {"code": "CTL-200"}, {}),
                        ({"sku": "P-200"}, {"code": "DRV-200"}, {}),
                    ],
                },
            ),
            conf,
        )
    )
    # The alias edge is keyed by the alias's digest identity, which the writer filled.
    with ConnectionManager(connection_config=conf) as connection:
        (alias,) = connection.fetch_docs("Alias")
        asyncio.run(
            write_container(
                _schema(),
                GraphContainer(
                    edges={ALIAS_OF: [({"id": alias["id"]}, {"serial": "KP-1"}, {})]}
                ),
                conf,
            )
        )
    with ConnectionManager(connection_config=conf) as connection:
        yield connection


def _m(ref: str, type_: str, quote: str = "", **props: Any) -> PartialRecord:
    return PartialRecord(
        ref=ref,
        type=type_,
        may_create=False,
        props={k: StatedProp(value=v) for k, v in props.items()},
        evidence={"quote": quote or ref},
    )


def _gather(db: Any, records: list[PartialRecord], **kwargs: Any):
    schema = _schema()
    graphs = [RecordGraph(source_id="s1", schema_ref="plant@1.0.0", records=records)]
    resolution = resolve_records(schema, graphs, db=db)
    return gather_candidates(
        schema,
        resolution,
        db,
        spec=SPEC,
        lookup_text=lambda r: (r.evidence or {}).get("quote"),
        **kwargs,
    )


def _entry(entries: list[Any], ref: str) -> Any:
    return next(e for e in entries if e.record.ref == ref)


def _found(entry: Any) -> list[tuple[str, list[str]]]:
    return [
        (next(iter(c.key.values())), [h.lane for h in c.lanes])
        for c in entry.candidates
    ]


# -- context ------------------------------------------------------------------


def test_a_reference_its_context_fixes_is_offered_around_a_resolved_entity(db):
    entries = _gather(
        db, [_m("p", "Product", sku="P-200"), _m("c", "Component", "Its controller")]
    )
    entry = _entry(entries, "c")
    assert _found(entry) == [("CTL-200", ["context"]), ("DRV-200", ["context"])]
    assert entry.candidates[0].lanes[0].anchor == "p"


def test_a_stated_label_ranks_the_neighbours_it_fits_first(db):
    records = [
        _m("p", "Product", sku="P-200"),
        _m("c", "Component", "the drive", name="drive"),
    ]
    entry = _entry(_gather(db, records), "c")
    assert [(c.key["code"], c.lanes[-1].level) for c in entry.candidates] == [
        ("DRV-200", "compatible"),
        ("CTL-200", "differs"),
    ]


def test_a_neighbour_in_conflict_on_an_exclusive_field_is_flagged_not_accepted(db):
    records = [
        _m("p", "Product", sku="P-200"),
        _m("c", "Component", "the controller", name="controller", kind="ctl"),
    ]
    entry = _entry(_gather(db, records), "c")
    by_key = {c.key["code"]: c for c in entry.candidates}
    assert not by_key["CTL-200"].support.conflict
    assert by_key["DRV-200"].support.conflict
    assert support(by_key["DRV-200"]) == "propose"


def test_a_known_subject_anchors_the_lane(db):
    subjects = {"s1": [KnownSubject(vertex_type="Product", key={"sku": "P-200"})]}
    (entry,) = _gather(
        db, [_m("c", "Component", "the unit's drive")], subjects=subjects
    )
    hits = {c.key["code"]: c.lanes[0] for c in entry.candidates}
    assert set(hits) == {"CTL-200", "DRV-200"}
    assert all((h.lane, h.role) == ("context", "subject") for h in hits.values())


def test_no_anchor_no_context(db):
    (entry,) = _gather(db, [_m("c", "Component", "Its controller")])
    assert entry.candidates == []


# -- compatible, alias and support ---------------------------------------------------


def test_a_stated_less_specific_value_finds_its_entities_compatible(db):
    (entry,) = _gather(db, [_m("u", "Unit", "the unit", site="Berlin")])
    by_key = {c.key["serial"]: c for c in entry.candidates}
    assert set(by_key) == {"KP-1", "KP-2"}
    assert [(a.field, a.level) for a in by_key["KP-1"].attributes] == [
        ("site", "compatible")
    ]
    assert all(support(c) == "propose" for c in by_key.values())


def test_an_alias_naming_one_entity_is_identifying_support(db):
    (entry,) = _gather(db, [_m("u", "Unit", "the Berlin pump", site="Berlin")])
    by_key = {c.key["serial"]: c for c in entry.candidates}
    assert by_key["KP-1"].support.unique_alias
    assert (support(by_key["KP-1"]), support(by_key["KP-2"])) == ("accept", "propose")


@pytest.mark.parametrize(
    ("features", "verdict"),
    [
        (SupportFeatures(identifying_lane=True), "accept"),
        (SupportFeatures(exclusive_agreement=True), "accept"),
        (SupportFeatures(unique_alias=True), "accept"),
        (SupportFeatures(identifying_lane=True, conflict=True), "propose"),
        (SupportFeatures(), "propose"),
    ],
)
def test_support_needs_an_identifying_feature_and_no_conflict(features, verdict):
    candidate = EntityCandidate(
        vertex_type="Unit",
        key={"serial": "KP-1"},
        lanes=[CandidateHit(lane="compatible", matched="Berlin")],
        support=features,
    )
    assert support(candidate) == verdict


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"lane": "context", "anchor": "p"}, "level"),
        (
            {"lane": "context", "role": "subject", "anchor": "p", "level": "exact"},
            "anchor",
        ),
        ({"lane": "exact", "matched": "x", "targets": 1}, "targets"),
    ],
)
def test_a_hit_carries_exactly_the_fields_of_its_lane(fields, message):
    with pytest.raises(ValueError, match=message):
        CandidateHit(**fields)


def test_a_refused_source_is_never_looked_up_by_its_derived_keys(db):
    (entry,) = _gather(
        db,
        [_m("u", "Unit", "the Berlin unit", site="Berlin")],
        comparable=lambda _type, field: field != "site",
    )
    assert entry.candidates == []
