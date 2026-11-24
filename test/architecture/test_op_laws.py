"""Laws over the op registry, stated once for every op.

:mod:`graflo.architecture.evolution.op_laws` joins each op's apply function,
footprint and inverse into one record. The tests here hold for every record:
the registry is total, inversion is classified, and an op changes nothing its
declared writes do not account for (write soundness, in the sense of
:func:`~graflo.architecture.evolution.slot_projection.changes_outside`).
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.inverse import INVERTERS, IRREVERSIBLE
from graflo.architecture.evolution.op_laws import (
    OpLaws,
    laws_for,
    op_kind,
    op_laws,
    op_models,
)
from graflo.architecture.evolution.ops import ManifestOp
from graflo.architecture.evolution.slot_projection import (
    changes_outside,
    slot_contents,
)
from graflo.architecture.refusal import OpRefusal
from test.architecture.test_evolution_laws import (
    KINDS,
    PER_KIND,
    _op,
    manifests,
    ops_for,
)

# ── the registry ────────────────────────────────────────────────────────────


def test_every_op_in_the_vocabulary_has_exactly_one_record() -> None:
    assert set(op_laws()) == set(op_models())
    assert len({op_kind(model) for model in op_models()}) == len(op_models())


def test_every_op_is_invertible_or_irreversible_and_never_both() -> None:
    kinds = {op_kind(model) for model in op_models()}
    assert set(INVERTERS) | set(IRREVERSIBLE) == kinds
    assert not set(INVERTERS) & set(IRREVERSIBLE)
    for laws in op_laws().values():
        assert (laws.invert is None) == (laws.kind in IRREVERSIBLE)


def test_a_record_that_is_both_or_neither_is_refused() -> None:
    def apply(*_: object) -> None:
        return None

    def footprint(*_: object) -> set:
        return set()

    with pytest.raises(ValueError, match="either invertible or irreversible"):
        OpLaws("x", apply, footprint, footprint, invert=None, irreversible=None)
    with pytest.raises(ValueError, match="either invertible or irreversible"):
        OpLaws("x", apply, footprint, footprint, invert=apply, irreversible="why")


def test_a_non_op_has_no_laws() -> None:
    with pytest.raises(TypeError, match="not an evolution op"):
        laws_for(object())


def test_every_op_is_drawn_by_the_law_generators() -> None:
    """The generated laws cover the whole vocabulary, not the part that was easy."""
    assert {kind.partition(":")[0] for kind in KINDS} == {
        op_kind(model) for model in op_models()
    }


# ── write soundness ─────────────────────────────────────────────────────────


def _pinned(
    vertices: list[dict[str, Any]],
    edges: list[dict[str, Any]] | None = None,
    **blocks: Any,
) -> dict[str, Any]:
    """A manifest payload for a pinned case: a schema, plus any extra blocks.

    ``inverses``, ``symmetric`` and ``db_profile`` go into the schema; anything
    else (``ingestion_model``, ``bindings``) is a block of its own.
    """
    edge_config = {
        "edges": edges or [],
        "inverses": blocks.pop("inverses", []),
        "symmetric": blocks.pop("symmetric", []),
    }
    schema: dict[str, Any] = {
        "metadata": {"name": "laws"},
        "graph": {"vertex_config": {"vertices": vertices}, "edge_config": edge_config},
    }
    if "db_profile" in blocks:
        schema["db_profile"] = blocks.pop("db_profile")
    return {"schema": schema, **blocks}


def _v(name: str, *fields: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "properties": ["id", *fields], "identity": ["id"], **extra}


PERSON, COMPANY = _v("person"), _v("company")
KNOWS = {"source": "person", "target": "person", "relation": "knows"}
KNOWS_PAIR = [{"relation": "knows", "inverse": "known_by"}]
WEIGHED_KNOWS = {**KNOWS, "properties": ["weight"]}
WEIGHT_INDEXED = {"edge_specs": [{**KNOWS, "indexes": [{"fields": ["weight"]}]}]}


def _resource(*pipeline: dict[str, Any]) -> dict[str, Any]:
    """An ingestion block of one resource, ``rows``."""
    return {"resources": [{"name": "rows", "pipeline": list(pipeline)}]}


PRODUCES_PERSON = _resource({"vertex": "person"})
WRITES_KNOWS = _resource(
    {"vertex": "person"},
    {"edge": {"from": "person", "to": "person", "relation": "knows"}},
)
WIRED_ROWS = {
    "connectors": [{"name": "rows_files", "regex": "rows.*", "resource_name": "rows"}],
    "resource_connector": [{"resource": "rows", "connector": "rows_files"}],
}
TAG = {
    "name": "tag",
    "module": "graflo.util.transform",
    "foo": "tagged_key",
    "params": {"tag": "a"},
    "input": ["id"],
    "output": ["name"],
}

#: Cases each kind once changed outside its declared writes, checked before the
#: drawn ones: the manifest, and the op.
REGRESSIONS: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
    "remove_vertices": (
        _pinned([PERSON], [KNOWS], inverses=KNOWS_PAIR),
        {"op": "remove_vertices", "names": ["person"]},
    ),
    "remove_edges:triple": (
        _pinned([PERSON], [{**KNOWS, "directed": False}], symmetric=["knows"]),
        {"op": "remove_edges", "edges": [KNOWS]},
    ),
    "rename_vertices": (
        _pinned([PERSON], ingestion_model=PRODUCES_PERSON),
        {"op": "rename_vertices", "renames": {"person": "order"}},
    ),
    "remove_edges:relation": (
        _pinned([PERSON], [KNOWS], ingestion_model=WRITES_KNOWS),
        {"op": "remove_edges", "relations": ["knows"]},
    ),
    "rename_relations": (
        _pinned([PERSON], [KNOWS], ingestion_model=WRITES_KNOWS),
        {"op": "rename_relations", "renames": {"knows": "employs"}},
    ),
    "merge_edges": (
        _pinned([PERSON], [KNOWS], ingestion_model=WRITES_KNOWS),
        {"op": "merge_edges", "sources": ["knows"], "into": "employs"},
    ),
    "merge_vertices": (
        _pinned([PERSON], ingestion_model=PRODUCES_PERSON),
        {"op": "merge_vertices", "sources": ["person"], "into": "company"},
    ),
    "canonicalize:merge": (
        _pinned([PERSON, COMPANY], ingestion_model=PRODUCES_PERSON),
        {
            "op": "canonicalize",
            "vertices": {"person": "company", "company": "company"},
            "allow_merges": True,
        },
    ),
    "retarget_edges": (
        _pinned([PERSON, COMPANY], [KNOWS], ingestion_model=WRITES_KNOWS),
        {"op": "retarget_edges", "edges": [{**KNOWS, "new_target": "company"}]},
    ),
    "canonicalize": (
        _pinned([PERSON], ingestion_model=PRODUCES_PERSON),
        {"op": "canonicalize", "properties": {"person": {"id": "name"}}},
    ),
    "remove_edge_properties": (
        _pinned([PERSON], [WEIGHED_KNOWS], db_profile=WEIGHT_INDEXED),
        {"op": "remove_edge_properties", "removals": {"knows": ["weight"]}},
    ),
    "remove_edge_properties:edge": (
        _pinned([PERSON], [WEIGHED_KNOWS], db_profile=WEIGHT_INDEXED),
        {
            "op": "remove_edge_properties",
            "per_edge": [{**KNOWS, "fields": ["weight"]}],
        },
    ),
    "rename_vertex_properties": (
        _pinned([_v("person", "name")], ingestion_model=PRODUCES_PERSON),
        {"op": "rename_vertex_properties", "renames": {"person": {"name": "email"}}},
    ),
    "rename_edge_properties:edge": (
        _pinned(
            [PERSON],
            [{**WEIGHED_KNOWS, "identities": [["source", "target", "weight"]]}],
        ),
        {
            "op": "rename_edge_properties",
            "per_edge": [{**KNOWS, "renames": {"weight": "note"}}],
        },
    ),
    "add_inverse_edges:named": (
        _pinned([PERSON], [KNOWS], inverses=KNOWS_PAIR),
        {"op": "add_inverse_edges", "relations": ["knows"]},
    ),
    "remove_vertex_properties": (
        _pinned(
            [_v("person", "email")],
            db_profile={"vertex_indexes": {"person": [{"fields": ["email"]}]}},
        ),
        {"op": "remove_vertex_properties", "removals": {"person": ["email"]}},
    ),
    "add_resources": (
        _pinned([PERSON]),
        {"op": "add_resources", "resources": PRODUCES_PERSON["resources"]},
    ),
    "replace_resources": (
        _pinned([PERSON], ingestion_model=PRODUCES_PERSON),
        {
            "op": "replace_resources",
            "resources": PRODUCES_PERSON["resources"],
            "transforms": [TAG],
        },
    ),
    "add_resource_transforms": (
        _pinned([PERSON], ingestion_model=PRODUCES_PERSON),
        {
            "op": "add_resource_transforms",
            "additions": {"rows": [{"transform": {"call": {"use": "tag"}}}]},
            "transforms": [TAG],
        },
    ),
    "remove_resources": (
        _pinned([PERSON], ingestion_model=PRODUCES_PERSON, bindings=WIRED_ROWS),
        {"op": "remove_resources", "names": ["rows"]},
    ),
    "rename_resources": (
        _pinned([PERSON], ingestion_model=PRODUCES_PERSON, bindings=WIRED_ROWS),
        {"op": "rename_resources", "renames": {"rows": "feed"}},
    ),
    "add_secondary_identities": (
        _pinned([_v("person", "name")]),
        {
            "op": "add_secondary_identities",
            "additions": {"person": [{"name": "name_key", "fields": ["name"]}]},
        },
    ),
    "remove_secondary_identities": (
        _pinned(
            [
                _v(
                    "person",
                    "email",
                    secondary_identities=[{"name": "email_key", "fields": ["email"]}],
                )
            ]
        ),
        {"op": "remove_secondary_identities", "removals": {"person": ["email_key"]}},
    ),
    "replace_identity": (
        _pinned([_v("person", "name")]),
        {
            "op": "replace_identity",
            "replacements": {
                "person": {
                    "to": {"mode": "natural", "identity": ["name"]},
                    "retire": "demote",
                }
            },
        },
    ),
    "set_db_profile": (
        _pinned([_v("person", "name")]),
        {
            "op": "set_db_profile",
            "profile": {"vertex_indexes": {"person": [{"fields": ["name"]}]}},
        },
    ),
    "sanitize": (
        _pinned([PERSON], [KNOWS]),
        {"op": "sanitize", "db_flavor": "arango", "reserved_words": ["knows"]},
    ),
    "retract_edge_inverses": (
        _pinned([PERSON], [KNOWS], inverses=KNOWS_PAIR),
        {"op": "retract_edge_inverses", "relations": ["known_by"]},
    ),
}

#: Kinds whose declared writes miss a change: the mechanism, and the smallest
#: manifest and op that show it.
UNSOUND: dict[str, tuple[str, dict[str, Any], dict[str, Any]]] = {}


def _writes_sound(manifest: GraphManifest, op: ManifestOp) -> None:
    try:
        after = apply_evolution(manifest, [op], bump_version=False, finish_init=False)
    except OpRefusal:
        return
    writes = laws_for(op).writes(op, manifest)
    assert changes_outside(writes, slot_contents(manifest), slot_contents(after)) == []


def test_every_pinned_case_names_a_drawn_kind() -> None:
    assert set(UNSOUND) | set(REGRESSIONS) <= set(KINDS)


@pytest.mark.parametrize(
    "kind",
    [
        pytest.param(
            kind,
            marks=pytest.mark.xfail(
                strict=True, raises=AssertionError, reason=UNSOUND[kind][0]
            )
            if kind in UNSOUND
            else (),
        )
        for kind in KINDS
    ],
)
def test_an_op_changes_nothing_its_writes_do_not_account_for(kind: str) -> None:
    """Applied to a generated manifest, an op changes only what it declares.

    A change is accounted for by a write that covers its slot, or a slot its
    content names: removing a type changes the edges that name it, and an edge
    op reads its endpoints, so the merge sees that collision through reads.
    A kind known to break this is checked on its pinned case first, so its
    marker fails the same way on every run and turns red once the case holds.
    """
    if kind in UNSOUND:
        _reason, manifest, payload = UNSOUND[kind]
        _writes_sound(GraphManifest.from_dict(manifest), _op(payload))
    if kind in REGRESSIONS:
        manifest, payload = REGRESSIONS[kind]
        _writes_sound(GraphManifest.from_dict(manifest), _op(payload))

    @PER_KIND
    @given(st.data())
    def law(data: st.DataObject) -> None:
        manifest = data.draw(manifests())
        _writes_sound(manifest, data.draw(ops_for(manifest, kind)))

    law()


def _schema(vertices: list[str], edges: list[tuple[str, str, str]]):
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "t"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": v,
                                "properties": [{"name": "id"}],
                                "identity": ["id"],
                            }
                            for v in vertices
                        ]
                    },
                    "edge_config": {
                        "edges": [
                            {"source": s, "target": t, "relation": r}
                            for s, t, r in edges
                        ]
                    },
                },
            }
        }
    )


def test_a_change_no_write_covers_is_reported() -> None:
    before = slot_contents(_schema(["person"], []))
    after = slot_contents(_schema(["person", "company"], []))

    assert changes_outside(set(), before, after) != []
    assert changes_outside({("vertex", "company")}, before, after) == []


def test_a_change_to_content_naming_a_written_slot_is_accounted_for() -> None:
    """Dropping ``company`` drops the edge naming it, which lives under the relation."""
    before = slot_contents(
        _schema(["person", "company"], [("person", "company", "knows")])
    )
    after = slot_contents(_schema(["person"], []))

    assert changes_outside({("vertex", "company")}, before, after) == []
    assert changes_outside({("vertex", "person")}, before, after) != []
