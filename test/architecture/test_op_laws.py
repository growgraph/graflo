"""Laws over the op registry, stated once for every op.

:mod:`graflo.architecture.evolution.op_laws` joins each op's apply function,
footprint and inverse into one record. The tests here hold for every record:
the registry is total, inversion is classified, and an op changes nothing its
declared writes do not account for (write soundness, in the sense of
:func:`~graflo.architecture.evolution.slot_projection.changes_outside`).
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.inverse import INVERTERS, IRREVERSIBLE
from graflo.architecture.evolution.op_laws import (
    OpLaws,
    laws_for,
    op_kind,
    op_laws,
    op_models,
)
from graflo.architecture.evolution.slot_projection import (
    changes_outside,
    slot_contents,
)
from graflo.architecture.refusal import OpRefusal
from test.architecture.test_evolution_laws import KINDS, PER_KIND, manifests, ops_for

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


# ── write soundness ─────────────────────────────────────────────────────────

#: Kinds whose declared writes miss a change, with the mechanism.
UNSOUND: dict[str, str] = {}


@pytest.mark.parametrize(
    "kind",
    [
        pytest.param(
            kind,
            marks=pytest.mark.xfail(strict=True, reason=UNSOUND[kind])
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
    """

    @PER_KIND
    @given(st.data())
    def law(data: st.DataObject) -> None:
        manifest = data.draw(manifests())
        op = data.draw(ops_for(manifest, kind))
        try:
            after = apply_evolution(
                manifest, [op], bump_version=False, finish_init=False
            )
        except OpRefusal:
            return
        writes = laws_for(op).writes(op)
        assert (
            changes_outside(writes, slot_contents(manifest), slot_contents(after)) == []
        )

    law()


def _schema(vertices: list[str], edges: list[tuple[str, str, str]]):
    from graflo.architecture.contract.manifest import GraphManifest

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
