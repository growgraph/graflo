"""Algebraic laws of the contract operations, checked over generated inputs.

The example-based suites pin each law on one hand-built manifest per op. That
shows a law *can* hold; it does not show the law. Here the manifests and the ops
are generated from a small closed vocabulary, so a law is exercised against
every shape that vocabulary can express -- including the ones nobody thought to
write down, which is where a law that is only nearly true gives way.

Each law is stated as it is meant, not as it is known to pass. A law that does
not hold yet is marked ``xfail(strict=True)`` with the mechanism that breaks it:
the suite stays green, and turns red the moment the law starts to hold, so the
marker cannot outlive the defect it records.

A refusal is a result, not a failure: an op that does not apply raises
``ValueError``, and the laws are stated over outcomes (a manifest, or a
refusal). Anything else escaping a handler is a crash and fails the test.

The per-op laws are parametrized by op kind, so one kind breaking a law does not
hide another behind it.
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.autogenerate import (
    diff_manifests,
    diff_manifests_verified,
)
from graflo.architecture.evolution.codec import op_from_dict, ops_to_dicts
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.inverse import invert_op, invert_ops
from graflo.architecture.evolution.merge3 import (
    MergeError,
    merge_three_way,
    ops_independent,
)
from graflo.architecture.evolution.ops import ManifestOp

# Derandomized and database-free: a law suite that passes or fails depending on
# the seed is a flaky test, not a law.
LAWS = settings(
    max_examples=200,
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
PER_KIND = settings(LAWS, max_examples=60)

# The closed vocabulary. No two names share a canonical key, so a collision in a
# generated manifest is always one the generator intended.
VERTEX_POOL = ["person", "company", "city", "order"]
FIELD_POOL = ["name", "email", "since"]
RELATION_POOL = ["knows", "employs", "ships"]
EDGE_FIELD_POOL = ["weight", "note"]

#: Every op kind the generator can draw. ``remove_edges`` appears twice because
#: it addresses edges two ways, and the two ways have different footprints.
KINDS = [
    "add_vertices",
    "remove_vertices",
    "rename_vertices",
    "add_vertex_properties",
    "remove_vertex_properties",
    "rename_vertex_properties",
    "change_field_types",
    "set_vertex_descriptions",
    "add_edges",
    "remove_edges:relation",
    "remove_edges:triple",
    "retarget_edges",
    "rename_relations",
    "add_edge_properties",
    "remove_edge_properties",
]

#: ``change_field_types`` is declared irreversible: no inverse is ever offered,
#: so the inverse law has nothing to say about it.
REVERSIBLE_KINDS = [kind for kind in KINDS if kind != "change_field_types"]


# ── generators ──────────────────────────────────────────────────────────────


def _subset(pool: list[str]) -> st.SearchStrategy[list[str]]:
    return st.lists(st.sampled_from(pool), unique=True)


@st.composite
def manifests(draw: st.DrawFn) -> GraphManifest:
    """A schema-only manifest over the closed vocabulary."""
    names = draw(
        st.lists(st.sampled_from(VERTEX_POOL), unique=True, min_size=1, max_size=3)
    )
    vertices = [
        {
            "name": name,
            "properties": [{"name": f} for f in ["id", *draw(_subset(FIELD_POOL))]],
            "identity": ["id"],
        }
        for name in names
    ]
    triples = draw(
        st.lists(
            st.tuples(
                st.sampled_from(names),
                st.sampled_from(names),
                st.sampled_from(RELATION_POOL),
            ),
            unique=True,
            max_size=3,
        )
    )
    edges = [
        {
            "source": source,
            "target": target,
            "relation": relation,
            "properties": [{"name": f} for f in draw(_subset(EDGE_FIELD_POOL))],
        }
        for source, target, relation in triples
    ]
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "laws"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": edges},
                },
            }
        }
    )


def _core(manifest: GraphManifest) -> Any:
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema


def _payloads(manifest: GraphManifest) -> dict[str, st.SearchStrategy[dict[str, Any]]]:
    """A payload strategy for each op kind that has something to address here.

    Mostly well-addressed, deliberately not always applicable: removing an
    identity field, renaming onto a name already taken, removing the last
    vertex are all drawn, because the laws are about refusals as much as about
    results.
    """
    core = _core(manifest)
    present = sorted(core.vertex_config.vertex_set)
    absent = [name for name in VERTEX_POOL if name not in present]
    fields = {
        v.name: [f.name for f in v.properties] for v in core.vertex_config.vertices
    }
    edges = [(e.source, e.target, e.relation) for e in core.edge_config.edges]
    relations = sorted({relation for _, _, relation in edges})
    unused_relations = [r for r in RELATION_POOL if r not in relations]

    out: dict[str, st.SearchStrategy[dict[str, Any]]] = {}
    if absent:
        out["add_vertices"] = st.sampled_from(absent).map(
            lambda v: {
                "op": "add_vertices",
                "vertices": [{"name": v, "properties": ["id"], "identity": ["id"]}],
            }
        )
    if not present:
        return out

    vertex = st.sampled_from(present)
    field = st.sampled_from(FIELD_POOL)
    out["remove_vertices"] = vertex.map(
        lambda v: {"op": "remove_vertices", "names": [v]}
    )
    out["add_vertex_properties"] = st.tuples(vertex, field).map(
        lambda t: {"op": "add_vertex_properties", "additions": {t[0]: [t[1]]}}
    )
    out["remove_vertex_properties"] = st.tuples(
        vertex, st.sampled_from(["id", *FIELD_POOL])
    ).map(lambda t: {"op": "remove_vertex_properties", "removals": {t[0]: [t[1]]}})
    out["rename_vertex_properties"] = st.tuples(vertex, field, field).map(
        lambda t: {"op": "rename_vertex_properties", "renames": {t[0]: {t[1]: t[2]}}}
    )
    out["change_field_types"] = st.tuples(
        vertex, st.sampled_from(["STRING", "INT"])
    ).map(
        lambda t: {
            "op": "change_field_types",
            "vertices": {t[0]: {fields[t[0]][-1]: {"type": t[1]}}},
        }
    )
    out["set_vertex_descriptions"] = st.tuples(
        vertex, st.text("ab", min_size=1, max_size=3)
    ).map(lambda t: {"op": "set_vertex_descriptions", "descriptions": {t[0]: t[1]}})
    out["add_edges"] = st.tuples(vertex, vertex, st.sampled_from(RELATION_POOL)).map(
        lambda t: {
            "op": "add_edges",
            "edges": [{"source": t[0], "target": t[1], "relation": t[2]}],
        }
    )
    if absent:
        out["rename_vertices"] = st.tuples(vertex, st.sampled_from(absent)).map(
            lambda t: {"op": "rename_vertices", "renames": {t[0]: t[1]}}
        )
    if not edges:
        return out

    edge = st.sampled_from(edges)
    relation = st.sampled_from(relations)
    edge_field = st.sampled_from(EDGE_FIELD_POOL)
    out["remove_edges:relation"] = relation.map(
        lambda r: {"op": "remove_edges", "relations": [r]}
    )
    out["remove_edges:triple"] = edge.map(
        lambda e: {
            "op": "remove_edges",
            "edges": [{"source": e[0], "target": e[1], "relation": e[2]}],
        }
    )
    out["retarget_edges"] = st.tuples(edge, vertex).map(
        lambda t: {
            "op": "retarget_edges",
            "edges": [
                {
                    "source": t[0][0],
                    "target": t[0][1],
                    "relation": t[0][2],
                    "new_target": t[1],
                }
            ],
        }
    )
    out["add_edge_properties"] = st.tuples(relation, edge_field).map(
        lambda t: {"op": "add_edge_properties", "additions": {t[0]: [t[1]]}}
    )
    out["remove_edge_properties"] = st.tuples(relation, edge_field).map(
        lambda t: {"op": "remove_edge_properties", "removals": {t[0]: [t[1]]}}
    )
    if unused_relations:
        out["rename_relations"] = st.tuples(
            relation, st.sampled_from(unused_relations)
        ).map(lambda t: {"op": "rename_relations", "renames": {t[0]: t[1]}})
    return out


def _is_an_op(payload: dict[str, Any]) -> bool:
    """Whether the op model accepts *payload*.

    A rename onto its own name, a retarget that moves nothing: those are not
    ops, so they are not drawn -- unlike an op that is well formed and refused
    by the manifest it meets, which is a case the laws cover.
    """
    try:
        op_from_dict(payload)
    except ValueError:
        return False
    return True


def ops_for(
    manifest: GraphManifest, kind: str | None = None
) -> st.SearchStrategy[ManifestOp]:
    """Ops addressed at *manifest*: of one *kind*, or of any kind it admits."""
    payloads = _payloads(manifest)
    if kind is not None:
        assume(kind in payloads)
        chosen = payloads[kind]
    else:
        assume(payloads)
        chosen = st.one_of(list(payloads.values()))
    return chosen.filter(_is_an_op).map(op_from_dict)


@st.composite
def manifest_and_op(
    draw: st.DrawFn, kind: str | None = None
) -> tuple[GraphManifest, ManifestOp]:
    manifest = draw(manifests())
    return manifest, draw(ops_for(manifest, kind))


@st.composite
def manifest_and_two_ops(
    draw: st.DrawFn,
) -> tuple[GraphManifest, ManifestOp, ManifestOp]:
    """One base and two ops both drawn against it -- two branches, not a sequence."""
    manifest = draw(manifests())
    return manifest, draw(ops_for(manifest)), draw(ops_for(manifest))


@st.composite
def manifest_and_sequence(draw: st.DrawFn) -> tuple[GraphManifest, list[ManifestOp]]:
    """A base and ops each drawn against the state the previous one produced."""
    manifest = draw(manifests())
    current, sequence = manifest, []
    for _ in range(draw(st.integers(min_value=2, max_value=3))):
        op = draw(ops_for(current))
        following = _apply(current, [op])
        assume(following is not None)
        assert following is not None
        current = following
        sequence.append(op)
    return manifest, sequence


# ── outcomes ────────────────────────────────────────────────────────────────


def _apply(manifest: GraphManifest, ops: list[ManifestOp]) -> GraphManifest | None:
    """The manifest *ops* produce, or ``None`` when one of them is refused."""
    try:
        return apply_evolution(manifest, ops, bump_version=False, finish_init=False)
    except ValueError:
        return None


def _outcome(manifest: GraphManifest, ops: list[ManifestOp]) -> str:
    applied = _apply(manifest, ops)
    return "refused" if applied is None else manifest_hash(applied)


def _merge_outcome(
    base: GraphManifest, left: GraphManifest, right: GraphManifest
) -> tuple[str, Any]:
    try:
        merged, result = merge_three_way(base, left, right)
    except MergeError:
        return ("error", None)
    if merged is None:
        return ("conflict", sorted(tuple(c.slot) for c in result.conflicts))
    return ("clean", manifest_hash(merged))


def _two_branches(
    case: tuple[GraphManifest, ManifestOp, ManifestOp],
) -> tuple[GraphManifest, GraphManifest, GraphManifest]:
    base, a, b = case
    left, right = _apply(base, [a]), _apply(base, [b])
    assume(left is not None and right is not None)
    assert left is not None and right is not None
    return base, left, right


# ── preservation ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", KINDS)
@PER_KIND
@given(data=st.data())
def test_an_op_that_applies_leaves_a_manifest_that_loads(kind, data) -> None:
    """Well-formed in, well-formed out -- and ``finish_init`` adds nothing.

    Inversion and diffing run with ``finish_init=False``, so they are only sound
    if an applied op never depends on that pass to repair its result.
    """
    manifest, op = data.draw(manifest_and_op(kind))
    raw = _apply(manifest, [op])
    assume(raw is not None)
    assert raw is not None

    finished = apply_evolution(manifest, [op], bump_version=False, finish_init=True)

    assert manifest_hash(finished) == manifest_hash(raw)
    assert manifest_hash(GraphManifest.from_dict(finished.to_dict())) == manifest_hash(
        finished
    )


# ── inversion ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", REVERSIBLE_KINDS)
@PER_KIND
@given(data=st.data())
def test_an_inverse_restores_the_manifest(kind, data) -> None:
    """Exact or absent: an inverse that is offered lands back on the pre-state."""
    manifest, op = data.draw(manifest_and_op(kind))
    forward = _apply(manifest, [op])
    assume(forward is not None)
    assert forward is not None
    inverse = invert_op(op, manifest=manifest)
    assume(inverse is not None)
    assert inverse is not None

    restored = _apply(forward, [inverse])

    assert restored is not None, f"the inverse of {op.op} is refused"
    assert manifest_hash(restored) == manifest_hash(manifest)


@LAWS
@given(manifest_and_sequence())
def test_a_sequence_inverts_in_reverse(case) -> None:
    manifest, sequence = case
    inverses, blockers = invert_ops(sequence, manifest=manifest)
    assume(not blockers)
    forward = _apply(manifest, sequence)
    assert forward is not None

    restored = _apply(forward, inverses)

    assert restored is not None, "the inverse sequence is refused"
    assert manifest_hash(restored) == manifest_hash(manifest)


# ── diffing ─────────────────────────────────────────────────────────────────


@LAWS
@given(manifests(), manifests())
def test_a_diff_replays_to_its_target(base, target) -> None:
    """Over schema-only manifests every difference is expressible, so: no residue."""
    ops, warnings = diff_manifests_verified(base, target)

    assert not warnings
    assert _outcome(base, ops) == manifest_hash(target)


@LAWS
@given(manifests())
def test_a_manifest_differs_from_itself_by_nothing(manifest) -> None:
    assert diff_manifests(manifest, manifest) == ([], [])


# ── commutation and merge ───────────────────────────────────────────────────


@LAWS
@given(manifest_and_two_ops())
def test_independent_ops_commute(case) -> None:
    """Neither writes where the other writes or reads: one outcome, either order."""
    manifest, a, b = case
    assume(ops_independent(a, b, manifest))

    assert _outcome(manifest, [a, b]) == _outcome(manifest, [b, a])


@LAWS
@given(manifest_and_two_ops())
def test_merge_does_not_depend_on_which_side_is_left(case) -> None:
    base, left, right = _two_branches(case)

    assert _merge_outcome(base, left, right) == _merge_outcome(base, right, left)


@LAWS
@given(manifest_and_two_ops())
def test_a_clean_merge_loses_neither_side(case) -> None:
    """A clean merge is each side's change applied on top of the other.

    That is what "nothing was lost" means for a merge, and it is checkable: what
    the left changed and the right did not must apply to the right and land on
    the merged manifest, and the other way round. A change both sides made is
    agreement and is already in both. A merge that is clean and fails this
    dropped something.
    """
    base, left, right = _two_branches(case)
    left_ops, left_residue = diff_manifests_verified(base, left)
    right_ops, right_residue = diff_manifests_verified(base, right)
    # A change no op expresses cannot be merged, and the merge says so.
    assume(not left_residue and not right_residue)
    kind, merged_hash = _merge_outcome(base, left, right)
    assert kind != "error", "two expressible descendants must merge or conflict"
    assume(kind == "clean")

    left_dicts, right_dicts = ops_to_dicts(left_ops), ops_to_dicts(right_ops)
    left_only = [op for op, d in zip(left_ops, left_dicts) if d not in right_dicts]
    right_only = [op for op, d in zip(right_ops, right_dicts) if d not in left_dicts]

    assert _outcome(right, left_only) == merged_hash
    assert _outcome(left, right_only) == merged_hash
