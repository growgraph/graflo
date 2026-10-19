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
from hypothesis import HealthCheck, assume, event, given, settings
from hypothesis import strategies as st

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import merge3
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
    op_slots,
    ops_independent,
    split_by_slot,
    take_left,
    take_right,
)
from graflo.architecture.evolution.ops import ManifestOp
from test.hypothesis_budget import examples

# Derandomized and database-free: a law suite that passes or fails depending on
# the seed is a flaky test, not a law.
LAWS = settings(
    max_examples=examples(200),
    deadline=None,
    derandomize=True,
    database=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
PER_KIND = settings(LAWS, max_examples=examples(60))

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
    "set_edge_descriptions",
    "add_edge_properties",
    "remove_edge_properties",
    "add_edge_properties:edge",
    "remove_edge_properties:edge",
    "rename_edge_properties:edge",
    "change_field_types:edge",
]

#: ``change_field_types`` is declared irreversible: no inverse is ever offered,
#: so the inverse law has nothing to say about it.
REVERSIBLE_KINDS = [
    kind for kind in KINDS if kind.partition(":")[0] != "change_field_types"
]


# ── generators ──────────────────────────────────────────────────────────────


def _subset(pool: list[str]) -> st.SearchStrategy[list[str]]:
    return st.lists(st.sampled_from(pool), unique=True)


@st.composite
def manifests(draw: st.DrawFn, min_vertices: int = 1) -> GraphManifest:
    """A schema-only manifest over the closed vocabulary."""
    names = draw(
        st.lists(
            st.sampled_from(VERTEX_POOL), unique=True, min_size=min_vertices, max_size=3
        )
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
    out["set_edge_descriptions"] = st.tuples(
        edge, st.one_of(st.none(), st.text("ab", min_size=1, max_size=3))
    ).map(
        lambda t: {
            "op": "set_edge_descriptions",
            "edges": [
                {
                    "source": t[0][0],
                    "target": t[0][1],
                    "relation": t[0][2],
                    "description": t[1],
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

    def _on(e: tuple[str, str, str | None], **entry: Any) -> list[dict[str, Any]]:
        return [{"source": e[0], "target": e[1], "relation": e[2], **entry}]

    out["add_edge_properties:edge"] = st.tuples(edge, edge_field).map(
        lambda t: {
            "op": "add_edge_properties",
            "per_edge": _on(t[0], properties=[t[1]]),
        }
    )
    out["remove_edge_properties:edge"] = st.tuples(edge, edge_field).map(
        lambda t: {"op": "remove_edge_properties", "per_edge": _on(t[0], fields=[t[1]])}
    )
    out["rename_edge_properties:edge"] = st.tuples(edge, edge_field, edge_field).map(
        lambda t: {
            "op": "rename_edge_properties",
            "per_edge": _on(t[0], renames={t[1]: t[2]}),
        }
    )
    out["change_field_types:edge"] = st.tuples(
        edge, edge_field, st.sampled_from(["STRING", "INT"])
    ).map(
        lambda t: {
            "op": "change_field_types",
            "per_edge": _on(t[0], fields={t[1]: {"type": t[2]}}),
        }
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


# ── a decision is local ─────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", KINDS)
@PER_KIND
@given(data=st.data())
def test_an_op_split_by_slot_is_the_op(kind, data) -> None:
    """Its pieces, applied in order, land where the op lands -- or are refused."""
    manifest, op = data.draw(manifest_and_op(kind))

    assert _outcome(manifest, split_by_slot(op)) == _outcome(manifest, [op])


_RENAMES = {"rename_vertices", "rename_relations"}


def _describe_vertex(vertex: str, text: str) -> dict[str, Any]:
    return {"op": "set_vertex_descriptions", "descriptions": {vertex: text}}


def _retype(vertex: str, field: str, type_: str) -> dict[str, Any]:
    return {"op": "change_field_types", "vertices": {vertex: {field: {"type": type_}}}}


def _triple(edge: tuple[str, str, str]) -> dict[str, str]:
    return {"source": edge[0], "target": edge[1], "relation": edge[2]}


def _describe_edge(edge: tuple[str, str, str], text: str) -> dict[str, Any]:
    return {
        "op": "set_edge_descriptions",
        "edges": [{**_triple(edge), "description": text}],
    }


def _contention(
    manifest: GraphManifest, element: tuple[str, str]
) -> st.SearchStrategy[tuple[dict[str, Any], dict[str, Any]]]:
    """Two payloads that write one slot of *element* differently, in either order.

    Only pairs that collide whenever both apply: a rename or an edit of another
    field never meets its counterpart, so it would not make a conflict.
    """
    core = _core(manifest)
    kind, name = element
    texts = st.lists(
        st.text("ab", min_size=1, max_size=3), min_size=2, max_size=2, unique=True
    )
    if kind == "vertex":
        pairs = [
            texts.map(
                lambda t: (_describe_vertex(name, t[0]), _describe_vertex(name, t[1]))
            ),
            texts.map(
                lambda t: (
                    {"op": "remove_vertices", "names": [name]},
                    _describe_vertex(name, t[0]),
                )
            ),
        ]
        fields = [
            f.name
            for v in core.vertex_config.vertices
            if v.name == name
            for f in v.properties
            if f.name != "id"
        ]
        if fields:
            field = st.sampled_from(fields)
            pairs += [
                field.map(
                    lambda f: (_retype(name, f, "STRING"), _retype(name, f, "INT"))
                ),
                st.tuples(field, st.sampled_from(["STRING", "INT"])).map(
                    lambda t: (
                        {"op": "remove_vertex_properties", "removals": {name: [t[0]]}},
                        _retype(name, t[0], t[1]),
                    )
                ),
            ]
    else:
        edge = st.sampled_from(
            [
                (e.source, e.target, e.relation)
                for e in core.edge_config.edges
                if e.relation == name
            ]
        )
        pairs = [
            st.tuples(edge, texts).map(
                lambda t: (_describe_edge(t[0], t[1][0]), _describe_edge(t[0], t[1][1]))
            ),
            st.tuples(edge, texts).map(
                lambda t: (
                    {"op": "remove_edges", "edges": [_triple(t[0])]},
                    _describe_edge(t[0], t[1][0]),
                )
            ),
        ]
    return st.tuples(st.one_of(pairs), st.booleans()).map(
        lambda t: (t[0][1], t[0][0]) if t[1] else t[0]
    )


def _step(manifest: GraphManifest, payload: dict[str, Any]) -> GraphManifest:
    """*manifest* after *payload*; itself when the op is malformed or refused."""
    if not _is_an_op(payload):
        return manifest
    return _apply(manifest, [op_from_dict(payload)]) or manifest


@st.composite
def manifest_and_two_contending_branches(
    draw: st.DrawFn,
) -> tuple[GraphManifest, GraphManifest, GraphManifest]:
    """A base and two descendants that collide on two or three elements.

    The collisions are built, not filtered for. Each branch also takes up to one
    unrelated op: that is what a decision must not carry along with it.
    """
    base = draw(manifests(min_vertices=2))
    core = _core(base)
    elements = [("vertex", v) for v in sorted(core.vertex_config.vertex_set)] + [
        ("relation", r)
        for r in sorted({e.relation for e in core.edge_config.edges if e.relation})
    ]
    hot = draw(st.lists(st.sampled_from(elements), min_size=2, max_size=3, unique=True))
    left = right = base
    for element in hot:
        a, b = draw(_contention(base, element))
        left, right = _step(left, a), _step(right, b)

    def unrelated(branch: GraphManifest) -> GraphManifest:
        payloads = _payloads(branch)
        if not payloads or not draw(st.booleans()):
            return branch
        return _step(branch, draw(st.one_of(list(payloads.values()))))

    return base, unrelated(left), unrelated(right)


@LAWS
@given(manifest_and_two_contending_branches())
def test_a_decision_changes_nothing_outside_its_conflict(case) -> None:
    """Flipping one conflict's decision moves only what that conflict reaches.

    Each side's change set is one op per kind for the whole manifest; a decision
    that held a whole op would take the side's unrelated edits along with it.
    """
    base, left, right = case
    try:
        _none, result = merge_three_way(base, left, right)
    except MergeError:
        assume(False)
        return
    event(f"conflicts={min(len(result.conflicts), 3)}")
    assume(len(result.conflicts) >= 2)

    def merged(flip: int | None) -> GraphManifest | None:
        decisions = [
            take_right(c) if i == flip else take_left(c)
            for i, c in enumerate(result.conflicts)
        ]
        try:
            return merge_three_way(base, left, right, resolutions=decisions)[0]
        except MergeError:
            return None

    all_left = merged(None)
    assume(all_left is not None)
    assert all_left is not None
    for flip, conflict in enumerate(result.conflicts):
        flipped = merged(flip)
        if flipped is None:
            continue
        # Stated per element (a vertex, a relation): the diff may spell what
        # moved with a different op than the one that moved it -- an edge added
        # with a property reads back as that property -- but never on another
        # element. A rename reaches both its names; a vertex reaches the
        # relations incident to it, since removing it removes their edges.
        reach = {tuple(conflict.slot)[:2]} | {
            slot[:2]
            for op in [*conflict.left_ops, *conflict.right_ops]
            if op.op in _RENAMES
            for slot in op_slots(op)
        }
        reach |= {
            merge3._relation_slot(edge.relation)
            for manifest in (base, left, right, all_left, flipped)
            for edge in _core(manifest).edge_config.edges
            if edge.relation is not None
            and {merge3._vertex_slot(edge.source), merge3._vertex_slot(edge.target)}
            & reach
        }
        moved, _residue = diff_manifests(all_left, flipped)
        for op in moved:
            assert {slot[:2] for slot in op_slots(op)} <= reach, (
                f"flipping {conflict.slot} moved {op.op} outside it"
            )
