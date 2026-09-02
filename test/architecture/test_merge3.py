"""Three-way merge: slots, auto-merge, conflicts, determinism, re-merge.

The load-bearing property is the slot map. If it is too coarse, unrelated edits
collide and merging is useless; if it is too fine, colliding edits merge and the
result is a schema neither author wrote. The second failure is silent, so most
of what is asserted here is that things which *should* conflict do.
"""

from __future__ import annotations

import inspect
import typing

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import merge3
from graflo.architecture.evolution import ops as ops_module
from graflo.architecture.evolution.commit import build_commit
from graflo.architecture.evolution.history import History
from graflo.architecture.evolution.merge3 import (
    ConflictResolution,
    MergeRecipe,
    build_recipe,
    describe_slot,
    find_merge_base,
    merge_three_way,
    op_slots,
    re_merge,
    take_left,
    take_right,
)
from graflo.architecture.evolution.ops import (
    AddVertexPropertiesOp,
    ManifestOp,
    RenameVerticesOp,
)


def _vertex(name: str, properties: list[str], identity: list[str]) -> dict:
    return {
        "name": name,
        "properties": [{"name": p} for p in properties],
        "identity": identity,
    }


def _manifest(vertices: list[dict], edges: list[dict] | None = None) -> GraphManifest:
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "t"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": edges or []},
                },
            }
        }
    )


def _person(properties: list[str], identity: list[str] | None = None) -> GraphManifest:
    return _manifest([_vertex("person", properties, identity or ["id"])])


def _core(manifest):
    """The core schema, with the Optional narrowed for the type checker."""
    schema = manifest.graph_schema
    assert schema is not None
    return schema.core_schema


# ── the slot map ────────────────────────────────────────────────────────────


def test_every_op_in_the_vocabulary_has_an_explicit_slot_mapping() -> None:
    """No op may reach the catch-all by accident.

    An unmapped op is treated as touching the whole manifest, so it conflicts
    with everything: it does not corrupt a schema, it makes that op unmergeable
    -- quietly, and only for whoever happens to use it. Checking the dispatch
    source for every member of the union is what makes adding op 32 without a
    slot a failing test rather than a latent one.
    """
    source = inspect.getsource(merge3.op_slots)
    union = typing.get_args(typing.get_args(ManifestOp)[0])
    missing = sorted(
        model.__name__ for model in union if f"ops.{model.__name__}" not in source
    )
    assert not missing, (
        f"these ops have no slot mapping in op_slots: {missing}. Map each to "
        "the location it touches, or add it to the whole-manifest branch if it "
        "genuinely rewrites everything."
    )


def test_the_whole_manifest_ops_are_the_only_ones_that_conflict_with_everything() -> (
    None
):
    """`("manifest",)` is a deliberate answer for three ops, not a default."""
    assert op_slots(ops_module.SanitizeOp(db_flavor="arango")) == {("manifest",)}
    assert op_slots(ops_module.ProjectManifestOp(keep_vertices=["person"])) == {
        ("manifest",)
    }


def test_a_vertex_slot_is_convention_independent() -> None:
    """The fold of CORE-MERGE-001 into merge.

    Without this, one side's `order_line` and the other's `OrderLine` occupy
    different slots, merge cleanly, and produce a schema holding both as
    unrelated types with the data split between them -- the exact defect, now
    reachable through a tracked re-merge rather than only through compose.
    """
    snake = AddVertexPropertiesOp(additions={"order_line": ["qty"]})
    pascal = AddVertexPropertiesOp(additions={"OrderLine": ["qty"]})
    plural = AddVertexPropertiesOp(additions={"OrderLines": ["qty"]})
    assert op_slots(snake) == op_slots(pascal) == op_slots(plural)


def test_a_rename_occupies_both_of_its_names() -> None:
    rename = RenameVerticesOp(vertices={"person": "customer"})
    assert op_slots(rename) == {("vertex", "person"), ("vertex", "customer")}


def test_a_rename_collides_with_an_edit_to_the_thing_renamed() -> None:
    """Slot containment, asserted where it matters.

    The rename holds `vertex/person`; the edit holds
    `vertex/person/field/age`. They are different slots, so equality alone
    would merge them cleanly -- and produce a change set whose second half
    addresses a vertex the first half renamed away.
    """
    base = _person(["id"])
    renamed = _manifest([_vertex("customer", ["id"], ["id"])])
    edited = _person(["id", "age"])

    from graflo.architecture.evolution.autogenerate import RenameHints

    merged, result = merge_three_way(
        base,
        renamed,
        edited,
        hints=RenameHints(vertices={"person": "customer"}),
    )
    assert merged is None, "a rename and an edit to the renamed vertex must conflict"
    # The rename occupies both names, so either is a fair place to report it.
    assert {c.slot_key for c in result.conflicts} <= {
        ("vertex", "person"),
        ("vertex", "customer"),
    }
    assert result.conflicts


def test_slots_are_renderable_for_a_human() -> None:
    slots = op_slots(AddVertexPropertiesOp(additions={"person": ["age"]}))
    assert describe_slot(next(iter(slots))) == "vertex/person/field/age"


# ── automatic merge ─────────────────────────────────────────────────────────


def test_disjoint_changes_merge_automatically() -> None:
    base = _person(["id"])
    merged, result = merge_three_way(
        base, _person(["id", "age"]), _person(["id", "email"])
    )
    assert result.clean
    assert merged is not None
    names = [f.name for f in _core(merged).vertex_config.vertices[0].properties]
    assert set(names) == {"id", "age", "email"}


def test_the_identical_change_on_both_sides_is_agreement_not_duplication() -> None:
    base = _person(["id"])
    merged, result = merge_three_way(
        base, _person(["id", "age"]), _person(["id", "age"])
    )
    assert result.clean
    names = [f.name for f in _core(merged).vertex_config.vertices[0].properties]
    assert names.count("age") == 1


def test_a_side_that_changed_nothing_merges_to_the_other_side() -> None:
    base = _person(["id"])
    merged, result = merge_three_way(base, _person(["id", "age"]), _person(["id"]))
    assert result.clean
    assert merged is not None
    names = {f.name for f in _core(merged).vertex_config.vertices[0].properties}
    assert names == {"id", "age"}


def test_two_sides_changing_nothing_merge_to_the_base() -> None:
    base = _person(["id"])
    merged, result = merge_three_way(base, _person(["id"]), _person(["id"]))
    assert result.clean
    assert merged is not None


# ── conflicts ───────────────────────────────────────────────────────────────


def test_two_identities_on_one_vertex_conflict() -> None:
    """Identity is a whole-vertex slot, so different re-keys must collide."""
    base = _person(["id", "ssn", "email"], ["id"])
    merged, result = merge_three_way(
        base,
        _person(["id", "ssn", "email"], ["ssn"]),
        _person(["id", "ssn", "email"], ["email"]),
    )
    assert merged is None
    assert len(result.conflicts) == 1
    assert result.conflicts[0].slot_key[-1] == "identity"
    assert result.conflicts[0].left_ops and result.conflicts[0].right_ops


def test_a_conflict_carries_the_ancestor_state_for_a_human() -> None:
    base = _person(["id", "ssn"], ["id"])
    _merged, result = merge_three_way(
        base, _person(["id", "ssn"], ["ssn"]), _person(["id", "ssn"], ["id", "ssn"])
    )
    assert result.conflicts
    assert result.conflicts[0].base_excerpt.get("name") == "person"


def test_taking_a_side_resolves_the_conflict() -> None:
    base = _person(["id", "ssn", "email"], ["id"])
    left = _person(["id", "ssn", "email"], ["ssn"])
    right = _person(["id", "ssn", "email"], ["email"])

    _none, result = merge_three_way(base, left, right)
    merged, resolved = merge_three_way(
        base, left, right, resolutions=[take_left(result.conflicts[0])]
    )
    assert resolved.clean
    assert _core(merged).vertex_config.vertices[0].identity == ["ssn"]

    merged_right, _ = merge_three_way(
        base, left, right, resolutions=[take_right(result.conflicts[0])]
    )
    assert _core(merged_right).vertex_config.vertices[0].identity == ["email"]


def test_a_resolution_may_be_neither_side() -> None:
    """`ConflictResolution` holds ops, so a third answer needs no new machinery."""
    base = _person(["id", "ssn", "email"], ["id"])
    left = _person(["id", "ssn", "email"], ["ssn"])
    right = _person(["id", "ssn", "email"], ["email"])
    _none, result = merge_three_way(base, left, right)

    merged, resolved = merge_three_way(
        base,
        left,
        right,
        resolutions=[
            ConflictResolution(slot=list(result.conflicts[0].slot), ops=[]),
        ],
    )
    assert resolved.clean
    # Neither side's re-key was applied: the ancestor's identity stands.
    assert _core(merged).vertex_config.vertices[0].identity == ["id"]


def test_an_op_touching_a_contested_slot_is_held_back_whole() -> None:
    """Applying half an op is not a merge."""
    base = _manifest(
        [_vertex("person", ["id", "ssn"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    left = _manifest(
        [
            _vertex("person", ["id", "ssn"], ["ssn"]),
            _vertex("company", ["id", "left_only"], ["id"]),
        ]
    )
    right = _manifest(
        [
            _vertex("person", ["id", "ssn"], ["id", "ssn"]),
            _vertex("company", ["id"], ["id"]),
        ]
    )
    merged, result = merge_three_way(base, left, right)
    assert merged is None
    assert any(c.slot_key[-1] == "identity" for c in result.conflicts)


# ── determinism ─────────────────────────────────────────────────────────────


def test_the_same_inputs_produce_the_same_merge() -> None:
    base = _person(["id"])
    left, right = _person(["id", "age"]), _person(["id", "email"])
    first, _ = merge_three_way(base, left, right)
    second, _ = merge_three_way(base, left, right)
    from graflo.architecture.evolution.hashing import manifest_hash

    assert manifest_hash(first) == manifest_hash(second)


def test_conflicts_come_back_in_a_stable_order() -> None:
    base = _manifest(
        [_vertex("a", ["id", "p"], ["id"]), _vertex("b", ["id", "q"], ["id"])]
    )
    left = _manifest(
        [_vertex("a", ["id", "p"], ["p"]), _vertex("b", ["id", "q"], ["q"])]
    )
    right = _manifest(
        [_vertex("a", ["id", "p"], ["id", "p"]), _vertex("b", ["id", "q"], ["id", "q"])]
    )
    first = [c.slot for c in merge_three_way(base, left, right)[1].conflicts]
    second = [c.slot for c in merge_three_way(base, left, right)[1].conflicts]
    assert first == second == sorted(first)


# ── merge base ──────────────────────────────────────────────────────────────


def _linear_history() -> tuple[History, list]:
    from graflo.architecture.evolution.apply import apply_evolution

    base = _person(["id"])
    root = build_commit(base, [AddVertexPropertiesOp(additions={"person": ["a"]})])
    after_root = apply_evolution(
        base, list(root.ops), bump_version=False, finish_init=False
    )
    left = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["l"]})],
        parents=[root.id],
    )
    right = build_commit(
        after_root,
        [AddVertexPropertiesOp(additions={"person": ["r"]})],
        parents=[root.id],
    )
    return History(commits=[root, left, right]), [root, left, right]


def test_the_merge_base_of_two_branches_is_their_fork_point() -> None:
    history, (root, left, right) = _linear_history()
    assert find_merge_base(history, left.id, right.id) == root.id


def test_unrelated_lineages_have_no_merge_base() -> None:
    """Which is the signal that the operation you want is compose, not merge."""
    history, (_root, left, _right) = _linear_history()
    stranger = build_commit(
        _person(["id"]), [AddVertexPropertiesOp(additions={"person": ["x"]})]
    )
    combined = History(commits=[*history.commits, stranger])
    assert find_merge_base(combined, left.id, stranger.id) is None


def test_a_commit_is_its_own_merge_base_with_a_descendant() -> None:
    history, (root, left, _right) = _linear_history()
    assert find_merge_base(history, root.id, left.id) == root.id


# ── tracked merges ──────────────────────────────────────────────────────────


def test_a_recipe_is_addressed_by_content_not_by_input_order() -> None:
    base = _person(["id", "ssn", "email"], ["id"])
    left = _person(["id", "ssn", "email"], ["ssn"])
    right = _person(["id", "ssn", "email"], ["email"])
    _none, result = merge_three_way(base, left, right)
    resolution = take_left(result.conflicts[0])

    one = build_recipe(base, left, right, resolutions=[resolution])
    two = build_recipe(base, left, right, resolutions=[resolution])
    assert one.content_hash() == two.content_hash()

    different = build_recipe(
        base, left, right, resolutions=[take_right(result.conflicts[0])]
    )
    assert different.content_hash() != one.content_hash()


def test_re_merge_replays_a_recorded_resolution() -> None:
    """The rerere analogue: the same conflict does not have to be decided twice."""
    base = _person(["id", "ssn", "email"], ["id"])
    left = _person(["id", "ssn", "email"], ["ssn"])
    right = _person(["id", "ssn", "email"], ["email"])

    _none, result = merge_three_way(base, left, right)
    recipe = build_recipe(
        base, left, right, resolutions=[take_left(result.conflicts[0])]
    )

    merged, replayed = re_merge(recipe, base, left, right)
    assert replayed.clean
    assert _core(merged).vertex_config.vertices[0].identity == ["ssn"]


def test_re_merge_surfaces_only_genuinely_new_conflicts() -> None:
    base = _person(["id", "ssn", "email", "phone"], ["id"])
    left = _person(["id", "ssn", "email", "phone"], ["ssn"])
    right = _person(["id", "ssn", "email", "phone"], ["email"])
    _none, result = merge_three_way(base, left, right)
    recipe = build_recipe(
        base, left, right, resolutions=[take_left(result.conflicts[0])]
    )

    # The left side advances: same identity decision, plus a new field.
    advanced_left = _person(["id", "ssn", "email", "phone", "nickname"], ["ssn"])
    merged, replayed = re_merge(recipe, base, advanced_left, right)
    assert replayed.clean, replayed.conflicts
    names = {f.name for f in _core(merged).vertex_config.vertices[0].properties}
    assert "nickname" in names


def test_a_recorded_resolution_that_is_no_longer_needed_is_reported_not_forced() -> (
    None
):
    """A stale decision reapplied to an uncontested slot silently reverts work."""
    base = _person(["id", "ssn", "email"], ["id"])
    left = _person(["id", "ssn", "email"], ["ssn"])
    right = _person(["id", "ssn", "email"], ["email"])
    _none, result = merge_three_way(base, left, right)
    recipe = build_recipe(
        base, left, right, resolutions=[take_left(result.conflicts[0])]
    )

    # Both sides now agree on the identity, so nothing is contested.
    agreed = _person(["id", "ssn", "email"], ["ssn"])
    merged, replayed = re_merge(recipe, base, agreed, agreed)
    assert replayed.clean
    assert any("not needed this time" in w for w in replayed.warnings)
    assert _core(merged).vertex_config.vertices[0].identity == ["ssn"]


def test_a_recipe_round_trips_through_serialization() -> None:
    base = _person(["id", "ssn", "email"], ["id"])
    left = _person(["id", "ssn", "email"], ["ssn"])
    right = _person(["id", "ssn", "email"], ["email"])
    _none, result = merge_three_way(base, left, right)
    recipe = build_recipe(
        base, left, right, resolutions=[take_left(result.conflicts[0])]
    )
    restored = MergeRecipe.model_validate(recipe.to_dict())
    assert restored.content_hash() == recipe.content_hash()


def test_a_resolution_takes_the_place_of_the_ops_it_replaces() -> None:
    """Op order is a precondition, not a presentation detail.

    `diff_manifests` emits identity changes before the secondary-identity adds
    that depend on them. A resolution appended at the *end* of the merged
    change set inverts that: the demoted key is added as a secondary identity
    while it is still the primary, and the apply fails on a duplicate that
    exists only because of the reordering.
    """

    def keyed(identity: list[str], secondary: list[dict] | None = None):
        vertex: dict = {
            "name": "person",
            "properties": [{"name": n} for n in ("id", "ssn", "email")],
            "identity": identity,
        }
        if secondary:
            vertex["secondary_identities"] = secondary
        return _manifest([vertex])

    demoted = [{"name": "secondary_1", "fields": ["id"]}]
    base = keyed(["id"])
    left = keyed(["ssn"], demoted)
    right = keyed(["email"], demoted)

    _none, result = merge_three_way(base, left, right)
    merged, resolved = merge_three_way(
        base, left, right, resolutions=[take_left(result.conflicts[0])]
    )
    assert resolved.clean, resolved.conflicts
    assert merged is not None

    kinds = [op.op for op in resolved.ops]
    assert kinds.index("replace_identity") < kinds.index("add_secondary_identities")

    vertex = _core(merged).vertex_config.vertices[0]
    assert vertex.identity == ["ssn"]
    assert [s.fields for s in vertex.secondary_identities] == [["id"]]
