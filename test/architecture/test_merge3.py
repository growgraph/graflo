"""Three-way merge: slots, auto-merge, conflicts, determinism, re-merge.

The load-bearing property is the slot map. If it is too coarse, unrelated edits
collide and merging is useless; if it is too fine, colliding edits merge and the
result is a schema neither author wrote. The second failure is silent, so most
of what is asserted here is that things which *should* conflict do.
"""

from __future__ import annotations

import inspect
import typing

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution import footprint, merge3
from graflo.architecture.evolution import ops as ops_module
from graflo.architecture.evolution.commit import build_commit
from graflo.architecture.evolution.footprint import (
    describe_slot,
    op_reads,
    op_slots,
    ops_independent,
)
from graflo.architecture.evolution.history import History
from graflo.architecture.evolution.merge3 import (
    ConflictResolution,
    MergeError,
    MergeRecipe,
    build_recipe,
    find_merge_base,
    keep_both,
    merge_three_way,
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


def test_every_op_in_the_vocabulary_has_an_explicit_footprint() -> None:
    """Both footprint tables cover the op union, exactly.

    An op with no entry is refused rather than given a footprint by guess, so
    a missing entry makes that op unmergeable -- quietly, and only for whoever
    happens to use it. Comparing each table with the union is what makes adding
    an op without a footprint a failing test rather than a latent one; an entry
    for a model outside the union is one the dispatch never reaches.
    """
    union = set(typing.get_args(typing.get_args(ManifestOp)[0]))
    write_advice = (
        "Map each to the location it writes, or to the whole manifest if it "
        "genuinely rewrites everything."
    )
    read_advice = (
        "Map each to what it reads, or register it with `_reads_nothing` if "
        "it reads nothing beyond what it writes."
    )
    for table, advice in (
        (footprint.WRITES, write_advice),
        (footprint.READS, read_advice),
    ):
        missing = sorted(model.__name__ for model in union - set(table))
        extra = sorted(model.__name__ for model in set(table) - union)
        assert not missing, f"these ops have no footprint: {missing}. {advice}"
        assert not extra, f"these footprints are for no op in the union: {extra}"


def test_an_op_with_no_footprint_is_refused() -> None:
    """There is no catch-all: an unmapped op raises rather than merging."""
    stranger = typing.cast(ManifestOp, object())

    with pytest.raises(TypeError, match="no footprint for op object"):
        op_slots(stranger)
    with pytest.raises(TypeError, match="no footprint for op object"):
        op_reads(stranger)


def test_the_whole_manifest_ops_are_the_only_ones_that_conflict_with_everything() -> (
    None
):
    """`("manifest",)` is a deliberate answer for a few ops, not a default.

    Two ops rewrite everything, and realizing every declared inverse reaches
    whatever is paired when it runs. No other write footprint names it.
    """
    assert op_slots(ops_module.ProjectManifestOp(keep_vertices=["person"])) == {
        ("manifest",)
    }
    assert op_slots(ops_module.MergeManifestsOp()) == {("manifest",)}
    whole = {
        model
        for model, write in footprint.WRITES.items()
        if "WHOLE_MANIFEST" in inspect.getsource(write)
    }
    assert whole == {
        ops_module.ProjectManifestOp,
        ops_module.MergeManifestsOp,
        ops_module.AddInverseEdgesOp,
    }


def test_a_whole_manifest_op_is_independent_of_nothing() -> None:
    """Keeping only ``person`` and adding a field to ``company``: one order refuses."""
    base = _people_and_companies([KNOWS])
    project = ops_module.ProjectManifestOp(keep_vertices=["person"])

    assert not ops_independent(
        project, AddVertexPropertiesOp(additions={"company": ["name"]}), base
    )
    assert not ops_independent(project, ops_module.AddEdgesOp(edges=[SELF_KNOWS]), base)


def _with_inverse(edges: list[dict]) -> GraphManifest:
    manifest = _people_and_companies(edges)
    _core(manifest).edge_config.inverses = [
        {"relation": "knows", "inverse": "known_by"}  # type: ignore[list-item]
    ]
    return GraphManifest.from_dict(manifest.to_dict())


def test_realizing_every_declared_inverse_depends_on_the_whole_manifest() -> None:
    """With no relations named, its reach is whatever is paired when it runs."""
    realize_all = ops_module.AddInverseEdgesOp()

    assert op_slots(realize_all) == {("manifest",)}
    assert not ops_independent(
        realize_all,
        ops_module.AddEdgesOp(edges=[SELF_KNOWS]),
        _with_inverse([KNOWS]),
    )


def test_realizing_an_inverse_depends_on_the_inverse_relation_s_edges() -> None:
    """The op adds ``known_by`` edges; one added by the other side collides."""
    realize = ops_module.AddInverseEdgesOp(relations=["knows"])
    known_by = {"source": "company", "target": "person", "relation": "known_by"}
    base = _with_inverse([KNOWS])

    assert (*footprint.relation_slot("known_by"), "edges") in op_reads(realize, base)
    assert not ops_independent(realize, ops_module.AddEdgesOp(edges=[known_by]), base)


def test_sanitize_touches_only_the_profile() -> None:
    """Sanitizing records physical names; it no longer rewrites the logical model."""
    assert op_slots(ops_module.SanitizeOp(db_flavor="arango")) == {("db_profile",)}


def test_a_vertex_slot_is_convention_independent() -> None:
    """Slots key on the canonical name.

    Without this, one side's `order_line` and the other's `OrderLine` occupy
    different slots, merge cleanly, and produce a schema holding both as
    unrelated types with the data split between them -- the exact defect, now
    reachable through a tracked re-merge rather than only through merge.
    """
    snake = AddVertexPropertiesOp(additions={"order_line": ["qty"]})
    pascal = AddVertexPropertiesOp(additions={"OrderLine": ["qty"]})
    plural = AddVertexPropertiesOp(additions={"OrderLines": ["qty"]})
    assert op_slots(snake) == op_slots(pascal) == op_slots(plural)


def test_a_rename_occupies_both_of_its_names() -> None:
    rename = RenameVerticesOp(renames={"person": "customer"})
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


def test_a_rename_hinted_for_its_own_side_conflicts_as_a_rename() -> None:
    """A hint is emitted as a rename, so it belongs to the side that renamed."""
    from graflo.architecture.evolution.autogenerate import RenameHints

    base = _person(["id"])
    renamed = _manifest([_vertex("customer", ["id"], ["id"])])
    edited = _person(["id", "age"])

    merged, result = merge_three_way(
        base,
        renamed,
        edited,
        left_hints=RenameHints(vertices={"person": "customer"}),
    )
    assert merged is None
    (conflict,) = result.conflicts
    assert [op.op for op in conflict.left_ops] == ["rename_vertices"]
    assert [op.op for op in conflict.right_ops] == ["add_vertex_properties"]


def test_hints_from_ops_chain_renames_and_drop_names_the_base_lacks() -> None:
    from graflo.architecture.evolution.autogenerate import RenameHints

    hints = RenameHints.from_ops(
        [
            RenameVerticesOp(renames={"person": "customer"}),
            ops_module.RenameVertexPropertiesOp(renames={"customer": {"id": "key"}}),
            RenameVerticesOp(renames={"customer": "client", "ghost": "phantom"}),
            AddVertexPropertiesOp(additions={"client": ["age"]}),
        ],
        base=_person(["id"]),
    )
    assert hints.vertices == {"person": "client"}
    assert hints.vertex_properties == {"client": {"id": "key"}}
    assert hints.relations == {} and hints.resources == {}
    assert [op.op for op in hints.to_ops()] == [
        "rename_vertices",
        "rename_vertex_properties",
    ]


KNOWS = {"source": "person", "target": "company", "relation": "knows"}


def _people_and_companies(edges: list[dict]) -> GraphManifest:
    return _manifest(
        [_vertex("person", ["id"], ["id"]), _vertex("company", ["id"], ["id"])],
        edges,
    )


def test_an_edge_slot_nests_under_its_relation() -> None:
    """Relation-addressed and triple-addressed ops must see each other."""
    relation = footprint.relation_slot("knows")
    edge = op_slots(ops_module.SetEdgeDirectedOp(edges=[KNOWS], directed=False))
    assert edge == {(*relation, "edge", "person", "company", "directed")}
    assert op_slots(ops_module.RemoveEdgesOp(relations=["knows"])) == {relation}
    assert op_slots(ops_module.RemoveEdgesOp(edges=[KNOWS])) == {
        (*relation, "edge", "person", "company")
    }


def test_an_edge_slot_is_convention_independent() -> None:
    """The relation segment folds spelling the way the vertex slot does."""
    lower = ops_module.SetEdgeDirectedOp(edges=[KNOWS], directed=False)
    pascal = ops_module.SetEdgeDirectedOp(
        edges=[{**KNOWS, "relation": "Knows"}], directed=False
    )
    assert op_slots(lower) == op_slots(pascal)


def test_an_edge_with_no_relation_keeps_its_own_root() -> None:
    bare = {"source": "person", "target": "company"}
    assert op_slots(ops_module.SetEdgeDirectedOp(edges=[bare], directed=False)) == {
        ("edge", "person", "company", "directed")
    }


def test_removing_a_relation_conflicts_with_flipping_an_edge_on_it() -> None:
    """The blind spot: two disjoint slot trees merged this cleanly, then applied
    the flip to an edge the removal had already deleted."""
    base = _people_and_companies([KNOWS])
    removed = _people_and_companies([])
    flipped = _people_and_companies([{**KNOWS, "directed": False}])

    merged, result = merge_three_way(base, removed, flipped)

    assert merged is None
    assert [tuple(c.slot) for c in result.conflicts] == [
        footprint.relation_slot("knows")
    ]
    assert result.conflicts[0].base_excerpt["edges"][0]["relation"] == "knows"


def test_a_relation_wide_property_add_merges_with_an_edge_flip() -> None:
    """Independent edits under one relation stay independent."""
    base = _people_and_companies([KNOWS])
    widened = _people_and_companies([{**KNOWS, "properties": ["since"]}])
    flipped = _people_and_companies([{**KNOWS, "directed": False}])

    merged, result = merge_three_way(base, widened, flipped)

    assert merged is not None and not result.conflicts
    edge = _core(merged).edge_config.edges[0]
    assert [f.name for f in edge.properties] == ["since"]
    assert edge.directed is False


SELF_KNOWS = {"source": "company", "target": "company", "relation": "knows"}


@pytest.mark.parametrize("adder", ["left", "right"])
def test_a_relation_wide_property_add_conflicts_with_an_edge_added_to_the_relation(
    adder: str,
) -> None:
    """A relation-wide op reaches whatever edges the relation has when it runs.

    Applied after the new edge, the property lands on it too; applied before,
    it does not. Neither order is the merge, so the two sides conflict, on the
    edge the other side added.
    """
    base = _people_and_companies([KNOWS])
    widened = _people_and_companies([{**KNOWS, "properties": ["weight"]}])
    grown = _people_and_companies([KNOWS, SELF_KNOWS])
    left, right = (grown, widened) if adder == "left" else (widened, grown)

    merged, result = merge_three_way(base, left, right)

    assert merged is None
    (conflict,) = result.conflicts
    assert tuple(conflict.slot) == footprint.edge_slot("company", "company", "knows")
    ops_by_side = {
        "left": [op.op for op in conflict.left_ops],
        "right": [op.op for op in conflict.right_ops],
    }
    other = "right" if adder == "left" else "left"
    assert ops_by_side[adder] == ["add_edges"]
    assert ops_by_side[other] == ["add_edge_properties"]


def test_a_relation_wide_edge_op_depends_on_the_relation_s_edge_set() -> None:
    widen = ops_module.AddEdgePropertiesOp(additions={"knows": ["weight"]})
    grow = ops_module.AddEdgesOp(edges=[SELF_KNOWS])
    base = _people_and_companies([KNOWS])

    assert (*footprint.relation_slot("knows"), "edges") in op_reads(widen, base)
    assert not ops_independent(widen, grow, base)


def test_a_per_edge_property_add_merges_with_an_edge_added_to_the_relation() -> None:
    """A per-edge op names its edges, so a new sibling is out of its reach."""
    per_edge = ops_module.AddEdgePropertiesOp(
        per_edge=[{**KNOWS, "properties": ["weight"]}]
    )
    grow = ops_module.AddEdgesOp(edges=[SELF_KNOWS])

    assert ops_independent(per_edge, grow, _people_and_companies([KNOWS]))


def test_two_sides_adding_different_edges_to_one_relation_merge() -> None:
    """Growing a relation on both sides is not a conflict; only reach is."""
    base = _people_and_companies([KNOWS])
    person_knows = {"source": "person", "target": "person", "relation": "knows"}

    merged, result = merge_three_way(
        base,
        _people_and_companies([KNOWS, person_knows]),
        _people_and_companies([KNOWS, SELF_KNOWS]),
    )

    assert merged is not None and not result.conflicts
    assert len(_core(merged).edge_config.edges) == 3


@pytest.mark.parametrize("remover", ["left", "right"])
def test_removing_a_vertex_conflicts_with_adding_an_edge_onto_it(remover: str) -> None:
    """The same blind spot one level up: a vertex removal cascades over its edges.

    With the removal on the right the merge is clean and the edge the left added
    is gone; with it on the left the merged ops do not apply at all. Either way
    the two sides disagree about whether ``company`` exists, which is a conflict.
    """
    base = _people_and_companies([])
    removed = _manifest([_vertex("person", ["id"], ["id"])])
    linked = _people_and_companies([KNOWS])
    left, right = (removed, linked) if remover == "left" else (linked, removed)

    merged, result = merge_three_way(base, left, right)

    assert merged is None
    assert [tuple(c.slot) for c in result.conflicts] == [
        footprint.vertex_slot("company")
    ]


def test_the_dependent_op_is_reported_with_the_conflict() -> None:
    """Both halves of the disagreement are shown, not only the op that wrote."""
    base = _people_and_companies([])
    removed = _manifest([_vertex("person", ["id"], ["id"])])
    linked = _people_and_companies([KNOWS])

    _, result = merge_three_way(base, linked, removed)

    (conflict,) = result.conflicts
    assert [op.op for op in conflict.left_ops] == ["add_edges"]
    assert [op.op for op in conflict.right_ops] == ["remove_vertices"]
    assert "depends on it" in conflict.reason


def test_a_dependency_conflict_resolves_like_any_other() -> None:
    base = _people_and_companies([])
    removed = _manifest([_vertex("person", ["id"], ["id"])])
    linked = _people_and_companies([KNOWS])
    _, result = merge_three_way(base, linked, removed)

    kept, _ = merge_three_way(
        base, linked, removed, resolutions=[take_left(result.conflicts[0])]
    )
    dropped, _ = merge_three_way(
        base, linked, removed, resolutions=[take_right(result.conflicts[0])]
    )

    assert kept is not None and dropped is not None
    assert [e.edge_id for e in _core(kept).edge_config.edges] == [
        ("person", "company", "knows")
    ]
    assert _core(dropped).vertex_config.vertex_set == {"person"}


def test_flipping_an_edge_conflicts_with_removing_its_endpoint() -> None:
    base = _people_and_companies([KNOWS])
    flipped = _people_and_companies([{**KNOWS, "directed": False}])
    removed = _manifest([_vertex("person", ["id"], ["id"])])

    merged, result = merge_three_way(base, flipped, removed)

    assert merged is None
    assert footprint.vertex_slot("company") in {tuple(c.slot) for c in result.conflicts}


def test_a_relation_wide_edit_conflicts_with_removing_a_vertex_it_connects() -> None:
    """The op names the relation only; what it connects is read from the base."""
    base = _people_and_companies([KNOWS])
    widened = _people_and_companies([{**KNOWS, "properties": ["since"]}])
    removed = _manifest([_vertex("person", ["id"], ["id"])])

    merged, result = merge_three_way(base, widened, removed)

    assert merged is None
    assert footprint.vertex_slot("company") in {tuple(c.slot) for c in result.conflicts}


def test_re_keying_onto_a_field_conflicts_with_removing_it() -> None:
    base = _person(["id", "email"])
    rekeyed = _person(["id", "email"], ["email"])
    trimmed = _person(["id"])

    merged, result = merge_three_way(base, rekeyed, trimmed)

    assert merged is None
    assert footprint.field_slot("person", "email") in {
        tuple(c.slot) for c in result.conflicts
    }


def test_an_edge_onto_a_vertex_merges_with_a_new_field_on_it() -> None:
    """A read is disturbed at or above it, not beneath: fields are beneath."""
    base = _people_and_companies([])
    linked = _people_and_companies([KNOWS])
    widened = _manifest(
        [_vertex("person", ["id"], ["id"]), _vertex("company", ["id", "name"], ["id"])]
    )

    merged, result = merge_three_way(base, linked, widened)

    assert merged is not None and not result.conflicts
    assert [e.edge_id for e in _core(merged).edge_config.edges] == [
        ("person", "company", "knows")
    ]


def test_two_edges_onto_one_vertex_merge() -> None:
    """Both read ``company``; neither writes it. Reads do not collide."""
    base = _people_and_companies([])
    owns = {"source": "person", "target": "company", "relation": "owns"}

    merged, result = merge_three_way(
        base, _people_and_companies([KNOWS]), _people_and_companies([owns])
    )

    assert merged is not None and not result.conflicts
    assert len(_core(merged).edge_config.edges) == 2


def test_both_sides_removing_a_vertex_and_its_edge_is_agreement() -> None:
    """An op both sides made is on the reader's own side too: not a dependency."""
    base = _people_and_companies([KNOWS])
    removed = _manifest([_vertex("person", ["id"], ["id"])])

    merged, result = merge_three_way(base, removed, removed)

    assert merged is not None and not result.conflicts
    assert _core(merged).vertex_config.vertex_set == {"person"}


def test_independence_is_about_writes_and_reads() -> None:
    add_edge = ops_module.AddEdgesOp(edges=[KNOWS])
    remove_vertex = ops_module.RemoveVerticesOp(names=["company"])
    add_field = AddVertexPropertiesOp(additions={"company": ["name"]})

    assert op_reads(add_edge) == {("vertex", "person"), ("vertex", "company")}
    assert not ops_independent(add_edge, remove_vertex)
    assert ops_independent(add_edge, add_field)


def test_a_gain_on_one_sibling_edge_survives_a_merge() -> None:
    """One sibling edge gains a field: a per-edge addition carries it."""
    to_city = {"source": "person", "target": "city", "relation": "knows"}
    vertices = [_vertex("person", ["id"], ["id"]), _vertex("city", ["id"], ["id"])]
    loop = {"source": "person", "target": "person", "relation": "knows"}
    base = _manifest(vertices, [loop, to_city])
    gained = _manifest(vertices, [{**loop, "properties": ["since"]}, to_city])
    unrelated = _manifest(
        [*vertices, _vertex("order", ["id"], ["id"])], [loop, to_city]
    )

    merged, result = merge_three_way(base, gained, unrelated)

    assert merged is not None and not result.conflicts
    edges = {
        edge.edge_id: [f.name for f in edge.properties]
        for edge in _core(merged).edge_config.edges
    }
    assert edges == {
        ("person", "person", "knows"): ["since"],
        ("person", "city", "knows"): [],
    }
    assert "order" in _core(merged).vertex_config.vertex_set


def test_a_change_no_op_expresses_refuses_the_merge() -> None:
    """Dropping the ingestion block: no op says that, so merging would lose it."""
    base = GraphManifest.from_dict(
        {
            **_person(["id"]).to_dict(),
            "ingestion_model": {
                "resources": [{"name": "people", "pipeline": [{"vertex": "person"}]}]
            },
        }
    )
    dropped = _person(["id"])
    unrelated = GraphManifest.from_dict(
        {
            **_person(["id", "age"]).to_dict(),
            "ingestion_model": base.to_dict()["ingestion_model"],
        }
    )

    try:
        merge_three_way(base, dropped, unrelated)
    except MergeError as refusal:
        assert "no operation expresses" in str(refusal)
    else:
        raise AssertionError("a merge that drops a side's change must not be clean")


def test_a_grounding_only_change_survives_a_merge() -> None:
    """The differ never emitted the semantics ops, so a merge dropped them."""
    base = _person(["id"])
    grounded = _manifest(
        [{**_vertex("person", ["id"], ["id"]), "semantics": {"iri": "x:Person"}}]
    )
    widened = _person(["id", "age"])

    merged, result = merge_three_way(base, grounded, widened)

    assert merged is not None and not result.conflicts
    person = _core(merged).vertex_config["person"]
    assert person.semantics is not None and person.semantics.iri == "x:Person"
    assert [f.name for f in person.properties] == ["id", "age"]


def test_retargeting_onto_an_edge_the_other_side_adds_is_not_independent() -> None:
    """The retarget creates the edge it moves to; adding that edge too collides."""
    person_knows = {"source": "person", "target": "person", "relation": "knows"}
    retarget = ops_module.RetargetEdgesOp(edges=[{**KNOWS, "new_target": "person"}])

    assert not ops_independent(
        retarget,
        ops_module.AddEdgesOp(edges=[person_knows]),
        _people_and_companies([KNOWS]),
    )


def test_slot_values_for_the_edge_addressed_ops() -> None:
    """29 of 35 ops had only the source-text meta-check; pin the edge family."""
    relation = footprint.relation_slot("knows")
    edge = (*relation, "edge", "person", "company")
    assert op_slots(ops_module.AddEdgesOp(edges=[KNOWS])) == {edge}
    assert op_slots(
        ops_module.RetargetEdgesOp(edges=[{**KNOWS, "new_target": "person"}])
    ) == {edge, (*relation, "edge", "person", "person")}
    assert op_slots(
        ops_module.ReplaceEdgeIdentitiesOp(edges=[{**KNOWS, "identities": [["x"]]}])
    ) == {edge}
    assert op_slots(
        ops_module.AddEdgeIndexesOp(edges=[{**KNOWS, "indexes": [{"fields": ["x"]}]}])
    ) == {edge}
    assert op_slots(
        ops_module.RemoveEdgeIndexesOp(edges=[{**KNOWS, "fields": [["x"]]}])
    ) == {edge}
    assert op_slots(
        ops_module.SetEdgeSemanticsOp(edges=[KNOWS], semantics={"iri": "x:y"})
    ) == {(*edge, "semantics")}
    assert op_slots(
        ops_module.ChangeFieldTypesOp(
            vertices={"person": {"age": {"type": "INT"}}},
            edges={"knows": {"since": {"type": "INT"}}},
        )
    ) == {
        ("vertex", "person", "field", "age", "type"),
        (*relation, "field", "since", "type"),
    }
    assert op_slots(
        ops_module.SetFieldSemanticsOp(
            targets=[{"vertex": "person", "field": "age", "semantics": {"iri": "x:y"}}]
        )
    ) == {("vertex", "person", "field", "age", "semantics")}
    assert op_slots(ops_module.RenameResourcesOp(renames={"a": "b"})) == {
        ("resource", "a"),
        ("resource", "b"),
    }


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
    """Which is the signal that the operation you want is a union, not a three-way merge."""
    history, (_root, left, _right) = _linear_history()
    stranger = build_commit(
        _person(["id"]), [AddVertexPropertiesOp(additions={"person": ["x"]})]
    )
    combined = History(commits=[*history.commits, stranger])
    assert find_merge_base(combined, left.id, stranger.id) is None


def test_the_ops_between_two_commits_follow_first_parents() -> None:
    history, (root, left, right) = _linear_history()
    assert history.ops_between(root.id, left.id) == list(left.ops)
    assert history.ops_between(left.id, left.id) == []
    assert history.ops_between(left.id, right.id) is None


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


def test_a_replayed_resolution_is_reported_as_replayed_not_unused() -> None:
    """The warning has to describe what happened, not the opposite.

    A recorded resolution that *did its job* leaves no conflict behind. Judging
    "was it needed" by what survives the replay therefore reports every working
    resolution as unnecessary — and a maintainer acting on that reading deletes
    the decision that was holding the merge together.
    """
    base = _person(["id", "ssn", "email"], ["id"])
    left = _person(["id", "ssn", "email"], ["ssn"])
    right = _person(["id", "ssn", "email"], ["email"])

    _none, result = merge_three_way(base, left, right)
    recipe = build_recipe(
        base, left, right, resolutions=[take_left(result.conflicts[0])]
    )

    merged, replayed = re_merge(recipe, base, left, right)
    assert replayed.clean
    assert merged is not None
    assert any("replayed" in warning for warning in replayed.warnings)
    assert not any("not needed" in warning for warning in replayed.warnings)


def test_two_branches_retyping_one_field_conflict() -> None:
    """A field's type is its own slot, so two branches retyping it must collide.

    A regression guard rather than a fix: ``op_slots`` already narrows
    ``ChangeFieldTypesOp`` to ``(vertex, field, "type")``. It is asserted here
    because the field-merge policy now refuses this disagreement everywhere
    else, and the three-way plane must keep refusing it too.
    """
    base = _manifest([_vertex("person", ["id", "age"], ["id"])])

    def _aged(field_type: str) -> GraphManifest:
        return _manifest(
            [
                {
                    "name": "person",
                    "properties": ["id", {"name": "age", "type": field_type}],
                    "identity": ["id"],
                }
            ]
        )

    merged, result = merge_three_way(base, _aged("INT"), _aged("STRING"))
    assert merged is None
    assert len(result.conflicts) == 1
    assert result.conflicts[0].slot_key[-1] == "type"


# ── pipelines read what they name ───────────────────────────────────────────


def _with_resources(
    manifest: GraphManifest, resources: dict[str, list]
) -> GraphManifest:
    payload = manifest.to_dict(skip_defaults=False)
    payload["ingestion_model"] = {
        "resources": [
            {"name": name, "pipeline": pipeline} for name, pipeline in resources.items()
        ]
    }
    out = GraphManifest.from_config(payload)
    out.finish_init()
    return out


def test_a_resource_reads_the_vertices_and_relations_its_pipeline_names() -> None:
    op = ops_module.AddResourcesOp(
        resources=[
            {
                "name": "links",
                "pipeline": [
                    {"vertex": "person"},
                    {"vertex": "company"},
                    {"edge": {"from": "person", "to": "company", "relation": "knows"}},
                ],
            }
        ]
    )
    replace = ops_module.ReplaceResourcesOp(resources=op.resources)

    expected = {
        footprint.vertex_slot("person"),
        footprint.vertex_slot("company"),
        footprint.relation_slot("knows"),
    }
    assert op_reads(op) == expected
    assert op_reads(replace) == expected
    assert op_slots(replace) == {footprint.resource_slot("links")}


def test_adding_a_resource_conflicts_with_removing_a_vertex_it_casts() -> None:
    base = _with_resources(
        _people_and_companies([KNOWS]), {"people": [{"vertex": "person"}]}
    )
    added = _with_resources(
        _people_and_companies([KNOWS]),
        {"people": [{"vertex": "person"}], "companies": [{"vertex": "company"}]},
    )
    removed = _with_resources(
        _manifest([_vertex("person", ["id"], ["id"])]),
        {"people": [{"vertex": "person"}]},
    )

    merged, result = merge_three_way(base, added, removed)

    assert merged is None
    assert footprint.vertex_slot("company") in {tuple(c.slot) for c in result.conflicts}


def test_editing_a_pipeline_conflicts_with_removing_a_vertex_it_now_casts() -> None:
    base = _with_resources(
        _people_and_companies([KNOWS]),
        {"people": [{"vertex": "person"}], "companies": [{"vertex": "company"}]},
    )
    edited = _with_resources(
        _people_and_companies([KNOWS]),
        {
            "people": [{"vertex": "person"}, {"vertex": "company"}],
            "companies": [{"vertex": "company"}],
        },
    )
    removed = _with_resources(
        _manifest([_vertex("person", ["id"], ["id"])]),
        {"people": [{"vertex": "person"}]},
    )

    merged, result = merge_three_way(base, edited, removed)

    assert merged is None
    assert footprint.vertex_slot("company") in {tuple(c.slot) for c in result.conflicts}


def test_an_edit_to_a_relation_this_side_renamed_reads_its_endpoints() -> None:
    """The base knows the relation by its old name; the side's own rename says so."""
    base = _people_and_companies([{**KNOWS, "relation": "employs"}])
    left_ops: list[ManifestOp] = [
        ops_module.RenameRelationsOp(renames={"employs": "hires"}),
        ops_module.AddEdgePropertiesOp(additions={"hires": ["since"]}),
    ]
    right_ops: list[ManifestOp] = [
        ops_module.RenameVerticesOp(renames={"company": "firm"})
    ]

    found = merge3._dependencies(left_ops, right_ops, base)

    assert len(found) == 2


def test_a_resource_reads_the_fields_it_maps() -> None:
    op = ops_module.AddResourcesOp(
        resources=[
            {
                "name": "people",
                "pipeline": [{"vertex": "person", "from": {"email": "mail"}}],
            }
        ]
    )

    assert footprint.field_slot("person", "email") in op_reads(op)


def test_adding_a_resource_conflicts_with_removing_a_field_it_maps() -> None:
    base = _with_resources(_person(["id", "email"]), {"people": [{"vertex": "person"}]})
    added = _with_resources(
        _person(["id", "email"]),
        {
            "people": [{"vertex": "person"}],
            "mail": [{"vertex": "person", "from": {"id": "id", "email": "mail"}}],
        },
    )
    trimmed = _with_resources(_person(["id"]), {"people": [{"vertex": "person"}]})

    merged, result = merge_three_way(base, added, trimmed)

    assert merged is None
    assert footprint.field_slot("person", "email") in {
        tuple(c.slot) for c in result.conflicts
    }


# ── keeping both sides ──────────────────────────────────────────────────────


def _customer(properties: list[str]) -> GraphManifest:
    return _manifest([_vertex("Customer", properties, ["id"])])


def _account(properties: list[str]) -> GraphManifest:
    return _manifest([_vertex("Account", properties, ["id"])])


def _rename_against_edit(rename_on: str):
    """A rename of ``Customer`` on one side, a new ``tier`` on it on the other."""
    from graflo.architecture.evolution.autogenerate import RenameHints

    base = _customer(["id"])
    renamed, edited = _account(["id"]), _customer(["id", "tier"])
    hint = RenameHints(vertices={"Customer": "Account"})
    if rename_on == "left":
        return base, renamed, edited, {"left_hints": hint}
    return base, edited, renamed, {"right_hints": hint}


@pytest.mark.parametrize("rename_on", ["left", "right"])
def test_keeping_both_carries_the_edit_through_the_rename(rename_on: str) -> None:
    base, left, right, hints = _rename_against_edit(rename_on)
    _none, result = merge_three_way(base, left, right, **hints)
    (conflict,) = result.conflicts

    resolution = keep_both(base, conflict)
    assert resolution is not None
    assert resolution.slot == conflict.slot
    assert [op.op for op in resolution.ops] == [
        "add_vertex_properties",
        "rename_vertices",
    ]
    assert resolution.rationale == "kept both sides"

    merged, resolved = merge_three_way(
        base, left, right, resolutions=[resolution], **hints
    )
    assert resolved.clean
    assert merged is not None
    (vertex,) = _core(merged).vertex_config.vertices
    assert vertex.name == "Account"
    assert {f.name for f in vertex.properties} == {"id", "tier"}


def test_keeping_both_is_the_same_merge_whichever_side_renamed() -> None:
    merged_hashes = set()
    for rename_on in ("left", "right"):
        base, left, right, hints = _rename_against_edit(rename_on)
        _none, result = merge_three_way(base, left, right, **hints)
        resolution = keep_both(base, result.conflicts[0], rationale="v2 + v3")
        assert resolution is not None and resolution.rationale == "v2 + v3"
        _merged, resolved = merge_three_way(
            base, left, right, resolutions=[resolution], **hints
        )
        merged_hashes.add(resolved.merged_hash)
    assert len(merged_hashes) == 1


def test_keeping_both_refuses_when_both_sides_rename() -> None:
    from graflo.architecture.evolution.autogenerate import RenameHints

    base = _customer(["id"])
    left, right = _account(["id"]), _manifest([_vertex("Client", ["id"], ["id"])])
    _none, result = merge_three_way(
        base,
        left,
        right,
        left_hints=RenameHints(vertices={"Customer": "Account"}),
        right_hints=RenameHints(vertices={"Customer": "Client"}),
    )
    assert result.conflicts
    assert all(keep_both(base, conflict) is None for conflict in result.conflicts)


def test_re_merge_replays_a_kept_both_resolution() -> None:
    base, left, right, hints = _rename_against_edit("left")
    _none, result = merge_three_way(base, left, right, **hints)
    resolution = keep_both(base, result.conflicts[0])
    assert resolution is not None
    recipe = build_recipe(base, left, right, resolutions=[resolution])

    merged, replayed = re_merge(recipe, base, left, right, **hints)
    assert replayed.clean
    assert merged is not None
    assert _core(merged).vertex_config.vertex_set == {"Account"}
    assert any("1 recorded resolution(s) replayed" in w for w in replayed.warnings)


def test_keeping_both_refuses_ops_that_do_not_apply_together() -> None:
    """One side adds ``mail``; the other renames ``email`` onto that name."""
    from graflo.architecture.evolution.autogenerate import RenameHints

    base = _person(["id", "email"])
    renamed, added = _person(["id", "mail"]), _person(["id", "email", "mail"])
    _none, result = merge_three_way(
        base,
        renamed,
        added,
        left_hints=RenameHints(vertex_properties={"person": {"email": "mail"}}),
    )
    (conflict,) = result.conflicts
    assert keep_both(base, conflict) is None


def test_keeping_both_refuses_when_neither_side_renames() -> None:
    """Without a rename, applying both lists in sequence would let one side win."""
    base = _people_and_companies([])
    removed = _manifest([_vertex("person", ["id"], ["id"])])
    linked = _people_and_companies([KNOWS])
    _none, result = merge_three_way(base, removed, linked)
    (conflict,) = result.conflicts
    assert keep_both(base, conflict) is None


# ── a decision reaches its own slot only ────────────────────────────────────
#
# The differ emits one op per kind for the whole manifest -- every property any
# vertex gains sits in one ``add_vertex_properties``. A conflict must still hold
# only the part of that op at its slot, or deciding it decides unrelated edits too.


def _typed(name: str, fields: dict[str, str], identity: list[str]) -> dict:
    return {
        "name": name,
        "properties": [
            {"name": field, **({"type": kind} if kind else {})}
            for field, kind in fields.items()
        ],
        "identity": identity,
    }


def test_a_conflict_holds_only_the_ops_at_its_slot() -> None:
    from graflo.architecture.evolution.autogenerate import RenameHints

    base = _manifest(
        [_vertex("Customer", ["id"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    renamed = _manifest(
        [_vertex("Account", ["id"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    edited = _manifest(
        [
            _vertex("Customer", ["id", "tier"], ["id"]),
            _vertex("company", ["id", "name"], ["id"]),
        ]
    )
    hint = RenameHints(vertices={"Customer": "Account"})
    _none, result = merge_three_way(base, renamed, edited, left_hints=hint)

    (conflict,) = result.conflicts
    (edit,) = conflict.right_ops
    assert isinstance(edit, AddVertexPropertiesOp)
    assert list(edit.additions) == ["Customer"]


def test_taking_a_side_keeps_the_other_sides_unrelated_edits() -> None:
    from graflo.architecture.evolution.autogenerate import RenameHints

    base = _manifest(
        [_vertex("Customer", ["id"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    renamed = _manifest(
        [_vertex("Account", ["id"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    edited = _manifest(
        [
            _vertex("Customer", ["id", "tier"], ["id"]),
            _vertex("company", ["id", "name"], ["id"]),
        ]
    )
    hint = RenameHints(vertices={"Customer": "Account"})
    _none, result = merge_three_way(base, renamed, edited, left_hints=hint)

    merged, resolved = merge_three_way(
        base,
        renamed,
        edited,
        resolutions=[take_left(result.conflicts[0])],
        left_hints=hint,
    )
    assert resolved.clean
    assert merged is not None
    fields = {
        v.name: {f.name for f in v.properties}
        for v in _core(merged).vertex_config.vertices
    }
    assert fields == {"Account": {"id"}, "company": {"id", "name"}}


def test_different_choices_on_conflicts_from_one_op_merge() -> None:
    base = _manifest(
        [_vertex("person", ["id"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    left = _manifest(
        [
            _typed("person", {"id": "", "age": "INT"}, ["id"]),
            _typed("company", {"id": "", "name": "STRING"}, ["id"]),
        ]
    )
    right = _manifest(
        [
            _typed("person", {"id": "", "age": "STRING"}, ["id"]),
            _typed("company", {"id": "", "name": "INT"}, ["id"]),
        ]
    )
    _none, result = merge_three_way(base, left, right)
    by_vertex = {c.slot_key[1]: c for c in result.conflicts}
    assert set(by_vertex) == {"person", "company"}

    merged, resolved = merge_three_way(
        base,
        left,
        right,
        resolutions=[take_left(by_vertex["person"]), take_right(by_vertex["company"])],
    )
    assert resolved.clean
    assert merged is not None
    types = {
        (v.name, f.name): str(f.type)
        for v in _core(merged).vertex_config.vertices
        for f in v.properties
        if f.name != "id"
    }
    assert types == {("person", "age"): "INT", ("company", "name"): "INT"}


def test_agreement_survives_an_unrelated_edit_on_one_side() -> None:
    base = _manifest(
        [_vertex("person", ["id"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    left = _manifest(
        [
            _vertex("person", ["id", "age"], ["id"]),
            _vertex("company", ["id", "name"], ["id"]),
        ]
    )
    right = _manifest(
        [_vertex("person", ["id", "age"], ["id"]), _vertex("company", ["id"], ["id"])]
    )
    merged, result = merge_three_way(base, left, right)
    assert result.clean
    assert merged is not None
    fields = {
        v.name: [f.name for f in v.properties]
        for v in _core(merged).vertex_config.vertices
    }
    assert fields == {"person": ["id", "age"], "company": ["id", "name"]}


def test_a_written_resolution_replaces_both_sides() -> None:
    """The reviewer's own fix: neither side's type, two fields instead of one."""
    base = _manifest([_vertex("product", ["id"], ["id"])])
    left = _manifest([_typed("product", {"id": "", "category": "STRING"}, ["id"])])
    right = _manifest([_typed("product", {"id": "", "category": "INT"}, ["id"])])
    _none, result = merge_three_way(base, left, right)
    (conflict,) = result.conflicts

    written = ConflictResolution(
        slot=list(conflict.slot),
        ops=[
            AddVertexPropertiesOp(
                additions={
                    "product": [
                        {"name": "category", "type": "STRING"},
                        {"name": "category_id", "type": "INT"},
                    ]
                }
            )
        ],
        rationale="category names it, category_id keys it",
    )
    merged, resolved = merge_three_way(base, left, right, resolutions=[written])
    assert resolved.clean
    assert merged is not None
    (product,) = _core(merged).vertex_config.vertices
    assert {(f.name, str(f.type)) for f in product.properties if f.name != "id"} == {
        ("category", "STRING"),
        ("category_id", "INT"),
    }


def test_a_swap_is_not_split() -> None:
    """``{a: b, b: a}`` only means a swap as one op; split, it collides with itself."""
    swap = RenameVerticesOp(renames={"a": "b", "b": "a"})
    assert merge3.split_by_slot(swap) == [swap]


# ── cascades, splits and decisions ──────────────────────────────────────────


def _fed(
    resources: dict[str, list[str]], bindings: list[str] | None = None
) -> GraphManifest:
    """``person`` and ``company``, with one resource per entry producing its types."""
    payload: dict = {
        "schema": {
            "metadata": {"name": "t"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        _vertex("person", ["id"], ["id"]),
                        _vertex("company", ["id"], ["id"]),
                    ]
                },
                "edge_config": {"edges": []},
            },
        },
        "ingestion_model": {
            "resources": [
                {"name": name, "pipeline": [{"vertex": v} for v in types]}
                for name, types in resources.items()
            ]
        },
    }
    if bindings:
        payload["bindings"] = {
            "connectors": [
                {"name": f"{name}_files", "regex": f"{name}.*", "resource_name": name}
                for name in bindings
            ],
            "resource_connector": [
                {"resource": name, "connector": f"{name}_files"} for name in bindings
            ],
        }
    return GraphManifest.from_dict(payload)


def test_a_removal_writes_the_resources_its_cascade_trims_given_the_base() -> None:
    base = _fed({"rows": ["person"], "feed": ["person", "company"]}, ["feed"])
    remove = ops_module.RemoveVerticesOp(names=["company"])

    assert op_slots(remove) == {("vertex", "company")}
    assert op_slots(remove, base) == {
        ("vertex", "company"),
        ("resource", "feed"),
        ("bindings",),
    }


def test_two_removals_trimming_one_pipeline_conflict() -> None:
    """Together they would leave ``feed`` empty: a conflict, not a MergeError."""
    base = _fed({"rows": ["person"], "feed": ["person", "company"]})
    left = _applied(base, ops_module.RemoveVerticesOp(names=["company"]))
    right = _applied(base, ops_module.RemoveVerticesOp(names=["person"]))

    merged, result = merge_three_way(base, left, right)
    assert merged is None
    feed = next(c for c in result.conflicts if c.slot_key == ("resource", "feed"))
    assert sorted(
        op.names[0]
        for op in feed.cascaded
        if isinstance(op, ops_module.RemoveVerticesOp)
    ) == ["company", "person"]
    assert len(feed.cascaded) == 2
    assert feed.reason == _CASCADE_REASON


_CASCADE_REASON = (
    "one side's change reaches this slot only through what it removes or renames"
)


def test_a_removal_held_by_an_edited_pipeline_is_marked_cascaded() -> None:
    """The conflict on ``feed`` shows the removal as held through its cascade."""
    base = _fed({"rows": ["person"], "feed": ["person", "company"]})
    left = _applied(base, ops_module.RemoveVerticesOp(names=["company"]))
    right = _applied(
        base,
        ops_module.ReplaceResourcesOp(
            resources=[
                {
                    "name": "feed",
                    "pipeline": [{"vertex": "company"}, {"vertex": "person"}],
                }
            ]
        ),
    )

    _merged, result = merge_three_way(base, left, right)
    feed = next(c for c in result.conflicts if c.slot_key == ("resource", "feed"))
    assert [op.op for op in feed.cascaded] == ["remove_vertices"]
    assert [op.op for op in feed.right_ops] == ["replace_resources"]
    assert feed.reason == _CASCADE_REASON


def test_a_conflict_on_a_named_slot_marks_nothing_cascaded() -> None:
    base = _person(["id", "name"])
    left = _applied(
        base, ops_module.RemoveVertexPropertiesOp(removals={"person": ["name"]})
    )
    right = _applied(
        base,
        ops_module.ChangeFieldTypesOp(vertices={"person": {"name": {"type": "INT"}}}),
    )

    _merged, result = merge_three_way(base, left, right)
    assert result.conflicts
    assert all(conflict.cascaded == [] for conflict in result.conflicts)
    assert all(conflict.reason != _CASCADE_REASON for conflict in result.conflicts)


def _applied(base: GraphManifest, *ops: ManifestOp) -> GraphManifest:
    from graflo.architecture.evolution.apply import apply_evolution

    return apply_evolution(base, list(ops), bump_version=False, finish_init=False)


def test_set_edge_directed_splits_by_relation() -> None:
    """Every edge of a relation agrees on ``directed``, so a piece holds them all."""
    op = ops_module.SetEdgeDirectedOp(
        edges=[
            {"source": "person", "target": "person", "relation": "knows"},
            {"source": "person", "target": "company", "relation": "employs"},
            {"source": "person", "target": "company", "relation": "knows"},
        ],
        directed=False,
    )

    pieces = merge3.split_by_slot(op)
    assert all(isinstance(piece, ops_module.SetEdgeDirectedOp) for piece in pieces)
    assert [
        [
            edge.edge_id()
            for edge in typing.cast(ops_module.SetEdgeDirectedOp, piece).edges
        ]
        for piece in pieces
    ] == [
        [("person", "person", "knows"), ("person", "company", "knows")],
        [("person", "company", "employs")],
    ]


def test_set_edge_directed_depends_on_the_edges_of_its_relation() -> None:
    """An edge added to the relation would be left out in one order only."""
    self_knows = {"source": "person", "target": "person", "relation": "knows"}
    base = _people_and_companies([self_knows])
    undirect = ops_module.SetEdgeDirectedOp(edges=[self_knows], directed=False)

    assert not ops_independent(undirect, ops_module.AddEdgesOp(edges=[KNOWS]), base)


def test_resource_ops_split_per_resource() -> None:
    op = ops_module.ReplaceResourcesOp(
        resources=[
            {"name": "rows", "pipeline": [{"vertex": "person"}]},
            {"name": "feed", "pipeline": [{"vertex": "company"}]},
        ]
    )

    pieces = merge3.split_by_slot(op)
    assert [op_slots(piece) for piece in pieces] == [
        {("resource", "rows")},
        {("resource", "feed")},
    ]
    swap = ops_module.RenameResourcesOp(renames={"rows": "feed", "feed": "rows"})
    assert merge3.split_by_slot(swap) == [swap]


def test_an_op_held_by_two_conflicts_follows_both_decisions() -> None:
    """Taking a side's op in one of two conflicts holding it does not apply it."""
    base = _manifest(
        [
            _vertex("order", ["id", "name"], ["id"]),
            _vertex("city", ["id", "name"], ["id"]),
            {**_vertex("company", ["id"], ["id"]), "subclass_of": [{"vertex": "city"}]},
        ]
    )
    left = _applied(
        base,
        ops_module.ChangeFieldTypesOp(vertices={"order": {"name": {"type": "STRING"}}}),
        ops_module.RemoveVerticesOp(names=["city"]),
    )
    right = _applied(
        base,
        ops_module.ChangeFieldTypesOp(
            vertices={
                "order": {"name": {"type": "INT"}},
                "city": {"name": {"type": "INT"}},
            }
        ),
        ops_module.SetVertexSubclassOfOp(
            subclass_of={"order": [{"vertex": "company"}]}
        ),
    )
    _none, result = merge_three_way(base, left, right)
    holding = [
        conflict
        for conflict in result.conflicts
        if any(op.op == "set_vertex_subclass_of" for op in conflict.right_ops)
    ]
    assert len(holding) >= 2

    for flipped in holding:
        decisions = [
            take_right(c) if c is flipped else take_left(c) for c in result.conflicts
        ]
        merged, _resolved = merge_three_way(base, left, right, resolutions=decisions)
        assert merged is not None
        (order,) = [
            v for v in _core(merged).vertex_config.vertices if v.name == "order"
        ]
        assert order.subclass_of == []


def test_sanitize_depends_on_every_name() -> None:
    """The names it records depend on every type present, a new one included."""
    base = _people_and_companies([])
    sanitize = ops_module.SanitizeOp(db_flavor="tigergraph")
    order = ops_module.AddVerticesOp(vertices=[_vertex("order", ["id"], ["id"])])

    assert not ops_independent(sanitize, order, base)


def test_a_whole_profile_conflicts_with_an_index_added_beside_it() -> None:
    """A whole profile drops an index added beside it in one order only."""
    base = _person(["id", "name"])
    reset = ops_module.SetDbProfileOp(profile={"db_flavor": "arango"})
    index = ops_module.AddVertexIndexesOp(indexes={"person": [{"fields": ["id"]}]})

    assert not ops_independent(reset, index, base)


def test_bindings_depend_on_the_resources_they_wire() -> None:
    """A binding of a resource removed on the other side applies in one order only."""
    base = _fed({"rows": ["person"], "feed": ["company"]})
    remove = ops_module.RemoveResourcesOp(names=["feed"])
    wire = ops_module.SetBindingsOp(
        bindings={
            "connectors": [
                {"name": "feed_files", "regex": "feed.*", "resource_name": "feed"}
            ],
            "resource_connector": [{"resource": "feed", "connector": "feed_files"}],
        }
    )

    assert not ops_independent(remove, wire, base)


def test_a_new_pipeline_depends_on_the_properties_of_what_it_produces() -> None:
    """A property rename rewrites every pipeline producing the vertex, a new one too."""
    base = _fed({"rows": ["person"]})
    rename = ops_module.CanonicalizeOp(properties={"person": {"id": "name"}})
    add = ops_module.AddResourcesOp(
        resources=[{"name": "feed", "pipeline": [{"vertex": "person"}]}]
    )

    assert not ops_independent(rename, add, base)


def test_creating_the_ingestion_block_depends_on_the_bindings() -> None:
    """The block is refused while bindings wire a resource it lacks."""
    base = _person(["id"])
    create = ops_module.AddResourcesOp(
        resources=[{"name": "rows", "pipeline": [{"vertex": "person"}]}]
    )
    wire = ops_module.SetBindingsOp(
        bindings={
            "connectors": [
                {"name": "feed_files", "regex": "feed.*", "resource_name": "feed"}
            ]
        }
    )

    assert ("bindings",) in op_reads(create, base)
    assert not ops_independent(create, wire, base)


def _profiled(vertices: list[dict], profile: dict) -> GraphManifest:
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "t"},
                "graph": {
                    "vertex_config": {"vertices": vertices},
                    "edge_config": {"edges": []},
                },
                "db_profile": profile,
            }
        }
    )


_PERSON = _vertex("person", ["id", "name"], ["id"])
_ORDER = _vertex("order", ["id", "total"], ["id"])
_ORDER_INDEXED = {
    "db_flavor": "neo4j",
    "vertex_indexes": {"order": [{"fields": ["total"]}]},
}


def test_a_profile_replaced_beside_a_new_indexed_type_conflicts_either_way() -> None:
    """An index op reads the profile, so a whole-profile change on the other side meets it."""
    base = _profiled([_PERSON], {"db_flavor": "neo4j"})
    indexed = _profiled([_PERSON, _ORDER], _ORDER_INDEXED)
    replaced = _profiled([_PERSON], {"db_flavor": "arango"})

    for left, right in ((indexed, replaced), (replaced, indexed)):
        merged, result = merge_three_way(base, left, right)
        assert merged is None
        assert [c.slot for c in result.conflicts] == [["db_profile"]]


def test_index_additions_on_both_sides_merge_the_same_in_either_order() -> None:
    from graflo.architecture.evolution.hashing import manifest_hash

    base = _profiled([_PERSON], {"db_flavor": "neo4j"})
    indexed = _profiled([_PERSON, _ORDER], _ORDER_INDEXED)
    person_indexed = _profiled(
        [_PERSON],
        {"db_flavor": "neo4j", "vertex_indexes": {"person": [{"fields": ["name"]}]}},
    )

    first, _ = merge_three_way(base, indexed, person_indexed)
    second, _ = merge_three_way(base, person_indexed, indexed)
    assert first is not None and second is not None
    assert manifest_hash(first) == manifest_hash(second)
