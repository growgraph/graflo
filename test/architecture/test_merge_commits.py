"""Merging two commits: each side's renames come from its commits.

A diff cannot tell a rename from a removal plus an addition. Merged as bare
manifests, two sides renaming one field differently would each look like they
added a field, and the result would carry both names. Merged as commits, the
renames are read from the history, so they conflict where they should, and the
merge commit records a rename the other side made as a rename.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.autogenerate import RenameHints
from graflo.architecture.evolution.codec import op_to_dict
from graflo.architecture.evolution.commit import (
    build_commit,
    build_multi_parent_commit,
    build_root_commit,
)
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import History, checkout
from graflo.architecture.evolution.merge3 import (
    MergeError,
    first_parent_hints,
    merge_commits,
    merge_three_way,
    take_left,
    take_right,
)
from graflo.architecture.evolution.ops import (
    AddVertexPropertiesOp,
    ManifestOp,
    RenameVertexPropertiesOp,
    RenameVerticesOp,
)

TOTAL = ("vertex", "order", "field", "total")


def _base() -> GraphManifest:
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "shop"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "order",
                                "properties": [{"name": "id"}, {"name": "total"}],
                                "identity": ["id"],
                            },
                            {
                                "name": "product",
                                "properties": [{"name": "sku"}, {"name": "title"}],
                                "identity": ["sku"],
                            },
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            }
        }
    )


def _fork(
    left_ops: list[ManifestOp], right_ops: list[ManifestOp]
) -> tuple[GraphManifest, History, str, str]:
    """A root and two branches off it, as (base, history, left id, right id)."""
    base = _base()
    root = build_root_commit(base, label="root")
    left = build_commit(base, left_ops, parents=[root.id], label="left")
    right = build_commit(base, right_ops, parents=[root.id], label="right")
    return base, History(commits=[root, left, right]), left.id, right.id


def _fields(manifest: GraphManifest, vertex: str) -> list[str]:
    schema = manifest.graph_schema
    assert schema is not None
    (found,) = [
        v for v in schema.core_schema.vertex_config.vertices if v.name == vertex
    ]
    return list(found.property_names)


def _rename(vertex: str, old: str, new: str) -> RenameVertexPropertiesOp:
    return RenameVertexPropertiesOp(renames={vertex: {old: new}})


def _add(vertex: str, field: str) -> AddVertexPropertiesOp:
    return AddVertexPropertiesOp(additions={vertex: [field]})


def _record(merge, base: GraphManifest, history: History, left: str, right: str):
    """The merge commit, and the state its first-parent replay reaches."""
    assert merge.merged is not None
    commit = build_multi_parent_commit(
        merge.left, merge.merged, parents=[left, right], hints=merge.first_parent_hints
    )
    replayed = checkout(base, history.extend(commit), commit.id)
    return commit, replayed


def test_two_sides_renaming_one_field_differently_conflict_on_that_field() -> None:
    base, history, left, right = _fork(
        [_rename("order", "total", "amount")],
        [_rename("order", "total", "grand_total")],
    )
    merge = merge_commits(base, history, left, right)

    assert merge.merged is None
    assert [conflict.slot_key for conflict in merge.result.conflicts] == [TOTAL]
    assert merge.left_hints.vertex_properties == {"order": {"total": "amount"}}
    assert merge.right_hints.vertex_properties == {"order": {"total": "grand_total"}}

    decided = merge_commits(
        base,
        history,
        left,
        right,
        resolutions=[take_left(c) for c in merge.result.conflicts],
    )
    assert decided.merged is not None
    assert _fields(decided.merged, "order") == ["id", "amount"]


def test_a_rename_from_the_other_side_is_recorded_as_a_rename() -> None:
    base, history, left, right = _fork(
        [_add("order", "currency")], [_rename("product", "title", "name")]
    )
    merge = merge_commits(base, history, left, right)
    assert merge.result.clean
    assert merge.first_parent_hints is not None
    assert merge.first_parent_hints.vertex_properties == {"product": {"title": "name"}}

    commit, replayed = _record(merge, base, history, left, right)
    kinds = [op.op for op in commit.ops]
    assert "rename_vertex_properties" in kinds
    assert "remove_vertex_properties" not in kinds
    assert merge.merged is not None
    assert manifest_hash(replayed) == manifest_hash(merge.merged)


def test_a_contested_rename_decided_for_the_right_renames_the_left_name() -> None:
    base, history, left, right = _fork(
        [_rename("order", "total", "amount"), _add("product", "category")],
        [_rename("order", "total", "grand_total")],
    )
    conflicts = merge_commits(base, history, left, right).result.conflicts
    merge = merge_commits(
        base, history, left, right, resolutions=[take_right(c) for c in conflicts]
    )
    assert merge.merged is not None
    assert _fields(merge.merged, "order") == ["id", "grand_total"]
    assert merge.first_parent_hints is not None
    assert merge.first_parent_hints.vertex_properties == {
        "order": {"amount": "grand_total"}
    }

    commit, replayed = _record(merge, base, history, left, right)
    assert [op_to_dict(op) for op in commit.ops] == [
        {
            "op": "rename_vertex_properties",
            "renames": {"order": {"amount": "grand_total"}},
        }
    ]
    assert manifest_hash(replayed) == manifest_hash(merge.merged)


def test_the_first_parent_s_own_renames_are_not_hints_for_its_merge() -> None:
    """Undoing the left side's renames, owner and field, leaves only the right's."""
    base, history, left, right = _fork(
        [
            RenameVerticesOp(renames={"product": "item"}),
            _rename("item", "title", "label"),
        ],
        [_rename("order", "total", "amount")],
    )
    merge = merge_commits(base, history, left, right)
    assert merge.result.clean
    assert merge.first_parent_hints is not None
    assert merge.first_parent_hints.vertices == {}
    assert merge.first_parent_hints.vertex_properties == {"order": {"total": "amount"}}


def test_a_merge_base_off_a_side_s_first_parent_path_is_refused() -> None:
    """Renames reaching a side only through a merge's second parent are unknown."""
    base = _base()
    root = build_root_commit(base, label="root")
    a1 = build_commit(base, [_add("order", "a")], parents=[root.id])
    b1 = build_commit(base, [_add("order", "b")], parents=[root.id])
    after_a1 = apply_evolution(
        base, list(a1.ops), bump_version=False, finish_init=False
    )
    after_b1 = apply_evolution(
        base, list(b1.ops), bump_version=False, finish_init=False
    )
    both = apply_evolution(
        after_a1, list(b1.ops), bump_version=False, finish_init=False
    )
    joined = build_multi_parent_commit(after_a1, both, parents=[a1.id, b1.id])
    c1 = build_commit(after_b1, [_add("order", "c")], parents=[b1.id])
    history = History(commits=[root, a1, b1, joined, c1])

    with pytest.raises(MergeError, match="first-parent path"):
        merge_commits(base, history, joined.id, c1.id)


def test_commits_without_a_common_ancestor_are_refused() -> None:
    base = _base()
    one = build_commit(base, [_add("order", "a")], label="one")
    two = build_commit(base, [_add("order", "b")], label="two")
    with pytest.raises(MergeError, match="share no ancestor"):
        merge_commits(base, History(commits=[one, two]), one.id, two.id)


def test_commit_ids_may_be_given_as_unique_prefixes() -> None:
    base, history, left, right = _fork([_add("order", "a")], [_add("order", "b")])
    merge = merge_commits(base, history, left[:10], right[:10])
    assert merge.merged is not None
    assert _fields(merge.merged, "order") == ["id", "total", "a", "b"]


def test_first_parent_hints_serve_a_merge_of_bare_manifests() -> None:
    """A caller holding states and hints, not commits, records the same renames."""
    base = _base()
    left_ops: list[ManifestOp] = [_rename("order", "total", "amount")]
    right_ops: list[ManifestOp] = [_rename("order", "total", "grand_total")]
    left = apply_evolution(base, left_ops, bump_version=False, finish_init=False)
    right = apply_evolution(base, right_ops, bump_version=False, finish_init=False)
    left_hints = RenameHints.from_ops(left_ops, base=base)
    right_hints = RenameHints.from_ops(right_ops, base=base)
    _, unresolved = merge_three_way(
        base, left, right, left_hints=left_hints, right_hints=right_hints
    )
    merged, result = merge_three_way(
        base,
        left,
        right,
        resolutions=[take_right(c) for c in unresolved.conflicts],
        left_hints=left_hints,
        right_hints=right_hints,
    )
    assert merged is not None

    renames = first_parent_hints(base, left, left_hints, result)
    assert [op_to_dict(op) for op in renames.to_ops()] == [
        {
            "op": "rename_vertex_properties",
            "renames": {"order": {"amount": "grand_total"}},
        }
    ]
