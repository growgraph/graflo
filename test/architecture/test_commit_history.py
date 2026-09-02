"""The commit DAG: ids, verified replay, forks, and the store.

What the old ``RevisionChain`` guaranteed -- verified replay -- has to survive
the move to a DAG unchanged; what it got wrong -- raising on a fork -- has to
stop. These tests hold both ends.
"""

from __future__ import annotations

import pathlib

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.evolution.apply import apply_evolution
from graflo.architecture.evolution.commit import (
    Commit,
    CommitError,
    build_commit,
    build_merge_commit,
    compute_commit_id,
)
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import (
    FileCommitStore,
    History,
    checkout,
    verify_history,
)
from graflo.architecture.evolution.ops import AddVertexPropertiesOp


def _manifest(properties: list[str]) -> GraphManifest:
    return GraphManifest.from_dict(
        {
            "schema": {
                "metadata": {"name": "t"},
                "graph": {
                    "vertex_config": {
                        "vertices": [
                            {
                                "name": "a",
                                "properties": [{"name": p} for p in properties],
                                "identity": ["id"],
                            }
                        ]
                    },
                    "edge_config": {"edges": []},
                },
            }
        }
    )


def _add(field: str) -> AddVertexPropertiesOp:
    return AddVertexPropertiesOp(additions={"a": [field]})


@pytest.fixture
def linear() -> tuple[GraphManifest, History, Commit, Commit]:
    base = _manifest(["id"])
    first = build_commit(base, [_add("y")], label="add y")
    after_first = apply_evolution(
        base, list(first.ops), bump_version=False, finish_init=False
    )
    second = build_commit(after_first, [_add("z")], parents=[first.id], label="add z")
    return base, History(commits=[first, second]), first, second


# ── commit ids ──────────────────────────────────────────────────────────────


def test_the_commit_id_is_content_derived_and_stable() -> None:
    left = compute_commit_id([_add("y")], ["p1"])
    right = compute_commit_id([_add("y")], ["p1"])
    assert left == right, "the same change on the same parents must be the same commit"


def test_parent_order_is_part_of_the_commit_id() -> None:
    """A merge materialized against A-then-B is not the one against B-then-A."""
    assert compute_commit_id([_add("y")], ["a", "b"]) != compute_commit_id(
        [_add("y")], ["b", "a"]
    )


def test_a_commit_records_the_trees_it_moves_between(linear) -> None:
    base, _history, first, _second = linear
    assert first.tree_before == manifest_hash(base)
    applied = apply_evolution(
        base, list(first.ops), bump_version=False, finish_init=False
    )
    assert first.tree == manifest_hash(applied)


def test_an_empty_change_set_is_refused() -> None:
    with pytest.raises(CommitError, match="at least one operation"):
        build_commit(_manifest(["id"]), [])


def test_a_no_op_change_set_is_refused() -> None:
    """A commit that moves nothing is a lie about history, not a harmless entry."""
    base = _manifest(["id", "y"])
    with pytest.raises(CommitError, match="leave the manifest unchanged"):
        build_commit(base, [_add("y")])


def test_an_unknown_kind_is_refused() -> None:
    with pytest.raises(CommitError, match="unknown commit kind"):
        build_commit(_manifest(["id"]), [_add("y")], kind="rebase")


# ── the DAG ─────────────────────────────────────────────────────────────────


def test_a_linear_history_linearizes(linear) -> None:
    _base, history, first, second = linear
    assert [c.id for c in history.linearize()] == [first.id, second.id]
    assert [c.id for c in history.heads()] == [second.id]
    assert history.roots()[0].id == first.id


def test_checkout_replays_and_verifies(linear) -> None:
    base, history, first, second = linear
    assert manifest_hash(checkout(base, history)) == second.tree
    assert manifest_hash(checkout(base, history, first.id)) == first.tree


def test_checkout_accepts_an_unambiguous_prefix(linear) -> None:
    base, history, first, _second = linear
    assert manifest_hash(checkout(base, history, first.id[:8])) == first.tree


def test_replay_against_a_drifted_base_fails_loudly(linear) -> None:
    """The whole point of recording both trees."""
    _base, history, _first, _second = linear
    with pytest.raises(CommitError, match="has drifted"):
        checkout(_manifest(["id", "unexpected"]), history)


def test_a_parent_that_does_not_exist_is_rejected() -> None:
    orphan = build_commit(_manifest(["id"]), [_add("y")], parents=["nosuchcommit"])
    with pytest.raises(ValueError, match="names parents not in this history"):
        History(commits=[orphan])


def test_duplicate_commit_ids_are_rejected(linear) -> None:
    _base, _history, first, _second = linear
    with pytest.raises(ValueError, match="duplicate commit ids"):
        History(commits=[first, first])


def test_a_broken_first_parent_edge_is_rejected(linear) -> None:
    base, _history, first, second = linear
    detached = second.model_copy(update={"tree_before": manifest_hash(base)})
    with pytest.raises(ValueError, match="expects to start from tree"):
        History(commits=[first, detached])


# ── forks are facts, not errors ─────────────────────────────────────────────


@pytest.fixture
def forked(linear) -> tuple[GraphManifest, History, Commit, Commit]:
    base, _history, first, second = linear
    after_first = apply_evolution(
        base, list(first.ops), bump_version=False, finish_init=False
    )
    sibling = build_commit(after_first, [_add("w")], parents=[first.id], label="add w")
    return base, History(commits=[first, second, sibling]), second, sibling


def test_a_fork_is_recorded_rather_than_raised(forked) -> None:
    """`RevisionChain` raised here. A history that cannot hold a fork is not a record."""
    _base, history, second, sibling = forked
    assert {c.id for c in history.heads()} == {second.id, sibling.id}


def test_linearize_refuses_a_forked_history_by_name(forked) -> None:
    _base, history, _second, _sibling = forked
    with pytest.raises(CommitError, match="forked into 2 heads"):
        history.linearize()


def test_checkout_without_a_commit_refuses_to_guess_which_head(forked) -> None:
    base, history, _second, _sibling = forked
    with pytest.raises(CommitError, match="name the commit to check out"):
        checkout(base, history)


def test_each_head_of_a_fork_replays_on_its_own(forked) -> None:
    base, history, second, sibling = forked
    assert manifest_hash(checkout(base, history, second.id)) == second.tree
    assert manifest_hash(checkout(base, history, sibling.id)) == sibling.tree
    assert verify_history(base, history) == []


def test_topological_order_is_deterministic(forked) -> None:
    _base, history, _second, _sibling = forked
    assert [c.id for c in history.topological()] == [
        c.id for c in history.topological()
    ]


def test_ancestors_walks_every_parent_edge(forked) -> None:
    _base, history, second, _sibling = forked
    root = history.roots()[0]
    assert history.ancestors(second.id) == {root.id}
    assert history.ancestors(second.id, include_self=True) == {root.id, second.id}


def test_a_cycle_is_rejected() -> None:
    """`ancestors` must terminate, so the DAG-ness is validated not assumed."""
    base = _manifest(["id"])
    real = build_commit(base, [_add("y")])
    looped = real.model_copy(update={"parents": [real.id]})
    with pytest.raises(ValueError, match="cycle"):
        History(commits=[looped])


# ── merge commits ───────────────────────────────────────────────────────────


def test_a_merge_commit_is_materialized_against_its_first_parent(linear) -> None:
    """The decision that keeps replay uniform across edit and merge commits."""
    base, _history, first, _second = linear
    first_parent_state = apply_evolution(
        base, list(first.ops), bump_version=False, finish_init=False
    )
    merged = _manifest(["id", "y", "from_the_other_side"])

    commit = build_merge_commit(
        first_parent_state, merged, parents=[first.id, "otherbranch1"], kind="merge"
    )
    assert commit.is_merge
    assert commit.first_parent == first.id
    assert commit.tree_before == manifest_hash(first_parent_state)
    assert commit.tree == manifest_hash(merged)

    replayed = apply_evolution(
        first_parent_state, list(commit.ops), bump_version=False, finish_init=False
    )
    assert manifest_hash(replayed) == manifest_hash(merged)


def test_a_merge_commit_needs_at_least_two_parents(linear) -> None:
    base, _history, _first, _second = linear
    with pytest.raises(CommitError, match="at least two parents"):
        build_merge_commit(base, _manifest(["id", "q"]), parents=["only-one"])


def test_a_merge_identical_to_its_first_parent_is_refused(linear) -> None:
    base, _history, _first, _second = linear
    with pytest.raises(CommitError, match="nothing to record"):
        build_merge_commit(base, _manifest(["id"]), parents=["a", "b"])


# ── the store ───────────────────────────────────────────────────────────────


def test_the_store_round_trips_a_forked_history(forked, tmp_path: pathlib.Path) -> None:
    _base, history, second, sibling = forked
    store = FileCommitStore(tmp_path / "commits")
    store.save(history)

    restored = store.load()
    assert {c.id for c in restored.commits} == {c.id for c in history.commits}
    assert {c.id for c in restored.heads()} == {second.id, sibling.id}
    # Ops survive the codec round trip, which is what replay depends on.
    restored_op = restored.require(second.id).ops[0]
    assert isinstance(restored_op, AddVertexPropertiesOp)
    assert restored_op.additions == {"a": ["z"]}


def test_the_store_rebuilds_the_dag_from_parent_ids_not_filenames(
    forked, tmp_path: pathlib.Path
) -> None:
    """Filename order is a convenience; the parent ids are the truth."""
    _base, history, _second, _sibling = forked
    store = FileCommitStore(tmp_path / "commits")
    store.save(history)
    for path in sorted((tmp_path / "commits").glob("*.yaml")):
        path.rename(path.with_name(f"zzz_{path.name}"))
    restored = FileCommitStore(tmp_path / "commits").load()
    assert len(restored.heads()) == 2


def test_appending_validates_the_dag(linear, tmp_path: pathlib.Path) -> None:
    _base, history, _first, _second = linear
    store = FileCommitStore(tmp_path / "commits")
    store.save(history)
    orphan = build_commit(_manifest(["id"]), [_add("q")], parents=["nosuchparent"])
    with pytest.raises(ValueError, match="names parents not in this history"):
        store.append(orphan)


def test_an_empty_store_loads_an_empty_history(tmp_path: pathlib.Path) -> None:
    assert not FileCommitStore(tmp_path / "nothing-here").load().commits
