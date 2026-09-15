"""Recording a merge as a two-parent commit.

A merged manifest used to be a lineage dead-end: it dropped the left's
provenance (correctly -- it is a new artifact with a new content address) and
nothing downstream picked it up, so nothing recorded which two manifests
produced it or under which equivalences.

The commit is materialized exactly as a merge commit is -- the verified diff
from its **first** parent -- so replay and hash verification need no special
case. What makes it a merge is the recipe riding alongside it.
"""

from __future__ import annotations

import pytest

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import stamp_provenance
from graflo.architecture.evolution import MergeManifestsOp, merge_manifests
from graflo.architecture.evolution.commit import CommitError, build_root_commit
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import History, checkout
from graflo.architecture.evolution.merge3 import build_merge_recipe
from graflo.architecture.evolution.merge_commit import (
    build_merge_commit,
    find_commit_by_tree,
)


def _manifest(vertex: str, *, bindings: bool = False) -> GraphManifest:
    payload: dict = {
        "schema": {
            "metadata": {"name": vertex.lower(), "version": "1.0.0"},
            "graph": {
                "vertex_config": {
                    "vertices": [
                        {
                            "name": vertex,
                            "properties": [{"name": "id"}],
                            "identity": ["id"],
                        }
                    ]
                },
                "edge_config": {"edges": []},
            },
        }
    }
    if bindings:
        payload["ingestion_model"] = {
            "resources": [{"name": "src", "pipeline": [{"vertex": vertex}]}]
        }
        payload["bindings"] = {
            "connectors": [{"regex": r"^src\.csv$", "resource_name": "src"}]
        }
    manifest = GraphManifest.from_config(payload)
    manifest.finish_init()
    return manifest


def _compose(left: GraphManifest, right: GraphManifest) -> GraphManifest:
    return merge_manifests(
        left, right, MergeManifestsOp(), bump_version=False, finish_init=False
    )


class TestTheCommit:
    def test_a_compose_commit_names_both_parents(self) -> None:
        left, right = _manifest("Server"), _manifest("Scan")
        merged = _compose(left, right)
        recipe = build_merge_recipe(left, right, MergeManifestsOp())

        entry = build_merge_commit(
            left, merged, parents=["a" * 12, "b" * 12], recipe=recipe, label="join"
        )

        assert entry.kind == "merge"
        assert entry.parents == ["a" * 12, "b" * 12]
        assert entry.is_multi_parent
        assert entry.tree == manifest_hash(merged)

    def test_the_recipe_rides_along_flavoured_as_a_compose(self) -> None:
        """A re-merge reads this; merge3's own recipe would not describe it."""
        left, right = _manifest("Server"), _manifest("Scan")
        op = MergeManifestsOp(name_conflict="prefix_right")
        recipe = build_merge_recipe(left, right, op)

        entry = build_merge_commit(
            left, _compose(left, right), parents=["a" * 12, "b" * 12], recipe=recipe
        )

        assert entry.merge_recipe is not None
        assert entry.merge_recipe.kind == "merge"
        assert entry.merge_recipe.payload["name_conflict"] == "prefix_right"
        # No merge base -- the absence is the signal, and it is what
        # distinguishes merge from a three-way merge. A recorded base would
        # send a re-merge looking for a common ancestor there is none of.
        assert "base" not in entry.merge_recipe.payload

    def test_a_compose_carrying_bindings_is_recordable(self) -> None:
        """The case that could not be recorded before ``set_bindings`` existed.

        Merge unions the bindings registries, so the diff from the left side
        has to be able to say the block changed -- otherwise the flagship
        overlay merge is exactly the one with no lineage.
        """
        left, right = _manifest("Server"), _manifest("Scan", bindings=True)
        merged = _compose(left, right)

        entry = build_merge_commit(
            left,
            merged,
            parents=["a" * 12, "b" * 12],
            recipe=build_merge_recipe(left, right, MergeManifestsOp()),
        )

        assert "set_bindings" in {op.op for op in entry.ops}

    def test_recomposing_the_same_inputs_yields_the_same_commit(self) -> None:
        """Content-derived, so a re-run is the same commit and not a duplicate."""
        left, right = _manifest("Server"), _manifest("Scan")
        recipe = build_merge_recipe(left, right, MergeManifestsOp())
        parents = ["a" * 12, "b" * 12]

        first = build_merge_commit(
            left, _compose(left, right), parents=parents, recipe=recipe
        )
        second = build_merge_commit(
            left, _compose(left, right), parents=parents, recipe=recipe
        )

        assert first.id == second.id

    def test_one_parent_is_refused(self) -> None:
        left, right = _manifest("Server"), _manifest("Scan")
        with pytest.raises(CommitError, match="at least two parents"):
            build_merge_commit(
                left,
                _compose(left, right),
                parents=["a" * 12],
                recipe=build_merge_recipe(left, right, MergeManifestsOp()),
            )


class TestReplay:
    def test_the_compose_commit_replays_from_its_first_parent(self) -> None:
        """First-parent replay, with no special case for the second lineage."""
        left, right = _manifest("Server"), _manifest("Scan", bindings=True)
        merged = _compose(left, right)

        left_root = build_root_commit(left, scope="left")
        right_root = build_root_commit(right, scope="right")
        entry = build_merge_commit(
            left,
            merged,
            parents=[left_root.id, right_root.id],
            recipe=build_merge_recipe(left, right, MergeManifestsOp()),
        )
        history = History(commits=[left_root, right_root, entry])

        assert manifest_hash(checkout(left, history, entry.id)) == manifest_hash(merged)


class TestFindingAParentByContent:
    def test_a_manifest_resolves_to_the_commit_that_produced_it(self) -> None:
        left = _manifest("Server")
        root = build_root_commit(left, scope="left")
        history = History(commits=[root])

        assert find_commit_by_tree(history, left) is root

    def test_a_manifest_no_commit_reached_resolves_to_nothing(self) -> None:
        history = History(commits=[build_root_commit(_manifest("Server"), scope="l")])

        assert find_commit_by_tree(history, _manifest("Other")) is None


class TestStampingDoesNotMoveTheContentAddress:
    def test_recording_lineage_leaves_the_tree_where_it_was(self) -> None:
        """The invariant the whole design rests on.

        If writing provenance moved the hash, the commit would record a tree the
        artifact no longer has, and two routes to one world model could never be
        recognised as the same.
        """
        left, right = _manifest("Server"), _manifest("Scan")
        merged = _compose(left, right)
        before = manifest_hash(merged)

        stamp_provenance(
            merged,
            content_hash=before,
            canon="graflo/canon@2",
            commit="c" * 12,
            parents=["a" * 12, "b" * 12],
            merge_recipe="d" * 64,
        )

        assert manifest_hash(merged) == before
        assert merged.metadata is not None
        assert merged.metadata.provenance is not None
        assert merged.metadata.provenance.parents == ["a" * 12, "b" * 12]
        assert merged.metadata.provenance.is_multi_parent
