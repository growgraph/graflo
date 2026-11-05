"""Recording a lift as a ``lift`` commit carrying its recipe.

What a lift adds cannot be told apart from vocabulary a manifest declared
itself, so the lift is recorded as the event it was: a single-parent commit of
the planned ops, with the aspects, their versions and the spec alongside. A
re-lift reads that recipe; a revert retracts the lift like any other change.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from graflo.architecture.contract.manifest import GraphManifest
from graflo.architecture.contract.provenance import stamp_provenance
from graflo.architecture.evolution import apply_evolution
from graflo.architecture.evolution.aspects import (
    ASPECTS,
    LiftError,
    LiftSpec,
    build_lift_recipe,
    plan_lift,
    plan_relift,
    recipe_drift,
)
from graflo.architecture.evolution.commit import (
    Commit,
    CommitError,
    RecipeRef,
    build_revert_commit,
    build_root_commit,
)
from graflo.architecture.evolution.hashing import manifest_hash
from graflo.architecture.evolution.history import (
    FileCommitStore,
    History,
    _commit_to_dict,
    _recorded_recipe,
    checkout,
    checkout_parent,
)
from graflo.architecture.evolution.lift_commit import build_lift_commit, lift_recipe_of

PROVENANCE_ONLY = ["provenance"]

PLAIN: dict = {
    "schema": {
        "metadata": {"name": "support", "version": "1.0.0"},
        "graph": {
            "vertex_config": {
                "vertices": [
                    {
                        "name": "Ticket",
                        "properties": [{"name": "ticket_id", "type": "STRING"}],
                        "identity": ["ticket_id"],
                    },
                    {
                        "name": "Product",
                        "properties": [{"name": "sku", "type": "STRING"}],
                        "identity": ["sku"],
                    },
                ]
            },
            "edge_config": {"edges": []},
        },
    }
}

SPEC = LiftSpec(provenance_for=["Ticket", "Product"], mentions_for=["Product"])


def _manifest(config: dict | None = None) -> GraphManifest:
    manifest = GraphManifest.from_config(config or PLAIN)
    manifest.finish_init()
    return manifest


def _lifted_history() -> tuple[GraphManifest, GraphManifest, Commit, Commit]:
    """A root naming the plain manifest, and the lift of it as its child."""
    base = _manifest()
    ops = plan_lift(base, SPEC, aspects=PROVENANCE_ONLY)
    root = build_root_commit(base, label="base")
    entry = build_lift_commit(
        base,
        ops,
        parent=root.id,
        recipe=build_lift_recipe(base, SPEC, aspects=PROVENANCE_ONLY),
    )
    return base, apply_evolution(base, ops), root, entry


class TestTheRecipe:
    def test_it_names_each_aspect_with_its_version(self) -> None:
        recipe = build_lift_recipe(_manifest(), SPEC, aspects=["provenance", "access"])

        assert recipe.kind == "lift"
        assert recipe.aspects == ["provenance", "access"]
        assert recipe.versions == {
            "provenance": ASPECTS["provenance"].version,
            "access": ASPECTS["access"].version,
        }
        assert recipe.base == manifest_hash(_manifest())

    def test_the_spec_is_recorded_without_its_defaults(self) -> None:
        """A field added to LiftSpec later cannot move a recorded recipe's hash."""
        recipe = build_lift_recipe(_manifest(), SPEC, aspects=PROVENANCE_ONLY)

        assert recipe.spec == {
            "provenance_for": ["Product", "Ticket"],
            "mentions_for": ["Product"],
        }

    def test_a_reordered_declaration_is_the_same_recipe(self) -> None:
        """The planners read these lists as sets, so their order means nothing."""
        manifest = _manifest()
        one = build_lift_recipe(
            manifest,
            LiftSpec(provenance_for=["Ticket", "Product"]),
            aspects=PROVENANCE_ONLY,
        )
        other = build_lift_recipe(
            manifest,
            LiftSpec(provenance_for=["Product", "Ticket"]),
            aspects=PROVENANCE_ONLY,
        )

        assert one.content_hash() == other.content_hash()

    def test_the_hash_says_what_was_asked_not_which_graflo_asked(self) -> None:
        recipe = build_lift_recipe(_manifest(), SPEC, aspects=PROVENANCE_ONLY)
        elsewhere = recipe.model_copy(update={"graflo_version": "0.0.1"})

        assert elsewhere.content_hash() == recipe.content_hash()
        assert (
            recipe.model_copy(update={"versions": {"provenance": 99}}).content_hash()
            != recipe.content_hash()
        )

    def test_an_unknown_aspect_is_refused_by_name(self) -> None:
        with pytest.raises(LiftError, match="no aspect named"):
            build_lift_recipe(_manifest(), SPEC, aspects=["no-such-aspect"])


class TestTheCommit:
    def test_a_lift_is_a_single_parent_commit_carrying_its_recipe(self) -> None:
        base, lifted, root, entry = _lifted_history()

        assert entry.kind == "lift"
        assert entry.parents == [root.id]
        assert entry.recipe is not None and entry.recipe.kind == "lift"
        assert entry.tree_before == manifest_hash(base)
        assert entry.tree == manifest_hash(lifted)
        assert entry.reversible
        assert entry.label == "lift provenance"

    def test_the_recipe_rides_with_the_commit_whole(self) -> None:
        base, _, _, entry = _lifted_history()
        recipe = lift_recipe_of(entry)

        assert entry.recipe is not None
        assert entry.recipe.hash == recipe.content_hash()
        assert recipe == build_lift_recipe(base, SPEC, aspects=PROVENANCE_ONLY)

    def test_a_recipe_planned_on_another_manifest_is_refused(self) -> None:
        base = _manifest()
        other = apply_evolution(base, plan_lift(base, SPEC, aspects=PROVENANCE_ONLY))

        with pytest.raises(CommitError, match="planned on tree"):
            build_lift_commit(
                base,
                plan_lift(base, SPEC, aspects=PROVENANCE_ONLY),
                parent="a" * 12,
                recipe=build_lift_recipe(other, SPEC, aspects=PROVENANCE_ONLY),
            )

    def test_a_manifest_that_already_carries_the_aspects_records_nothing(self) -> None:
        """Lifting twice is lifting once, so a second lift has no event to record."""
        _, lifted, _, _ = _lifted_history()

        with pytest.raises(CommitError):
            build_lift_commit(
                lifted,
                plan_lift(lifted, SPEC, aspects=PROVENANCE_ONLY),
                parent="a" * 12,
                recipe=build_lift_recipe(lifted, SPEC, aspects=PROVENANCE_ONLY),
            )

    def test_the_recipe_of_another_kind_of_commit_is_refused(self) -> None:
        _, _, root, _ = _lifted_history()

        with pytest.raises(CommitError, match="records no lift recipe"):
            lift_recipe_of(root)


class TestARecipeMatchesItsCommit:
    """A replay reads a recipe by its commit's kind, so a mismatch fails early."""

    def _with(self, entry: Commit, **update) -> dict:
        return {**entry.model_dump(), **update}

    def test_a_lift_with_two_parents_is_refused(self) -> None:
        _, _, root, entry = _lifted_history()

        with pytest.raises(ValueError, match="exactly one"):
            Commit.model_validate(self._with(entry, parents=[root.id, "b" * 12]))

    def test_a_lift_carrying_a_merge_recipe_is_refused(self) -> None:
        _, _, _, entry = _lifted_history()
        merge = RecipeRef(hash="d" * 64, kind="merge")

        with pytest.raises(ValueError, match="carries a merge recipe"):
            Commit.model_validate(self._with(entry, recipe=merge.model_dump()))

    def test_an_edit_carrying_a_recipe_is_refused(self) -> None:
        _, _, _, entry = _lifted_history()

        with pytest.raises(ValueError, match="carries no recipe"):
            Commit.model_validate(self._with(entry, kind="edit"))


class TestHistory:
    def test_checkout_replays_through_a_lift(self) -> None:
        base, lifted, root, entry = _lifted_history()
        history = History(commits=[root, entry])

        assert manifest_hash(checkout(base, history, entry.id)) == manifest_hash(lifted)

    def test_a_revert_retracts_the_lift(self) -> None:
        base, lifted, root, entry = _lifted_history()
        history = History(commits=[root, entry])

        undo = build_revert_commit(
            lifted,
            entry,
            before=checkout_parent(base, history, entry.id),
            parents=[entry.id],
        )

        assert undo.kind == "revert"
        assert undo.tree == manifest_hash(base)

    def test_the_store_keeps_the_recipe(self, tmp_path: Path) -> None:
        _, _, root, entry = _lifted_history()
        store = FileCommitStore(tmp_path / "commits")
        store.append(root)
        store.append(entry)

        loaded = store.load().require(entry.id)

        assert loaded.kind == "lift"
        assert lift_recipe_of(loaded) == lift_recipe_of(entry)

    def test_a_recipe_recorded_under_its_old_key_still_loads(
        self, tmp_path: Path
    ) -> None:
        """History cannot be edited, so the old key is translated on read."""
        _, _, root, entry = _lifted_history()
        store = FileCommitStore(tmp_path / "commits")
        store.append(root)
        recorded = _commit_to_dict(entry)
        reference = recorded.pop("recipe")
        del reference["kind"]
        recorded["merge_recipe"] = reference
        old = store.root / f"0001_{entry.id}_old.yaml"
        old.write_text(yaml.safe_dump(recorded), encoding="utf-8")

        assert store.load().require(entry.id).recipe == entry.recipe

    def test_a_recorded_recipe_without_a_kind_was_a_merge3(self) -> None:
        """``merge3`` was the default, so a dump left it out."""
        data = _recorded_recipe({"merge_recipe": {"hash": "d" * 64, "payload": {}}})

        assert data == {"recipe": {"hash": "d" * 64, "payload": {}, "kind": "merge3"}}


class TestRelift:
    def test_it_plans_the_recorded_lift_again(self) -> None:
        base, _, _, entry = _lifted_history()

        assert plan_relift(lift_recipe_of(entry), base) == plan_lift(
            base, SPEC, aspects=PROVENANCE_ONLY
        )

    def test_a_manifest_that_moved_on_gains_the_aspects_again(self) -> None:
        """A re-lift is for a manifest that has been edited since the lift."""
        _, _, _, entry = _lifted_history()
        moved = _manifest(
            {
                "schema": {
                    **PLAIN["schema"],
                    "graph": {
                        **PLAIN["schema"]["graph"],
                        "vertex_config": {
                            "vertices": [
                                *PLAIN["schema"]["graph"]["vertex_config"]["vertices"],
                                {
                                    "name": "Customer",
                                    "properties": [{"name": "email", "type": "STRING"}],
                                    "identity": ["email"],
                                },
                            ]
                        },
                    },
                }
            }
        )

        relifted = apply_evolution(moved, plan_relift(lift_recipe_of(entry), moved))
        vertices = relifted.require_schema().core_schema.vertex_config.vertex_set

        assert {"SourceRecord", "Agent", "Customer"} <= vertices

    def test_drift_names_an_aspect_whose_version_moved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, _, _, entry = _lifted_history()
        recipe = lift_recipe_of(entry)
        assert recipe_drift(recipe) == {}

        moved = ASPECTS["provenance"].model_copy(update={"version": 2})
        monkeypatch.setitem(ASPECTS, "provenance", moved)

        assert recipe_drift(recipe) == {"provenance": (1, 2)}


class TestStampingDoesNotMoveTheContentAddress:
    def test_recording_the_recipe_leaves_the_tree_where_it_was(self) -> None:
        _, lifted, root, entry = _lifted_history()
        before = manifest_hash(lifted)

        assert entry.recipe is not None
        stamp_provenance(
            lifted,
            content_hash=before,
            canon="graflo/canon@test",
            commit=entry.id,
            parents=[root.id],
            recipe=entry.recipe.hash,
        )

        assert manifest_hash(lifted) == before == entry.tree
        assert lifted.metadata is not None and lifted.metadata.provenance is not None
        assert lifted.metadata.provenance.recipe == entry.recipe.hash
