"""Recording a lift as a commit.

Kept apart from ``aspects/``, which plans a lift and knows nothing of history,
for the reason ``merge_commit`` is kept apart from ``merge``: recording is what
a commit point does to a result afterwards.

A lift commit is an ordinary single-parent commit whose ops are the **planned**
lift, not a diff of its result, so each op keeps its inverse and a ``revert``
retracts the lift like any other change. What makes it a lift is the
:class:`~graflo.architecture.evolution.aspects.recipe.LiftRecipe` riding
alongside, which a re-lift reads.
"""

from __future__ import annotations

from graflo.architecture.contract.manifest import GraphManifest

from .aspects.recipe import LiftRecipe
from .commit import Commit, CommitError, RecipeRef, build_commit
from .hashing import manifest_hash
from .ops import ManifestOp


def build_lift_commit(
    base: GraphManifest,
    ops: list[ManifestOp],
    *,
    parent: str,
    recipe: LiftRecipe,
    label: str | None = None,
    created_at: str | None = None,
    notes: str | None = None,
) -> Commit:
    """Record the lift *ops* of *base* as a ``lift`` commit carrying *recipe*.

    Args:
        base: The manifest the lift was planned on, the state of *parent*.
        ops: The planned lift, as ``plan_lift`` returned it.
        parent: The commit whose tree is *base*.
        recipe: What the lift was asked to do, planned on *base*.
        label: Short human-readable name; defaults to the aspects lifted.
        created_at: ISO-8601 timestamp.
        notes: Free-form annotation.

    Raises:
        CommitError: *recipe* was planned on another manifest, or *ops* is
            empty or changes nothing -- a manifest that already carries the
            aspects has no lift to record.
    """
    if recipe.base != manifest_hash(base):
        raise CommitError(
            f"the lift recipe was planned on tree {recipe.base[:12]}, not on the "
            f"manifest given as its base ({manifest_hash(base)[:12]})"
        )
    return build_commit(
        base,
        ops,
        parents=[parent],
        kind="lift",
        label=label or f"lift {', '.join(recipe.aspects)}",
        created_at=created_at,
        notes=notes,
        recipe=RecipeRef(
            hash=recipe.content_hash(), kind=recipe.kind, payload=recipe.to_dict()
        ),
    )


def lift_recipe_of(commit: Commit) -> LiftRecipe:
    """The recipe a ``lift`` commit recorded.

    Raises:
        CommitError: *commit* is not a lift commit, or records no recipe.
    """
    if commit.kind != "lift" or commit.recipe is None:
        raise CommitError(
            f"commit '{commit.short()}' is a {commit.kind} commit and records no "
            "lift recipe"
        )
    return LiftRecipe.model_validate(commit.recipe.payload)


__all__ = ["build_lift_commit", "lift_recipe_of"]
