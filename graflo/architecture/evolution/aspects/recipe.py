"""What a lift was asked to do, recorded so that it can be done again.

A lift is a join, so what it adds cannot be told apart from vocabulary the
manifest declared itself, and the spec that produced it cannot be read back
from the result. A :class:`LiftRecipe` keeps both: the aspects as named, the
version of each, and the spec. A lift commit carries one, the way a merge
commit carries a ``MergeRecipe``, and :func:`plan_relift` plans the same lift
again on a manifest that has moved on since.
"""

from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Literal

import suthing
from pydantic import Field as PydanticField

from graflo.architecture.base import ConfigBaseModel
from graflo.architecture.contract.manifest import GraphManifest

from ..hashing import manifest_hash
from ..ops import ManifestOp
from .spec import LiftSpec

#: Spec lists the planners read as sets, so their order is not recorded.
_UNORDERED_SPEC_LISTS: tuple[str, ...] = ("provenance_for", "mentions_for", "observed")


class LiftRecipe(ConfigBaseModel):
    """The aspects a manifest was lifted with, their versions and the spec."""

    kind: Literal["lift"] = "lift"
    base: str = PydanticField(
        ..., description="Content hash of the manifest the lift was planned on."
    )
    aspects: list[str] = PydanticField(
        ..., min_length=1, description="The aspects lifted, in the order named."
    )
    versions: dict[str, int] = PydanticField(
        ..., description="The version of each aspect at the time of the lift."
    )
    spec: dict[str, Any] = PydanticField(
        default_factory=dict,
        description="The lift spec, without its defaults (see `recorded_spec`).",
    )
    graflo_version: str | None = PydanticField(
        default=None, description="The graflo that planned the lift, when known."
    )

    def content_hash(self) -> str:
        """Content address of what the lift declared.

        ``graflo_version`` is left out: it says which tool planned the lift,
        not what was asked of it, and the aspect versions already say whether
        the vocabulary differs.
        """
        return suthing.stable_hash(
            {
                "kind": self.kind,
                "base": self.base,
                "aspects": list(self.aspects),
                "versions": dict(self.versions),
                "spec": self.spec,
            }
        )

    def lift_spec(self) -> LiftSpec:
        """The recorded spec as a :class:`LiftSpec`."""
        return LiftSpec.model_validate(self.spec)


def recorded_spec(spec: LiftSpec) -> dict[str, Any]:
    """*spec* as a recipe records it: no defaults, set-like lists sorted.

    Without defaults, a field added to :class:`LiftSpec` later does not move
    the hash of a recipe recorded before it. The lists the planners read as
    sets are sorted, so two declarations that plan the same lift are the same
    recipe.
    """
    recorded = spec.to_dict(skip_defaults=True)
    for name in _UNORDERED_SPEC_LISTS:
        if name in recorded:
            recorded[name] = sorted(recorded[name])
    return recorded


def build_lift_recipe(
    manifest: GraphManifest,
    spec: LiftSpec | None = None,
    *,
    aspects: Iterable[str],
) -> LiftRecipe:
    """The recipe of lifting *manifest* with *aspects* under *spec*.

    Raises:
        LiftError: an aspect name no aspect has.
    """
    from . import resolve_aspects

    chosen = resolve_aspects(aspects)
    return LiftRecipe(
        base=manifest_hash(manifest),
        aspects=[aspect.name for aspect in chosen],
        versions={aspect.name: aspect.version for aspect in chosen},
        spec=recorded_spec(spec or LiftSpec()),
        graflo_version=_graflo_version(),
    )


def plan_relift(
    recipe: LiftRecipe,
    manifest: GraphManifest,
    *,
    authored: dict[str, Any] | None = None,
) -> list[ManifestOp]:
    """The ops that lift *manifest* again as *recipe* records.

    *manifest* need not be the one the recipe was planned on: a re-lift is
    for a manifest that has moved on, or for an aspect whose version has
    (see :func:`recipe_drift`). A manifest that already carries everything
    plans no ops.

    Raises:
        LiftError: as :func:`~graflo.architecture.evolution.aspects.plan_lift`.
    """
    from . import plan_lift

    return plan_lift(
        manifest, recipe.lift_spec(), aspects=recipe.aspects, authored=authored
    )


def recipe_drift(recipe: LiftRecipe) -> dict[str, tuple[int, int]]:
    """The aspects whose version moved since *recipe*: ``{name: (recorded, now)}``.

    Raises:
        LiftError: an aspect the recipe names that no longer exists.
    """
    from . import resolve_aspects

    current = {
        aspect.name: aspect.version for aspect in resolve_aspects(recipe.versions)
    }
    return {
        name: (recorded, current[name])
        for name, recorded in recipe.versions.items()
        if current[name] != recorded
    }


def _graflo_version() -> str | None:
    try:
        return version("graflo")
    except PackageNotFoundError:
        return None


__all__ = [
    "LiftRecipe",
    "build_lift_recipe",
    "plan_relift",
    "recipe_drift",
    "recorded_spec",
]
