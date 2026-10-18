"""Aspects: cross-cutting vocabularies a manifest is lifted with.

An aspect adds the types and relations every domain needs and none owns --
where a fact came from, how a fact changes over time -- grounded in a standard
vocabulary, as a reviewable op list over the manifest. Each aspect plans on
its own; :func:`plan_lift` composes the ones a caller names.

Two ship today:

- ``provenance``: ``SourceRecord`` and ``Agent`` with ``wasDerivedFrom`` and
  ``wasAttributedTo`` (PROV-O). Needs no spec.
- ``state``: ``<Type>State`` with a validity interval and ``<Type>Observation``
  with a unit per row (PROV-O, SOSA, QUDT). Needs a
  :class:`~graflo.architecture.evolution.aspects.spec.LiftSpec`, because a
  planner can see structure but not meaning.

The output is an op list, so the conversion is replayable, invertible and
reviewable through the machinery every other manifest change already uses.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest

from ..ops import ManifestOp
from .base import Aspect, LiftError
from .provenance.plan import plan_provenance
from .spec import EdgeGrounding, Grounding, LiftSpec
from .state.plan import StatePlan, plan_state

PROVENANCE = "provenance"
STATE = "state"

ASPECTS: dict[str, Aspect] = {
    PROVENANCE: Aspect(
        name=PROVENANCE,
        description=(
            "SourceRecord and Agent with wasDerivedFrom and wasAttributedTo: "
            "where a fact came from and who produced it (PROV-O)."
        ),
        needs_spec=False,
    ),
    STATE: Aspect(
        name=STATE,
        description=(
            "<Type>State with a validity interval and <Type>Observation with a "
            "unit per row: how facts change and what was measured "
            "(PROV-O, SOSA, QUDT)."
        ),
        needs_spec=True,
    ),
}

#: What ``graflo lift`` lifts when no aspect is named.
DEFAULT_ASPECTS: tuple[str, ...] = (PROVENANCE, STATE)


def resolve_aspects(names: Iterable[str]) -> list[Aspect]:
    """The aspects called *names*, in the order given.

    Raises:
        LiftError: a name no aspect has.
    """
    chosen = list(names)
    unknown = sorted(set(chosen) - set(ASPECTS))
    if unknown:
        raise LiftError(f"no aspect named {unknown}; the aspects are {sorted(ASPECTS)}")
    return [ASPECTS[name] for name in chosen]


def plan_lift(
    manifest: GraphManifest,
    spec: LiftSpec | None = None,
    *,
    aspects: Iterable[str] = DEFAULT_ASPECTS,
    authored: dict[str, Any] | None = None,
) -> list[ManifestOp]:
    """The ops that lift *manifest* with the named *aspects*.

    Args:
        manifest: the manifest to lift, already ``finish_init()``-ed.
        spec: the semantic declarations a planner cannot infer. Required by the
            ``state`` aspect; the ``provenance`` aspect reads only
            ``provenance_for`` and ``provenance`` from it.
        aspects: which aspects to lift, by name. The ``state`` aspect's
            scaffolds are attached to ``SourceRecord`` when both are named.
        authored: the manifest as written, when available; see
            :func:`~graflo.architecture.evolution.aspects.state.plan.plan_state`.

    Returns:
        Ops in application order: the additive ops of every aspect, then the
        removals, so a reviewer sees everything that is added before anything
        that is taken away.

    Raises:
        LiftError: an unknown aspect, a missing declaration, or a name the lift
            would mint that is already taken.
    """
    chosen = {aspect.name for aspect in resolve_aspects(aspects)}
    spec = spec or LiftSpec()
    ops: list[ManifestOp] = []
    state: StatePlan | None = None
    if STATE in chosen:
        state = plan_state(manifest, spec, authored=authored)
        ops += state.ops

    if PROVENANCE in chosen and spec.provenance:
        schema = manifest.graph_schema
        existing = (
            set(schema.core_schema.vertex_config.vertex_set)
            if schema is not None
            else set()
        )
        unknown = sorted(set(spec.provenance_for) - existing)
        if unknown:
            raise LiftError(
                f"provenance_for names types the manifest does not have: {unknown}"
            )
        attach = [
            *sorted(spec.provenance_for),
            *(state.scaffolded if state is not None else []),
        ]
        ops += plan_provenance(manifest, attach=attach)

    if state is not None:
        ops += state.retire
    return ops


__all__ = [
    "ASPECTS",
    "DEFAULT_ASPECTS",
    "PROVENANCE",
    "STATE",
    "Aspect",
    "EdgeGrounding",
    "Grounding",
    "LiftError",
    "LiftSpec",
    "StatePlan",
    "plan_lift",
    "plan_provenance",
    "plan_state",
    "resolve_aspects",
]
