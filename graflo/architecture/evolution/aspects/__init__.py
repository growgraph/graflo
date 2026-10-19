"""Aspects: cross-cutting vocabularies a manifest is lifted with.

An aspect adds the types and relations every domain needs and none owns --
where a fact came from, how a fact changes over time -- grounded in a standard
vocabulary, as a reviewable op list over the manifest. Each aspect plans on
its own; :func:`plan_lift` composes the ones a caller names.

Three ship today:

- ``provenance``: ``SourceRecord`` and ``Agent`` with ``wasDerivedFrom`` and
  ``wasAttributedTo`` (PROV-O). Needs no spec.
- ``state``: ``<Type>State`` with a validity interval and ``<Type>Observation``
  with a unit per row (PROV-O, SOSA, QUDT). Needs a
  :class:`~graflo.architecture.evolution.aspects.spec.LiftSpec`, because a
  planner can see structure but not meaning.
- ``access``: a reserved ``access_zones`` label on the types a spec names, each
  with a merge mode. Needs a spec; not lifted by default.

The output is an op list, so the conversion is replayable, invertible and
reviewable through the machinery every other manifest change already uses.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from graflo.architecture.contract.manifest import GraphManifest

from ..ops import ManifestOp
from .access.plan import plan_access
from .base import Aspect, LiftError
from .provenance import vocabulary as provenance_vocab
from .provenance.plan import plan_provenance
from .spec import EdgeGrounding, Grounding, LiftSpec
from .state.plan import StatePlan, plan_state

PROVENANCE = "provenance"
STATE = "state"
ACCESS = "access"

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
    ACCESS: Aspect(
        name=ACCESS,
        description=(
            "A reserved access_zones label on declared types: the zone ids a "
            "writer stamps and a read guard checks."
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
            ``state`` and ``access`` aspects; the ``provenance`` aspect reads
            only ``provenance_for`` and ``provenance`` from it, the ``access``
            aspect only ``access_for`` and ``access_edges``.
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

    if ACCESS in chosen:
        lifted_provenance = PROVENANCE in chosen and spec.provenance
        ops += plan_access(
            manifest,
            vertices=spec.access_for,
            edges=spec.access_edges,
            minted_vertices=[
                *(
                    [provenance_vocab.SOURCE_RECORD, provenance_vocab.AGENT]
                    if lifted_provenance
                    else []
                ),
                *(state.scaffolded if state is not None else []),
            ],
            minted_relations=(
                ["wasDerivedFrom", "wasAttributedTo"] if lifted_provenance else []
            ),
        )

    if state is not None:
        ops += state.retire
    return ops


__all__ = [
    "ACCESS",
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
    "plan_access",
    "plan_lift",
    "plan_provenance",
    "plan_state",
    "resolve_aspects",
]
