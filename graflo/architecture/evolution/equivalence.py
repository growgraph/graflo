"""Equivalence clusters over compose-time vertex/relation mappings.

A :class:`~graflo.architecture.evolution.ops.VertexEquivalence` (or
:class:`~graflo.architecture.evolution.ops.RelationEquivalence`) declares one
n-ary cluster directly: ``left`` / ``right`` name one or more members on each
side, collapsing onto one ``into`` label. :func:`index_clusters` is the
consistency check over the *declared* clusters of one
:class:`~graflo.architecture.evolution.ops.ComposeManifestsOp` — there is no
connected-component search left to do (one declaration *is* one cluster); it
validates that the declarations do not overlap or collapse into each other by
accident:

* no ``(side, name)`` may be claimed by two declarations — that is the
  author's job to state as one cluster, not two;
* two declarations must not share one ``into`` — sharing an `into` collapses
  them into one composed class, which must be spelled as one n-ary cluster so
  it is visible to review, not left implicit;
* an ``into`` that already exists as a *different*, non-member class on a side
  must not be silently merged into — add it to the cluster explicitly.

Nodes are ``(side, name)`` pairs so a class named ``Org`` on the left is never
confused with ``Org`` on the right.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Literal

from .ops import ComposeManifestsOp, RelationEquivalence, VertexEquivalence

Side = Literal["left", "right"]


class ClusterConflictError(ValueError):
    """Two or more equivalence declarations conflict over cluster membership."""


@dataclass(frozen=True)
class Cluster:
    """One declared n-ary vertex-equivalence cluster."""

    left: tuple[str, ...]
    right: tuple[str, ...]
    into: str
    declaration: VertexEquivalence

    def members(self, side: Side) -> tuple[str, ...]:
        return self.left if side == "left" else self.right

    @property
    def left_names(self) -> frozenset[str]:
        return frozenset(self.left)

    @property
    def right_names(self) -> frozenset[str]:
        return frozenset(self.right)


@dataclass(frozen=True)
class RelationCluster:
    """One declared n-ary relation-equivalence cluster."""

    left: tuple[str, ...]
    right: tuple[str, ...]
    into: str
    declaration: RelationEquivalence

    def members(self, side: Side) -> tuple[str, ...]:
        return self.left if side == "left" else self.right


@dataclass(frozen=True)
class ClusterIndex:
    """Every declared cluster of one compose op, validated for consistency."""

    vertices: tuple[Cluster, ...]
    relations: tuple[RelationCluster, ...]

    @property
    def labels(self) -> frozenset[str]:
        return frozenset(c.into for c in self.vertices)

    def relation_labels(self) -> frozenset[str]:
        return frozenset(c.into for c in self.relations)

    def vertex_members(self, side: Side) -> frozenset[str]:
        out: set[str] = set()
        for c in self.vertices:
            out.update(c.members(side))
        return frozenset(out)

    def relation_members(self, side: Side) -> frozenset[str]:
        out: set[str] = set()
        for c in self.relations:
            out.update(c.members(side))
        return frozenset(out)

    def cluster_for(self, side: Side, name: str) -> Cluster | None:
        for c in self.vertices:
            if name in c.members(side):
                return c
        return None


def _check_declarations(
    declarations: Sequence[tuple[tuple[str, ...], tuple[str, ...], str]],
    *,
    kind: str,
    left_names: Collection[str],
    right_names: Collection[str],
) -> None:
    """Shared overlap / shared-into / occupied-into checks for one declaration kind."""
    claimed: dict[tuple[Side, str], int] = {}
    into_owner: dict[str, int] = {}
    for index, (left, right, into) in enumerate(declarations):
        for side, members in (("left", left), ("right", right)):
            for name in members:
                key: tuple[Side, str] = (side, name)  # type: ignore[assignment]
                prior = claimed.get(key)
                if prior is not None and prior != index:
                    raise ClusterConflictError(
                        f"{kind}: {side}:{name} is claimed by two equivalence "
                        f"declarations (into {declarations[prior][2]!r} and "
                        f"into {into!r}); merge them into one declaration"
                    )
                claimed[key] = index
        prior_owner = into_owner.get(into)
        if prior_owner is not None and prior_owner != index:
            raise ClusterConflictError(
                f"{kind}: two equivalence declarations both target into "
                f"{into!r}; two declarations sharing one `into` collapse into "
                "one composed class — spell it as one declaration naming "
                "every member"
            )
        into_owner[into] = index
        for side, members, names in (
            ("left", left, left_names),
            ("right", right, right_names),
        ):
            if into in names and into not in members:
                raise ClusterConflictError(
                    f"{kind}: into {into!r} already exists on the {side} side "
                    f"but is not a member of its cluster "
                    f"({side}={list(members)}); add it to `{side}` to merge "
                    "into it, or pick a different `into`"
                )


def index_clusters(
    op: ComposeManifestsOp,
    *,
    left_vertices: Collection[str] = (),
    right_vertices: Collection[str] = (),
    left_relations: Collection[str] = (),
    right_relations: Collection[str] = (),
) -> ClusterIndex:
    """Validate and index the declared clusters of *op*.

    Raises :class:`ClusterConflictError` on an overlapping declaration, two
    declarations sharing one ``into``, or an ``into`` that would silently
    occupy an existing non-member class on a side.
    """
    _check_declarations(
        [(tuple(v.left_members), tuple(v.right_members), v.into) for v in op.vertices],
        kind="vertex equivalence",
        left_names=left_vertices,
        right_names=right_vertices,
    )
    _check_declarations(
        [(tuple(r.left_members), tuple(r.right_members), r.into) for r in op.relations],
        kind="relation equivalence",
        left_names=left_relations,
        right_names=right_relations,
    )
    return ClusterIndex(
        vertices=tuple(
            Cluster(
                left=tuple(v.left_members),
                right=tuple(v.right_members),
                into=v.into,
                declaration=v,
            )
            for v in op.vertices
        ),
        relations=tuple(
            RelationCluster(
                left=tuple(r.left_members),
                right=tuple(r.right_members),
                into=r.into,
                declaration=r,
            )
            for r in op.relations
        ),
    )
